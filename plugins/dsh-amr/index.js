/**
 * plugins/dsh-amr/index.js
 * AI Memory Runtime (AMR) Zero-Friction Plugin for DeepSeek Harness (DSH).
 *
 * Requirements:
 * 1. Never pass Promise or non-string to context.text (prevents text.indexOf is not a function).
 * 2. Hook `agent/pre-step` to perform async recall on incoming user messages and inject
 *    memory context using createUserMessage with XML isolation and Fail-open safety.
 * 3. Hook `session/event` (for turn/end) or turn completion to asynchronously ingest
 *    dialogue turns via AMR session.ingest without blocking main turn execution.
 * 4. Compliant with DOC-AMR-09 (AMR 2.x client contract):
 *    - Local adapter UDS; identity is adapter-bound, so no source_agent/agent_id.
 *    - Zero-VRAM: purely UDS client, 0MB host model/GPU overhead.
 *    - Hard deadline: default 80ms timeout on recall.
 *    - Persist-first ingestion: acked requests only, spool for transient failures.
 *    - Fail-open: all exceptions caught, logging gracefully, never crashing DSH.
 */

import crypto from "node:crypto";

import { AmrUdsClient, isTransientError } from "./lib/uds_client.js";
import { buildMemoryContext } from "./lib/context_builder.js";
import { mapSessionToIngest, extractContent } from "./lib/session_mapper.js";
import { IngestSpool } from "./lib/ingest_spool.js";
import { logger } from "./lib/logger.js";

const DEFAULT_DEADLINE_MS = 80;
const DEFAULT_INGEST_DEADLINE_MS = 1500;
const DEFAULT_SCORE_THRESHOLD = 0.65;
const DEFAULT_LIMIT = 3;

export const name = "@deepseek-ai/dsh-amr";
export const inject = [];

/**
 * Check if the text is internal heartbeat, ping or empty noise.
 * @param {string} text
 * @returns {boolean}
 */
export function isNoiseOrHeartbeat(text) {
  if (!text || typeof text !== "string") return true;
  const trimmed = text.trim();
  if (trimmed.length === 0) return true;
  if (/^HEARTBEAT/i.test(trimmed)) return true;
  if (/^PING/i.test(trimmed)) return true;
  if (/^PONG/i.test(trimmed)) return true;
  return false;
}

/**
 * Extract user prompt text from user message content blocks.
 * @param {any} message
 * @returns {string}
 */
export function extractUserPrompt(message) {
  if (!message) return "";
  if (typeof message === "string") return message;
  return extractContent(message);
}

/**
 * Cordis plugin entrypoint for DSH.
 * @param {object} ctx Cordis context
 * @param {object} [config] Plugin options
 */
export function apply(ctx, config = {}) {
  const deadlineMs = config.deadlineMs || DEFAULT_DEADLINE_MS;
  const ingestDeadlineMs = config.ingestDeadlineMs !== undefined
    ? config.ingestDeadlineMs
    : DEFAULT_INGEST_DEADLINE_MS;
  const scoreThreshold = config.scoreThreshold || DEFAULT_SCORE_THRESHOLD;
  const limit = config.limit || DEFAULT_LIMIT;

  const udsClient = new AmrUdsClient({
    socketPath: config.socketPath,
    defaultDeadlineMs: deadlineMs,
  });

  const spool = new IngestSpool({
    persistenceDir: config.persistenceDir,
  });

  // 在途归档的 spool key：replay 必须跳过它们，避免“直发 + 重放”双发。
  const inFlight = new Set();
  let activeArchives = 0;
  let disposed = false;
  ctx.on?.("dispose", () => {
    disposed = true;
  });

  // De-duplicate turn-end callbacks that DSH may emit twice for one turn.
  const archivedTurns = new Set();
  const markTurnHandled = (key) => {
    if (!key) return false;
    if (archivedTurns.has(key)) return true;
    archivedTurns.add(key);
    if (archivedTurns.size > 1024) {
      const oldest = archivedTurns.values().next().value;
      archivedTurns.delete(oldest);
    }
    return false;
  };

  const reportPermanentFailure = (record, err) => {
    const code = err && err.code !== undefined ? err.code : "unknown";
    const key = record && record.key ? record.key : "(unspooled)";
    logger.warn(
      `[dsh-amr] session.ingest permanently failed (code=${code}); record retained for manual handling: ${key}`
    );
  };

  const scheduleReplay = () => {
    if (disposed || activeArchives > 0) return;
    setImmediate(() => {
      if (disposed || activeArchives > 0) return;
      spool.replay(udsClient, {
        deadlineMs: ingestDeadlineMs,
        onPermanent: reportPermanentFailure,
        skipKeys: new Set(inFlight),
      }).catch((err) => {
        logger.debug(`[dsh-amr] spool replay failed open: ${err?.message || err}`);
      });
    });
  };

  /**
   * Persist-first archive: enqueue -> request with id -> remove on ack.
   * Transient failures stay in the spool; permanent failures are retained and
   * never retried forever.
   */
  const archiveTurn = async (sessionId, messages) => {
    if (disposed || !sessionId) return;
    let payload;
    try {
      payload = mapSessionToIngest({ sessionId, messages });
    } catch (err) {
      logger.debug(`[dsh-amr] session mapping skipped: ${err?.message || err}`);
      return;
    }
    if (!payload || !Array.isArray(payload.messages) || payload.messages.length === 0) {
      return;
    }

    let recordKey = null;
    let acknowledged = false;
    activeArchives++;
    try {
      recordKey = spool.enqueue(payload);

      // A record already marked permanent (e.g. -32012) must not be retried forever.
      if (recordKey && spool.get(recordKey)?.permanent) {
        logger.debug(`[dsh-amr] skipping permanently failed spool record ${recordKey}`);
        return;
      }
      if (recordKey) inFlight.add(recordKey);

      await udsClient.request("session.ingest", payload, ingestDeadlineMs);
      acknowledged = true;
      if (recordKey) {
        spool.remove(recordKey);
      }
      logger.debug(`[dsh-amr] session.ingest acknowledged for session ${sessionId}`);
    } catch (err) {
      if (isTransientError(err)) {
        logger.debug(
          `[dsh-amr] session.ingest deferred (transient ${err?.code || "error"}); retained in spool: ${recordKey || "unspooled"}`
        );
        return;
      }

      if (recordKey) {
        const existing = spool.get(recordKey);
        if (!existing?.permanent) {
          const retained = spool.markPermanent(recordKey, err);
          if (retained) {
            reportPermanentFailure(retained, err);
          }
        }
      } else {
        reportPermanentFailure({ key: `${sessionId}:(unspooled)` }, err);
      }
    } finally {
      if (recordKey) inFlight.delete(recordKey);
      activeArchives--;
      // 只在没有在途归档时排空 backlog，避免 replay 与直发双发。
      if (acknowledged) scheduleReplay();
    }
  };

  // Non-blocking startup health check + spool replay. Both fail open.
  setImmediate(() => {
    udsClient.ping(deadlineMs).then((res) => {
      if (res && res.status === "ok") {
        logger.debug("[dsh-amr] amr.ping ok");
      } else {
        logger.info(`[dsh-amr] amr.ping returned unexpected response: ${JSON.stringify(res)}`);
      }
    }).catch((err) => {
      logger.debug(`[dsh-amr] amr.ping unavailable (fail-open): ${err?.message || err}`);
    });

    scheduleReplay();
  });

  /**
   * 1. Dynamic Recall via agent/pre-step
   * In DSH, agent/pre-step is an async waterfall:
   * ({ agent, messages, turn, step, signal }, next) => Promise<PreStepDecision>
   * Only query memory on step === 1 (fresh user input for the turn)
   */
  ctx.on("agent/pre-step", async ({ agent, messages, turn, step, signal }, next) => {
    let decision;
    try {
      decision = await next();
    } catch (err) {
      logger.error(`[dsh-amr] Error in downstream pre-step: ${err?.message || err}`);
      throw err;
    }

    try {
      if (!decision || decision.kind === "reject" || signal?.aborted) {
        return decision;
      }

      // Only perform implicit recall on the first step of a turn where user messages exist
      if (step !== 1 || !Array.isArray(messages) || messages.length === 0) {
        return decision;
      }

      // Extract user prompt from direct user messages
      const userMessages = messages.filter((m) => {
        const sourceKind = m?.source?.kind;
        return sourceKind === "user" || sourceKind === "client" || sourceKind === undefined;
      });

      const candidateText = userMessages
        .map((m) => extractUserPrompt(m))
        .filter(Boolean)
        .join("\n")
        .trim();

      if (!candidateText || isNoiseOrHeartbeat(candidateText)) {
        return decision;
      }

      // Search AMR via UDS with hard deadline
      let searchRes = null;
      try {
        searchRes = await udsClient.request("memory.search", {
          query: candidateText,
          limit,
          score_threshold: scoreThreshold,
        }, deadlineMs);
      } catch (udsErr) {
        logger.debug(`[dsh-amr] Recall skipped or timed out: ${udsErr.message}`);
        return decision;
      }

      if (signal?.aborted) return decision;

      const items = searchRes?.results || searchRes?.memories || (Array.isArray(searchRes) ? searchRes : []);
      if (!items || items.length === 0) {
        return decision;
      }

      const contextXml = buildMemoryContext(items, {
        limit,
        maxItemChars: 800,
        maxTotalChars: 3000,
      });

      if (!contextXml || typeof contextXml !== "string") {
        return decision;
      }

      // Dynamically load createUserMessage from @deepseek-ai/dsh-llm if available
      let createUserMsgFn = null;
      try {
        const dshLlm = await import("@deepseek-ai/dsh-llm");
        if (typeof dshLlm?.createUserMessage === "function") {
          createUserMsgFn = dshLlm.createUserMessage;
        }
      } catch (_) {}

      // DSH Format v4 requires producer-owned source kind (e.g. `plugin:${name}`)
      // and explicitly forbids retired `kind: "plugin"` wrapper.
      const producerKind = `plugin:${name}`;

      const memoryMessage = createUserMsgFn ? createUserMsgFn({
        content: [{
          type: "text",
          text: contextXml,
        }],
        source: {
          kind: producerKind,
          form: "snapshot",
          sections: [{
            name,
            text: contextXml,
          }],
        },
      }) : {
        id: `amr-recall-${Date.now()}`,
        role: "user",
        content: [{
          type: "text",
          text: contextXml,
        }],
        source: {
          kind: producerKind,
          form: "snapshot",
          sections: [{
            name,
            text: contextXml,
          }],
        },
      };

      return {
        ...decision,
        messages: [memoryMessage, ...decision.messages],
      };
    } catch (err) {
      // Fail-open: Never block or crash DSH
      logger.error(`[dsh-amr] Fail-open in agent/pre-step: ${err?.message || err}`);
      return decision;
    }
  }, { prepend: true });

  /**
   * 2. Implicit Ingest on Turn Completion
   * In DSH, when a turn finishes, session.append("turn/end", { turn, reason }) is called,
   * which emits "session/event" with event.type === "turn/end".
   * We also listen to legacy/fallback "turn/end" event if emitted directly by other layers.
   */
  const stableMessagesKey = (messages) => {
    const parts = (Array.isArray(messages) ? messages : []).map((message) => {
      if (!message) return "";
      const id = message.message_id || message.messageId || message.id;
      if (id !== undefined && id !== null && String(id).length > 0) {
        return String(id);
      }
      return extractContent(message);
    });
    return crypto
      .createHash("sha256")
      .update(parts.join("\u0000"))
      .digest("hex")
      .slice(0, 16);
  };

  const handleTurnEnd = (session, turnNumber) => {
    try {
      if (!session || typeof session.snapshotEvents !== "function") return;

      const events = session.snapshotEvents();
      if (!Array.isArray(events) || events.length === 0) return;

      // Extract messages from the session snapshot. Re-ingestion is safe:
      // the server dedupes by (session_id, message_id).
      const turnMessages = [];
      for (const ev of events) {
        if (!ev || !ev.type) continue;
        if (ev.type === "user/message" && ev.data) {
          turnMessages.push({
            role: "user",
            content: extractContent(ev.data),
            id: ev.data.id || `msg-${ev.seq}`,
            timestamp: ev.time || Math.floor(Date.now() / 1000),
          });
        } else if (ev.type === "assistant/message" && ev.data?.message) {
          turnMessages.push({
            role: "assistant",
            content: extractContent(ev.data.message),
            id: ev.data.message.id || `msg-${ev.seq}`,
            timestamp: ev.time || Math.floor(Date.now() / 1000),
          });
        }
      }

      if (turnMessages.length === 0) return;

      // 没有稳定 session id 时不伪造带时间戳的会话，否则同一会话会被拆散；
      // 直接跳过并记录，等待下一次带 id 的事件。
      if (!session.id) {
        logger.debug("[dsh-amr] skipping ingest: session id is missing");
        return;
      }

      // 没有 turn 序号时用消息内容哈希去重，避免不同 turn 都被当成 "latest"
      // 而只归档第一条。key 与 turn/end 监听器保持同一格式。
      const turnKey = `turn:${session.id}:${
        turnNumber !== undefined && turnNumber !== null
          ? turnNumber
          : stableMessagesKey(turnMessages)
      }`;
      if (markTurnHandled(turnKey)) return;

      // Fire-and-forget: archiveTurn catches all errors and never blocks DSH.
      void archiveTurn(session.id, turnMessages);
    } catch (err) {
      logger.debug(`[dsh-amr] Ingest failed fail-open: ${err?.message || err}`);
    }
  };

  ctx.on("session/event", (session, event) => {
    try {
      if (event?.type === "turn/end") {
        handleTurnEnd(session, event?.data?.turn);
      }
    } catch (_) {}
  });

  ctx.on("turn/end", (turn) => {
    try {
      if (!turn || !turn.sessionId || !Array.isArray(turn.messages)) return;
      const explicitTurnId = turn.id ?? turn.turn ?? turn.turnNumber;
      const suffix =
        explicitTurnId !== undefined && explicitTurnId !== null
          ? String(explicitTurnId)
          : stableMessagesKey(turn.messages);
      const turnKey = `turn:${turn.sessionId}:${suffix}`;
      if (markTurnHandled(turnKey)) return;
      void archiveTurn(turn.sessionId, turn.messages);
    } catch (_) {}
  });
}

export default { name, inject, apply };

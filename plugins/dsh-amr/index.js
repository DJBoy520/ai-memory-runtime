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
import { SQLiteSpool } from "./lib/sqlite_spool.js";
import { logger } from "./lib/logger.js";

const FLUSH_RETRY_LIMIT = 3;
const DEFAULT_DEADLINE_MS = 80;
const DEFAULT_INGEST_DEADLINE_MS = 1500;
const DEFAULT_SCORE_THRESHOLD = 0.65;
const DEFAULT_LIMIT = 3;

export const name = "@deepseek-ai/dsh-amr";
export const inject = ["sessions"];

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

function isHumanUserMessage(message) {
  const kind = message?.source?.kind;
  return kind === undefined || kind === "user" || kind === "client";
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

  let spool;
  try {
    const backend = config.spoolBackend ?? "jsonl";
    if (!["jsonl", "sqlite"].includes(backend)) throw new Error("unknown spoolBackend");
    const options = { persistenceDir: config.persistenceDir, maxBytes: config.spoolMaxBytes, maxRecords: config.spoolMaxRecords };
    if (backend === "sqlite") {
      let instance;
      let openRetryable = false;
      const open = () => {
        if (instance) return instance;
        try { instance = new SQLiteSpool(options); openRetryable = false; }
        catch (err) {
          openRetryable = [5, 6].includes((err.errcode ?? -1) & 0xff);
          logger.warn(`[dsh-amr] SQLite spool unavailable: ${err?.message || err}`);
        }
        return instance;
      };
      spool = {
        enqueue: (...args) => open()?.enqueue(...args) ?? null,
        get: (...args) => open()?.get(...args),
        remove: (...args) => open()?.remove(...args) ?? false,
        markPermanent: (...args) => open()?.markPermanent(...args) ?? null,
        replay: (...args) => open()?.replay(...args) ?? Promise.resolve({ skipped: 1, transient: 0 }),
        get _replaying() { return instance?._replaying ?? false; },
        get lastEnqueueRetryable() { return instance ? instance.lastEnqueueRetryable : openRetryable; },
        close: () => { instance?.close(); instance = undefined; },
      };
    } else {
      spool = new IngestSpool(options);
    }
  } catch (err) {
    logger.error(`[dsh-amr] spool unavailable; archival disabled: ${err?.message || err}`);
    spool = { enqueue: () => null, get: () => undefined, replay: async () => ({ skipped: 0, transient: 0 }) };
  }

  // 在途归档的 spool key：replay 必须跳过它们，避免“直发 + 重放”双发。
  const inFlight = new Set();
  let activeArchives = 0;
  let disposed = false;
  const initialDelay = Math.max(10, Number(config.replayInitialDelayMs) || 1000);
  const maxDelay = Math.max(initialDelay, Number(config.replayMaxDelayMs) || 60000);
  let retryDelay = initialDelay;
  let replayTimer = null;
  let deferredTimer = null;
  let deferredBytes = 0;
  const deferred = new Map();
  let flushTimer = null;
  let flushBytes = 0;
  let flushDelay = initialDelay;
  const flushJobs = new Map();
  ctx.on?.("dispose", () => {
    disposed = true;
    if (replayTimer) clearTimeout(replayTimer);
    if (deferredTimer) clearTimeout(deferredTimer);
    if (flushTimer) clearTimeout(flushTimer);
    const closeWhenIdle = () => {
      if (activeArchives || spool._replaying) {
        const timer = setTimeout(closeWhenIdle, 10);
        timer.unref?.();
      } else if (typeof spool.close === "function") {
        spool.close();
      }
    };
    closeWhenIdle();
  });

  // De-duplicate turn-end callbacks that DSH may emit twice for one turn.
  const archivedTurns = new Map();
  const markTurnHandled = (key, revision = null) => {
    if (!key) return false;
    if (archivedTurns.has(key)) {
      const previous = archivedTurns.get(key);
      if (revision === null || (previous !== null && revision <= previous)) return true;
      archivedTurns.set(key, revision);
      return false;
    }
    archivedTurns.set(key, revision);
    if (archivedTurns.size > 1024) {
      const oldest = archivedTurns.keys().next().value;
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

  const scheduleReplay = (delay = 0) => {
    if (disposed || replayTimer) return;
    replayTimer = setTimeout(() => {
      replayTimer = null;
      if (disposed) return;
      if (activeArchives > 0) { scheduleReplay(initialDelay); return; }
      spool.replay(udsClient, {
        deadlineMs: ingestDeadlineMs,
        onPermanent: reportPermanentFailure,
        skipKeys: inFlight,
      }).then((summary) => {
        if (summary.transient || summary.skipped) {
          scheduleReplay(retryDelay);
          retryDelay = Math.min(maxDelay, retryDelay * 2);
        } else {
          retryDelay = initialDelay;
        }
      }).catch((err) => {
        logger.debug(`[dsh-amr] spool replay failed open: ${err?.message || err}`);
        scheduleReplay(retryDelay);
      });
    }, delay);
    replayTimer.unref?.();
  };

  const scheduleDeferred = () => {
    if (disposed || deferredTimer || deferred.size === 0) return;
    deferredTimer = setTimeout(async () => {
      deferredTimer = null;
      if (disposed || deferred.size === 0) return;
      const [key, job] = deferred.entries().next().value;
      const done = await archiveTurn(job.sessionId, job.messages, key, true);
      if (done !== false && deferred.get(key) === job) { deferred.delete(key); deferredBytes -= job.bytes; }
      scheduleDeferred();
    }, initialDelay);
    deferredTimer.unref?.();
  };

  const deferTurn = (sessionId, messages, turnKey) => {
    if (!turnKey) return;
    const previous = deferred.get(turnKey);
    const revision = (items) => items.reduce((max, item) => Number.isSafeInteger(item?.source_revision) ? Math.max(max, item.source_revision) : max, -1);
    if (previous && revision(messages) <= revision(previous.messages)) return;
    const snapshot = JSON.stringify(messages);
    const bytes = Buffer.byteLength(snapshot, "utf8");
    if ((!previous && deferred.size >= 100) || deferredBytes - (previous?.bytes || 0) + bytes > 2 * 1024 * 1024) {
      archivedTurns.delete(turnKey);
      logger.error(`[dsh-amr] temporary in-memory retry queue full; archive not persisted: ${sessionId}`);
      return;
    }
    deferred.set(turnKey, { sessionId, messages: JSON.parse(snapshot), bytes });
    deferredBytes += bytes - (previous?.bytes || 0);
    logger.warn(`[dsh-amr] database busy; archive temporarily held in memory: ${sessionId}`);
    scheduleDeferred();
  };

  const scheduleFlush = (delay = 0) => {
    if (disposed || flushTimer || flushJobs.size === 0) return;
    flushTimer = setTimeout(async () => {
      flushTimer = null;
      if (disposed || flushJobs.size === 0) return;
      const [key, job] = flushJobs.entries().next().value;
      const release = () => {
        flushJobs.delete(key);
        flushBytes -= job.bytes;
        flushDelay = initialDelay;
        void archiveTurn(job.session.id, job.messages, job.turnKey);
        scheduleFlush();
      };
      // A disposed/detached session can never settle a durability barrier;
      // waiting on it would block the queue head forever and starve every
      // later archive. Archive without the barrier instead of losing the turn.
      let live = job.session;
      try {
        if (typeof ctx.sessions?.get === "function") live = ctx.sessions.get(job.session.id);
      } catch (_) {}
      if (live !== job.session) {
        logger.warn(`[dsh-amr] session is no longer live; archiving without durability barrier: ${job.session.id}`);
        release();
        return;
      }
      try {
        if (typeof ctx.sessions?.flush !== "function" || await ctx.sessions.flush(job.session) !== true) {
          throw new Error("no session durability listener");
        }
        release();
      } catch (err) {
        job.failures = (Number(job.failures) || 0) + 1;
        if (job.failures >= FLUSH_RETRY_LIMIT) {
          logger.error(`[dsh-amr] durability barrier unavailable after ${job.failures} attempts; archiving without it: ${job.session.id}`);
          release();
          return;
        }
        logger.warn(`[dsh-amr] session durability checkpoint failed; retrying (${job.failures}/${FLUSH_RETRY_LIMIT}): ${err?.message || err}`);
        scheduleFlush(flushDelay);
        flushDelay = Math.min(maxDelay, flushDelay * 2);
      }
    }, delay);
    flushTimer.unref?.();
  };

  const queueVersionedArchive = (session, messages, turnKey, revision) => {
    const key = JSON.stringify([turnKey, revision]);
    if (flushJobs.has(key)) return;
    const bytes = Buffer.byteLength(JSON.stringify(messages), "utf8");
    if (flushJobs.size >= 100 || flushBytes + bytes > 2 * 1024 * 1024) {
      archivedTurns.delete(turnKey);
      logger.error(`[dsh-amr] session checkpoint queue full; archive not persisted: ${session.id}`);
      return;
    }
    flushJobs.set(key, { session, messages, turnKey, bytes, failures: 0 });
    flushBytes += bytes;
    // Defer until session/event publication finishes, so all persistence
    // listeners have observed the turn/end before the durability barrier.
    scheduleFlush();
  };

  /**
   * Persist-first archive: enqueue -> request with id -> remove on ack.
   * Transient failures stay in the spool; permanent failures are retained and
   * never retried forever.
   */
  const archiveTurn = async (sessionId, messages, turnKey, fromDeferred = false) => {
    if (disposed || !sessionId) return;
    if (!fromDeferred && deferred.size) {
      deferTurn(sessionId, messages, turnKey);
      return;
    }
    let payload;
    try {
      const archivalMessages = (Array.isArray(messages) ? messages : []).filter((m) => {
        const role = String(m?.role || m?.senderKind || "user").toLowerCase();
        return !["user", "human"].includes(role) || isHumanUserMessage(m);
      });
      payload = mapSessionToIngest({ sessionId, messages: archivalMessages });
    } catch (err) {
      logger.debug(`[dsh-amr] session mapping skipped: ${err?.message || err}`);
      return;
    }
    if (!payload || !Array.isArray(payload.messages) || payload.messages.length === 0) {
      return;
    }

    let recordKey = null;
    let recordGeneration;
    let storedPayload = payload;
    let acknowledged = false;
    let ownsInFlight = false;
    activeArchives++;
    try {
      const queued = spool.enqueue(payload);
      if (!queued) {
        if (spool.lastEnqueueRetryable) {
          if (!fromDeferred) deferTurn(sessionId, messages, turnKey);
          return false;
        }
        if (turnKey) archivedTurns.delete(turnKey);
        logger.error(`[dsh-amr] session.ingest not sent: spool write failed for session ${sessionId}`);
        return;
      }
      recordKey = typeof queued === "string" ? queued : queued.key;
      recordGeneration = typeof queued === "string" ? undefined : queued.generation;
      storedPayload = typeof queued === "string" ? payload : queued.payload;

      // A record already marked permanent (e.g. -32012) must not be retried forever.
      if ((typeof queued === "string" ? spool.get(recordKey) : queued)?.permanent) {
        logger.debug(`[dsh-amr] skipping permanently failed spool record ${recordKey}`);
        return;
      }
      if (inFlight.has(recordKey)) return;
      if (recordKey) {
        inFlight.add(recordKey);
        ownsInFlight = true;
      }

      await udsClient.request("session.ingest", storedPayload, ingestDeadlineMs);
      acknowledged = true;
      if (recordKey) {
        try {
          // The server accepted the batch; a failed local delete must leave the
          // record for replay (at-least-once) rather than silently drop it.
          if (spool.remove(recordKey, recordGeneration) === false && spool.get(recordKey)) {
            logger.warn(`[dsh-amr] acknowledgement cleanup failed; record retained: ${recordKey}`);
            scheduleReplay(retryDelay);
          }
        } catch (removeErr) {
          logger.warn(`[dsh-amr] acknowledgement cleanup failed: ${removeErr?.message || removeErr}`);
          scheduleReplay(retryDelay);
        }
      }
      logger.debug(`[dsh-amr] session.ingest acknowledged for session ${sessionId}`);
    } catch (err) {
      if (isTransientError(err)) {
        logger.debug(
          `[dsh-amr] session.ingest deferred (transient ${err?.code || "error"}); retained in spool: ${recordKey || "unspooled"}`
        );
        scheduleReplay(retryDelay);
        retryDelay = Math.min(maxDelay, retryDelay * 2);
        return;
      }

      if (recordKey) {
        try {
          const existing = spool.get(recordKey);
          if (!existing?.permanent) {
            const retained = spool.markPermanent(recordKey, err, recordGeneration);
            if (retained) {
              reportPermanentFailure(retained, err);
            } else {
              logger.error(`[dsh-amr] permanent failure marker could not be persisted: ${recordKey}`);
              scheduleReplay(retryDelay);
            }
          }
        } catch (spoolErr) {
          logger.error(`[dsh-amr] cannot inspect failed spool record: ${spoolErr?.message || spoolErr}`);
          scheduleReplay(retryDelay);
        }
      } else {
        reportPermanentFailure({ key: `${sessionId}:(unspooled)` }, err);
      }
    } finally {
      if (ownsInFlight) inFlight.delete(recordKey);
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

      // Prefer the host's projected surface: later events may replace earlier
      // messages without changing their IDs. A consistent snapshot watermark
      // lets the server reject older snapshots arriving after newer ones.
      const turnMessages = [];
      const lastSeq = events[events.length - 1]?.seq;
      const canVersion = typeof session.deriveMessages === "function" &&
        Number.isSafeInteger(lastSeq) && lastSeq >= 0 && session.seq === lastSeq + 1;
      if (canVersion) {
        const timeById = new Map();
        for (const ev of events) {
          const msg = ev?.type === "user/message" ? ev.data :
            ev?.type === "assistant/message" ? ev.data?.message : null;
          if (msg?.id && typeof ev.time === "number") timeById.set(String(msg.id), ev.time);
        }
        const projected = session.deriveMessages();
        if (!Array.isArray(projected)) return;
        for (const msg of projected) {
          if (!msg || !["user", "assistant"].includes(msg.role)) continue;
          if (msg.role === "user" && !isHumanUserMessage(msg)) continue;
          if (!msg.id) continue;
          turnMessages.push({
            role: msg.role, content: extractContent(msg), id: msg.id,
            timestamp: timeById.get(String(msg.id)) ?? Math.floor(Date.now() / 1000),
            source_revision: lastSeq,
          });
        }
      } else {
        // Old hosts without a trustworthy projection/watermark retain their
        // legacy behavior only when explicitly enabled.
        if (config.legacyUnversionedSnapshots !== true) {
          logger.warn("[dsh-amr] session snapshot has no trusted watermark; archival skipped");
          return;
        }
        for (const ev of events) {
          if (!ev || !ev.type) continue;
          if (ev.type === "user/message" && ev.data) {
            if (!isHumanUserMessage(ev.data)) continue;
            turnMessages.push({
              role: "user", content: extractContent(ev.data),
              id: ev.data.id || `msg-${ev.seq}`,
              timestamp: ev.time || Math.floor(Date.now() / 1000),
            });
          } else if (ev.type === "assistant/message" && ev.data?.message) {
            turnMessages.push({
              role: "assistant", content: extractContent(ev.data.message),
              id: ev.data.message.id || `msg-${ev.seq}`,
              timestamp: ev.time || Math.floor(Date.now() / 1000),
            });
          }
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
      if (markTurnHandled(turnKey, canVersion ? lastSeq : null)) return;

      // Fire-and-forget: archiveTurn catches all errors and never blocks DSH.
      if (canVersion) queueVersionedArchive(session, turnMessages, turnKey, lastSeq);
      else void archiveTurn(session.id, turnMessages, turnKey);
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
      // Legacy callbacks have no trustworthy session watermark; enabling this
      // fallback can conflict with versioned session/event snapshots.
      if (config.legacyTurnEnd !== true) return;
      if (!turn || !turn.sessionId || !Array.isArray(turn.messages)) return;
      const explicitTurnId = turn.id ?? turn.turn ?? turn.turnNumber;
      const suffix =
        explicitTurnId !== undefined && explicitTurnId !== null
          ? String(explicitTurnId)
          : stableMessagesKey(turn.messages);
      const turnKey = `turn:${turn.sessionId}:${suffix}`;
      if (markTurnHandled(turnKey)) return;
      void archiveTurn(turn.sessionId, turn.messages, turnKey);
    } catch (_) {}
  });
}

export default { name, inject, apply };

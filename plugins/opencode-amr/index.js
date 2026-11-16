/**
 * index.js
 * OpenCode Native AI Memory Runtime (AMR) Plugin — AMR 2.x adapter client.
 *
 * Zero-Friction (无感使用) guarantees:
 * 1. Prefetch hook: intercepts `chat.message`, searches AMR over the local
 *    adapter UDS with a hard 80ms deadline, and injects recalled memories as a
 *    synthetic context part.
 * 2. Session ingest: `session.idle` / `session.completed` archives the
 *    conversation through the 2.x `session.ingest` request (with JSON-RPC id),
 *    persist-first into a bounded JSONL spool. Only an adapter acknowledgement
 *    removes the spool record; transient failures are replayed later.
 * 3. Fail-open architecture: every boundary is wrapped, no hook blocks or
 *    crashes OpenCode, and 2.x identity is adapter-bound (no `source_agent` /
 *    `agent_id` is ever sent).
 *
 * Contract: docs/DOC-AMR-09-CLIENT-2X.md
 */

import crypto from "node:crypto";

import { AmrUdsClient, isTransientError } from "./lib/uds_client.js";
import { buildMemoryContext } from "./lib/context_builder.js";
import { mapSessionToIngest, normalizeRole, extractContent } from "./lib/session_mapper.js";
import { IngestSpool } from "./lib/ingest_spool.js";
import { logger } from "./lib/logger.js";

const DEFAULT_DEADLINE_MS = 80;
const DEFAULT_INGEST_DEADLINE_MS = 1500;
const DEFAULT_SCORE_THRESHOLD = 0.65;
const DEFAULT_LIMIT = 3;
const MAX_QUERY_CHARS = 4000;

export function isNoiseOrHeartbeat(text) {
  if (!text || typeof text !== "string") return true;
  const trimmed = text.trim();
  if (trimmed.length === 0) return true;
  if (/^HEARTBEAT/i.test(trimmed)) return true;
  if (/^PING/i.test(trimmed)) return true;
  return false;
}

export const id = "opencode-amr";

/**
 * Stable fallback message id used when OpenCode does not expose `m.info.id`.
 * sha256(sessionId + seq + role + text) truncated; stable across restarts.
 *
 * @param {string} sessionId
 * @param {number} seq
 * @param {string} role
 * @param {string} text
 * @returns {string}
 */
export function stableMessageId(sessionId, seq, role, text) {
  return crypto
    .createHash("sha256")
    .update(`${sessionId}\u0000${seq}\u0000${role}\u0000${text}`)
    .digest("hex")
    .slice(0, 24);
}

/**
 * Convert OpenCode `client.session.messages()` output into AMR message
 * objects. Text parts only; synthetic parts (including our own recalled
 * context) are excluded. A missing `info.id` falls back to a stable hash so
 * spool keys never rely on wall-clock values.
 *
 * @param {string} sessionId
 * @param {Array<any>} rawMessages
 * @returns {Array<{message_id: string, role: string, content: string, sequence: number, timestamp: number}>}
 */
export function buildIngestMessages(sessionId, rawMessages) {
  const messages = [];
  const list = Array.isArray(rawMessages) ? rawMessages : [];
  let seq = 0;

  for (const raw of list) {
    if (!raw) continue;

    const info = raw.info && typeof raw.info === "object" ? raw.info : {};
    const role = normalizeRole(info.role || raw.role || "user");

    let text = "";
    if (Array.isArray(raw.parts)) {
      text = raw.parts
        .filter((part) => part && part.type === "text" && !part.synthetic)
        .map((part) => (typeof part.text === "string" ? part.text : ""))
        .filter(Boolean)
        .join("\n");
    } else {
      text = extractContent(raw);
    }

    if (!text || text.trim().length === 0) continue;
    seq++;

    const explicitId = info.id ?? raw.message_id ?? raw.messageId ?? raw.id;
    const messageId =
      explicitId !== undefined && explicitId !== null && String(explicitId).trim().length > 0
        ? String(explicitId)
        : stableMessageId(String(sessionId), seq, role, text);

    const createdAt = info.createdAt ?? raw.createdAt ?? raw.timestamp;
    let timestamp = Math.floor(Date.now() / 1000);
    if (createdAt instanceof Date) {
      timestamp = Math.floor(createdAt.getTime() / 1000);
    } else if (typeof createdAt === "number" && Number.isFinite(createdAt)) {
      timestamp = createdAt > 1e11 ? Math.floor(createdAt / 1000) : Math.floor(createdAt);
    } else if (typeof createdAt === "string") {
      const parsed = Date.parse(createdAt);
      if (!Number.isNaN(parsed)) timestamp = Math.floor(parsed / 1000);
    }

    messages.push({
      message_id: messageId,
      role,
      content: text.trim(),
      sequence: seq,
      timestamp,
    });
  }

  return messages;
}

/**
 * OpenCode plugin server entrypoint.
 *
 * Plugin options may be supplied either on the server input object or as the
 * second argument (OpenCode plugin options):
 * - `socketPath`: explicit adapter socket path (preferred over `AMR2_SOCKET_PATH`).
 * - `tokenFile`: private per-agent credential file (also `AMR2_TOKEN_FILE`).
 * - `deadlineMs`: recall/search deadline (default 80ms).
 * - `ingestDeadlineMs`: session.ingest deadline (default 1500ms).
 * - `persistenceDir`: spool directory (env `AMR_OPENCODE_SPOOL_DIR` fallback).
 *
 * @param {object} [input] OpenCode `{ project, client, $, directory, worktree }`
 * @param {object} [pluginOptions]
 */
export const server = async (input = {}, pluginOptions = {}) => {
  const base = input && typeof input === "object" ? input : {};
  const config = pluginOptions && typeof pluginOptions === "object"
    ? { ...base, ...pluginOptions }
    : base;

  const deadlineMs = config.deadlineMs || DEFAULT_DEADLINE_MS;
  const ingestDeadlineMs = config.ingestDeadlineMs !== undefined && config.ingestDeadlineMs !== null
    ? config.ingestDeadlineMs
    : DEFAULT_INGEST_DEADLINE_MS;
  const scoreThreshold = config.scoreThreshold || DEFAULT_SCORE_THRESHOLD;
  const limit = config.limit || DEFAULT_LIMIT;
  const client = config.client;
  const directory = config.directory;

  const udsClient = new AmrUdsClient({
    socketPath: config.socketPath,
    tokenFile: config.tokenFile,
    defaultDeadlineMs: deadlineMs,
  });

  const spool = new IngestSpool({
    persistenceDir: config.persistenceDir,
  });

  let disposed = false;
  let replayScheduled = false;
  let activeArchives = 0;
  // Spool keys currently being sent directly: replay must skip them so a
  // record is never delivered twice (direct send + replay).
  const inFlight = new Set();

  const reportPermanentFailure = (record, err) => {
    const code = err && err.code !== undefined ? err.code : "unknown";
    const key = record && record.key ? record.key : "(unspooled)";
    logger.warn(
      `[opencode-amr] session.ingest permanently failed (code=${code}); record retained for manual handling: ${key}`
    );
  };

  const scheduleReplay = () => {
    if (disposed || activeArchives > 0 || replayScheduled) return;
    replayScheduled = true;
    setImmediate(() => {
      replayScheduled = false;
      if (disposed || activeArchives > 0) return;
      spool.replay(udsClient, {
        deadlineMs: ingestDeadlineMs,
        onPermanent: reportPermanentFailure,
        skipKeys: new Set(inFlight),
      }).catch((err) => {
        logger.debug(`[opencode-amr] spool replay failed open: ${err?.message || err}`);
      });
    });
  };

  /**
   * Persist-first archive: enqueue -> request with id -> remove on ack.
   * Transient failures (-32010 / timeout / connection errors) stay pending;
   * permanent failures are marked once and retained, never retried forever.
   *
   * @param {string} sessionId
   * @param {Array<object>} messages
   */
  const archiveTurn = async (sessionId, messages) => {
    if (disposed || !sessionId) return;

    let payload;
    try {
      payload = mapSessionToIngest({ sessionId, messages });
    } catch (err) {
      logger.debug(`[opencode-amr] session mapping skipped: ${err?.message || err}`);
      return;
    }
    if (!payload || !Array.isArray(payload.messages) || payload.messages.length === 0) {
      return;
    }

    let recordKey = null;
    let ownsInFlight = false;
    let acknowledged = false;
    activeArchives++;
    try {
      recordKey = spool.enqueue(payload);

      // A record already marked permanent (e.g. -32012) must not be retried.
      if (recordKey && spool.get(recordKey)?.permanent) {
        logger.debug(`[opencode-amr] skipping permanently failed spool record ${recordKey}`);
        return;
      }

      if (recordKey) {
        // Another idle/completed callback may already be archiving this exact
        // snapshot; skip it instead of sending the same record twice.
        if (inFlight.has(recordKey)) {
          logger.debug(`[opencode-amr] skipping duplicate in-flight archive ${recordKey}`);
          return;
        }
        inFlight.add(recordKey);
        ownsInFlight = true;
      }

      await udsClient.request("session.ingest", payload, ingestDeadlineMs);
      acknowledged = true;
      if (recordKey) {
        spool.remove(recordKey);
      }
      logger.debug(`[opencode-amr] session.ingest acknowledged for session ${sessionId}`);
    } catch (err) {
      if (isTransientError(err)) {
        logger.debug(
          `[opencode-amr] session.ingest deferred (transient ${err?.code || "error"}); retained in spool: ${recordKey || "unspooled"}`
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
      if (recordKey && ownsInFlight) inFlight.delete(recordKey);
      activeArchives--;
      // Only drain the backlog when the adapter is healthy (ack) and no
      // other archive is in flight, so replay and direct send cannot overlap.
      if (acknowledged) scheduleReplay();
    }
  };

  const handleSessionEvent = async (ev) => {
    const eventObj = ev && ev.event && typeof ev.event === "object" ? ev.event : ev;
    if (!eventObj || (eventObj.type !== "session.idle" && eventObj.type !== "session.completed")) {
      return;
    }

    const properties = eventObj.properties && typeof eventObj.properties === "object"
      ? eventObj.properties
      : {};
    const sessionId =
      eventObj.sessionID ||
      eventObj.sessionId ||
      properties.sessionID ||
      properties.sessionId ||
      properties.id ||
      eventObj.id;
    if (!sessionId) {
      logger.debug("[opencode-amr] skipping ingest: session id is missing");
      return;
    }

    if (!client || typeof client.session?.messages !== "function") {
      return;
    }

    const resp = await client.session.messages({ path: { id: sessionId } });
    const rawMessages = Array.isArray(resp?.data)
      ? resp.data
      : Array.isArray(resp)
        ? resp
        : [];
    const messages = buildIngestMessages(String(sessionId), rawMessages);
    if (messages.length === 0) return;

    await archiveTurn(String(sessionId), messages);
  };

  // Non-blocking startup health check + spool replay. Both fail open.
  setImmediate(() => {
    try {
      udsClient.ping(deadlineMs).then((res) => {
        if (res && res.status === "ok") {
          logger.debug("[opencode-amr] amr.ping ok");
        } else {
          logger.info(`[opencode-amr] amr.ping returned unexpected response: ${JSON.stringify(res)}`);
        }
      }).catch((err) => {
        logger.debug(`[opencode-amr] amr.ping unavailable (fail-open): ${err?.message || err}`);
      });

      scheduleReplay();
    } catch (err) {
      logger.debug(`[opencode-amr] startup background task failed open: ${err?.message || err}`);
    }
  });

  logger.info(`[opencode-amr] Initialized for workspace: ${directory || "default"}`);

  return {
    /**
     * OpenCode lifecycle hook: chat.message
     * Zero-friction prefetch; injects recalled memory as a synthetic part.
     */
    "chat.message": async (inputMessage, output) => {
      try {
        if (!output || !Array.isArray(output.parts)) return;

        const textParts = output.parts.filter((p) => p && p.type === "text" && !p.synthetic);
        if (textParts.length === 0) return;

        const userMessage = textParts
          .map((p) => (typeof p.text === "string" ? p.text : ""))
          .join("\n");
        if (!userMessage.trim() || isNoiseOrHeartbeat(userMessage)) return;
        // 同一消息若已被本插件注入过上下文，不要重复注入导致上下文膨胀。
        const alreadyInjected = output.parts.some(
          (part) =>
            part &&
            part.synthetic &&
            typeof part.text === "string" &&
            part.text.includes("<amr_recalled_context>")
        );
        if (alreadyInjected) return;
        if (
          userMessage.includes("# User Profile Analysis") ||
          (userMessage.includes("Analyze this conversation.") && userMessage.includes('type="skip"'))
        ) {
          return;
        }

        const startTime = Date.now();
        let searchResult = null;
        try {
          // 2.x: identity is adapter-bound; never send source_agent/agent_id.
          // 超长输入截断，避免把巨型 query 送进 UDS/检索后端。
          const query = userMessage.length > MAX_QUERY_CHARS
            ? userMessage.slice(0, MAX_QUERY_CHARS)
            : userMessage;
          searchResult = await udsClient.request("memory.search", {
            query,
            limit,
            score_threshold: scoreThreshold,
          }, deadlineMs);
        } catch (udsErr) {
          logger.debug(`[opencode-amr] Prefetch skipped/timed out: ${udsErr?.message || udsErr}`);
          return;
        }

        const items = searchResult?.results ||
          searchResult?.memories ||
          (Array.isArray(searchResult) ? searchResult : []);
        if (!items || items.length === 0) {
          logger.debug(`[opencode-amr] No relevant memories found (${Date.now() - startTime}ms)`);
          return;
        }

        const contextXml = buildMemoryContext(items, {
          limit,
          maxItemChars: 800,
          maxTotalChars: 3000,
        });

        if (contextXml && typeof contextXml === "string") {
          // Prepend as synthetic context part so the user never has to invoke
          // anything manually. Synthetic parts are excluded from ingest.
          output.parts.unshift({
            type: "text",
            text: contextXml,
            synthetic: true,
          });
          logger.info(
            `[opencode-amr] Injected ${items.length} AMR memories into context (${Date.now() - startTime}ms)`
          );
        }
      } catch (err) {
        // Fail-open guarantee: never disrupt editor/chat flow.
        logger.error(`[opencode-amr] Error in chat.message hook: ${err?.message || err}`);
      }
    },

    /**
     * OpenCode session completion/idle hook. Runs detached (fire-and-forget)
     * so the host is never blocked by UDS latency or spool IO.
     */
    event: (ev) => {
      try {
        void handleSessionEvent(ev).catch((err) => {
          logger.debug(`[opencode-amr] Failed to ingest session (fail-open): ${err?.message || err}`);
        });
      } catch (err) {
        logger.debug(`[opencode-amr] Event hook failed open: ${err?.message || err}`);
      }
    },
  };
};

export default { id, server };

/**
 * index.js
 * OpenClaw Native AI Memory Runtime (AMR) 2.x Adapter Plugin.
 *
 * Capabilities:
 * 1. Zero-Friction Prefetch: Intercepts before_prompt_build, queries AMR UDS with hard deadline,
 *    and injects sanitized XML memory block into system context.
 * 2. Reliable Session Ingest: Hooks agent_end to persist session conversations into AMR's SQLite
 *    sessions.db via confirmed UDS session.ingest request with configurable deadline timeout.
 *    The adapter socket binds the agent identity; no agent_id/source_agent is sent.
 * 3. Fail-Open Architecture: Catches all errors at boundary, zero crashes, zero VRAM overhead.
 *    Startup amr.ping is fire-and-forget. Transient failures stay in the offline spool;
 *    permanent 2.x failures are retained for manual handling and never retried forever.
 */

import { AmrUdsClient, isTransientError, isPermanentError } from "./lib/uds_client.js";
import { buildMemoryContext } from "./lib/context_builder.js";
import { mapSessionToIngest } from "./lib/session_mapper.js";
import { SessionPersistenceManager } from "./lib/session_persistence.js";
import { logger } from "./lib/logger.js";

export { SessionPersistenceManager };

/**
 * Checks if input is a system heartbeat or noise query that should bypass memory prefetch.
 * @param {string} prompt
 * @returns {boolean}
 */
function extractPromptText(prompt) {
  if (typeof prompt === "string") return prompt;
  if (!prompt || typeof prompt !== "object") return "";
  if (typeof prompt.text === "string") return prompt.text;
  if (Array.isArray(prompt.parts)) {
    return prompt.parts
      .filter((part) => part && part.type === "text" && typeof part.text === "string")
      .map((part) => part.text)
      .join("\n");
  }
  if (Array.isArray(prompt)) {
    return prompt
      .filter((part) => part && part.type === "text" && typeof part.text === "string")
      .map((part) => part.text)
      .join("\n");
  }
  return "";
}

export function isHeartbeatOrNoise(prompt) {
  const text = extractPromptText(prompt);
  if (!text) return true;
  const trimmed = text.trim();
  if (trimmed.length === 0) return true;
  if (/^HEARTBEAT/i.test(trimmed)) return true;
  if (/^PING/i.test(trimmed)) return true;
  if (trimmed === "ping" || trimmed === "pong") return true;
  return false;
}

/**
 * Primary Plugin Entry for OpenClaw.
 * Supports both definePluginEntry wrapping and direct export.
 */
export const plugin = {
  id: "openclaw-amr",
  name: "AMR Memory",
  description: "Native AI Memory Runtime (AMR) 2.x adapter for OpenClaw with Zero-VRAM and Fail-Open guarantees",
  version: "2.0.0",

  register(api) {
    if (!api) {
      logger.warn("register called with null or invalid OpenClaw api");
      return;
    }

    if (api.logger) {
      logger.setHostLogger(api.logger);
    }

    const cfg = api.pluginConfig || {};
    const socketPath = cfg.socketPath;
    const deadlineMs = typeof cfg.deadlineMs === "number" ? cfg.deadlineMs : 80;
    const ingestDeadlineMs = typeof cfg.ingestDeadlineMs === "number"
      ? cfg.ingestDeadlineMs
      : (typeof cfg.ingestTimeoutMs === "number"
          ? cfg.ingestTimeoutMs
          : 1500);
    const scoreThreshold = typeof cfg.scoreThreshold === "number" ? cfg.scoreThreshold : 0.65;
    const limit = typeof cfg.limit === "number" ? cfg.limit : 3;
    const maxItemChars = typeof cfg.maxItemChars === "number" ? cfg.maxItemChars : 1000;
    const maxTotalChars = typeof cfg.maxTotalChars === "number" ? cfg.maxTotalChars : 4000;
    const configuredProjectId = cfg.projectId || null;

    const udsClient = new AmrUdsClient({
      socketPath,
      defaultDeadlineMs: deadlineMs,
    });

    const persistence = new SessionPersistenceManager({
      storageDir: cfg.persistenceDir || cfg.spoolDir || cfg.storageDir,
      udsClient,
      ingestDeadlineMs,
      autoReplayOnStartup: typeof cfg.autoReplayOnStartup === "boolean"
        ? cfg.autoReplayOnStartup
        : Boolean(cfg.persistenceDir || cfg.spoolDir || process.env.AMR_SESSION_SPOOL_DIR),
      logger,
    });

    plugin.persistence = persistence;
    plugin.getPersistenceManager = () => persistence;
    plugin.replayOfflineSessions = (opts) => persistence.replayPending(opts);
    if (api) {
      api.amrPersistence = persistence;
    }

    if (persistence.autoReplayOnStartup) {
      setImmediate(async () => {
        try {
          const replayRes = await persistence.replayPending();
          if (replayRes.replayed > 0) {
            logger.info(`Startup replay completed: ${replayRes.succeeded}/${replayRes.replayed} sessions sent to AMR`);
          }
        } catch (err) {
          logger.debug(`Startup replay skipped or failed: ${err?.message || err}`);
        }
      });
    }

    // 2.x control-method health check (DOC-AMR-09 section 2). Fire-and-forget:
    // a missing/broken adapter must never block registration or a host hook.
    setImmediate(() => {
      try {
        udsClient.ping(deadlineMs).then((res) => {
          if (res && res.status === "ok") {
            logger.debug("amr.ping ok");
          } else {
            logger.debug(`amr.ping returned unexpected response: ${JSON.stringify(res)}`);
          }
        }).catch((err) => {
          logger.debug(`amr.ping unavailable (fail-open): ${err?.message || err}`);
        });
      } catch (err) {
        logger.debug(`amr.ping dispatch failed (fail-open): ${err?.message || err}`);
      }
    });

    logger.info(`Initialized openclaw-amr plugin (AMR 2.x). Socket: ${udsClient.getSocketPath()}, Deadline: ${deadlineMs}ms, IngestDeadline: ${ingestDeadlineMs}ms, PersistenceDir: ${persistence.getStorageDir()}`);

    // =========================================================================
    // Hook 1: Input Prefetch & Injection (before_prompt_build)
    // =========================================================================
    const register = typeof api.on === "function" ? api.on.bind(api) : (typeof api.registerHook === "function" ? api.registerHook.bind(api) : null);
    if (register) {
      register("before_prompt_build", async (event, ctx) => {
        try {
          const userQuery = event?.currentUserMessage || event?.prompt || "";
          if (isHeartbeatOrNoise(userQuery)) {
            logger.debug("Skipping prefetch: heartbeat or empty prompt");
            return;
          }

          const projectId = ctx?.activeProjectKeys?.[0] || configuredProjectId || null;
          const searchParams = {
            query: userQuery,
            limit,
            score_threshold: scoreThreshold,
          };
          if (projectId) {
            searchParams.project_id = projectId;
          }

          logger.debug(`Triggering prefetch for query: "${userQuery.slice(0, 50)}..."`);
          const startTime = Date.now();

          let searchResult = null;
          try {
            searchResult = await udsClient.request("memory.search", searchParams, deadlineMs);
          } catch (udsErr) {
            // Fail-open: log debug/warning, do NOT throw or fail user prompt
            logger.debug(`AMR prefetch skipped or timed out (${Date.now() - startTime}ms): ${udsErr?.message || udsErr}`);
            return;
          }

          const items = searchResult?.results || searchResult?.memories || (Array.isArray(searchResult) ? searchResult : []);
          if (!items || items.length === 0) {
            logger.debug(`No relevant AMR memories found (${Date.now() - startTime}ms)`);
            return;
          }

          const contextXml = buildMemoryContext(items, {
            limit,
            maxItemChars,
            maxTotalChars,
          });

          if (!contextXml) {
            return;
          }

          logger.info(`Injected ${items.length} AMR memories into prompt (${Date.now() - startTime}ms)`);
          return {
            prependSystemContext: contextXml,
          };
        } catch (fatalErr) {
          // Fail-open guarantee: absolutely never disrupt user interaction
          logger.throttledError("before_prompt_build_err", `Error in before_prompt_build hook: ${fatalErr?.message || fatalErr}`);
        }
      });

      // =========================================================================
      // Hook 2: Session Ingestion (agent_end)
      // =========================================================================
      register("agent_end", async (event, ctx) => {
        const startTime = Date.now();
        let sessionId = null;
        try {
          sessionId = ctx?.sessionId || ctx?.sessionKey || event?.runId;
          if (!sessionId) {
            logger.debug("agent_end: skipped ingest, no sessionId found in context");
            return;
          }

          const messages = event?.messages || [];
          if (!Array.isArray(messages) || messages.length === 0) {
            return;
          }

          const projectId = ctx?.activeProjectKeys?.[0] || configuredProjectId || null;
          // AMR 2.x: identity is bound by the adapter socket, so the ingest
          // payload must not carry agent_id/source_agent.
          const ingestPayload = mapSessionToIngest({
            sessionId,
            projectId,
            messages,
          });

          if (!ingestPayload.messages || ingestPayload.messages.length === 0) {
            return;
          }

          // Persist-First: Save session to disk before network transmission
          let persistedRecord = null;
          try {
            persistedRecord = await persistence.persistSession(sessionId, ingestPayload, "pending");
          } catch (persistErr) {
            logger.warn(`Failed to persist session ${sessionId} offline: ${persistErr?.message || persistErr}`);
          }

          // A record that already failed permanently in this process is kept
          // for manual handling but must not be retried on every turn.
          if (persistedRecord?.permanent) {
            logger.warn(
              `Session ${sessionId} previously failed permanently; retained for manual handling and not retried`
            );
            return {
              ok: false,
              sessionId,
              error: persistedRecord.lastError || "Permanent failure retained for manual handling",
              persisted: true,
              permanent: true,
              skipped: true,
            };
          }

          try {
            logger.debug(`Triggering confirmed session.ingest for session ${sessionId} (${ingestPayload.messages.length} messages, timeout: ${ingestDeadlineMs}ms)`);
            const ingestResult = await udsClient.request("session.ingest", ingestPayload, ingestDeadlineMs);
            const elapsedMs = Date.now() - startTime;
            logger.info(`Session ${sessionId} successfully saved to AMR (${ingestPayload.messages.length} messages, ${elapsedMs}ms)`);

            if (persistedRecord) {
              try {
                await persistence.markSuccess(sessionId, ingestResult);
              } catch (markErr) {
                logger.warn(`Failed to mark session ${sessionId} success: ${markErr?.message || markErr}`);
              }
            }

            return {
              ok: true,
              sessionId,
              messageCount: ingestPayload.messages.length,
              result: ingestResult,
              persisted: !!persistedRecord,
            };
          } catch (sendErr) {
            const elapsedMs = Date.now() - startTime;
            const errDetail = sendErr?.code ? `[code: ${sendErr.code}] ${sendErr.message}` : sendErr?.message || String(sendErr);
            const logMsg = `Failed to save session ${sessionId || "unknown"} to AMR (${elapsedMs}ms): ${errDetail}`;
            logger.error(logMsg);

            const transient = isTransientError(sendErr);
            const permanent = isPermanentError(sendErr);

            // Transient failures stay queued for replay; permanent 2.x errors
            // (-32001/-32002/-32005/-32011/-32012, ...) are retained on disk
            // for manual handling but never retried automatically.
            if (persistedRecord) {
              try {
                if (transient) {
                  await persistence.markFailed(sessionId, sendErr);
                } else {
                  const retained = await persistence.markPermanent(sessionId, sendErr);
                  if (retained) {
                    logger.warn(
                      `Session ${sessionId} permanently failed (code: ${sendErr?.code ?? "unknown"}); retained for manual handling and will not be retried`
                    );
                  }
                }
              } catch (markErr) {
                logger.warn(`Failed to update failed status for session ${sessionId}: ${markErr?.message || markErr}`);
              }
            }

            return {
              ok: false,
              sessionId,
              error: errDetail,
              persisted: !!persistedRecord,
              transient,
              permanent,
            };
          }
        } catch (err) {
          const elapsedMs = Date.now() - startTime;
          const errDetail = err?.code ? `[code: ${err.code}] ${err?.message || err}` : err?.message || String(err);
          const logMsg = `Failed to process agent_end for session ${sessionId || "unknown"} (${elapsedMs}ms): ${errDetail}`;
          logger.error(logMsg);
          return {
            ok: false,
            sessionId,
            error: errDetail,
          };
        }
      });
    } else {
      logger.warn("OpenClaw plugin API did not expose on or registerHook");
    }
  },
};

export default plugin;

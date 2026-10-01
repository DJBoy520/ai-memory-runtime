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
 * 4. Compliant with DOC-AMR-10-OPENCODE-DSH-INTEGRATION:
 *    - Zero-VRAM: purely UDS client, 0MB host model/GPU overhead.
 *    - Hard deadline: default 80ms timeout on recall.
 *    - Fail-open: all exceptions caught, logging gracefully, never crashing DSH.
 */

import { AmrUdsClient } from "./lib/uds_client.js";
import { buildMemoryContext } from "./lib/context_builder.js";
import { mapSessionToIngest, extractContent } from "./lib/session_mapper.js";
import { logger } from "./lib/logger.js";

const DEFAULT_DEADLINE_MS = 80;
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
  const scoreThreshold = config.scoreThreshold || DEFAULT_SCORE_THRESHOLD;
  const limit = config.limit || DEFAULT_LIMIT;

  const udsClient = new AmrUdsClient({
    socketPath: config.socketPath,
    defaultDeadlineMs: deadlineMs,
  });

  // Keep track of turns we have already ingested or handled
  const ingestedTurns = new WeakSet();

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
          source_agent: "dsh",
        }, deadlineMs);
      } catch (udsErr) {
        logger.debug(`[dsh-amr] Recall skipped or timed out: ${udsErr.message}`);
        return decision;
      }

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

      const memoryMessage = createUserMsgFn ? createUserMsgFn({
        content: [{
          type: "text",
          text: contextXml,
        }],
        source: {
          kind: "plugin",
          plugin: name,
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
          kind: "plugin",
          plugin: name,
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
  const handleTurnEnd = (session, turnNumber) => {
    try {
      if (!session || typeof session.snapshotEvents !== "function") return;
      const turnKey = `${session.id}:${turnNumber}`;
      if (ingestedTurns.has(session)) return;

      const events = session.snapshotEvents();
      if (!Array.isArray(events) || events.length === 0) return;

      // Extract messages from this turn
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

      const ingestPayload = mapSessionToIngest({
        sessionId: session.id || `dsh-${Date.now()}`,
        messages: turnMessages,
        agentId: "dsh",
      });

      udsClient.notify("session.ingest", ingestPayload);
      logger.debug(`[dsh-amr] Dispatched session.ingest for session ${session.id}`);
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
      if (turn?.sessionId && Array.isArray(turn?.messages)) {
        const ingestPayload = mapSessionToIngest({
          sessionId: turn.sessionId,
          messages: turn.messages,
          agentId: "dsh",
        });
        udsClient.notify("session.ingest", ingestPayload);
      }
    } catch (_) {}
  });
}

export default { name, inject, apply };

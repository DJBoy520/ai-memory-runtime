/**
 * index.js
 * OpenCode Native AI Memory Runtime (AMR) Plugin.
 *
 * Zero-Friction (无感使用) Guarantees:
 * 1. Prefetch Hook: Intercepts `chat.message` / `experimental.chat.system.transform`.
 *    Automatically searches AMR via Unix Domain Socket (UDS) and injects relevant memories into context.
 * 2. Session Ingest Hook: Intercepts `session.idle` or session completion to ingest conversation history.
 * 3. Fail-Open Architecture: 100% boundary try/catch, zero crash risk, 0MB additional VRAM.
 */

import { AmrUdsClient } from "./lib/uds_client.js";
import { buildMemoryContext } from "./lib/context_builder.js";
import { logger } from "./lib/logger.js";

const DEFAULT_DEADLINE_MS = 80;
const DEFAULT_SCORE_THRESHOLD = 0.65;
const DEFAULT_LIMIT = 3;

export function isNoiseOrHeartbeat(text) {
  if (!text || typeof text !== "string") return true;
  const trimmed = text.trim();
  if (trimmed.length === 0) return true;
  if (/^HEARTBEAT/i.test(trimmed)) return true;
  if (/^PING/i.test(trimmed)) return true;
  return false;
}

export const id = "opencode-amr";

export const server = async ({ project, client, $, directory, worktree }) => {
  const udsClient = new AmrUdsClient({
    defaultDeadlineMs: DEFAULT_DEADLINE_MS,
  });

  logger.info(`[opencode-amr] Initialized for workspace: ${directory || "default"}`);

  return {
    /**
     * OpenCode lifecycle hook: chat.message
     * Triggered on user incoming message. Performs zero-friction prefetch and injects context part.
     */
    "chat.message": async (input, output) => {
      try {
        if (!output || !Array.isArray(output.parts)) return;

        const textParts = output.parts.filter((p) => p.type === "text" && !p.synthetic);
        if (textParts.length === 0) return;

        const userMessage = textParts.map((p) => p.text).join("\n");
        // Filter out noise, empty or internal synthetic messages
        if (!userMessage.trim() || isNoiseOrHeartbeat(userMessage)) return;
        if (userMessage.includes("# User Profile Analysis") || 
            (userMessage.includes("Analyze this conversation.") && userMessage.includes('type="skip"'))) {
          return;
        }

        const startTime = Date.now();
        let searchResult = null;
        try {
          searchResult = await udsClient.request("memory.search", {
            query: userMessage,
            limit: DEFAULT_LIMIT,
            score_threshold: DEFAULT_SCORE_THRESHOLD,
            source_agent: "opencode",
          }, DEFAULT_DEADLINE_MS);
        } catch (udsErr) {
          logger.debug(`[opencode-amr] Prefetch skipped/timed out: ${udsErr.message}`);
          return;
        }

        const items = searchResult?.results || searchResult?.memories || (Array.isArray(searchResult) ? searchResult : []);
        if (!items || items.length === 0) {
          logger.debug(`[opencode-amr] No relevant memories found (${Date.now() - startTime}ms)`);
          return;
        }

        const contextXml = buildMemoryContext(items, {
          limit: DEFAULT_LIMIT,
          maxItemChars: 800,
          maxTotalChars: 3000,
        });

        if (contextXml) {
          // Prepend as synthetic context part so user doesn't have to invoke anything manually
          output.parts.unshift({
            type: "text",
            text: contextXml,
            synthetic: true,
          });
          logger.info(`[opencode-amr] Injected ${items.length} AMR memories into context (${Date.now() - startTime}ms)`);
        }
      } catch (err) {
        // Fail-open guarantee: never disrupt editor/chat flow
        logger.error(`[opencode-amr] Error in chat.message hook: ${err.message}`);
      }
    },

    /**
     * Session completion/idle hook: async ingest into AMR SQLite & Vector DB
     */
    event: async (ev) => {
      try {
        if (!ev || (ev.type !== "session.idle" && ev.type !== "session.completed")) return;
        const sessionId = ev.sessionID || ev.sessionId || ev.id;
        if (!sessionId || !client?.session?.messages) return;

        const resp = await client.session.messages({ path: { id: sessionId } });
        const messages = resp?.data || [];
        if (!Array.isArray(messages) || messages.length === 0) return;

        const mappedMessages = [];
        for (const m of messages) {
          const role = m.info?.role || "user";
          const text = (m.parts || [])
            .filter((p) => p.type === "text" && !p.synthetic)
            .map((p) => p.text)
            .join("\n");
          if (text.trim()) {
            mappedMessages.push({
              message_id: m.info?.id || `msg-${Date.now()}`,
              role,
              content: text,
              created_at: m.info?.createdAt || new Date().toISOString(),
            });
          }
        }

        if (mappedMessages.length > 0) {
          udsClient.notify("session.ingest", {
            session_id: sessionId,
            agent_id: "opencode",
            messages: mappedMessages,
          });
          logger.debug(`[opencode-amr] Dispatched session.ingest for session ${sessionId}`);
        }
      } catch (err) {
        logger.debug(`[opencode-amr] Failed to ingest session: ${err.message}`);
      }
    }
  };
};

export default { id, server };

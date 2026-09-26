import { AmrUdsClient } from "./lib/uds_client.js";
import { buildMemoryContext } from "./lib/context_builder.js";

const DEFAULT_DEADLINE_MS = 80;
const DEFAULT_SCORE_THRESHOLD = 0.65;
const DEFAULT_LIMIT = 3;

export const name = "@deepseek-ai/dsh-amr";
export const inject = ["systemPrompt"];

export function apply(ctx, config = {}) {
  const udsClient = new AmrUdsClient({
    defaultDeadlineMs: DEFAULT_DEADLINE_MS,
  });

  // Inject dynamic prompt context before every LLM step
  ctx.systemPrompt.context({
    name: "amr:memories",
    order: 105, // High-priority runtime context
    text: async (context) => {
      try {
        const userPrompt = context?.userPrompt || context?.prompt || "";
        if (!userPrompt || userPrompt.trim().length === 0) return "";
        if (/^HEARTBEAT/i.test(userPrompt) || /^PING/i.test(userPrompt)) return "";

        const res = await udsClient.request("memory.search", {
          query: userPrompt,
          limit: DEFAULT_LIMIT,
          score_threshold: DEFAULT_SCORE_THRESHOLD,
          source_agent: "dsh",
        }, DEFAULT_DEADLINE_MS);

        const items = res?.results || res?.memories || (Array.isArray(res) ? res : []);
        if (!items || items.length === 0) return "";

        return buildMemoryContext(items, {
          limit: DEFAULT_LIMIT,
          maxItemChars: 800,
          maxTotalChars: 3000,
        });
      } catch (e) {
        // Fail-open: Never block or crash DSH
        return "";
      }
    }
  });

  // Hook turn completion for automatic memory ingestion
  ctx.on("turn/end", async (turn) => {
    try {
      const messages = turn?.messages || [];
      if (!Array.isArray(messages) || messages.length === 0) return;

      const mapped = [];
      let seq = 0;
      for (const m of messages) {
        if (!m.content) continue;
        seq++;
        mapped.push({
          message_id: m.id || `msg-${Date.now()}-${seq}`,
          role: m.role || "user",
          content: typeof m.content === "string" ? m.content : JSON.stringify(m.content),
          timestamp: Math.floor(Date.now() / 1000)
        });
      }

      if (mapped.length > 0) {
        udsClient.notify("session.ingest", {
          session_id: turn.sessionId || `dsh-${Date.now()}`,
          agent_id: "dsh",
          messages: mapped
        });
      }
    } catch (_) {}
  });
}

export default { name, inject, apply };

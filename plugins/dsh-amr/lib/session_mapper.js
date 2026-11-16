/**
 * lib/session_mapper.js
 * Session and Message mapper conforming to AMR session_store.py contract.
 *
 * AMR 2.x note: the local adapter owns the agent identity (bound to the
 * socket). The payload must NOT contain `agent_id` or `source_agent`; the
 * server overrides identity from the authenticated adapter. `agentId` is
 * accepted for 1.x source compatibility but is deliberately ignored.
 *
 * Contract:
 * {
 *   "session_id": "string",
 *   "project_id": "string" | null,
 *   "messages": [
 *     {
 *       "message_id": "string",
 *       "role": "user" | "assistant" | "system",
 *       "content": "string",
 *       "sequence": 1, // monotonically increasing from 1
 *       "timestamp": 1790400000 // unix timestamp in seconds
 *     }
 *   ]
 * }
 */

import crypto from "node:crypto";

/**
 * Normalizes role string to AMR contract allowed values.
 * @param {string} role
 * @returns {"user" | "assistant" | "system"}
 */
export function normalizeRole(role) {
  if (typeof role !== "string") return "user";
  const lower = role.toLowerCase();
  if (lower.includes("assistant") || lower.includes("bot") || lower.includes("model") || lower === "ai") {
    return "assistant";
  }
  if (lower.includes("system") || lower === "sys") {
    return "system";
  }
  return "user";
}

/**
 * Extracts plain text content from various DSH / OpenClaw / LLM message formats.
 * @param {any} raw
 * @returns {string}
 */
export function extractContent(raw) {
  if (!raw) return "";
  if (typeof raw === "string") return raw;
  if (typeof raw.text === "string") return raw.text;
  if (typeof raw.content === "string") return raw.content;
  if (Array.isArray(raw.content)) {
    // Multi-part content blocks [{ type: "text", text: "..." }]
    return raw.content
      .map((part) => {
        if (typeof part === "string") return part;
        if (part && typeof part.text === "string") return part.text;
        return "";
      })
      .filter(Boolean)
      .join("\n");
  }
  if (Array.isArray(raw.parts)) {
    // Multi-part parts array ["...", { type: "text", text: "..." }]
    return raw.parts
      .map((part) => {
        if (typeof part === "string") return part;
        if (part && typeof part.text === "string") return part.text;
        return "";
      })
      .filter(Boolean)
      .join("\n");
  }
  return String(raw);
}

/**
 * Maps raw messages and session metadata to AMR session.ingest payload.
 *
 * @param {object} params
 * @param {string} params.sessionId
 * @param {string} [params.projectId]
 * @param {Array<any>} params.messages
 * @param {string} [params.agentId] Ignored in 2.x (identity is adapter-bound).
 * @returns {object} Formatted session ingest payload conforming to session_store.py
 */
export function mapSessionToIngest(params) {
  const { sessionId, projectId, messages } = params || {};

  if (!sessionId || typeof sessionId !== "string") {
    throw new Error("AMR_MAPPER_INVALID_SESSION_ID: sessionId is required");
  }

  const nowSec = Math.floor(Date.now() / 1000);
  const mappedMessages = [];
  const rawList = Array.isArray(messages) ? messages : [];

  let seq = 1;
  for (const rawMsg of rawList) {
    if (!rawMsg) continue;

    const content = extractContent(rawMsg);
    if (!content || content.trim().length === 0) {
      continue;
    }

    const role = normalizeRole(rawMsg.role || rawMsg.senderKind);
    // Deterministic fallback id keeps spool keys stable across repeated
    // snapshots/turns when the host message has no explicit id.
    const fallbackHash = crypto
      .createHash("sha256")
      .update(`${sessionId}\u0000${seq}\u0000${role}\u0000${content}`)
      .digest("hex")
      .slice(0, 12);
    const msgId =
      rawMsg.message_id ||
      rawMsg.messageId ||
      rawMsg.id ||
      `${sessionId}_${seq}_${fallbackHash}`;

    let timestamp = nowSec;
    if (typeof rawMsg.timestamp === "number") {
      timestamp = rawMsg.timestamp > 1e11 ? Math.floor(rawMsg.timestamp / 1000) : Math.floor(rawMsg.timestamp);
    } else if (rawMsg.timestamp instanceof Date) {
      timestamp = Math.floor(rawMsg.timestamp.getTime() / 1000);
    }

    const mapped = {
      message_id: String(msgId),
      role,
      content: content.trim(),
      sequence: seq++,
      timestamp,
    };
    if (Number.isSafeInteger(rawMsg.source_revision) && rawMsg.source_revision >= 0) {
      mapped.source_revision = rawMsg.source_revision;
    }
    mappedMessages.push(mapped);
  }

  return {
    session_id: String(sessionId),
    project_id: projectId ? String(projectId) : null,
    messages: mappedMessages,
  };
}

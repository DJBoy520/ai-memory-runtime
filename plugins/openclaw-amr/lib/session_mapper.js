/**
 * lib/session_mapper.js
 * Session and Message mapper conforming to AMR session_store.py contract.
 *
 * Contract (AMR 2.x, DOC-AMR-09):
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
 *
 * The adapter binds the agent identity to the socket; the payload deliberately
 * carries no `agent_id` / `source_agent` field. Passing `agentId` is accepted
 * for backwards compatibility but ignored.
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
  if (lower.includes("assistant") || lower.includes("bot") || lower.includes("model")) {
    return "assistant";
  }
  if (lower.includes("system")) {
    return "system";
  }
  return "user";
}

/**
 * Extracts plain text content from various OpenClaw / LLM message formats.
 * @param {any} raw
 * @returns {string}
 */
export function extractContent(raw) {
  if (!raw) return "";
  if (typeof raw === "string") return raw;
  if (typeof raw.content === "string") return raw.content;
  if (typeof raw.text === "string") return raw.text;
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
  return String(raw);
}

/**
 * Maps raw messages and session metadata to AMR session.ingest payload.
 *
 * @param {object} params
 * @param {string} params.sessionId
 * @param {string} [params.projectId]
 * @param {Array<any>} params.messages
 * @param {string} [params.agentId] Accepted for 1.x compatibility but ignored
 * @returns {object} Formatted session ingest payload conforming to session_store.py
 */
export function mapSessionToIngest(params) {
  // `agentId` is intentionally not read: 2.x identity is adapter-bound.
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
    let msgId = rawMsg.message_id || rawMsg.messageId || rawMsg.id;
    if (!msgId) {
      // Deterministic content hash (never Date.now()/random): incorporating
      // sessionId, monotonic sequence, role and content keeps replay idempotent
      // while the sequence still distinguishes repeated identical messages.
      const digest = crypto
        .createHash("sha256")
        .update(`${sessionId}:${seq}:${role}:${content}`)
        .digest("hex")
        .slice(0, 8);
      msgId = `msg_${sessionId}_${seq}_${digest}`;
    }

    let timestamp = nowSec;
    if (typeof rawMsg.timestamp === "number") {
      timestamp = rawMsg.timestamp > 1e11 ? Math.floor(rawMsg.timestamp / 1000) : Math.floor(rawMsg.timestamp);
    } else if (rawMsg.timestamp instanceof Date) {
      timestamp = Math.floor(rawMsg.timestamp.getTime() / 1000);
    }

    mappedMessages.push({
      message_id: String(msgId),
      role,
      content: content.trim(),
      sequence: seq++,
      timestamp,
    });
  }

  return {
    session_id: String(sessionId),
    project_id: projectId ? String(projectId) : null,
    messages: mappedMessages,
  };
}

/**
 * Removes legacy/forged identity fields before an ingest payload is sent to a
 * 2.x adapter. The adapter strips them server-side too, but plugins must not
 * send them in the first place (DOC-AMR-09 section 2).
 *
 * @param {object} payload
 * @returns {object}
 */
export function stripIdentityFields(payload) {
  if (!payload || typeof payload !== "object") return payload;
  const { agent_id, source_agent, ...rest } = payload;
  return rest;
}

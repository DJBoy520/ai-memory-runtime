/**
 * lib/session_mapper.js
 * Session and Message mapper conforming to AMR session_store.py contract.
 *
 * Contract:
 * {
 *   "session_id": "string",
 *   "agent_id": "openclaw",
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
 * @param {string} [params.agentId="openclaw"]
 * @returns {object} Formatted session ingest payload conforming to session_store.py
 */
export function mapSessionToIngest(params) {
  const { sessionId, projectId, messages, agentId = "openclaw" } = params || {};

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
    const msgId =
      rawMsg.message_id ||
      rawMsg.messageId ||
      rawMsg.id ||
      `msg_${sessionId}_${seq}_${crypto.randomBytes(4).toString("hex")}`;

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
    agent_id: agentId,
    project_id: projectId ? String(projectId) : null,
    messages: mappedMessages,
  };
}

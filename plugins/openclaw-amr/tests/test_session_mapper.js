import test from "node:test";
import assert from "node:assert/strict";
import { normalizeRole, extractContent, mapSessionToIngest } from "../lib/session_mapper.js";

test("normalizeRole normalizes various inputs to user/assistant/system", () => {
  assert.equal(normalizeRole("user"), "user");
  assert.equal(normalizeRole("human"), "user");
  assert.equal(normalizeRole("assistant"), "assistant");
  assert.equal(normalizeRole("bot"), "assistant");
  assert.equal(normalizeRole("model"), "assistant");
  assert.equal(normalizeRole("system"), "system");
  assert.equal(normalizeRole(undefined), "user");
});

test("extractContent handles text, string, object, and content array", () => {
  assert.equal(extractContent("plain text"), "plain text");
  assert.equal(extractContent({ content: "nested text" }), "nested text");
  assert.equal(extractContent({ text: "text field" }), "text field");
  assert.equal(
    extractContent({
      content: [
        { type: "text", text: "part 1" },
        { type: "text", text: "part 2" },
      ],
    }),
    "part 1\npart 2"
  );
  assert.equal(extractContent(null), "");
});

test("mapSessionToIngest aligns with session_store.py contract and increments sequence", () => {
  const input = {
    sessionId: "sess_openclaw_1001",
    projectId: "project_red",
    messages: [
      { role: "user", content: "What is our architecture policy?", timestamp: 1790400000 },
      { role: "assistant", content: "We strictly follow DDD and Zero-VRAM.", timestamp: 1790400005 },
      { role: "user", content: "Got it, thanks!", timestamp: 1790400010 }
    ]
  };

  const payload = mapSessionToIngest(input);

  assert.equal(payload.session_id, "sess_openclaw_1001");
  assert.equal(Object.hasOwn(payload, "agent_id"), false);
  assert.equal(Object.hasOwn(payload, "source_agent"), false);
  assert.equal(payload.project_id, "project_red");
  assert.equal(payload.messages.length, 3);

  // Check monotonic sequence
  assert.equal(payload.messages[0].sequence, 1);
  assert.equal(payload.messages[1].sequence, 2);
  assert.equal(payload.messages[2].sequence, 3);

  // Check message_id and timestamp
  assert.ok(payload.messages[0].message_id);
  assert.equal(payload.messages[0].role, "user");
  assert.equal(payload.messages[0].timestamp, 1790400000);

  assert.equal(payload.messages[1].role, "assistant");
  assert.equal(payload.messages[1].content, "We strictly follow DDD and Zero-VRAM.");
});

test("mapSessionToIngest throws on missing sessionId", () => {
  assert.throws(() => {
    mapSessionToIngest({ messages: [] });
  }, /AMR_MAPPER_INVALID_SESSION_ID/);
});

test("mapSessionToIngest accepts but ignores agentId (2.x identity is adapter-bound)", () => {
  const payload = mapSessionToIngest({
    sessionId: "sess_identity",
    messages: [{ role: "user", content: "hello" }],
    agentId: "openclaw",
  });
  assert.equal(Object.hasOwn(payload, "agent_id"), false);
  assert.equal(Object.hasOwn(payload, "source_agent"), false);
});

test("missing message ids use a stable content hash, not Date.now/random", () => {
  const input = {
    sessionId: "sess_hash",
    messages: [
      { role: "user", content: "same text" },
      { role: "user", content: "same text" },
    ],
  };
  const first = mapSessionToIngest(input);
  const second = mapSessionToIngest(input);
  assert.deepEqual(
    first.messages.map((m) => m.message_id),
    second.messages.map((m) => m.message_id)
  );
  assert.notEqual(first.messages[0].message_id, first.messages[1].message_id);
  assert.match(first.messages[0].message_id, /^msg_sess_hash_1_[0-9a-f]{8}$/);
});

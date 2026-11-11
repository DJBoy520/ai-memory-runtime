/**
 * tests/test_agent_end_reliability.js
 *
 * Dedicated reliability test suite for openclaw-amr agent_end hook:
 * - Confirmed request-response over UDS with JSON-RPC 2.0
 * - Deadline timeout and socket destruction
 * - Server RPC error reporting
 * - Premature connection drop & offline socket resilience
 * - Clear error logging without unhandled rejections (Fail-Open)
 * - Message mapping, filtering, and configuration overrides
 */

import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { plugin } from "../index.js";
import { encodeFrame, FrameDecoder } from "../lib/uds_client.js";

/**
 * Helper to spin up an isolated temporary mock UDS server
 */
function createMockServer(socketPath, onFrame) {
  if (fs.existsSync(socketPath)) {
    try { fs.unlinkSync(socketPath); } catch (_) {}
  }
  const server = net.createServer((socket) => {
    const decoder = new FrameDecoder();
    socket.on("data", (chunk) => {
      try {
        const frames = decoder.feed(chunk);
        for (const frame of frames) {
          const req = JSON.parse(frame.toString("utf8"));
          onFrame(req, socket);
        }
      } catch (err) {
        socket.destroy(err);
      }
    });
  });

  return new Promise((resolve) => {
    server.listen(socketPath, () => resolve(server));
  });
}

function createTempSocketPath(prefix = "openclaw_amr_test") {
  return path.join(
    os.tmpdir(),
    `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2)}.sock`
  );
}

// ============================================================================
// Suite: agent_end Confirmed Request & Reliability
// ============================================================================

test("agent_end: sends confirmed request with numeric ID and resolves on server success", async () => {
  const sockPath = createTempSocketPath("confirm");
  let serverReceived = null;

  const server = await createMockServer(sockPath, (req, socket) => {
    serverReceived = req;
    const resp = {
      jsonrpc: "2.0",
      id: req.id,
      result: {
        session_id: req.params.session_id,
        inserted_count: req.params.messages.length,
      },
    };
    socket.write(encodeFrame(resp));
  });

  try {
    let agentEndHook = null;
    const mockApi = {
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 500,
      },
      registerHook(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    };

    plugin.register(mockApi);
    assert.ok(typeof agentEndHook === "function");

    const event = {
      runId: "run-abc-123",
      messages: [
        { role: "user", content: "Hello AMR" },
        { role: "assistant", content: "Hello! Memory saved." },
      ],
    };
    const ctx = {
      sessionId: "session-001",
      activeProjectKeys: ["proj-alpha"],
    };

    const outcome = await agentEndHook(event, ctx);

    assert.ok(outcome, "agent_end should return an outcome object");
    assert.equal(outcome.ok, true);
    assert.equal(outcome.sessionId, "session-001");
    assert.equal(outcome.messageCount, 2);
    assert.deepEqual(outcome.result, { session_id: "session-001", inserted_count: 2 });

    // Verify raw frame received by mock UDS server
    assert.ok(serverReceived);
    assert.equal(serverReceived.jsonrpc, "2.0");
    assert.equal(serverReceived.method, "session.ingest");
    assert.equal(typeof serverReceived.id, "number", "Must contain a valid JSON-RPC request ID");
    assert.equal(serverReceived.params.session_id, "session-001");
    assert.equal(serverReceived.params.project_id, "proj-alpha");
    assert.equal(serverReceived.params.agent_id, "openclaw");
    assert.equal(serverReceived.params.messages.length, 2);
    assert.equal(serverReceived.params.messages[0].sequence, 1);
    assert.equal(serverReceived.params.messages[1].sequence, 2);
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: times out after ingestDeadlineMs, destroys socket, and clearly records failure", async () => {
  const sockPath = createTempSocketPath("timeout");
  let socketWasDestroyed = false;

  // Server accepts connection but deliberately does NOT reply
  const server = await createMockServer(sockPath, (_req, socket) => {
    socket.on("close", () => {
      socketWasDestroyed = true;
    });
  });

  const loggedErrors = [];
  try {
    let agentEndHook = null;
    const mockApi = {
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 60, // Short deadline for testing
      },
      logger: {
        info: () => {},
        debug: () => {},
        warn: () => {},
        error: (msg) => loggedErrors.push(msg),
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    };

    plugin.register(mockApi);

    const startTime = Date.now();
    const outcome = await agentEndHook(
      { messages: [{ role: "user", content: "Will time out" }] },
      { sessionId: "sess-timeout-999" }
    );
    const duration = Date.now() - startTime;

    assert.ok(duration >= 50 && duration < 300, `Timed out in expected window, took ${duration}ms`);
    assert.equal(outcome.ok, false);
    assert.equal(outcome.sessionId, "sess-timeout-999");
    assert.match(outcome.error, /AMR_DEADLINE_EXCEEDED|ETIMEDOUT/);

    // Verify clear error recording
    assert.ok(loggedErrors.length >= 1, "Must log error on timeout");
    const errorLog = loggedErrors[0];
    assert.ok(errorLog.includes("sess-timeout-999"), "Log must mention session ID");
    assert.ok(errorLog.includes("ETIMEDOUT") || errorLog.includes("AMR_DEADLINE_EXCEEDED"));

    // Wait a brief moment to ensure client socket.destroy closed server socket
    await new Promise((r) => setTimeout(r, 50));
    assert.ok(socketWasDestroyed, "Client must destroy socket upon deadline exceeded");
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: catches JSON-RPC server error and logs error details clearly", async () => {
  const sockPath = createTempSocketPath("rpc_err");

  const server = await createMockServer(sockPath, (req, socket) => {
    const errorResp = {
      jsonrpc: "2.0",
      id: req.id,
      error: {
        code: -32603,
        message: "SQLite busy: database is locked",
        data: { table: "sessions" },
      },
    };
    socket.write(encodeFrame(errorResp));
  });

  const loggedErrors = [];
  try {
    let agentEndHook = null;
    const mockApi = {
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 300,
      },
      logger: {
        info: () => {},
        debug: () => {},
        warn: () => {},
        error: (msg) => loggedErrors.push(msg),
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    };

    plugin.register(mockApi);

    const outcome = await agentEndHook(
      { messages: [{ role: "user", content: "Trigger DB lock" }] },
      { sessionId: "sess-dblock" }
    );

    assert.equal(outcome.ok, false);
    assert.equal(outcome.sessionId, "sess-dblock");
    assert.match(outcome.error, /SQLite busy: database is locked/);
    assert.match(outcome.error, /-32603/);

    // Verify clear failure logging
    assert.ok(loggedErrors.length >= 1);
    const logStr = loggedErrors.find((l) => l.includes("sess-dblock"));
    assert.ok(logStr, "Failure log must be recorded");
    assert.ok(logStr.includes("-32603"));
    assert.ok(logStr.includes("database is locked"));
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: handles server error with id: null immediately without waiting for timeout", async () => {
  const sockPath = createTempSocketPath("null_id_err");

  const server = await createMockServer(sockPath, (_req, socket) => {
    const errorResp = {
      jsonrpc: "2.0",
      id: null,
      error: {
        code: -32700,
        message: "Parse error: invalid payload encoding",
      },
    };
    socket.write(encodeFrame(errorResp));
  });

  const loggedErrors = [];
  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 1000, // Should NOT wait for 1000ms
      },
      logger: {
        info: () => {},
        debug: () => {},
        warn: () => {},
        error: (msg) => loggedErrors.push(msg),
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const startTime = Date.now();
    const outcome = await agentEndHook(
      { messages: [{ role: "user", content: "Test null ID error" }] },
      { sessionId: "sess-null-id" }
    );
    const duration = Date.now() - startTime;

    assert.ok(duration < 400, `Should reject immediately on server error frame, took ${duration}ms`);
    assert.equal(outcome.ok, false);
    assert.match(outcome.error, /Parse error/);
    assert.match(outcome.error, /-32700/);
    assert.ok(loggedErrors.some((l) => l.includes("-32700") && l.includes("sess-null-id")));
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: handles offline socket (ENOENT) gracefully and logs failure", async () => {
  const offlineSock = path.join(os.tmpdir(), `non_existent_${Date.now()}_amr.sock`);
  const loggedErrors = [];

  let agentEndHook = null;
  plugin.register({
    pluginConfig: {
      socketPath: offlineSock,
      ingestDeadlineMs: 200,
    },
    logger: {
      info: () => {},
      debug: () => {},
      warn: () => {},
      error: (msg) => loggedErrors.push(msg),
    },
    on(name, fn) {
      if (name === "agent_end") agentEndHook = fn;
    },
  });

  const startTime = Date.now();
  const outcome = await agentEndHook(
    { messages: [{ role: "user", content: "Server is down" }] },
    { sessionId: "sess-offline-1" }
  );
  const duration = Date.now() - startTime;

  assert.ok(duration < 100, `Connection error should fail fast, took ${duration}ms`);
  assert.equal(outcome.ok, false);
  assert.equal(outcome.sessionId, "sess-offline-1");
  assert.ok(loggedErrors.length >= 1, "Must log failure when AMR socket is offline");
  assert.ok(loggedErrors[0].includes("sess-offline-1"));
  assert.ok(loggedErrors[0].includes("ENOENT") || loggedErrors[0].includes("connect"));
});

test("agent_end: handles premature socket closure (AMR_CONNECTION_CLOSED) and logs failure", async () => {
  const sockPath = createTempSocketPath("premature_close");

  const server = await createMockServer(sockPath, (_req, socket) => {
    // Abruptly destroy socket before replying
    socket.destroy();
  });

  const loggedErrors = [];
  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 500,
      },
      logger: {
        info: () => {},
        debug: () => {},
        warn: () => {},
        error: (msg) => loggedErrors.push(msg),
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const outcome = await agentEndHook(
      { messages: [{ role: "user", content: "Abrupt disconnect" }] },
      { sessionId: "sess-abrupt" }
    );

    assert.equal(outcome.ok, false);
    assert.equal(outcome.sessionId, "sess-abrupt");
    assert.match(outcome.error, /AMR_CONNECTION_CLOSED/);
    assert.ok(loggedErrors.some((l) => l.includes("AMR_CONNECTION_CLOSED") && l.includes("sess-abrupt")));
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: skips ingest when sessionId is missing without attempting UDS connection", async () => {
  const sockPath = createTempSocketPath("missing_sid");
  let serverHit = false;

  const server = await createMockServer(sockPath, (_req, socket) => {
    serverHit = true;
    socket.write(encodeFrame({ jsonrpc: "2.0", id: 1, result: { ok: true } }));
  });

  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: { socketPath: sockPath },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const res1 = await agentEndHook({ messages: [{ role: "user", content: "Hi" }] }, {});
    const res2 = await agentEndHook({ messages: [{ role: "user", content: "Hi" }] }, null);

    assert.equal(res1, undefined);
    assert.equal(res2, undefined);
    assert.equal(serverHit, false, "UDS server must not be contacted when sessionId is absent");
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: skips ingest when messages array is empty or missing", async () => {
  const sockPath = createTempSocketPath("empty_msgs");
  let serverHit = false;

  const server = await createMockServer(sockPath, (_req) => {
    serverHit = true;
  });

  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: { socketPath: sockPath },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    assert.equal(await agentEndHook({ messages: [] }, { sessionId: "s1" }), undefined);
    assert.equal(await agentEndHook({ messages: null }, { sessionId: "s1" }), undefined);
    assert.equal(await agentEndHook({}, { sessionId: "s1" }), undefined);
    assert.equal(serverHit, false);
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: skips ingest when all messages contain only whitespace or empty content", async () => {
  const sockPath = createTempSocketPath("whitespace_msgs");
  let serverHit = false;

  const server = await createMockServer(sockPath, (_req) => {
    serverHit = true;
  });

  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: { socketPath: sockPath },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const outcome = await agentEndHook(
      {
        messages: [
          { role: "user", content: "    " },
          { role: "assistant", content: "\n\t  " },
        ],
      },
      { sessionId: "s-blank" }
    );

    assert.equal(outcome, undefined);
    assert.equal(serverHit, false);
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: honors ingestDeadlineMs configuration override", async () => {
  const sockPath = createTempSocketPath("custom_deadline");

  // Server delays response by 120ms
  const server = await createMockServer(sockPath, (req, socket) => {
    setTimeout(() => {
      try {
        socket.write(encodeFrame({ jsonrpc: "2.0", id: req.id, result: { ok: true } }));
      } catch (_) {}
    }, 120);
  });

  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 50, // 50ms < 120ms delay, should trigger timeout
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const startTime = Date.now();
    const outcome = await agentEndHook(
      { messages: [{ role: "user", content: "Delayed response" }] },
      { sessionId: "s-delay" }
    );
    const duration = Date.now() - startTime;

    assert.ok(duration >= 40 && duration < 110, `Must time out around 50ms, took ${duration}ms`);
    assert.equal(outcome.ok, false);
    assert.match(outcome.error, /AMR_DEADLINE_EXCEEDED|ETIMEDOUT/);
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: honors ingestTimeoutMs alias if ingestDeadlineMs not set", async () => {
  const sockPath = createTempSocketPath("alias_deadline");

  const server = await createMockServer(sockPath, (req, socket) => {
    setTimeout(() => {
      try {
        socket.write(encodeFrame({ jsonrpc: "2.0", id: req.id, result: { ok: true } }));
      } catch (_) {}
    }, 140);
  });

  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: {
        socketPath: sockPath,
        ingestTimeoutMs: 60, // Alias
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const startTime = Date.now();
    const outcome = await agentEndHook(
      { messages: [{ role: "user", content: "Alias test" }] },
      { sessionId: "s-alias" }
    );
    const duration = Date.now() - startTime;

    assert.ok(duration >= 50 && duration < 130, `Must time out around 60ms, took ${duration}ms`);
    assert.equal(outcome.ok, false);
    assert.match(outcome.error, /AMR_DEADLINE_EXCEEDED|ETIMEDOUT/);
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: handles concurrent session ingest requests without state collision", async () => {
  const sockPath = createTempSocketPath("concurrent");
  const processedSessions = [];

  const server = await createMockServer(sockPath, (req, socket) => {
    processedSessions.push(req.params.session_id);
    const resp = {
      jsonrpc: "2.0",
      id: req.id,
      result: { ok: true, session: req.params.session_id },
    };
    socket.write(encodeFrame(resp));
  });

  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 800,
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const sessions = Array.from({ length: 8 }, (_, i) => `concurrent-sess-${i}`);
    const results = await Promise.all(
      sessions.map((sid) =>
        agentEndHook(
          { messages: [{ role: "user", content: `Message for ${sid}` }] },
          { sessionId: sid }
        )
      )
    );

    assert.equal(results.length, 8);
    for (let i = 0; i < 8; i++) {
      assert.equal(results[i].ok, true);
      assert.equal(results[i].sessionId, sessions[i]);
      assert.equal(results[i].result.session, sessions[i]);
    }
    assert.equal(processedSessions.length, 8);
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: absolute Fail-Open guarantee on null event or malformed inputs", async () => {
  let agentEndHook = null;
  plugin.register({
    pluginConfig: {},
    on(name, fn) {
      if (name === "agent_end") agentEndHook = fn;
    },
  });

  // None of these should throw
  assert.equal(await agentEndHook(null, null), undefined);
  assert.equal(await agentEndHook(undefined, undefined), undefined);
  assert.equal(await agentEndHook({ messages: "not-an-array" }, { sessionId: "s1" }), undefined);
});

test("agent_end: handles malformed JSON frame from server gracefully and logs error", async () => {
  const sockPath = createTempSocketPath("corrupt_frame");

  const server = await createMockServer(sockPath, (_req, socket) => {
    // Send 4-byte header followed by invalid non-JSON data
    const badPayload = Buffer.from("NOT_VALID_JSON{abc", "utf8");
    const header = Buffer.alloc(4);
    header.writeUInt32BE(badPayload.length, 0);
    socket.write(Buffer.concat([header, badPayload]));
  });

  const loggedErrors = [];
  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 400,
      },
      logger: {
        info: () => {},
        debug: () => {},
        warn: () => {},
        error: (msg) => loggedErrors.push(msg),
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    const outcome = await agentEndHook(
      { messages: [{ role: "user", content: "Malformed payload test" }] },
      { sessionId: "sess-corrupt" }
    );

    assert.equal(outcome.ok, false);
    assert.equal(outcome.sessionId, "sess-corrupt");
    assert.ok(loggedErrors.length >= 1, "Must log failure on malformed frame");
    assert.ok(loggedErrors[0].includes("sess-corrupt"));
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

test("agent_end: delegates logging to hostLogger on both success and failure", async () => {
  const sockPath = createTempSocketPath("host_logger");
  let replyWithError = false;

  const server = await createMockServer(sockPath, (req, socket) => {
    if (replyWithError) {
      socket.write(
        encodeFrame({
          jsonrpc: "2.0",
          id: req.id,
          error: { code: -32000, message: "Storage quota reached" },
        })
      );
    } else {
      socket.write(
        encodeFrame({
          jsonrpc: "2.0",
          id: req.id,
          result: { ok: true },
        })
      );
    }
  });

  const hostLogs = { info: [], error: [] };
  const mockHostLogger = {
    info: (m) => hostLogs.info.push(m),
    error: (m) => hostLogs.error.push(m),
    warn: () => {},
    debug: () => {},
  };

  try {
    let agentEndHook = null;
    plugin.register({
      pluginConfig: {
        socketPath: sockPath,
        ingestDeadlineMs: 300,
      },
      logger: mockHostLogger,
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    });

    // 1. Success case
    replyWithError = false;
    const okOutcome = await agentEndHook(
      { messages: [{ role: "user", content: "Good turn" }] },
      { sessionId: "sess-logger-good" }
    );
    assert.equal(okOutcome.ok, true);
    assert.ok(
      hostLogs.info.some((msg) => msg.includes("sess-logger-good") && msg.includes("successfully saved")),
      "Host logger must receive success log"
    );

    // 2. Failure case
    replyWithError = true;
    const failOutcome = await agentEndHook(
      { messages: [{ role: "user", content: "Failing turn" }] },
      { sessionId: "sess-logger-bad" }
    );
    assert.equal(failOutcome.ok, false);
    assert.ok(
      hostLogs.error.some(
        (msg) => msg.includes("sess-logger-bad") && msg.includes("Storage quota reached")
      ),
      "Host logger must receive error log"
    );
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
  }
});

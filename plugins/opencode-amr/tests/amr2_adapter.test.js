/**
 * AMR 2.x adapter integration tests for the OpenCode plugin.
 *
 * A fake local adapter UDS (4-byte length-prefixed JSON-RPC 2.0) is started in
 * a temporary directory. No fixed socket paths, no production connections.
 */

import test, { after } from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import plugin, { buildIngestMessages } from "../index.js";
import {
  AmrUdsClient,
  FrameDecoder,
  encodeFrame,
  isTransientError,
  AMR_ERROR_CODES,
} from "../lib/uds_client.js";
import { IngestSpool, computeRecordKey } from "../lib/ingest_spool.js";
import { mapSessionToIngest } from "../lib/session_mapper.js";
import { logger } from "../lib/logger.js";

// Keep the adapter/spool assertions independent of the developer environment.
const ENV_KEYS = [
  "AMR2_SOCKET_PATH",
  "AMR_SOCKET_PATH",
  "XDG_RUNTIME_DIR",
  "AMR_OPENCODE_SPOOL_DIR",
];
const savedEnv = new Map(ENV_KEYS.map((key) => [key, process.env[key]]));
for (const key of ENV_KEYS) delete process.env[key];
after(() => {
  for (const [key, value] of savedEnv) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

// Keep test output readable; assertions still inspect real state.
logger.setLevel("error");

class FakeAdapter {
  constructor(dir) {
    this.dir = dir;
    this.socketPath = path.join(dir, "amr2-opencode.sock");
    this.server = net.createServer((socket) => this._onConnection(socket));
    this.sockets = new Set();
    this.requests = [];
    this.errorForMethod = new Map();
    this.silentMethods = new Set();
    this.handlers = {
      "amr.ping": () => ({ status: "ok" }),
      "memory.search": () => ({
        results: [{ content: "AMR memory", score: 0.9, scope: "global" }],
      }),
      "session.ingest": (params) => ({
        status: "success",
        total_received: Array.isArray(params?.messages) ? params.messages.length : 0,
      }),
    };
  }

  async start() {
    await new Promise((resolve, reject) => {
      const onError = (err) => {
        this.server.removeListener("listening", onListening);
        reject(err);
      };
      const onListening = () => {
        this.server.removeListener("error", onError);
        resolve();
      };
      this.server.once("error", onError);
      this.server.once("listening", onListening);
      this.server.listen(this.socketPath);
    });
    return this;
  }

  _onConnection(socket) {
    this.sockets.add(socket);
    socket.on("close", () => this.sockets.delete(socket));
    socket.on("error", () => {});

    const decoder = new FrameDecoder();
    socket.on("data", (chunk) => {
      let frames;
      try {
        frames = decoder.feed(chunk);
      } catch (_) {
        socket.destroy();
        return;
      }
      for (const frame of frames) {
        let req;
        try {
          req = JSON.parse(frame.toString("utf8"));
        } catch (_) {
          continue;
        }
        this._handle(socket, req);
      }
    });
  }

  _respond(socket, obj) {
    if (!socket.destroyed) {
      socket.write(encodeFrame(obj));
    }
  }

  _handle(socket, req) {
    this.requests.push(req);

    if (this.silentMethods.has(req.method)) {
      return;
    }

    const injected = this.errorForMethod.get(req.method);
    if (injected) {
      this._respond(socket, {
        jsonrpc: "2.0",
        id: req.id,
        error: { code: injected.code, message: injected.message || "injected error" },
      });
      return;
    }

    const handler = this.handlers[req.method];
    if (!handler) {
      this._respond(socket, {
        jsonrpc: "2.0",
        id: req.id,
        error: { code: AMR_ERROR_CODES.METHOD_NOT_FOUND, message: "method not found" },
      });
      return;
    }

    try {
      const result = handler(req.params || {}, req);
      this._respond(socket, { jsonrpc: "2.0", id: req.id, result });
    } catch (err) {
      this._respond(socket, {
        jsonrpc: "2.0",
        id: req.id,
        error: { code: AMR_ERROR_CODES.INTERNAL, message: err.message },
      });
    }
  }

  setError(method, code, message) {
    this.errorForMethod.set(method, { code, message });
  }

  clearError(method) {
    this.errorForMethod.delete(method);
  }

  count(method) {
    return this.requests.filter((r) => r.method === method).length;
  }

  async close() {
    for (const socket of this.sockets) {
      socket.destroy();
    }
    this.sockets.clear();
    await new Promise((resolve) => this.server.close(() => resolve()));
  }
}

async function withAdapter(fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-opencode-test-"));
  const adapter = new FakeAdapter(dir);
  await adapter.start();
  try {
    return await fn(adapter, dir);
  } finally {
    await adapter.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

async function waitFor(predicate, message, timeoutMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail(`timed out waiting for: ${message}`);
}

function mockClientFor(messages) {
  const calls = [];
  return {
    calls,
    session: {
      async messages({ path: requestPath }) {
        calls.push(requestPath);
        return { data: messages };
      },
    },
  };
}

async function startPlugin(adapter, options = {}) {
  const client = options.client || mockClientFor([]);
  return plugin.server(
    {
      project: {},
      client,
      $: {},
      directory: adapter.dir,
      worktree: adapter.dir,
    },
    {
      socketPath: adapter.socketPath,
      persistenceDir: path.join(adapter.dir, "spool"),
      deadlineMs: 500,
      ingestDeadlineMs: 500,
      ...options,
    }
  );
}

function sessionMessages(sessionId) {
  return [
    {
      info: { id: `${sessionId}-u1`, role: "user", createdAt: 1790000000000 },
      parts: [{ type: "text", text: "Hello AMR" }],
    },
    {
      info: { id: `${sessionId}-a1`, role: "assistant", createdAt: 1790000001000 },
      parts: [{ type: "text", text: "Hello human" }],
    },
  ];
}

test("a. amr.ping success and failure both fail open", async () => {
  await withAdapter(async (adapter) => {
    const client = new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 300 });
    const pong = await client.ping(300);
    assert.deepEqual(pong, { status: "ok" });
    assert.equal(adapter.requests[0].method, "amr.ping");

    const instance = await startPlugin(adapter);
    assert.equal(typeof instance.event, "function");
    await waitFor(() => adapter.count("amr.ping") >= 2, "plugin startup amr.ping");

    // Missing adapter: transient error, plugin initialization and hooks fail open.
    const missingDir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-opencode-missing-"));
    const missingSocket = path.join(missingDir, "absent.sock");
    try {
      const missingClient = new AmrUdsClient({ socketPath: missingSocket, defaultDeadlineMs: 80 });
      await assert.rejects(
        () => missingClient.ping(80),
        (err) => isTransientError(err)
      );

      const missingPlugin = await plugin.server(
        { project: {}, client: mockClientFor([]), $: {}, directory: missingDir, worktree: missingDir },
        {
          socketPath: missingSocket,
          persistenceDir: path.join(missingDir, "spool"),
          deadlineMs: 40,
          ingestDeadlineMs: 40,
        }
      );
      const output = { parts: [{ type: "text", text: "ping adapter" }] };
      await missingPlugin["chat.message"]({}, output);
      assert.equal(output.parts.length, 1);
      // Let the startup ping rejection settle; no unhandled rejection may escape.
      await new Promise((resolve) => setTimeout(resolve, 80));
    } finally {
      fs.rmSync(missingDir, { recursive: true, force: true });
    }
  });
});

test("b. memory.search does not spoof source_agent/agent_id", async () => {
  await withAdapter(async (adapter) => {
    const instance = await startPlugin(adapter);
    const output = { parts: [{ type: "text", text: "How do I configure the AMR 2.x adapter?" }] };

    await instance["chat.message"]({}, output);

    const search = adapter.requests.find((r) => r.method === "memory.search");
    assert.ok(search, "memory.search should be sent to the adapter");
    assert.equal(search.params.query, "How do I configure the AMR 2.x adapter?");
    assert.equal(search.params.limit, 3);
    assert.equal(Object.hasOwn(search.params, "source_agent"), false);
    assert.equal(Object.hasOwn(search.params, "agent_id"), false);
    assert.equal(search.params.source_agent, undefined);
    assert.equal(search.params.agent_id, undefined);

    // Existing injection behaviour is preserved: a synthetic context part is
    // prepended and contains the adapter-provided memory.
    assert.equal(output.parts[0].synthetic, true);
    assert.ok(output.parts[0].text.includes("<amr_recalled_context>"));
    assert.ok(output.parts[0].text.includes("AMR memory"));
  });
});

test("c. session.ingest is a request with an id, no identity fields, and clears the spool on ack", async () => {
  await withAdapter(async (adapter) => {
    const persistenceDir = path.join(adapter.dir, "spool");
    const client = mockClientFor(sessionMessages("sess-c"));
    const instance = await startPlugin(adapter, { client, persistenceDir });

    instance.event({ type: "session.idle", sessionID: "sess-c" });

    await waitFor(() => adapter.count("session.ingest") >= 1, "session.ingest request");
    const ingest = adapter.requests.find((r) => r.method === "session.ingest");
    assert.ok(ingest.id !== undefined && ingest.id !== null, "request must carry a JSON-RPC id");
    assert.equal(ingest.params.session_id, "sess-c");
    assert.equal(Object.hasOwn(ingest.params, "agent_id"), false);
    assert.equal(Object.hasOwn(ingest.params, "source_agent"), false);
    assert.equal(ingest.params.messages.length, 2);
    assert.deepEqual(
      ingest.params.messages.map((m) => m.message_id),
      ["sess-c-u1", "sess-c-a1"]
    );

    await waitFor(
      () => new IngestSpool({ persistenceDir }).pending().length === 0,
      "spool to be empty after ack"
    );
  });
});

test("d. -32010 keeps the record spooled; replay after recovery delivers and clears it", async () => {
  await withAdapter(async (adapter) => {
    const persistenceDir = path.join(adapter.dir, "spool");
    const client = mockClientFor(sessionMessages("sess-d"));
    const instance = await startPlugin(adapter, { client, persistenceDir });

    // Let the plugin's startup ping/replay settle on the empty spool before
    // injecting the failure, so the attempt count is deterministic.
    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");
    await new Promise((resolve) => setTimeout(resolve, 30));

    adapter.setError("session.ingest", AMR_ERROR_CODES.ADAPTER_UNAVAILABLE, "adapter unavailable");
    instance.event({ type: "session.idle", sessionID: "sess-d" });

    await waitFor(() => adapter.count("session.ingest") >= 1, "initial session.ingest attempt");
    const spool = new IngestSpool({ persistenceDir });
    await waitFor(() => spool.pending().length === 1, "record retained in spool");

    const [record] = spool.pending();
    assert.equal(record.key, computeRecordKey(record.payload));
    assert.notEqual(record.permanent, true);
    assert.equal(adapter.count("session.ingest"), 1);

    adapter.clearError("session.ingest");
    const replayClient = new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 500 });
    const summary = await spool.replay(replayClient, { deadlineMs: 500 });

    assert.equal(summary.delivered, 1);
    assert.equal(summary.transient, 0);
    assert.equal(spool.pending().length, 0);
    assert.equal(adapter.count("session.ingest"), 2);
  });
});

test("e. -32012 is attempted once, marked permanent, and retained", async () => {
  await withAdapter(async (adapter) => {
    const persistenceDir = path.join(adapter.dir, "spool");
    const client = mockClientFor(sessionMessages("sess-e"));
    const instance = await startPlugin(adapter, { client, persistenceDir });

    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");
    await new Promise((resolve) => setTimeout(resolve, 30));

    adapter.setError(
      "session.ingest",
      AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED,
      "remote auth failed"
    );
    instance.event({ type: "session.idle", sessionID: "sess-e" });

    await waitFor(() => adapter.count("session.ingest") === 1, "first session.ingest attempt");
    const spool = new IngestSpool({ persistenceDir });
    await waitFor(() => spool.pending()[0]?.permanent === true, "record marked permanent");
    assert.equal(spool.pending().length, 1);
    assert.equal(spool.pending()[0].lastError.code, AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED);

    // A repeated idle callback for the same snapshot must not resend.
    instance.event({ type: "session.idle", sessionID: "sess-e" });
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(adapter.count("session.ingest"), 1);

    // Replay skips permanent records even after the adapter recovers.
    adapter.clearError("session.ingest");
    const replayClient = new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 500 });
    const summary = await spool.replay(replayClient, { deadlineMs: 500 });
    assert.equal(summary.attempted, 0);
    assert.equal(summary.skippedRecords, 1);
    assert.equal(adapter.count("session.ingest"), 1);
    assert.equal(spool.pending().length, 1);
  });
});

test("f. consecutive requests and error responses do not crash", async () => {
  await withAdapter(async (adapter) => {
    const client = new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 500 });

    const first = await client.request("memory.search", { query: "one" }, 500);
    assert.ok(Array.isArray(first.results));

    adapter.setError("memory.search", AMR_ERROR_CODES.ADAPTER_PROTOCOL_ERROR, "bad frame");
    await assert.rejects(
      () => client.request("memory.search", { query: "two" }, 500),
      (err) => {
        assert.equal(err.code, AMR_ERROR_CODES.ADAPTER_PROTOCOL_ERROR);
        assert.equal(isTransientError(err), false);
        return true;
      }
    );
    adapter.clearError("memory.search");

    const third = await client.request("memory.search", { query: "three" }, 500);
    assert.ok(Array.isArray(third.results));

    // The plugin hook must also fail open on adapter business errors.
    const instance = await startPlugin(adapter);
    adapter.setError("memory.search", AMR_ERROR_CODES.INTERNAL, "boom");
    const failedOutput = { parts: [{ type: "text", text: "error path" }] };
    await assert.doesNotReject(() => instance["chat.message"]({}, failedOutput));
    assert.equal(failedOutput.parts.length, 1);

    adapter.clearError("memory.search");
    const recoveredOutput = { parts: [{ type: "text", text: "recovered path" }] };
    await instance["chat.message"]({}, recoveredOutput);
    assert.equal(recoveredOutput.parts[0].synthetic, true);

    assert.equal(adapter.count("memory.search"), 5);
  });
});

test("g. messages without message_id get distinct, stable spool keys", () => {
  const sessionId = "sess-g";
  const first = buildIngestMessages(sessionId, [
    { info: { role: "user" }, parts: [{ type: "text", text: "first message" }] },
  ]);
  const second = buildIngestMessages(sessionId, [
    { info: { role: "user" }, parts: [{ type: "text", text: "second message" }] },
  ]);
  const firstAgain = buildIngestMessages(sessionId, [
    { info: { role: "user" }, parts: [{ type: "text", text: "first message" }] },
  ]);

  assert.match(first[0].message_id, /^[0-9a-f]{24}$/);
  assert.equal(first[0].message_id, firstAgain[0].message_id, "fallback id must be stable");
  assert.notEqual(first[0].message_id, second[0].message_id);

  const firstKey = computeRecordKey(mapSessionToIngest({ sessionId, messages: first }));
  const secondKey = computeRecordKey(mapSessionToIngest({ sessionId, messages: second }));
  assert.notEqual(firstKey, secondKey);

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-opencode-spool-key-"));
  try {
    const spool = new IngestSpool({ persistenceDir: dir });
    spool.enqueue(mapSessionToIngest({ sessionId, messages: first }));
    spool.enqueue(mapSessionToIngest({ sessionId, messages: second }));
    assert.equal(spool.pending().length, 2);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

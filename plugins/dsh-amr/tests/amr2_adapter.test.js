/**
 * AMR 2.x adapter integration tests.
 *
 * A fake local adapter UDS (4-byte length-prefixed JSON-RPC 2.0) is started in
 * a temporary directory. No fixed socket paths, no production connections.
 */

import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { createRequire } from "node:module";

import {
  AmrUdsClient,
  resolveSocketPath,
  FrameDecoder,
  encodeFrame,
  isTransientError,
  AMR_ERROR_CODES,
} from "../lib/uds_client.js";

test("explicit socket config takes precedence over legacy environment", () => {
  const saved = process.env.AMR2_SOCKET_PATH;
  process.env.AMR2_SOCKET_PATH = "/environment.sock";
  try { assert.equal(resolveSocketPath("/configured.sock"), "/configured.sock"); }
  finally { if (saved === undefined) delete process.env.AMR2_SOCKET_PATH; else process.env.AMR2_SOCKET_PATH = saved; }
});
import { IngestSpool, computeRecordKey } from "../lib/ingest_spool.js";
import { SQLiteSpool } from "../lib/sqlite_spool.js";
import { mapSessionToIngest } from "../lib/session_mapper.js";
import { apply } from "../index.js";

class FakeAdapter {
  constructor(dir) {
    this.dir = dir;
    this.socketPath = path.join(dir, "amr2-dsh.sock");
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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-adapter-test-"));
  const adapter = new FakeAdapter(dir);
  await adapter.start();
  try {
    return await fn(adapter, dir);
  } finally {
    await adapter.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

async function waitFor(predicate, message, timeoutMs = 1500) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail(`timed out waiting for: ${message}`);
}

function installPlugin(config, services = {}) {
  const handlers = new Map();
  const ctx = {
    sessions: services.sessions ?? { flush: async () => true },
    on(event, handler) {
      handlers.set(event, handler);
    },
  };
  apply(ctx, { legacyUnversionedSnapshots: true, ...config });
  return handlers;
}

function turnSession(sessionId) {
  return {
    id: sessionId,
    snapshotEvents() {
      return [
        {
          type: "user/message",
          seq: 1,
          time: 1790000000,
          data: { id: `${sessionId}-u1`, content: [{ type: "text", text: "Hello AMR" }] },
        },
        {
          type: "assistant/message",
          seq: 2,
          time: 1790000001,
          data: {
            message: {
              id: `${sessionId}-a1`,
              content: [{ type: "text", text: "Hello human" }],
            },
          },
        },
      ];
    },
  };
}

function readSpool(persistenceDir) {
  return new IngestSpool({ persistenceDir });
}

test("a. amr.ping succeeds and failures fail open without crashing the host", async () => {
  await withAdapter(async (adapter, dir) => {
    const client = new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 300 });
    const pong = await client.ping(300);
    assert.deepEqual(pong, { status: "ok" });
    assert.equal(adapter.requests[0].method, "amr.ping");

    // Timeout is transient and must not crash the host either.
    adapter.silentMethods.add("memory.search");
    await assert.rejects(
      () => client.request("memory.search", { query: "slow" }, 40),
      (err) => {
        assert.equal(err.code, "ETIMEDOUT");
        assert.equal(isTransientError(err), true);
        return true;
      }
    );
    adapter.silentMethods.delete("memory.search");

    const missingDir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-missing-"));
    const missingSocket = path.join(missingDir, "absent.sock");
    try {
      const missingClient = new AmrUdsClient({ socketPath: missingSocket, defaultDeadlineMs: 80 });
      await assert.rejects(
        () => missingClient.ping(80),
        (err) => isTransientError(err)
      );

      // Plugin apply() must never throw or produce an unhandled rejection.
      assert.doesNotThrow(() => {
        installPlugin({
          socketPath: missingSocket,
          persistenceDir: path.join(missingDir, "spool"),
          deadlineMs: 40,
          ingestDeadlineMs: 40,
        });
      });
      await new Promise((resolve) => setTimeout(resolve, 80));
    } finally {
      fs.rmSync(missingDir, { recursive: true, force: true });
    }
  });
});

test("versioned archival waits for durable session flush and retries failure", async () => {
  await withAdapter(async (adapter, dir) => {
    let allow = false, flushes = 0;
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool"), replayInitialDelayMs: 20 }, {
      sessions: { flush: async () => { flushes++; return allow; } },
    });
    const session = {
      id: "durability", seq: 2,
      snapshotEvents: () => [{ type: "user/message", seq: 0, time: 1000, data: { id: "u", content: "hello" } }, { type: "turn/end", seq: 1 }],
      deriveMessages: () => [{ id: "u", role: "user", source: { kind: "user" }, content: "hello" }],
    };
    handlers.get("session/event")(session, { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => flushes >= 1, "initial flush");
    assert.equal(adapter.count("session.ingest"), 0);
    allow = true;
    await waitFor(() => adapter.count("session.ingest") === 1, "archive after durable flush");
    assert.ok(flushes >= 2);
    handlers.get("dispose")();
  });
});

test("a stalled durability barrier cannot block the archive queue forever", async () => {
  await withAdapter(async (adapter, dir) => {
    let flushes = 0;
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool"), replayInitialDelayMs: 10 }, {
      sessions: { flush: async () => { flushes++; return false; } },
    });
    const session = {
      id: "no-durability", seq: 2,
      snapshotEvents: () => [{ type: "user/message", seq: 0, time: 1000, data: { id: "u", content: "hello" } }, { type: "turn/end", seq: 1 }],
      deriveMessages: () => [{ id: "u", role: "user", source: { kind: "user" }, content: "hello" }],
    };
    handlers.get("session/event")(session, { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "archive after bounded flush retries");
    assert.equal(flushes, 3);
    handlers.get("dispose")();
  });
});

test("detached session archives without waiting on an impossible barrier", async () => {
  await withAdapter(async (adapter, dir) => {
    let flushes = 0;
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool"), replayInitialDelayMs: 10 }, {
      sessions: { flush: async () => { flushes++; return true; }, get: () => undefined },
    });
    const session = {
      id: "detached", seq: 2,
      snapshotEvents: () => [{ type: "user/message", seq: 0, time: 1000, data: { id: "u", content: "hello" } }, { type: "turn/end", seq: 1 }],
      deriveMessages: () => [{ id: "u", role: "user", source: { kind: "user" }, content: "hello" }],
    };
    handlers.get("session/event")(session, { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "archive of detached session");
    assert.equal(flushes, 0);
    handlers.get("dispose")();
  });
});

test("b. memory.search does not spoof source_agent/agent_id", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({
      socketPath: adapter.socketPath,
      persistenceDir: path.join(dir, "spool"),
      deadlineMs: 500,
      ingestDeadlineMs: 500,
    });

    const hook = handlers.get("agent/pre-step");
    assert.equal(typeof hook, "function");

    const decision = await hook(
      {
        agent: {},
        messages: [{ content: "How do I configure the AMR 2.x adapter?" }],
        turn: 1,
        step: 1,
        signal: new AbortController().signal,
      },
      async () => ({
        kind: "enter",
        messages: [
          {
            id: "m1",
            content: [{ type: "text", text: "How do I configure the AMR 2.x adapter?" }],
            source: { kind: "user" },
          },
        ],
      })
    );

    const search = adapter.requests.find((r) => r.method === "memory.search");
    assert.ok(search, "memory.search should be sent to the adapter");
    assert.equal(search.params.query, "How do I configure the AMR 2.x adapter?");
    assert.equal(Object.hasOwn(search.params, "source_agent"), false);
    assert.equal(Object.hasOwn(search.params, "agent_id"), false);
    assert.equal(search.params.source_agent, undefined);
    assert.equal(search.params.agent_id, undefined);
    assert.ok(Array.isArray(decision.messages));
  });
});

test("c. session.ingest is a request with an id, no identity fields, and clears the spool on ack", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "spool");
    const handlers = installPlugin({
      socketPath: adapter.socketPath,
      persistenceDir,
      deadlineMs: 500,
      ingestDeadlineMs: 500,
    });

    handlers.get("session/event")(turnSession("sess-c"), {
      type: "turn/end",
      data: { turn: 1 },
    });

    await waitFor(
      () => adapter.count("session.ingest") >= 1,
      "session.ingest request"
    );

    const ingest = adapter.requests.find((r) => r.method === "session.ingest");
    assert.ok(ingest.id !== undefined && ingest.id !== null, "request must carry a JSON-RPC id");
    assert.equal(ingest.params.session_id, "sess-c");
    assert.equal(Object.hasOwn(ingest.params, "agent_id"), false);
    assert.equal(Object.hasOwn(ingest.params, "source_agent"), false);
    assert.equal(ingest.params.messages.length, 2);

    await waitFor(
      () => readSpool(persistenceDir).pending().length === 0,
      "spool to be empty after ack"
    );
    assert.equal(readSpool(persistenceDir).pending().length, 0);
  });
});

test("d. -32010 keeps the record spooled; replay after recovery delivers and clears it", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "spool");
    const handlers = installPlugin({
      socketPath: adapter.socketPath,
      persistenceDir,
      deadlineMs: 500,
      ingestDeadlineMs: 500,
    });

    // Let the plugin's startup ping/replay settle on the empty spool before
    // injecting the failure, so the attempt count is deterministic.
    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");
    await new Promise((resolve) => setTimeout(resolve, 20));

    adapter.setError("session.ingest", AMR_ERROR_CODES.ADAPTER_UNAVAILABLE, "adapter unavailable");

    handlers.get("session/event")(turnSession("sess-d"), {
      type: "turn/end",
      data: { turn: 1 },
    });

    await waitFor(
      () => adapter.count("session.ingest") >= 1,
      "initial session.ingest attempt"
    );
    await waitFor(
      () => readSpool(persistenceDir).pending().length === 1,
      "record retained in spool"
    );

    const spool = readSpool(persistenceDir);
    const [record] = spool.pending();
    assert.equal(record.key, computeRecordKey(record.payload));
    assert.notEqual(record.permanent, true);

    adapter.clearError("session.ingest");
    const client = new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 500 });
    const summary = await spool.replay(client, { deadlineMs: 500 });

    assert.equal(summary.delivered, 1);
    assert.equal(summary.transient, 0);
    assert.equal(spool.pending().length, 0);
    assert.equal(adapter.count("session.ingest"), 2);
  });
});

test("e. -32012 is not retried forever and the record is retained for manual handling", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "spool");
    const spool = new IngestSpool({ persistenceDir });
    const payload = mapSessionToIngest({
      sessionId: "sess-e",
      messages: [
        { message_id: "sess-e-u1", role: "user", content: "hello" },
        { message_id: "sess-e-a1", role: "assistant", content: "hi" },
      ],
    });
    spool.enqueue(payload);
    assert.equal(spool.pending().length, 1);

    adapter.setError(
      "session.ingest",
      AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED,
      "remote auth failed"
    );
    const client = new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 500 });

    let permanentCallbacks = 0;
    const first = await spool.replay(client, {
      deadlineMs: 500,
      onPermanent: () => {
        permanentCallbacks++;
      },
    });
    assert.equal(first.attempted, 1);
    assert.equal(first.permanent, 1);
    assert.equal(permanentCallbacks, 1);
    assert.equal(adapter.count("session.ingest"), 1);

    const retained = spool.pending();
    assert.equal(retained.length, 1, "record must be retained for manual handling");
    assert.equal(retained[0].permanent, true);
    assert.equal(retained[0].lastError.code, AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED);

    // Even after the adapter recovers, a permanently failed record is not retried.
    adapter.clearError("session.ingest");
    const second = await spool.replay(client, { deadlineMs: 500 });
    assert.equal(second.attempted, 0);
    assert.equal(second.skippedRecords, 1);
    assert.equal(adapter.count("session.ingest"), 1);
    assert.equal(spool.pending().length, 1);
  });
});

test("e2. plugin marks -32012 permanent once and never retries the same record", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "spool");
    const handlers = installPlugin({
      socketPath: adapter.socketPath,
      persistenceDir,
      deadlineMs: 500,
      ingestDeadlineMs: 500,
    });

    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");
    await new Promise((resolve) => setTimeout(resolve, 20));

    adapter.setError(
      "session.ingest",
      AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED,
      "remote auth failed"
    );
    handlers.get("session/event")(turnSession("sess-e2"), {
      type: "turn/end",
      data: { turn: 1 },
    });

    await waitFor(() => adapter.count("session.ingest") === 1, "first ingest attempt");
    await waitFor(
      () => readSpool(persistenceDir).pending()[0]?.permanent === true,
      "record marked permanent"
    );

    // A later turn with the same message ids maps to the same record key and
    // must not be sent again.
    handlers.get("session/event")(turnSession("sess-e2"), {
      type: "turn/end",
      data: { turn: 2 },
    });
    await new Promise((resolve) => setTimeout(resolve, 30));

    assert.equal(adapter.count("session.ingest"), 1);
    const retained = readSpool(persistenceDir).pending();
    assert.equal(retained.length, 1);
    assert.equal(retained[0].permanent, true);
  });
});

test("spool enforces capacity and dedupes record keys", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-spool-test-"));
  const silentLogger = { debug() {}, info() {}, warn() {}, error() {} };
  try {
    const spool = new IngestSpool({ persistenceDir: dir, maxRecords: 5, logger: silentLogger });
    const makePayload = (i, content = `message ${i}`) => mapSessionToIngest({
      sessionId: "cap",
      messages: [{ message_id: `m-${i}`, role: "user", content }],
    });

    for (let i = 0; i < 5; i++) {
      spool.enqueue(makePayload(i));
    }
    assert.equal(spool.enqueue(makePayload(5)), null);
    const keys = spool.pending().map((r) => r.key);
    assert.ok(spool.pending().length <= 5);
    assert.equal(keys.includes(computeRecordKey(makePayload(0))), true, "oldest record must be retained");
    assert.equal(keys.includes(computeRecordKey(makePayload(5))), false, "new record must be rejected");

    // Re-enqueueing the same payload is idempotent.
    const before = spool.pending();
    const duplicate = before[0];
    assert.equal(spool.enqueue(duplicate.payload), duplicate.key);
    assert.equal(spool.pending().length, before.length);

    // Byte cap: oldest records are trimmed until the file fits.
    const byteDir = path.join(dir, "bytes");
    const byteSpool = new IngestSpool({
      persistenceDir: byteDir,
      maxBytes: 1200,
      logger: silentLogger,
    });
    for (let i = 0; i < 8; i++) {
      byteSpool.enqueue(makePayload(i, "X".repeat(300)));
    }
    const size = fs.statSync(byteSpool.filePath).size;
    assert.ok(size <= 1200, `spool file should be <= 1200 bytes, got ${size}`);
    assert.ok(byteSpool.pending().length < 8);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("spool keys distinguish delimiter collisions and changed middle messages", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr-spool-key-collision-"));
  try {
    const spool = new IngestSpool({ persistenceDir: dir });
    const message = (id, content = id) => ({ message_id: id, role: "user", content });
    const a = { session_id: "a:b", messages: [message("c"), message("d")] };
    const b = { session_id: "a", messages: [message("b:c"), message("d")] };
    const c = { session_id: "a:b", messages: [message("c"), message("middle", "new"), message("d")] };
    const d = { session_id: "a:b", messages: [message("c"), message("middle", "old"), message("d")] };
    for (const item of [a, b, c, d]) assert.ok(spool.enqueue(item));
    assert.equal(spool.pending().length, 4);
    spool.remove(computeRecordKey(c));
    assert.equal(spool.pending().length, 3);
    assert.ok(spool.get(computeRecordKey(d)));
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("failed persistence prevents delivery", async () => {
  await withAdapter(async (adapter, dir) => {
    const invalidDir = path.join(dir, "not-a-directory");
    fs.writeFileSync(invalidDir, "file");
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: invalidDir });
    await waitFor(() => adapter.count("amr.ping") > 0, "startup ping");
    handlers.get("session/event")(turnSession("no-persist"), { type: "turn/end", data: { turn: 1 } });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(adapter.count("session.ingest"), 0);
    fs.rmSync(invalidDir);
    fs.mkdirSync(invalidDir);
    handlers.get("session/event")(turnSession("no-persist"), { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "retry same turn after storage recovery");
    handlers.get("dispose")();
  });
});

test("plugin supports SQLite spool and acknowledges the stored generation", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "sqlite-spool");
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir, spoolBackend: "sqlite" });
    await waitFor(() => adapter.count("amr.ping") > 0, "startup ping");
    handlers.get("session/event")(turnSession("sqlite"), { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "sqlite delivery");
    await waitFor(() => {
      const spool = new SQLiteSpool({ persistenceDir });
      try { return spool.pending().length === 0; } finally { spool.close(); }
    }, "sqlite ack removal");
    handlers.get("dispose")();
  });
});

test("turn archive excludes injected memory and runtime snapshots", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool") });
    const session = { id: "snapshot-test", snapshotEvents: () => [
      { type: "user/message", seq: 1, data: { id: "recall", source: { kind: "plugin:@deepseek-ai/dsh-amr", form: "snapshot" }, content: "recalled memory" } },
      { type: "user/message", seq: 2, data: { id: "user", source: { kind: "user" }, content: "real prompt" } },
      { type: "user/message", seq: 3, data: { id: "runtime", source: { kind: "runtime-context", form: "snapshot" }, content: "runtime context" } },
      { type: "assistant/message", seq: 4, data: { message: { id: "assistant", content: "answer" } } },
    ] };
    handlers.get("session/event")(session, { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "archive");
    const req = adapter.requests.find((r) => r.method === "session.ingest");
    assert.deepEqual(req.params.messages.map((m) => m.content), ["real prompt", "answer"]);
    handlers.get("dispose")();
  });
});

test("DSH projection snapshot carries one stable session watermark", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool") });
    const session = {
      id: "projected", seq: 4,
      snapshotEvents: () => [
        { type: "user/message", seq: 0, time: 1000, data: { id: "u", content: "before" } },
        { type: "assistant/message", seq: 1, time: 1001, data: { message: { id: "a", content: "answer" } } },
        { type: "surface/replace", seq: 2 }, { type: "turn/end", seq: 3 },
      ],
      deriveMessages: () => [
        { id: "u", role: "user", source: { kind: "user" }, content: "projected edit" },
        { id: "a", role: "assistant", content: "answer" },
      ],
    };
    handlers.get("session/event")(session, { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "projected archive");
    const messages = adapter.requests.find((r) => r.method === "session.ingest").params.messages;
    assert.deepEqual(messages.map((m) => m.content), ["projected edit", "answer"]);
    assert.deepEqual(messages.map((m) => m.source_revision), [3, 3]);
    handlers.get("dispose")();
  });
});

test("legacy turn callback is disabled by default, preserving the versioned snapshot", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool") });
    const messages = [{ id: "u", role: "user", content: "hello", source: { kind: "user" } }];
    handlers.get("turn/end")({ sessionId: "order", turn: 1, messages });
    const session = {
      id: "order", seq: 2,
      snapshotEvents: () => [{ type: "user/message", seq: 0, time: 1000, data: messages[0] }, { type: "turn/end", seq: 1 }],
      deriveMessages: () => messages,
    };
    handlers.get("session/event")(session, { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "versioned archive following ignored legacy event");
    const requests = adapter.requests.filter((r) => r.method === "session.ingest");
    assert.equal(requests[0].params.messages[0].source_revision, 1);
    handlers.get("dispose")();
  });
});

test("unversioned session snapshots are disabled by default", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool"), legacyUnversionedSnapshots: false });
    handlers.get("session/event")(turnSession("no-watermark"), { type: "turn/end", data: { turn: 1 } });
    await new Promise((resolve) => setTimeout(resolve, 80));
    assert.equal(adapter.count("session.ingest"), 0);
    handlers.get("dispose")();
  });
});

test("newer versioned snapshot of the same turn is not suppressed", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir: path.join(dir, "spool") });
    let revision = 1, content = "old";
    const session = {
      id: "same-turn", get seq() { return revision + 1; },
      snapshotEvents: () => [{ type: "user/message", seq: 0, time: 1000, data: { id: "u", content } }, { type: "turn/end", seq: revision }],
      deriveMessages: () => [{ id: "u", role: "user", source: { kind: "user" }, content }],
    };
    handlers.get("session/event")(session, { type: "turn/end", data: {} });
    revision = 3; content = "new";
    handlers.get("session/event")(session, { type: "turn/end", data: {} });
    await waitFor(() => adapter.count("session.ingest") === 2, "newer snapshot");
    const requests = adapter.requests.filter((r) => r.method === "session.ingest");
    assert.deepEqual(requests.map((r) => r.params.messages[0].source_revision), [1, 3]);
    handlers.get("dispose")();
  });
});

test("a deferred legacy turn is replaced by the versioned session snapshot", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "busy-versioned");
    const seed = new SQLiteSpool({ persistenceDir }); seed.close();
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite");
    const lock = new DatabaseSync(path.join(persistenceDir, "ingest-spool.sqlite3"));
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir, spoolBackend: "sqlite", replayInitialDelayMs: 20, legacyTurnEnd: true });
    await waitFor(() => adapter.count("amr.ping") > 0, "startup ping");
    lock.exec("BEGIN EXCLUSIVE");
    const messages = [{ id: "u", role: "user", content: "hello", source: { kind: "user" } }];
    handlers.get("turn/end")({ sessionId: "order-busy", turn: 1, messages });
    handlers.get("session/event")({ id: "order-busy", seq: 2,
      snapshotEvents: () => [{ type: "user/message", seq: 0, time: 1000, data: messages[0] }, { type: "turn/end", seq: 1 }],
      deriveMessages: () => messages }, { type: "turn/end", data: { turn: 1 } });
    lock.exec("ROLLBACK"); lock.close();
    await waitFor(() => adapter.count("session.ingest") >= 1, "deferred archive");
    const req = adapter.requests.find((r) => r.method === "session.ingest");
    assert.equal(req.params.messages[0].source_revision, 1);
    await waitFor(() => { const s = new SQLiteSpool({ persistenceDir }); try { return s.pending().length === 0; } finally { s.close(); } }, "deferred ack");
    handlers.get("dispose")();
  });
});

test("SQLite startup lock can recover without restarting plugin", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "sqlite-locked");
    const seed = new SQLiteSpool({ persistenceDir });
    seed.close();
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite");
    const lock = new DatabaseSync(path.join(persistenceDir, "ingest-spool.sqlite3"));
    lock.exec("BEGIN EXCLUSIVE");
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir, spoolBackend: "sqlite", replayInitialDelayMs: 20 });
    await new Promise((resolve) => setTimeout(resolve, 100));
    lock.exec("ROLLBACK");
    lock.close();
    handlers.get("session/event")(turnSession("recover-startup"), { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => adapter.count("session.ingest") === 1, "delivery after startup lock released");
    handlers.get("dispose")();
  });
});

test("turn received while SQLite is busy is retried after the lock clears", async () => {
  await withAdapter(async (adapter, dir) => {
    const persistenceDir = path.join(dir, "sqlite-busy-turn");
    const seed = new SQLiteSpool({ persistenceDir });
    seed.close();
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite");
    const lock = new DatabaseSync(path.join(persistenceDir, "ingest-spool.sqlite3"));
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir, spoolBackend: "sqlite", replayInitialDelayMs: 20, replayMaxDelayMs: 50 });
    await waitFor(() => adapter.count("amr.ping") > 0, "startup ping");
    lock.exec("BEGIN EXCLUSIVE");
    handlers.get("session/event")(turnSession("busy-turn"), { type: "turn/end", data: { turn: 1 } });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(adapter.count("session.ingest"), 0);
    lock.exec("ROLLBACK");
    lock.close();
    await waitFor(() => adapter.count("session.ingest") === 1, "automatic retry after database lock");
    await waitFor(() => {
      const spool = new SQLiteSpool({ persistenceDir });
      try { return spool.pending().length === 0; } finally { spool.close(); }
    }, "busy turn acknowledgment");
    handlers.get("dispose")();
  });
});

test("plugin automatically replays pending records after recovery", async () => {
  await withAdapter(async (adapter, dir) => {
    adapter.setError("session.ingest", AMR_ERROR_CODES.ADAPTER_UNAVAILABLE);
    const persistenceDir = path.join(dir, "spool");
    const handlers = installPlugin({ socketPath: adapter.socketPath, persistenceDir, replayInitialDelayMs: 30, replayMaxDelayMs: 60 });
    await waitFor(() => adapter.count("amr.ping") > 0, "startup ping");
    handlers.get("session/event")(turnSession("auto-replay"), { type: "turn/end", data: { turn: 1 } });
    await waitFor(() => readSpool(persistenceDir).pending().length === 1, "pending record");
    await waitFor(() => adapter.count("session.ingest") >= 1, "first failed delivery");
    await new Promise((resolve) => setTimeout(resolve, 20));
    adapter.clearError("session.ingest");
    await waitFor(() => readSpool(persistenceDir).pending().length === 0, "automatic replay");
    assert.ok(adapter.count("session.ingest") >= 2);
    handlers.get("dispose")();
  });
});

test("replay checks in-flight keys again before each delivery", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-replay-race-"));
  try {
    const spool = new IngestSpool({ persistenceDir: dir });
    const payload = (id) => ({ session_id: "race", messages: [{ message_id: id, content: id }] });
    spool.enqueue(payload("first"));
    const secondKey = spool.enqueue(payload("second"));
    const inFlight = new Set();
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const sent = [];
    const replay = spool.replay({ async request(_method, item) {
      sent.push(item.messages[0].message_id);
      if (sent.length === 1) await gate;
    } }, { skipKeys: inFlight });
    await waitFor(() => sent.length === 1, "first replay request");
    inFlight.add(secondKey);
    release();
    await replay;
    assert.deepEqual(sent, ["first"]);
    assert.ok(spool.get(secondKey));
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("failed spool rewrite keeps in-memory records consistent with disk", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-rewrite-fail-"));
  const originalRename = fs.renameSync;
  try {
    const spool = new IngestSpool({ persistenceDir: dir });
    const key = spool.enqueue({ session_id: "s", messages: [{ message_id: "m", content: "x" }] });
    fs.renameSync = () => { throw Object.assign(new Error("disk error"), { code: "EIO" }); };
    assert.equal(spool.markPermanent(key, { code: -32012 }), null);
    assert.notEqual(spool.get(key).permanent, true);
    assert.equal(spool.remove(key), false);
    assert.ok(spool.get(key));
  } finally {
    fs.renameSync = originalRename;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("corrupt spool is preserved and blocks mutation and replay", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-corrupt-spool-"));
  try {
    const spool = new IngestSpool({ persistenceDir: dir });
    const good = JSON.stringify({ key: "s:m:m", payload: { session_id: "s", messages: [{ message_id: "m" }] } });
    const original = `${good}\n{broken json\n`;
    fs.writeFileSync(spool.filePath, original);
    assert.equal(spool.enqueue({ session_id: "new", messages: [] }), null);
    assert.equal(spool.remove("s:m:m"), false);
    let sent = 0;
    await spool.replay({ request: async () => { sent++; } });
    assert.equal(sent, 0);
    assert.equal(fs.readFileSync(spool.filePath, "utf8"), original);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("legacy key collision in existing spool fails closed and preserves both lines", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-legacy-collision-"));
  try {
    const file = path.join(dir, "ingest-spool.jsonl");
    const make = (session, first) => ({ key: "a:b:c:d", payload: { session_id: session, messages: [{ message_id: first, content: first }, { message_id: "d", content: "d" }] } });
    const source = [make("a:b", "c"), make("a", "b:c")].map(JSON.stringify).join("\n") + "\n";
    fs.writeFileSync(file, source);
    const spool = new IngestSpool({ persistenceDir: dir });
    assert.equal(spool.pending().length, 0);
    assert.equal(spool.enqueue({ session_id: "new", messages: [] }), null);
    assert.equal(fs.readFileSync(file, "utf8"), source);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("legacy permanent spool record remains permanent after key upgrade", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-permanent-legacy-"));
  try {
    const payload = { session_id: "s", messages: [{ message_id: "m", role: "user", content: "x" }] };
    fs.writeFileSync(path.join(dir, "ingest-spool.jsonl"), JSON.stringify({ key: "s:m:m", payload, permanent: true }) + "\n");
    const spool = new IngestSpool({ persistenceDir: dir });
    const key = spool.enqueue(payload);
    assert.equal(key, "s:m:m");
    assert.equal(spool.get(key).permanent, true);
    assert.equal(spool.pending().length, 1);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("duplicate legacy and v2 records with conflicting status fail closed", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-status-conflict-"));
  try {
    const p = { session_id: "s", messages: [{ message_id: "m", role: "user", content: "x" }] };
    const source = [
      { key: "s:m:m", payload: p, permanent: true },
      { key: computeRecordKey(p), payload: p },
    ].map(JSON.stringify).join("\n") + "\n";
    const file = path.join(dir, "ingest-spool.jsonl");
    fs.writeFileSync(file, source);
    const spool = new IngestSpool({ persistenceDir: dir });
    assert.equal(spool.pending().length, 0);
    assert.equal(spool.enqueue(p), null);
    assert.equal(fs.readFileSync(file, "utf8"), source);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("f. sequential requests and error responses do not crash the client", async () => {
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
    assert.equal(adapter.count("memory.search"), 3);

    // The fake adapter also supports multiple back-to-back frames on one
    // connection; verify it (and the client decoder contract) stay healthy.
    const socket = net.createConnection({ path: adapter.socketPath });
    try {
      await new Promise((resolve, reject) => {
        socket.once("connect", resolve);
        socket.once("error", reject);
      });
      socket.write(Buffer.concat([
        encodeFrame({
          jsonrpc: "2.0",
          id: 900,
          method: "memory.search",
          params: { query: "pipelined-1" },
        }),
        encodeFrame({
          jsonrpc: "2.0",
          id: 901,
          method: "amr.ping",
          params: {},
        }),
      ]));
      const decoder = new FrameDecoder();
      const ids = [];
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("pipelined response timeout")), 1000);
        socket.on("data", (chunk) => {
          for (const payload of decoder.feed(chunk)) {
            ids.push(JSON.parse(payload.toString("utf8")).id);
          }
          if (ids.length === 2) {
            clearTimeout(timer);
            resolve();
          }
        });
        socket.on("error", (err) => {
          clearTimeout(timer);
          reject(err);
        });
      });
      assert.deepEqual(ids, [900, 901]);
    } finally {
      socket.destroy();
    }
  });
});

test("g. distinct turn/end events without turn numbers are not collapsed", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({
      socketPath: adapter.socketPath,
      persistenceDir: path.join(dir, "spool"),
      deadlineMs: 500,
      ingestDeadlineMs: 500,
      legacyTurnEnd: true,
    });
    const hook = handlers.get("turn/end");
    assert.equal(typeof hook, "function");

    const turn = (messageId, text) => ({
      sessionId: "sess-g",
      messages: [
        { message_id: messageId, role: "user", content: [{ type: "text", text }] },
      ],
    });

    hook(turn("g-1", "one"));
    hook(turn("g-1", "one")); // 完全重复的回调应去重
    hook(turn("g-2", "two"));

    await waitFor(
      () => adapter.count("session.ingest") >= 2,
      "two distinct turn ingests"
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(adapter.count("session.ingest"), 2);
  });
});

test("spool keys are content-derived when message ids are missing", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-spool-key-"));
  try {
    const spool = new IngestSpool({ persistenceDir: dir });
    const payload = (text) => ({
      session_id: "no-ids",
      messages: [{ role: "user", content: text }],
    });
    const firstKey = computeRecordKey(payload("first"));
    const secondKey = computeRecordKey(payload("second"));
    assert.notEqual(firstKey, secondKey);
    spool.enqueue(payload("first"));
    spool.enqueue(payload("second"));
    assert.equal(spool.pending().length, 2);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("replay retries an unreadable spool after access is restored", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-spool-retry-read-"));
  const original = fs.readFileSync;
  try {
    const seed = new IngestSpool({ persistenceDir: dir });
    seed.enqueue({ session_id: "s", messages: [{ message_id: "m", role: "user", content: "x" }] });
    const spool = new IngestSpool({ persistenceDir: dir });
    fs.readFileSync = function (file, ...args) {
      if (String(file) === spool.filePath) throw Object.assign(new Error("injected"), { code: "EACCES" });
      return original.call(this, file, ...args);
    };
    const first = await spool.replay({ request: async () => ({}) });
    assert.ok(first.transient > 0);
    fs.readFileSync = original;
    const second = await spool.replay({ request: async () => ({}) });
    assert.equal(second.delivered, 1);
    assert.equal(spool.pending().length, 0);
  } finally { fs.readFileSync = original; fs.rmSync(dir, { recursive: true, force: true }); }
});

test("spool refuses to enqueue or rewrite when the existing file cannot be read", () => {
  if (typeof process.getuid === "function" && process.getuid() === 0) return;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-spool-load-"));
  try {
    const seed = new IngestSpool({ persistenceDir: dir });
    seed.enqueue({
      session_id: "load",
      messages: [{ message_id: "m-1", role: "user", content: "seed" }],
    });
    fs.chmodSync(seed.filePath, 0o000);

    const broken = new IngestSpool({ persistenceDir: dir });
    const key = broken.enqueue({
      session_id: "load",
      messages: [{ message_id: "m-2", role: "user", content: "new" }],
    });
    assert.equal(key, null);
    assert.equal(broken.pending().length, 0);

    fs.chmodSync(seed.filePath, 0o600);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("h. session/event and turn/end for the same turn archive only once", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({
      socketPath: adapter.socketPath,
      persistenceDir: path.join(dir, "spool"),
      deadlineMs: 500,
      ingestDeadlineMs: 500,
    });
    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");

    const messages = [
      { message_id: "h-1", role: "user", content: [{ type: "text", text: "one" }] },
      { message_id: "h-2", role: "assistant", content: [{ type: "text", text: "two" }] },
    ];
    const session = {
      id: "sess-h",
      snapshotEvents() {
        return [
          { type: "user/message", seq: 1, data: { id: "h-1", content: [{ type: "text", text: "one" }] } },
          {
            type: "assistant/message",
            seq: 2,
            data: { message: { id: "h-2", content: [{ type: "text", text: "two" }] } },
          },
        ];
      },
    };

    handlers.get("session/event")(session, { type: "turn/end", data: { turn: 1 } });
    handlers.get("turn/end")({ sessionId: "sess-h", turn: 1, messages });

    await waitFor(() => adapter.count("session.ingest") >= 1, "first ingest");
    await new Promise((resolve) => setTimeout(resolve, 120));
    assert.equal(adapter.count("session.ingest"), 1);
  });
});

test("i. a session without a stable id is not archived under a fake id", async () => {
  await withAdapter(async (adapter, dir) => {
    const handlers = installPlugin({
      socketPath: adapter.socketPath,
      persistenceDir: path.join(dir, "spool"),
      deadlineMs: 500,
      ingestDeadlineMs: 500,
    });
    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");

    handlers.get("session/event")(
      {
        snapshotEvents() {
          return [
            { type: "user/message", seq: 1, data: { id: "no-id-1", content: [{ type: "text", text: "x" }] } },
          ];
        },
      },
      { type: "turn/end", data: { turn: 1 } }
    );
    await new Promise((resolve) => setTimeout(resolve, 120));
    assert.equal(adapter.count("session.ingest"), 0);
  });
});

test("encodeFrame rejects payloads beyond the 4MB protocol ceiling", () => {
  assert.throws(
    () => encodeFrame({ text: "x".repeat(4 * 1024 * 1024 + 1) }),
    /AMR_FRAME_SIZE_INVALID/
  );
  assert.doesNotThrow(() => encodeFrame({ text: "ok" }));
});

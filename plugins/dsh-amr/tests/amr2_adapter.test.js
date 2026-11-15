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

import {
  AmrUdsClient,
  FrameDecoder,
  encodeFrame,
  isTransientError,
  AMR_ERROR_CODES,
} from "../lib/uds_client.js";
import { IngestSpool, computeRecordKey } from "../lib/ingest_spool.js";
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

function installPlugin(config) {
  const handlers = new Map();
  const ctx = {
    on(event, handler) {
      handlers.set(event, handler);
    },
  };
  apply(ctx, config);
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

    for (let i = 0; i < 6; i++) {
      spool.enqueue(makePayload(i));
    }
    const keys = spool.pending().map((r) => r.key);
    assert.ok(spool.pending().length <= 5);
    assert.equal(keys.includes("cap:m-0:m-0"), false, "oldest record should be dropped");
    assert.equal(keys.includes("cap:m-5:m-5"), true, "newest record should be kept");

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

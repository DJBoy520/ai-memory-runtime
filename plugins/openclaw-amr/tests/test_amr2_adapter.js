import "./test_env.js";
/**
 * AMR 2.x adapter integration tests for plugins/openclaw-amr.
 *
 * A fake local adapter UDS (4-byte length-prefixed JSON-RPC 2.0) is started in
 * a fresh temporary directory. No fixed socket paths, no production services,
 * and every temp directory/file is cleaned up.
 *
 * Covers DOC-AMR-09 requirements:
 * a. amr.ping health check, fail-open when unavailable
 * b. memory.search without spoofed source_agent/agent_id
 * c. confirmed session.ingest clears the offline spool
 * d. -32010 (ADAPTER_UNAVAILABLE) keeps the record for replay
 * e. -32012 (ADAPTER_REMOTE_AUTH_FAILED) is not retried forever
 * f. JSON-RPC error responses never crash the host
 */

import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";

import { plugin } from "../index.js";
import {
  AmrUdsClient,
  FrameDecoder,
  encodeFrame,
  AMR_ERROR_CODES,
  isTransientError,
} from "../lib/uds_client.js";
import { SessionPersistenceManager } from "../lib/session_persistence.js";

const silentLogger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};

class FakeAdapter {
  constructor(dir) {
    this.dir = dir;
    this.socketPath = path.join(dir, "amr2-openclaw.sock");
    this.server = net.createServer((socket) => this._onConnection(socket));
    this.sockets = new Set();
    this.requests = [];
    this.errorForMethod = new Map();
    this.silentMethods = new Set();
    this.handlers = {
      "amr.ping": () => ({ status: "ok" }),
      "memory.search": () => ({
        results: [{ content: "AMR 2.x memory", score: 0.93, scope: "global" }],
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

  find(method) {
    return this.requests.find((r) => r.method === method);
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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr2-openclaw-test-"));
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

function registerPlugin(config) {
  const hooks = new Map();
  const api = {
    pluginConfig: config,
    logger: silentLogger,
    on(name, fn) {
      hooks.set(name, fn);
    },
  };
  plugin.register(api);
  return hooks;
}

function baseConfig(adapter, dir) {
  return {
    socketPath: adapter.socketPath,
    persistenceDir: path.join(dir, "spool"),
    deadlineMs: 300,
    ingestDeadlineMs: 400,
    autoReplayOnStartup: false,
  };
}

function newClient(adapter) {
  return new AmrUdsClient({ socketPath: adapter.socketPath, defaultDeadlineMs: 400 });
}

function assertNoIdentity(params) {
  assert.equal(Object.hasOwn(params, "agent_id"), false);
  assert.equal(Object.hasOwn(params, "source_agent"), false);
  assert.equal(params.agent_id, undefined);
  assert.equal(params.source_agent, undefined);
}

test("a. startup amr.ping succeeds and a missing adapter fails open", async () => {
  await withAdapter(async (adapter, dir) => {
    const hooks = registerPlugin(baseConfig(adapter, dir));
    assert.equal(typeof hooks.get("before_prompt_build"), "function");
    assert.equal(typeof hooks.get("agent_end"), "function");

    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");
    const ping = adapter.find("amr.ping");
    assert.deepEqual(ping.params, {});
    assertNoIdentity(ping.params);

    // Direct client ping also works.
    const pong = await newClient(adapter).ping(300);
    assert.deepEqual(pong, { status: "ok" });

    // A completely missing adapter must not throw or create an unhandled
    // rejection; the host keeps its hooks.
    const missingSocket = path.join(dir, "missing-adapter.sock");
    assert.doesNotThrow(() => {
      registerPlugin({
        socketPath: missingSocket,
        persistenceDir: path.join(dir, "missing-spool"),
        deadlineMs: 40,
        ingestDeadlineMs: 40,
        autoReplayOnStartup: false,
      });
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
  });
});

test("b. memory.search carries no agent identity fields", async () => {
  await withAdapter(async (adapter, dir) => {
    const hooks = registerPlugin(baseConfig(adapter, dir));

    const result = await hooks.get("before_prompt_build")(
      { currentUserMessage: "How do I configure the AMR 2.x adapter?" },
      { activeProjectKeys: ["project-2x"] }
    );

    assert.ok(result?.prependSystemContext);
    assert.match(result.prependSystemContext, /AMR 2\.x memory/);

    const search = adapter.find("memory.search");
    assert.ok(search, "memory.search should reach the adapter");
    assert.equal(search.params.query, "How do I configure the AMR 2.x adapter?");
    assert.equal(search.params.project_id, "project-2x");
    assertNoIdentity(search.params);
  });
});

test("c. confirmed session.ingest drains the spool (no pending records)", async () => {
  await withAdapter(async (adapter, dir) => {
    const spoolDir = path.join(dir, "spool");
    const hooks = registerPlugin(baseConfig(adapter, dir));

    const outcome = await hooks.get("agent_end")(
      {
        messages: [
          { message_id: "c-u1", role: "user", content: "Hello AMR 2.x" },
          { message_id: "c-a1", role: "assistant", content: "Memory stored" },
        ],
      },
      { sessionId: "sess-c" }
    );

    assert.equal(outcome.ok, true);
    assert.equal(outcome.persisted, true);

    const ingest = adapter.find("session.ingest");
    assert.ok(ingest, "session.ingest should reach the adapter");
    assert.ok(ingest.id !== undefined && ingest.id !== null, "request must carry a JSON-RPC id");
    assert.equal(ingest.params.session_id, "sess-c");
    assert.equal(ingest.params.messages.length, 2);
    assertNoIdentity(ingest.params);

    const manager = new SessionPersistenceManager({ storageDir: spoolDir });
    const record = await manager.getRecord("sess-c");
    assert.ok(record);
    assert.equal(record.status, "success");

    // Drained: nothing would be sent again by startup replay. Success records
    // are retained as an audit trail by the existing persistence design.
    const pending = (await manager.listRecords()).filter((r) => r.status !== "success");
    assert.equal(pending.length, 0, "spool must have no pending/failed records after ack");

    const replay = await manager.replayPending({ udsClient: newClient(adapter) });
    assert.equal(replay.replayed, 0);
    assert.equal(replay.failed, 0);
    assert.equal(adapter.count("session.ingest"), 1);

    const leftovers = fs.readdirSync(spoolDir).filter((f) => f.includes(".tmp"));
    assert.deepEqual(leftovers, []);
  });
});

test("d. -32010 keeps the record spooled and a later replay delivers it", async () => {
  await withAdapter(async (adapter, dir) => {
    const spoolDir = path.join(dir, "spool");
    const hooks = registerPlugin(baseConfig(adapter, dir));

    adapter.setError("session.ingest", AMR_ERROR_CODES.ADAPTER_UNAVAILABLE, "adapter unavailable");
    const outcome = await hooks.get("agent_end")(
      { messages: [{ message_id: "d-u1", role: "user", content: "Keep me" }] },
      { sessionId: "sess-d" }
    );

    assert.equal(outcome.ok, false);
    assert.equal(outcome.transient, true);
    assert.equal(outcome.permanent, false);
    assert.equal(isTransientError({ code: AMR_ERROR_CODES.ADAPTER_UNAVAILABLE }), true);

    const manager = new SessionPersistenceManager({ storageDir: spoolDir });
    let record = await manager.getRecord("sess-d");
    assert.ok(record, "record must be retained on disk");
    assert.equal(record.status, "failed");
    assert.equal(record.permanent, false);
    assert.equal(record.retryCount, 1);
    assert.match(record.lastError, /-32010/);

    adapter.clearError("session.ingest");
    const summary = await manager.replayPending({
      udsClient: newClient(adapter),
      deadlineMs: 400,
    });
    assert.equal(summary.replayed, 1);
    assert.equal(summary.succeeded, 1);
    assert.equal(summary.failed, 0);
    assert.equal(summary.skippedPermanent, 0);
    assert.equal(adapter.count("session.ingest"), 2);

    record = await manager.getRecord("sess-d");
    assert.equal(record.status, "success");
    assert.equal(record.permanent, false);
  });
});

test("e. -32012 is retained but never retried forever", async () => {
  await withAdapter(async (adapter, dir) => {
    const spoolDir = path.join(dir, "spool");
    const hooks = registerPlugin(baseConfig(adapter, dir));
    const event = {
      messages: [
        { message_id: "e-u1", role: "user", content: "hello" },
        { message_id: "e-a1", role: "assistant", content: "hi" },
      ],
    };

    adapter.setError(
      "session.ingest",
      AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED,
      "remote auth failed"
    );

    const first = await hooks.get("agent_end")(event, { sessionId: "sess-e" });
    assert.equal(first.ok, false);
    assert.equal(first.permanent, true);
    assert.equal(first.transient, false);
    assert.equal(adapter.count("session.ingest"), 1);

    const manager = new SessionPersistenceManager({ storageDir: spoolDir });
    let record = await manager.getRecord("sess-e");
    assert.ok(record);
    assert.equal(record.status, "failed");
    assert.equal(record.permanent, true);
    assert.equal(record.retryCount, 1);
    assert.match(record.lastError, /-32012/);

    // A later turn with the same payload in the same process must not retry.
    const second = await hooks.get("agent_end")(event, { sessionId: "sess-e" });
    assert.equal(second.ok, false);
    assert.equal(second.skipped, true);
    assert.equal(second.permanent, true);
    assert.equal(adapter.count("session.ingest"), 1);

    // Startup/replay must skip the permanent record as well.
    adapter.clearError("session.ingest");
    const summary = await manager.replayPending({
      udsClient: newClient(adapter),
      deadlineMs: 400,
    });
    assert.equal(summary.replayed, 0);
    assert.equal(summary.succeeded, 0);
    assert.equal(summary.failed, 0);
    assert.equal(summary.skippedPermanent, 1);
    assert.equal(adapter.count("session.ingest"), 1);

    record = await manager.getRecord("sess-e");
    assert.equal(record.status, "failed");
    assert.equal(record.permanent, true);
  });
});

test("f. JSON-RPC error responses do not crash the client or the host hook", async () => {
  await withAdapter(async (adapter, dir) => {
    const hooks = registerPlugin(baseConfig(adapter, dir));

    adapter.setError(
      "memory.search",
      AMR_ERROR_CODES.ADAPTER_PROTOCOL_ERROR,
      "remote response did not match the protocol"
    );
    const failed = await hooks.get("before_prompt_build")(
      { currentUserMessage: "trigger a protocol error" },
      {}
    );
    assert.equal(failed, undefined, "prefetch must fail open");

    adapter.clearError("memory.search");
    const recovered = await hooks.get("before_prompt_build")(
      { currentUserMessage: "try again after the error" },
      {}
    );
    assert.ok(recovered?.prependSystemContext, "client must remain usable after an error response");

    // Direct client classification: protocol errors are not transient.
    const client = newClient(adapter);
    adapter.setError("memory.search", AMR_ERROR_CODES.ADAPTER_PROTOCOL_ERROR, "bad frame");
    await assert.rejects(
      () => client.request("memory.search", { query: "two" }, 400),
      (err) => {
        assert.equal(err.code, AMR_ERROR_CODES.ADAPTER_PROTOCOL_ERROR);
        assert.equal(isTransientError(err), false);
        return true;
      }
    );
    adapter.clearError("memory.search");
    const ok = await client.request("memory.search", { query: "three" }, 400);
    assert.ok(Array.isArray(ok.results));
    assert.equal(adapter.count("memory.search"), 4);
  });
});

test("g. payloads remain deterministic and identity-free across retries", async () => {
  await withAdapter(async (adapter, dir) => {
    const hooks = registerPlugin(baseConfig(adapter, dir));
    const event = {
      messages: [
        { role: "user", content: "no explicit ids" },
        { role: "assistant", content: "still deterministic" },
      ],
    };

    await hooks.get("agent_end")(event, { sessionId: "sess-g" });
    await hooks.get("agent_end")(event, { sessionId: "sess-g" });

    const ingestRequests = adapter.requests.filter((r) => r.method === "session.ingest");
    assert.equal(ingestRequests.length, 2);
    assert.deepEqual(
      ingestRequests[0].params.messages.map((m) => m.message_id),
      ingestRequests[1].params.messages.map((m) => m.message_id)
    );
    assertNoIdentity(ingestRequests[0].params);
    assertNoIdentity(ingestRequests[1].params);
  });
});

test("h. legacy 1.x spool records with agent_id are replayed without identity fields", async () => {
  await withAdapter(async (adapter, dir) => {
    const spoolDir = path.join(dir, "spool");
    const seed = new SessionPersistenceManager({ storageDir: spoolDir });

    // Simulate a record written by the 1.x plugin before the upgrade.
    await seed.persistSession(
      "sess-h",
      {
        session_id: "sess-h",
        agent_id: "openclaw",
        source_agent: "openclaw",
        project_id: null,
        messages: [
          { message_id: "h-u1", role: "user", content: "legacy", sequence: 1, timestamp: 100 },
        ],
      },
      "failed"
    );

    const replayManager = new SessionPersistenceManager({
      storageDir: spoolDir,
      udsClient: newClient(adapter),
      ingestDeadlineMs: 400,
    });
    const summary = await replayManager.replayPending({ deadlineMs: 400 });
    assert.equal(summary.replayed, 1);
    assert.equal(summary.succeeded, 1);

    const ingest = adapter.find("session.ingest");
    assert.ok(ingest);
    assert.equal(ingest.params.session_id, "sess-h");
    assertNoIdentity(ingest.params);
  });
});

test("i. startup replay skips an on-disk permanent record", async () => {
  await withAdapter(async (adapter, dir) => {
    const spoolDir = path.join(dir, "spool");
    const seed = new SessionPersistenceManager({ storageDir: spoolDir });
    await seed.persistSession(
      "sess-i",
      {
        session_id: "sess-i",
        messages: [
          { message_id: "i-u1", role: "user", content: "do not retry", sequence: 1, timestamp: 100 },
        ],
      },
      "failed"
    );
    await seed.markPermanent("sess-i", {
      code: AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED,
      message: "remote auth failed",
    });

    registerPlugin({
      ...baseConfig(adapter, dir),
      autoReplayOnStartup: true,
    });

    await waitFor(() => adapter.count("amr.ping") >= 1, "startup amr.ping");
    await new Promise((resolve) => setTimeout(resolve, 120));

    assert.equal(adapter.count("session.ingest"), 0, "permanent record must not be replayed");
    const record = await seed.getRecord("sess-i");
    assert.equal(record.permanent, true);
    assert.equal(record.status, "failed");
  });
});

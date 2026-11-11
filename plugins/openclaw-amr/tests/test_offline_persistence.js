/**
 * tests/test_offline_persistence.js
 *
 * Dedicated test suite for OpenClaw AMR session offline persistence and retry:
 * 1. Security & File Permissions (0700 dir, 0600 files)
 * 2. Atomic Writes & Corrupted Leftover Prevention
 * 3. Persist-First Architecture (Persist before sending)
 * 4. Failure Retention (Keep failed records on disk)
 * 5. Idempotent Replay for both Success and Failure cases
 * 6. Process Restart Replay (Startup replay of pending/failed spools)
 * 7. Messages without original ID & No loss of identical text messages
 * 8. Concurrency Protection (Keyed mutex & replay lock)
 * 9. Path Traversal & Data Exposure Limits
 * 10. Strict Isolation & Mock UDS (Zero production/real service touches)
 */

import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import net from "node:net";
import { plugin } from "../index.js";
import { AmrUdsClient, encodeFrame, FrameDecoder } from "../lib/uds_client.js";
import { SessionPersistenceManager, resolveStorageDir, getSafeFileName } from "../lib/session_persistence.js";
import { mapSessionToIngest } from "../lib/session_mapper.js";

/**
 * Creates an isolated real temporary directory for testing.
 */
function createIsolatedTempDir(prefix = "amr_test_persist_") {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}${Date.now()}_`));
}

/**
 * Creates an isolated mock UDS server on a temporary socket.
 */
function createMockUdsServer(socketPath, onFrame) {
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

function createTempSocketPath(prefix = "openclaw_persist_test") {
  return path.join(
    os.tmpdir(),
    `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2)}.sock`
  );
}

// ============================================================================
// Suite 1: Security & File Permissions (0700 dir, 0600 files)
// ============================================================================

test("Persistence Security: directory mode is 0700 and file mode is 0600", async () => {
  const isolatedDir = createIsolatedTempDir("sec_perm_");
  const testSubDir = path.join(isolatedDir, "sessions_sub");

  try {
    const manager = new SessionPersistenceManager({ storageDir: testSubDir });
    assert.equal(fs.existsSync(testSubDir), true);

    if (process.platform !== "win32") {
      const dirStat = fs.statSync(testSubDir);
      assert.equal(dirStat.mode & 0o777, 0o700, "Directory must have strict 0700 permissions");
    }

    const payload = {
      session_id: "sess_sec_01",
      agent_id: "openclaw",
      messages: [{ message_id: "m1", role: "user", content: "test", sequence: 1, timestamp: 12345 }],
    };

    const record = await manager.persistSession("sess_sec_01", payload);
    assert.equal(record.sessionId, "sess_sec_01");

    const recordPath = manager.getRecordPath("sess_sec_01");
    assert.equal(fs.existsSync(recordPath), true);

    if (process.platform !== "win32") {
      const fileStat = fs.statSync(recordPath);
      assert.equal(fileStat.mode & 0o777, 0o600, "Session file must have strict 0600 permissions");
    }
  } finally {
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 2: Atomic Writes & Corrupted Leftover Prevention
// ============================================================================

test("Persistence Atomic Writes: writes complete JSON and leaves no temporary files", async () => {
  const isolatedDir = createIsolatedTempDir("atomic_");

  try {
    const manager = new SessionPersistenceManager({ storageDir: isolatedDir });

    for (let i = 0; i < 5; i++) {
      await manager.persistSession(`sess_atomic_${i}`, {
        session_id: `sess_atomic_${i}`,
        agent_id: "openclaw",
        messages: [{ message_id: `m_${i}`, role: "user", content: `msg ${i}`, sequence: 1, timestamp: 12345 }],
      });
    }

    const allFiles = fs.readdirSync(isolatedDir);
    const tmpFiles = allFiles.filter((f) => f.includes(".tmp"));
    const jsonFiles = allFiles.filter((f) => f.endsWith(".json"));

    assert.equal(tmpFiles.length, 0, "No temporary (.tmp) files should remain after atomic write");
    assert.equal(jsonFiles.length, 5, "All 5 session files must exist");

    for (const file of jsonFiles) {
      const content = fs.readFileSync(path.join(isolatedDir, file), "utf8");
      const parsed = JSON.parse(content);
      assert.equal(parsed.version, 1);
      assert.ok(parsed.sessionId);
      assert.equal(parsed.status, "pending");
    }
  } finally {
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 3: Persist-First Architecture (Persist before sending)
// ============================================================================

test("Persist-First: session is written to disk before network response and marked success upon delivery", async () => {
  const isolatedDir = createIsolatedTempDir("persist_first_");
  const sockPath = createTempSocketPath("first");
  let serverSawRecordOnDiskBeforeReplying = false;

  const manager = new SessionPersistenceManager({ storageDir: isolatedDir });

  const server = await createMockUdsServer(sockPath, (req, socket) => {
    // Check if file is already on disk while server is processing the request!
    const recordPath = manager.getRecordPath(req.params.session_id);
    if (fs.existsSync(recordPath)) {
      const data = JSON.parse(fs.readFileSync(recordPath, "utf8"));
      if (data.status === "pending") {
        serverSawRecordOnDiskBeforeReplying = true;
      }
    }

    socket.write(encodeFrame({
      jsonrpc: "2.0",
      id: req.id,
      result: { session_id: req.params.session_id, inserted: 2 },
    }));
  });

  try {
    let agentEndHook = null;
    const mockApi = {
      pluginConfig: {
        socketPath: sockPath,
        persistenceDir: isolatedDir,
        ingestDeadlineMs: 500,
        autoReplayOnStartup: false,
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    };

    plugin.register(mockApi);
    assert.ok(typeof agentEndHook === "function");

    const outcome = await agentEndHook(
      {
        messages: [
          { role: "user", content: "Query A" },
          { role: "assistant", content: "Answer A" },
        ],
      },
      { sessionId: "sess_persist_first_1" }
    );

    assert.equal(outcome.ok, true);
    assert.equal(outcome.persisted, true);
    assert.equal(serverSawRecordOnDiskBeforeReplying, true, "File must be persisted to disk with status 'pending' before network responds");

    const record = await manager.getRecord("sess_persist_first_1");
    assert.equal(record.status, "success");
    assert.equal(record.lastError, null);
    assert.deepEqual(record.lastResult, { session_id: "sess_persist_first_1", inserted: 2 });
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 4: Failure Retention (重试失败保留)
// ============================================================================

test("Failure Retention: failed session is retained on disk with retry count and error details", async () => {
  const isolatedDir = createIsolatedTempDir("fail_retain_");
  const offlineSockPath = path.join(isolatedDir, "non_existent.sock");

  try {
    let agentEndHook = null;
    const mockApi = {
      pluginConfig: {
        socketPath: offlineSockPath,
        persistenceDir: isolatedDir,
        ingestDeadlineMs: 100,
        autoReplayOnStartup: false,
      },
      on(name, fn) {
        if (name === "agent_end") agentEndHook = fn;
      },
    };

    plugin.register(mockApi);

    const outcome = await agentEndHook(
      {
        messages: [
          { role: "user", content: "Important offline data" },
        ],
      },
      { sessionId: "sess_fail_retain_01" }
    );

    assert.equal(outcome.ok, false);
    assert.equal(outcome.persisted, true);

    const manager = new SessionPersistenceManager({ storageDir: isolatedDir });
    const record = await manager.getRecord("sess_fail_retain_01");

    assert.ok(record, "Record must be retained on disk after delivery failure");
    assert.equal(record.status, "failed");
    assert.equal(record.retryCount, 1);
    assert.ok(record.lastError);
    assert.ok(record.lastError.includes("ENOENT") || record.lastError.includes("ECONNREFUSED"));
    assert.equal(record.payload.messages.length, 1);
  } finally {
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 5 & 6: Idempotent Replay for Both Success and Failure
// ============================================================================

test("Idempotent Replay: successfully replays failed sessions and allows safe replay of successful sessions", async () => {
  const isolatedDir = createIsolatedTempDir("idempotent_");
  const sockPath = createTempSocketPath("idempotent");
  const serverReceivedMessages = [];

  const server = await createMockUdsServer(sockPath, (req, socket) => {
    serverReceivedMessages.push(req);
    socket.write(encodeFrame({
      jsonrpc: "2.0",
      id: req.id,
      result: { session_id: req.params.session_id, status: "success", count: req.params.messages.length },
    }));
  });

  try {
    const udsClient = new AmrUdsClient({ socketPath: sockPath, defaultDeadlineMs: 500 });
    const manager = new SessionPersistenceManager({
      storageDir: isolatedDir,
      udsClient,
      ingestDeadlineMs: 500,
    });

    // 1. Create a failed session record manually (simulating past failure)
    const payload = mapSessionToIngest({
      sessionId: "sess_replay_test",
      messages: [
        { role: "user", content: "Prompt 1" },
        { role: "assistant", content: "Response 1" },
      ],
    });

    await manager.persistSession("sess_replay_test", payload, "failed");
    let rec = await manager.getRecord("sess_replay_test");
    assert.equal(rec.status, "failed");

    // 2. Replay the failed session
    const replay1 = await manager.replaySession("sess_replay_test");
    assert.equal(replay1.ok, true);
    assert.equal(replay1.replayed, true);
    assert.equal(replay1.status, "success");

    rec = await manager.getRecord("sess_replay_test");
    assert.equal(rec.status, "success");
    assert.equal(serverReceivedMessages.length, 1);

    // 3. Replay without force on a successful record: skips safely
    const replay2 = await manager.replaySession("sess_replay_test", { force: false });
    assert.equal(replay2.ok, true);
    assert.equal(replay2.skipped, true);
    assert.equal(serverReceivedMessages.length, 1, "Should not send network request when skipped");

    // 4. Replay with force=true on a successful record (Idempotent replay test)
    const replay3 = await manager.replaySession("sess_replay_test", { force: true });
    assert.equal(replay3.ok, true);
    assert.equal(replay3.replayed, true);
    assert.equal(serverReceivedMessages.length, 2);

    // Verify both network payloads had identical session_id and identical message IDs
    const req1 = serverReceivedMessages[0];
    const req2 = serverReceivedMessages[1];
    assert.equal(req1.params.session_id, req2.params.session_id);
    assert.equal(req1.params.messages[0].message_id, req2.params.messages[0].message_id);
    assert.equal(req1.params.messages[1].message_id, req2.params.messages[1].message_id);

    // 5. Test replayAll() across all records
    const replaySummary = await manager.replayAll();
    assert.equal(replaySummary.total, 1);
    assert.equal(replaySummary.replayed, 1);
    assert.equal(replaySummary.succeeded, 1);
    assert.equal(replaySummary.failed, 0);
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 7: Process Restart Replay (进程重启重放)
// ============================================================================

test("Process Restart Replay: startup automatically replays pending/failed offline spools", async () => {
  const isolatedDir = createIsolatedTempDir("restart_");
  const sockPath = createTempSocketPath("restart");

  // Step 1: Pre-populate 2 sessions in isolatedDir as "failed" (simulating past process run where AMR was down)
  const preManager = new SessionPersistenceManager({ storageDir: isolatedDir });
  await preManager.persistSession("sess_restart_1", {
    session_id: "sess_restart_1",
    agent_id: "openclaw",
    messages: [{ message_id: "m1", role: "user", content: "Query 1", sequence: 1, timestamp: 100 }],
  }, "failed");
  await preManager.persistSession("sess_restart_2", {
    session_id: "sess_restart_2",
    agent_id: "openclaw",
    messages: [{ message_id: "m2", role: "user", content: "Query 2", sequence: 1, timestamp: 101 }],
  }, "failed");
  preManager.close();

  const receivedByServer = [];
  const server = await createMockUdsServer(sockPath, (req, socket) => {
    receivedByServer.push(req.params.session_id);
    socket.write(encodeFrame({
      jsonrpc: "2.0",
      id: req.id,
      result: { session_id: req.params.session_id, status: "ingested" },
    }));
  });

  try {
    // Step 2: Simulate process startup with new plugin registration pointing to same storageDir
    const mockApi = {
      pluginConfig: {
        socketPath: sockPath,
        persistenceDir: isolatedDir,
        autoReplayOnStartup: true, // Enable startup replay
        ingestDeadlineMs: 500,
      },
      on() {},
    };

    plugin.register(mockApi);

    // Wait for startup async replay to execute
    for (let i = 0; i < 20; i++) {
      if (receivedByServer.length >= 2) break;
      await new Promise((r) => setTimeout(r, 50));
    }

    assert.equal(receivedByServer.length, 2, "Both offline sessions should have been replayed on process restart");
    assert.ok(receivedByServer.includes("sess_restart_1"));
    assert.ok(receivedByServer.includes("sess_restart_2"));

    // Verify both files on disk transitioned to "success"
    const rec1 = await preManager.getRecord("sess_restart_1");
    const rec2 = await preManager.getRecord("sess_restart_2");
    assert.equal(rec1.status, "success");
    assert.equal(rec2.status, "success");
  } finally {
    server.close();
    if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 8: Messages Without Original ID & Preserving Identical Text
// ============================================================================

test("Messages without original ID: identical text messages are NOT dropped and get distinct deterministic IDs", () => {
  const sessionId = "sess_repeated_text_99";
  const rawMessages = [
    { role: "user", content: "Confirm action" },
    { role: "assistant", content: "Are you sure?" },
    { role: "user", content: "Confirm action" }, // Identical text
    { role: "user", content: "Confirm action" }, // Identical text again
    { role: "assistant", content: "Action executed." },
  ];

  const payload = mapSessionToIngest({
    sessionId,
    messages: rawMessages,
  });

  // Verify: All 5 messages MUST be retained. None dropped!
  assert.equal(payload.messages.length, 5, "Must NOT drop messages with identical text");

  // Verify: Monotonic sequence
  for (let i = 0; i < 5; i++) {
    assert.equal(payload.messages[i].sequence, i + 1);
  }

  // Verify: Messages 0, 2, and 3 have identical content, but distinct message_ids
  assert.equal(payload.messages[0].content, "Confirm action");
  assert.equal(payload.messages[2].content, "Confirm action");
  assert.equal(payload.messages[3].content, "Confirm action");

  const id0 = payload.messages[0].message_id;
  const id2 = payload.messages[2].message_id;
  const id3 = payload.messages[3].message_id;

  assert.notEqual(id0, id2, "Message 0 and Message 2 must have distinct message_ids");
  assert.notEqual(id2, id3, "Message 2 and Message 3 must have distinct message_ids");
  assert.notEqual(id0, id3, "Message 0 and Message 3 must have distinct message_ids");

  // Verify: Sequence is reflected in ID
  assert.ok(id0.includes("_1_"));
  assert.ok(id2.includes("_3_"));
  assert.ok(id3.includes("_4_"));

  // Verify: Deterministic replay: mapping the exact same session yields the exact same IDs
  const payload2 = mapSessionToIngest({
    sessionId,
    messages: rawMessages,
  });

  assert.equal(payload2.messages[0].message_id, id0, "Replay must yield identical deterministic ID");
  assert.equal(payload2.messages[2].message_id, id2, "Replay must yield identical deterministic ID");
  assert.equal(payload2.messages[3].message_id, id3, "Replay must yield identical deterministic ID");
});

// ============================================================================
// Suite 9: Concurrency Protection (Keyed Mutex & Replay Lock)
// ============================================================================

test("Concurrency Protection: parallel operations on same session execute sequentially without corrupting files", async () => {
  const isolatedDir = createIsolatedTempDir("concurrency_");
  const manager = new SessionPersistenceManager({ storageDir: isolatedDir });

  try {
    const sessionId = "sess_concurrent_01";

    // Launch 15 concurrent updates on the same sessionId
    const ops = Array.from({ length: 15 }, (_, i) => {
      const payload = {
        session_id: sessionId,
        agent_id: "openclaw",
        messages: [{ message_id: `m_${i}`, role: "user", content: `msg ${i}`, sequence: i + 1, timestamp: 1000 + i }],
      };
      return manager.persistSession(sessionId, payload);
    });

    const results = await Promise.all(ops);
    assert.equal(results.length, 15);

    // Verify record on disk is intact and valid JSON
    const record = await manager.getRecord(sessionId);
    assert.ok(record);
    assert.equal(record.sessionId, sessionId);
    assert.equal(typeof record.updatedAt, "number");

    // Verify no stray temp files
    const remaining = fs.readdirSync(isolatedDir);
    assert.equal(remaining.filter((f) => f.includes(".tmp")).length, 0);
  } finally {
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 10: Path Traversal & Data Exposure Limits
// ============================================================================

test("Data Exposure & Path Traversal: malicious session IDs are sanitized and stay inside storageDir", async () => {
  const isolatedDir = createIsolatedTempDir("path_traversal_");
  const manager = new SessionPersistenceManager({ storageDir: isolatedDir });

  try {
    const maliciousId = "../../etc/passwd_evil/test\0special*chars";
    const record = await manager.persistSession(maliciousId, {
      session_id: maliciousId,
      agent_id: "openclaw",
      messages: [{ message_id: "m1", role: "user", content: "secret prompt", sequence: 1, timestamp: 100 }],
    });

    const filePath = manager.getRecordPath(maliciousId);

    // Verify file is strictly inside isolatedDir
    assert.equal(path.dirname(filePath), isolatedDir, "Must NOT escape storageDir");
    assert.equal(fs.existsSync(filePath), true);

    const safeName = getSafeFileName(maliciousId);
    assert.ok(!safeName.includes("/"));
    assert.ok(!safeName.includes("\\"));
    assert.ok(!safeName.includes(".."));
  } finally {
    fs.rmSync(isolatedDir, { recursive: true, force: true });
  }
});

// ============================================================================
// Suite 11: Real Service Prohibition (严禁对真实服务或生产数据测试)
// ============================================================================

test("Zero Production Touch: resolves only isolated temp socket and isolated directories", () => {
  const customDir = "/tmp/my_custom_isolated_spool";
  const resolved = resolveStorageDir(customDir);
  assert.equal(resolved, path.resolve(customDir));

  // Verify default client does not connect to live socket during instantiation
  const client = new AmrUdsClient({ socketPath: "/tmp/non_existent_mock.sock" });
  assert.equal(client.getSocketPath(), "/tmp/non_existent_mock.sock");
});

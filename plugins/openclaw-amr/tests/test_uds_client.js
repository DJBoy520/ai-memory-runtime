import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import {
  encodeFrame,
  FrameDecoder,
  resolveSocketPath,
  AmrUdsClient,
} from "../lib/uds_client.js";

test("Frame encoding produces 4-byte Big-Endian length prefix", () => {
  const payload = { hello: "world", count: 42 };
  const encoded = encodeFrame(payload);

  const len = encoded.readUInt32BE(0);
  const jsonStr = encoded.subarray(4).toString("utf8");

  assert.equal(len, encoded.length - 4);
  assert.deepEqual(JSON.parse(jsonStr), payload);
});

test("FrameDecoder handles sticky packets (粘包)", () => {
  const decoder = new FrameDecoder();
  const msg1 = { id: 1, text: "first" };
  const msg2 = { id: 2, text: "second" };

  const frame1 = encodeFrame(msg1);
  const frame2 = encodeFrame(msg2);

  // Concatenate both frames into one chunk
  const stickyChunk = Buffer.concat([frame1, frame2]);
  const results = decoder.feed(stickyChunk);

  assert.equal(results.length, 2);
  assert.deepEqual(JSON.parse(results[0].toString("utf8")), msg1);
  assert.deepEqual(JSON.parse(results[1].toString("utf8")), msg2);
});

test("FrameDecoder handles partial packets / fragmentation (半包)", () => {
  const decoder = new FrameDecoder();
  const msg = { id: 99, data: "a".repeat(500) };
  const frame = encodeFrame(msg);

  // Split into 3 parts: header partial, body part 1, body part 2
  const part1 = frame.subarray(0, 2); // 2 bytes of header
  const part2 = frame.subarray(2, 50); // rest of header + 46 bytes
  const part3 = frame.subarray(50); // remaining bytes

  assert.deepEqual(decoder.feed(part1), []);
  assert.deepEqual(decoder.feed(part2), []);
  const results = decoder.feed(part3);

  assert.equal(results.length, 1);
  assert.deepEqual(JSON.parse(results[0].toString("utf8")), msg);
});

test("FrameDecoder rejects oversized frames (> 4MB)", () => {
  const decoder = new FrameDecoder();
  const maliciousHeader = Buffer.alloc(4);
  maliciousHeader.writeUInt32BE(5 * 1024 * 1024, 0); // 5MB

  assert.throws(() => {
    decoder.feed(maliciousHeader);
  }, /AMR_FRAME_SIZE_INVALID/);
});

test("AmrUdsClient handles request & response with mock UDS server", async () => {
  const tmpSock = path.join(os.tmpdir(), `test_uds_${Date.now()}_${Math.random().toString(36).slice(2)}.sock`);
  if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);

  const server = net.createServer((socket) => {
    const decoder = new FrameDecoder();
    socket.on("data", (chunk) => {
      const frames = decoder.feed(chunk);
      for (const f of frames) {
        const req = JSON.parse(f.toString("utf8"));
        if (req.method === "memory.search") {
          const resp = {
            jsonrpc: "2.0",
            id: req.id,
            result: {
              results: [
                { content: "recalled fact", score: 0.95 }
              ]
            }
          };
          socket.write(encodeFrame(resp));
        }
      }
    });
  });

  await new Promise((r) => server.listen(tmpSock, r));

  try {
    const client = new AmrUdsClient({ socketPath: tmpSock, defaultDeadlineMs: 500 });
    const res = await client.request("memory.search", { query: "hello" });
    assert.ok(res.results);
    assert.equal(res.results.length, 1);
    assert.equal(res.results[0].content, "recalled fact");
  } finally {
    server.close();
    if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);
  }
});

test("AmrUdsClient enforces Hard Deadline and socket destruction on timeout", async () => {
  const tmpSock = path.join(os.tmpdir(), `test_uds_timeout_${Date.now()}_${Math.random().toString(36).slice(2)}.sock`);
  if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);

  // Server that intentionally hangs and never responds
  const server = net.createServer((_socket) => {});
  await new Promise((r) => server.listen(tmpSock, r));

  try {
    const client = new AmrUdsClient({ socketPath: tmpSock, defaultDeadlineMs: 50 });
    const startTime = Date.now();

    await assert.rejects(
      async () => {
        await client.request("memory.search", { query: "slow" }, 50);
      },
      (err) => {
        assert.match(err.message, /AMR_DEADLINE_EXCEEDED/);
        return true;
      }
    );

    const elapsed = Date.now() - startTime;
    assert.ok(elapsed >= 40 && elapsed < 200, `Deadline should trigger promptly, took ${elapsed}ms`);
  } finally {
    server.close();
    if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);
  }
});

test("AmrUdsClient fire-and-forget notify sends data without throwing", async () => {
  const tmpSock = path.join(os.tmpdir(), `test_uds_notify_${Date.now()}_${Math.random().toString(36).slice(2)}.sock`);
  if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);

  let receivedPayload = null;
  const server = net.createServer((socket) => {
    const decoder = new FrameDecoder();
    socket.on("data", (chunk) => {
      const frames = decoder.feed(chunk);
      for (const f of frames) {
        receivedPayload = JSON.parse(f.toString("utf8"));
      }
    });
  });

  await new Promise((r) => server.listen(tmpSock, r));

  try {
    const client = new AmrUdsClient({ socketPath: tmpSock });
    client.notify("session.ingest", { session_id: "s123", messages: [] });

    // Wait short time for async setImmediate
    await new Promise((r) => setTimeout(r, 80));

    assert.ok(receivedPayload);
    assert.equal(receivedPayload.method, "session.ingest");
    assert.equal(receivedPayload.params.session_id, "s123");
  } finally {
    server.close();
    if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);
  }
});

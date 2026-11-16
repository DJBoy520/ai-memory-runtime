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
  AMR_ERROR_CODES,
  ERROR_CODES,
  isTransientError,
  isPermanentError,
  MAX_FRAME_SIZE,
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

test("encodeFrame rejects payloads beyond the 4MB protocol ceiling", () => {
  assert.throws(
    () => encodeFrame({ text: "x".repeat(MAX_FRAME_SIZE + 1) }),
    /AMR_FRAME_SIZE_INVALID/
  );
  assert.doesNotThrow(() => encodeFrame({ text: "ok" }));
});

test("AMR 2.x error codes and transient/permanent classification", () => {
  assert.equal(AMR_ERROR_CODES.ADAPTER_UNAVAILABLE, -32010);
  assert.equal(AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED, -32012);
  assert.equal(ERROR_CODES.AGENT_UNAUTHORIZED, -32001);
  assert.equal(ERROR_CODES.NOT_FOUND, -32005);
  assert.equal(ERROR_CODES.ADAPTER_PROTOCOL_ERROR, -32011);

  assert.equal(isTransientError({ code: AMR_ERROR_CODES.ADAPTER_UNAVAILABLE }), true);
  assert.equal(isTransientError({ code: "ETIMEDOUT" }), true);
  assert.equal(isTransientError({ code: "ECONNREFUSED" }), true);
  assert.equal(isTransientError({ code: AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED }), false);
  assert.equal(isPermanentError({ code: AMR_ERROR_CODES.ADAPTER_REMOTE_AUTH_FAILED }), true);
  assert.equal(isPermanentError({ code: AMR_ERROR_CODES.AGENT_UNAUTHORIZED }), true);
  assert.equal(isPermanentError({ code: AMR_ERROR_CODES.ADAPTER_UNAVAILABLE }), false);
});

test("AmrUdsClient.ping uses the amr.ping control method", async () => {
  const tmpSock = path.join(os.tmpdir(), `test_uds_ping_${Date.now()}_${Math.random().toString(36).slice(2)}.sock`);
  if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);

  const requests = [];
  const server = net.createServer((socket) => {
    const decoder = new FrameDecoder();
    socket.on("data", (chunk) => {
      for (const f of decoder.feed(chunk)) {
        const req = JSON.parse(f.toString("utf8"));
        requests.push(req);
        socket.write(encodeFrame({ jsonrpc: "2.0", id: req.id, result: { status: "ok" } }));
      }
    });
  });

  await new Promise((r) => server.listen(tmpSock, r));
  try {
    const client = new AmrUdsClient({ socketPath: tmpSock, defaultDeadlineMs: 300 });
    const res = await client.ping(300);
    assert.deepEqual(res, { status: "ok" });
    assert.equal(requests[0].method, "amr.ping");
    assert.deepEqual(requests[0].params, {});
  } finally {
    server.close();
    if (fs.existsSync(tmpSock)) fs.unlinkSync(tmpSock);
  }
});

test("per-agent credential is sent and missing credential fails", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "amr-auth-"));
  const sock = path.join(dir, "a.sock");
  const tokenFile = path.join(dir, "token");
  fs.writeFileSync(tokenFile, "x".repeat(43), {mode: 0o600});
  const server = net.createServer(socket => {
    const decoder = new FrameDecoder();
    socket.on("data", chunk => {
      for (const frame of decoder.feed(chunk)) {
        const req = JSON.parse(frame.toString());
        socket.end(encodeFrame({jsonrpc:"2.0", id:req.id, result:{token:req.auth_token}}));
      }
    });
  });
  await new Promise(resolve => server.listen(sock, resolve));
  try {
    const client = new AmrUdsClient({socketPath:sock, tokenFile, defaultDeadlineMs:1000});
    assert.equal((await client.ping()).token, "x".repeat(43));
    fs.unlinkSync(tokenFile);
    await assert.rejects(client.ping(), err => isTransientError(err));
  } finally {
    await new Promise(resolve => server.close(resolve));
    fs.rmSync(dir, {recursive:true, force:true});
  }
});

test('in-flight credential rotation is retryable', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'amr-rotate-'));
  const sock = path.join(dir, 's.sock'), file = path.join(dir, 'token');
  fs.writeFileSync(file, 'a'.repeat(43), {mode:0o600});
  const server = net.createServer(socket => {
    const decoder = new FrameDecoder();
    socket.on('data', chunk => {
      for (const frame of decoder.feed(chunk)) {
        const req = JSON.parse(frame.toString());
        fs.writeFileSync(file, 'b'.repeat(43));
        socket.end(encodeFrame({jsonrpc:'2.0',id:req.id,error:{code:-32012,message:'AUTH_FAILED'}}));
      }
    });
  });
  await new Promise(resolve => server.listen(sock, resolve));
  try {
    const client = new AmrUdsClient({socketPath:sock,tokenFile:file,defaultDeadlineMs:1000});
    await assert.rejects(client.ping(), err => isTransientError(err));
  } finally {
    await new Promise(resolve => server.close(resolve));
    fs.rmSync(dir,{recursive:true,force:true});
  }
});

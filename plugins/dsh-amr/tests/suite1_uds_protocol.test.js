import test from "node:test";
import assert from "node:assert/strict";
import { FrameDecoder, encodeFrame } from "../lib/uds_client.js";

test("Suite 1: UDS Protocol & Frame Stream Processing (30 tests)", async (t) => {
  // 1-5: Basic Frame Encoding & Length Headers
  await t.test("T001: encodeFrame encodes empty payload with length 2 ('{}')", () => {
    const frame = encodeFrame({});
    assert.equal(frame.length, 6);
    assert.equal(frame.readUInt32BE(0), 2);
    assert.equal(frame.subarray(4).toString("utf8"), "{}");
  });

  await t.test("T002: encodeFrame correctly calculates byte length for UTF-8 multibyte characters", () => {
    const obj = { text: "你好世界" }; // 4 Chinese chars = 12 bytes, total json {"text":"你好世界"} = 22 bytes
    const frame = encodeFrame(obj);
    const expectedJson = JSON.stringify(obj);
    const expectedByteLen = Buffer.byteLength(expectedJson, "utf8");
    assert.equal(frame.readUInt32BE(0), expectedByteLen);
    assert.equal(frame.length, 4 + expectedByteLen);
    assert.equal(frame.subarray(4).toString("utf8"), expectedJson);
  });

  await t.test("T003: encodeFrame preserves full Big-Endian UInt32 header value", () => {
    const frame = encodeFrame({ num: 100 });
    const b0 = frame[0], b1 = frame[1], b2 = frame[2], b3 = frame[3];
    assert.equal(b0, 0);
    assert.equal(b1, 0);
    assert.equal(b2, 0);
    assert.equal(b3, 11); // {"num":100} has length 11
  });

  await t.test("T004: encodeFrame handles null fields and boolean literals in JSON-RPC", () => {
    const req = { jsonrpc: "2.0", id: 1, result: null, ok: true };
    const frame = encodeFrame(req);
    const len = frame.readUInt32BE(0);
    const parsed = JSON.parse(frame.subarray(4).toString("utf8"));
    assert.equal(parsed.result, null);
    assert.equal(parsed.ok, true);
    assert.equal(parsed.id, 1);
  });

  await t.test("T005: encodeFrame supports complex nested arrays and objects", () => {
    const payload = { jsonrpc: "2.0", method: "test", params: { nested: [1, "two", { three: 3 }] } };
    const frame = encodeFrame(payload);
    const len = frame.readUInt32BE(0);
    assert.equal(len, Buffer.byteLength(JSON.stringify(payload), "utf8"));
    assert.deepEqual(JSON.parse(frame.subarray(4).toString("utf8")), payload);
  });

  // 6-12: Single Frame Decoding & FrameDecoder Basics
  await t.test("T006: FrameDecoder parses single frame exactly", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ id: 101, method: "ping" });
    const frames = decoder.feed(frame);
    assert.equal(frames.length, 1);
    assert.deepEqual(JSON.parse(frames[0].toString("utf8")), { id: 101, method: "ping" });
  });

  await t.test("T007: FrameDecoder returns empty array on empty Buffer feed", () => {
    const decoder = new FrameDecoder();
    const frames = decoder.feed(Buffer.alloc(0));
    assert.equal(frames.length, 0);
  });

  await t.test("T008: FrameDecoder returns empty array on null/undefined feed", () => {
    const decoder = new FrameDecoder();
    assert.equal(decoder.feed(null).length, 0);
    assert.equal(decoder.feed(undefined).length, 0);
  });

  await t.test("T009: FrameDecoder internal buffer resets cleanly upon calling reset()", () => {
    const decoder = new FrameDecoder();
    decoder.feed(Buffer.from([0, 0, 0])); // 3 bytes, partial header
    assert.equal(decoder.buffer.length, 3);
    decoder.reset();
    assert.equal(decoder.buffer.length, 0);
  });

  await t.test("T010: FrameDecoder handles exact 4-byte header arrival with 0 payload buffered", () => {
    const decoder = new FrameDecoder();
    const payload = Buffer.from(JSON.stringify({ a: 1 }));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length, 0);
    const frames1 = decoder.feed(header);
    assert.equal(frames1.length, 0);
    assert.equal(decoder.buffer.length, 4);
    const frames2 = decoder.feed(payload);
    assert.equal(frames2.length, 1);
    assert.deepEqual(JSON.parse(frames2[0].toString("utf8")), { a: 1 });
  });

  await t.test("T011: FrameDecoder decodes large payload within 4MB bounds", () => {
    const decoder = new FrameDecoder();
    const bigContent = "A".repeat(100 * 1024); // 100KB
    const frame = encodeFrame({ big: bigContent });
    const frames = decoder.feed(frame);
    assert.equal(frames.length, 1);
    const parsed = JSON.parse(frames[0].toString("utf8"));
    assert.equal(parsed.big.length, 100 * 1024);
  });

  await t.test("T012: FrameDecoder handles exact max boundary size without throw", () => {
    const maxLimit = 1024;
    const decoder = new FrameDecoder(maxLimit);
    const payload = { text: "x".repeat(100) };
    const frame = encodeFrame(payload);
    assert.ok(frame.readUInt32BE(0) <= maxLimit);
    const res = decoder.feed(frame);
    assert.equal(res.length, 1);
  });

  // 13-20: Network Fragmentation & Partial Chunks (半包)
  await t.test("T013: Chunk split byte-by-byte (1 byte per feed)", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ chunk: "byte-by-byte" });
    let collected = [];
    for (let i = 0; i < frame.length; i++) {
      const res = decoder.feed(frame.subarray(i, i + 1));
      if (res.length > 0) collected.push(...res);
    }
    assert.equal(collected.length, 1);
    assert.deepEqual(JSON.parse(collected[0].toString("utf8")), { chunk: "byte-by-byte" });
  });

  await t.test("T014: Chunk split across header: 2 bytes then 2 bytes", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ split: "header" });
    const part1 = frame.subarray(0, 2);
    const part2 = frame.subarray(2);
    assert.equal(decoder.feed(part1).length, 0);
    const res2 = decoder.feed(part2);
    assert.equal(res2.length, 1);
    assert.deepEqual(JSON.parse(res2[0].toString("utf8")), { split: "header" });
  });

  await t.test("T015: Chunk split across header: 1 byte then 3 bytes", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ split: "header13" });
    assert.equal(decoder.feed(frame.subarray(0, 1)).length, 0);
    const res = decoder.feed(frame.subarray(1));
    assert.equal(res.length, 1);
    assert.deepEqual(JSON.parse(res[0].toString("utf8")), { split: "header13" });
  });

  await t.test("T016: Chunk split across header: 3 bytes then 1 byte", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ split: "header31" });
    assert.equal(decoder.feed(frame.subarray(0, 3)).length, 0);
    const res = decoder.feed(frame.subarray(3));
    assert.equal(res.length, 1);
    assert.deepEqual(JSON.parse(res[0].toString("utf8")), { split: "header31" });
  });

  await t.test("T017: Chunk split in body: header complete, body broken in halves", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ body: "halves-data-string" });
    const mid = Math.floor(frame.length / 2);
    assert.equal(decoder.feed(frame.subarray(0, mid)).length, 0);
    const res = decoder.feed(frame.subarray(mid));
    assert.equal(res.length, 1);
    assert.deepEqual(JSON.parse(res[0].toString("utf8")), { body: "halves-data-string" });
  });

  await t.test("T018: Chunk split with 1 byte left at the end of body", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ body: "almost-there" });
    assert.equal(decoder.feed(frame.subarray(0, frame.length - 1)).length, 0);
    const res = decoder.feed(frame.subarray(frame.length - 1));
    assert.equal(res.length, 1);
    assert.deepEqual(JSON.parse(res[0].toString("utf8")), { body: "almost-there" });
  });

  await t.test("T019: Chunk split where body arrives in 5 random size slices", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ test: "random-slices-simulation" });
    const sliceSizes = [3, 7, 10, 5];
    let offset = 0;
    const collected = [];
    for (const size of sliceSizes) {
      if (offset + size <= frame.length) {
        collected.push(...decoder.feed(frame.subarray(offset, offset + size)));
        offset += size;
      }
    }
    if (offset < frame.length) {
      collected.push(...decoder.feed(frame.subarray(offset)));
    }
    assert.equal(collected.length, 1);
    assert.deepEqual(JSON.parse(collected[0].toString("utf8")), { test: "random-slices-simulation" });
  });

  await t.test("T020: Decoder leaves buffer clean (0 bytes) after complete frame consumed", () => {
    const decoder = new FrameDecoder();
    const frame = encodeFrame({ clean: true });
    decoder.feed(frame);
    assert.equal(decoder.buffer.length, 0);
  });

  // 21-25: Sticky Packets & Multiple Concatenated Frames (粘包)
  await t.test("T021: Two frames concatenated in single chunk", () => {
    const decoder = new FrameDecoder();
    const f1 = encodeFrame({ id: 1 });
    const f2 = encodeFrame({ id: 2 });
    const combined = Buffer.concat([f1, f2]);
    const res = decoder.feed(combined);
    assert.equal(res.length, 2);
    assert.equal(JSON.parse(res[0].toString("utf8")).id, 1);
    assert.equal(JSON.parse(res[1].toString("utf8")).id, 2);
  });

  await t.test("T022: Three frames concatenated in single chunk", () => {
    const decoder = new FrameDecoder();
    const f1 = encodeFrame({ n: 1 });
    const f2 = encodeFrame({ n: 2 });
    const f3 = encodeFrame({ n: 3 });
    const combined = Buffer.concat([f1, f2, f3]);
    const res = decoder.feed(combined);
    assert.equal(res.length, 3);
    assert.deepEqual(res.map(r => JSON.parse(r.toString("utf8")).n), [1, 2, 3]);
  });

  await t.test("T023: Two frames where second frame is truncated into next chunk", () => {
    const decoder = new FrameDecoder();
    const f1 = encodeFrame({ frame: 1 });
    const f2 = encodeFrame({ frame: 2 });
    const combined = Buffer.concat([f1, f2]);
    const cut = f1.length + 5;
    const res1 = decoder.feed(combined.subarray(0, cut));
    assert.equal(res1.length, 1);
    assert.equal(JSON.parse(res1[0].toString("utf8")).frame, 1);
    const res2 = decoder.feed(combined.subarray(cut));
    assert.equal(res2.length, 1);
    assert.equal(JSON.parse(res2[0].toString("utf8")).frame, 2);
  });

  await t.test("T024: Multiple frames with varied sizes (small, medium, tiny)", () => {
    const decoder = new FrameDecoder();
    const frames = [
      encodeFrame({ s: "a" }),
      encodeFrame({ s: "b".repeat(500) }),
      encodeFrame({ s: "c" })
    ];
    const combined = Buffer.concat(frames);
    const res = decoder.feed(combined);
    assert.equal(res.length, 3);
    assert.equal(JSON.parse(res[1].toString("utf8")).s.length, 500);
  });

  await t.test("T025: Five frames arriving together across 2 uneven batches", () => {
    const decoder = new FrameDecoder();
    const frames = [1, 2, 3, 4, 5].map(i => encodeFrame({ num: i }));
    const combined = Buffer.concat(frames);
    const partA = combined.subarray(0, Math.floor(combined.length * 0.7));
    const partB = combined.subarray(Math.floor(combined.length * 0.7));
    const resA = decoder.feed(partA);
    const resB = decoder.feed(partB);
    const all = [...resA, ...resB];
    assert.equal(all.length, 5);
    assert.deepEqual(all.map(b => JSON.parse(b.toString("utf8")).num), [1, 2, 3, 4, 5]);
  });

  // 26-30: Invalid Frame Length Protection & Safety Bounds
  await t.test("T026: Rejects 0-byte frame length with AMR_FRAME_SIZE_INVALID", () => {
    const decoder = new FrameDecoder();
    const zeroHeader = Buffer.alloc(4);
    zeroHeader.writeUInt32BE(0, 0);
    assert.throws(() => decoder.feed(zeroHeader), {
      message: /AMR_FRAME_SIZE_INVALID: frame length 0 exceeds bounds/
    });
  });

  await t.test("T027: Rejects frame length exceeding default MAX_FRAME_SIZE (4MB + 1 byte)", () => {
    const decoder = new FrameDecoder();
    const badHeader = Buffer.alloc(4);
    badHeader.writeUInt32BE(4 * 1024 * 1024 + 1, 0);
    assert.throws(() => decoder.feed(badHeader), {
      message: /AMR_FRAME_SIZE_INVALID/
    });
  });

  await t.test("T028: Rejects frame length exceeding custom maxFrameSize", () => {
    const decoder = new FrameDecoder(500);
    const badHeader = Buffer.alloc(4);
    badHeader.writeUInt32BE(501, 0);
    assert.throws(() => decoder.feed(badHeader), {
      message: /exceeds bounds \(0\.\.500\)/
    });
  });

  await t.test("T029: Decoder buffer is immediately cleared upon throw to avoid poisoning next feed", () => {
    const decoder = new FrameDecoder();
    const badHeader = Buffer.alloc(4);
    badHeader.writeUInt32BE(0, 0);
    try { decoder.feed(badHeader); } catch (_) {}
    assert.equal(decoder.buffer.length, 0);
    const validFrame = encodeFrame({ recovered: true });
    const res = decoder.feed(validFrame);
    assert.equal(res.length, 1);
    assert.equal(JSON.parse(res[0].toString("utf8")).recovered, true);
  });

  await t.test("T030: Rejects oversized frame when split across 2 chunks", () => {
    const decoder = new FrameDecoder();
    const headerPart1 = Buffer.from([0, 128]); // 0x0080....
    const headerPart2 = Buffer.from([0, 1]);   // = 8,388,609 > 4MB
    assert.equal(decoder.feed(headerPart1).length, 0);
    assert.throws(() => decoder.feed(headerPart2), {
      message: /AMR_FRAME_SIZE_INVALID/
    });
  });
});

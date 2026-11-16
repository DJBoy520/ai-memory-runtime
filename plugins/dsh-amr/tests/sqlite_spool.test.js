import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { SQLiteSpool, importLegacySpool } from "../lib/sqlite_spool.js";
import { IngestSpool } from "../lib/ingest_spool.js";

function withDir(fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dsh-sqlite-"));
  try { return fn(dir); } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}
const payload = (id) => ({ session_id: "s", messages: [{ message_id: id, role: "user", content: id }] });

test("two spool instances preserve each other's pending records", () => withDir((dir) => {
  const a = new SQLiteSpool({ persistenceDir: dir });
  const b = new SQLiteSpool({ persistenceDir: dir });
  try {
    const ra = a.enqueue(payload("a"));
    const rb = b.enqueue(payload("b"));
    assert.equal(a.remove(ra.key, ra.generation), true);
    assert.deepEqual(b.pending().map((r) => r.key), [rb.key]);
  } finally { a.close(); b.close(); }
}));

test("capacity is enforced across instances and duplicate keys remain stable", () => withDir((dir) => {
  const a = new SQLiteSpool({ persistenceDir: dir, maxRecords: 1 });
  const b = new SQLiteSpool({ persistenceDir: dir, maxRecords: 1 });
  try {
    const record = a.enqueue(payload("a"));
    assert.equal(b.enqueue(payload("a")).generation, record.generation);
    assert.equal(b.enqueue(payload("b")), null);
    assert.equal(b.pending().length, 1);
  } finally { a.close(); b.close(); }
}));

test("stale acknowledgement cannot delete a new generation", () => withDir((dir) => {
  const a = new SQLiteSpool({ persistenceDir: dir });
  const b = new SQLiteSpool({ persistenceDir: dir });
  try {
    const record = a.enqueue(payload("a"));
    const key = record.key;
    const old = record.generation;
    assert.equal(b.remove(key, old), true);
    b.enqueue(payload("a"));
    assert.notEqual(b.get(key).generation, old);
    assert.equal(a.remove(key, old), false);
    assert.ok(b.get(key));
  } finally { a.close(); b.close(); }
}));

test("existing legacy JSONL blocks SQLite until offline import", () => withDir((dir) => {
  fs.writeFileSync(path.join(dir, "ingest-spool.jsonl"), "legacy\n");
  assert.throws(() => new SQLiteSpool({ persistenceDir: dir }), /legacy.*import/i);
}));

test("JSONL backend refuses a directory with a SQLite spool", () => withDir((dir) => {
  const sqlite = new SQLiteSpool({ persistenceDir: dir });
  const record = sqlite.enqueue(payload("a"));
  const legacy = new IngestSpool({ persistenceDir: dir });
  try {
    assert.equal(legacy.enqueue(payload("b")), null);
    assert.equal(legacy.pending().length, 0);
    assert.ok(sqlite.get(record.key));
  } finally { sqlite.close(); }
}));

test("offline import is idempotent, preserves legacy and detects later changes", () => withDir((dir) => {
  const file = path.join(dir, "ingest-spool.jsonl");
  const body = JSON.stringify({ key: "s:a:a", payload: payload("a"), permanent: true }) + "\n";
  fs.writeFileSync(file, body);
  assert.equal(importLegacySpool({ persistenceDir: dir }).imported, 1);
  assert.equal(importLegacySpool({ persistenceDir: dir }).imported, 0);
  const spool = new SQLiteSpool({ persistenceDir: dir });
  assert.equal(spool.pending()[0].permanent, true);
  spool.close();
  assert.equal(fs.readFileSync(file, "utf8"), body);
  fs.appendFileSync(file, " ");
  assert.throws(() => new SQLiteSpool({ persistenceDir: dir }), /changed/);
  assert.throws(() => importLegacySpool({ persistenceDir: dir }), /changed/);
}));

test("corrupt or oversized legacy import fails atomically", () => withDir((dir) => {
  const file = path.join(dir, "ingest-spool.jsonl");
  fs.writeFileSync(file, JSON.stringify({ payload: payload("a") }) + "\n{broken\n");
  assert.throws(() => importLegacySpool({ persistenceDir: dir }), /invalid legacy/);
  fs.writeFileSync(file, ["a", "b"].map((id) => JSON.stringify({ payload: payload(id) })).join("\n"));
  assert.throws(() => importLegacySpool({ persistenceDir: dir, maxRecords: 1 }), /capacity/);
  const spool = new SQLiteSpool({ persistenceDir: dir, _offlineImport: true });
  assert.equal(spool.pending().length, 0);
  spool.close();
}));

test("legacy import rejects invalid UTF-8 without modifying source", () => withDir((dir) => {
  const file = path.join(dir, "ingest-spool.jsonl");
  const bytes = Buffer.concat([Buffer.from('{"payload":{"session_id":"s","messages":[{"content":"'), Buffer.from([0xff]), Buffer.from('"}]}}\n')]);
  fs.writeFileSync(file, bytes);
  assert.throws(() => importLegacySpool({ persistenceDir: dir }), /UTF-8/);
  assert.deepEqual(fs.readFileSync(file), bytes);
}));

test("SQLite spool rejects insecure existing database permissions", () => withDir((dir) => {
  const file = path.join(dir, "ingest-spool.sqlite3");
  fs.writeFileSync(file, "", { mode: 0o644 });
  fs.chmodSync(file, 0o644);
  assert.throws(() => new SQLiteSpool({ persistenceDir: dir }), /permissions/);
}));

test("offline import CLI requires an explicit stopped-writers assertion", () => withDir((dir) => {
  fs.writeFileSync(path.join(dir, "ingest-spool.jsonl"), JSON.stringify({ payload: payload("a") }) + "\n");
  const cli = new URL("../migrate-spool.js", import.meta.url).pathname;
  const denied = spawnSync(process.execPath, ["--no-warnings", cli, "--spool-dir", dir], { encoding: "utf8" });
  assert.notEqual(denied.status, 0);
  assert.equal(fs.existsSync(path.join(dir, "ingest-spool.sqlite3")), false);
  const done = spawnSync(process.execPath, ["--no-warnings", cli, "--spool-dir", dir, "--writers-stopped"], { encoding: "utf8" });
  assert.equal(done.status, 0, done.stderr);
  assert.equal(JSON.parse(done.stdout).imported, 1);
}));

test("different content with the same message ID is independently retained", () => withDir((dir) => {
  const spool = new SQLiteSpool({ persistenceDir: dir });
  try {
    assert.ok(spool.enqueue(payload("a")));
    const conflict = payload("a");
    conflict.messages[0].content = "changed";
    assert.ok(spool.enqueue(conflict));
    assert.equal(spool.pending().length, 2);
    assert.equal(spool.pending()[0].payload.messages[0].content, "a");
  } finally { spool.close(); }
}));

test("legacy SQLite permanent record is not revived by a new key", () => withDir((dir) => {
  const spool = new SQLiteSpool({ persistenceDir: dir });
  try {
    const p = payload("a");
    const old = { key: "s:a:a", generation: "old-generation", payload: p, permanent: true };
    const text = JSON.stringify(old);
    spool.db.prepare("INSERT INTO records(key,generation,record,bytes) VALUES(?,?,?,?)").run(old.key, old.generation, text, Buffer.byteLength(text));
    assert.equal(spool.enqueue(p).permanent, true);
    assert.equal(spool.pending().length, 1);
  } finally { spool.close(); }
}));

test("SQLite startup rejects duplicate payloads with conflicting permanent status", () => withDir((dir) => {
  const s = new SQLiteSpool({ persistenceDir: dir });
  const p = payload("a");
  const current = s.enqueue(p);
  const old = { key: "s:a:a", generation: "old", payload: p, permanent: true };
  const text = JSON.stringify(old);
  s.db.prepare("INSERT INTO records(key,generation,record,bytes) VALUES(?,?,?,?)").run(old.key, old.generation, text, Buffer.byteLength(text));
  assert.ok(current);
  s.close();
  assert.throws(() => new SQLiteSpool({ persistenceDir: dir }), /conflicting.*status/);
}));

test("separate source revisions of the same snapshot remain independently queued", () => withDir((dir) => {
  const spool = new SQLiteSpool({ persistenceDir: dir });
  try {
    const older = payload("a"); older.messages[0].source_revision = 10;
    const newer = payload("a"); newer.messages[0].source_revision = 20;
    newer.messages[0].content = "new revision";
    assert.ok(spool.enqueue(older));
    assert.ok(spool.enqueue(newer));
    assert.equal(spool.pending().length, 2);
  } finally { spool.close(); }
}));

test("permanent marker is stored even when payload quota is full", () => withDir((dir) => {
  const spool = new SQLiteSpool({ persistenceDir: dir, maxRecords: 1, maxBytes: 512 });
  try {
    const record = spool.enqueue(payload("a"));
    assert.ok(record);
    const marked = spool.markPermanent(record.key, { code: -32012, message: "x".repeat(1000) }, record.generation);
    assert.equal(marked?.permanent, true);
    assert.equal(spool.get(record.key).permanent, true);
  } finally { spool.close(); }
}));

test("cleanup and permanent-marker failures signal retry rather than dropping records", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dsh-sqlite-retry-"));
  const spool = new SQLiteSpool({ persistenceDir: dir });
  try {
    spool.enqueue(payload("a"));
    const remove = spool.remove.bind(spool);
    spool.remove = () => { throw new Error("database is locked"); };
    const first = await spool.replay({ request: async () => ({}) });
    assert.equal(first.transient, 1);
    assert.equal(first.delivered, 0);
    assert.equal(spool.pending().length, 1);
    spool.remove = remove;
    assert.equal((await spool.replay({ request: async () => ({}) })).delivered, 1);
    spool.enqueue(payload("b"));
    const mark = spool.markPermanent.bind(spool);
    spool.markPermanent = () => null;
    const fail = { request: async () => { throw Object.assign(new Error("unauthorized"), { code: -32012 }); } };
    assert.equal((await spool.replay(fail)).transient, 1);
    spool.markPermanent = mark;
    await spool.replay(fail);
    assert.equal(spool.pending()[0].permanent, true);
  } finally { spool.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

test("replay skips a generation replaced before dispatch", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dsh-sqlite-replay-aba-"));
  const a = new SQLiteSpool({ persistenceDir: dir });
  const b = new SQLiteSpool({ persistenceDir: dir });
  try {
    a.enqueue(payload("first"));
    const old = a.enqueue(payload("second"));
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const sent = [];
    const replay = a.replay({ request: async (_method, value) => {
      sent.push(value.messages[0].content);
      if (sent.length === 1) await gate;
    } });
    assert.equal(b.remove(old.key, old.generation), true);
    const newer = payload("second");
    const newRecord = b.enqueue(newer);
    release();
    const summary = await replay;
    assert.deepEqual(sent, ["first"]);
    assert.equal(summary.skippedRecords, 1);
    assert.equal(a.get(old.key).generation, newRecord.generation);
  } finally { a.close(); b.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

test("replay schedules another sweep when the in-flight generation was replaced", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dsh-sqlite-inflight-"));
  const a = new SQLiteSpool({ persistenceDir: dir });
  const b = new SQLiteSpool({ persistenceDir: dir });
  try {
    const first = a.enqueue(payload("a"));
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const replay = a.replay({ request: async () => { await gate; } });
    assert.equal(b.remove(first.key, first.generation), true);
    b.enqueue(payload("a"));
    release();
    const result = await replay;
    assert.equal(a.pending().length, 1);
    assert.ok(result.transient > 0, "replacement needs another replay sweep");
  } finally { a.close(); b.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

const workerModule = new URL("../lib/sqlite_spool.js", import.meta.url).href;
function runWorker(code, dir, label) {
  const child = spawn(process.execPath, ["--no-warnings", "--input-type=module", "-e", code, dir, label], { stdio: ["ignore", "pipe", "pipe"] });
  let out = "", err = "";
  child.stdout.on("data", (chunk) => { out += chunk; });
  child.stderr.on("data", (chunk) => { err += chunk; });
  return { child, done: new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", (code, signal) => code === 0 ? resolve(out) : reject(new Error(`worker ${code || signal}: ${err}`)));
  }) };
}

test("separate processes enqueue and acknowledge without losing other records", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dsh-sqlite-process-"));
  const code = `import { SQLiteSpool } from ${JSON.stringify(workerModule)};
    const [dir, label] = process.argv.slice(1); const s = new SQLiteSpool({persistenceDir:dir});
    for (let i=0; i<20; i++) { const id=label+i; let r;
      for(let n=0; n<100 && !r; n++) r=s.enqueue({session_id:"s",messages:[{message_id:id,content:id}]});
      if(!r) process.exit(2); if(i%2===0 && !s.remove(r.key,r.generation)) process.exit(3);
    } s.close();`;
  try {
    await Promise.all([runWorker(code, dir, "A").done, runWorker(code, dir, "B").done]);
    const spool = new SQLiteSpool({ persistenceDir: dir });
    try { assert.equal(spool.pending().length, 20); }
    finally { spool.close(); }
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("concurrent processes cannot exceed record capacity", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dsh-sqlite-capacity-"));
  const seed = new SQLiteSpool({ persistenceDir: dir, maxRecords: 1 });
  seed.close();
  const code = `import { SQLiteSpool } from ${JSON.stringify(workerModule)};
    const [dir,label]=process.argv.slice(1); const logger={warn(){}};
    const s=new SQLiteSpool({persistenceDir:dir,maxRecords:1,logger});
    const r=s.enqueue({session_id:'s',messages:[{message_id:label,content:label}]});
    process.stdout.write(r ? 'accepted' : 'rejected'); s.close();`;
  try {
    const results = await Promise.all([runWorker(code, dir, "A").done, runWorker(code, dir, "B").done]);
    assert.deepEqual(results.sort(), ["accepted", "rejected"]);
    const spool = new SQLiteSpool({ persistenceDir: dir, maxRecords: 1 });
    try { assert.equal(spool.pending().length, 1); } finally { spool.close(); }
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("killed transaction rolls back and releases the database lock", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dsh-sqlite-crash-"));
  const seed = new SQLiteSpool({ persistenceDir: dir });
  seed.enqueue(payload("committed"));
  seed.close();
  const code = `import { DatabaseSync } from 'node:sqlite'; import path from 'node:path';
    const db=new DatabaseSync(path.join(process.argv[1], 'ingest-spool.sqlite3'));
    db.exec('BEGIN IMMEDIATE; DELETE FROM records'); process.stdout.write('locked\\n'); setInterval(()=>{},1000);`;
  const worker = runWorker(code, dir, "");
  // SIGKILL is the expected exit for this worker.
  worker.done.catch(() => {});
  try {
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("worker did not lock database")), 3000);
      worker.child.stdout.once("data", () => { clearTimeout(timeout); resolve(); });
      worker.child.once("error", reject);
    });
    worker.child.kill("SIGKILL");
    await worker.done.catch(() => {});
    const spool = new SQLiteSpool({ persistenceDir: dir });
    try { assert.equal(spool.pending().length, 1); assert.ok(spool.enqueue(payload("after"))); }
    finally { spool.close(); }
  } finally { worker.child.kill("SIGKILL"); fs.rmSync(dir, { recursive: true, force: true }); }
});

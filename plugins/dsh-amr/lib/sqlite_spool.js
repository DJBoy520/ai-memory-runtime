/** Transactional, local-filesystem spool. Legacy JSONL is imported offline. */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { createRequire } from "node:module";
import { IngestSpool, computeRecordKey, computeLegacyRecordKey } from "./ingest_spool.js";

const require = createRequire(import.meta.url);
const DB_NAME = "ingest-spool.sqlite3";

export class SQLiteSpool extends IngestSpool {
  constructor(options = {}) {
    super(options);
    let DatabaseSync;
    try { ({ DatabaseSync } = require("node:sqlite")); }
    catch { throw new Error("SQLite spool requires Node.js with node:sqlite support"); }
    fs.mkdirSync(this.dir, { recursive: true, mode: 0o700 });
    const directory = fs.lstatSync(this.dir);
    if (!directory.isDirectory() || (directory.mode & 0o077) ||
        (typeof process.getuid === "function" && directory.uid !== process.getuid())) {
      throw new Error("spool directory ownership or permissions are insecure");
    }
    this.filePath = path.join(this.dir, DB_NAME);
    const flags = fs.constants.O_CREAT | fs.constants.O_RDWR | (fs.constants.O_NOFOLLOW || 0);
    const fd = fs.openSync(this.filePath, flags, 0o600);
    try {
      const file = fs.fstatSync(fd);
      if (!file.isFile() || (file.mode & 0o077) ||
          (typeof process.getuid === "function" && file.uid !== process.getuid())) {
        throw new Error("spool database ownership or permissions are insecure");
      }
    } finally { fs.closeSync(fd); }
    this.db = new DatabaseSync(this.filePath);
    try {
      this.db.exec("PRAGMA busy_timeout=50; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;");
      this.db.exec(`CREATE TABLE IF NOT EXISTS records (
        key TEXT PRIMARY KEY, generation TEXT NOT NULL, record TEXT NOT NULL, bytes INTEGER NOT NULL
      ); CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);`);
      if (!options._offlineImport) this._checkLegacy();
      this._checkRecordConsistency();
    } catch (err) { this.db.close(); throw err; }
  }

  _checkRecordConsistency() {
    const identities = new Map();
    for (const row of this.db.prepare("SELECT record FROM records").all()) {
      const record = JSON.parse(row.record);
      if (!record?.payload || typeof record.payload.session_id !== "string") throw new Error("invalid SQLite spool record");
      const identity = computeRecordKey(record.payload);
      const status = !!record.permanent;
      if (identities.has(identity) && identities.get(identity) !== status) {
        throw new Error("conflicting SQLite spool record status; manual repair required");
      }
      identities.set(identity, status);
    }
  }

  _checkLegacy() {
    const legacy = path.join(this.dir, "ingest-spool.jsonl");
    if (!fs.existsSync(legacy)) return;
    const digest = crypto.createHash("sha256").update(fs.readFileSync(legacy)).digest("hex");
    const marker = this.db.prepare("SELECT value FROM metadata WHERE key='legacy_sha256'").get();
    if (!marker || marker.value !== digest) {
      throw new Error("legacy JSONL requires offline import or changed after import");
    }
  }

  _transaction(fn) {
    this.db.exec("BEGIN IMMEDIATE");
    try { const value = fn(); this.db.exec("COMMIT"); return value; }
    catch (err) { try { this.db.exec("ROLLBACK"); } catch {} throw err; }
  }

  _insert(record) {
    const text = JSON.stringify(record);
    const bytes = Buffer.byteLength(text, "utf8") + 1;
    const totals = this.db.prepare("SELECT COUNT(*) AS count, COALESCE(SUM(bytes),0) AS bytes FROM records").get();
    if (totals.count >= this.maxRecords || totals.bytes + bytes > this.maxBytes) throw new Error("spool capacity exceeded");
    this.db.prepare("INSERT INTO records(key,generation,record,bytes) VALUES(?,?,?,?)")
      .run(record.key, record.generation, text, bytes);
  }

  enqueue(payload) {
    this.lastEnqueueRetryable = false;
    if (!payload || typeof payload.session_id !== "string") return null;
    try {
      return this._transaction(() => {
        const key = computeRecordKey(payload);
        let row = this.db.prepare("SELECT record FROM records WHERE key=?").get(key);
        if (!row) {
          const legacy = this.db.prepare("SELECT record FROM records WHERE key=?").get(computeLegacyRecordKey(payload));
          if (legacy && computeRecordKey(JSON.parse(legacy.record).payload) === key) row = legacy;
        }
        if (row) {
          const old = JSON.parse(row.record);
          const identity = (p) => JSON.stringify({ ...p, messages: p.messages?.map(({ timestamp, ...m }) => m) });
          if (identity(old.payload) !== identity(payload)) throw new Error("spool key conflicts with existing payload");
          return old;
        }
        const record = { key, generation: crypto.randomUUID(), payload, enqueuedAt: Date.now() };
        this._insert(record);
        return record;
      });
    } catch (err) {
      this.lastEnqueueRetryable = [5, 6].includes((err.errcode ?? -1) & 0xff);
      this.logger.warn(`[sqlite_spool] enqueue failed: ${err.message}`);
      return null;
    }
  }

  pending() {
    return this.db.prepare("SELECT record FROM records ORDER BY rowid").all().map((r) => JSON.parse(r.record));
  }
  get(key) {
    const row = this.db.prepare("SELECT record FROM records WHERE key=?").get(key);
    return row ? JSON.parse(row.record) : undefined;
  }
  remove(key, generation) {
    if (!generation) return false;
    try {
      return this.db.prepare("DELETE FROM records WHERE key=? AND generation=?").run(key, generation).changes > 0;
    } catch (err) {
      // Keep the base-class contract: a failed delete leaves the record for the
      // next replay instead of unmasking a raw SQLITE_BUSY to the caller.
      this.logger.warn(`[sqlite_spool] remove failed; record retained: ${err.message}`);
      return false;
    }
  }
  markPermanent(key, err, generation) {
    if (!generation) return null;
    try {
      return this._transaction(() => {
        const row = this.db.prepare("SELECT record FROM records WHERE key=? AND generation=?").get(key, generation);
        if (!row) return null;
        const record = JSON.parse(row.record);
        if (record.permanent) return record;
        record.permanent = true;
        record.attempts = (Number(record.attempts) || 0) + 1;
        record.lastError = { code: err?.code ?? null, message: String(err?.message || "unknown error").slice(0, 512) };
        record.failedAt = Date.now();
        const text = JSON.stringify(record);
        // `bytes` accounts for the enqueued payload; bounded failure metadata
        // must remain writable when that quota is full.
        this.db.prepare("UPDATE records SET record=? WHERE key=? AND generation=?").run(text, key, generation);
        return record;
      });
    } catch (error) { this.logger.warn(`[sqlite_spool] permanent marker failed: ${error.message}`); return null; }
  }
  close() { this.db.close(); }
}

/** Offline only: stop every old and new writer before invoking this function. */
export function importLegacySpool(options = {}) {
  const spool = new SQLiteSpool({ ...options, _offlineImport: true });
  try {
    const file = path.join(spool.dir, "ingest-spool.jsonl");
    const source = fs.readFileSync(file);
    const digest = crypto.createHash("sha256").update(source).digest("hex");
    const records = [];
    const seen = new Set();
    let text;
    try { text = new TextDecoder("utf-8", { fatal: true }).decode(source); }
    catch { throw new Error("legacy JSONL is not valid UTF-8"); }
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      let record;
      try { record = JSON.parse(line); } catch { throw new Error("invalid legacy JSONL record"); }
      if (!record || !record.payload || typeof record.payload.session_id !== "string" || !Array.isArray(record.payload.messages)) {
        throw new Error("invalid legacy JSONL record");
      }
      const key = computeRecordKey(record.payload);
      if (record.key && record.key !== key && record.key !== computeLegacyRecordKey(record.payload)) throw new Error("invalid legacy record key");
      if (seen.has(key)) throw new Error("invalid legacy duplicate record key");
      seen.add(key);
      records.push({ ...record, key, generation: crypto.randomUUID() });
    }
    return spool._transaction(() => {
      const marker = spool.db.prepare("SELECT value FROM metadata WHERE key='legacy_sha256'").get();
      if (marker) {
        if (marker.value !== digest) throw new Error("legacy JSONL changed after import");
        return { imported: 0 };
      }
      const count = spool.db.prepare("SELECT COUNT(*) AS count FROM records").get().count;
      if (count) throw new Error("SQLite spool must be empty before import");
      for (const record of records) spool._insert(record);
      spool.db.prepare("INSERT INTO metadata(key,value) VALUES('legacy_sha256',?)").run(digest);
      return { imported: records.length };
    });
  } finally { spool.close(); }
}

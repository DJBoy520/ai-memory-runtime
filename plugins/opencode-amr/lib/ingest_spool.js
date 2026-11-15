/**
 * lib/ingest_spool.js
 * Durable JSONL spool for the OpenCode AMR 2.x `session.ingest` adapter.
 *
 * Design (DOC-AMR-09 §4):
 * - Persist-first: a payload is appended to the spool before the request is
 *   attempted, and removed only after the adapter acknowledges it.
 * - Transient failures (-32010 / ETIMEDOUT / connection errors) stay pending
 *   and are replayed later; permanent failures are marked once and retained
 *   for manual handling (never retried forever).
 * - Replay is non-blocking, serial and re-entrancy guarded.
 * - Bounded capacity: max 1000 records and max 8MB on disk. Over-capacity
 *   drops the oldest records with a warning instead of growing without bound.
 * - Every filesystem operation is wrapped: IO errors are logged and never
 *   crash the host process.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { logger as defaultLogger } from "./logger.js";
import { isTransientError } from "./uds_client.js";

export const MAX_SPOOL_BYTES = 8 * 1024 * 1024;
export const MAX_SPOOL_RECORDS = 1000;
export const SPOOL_FILE_NAME = "ingest-spool.jsonl";

/**
 * Resolve the spool directory.
 * Priority: explicit `persistenceDir` -> `AMR_OPENCODE_SPOOL_DIR` ->
 * ~/.local/share/amr-spool/opencode.
 *
 * @param {string} [configuredDir]
 * @returns {string}
 */
export function resolveSpoolDir(configuredDir) {
  if (configuredDir && typeof configuredDir === "string" && configuredDir.trim().length > 0) {
    return path.resolve(configuredDir.trim());
  }
  const envDir = process.env.AMR_OPENCODE_SPOOL_DIR;
  if (envDir && envDir.trim().length > 0) {
    return path.resolve(envDir.trim());
  }
  const home = os.homedir();
  if (home && home.trim().length > 0) {
    return path.join(home, ".local", "share", "amr-spool", "opencode");
  }
  const uid = typeof process.getuid === "function" ? process.getuid() : "nouser";
  return path.join(os.tmpdir(), `amr-spool-opencode-${uid}`);
}

/**
 * Stable record key: `session_id:first_message_id:last_message_id`.
 * Identical payloads map to the same key, so re-enqueue is a no-op; server-side
 * `(session_id, message_id)` primary key dedupes replayed messages.
 *
 * @param {object} payload session.ingest payload
 * @returns {string}
 */
export function computeRecordKey(payload) {
  const sessionId = payload && payload.session_id != null ? String(payload.session_id) : "";
  const messages = Array.isArray(payload?.messages) ? payload.messages : [];
  const first = messages.length > 0 && messages[0]?.message_id != null
    ? String(messages[0].message_id)
    : "";
  const last = messages.length > 0 && messages[messages.length - 1]?.message_id != null
    ? String(messages[messages.length - 1].message_id)
    : "";
  if (!first && !last) {
    // 没有 message_id 时不能退化成固定的 `sessionId::`，否则不同消息会
    // 被误判为同一条而丢弃。用内容哈希作为稳定 key。
    const digest = crypto
      .createHash("sha256")
      .update(JSON.stringify(payload?.messages ?? payload ?? {}))
      .digest("hex")
      .slice(0, 16);
    return `${sessionId}::h:${digest}`;
  }
  return `${sessionId}:${first}:${last}`;
}

/**
 * @typedef {object} SpoolRecord
 * @property {string} key
 * @property {object} payload
 * @property {number} enqueuedAt
 * @property {number} [attempts]
 * @property {boolean} [permanent]
 * @property {{code?: any, message?: string}} [lastError]
 * @property {number} [failedAt]
 */

export class IngestSpool {
  /**
   * @param {object} [options]
   * @param {string} [options.persistenceDir] Override spool directory.
   * @param {number} [options.maxBytes=8388608]
   * @param {number} [options.maxRecords=1000]
   * @param {object} [options.logger]
   */
  constructor(options = {}) {
    this.dir = resolveSpoolDir(options.persistenceDir);
    this.filePath = path.join(this.dir, SPOOL_FILE_NAME);
    this.maxBytes = options.maxBytes || MAX_SPOOL_BYTES;
    this.maxRecords = options.maxRecords || MAX_SPOOL_RECORDS;
    this.logger = options.logger || defaultLogger;
    /** @type {SpoolRecord[]} */
    this._records = [];
    this._loaded = false;
    this._loadFailed = false;
    this._replaying = false;
  }

  /**
   * Load the JSONL file once. Malformed/partial trailing lines are skipped.
   * @private
   */
  _load() {
    if (this._loaded) return;
    this._loaded = true;

    let text;
    try {
      text = fs.readFileSync(this.filePath, "utf8");
    } catch (err) {
      if (err && err.code !== "ENOENT") {
        // 读取失败时不能把空的内存状态当作真实状态：否则后续 rewrite
        // 会覆盖掉未读出的历史记录。标记失败并让写路径拒绝操作。
        this._loadFailed = true;
        this.logger.warn(`[ingest_spool] unable to read spool ${this.filePath}: ${err.message}`);
      }
      return;
    }

    const seen = new Set();
    let malformed = 0;
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      try {
        const parsed = JSON.parse(line);
        if (!parsed || typeof parsed !== "object" || !parsed.payload || typeof parsed.payload !== "object") {
          malformed++;
          continue;
        }
        const key = typeof parsed.key === "string" && parsed.key.length > 0
          ? parsed.key
          : computeRecordKey(parsed.payload);
        if (seen.has(key)) continue; // de-duplicate repeated append after a crash
        seen.add(key);
        this._records.push({
          ...parsed,
          key,
        });
      } catch (_) {
        malformed++;
      }
    }

    if (malformed > 0) {
      this.logger.warn(`[ingest_spool] skipped ${malformed} malformed spool record(s) in ${this.filePath}`);
    }
  }

  /** @private */
  _ensureDir() {
    fs.mkdirSync(this.dir, { recursive: true, mode: 0o700 });
  }

  /** @private */
  _serialize(record) {
    return `${JSON.stringify(record)}\n`;
  }

  /**
   * Append one payload to the spool (persist-first). Duplicate keys are
   * idempotent and return the existing key without appending again.
   *
   * @param {object} payload session.ingest payload
   * @returns {string|null} record key, or null when the payload is invalid or
   *   the write failed. Never throws.
   */
  enqueue(payload) {
    if (!payload || typeof payload !== "object" || typeof payload.session_id !== "string") {
      this.logger.warn("[ingest_spool] refusing to enqueue payload without session_id");
      return null;
    }

    try {
      this._load();
    } catch (err) {
      this.logger.warn(`[ingest_spool] load failed before enqueue: ${err?.message || err}`);
    }
    if (this._loadFailed) {
      this.logger.warn("[ingest_spool] refusing to enqueue: spool could not be loaded");
      return null;
    }

    const key = computeRecordKey(payload);
    const existing = this._records.find((r) => r.key === key);
    if (existing) {
      return existing.key;
    }

    const record = {
      key,
      payload,
      enqueuedAt: Date.now(),
    };

    try {
      this._ensureDir();
      fs.appendFileSync(this.filePath, this._serialize(record), { encoding: "utf8", mode: 0o600 });
    } catch (err) {
      this.logger.warn(`[ingest_spool] append failed; record not persisted: ${err?.message || err}`);
      return null;
    }

    this._records.push(record);

    try {
      this._enforceCapacity();
    } catch (err) {
      this.logger.warn(`[ingest_spool] capacity enforcement failed: ${err?.message || err}`);
    }

    return key;
  }

  /**
   * Return a snapshot of all pending records (including permanently failed
   * records retained for manual handling). Never throws.
   *
   * @returns {SpoolRecord[]}
   */
  pending() {
    try {
      this._load();
    } catch (err) {
      this.logger.warn(`[ingest_spool] load failed in pending(): ${err?.message || err}`);
      return [];
    }
    return this._records.slice();
  }

  /**
   * Find one record by key.
   * @param {string} recordKey
   * @returns {SpoolRecord|undefined}
   */
  get(recordKey) {
    try {
      this._load();
    } catch (_) {
      return undefined;
    }
    return this._records.find((r) => r.key === recordKey);
  }

  /**
   * Remove one record by key and rewrite the spool. Never throws.
   *
   * @param {string} recordKey
   * @returns {boolean} whether a record was removed
   */
  remove(recordKey) {
    return this.removeMany([recordKey]) > 0;
  }

  /**
   * Remove several records in one rewrite.
   *
   * @param {string[]} keys
   * @returns {number} number removed
   */
  removeMany(keys) {
    if (!Array.isArray(keys) || keys.length === 0) return 0;
    const keySet = new Set(keys.map(String));
    try {
      this._load();
    } catch (err) {
      this.logger.warn(`[ingest_spool] load failed in remove(): ${err?.message || err}`);
      return 0;
    }
    if (this._loadFailed) return 0;

    const before = this._records.length;
    this._records = this._records.filter((r) => !keySet.has(r.key));
    const removed = before - this._records.length;
    if (removed > 0) {
      this._rewrite();
    }
    return removed;
  }

  /**
   * Mark a record as permanently failed, retaining it for manual handling.
   * Idempotent: an already-permanent record is not modified again.
   *
   * @param {string} recordKey
   * @param {any} err
   * @returns {SpoolRecord|null} the record, or null when unknown / not writable
   */
  markPermanent(recordKey, err) {
    try {
      this._load();
    } catch (loadErr) {
      this.logger.warn(`[ingest_spool] load failed in markPermanent: ${loadErr?.message || loadErr}`);
      return null;
    }

    if (this._loadFailed) return null;
    const record = this._records.find((r) => r.key === recordKey);
    if (!record) return null;
    if (record.permanent) return record;

    record.permanent = true;
    record.attempts = (Number(record.attempts) || 0) + 1;
    record.lastError = {
      code: err && err.code !== undefined ? err.code : null,
      message: String(err?.message || err || "unknown error"),
    };
    record.failedAt = Date.now();
    this._rewrite();
    return record;
  }

  /**
   * Replay pending (non-permanent) records in FIFO order.
   *
   * - Success -> remove from spool.
   * - Transient error -> leave pending and stop this sweep (adapter is down;
   *   avoids a deadline stall per record).
   * - Permanent error -> mark once, invoke `onPermanent(record, err)`, retain.
   *
   * Non-blocking for callers: callers must not await this on the ingestion
   * hot path. Re-entrant calls are skipped.
   *
   * @param {{request: Function}} client AmrUdsClient-like object
   * @param {object} [options]
   * @param {number} [options.deadlineMs=1500]
   * @param {(record: SpoolRecord, err: any) => void} [options.onPermanent]
   * @returns {Promise<{attempted: number, delivered: number, permanent: number, transient: number, skipped: number, skippedRecords: number}>}
   */
  async replay(client, options = {}) {
    const deadlineMs = options.deadlineMs !== undefined && options.deadlineMs !== null
      ? options.deadlineMs
      : 1500;
    const onPermanent = typeof options.onPermanent === "function" ? options.onPermanent : null;
    const skipKeys = options.skipKeys instanceof Set
      ? options.skipKeys
      : new Set(Array.isArray(options.skipKeys) ? options.skipKeys : []);

    const summary = {
      attempted: 0,
      delivered: 0,
      permanent: 0,
      transient: 0,
      skipped: 0,
      skippedRecords: 0,
    };

    if (this._replaying) {
      summary.skipped = 1;
      return summary;
    }
    if (!client || typeof client.request !== "function") {
      this.logger.warn("[ingest_spool] replay skipped: invalid client");
      summary.skipped = 1;
      return summary;
    }

    this._replaying = true;
    try {
      const all = this.pending();
      const records = all.filter(
        (r) => !r.permanent && !skipKeys.has(r.key)
      );
      summary.skippedRecords = all.length - records.length;

      for (const record of records) {
        summary.attempted++;
        try {
          await client.request("session.ingest", record.payload, deadlineMs);
          this.remove(record.key);
          summary.delivered++;
        } catch (err) {
          if (isTransientError(err)) {
            summary.transient++;
            // Adapter still unavailable: keep this and later records pending.
            break;
          }

          summary.permanent++;
          const updated = this.markPermanent(record.key, err);
          if (onPermanent) {
            try {
              onPermanent(updated || record, err);
            } catch (cbErr) {
              this.logger.warn(`[ingest_spool] onPermanent callback failed: ${cbErr?.message || cbErr}`);
            }
          }
        }
      }
    } catch (err) {
      this.logger.warn(`[ingest_spool] replay sweep failed open: ${err?.message || err}`);
    } finally {
      this._replaying = false;
    }

    return summary;
  }

  /**
   * Drop oldest records until both the record count and the on-disk size fit.
   * Records are trimmed in one batch to avoid O(n^2) rewrites.
   * @private
   */
  _enforceCapacity() {
    let fileBytes = 0;
    try {
      fileBytes = fs.statSync(this.filePath).size;
    } catch (err) {
      if (err && err.code !== "ENOENT") {
        this.logger.warn(`[ingest_spool] stat failed during capacity check: ${err.message}`);
      }
      fileBytes = 0;
    }

    const overRecords = this._records.length > this.maxRecords;
    const overBytes = fileBytes > this.maxBytes;
    if (!overRecords && !overBytes) return;

    let dropCount = 0;
    if (overRecords) {
      // Batch trim to 90% of the cap so we do not rewrite on every append.
      const target = Math.max(1, Math.floor(this.maxRecords * 0.9));
      dropCount = Math.max(0, this._records.length - target);
    }

    const sizes = this._records.map((r) => Buffer.byteLength(this._serialize(r), "utf8"));
    // 已经决定因记录数超限丢弃的前缀必须从总量中扣除，否则会额外多丢。
    let total = sizes.slice(dropCount).reduce((sum, n) => sum + n, 0);
    while (dropCount < this._records.length && total > this.maxBytes) {
      total -= sizes[dropCount];
      dropCount++;
    }

    if (dropCount <= 0) {
      // Oversized solely because of malformed/duplicate lines: compact the file
      // without dropping valid records.
      if (overBytes) this._rewrite();
      return;
    }

    const dropped = this._records.splice(0, dropCount);
    this._rewrite();
    this.logger.warn(
      `[ingest_spool] capacity exceeded (records=${this._records.length + dropped.length}, bytes=${fileBytes}); dropped ${dropped.length} oldest record(s)`
    );
  }

  /**
   * Atomically rewrite the spool from in-memory records.
   * @private
   */
  _rewrite() {
    if (this._loadFailed) {
      this.logger.warn("[ingest_spool] rewrite skipped: spool could not be loaded");
      return;
    }
    const tmpPath = `${this.filePath}.tmp-${process.pid}-${crypto.randomBytes(4).toString("hex")}`;
    try {
      this._ensureDir();
      const body = this._records.map((r) => this._serialize(r)).join("");
      fs.writeFileSync(tmpPath, body, { encoding: "utf8", mode: 0o600 });
      fs.renameSync(tmpPath, this.filePath);
    } catch (err) {
      this.logger.warn(`[ingest_spool] rewrite failed; spool may be stale: ${err?.message || err}`);
      try {
        fs.rmSync(tmpPath, { force: true });
      } catch (_) {}
    }
  }
}

export default IngestSpool;

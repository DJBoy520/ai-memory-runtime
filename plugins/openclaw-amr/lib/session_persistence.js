/**
 * lib/session_persistence.js
 * Persistent offline storage, atomic file operations, and idempotent replay engine for OpenClaw AMR.
 *
 * Capabilities:
 * 1. Persist-First Architecture: Saves session to disk before attempting network transport.
 * 2. Idempotent Replay: Supports deterministic replay for both success and failure cases without duplicating messages.
 * 3. Secure File Permissions: Directory enforced to 0700, session files enforced to 0600.
 * 4. Atomic Writes: Temp-file write + fsync/close + atomic rename prevents partial/corrupted reads.
 * 5. Concurrency Protection: In-memory async keyed mutex per session and global mutex for replay sweeps.
 * 6. Retention on Failure: Failed deliveries are retained on disk with retry count and error tracking.
 * 7. Path Traversal & Exposure Defense: Sanitized filename + sha256 hash prevents directory escape;
 *    zero message content printed in logs.
 * 8. Zero External Dependencies: Pure Node.js standard library (node:fs, node:path, node:os, node:crypto).
 */

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { logger as defaultLogger } from "./logger.js";
import { isPermanentError } from "./uds_client.js";
import { stripIdentityFields } from "./session_mapper.js";

/**
 * Standard Async FIFO Mutex for sequential execution.
 */
export class Mutex {
  constructor() {
    this._queue = Promise.resolve();
    this._active = 0;
    this._waiters = 0;
  }

  async runExclusive(fn) {
    let release;
    const next = new Promise((resolve) => {
      release = resolve;
    });
    const current = this._queue;
    this._queue = current.then(() => next);
    this._waiters += 1;
    await current;
    this._waiters -= 1;
    this._active += 1;
    try {
      return await fn();
    } finally {
      this._active -= 1;
      release();
    }
  }

  /** True when no task is running and none is waiting. */
  get idle() {
    return this._active === 0 && this._waiters === 0;
  }
}

/**
 * Keyed Mutex ensuring serialized operations per session key.
 */
export class KeyedMutex {
  constructor() {
    this._mutexes = new Map();
  }

  async runExclusive(key, fn) {
    let mutex = this._mutexes.get(key);
    if (!mutex) {
      mutex = new Mutex();
      this._mutexes.set(key, mutex);
    }
    try {
      return await mutex.runExclusive(fn);
    } finally {
      // 旧实现比较 `mutex._queue === Promise.resolve()` 恒为 false，
      // 导致每个 session 的锁永久驻留 Map；改用真实空闲状态。
      if (this._mutexes.get(key) === mutex && mutex.idle) {
        this._mutexes.delete(key);
      }
    }
  }
}

/**
 * Resolves persistent storage directory with sensible security fallbacks:
 * 1. Explicit configured path
 * 2. process.env.AMR_SESSION_SPOOL_DIR
 * 3. process.env.XDG_DATA_HOME/openclaw-amr/sessions
 * 4. ~/.local/share/openclaw-amr/sessions
 * 5. os.tmpdir()/openclaw-amr-sessions
 *
 * @param {string} [configuredDir]
 * @returns {string}
 */
export function resolveStorageDir(configuredDir) {
  if (configuredDir && typeof configuredDir === "string" && configuredDir.trim().length > 0) {
    return path.resolve(configuredDir.trim());
  }
  if (process.env.AMR_SESSION_SPOOL_DIR && process.env.AMR_SESSION_SPOOL_DIR.trim().length > 0) {
    return path.resolve(process.env.AMR_SESSION_SPOOL_DIR.trim());
  }
  if (process.env.XDG_DATA_HOME && process.env.XDG_DATA_HOME.trim().length > 0) {
    return path.join(process.env.XDG_DATA_HOME.trim(), "openclaw-amr", "sessions");
  }
  const home = os.homedir();
  if (home) {
    return path.join(home, ".local", "share", "openclaw-amr", "sessions");
  }
  const uid = typeof process.getuid === "function" ? process.getuid() : "nouser";
  return path.join(os.tmpdir(), `openclaw-amr-sessions-${uid}`);
}

/**
 * Sanitizes sessionId into a safe filename with deterministic SHA-256 hash.
 * Completely eliminates Path Traversal (e.g. "../", slashes, null bytes).
 *
 * @param {string} sessionId
 * @returns {string} Safe filename (e.g. "session_123_4a8b...json")
 */
export function getSafeFileName(sessionId) {
  const raw = String(sessionId || "session");
  const cleanPrefix = raw.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 48);
  const hash = crypto.createHash("sha256").update(raw).digest("hex").slice(0, 16);
  return `${cleanPrefix || "session"}_${hash}.json`;
}

/**
 * Computes a stable fingerprint of an ingest payload for permanent-failure
 * tracking. Timestamps are deliberately excluded so a re-delivered event with
 * the same message ids/content is recognised as the same record.
 *
 * @param {object} payload
 * @returns {string}
 */
export function computePayloadFingerprint(payload) {
  const messages = Array.isArray(payload?.messages) ? payload.messages : [];
  const stable = {
    session_id: payload?.session_id ?? null,
    project_id: payload?.project_id ?? null,
    messages: messages.map((m) => ({
      message_id: m?.message_id ?? null,
      role: m?.role ?? null,
      content: m?.content ?? null,
      sequence: m?.sequence ?? null,
    })),
  };
  return crypto.createHash("sha256").update(JSON.stringify(stable)).digest("hex");
}

/**
 * Performs atomic file write with strict 0600 mode permissions.
 * Writes to a unique temp file in the same directory, then atomically renames.
 *
 * @param {string} filePath Target destination file
 * @param {any} data JSON-serializable object or string
 */
export function writeRecordAtomic(filePath, data) {
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const rand = crypto.randomBytes(6).toString("hex");
  const tmpPath = path.join(dir, `.${base}.${process.pid}.${Date.now()}.${rand}.tmp`);
  const content = typeof data === "string" ? data : JSON.stringify(data, null, 2);

  try {
    fs.writeFileSync(tmpPath, content, { mode: 0o600 });
    if (process.platform !== "win32") {
      try {
        fs.chmodSync(tmpPath, 0o600);
      } catch (_) {}
    }
    fs.renameSync(tmpPath, filePath);
  } catch (err) {
    try {
      if (fs.existsSync(tmpPath)) {
        fs.unlinkSync(tmpPath);
      }
    } catch (_) {}
    throw err;
  }
}

/**
 * SessionPersistenceManager
 * Manages offline spooling, atomic saves, failure retention, and idempotent retries.
 */
export class SessionPersistenceManager {
  /**
   * @param {object} [options]
   * @param {string} [options.storageDir] Storage directory path
   * @param {object} [options.udsClient] AmrUdsClient instance for sending session.ingest
   * @param {number} [options.ingestDeadlineMs=1500] Request deadline timeout
   * @param {boolean} [options.autoReplayOnStartup=false] Whether to replay pending/failed sessions on startup
   * @param {number} [options.maxRetries=Infinity] Maximum retry attempts before giving up (failed records are still retained)
   * @param {object} [options.logger] Logger instance
   */
  constructor(options = {}) {
    this.storageDir = resolveStorageDir(options.storageDir || options.persistenceDir || options.spoolDir);
    this.udsClient = options.udsClient || null;
    this.ingestDeadlineMs = typeof options.ingestDeadlineMs === "number" ? options.ingestDeadlineMs : 1500;
    this.autoReplayOnStartup = Boolean(options.autoReplayOnStartup);
    this.maxRetries = typeof options.maxRetries === "number" ? options.maxRetries : Infinity;
    this.logger = options.logger || defaultLogger;

    this._sessionMutex = new KeyedMutex();
    this._replayLock = new Mutex();
    this._isClosed = false;
    // Payload fingerprints that failed permanently during this process
    // lifetime. On-disk `permanent` records are still skipped by replay; this
    // set additionally suppresses repeat agent_end attempts within the process.
    this._permanentPayloadHashes = new Set();

    this._ensureStorageDir();
  }

  getStorageDir() {
    return this.storageDir;
  }

  /**
   * Ensures storage directory exists with strict 0700 permissions.
   */
  _ensureStorageDir() {
    try {
      if (!fs.existsSync(this.storageDir)) {
        fs.mkdirSync(this.storageDir, { recursive: true, mode: 0o700 });
      }
      if (process.platform !== "win32") {
        try {
          fs.chmodSync(this.storageDir, 0o700);
        } catch (_) {}
      }
    } catch (err) {
      this.logger.warn(`Failed to set permissions on storage dir ${this.storageDir}: ${err.message}`);
    }
  }

  /**
   * Returns full file path for given session ID.
   * @param {string} sessionId
   * @returns {string}
   */
  getRecordPath(sessionId) {
    const fileName = getSafeFileName(sessionId);
    return path.join(this.storageDir, fileName);
  }

  /**
   * Persists session ingest payload to disk before network dispatch.
   *
   * @param {string} sessionId
   * @param {object} payload AMR session.ingest contract payload
   * @param {"pending" | "success" | "failed"} [status="pending"]
   * @returns {Promise<object>} Persisted record
   */
  async persistSession(sessionId, payload, status = "pending") {
    if (!sessionId || typeof sessionId !== "string") {
      throw new Error("AMR_PERSIST_INVALID_SESSION_ID: sessionId is required");
    }

    return this._sessionMutex.runExclusive(sessionId, async () => {
      this._ensureStorageDir();
      const filePath = this.getRecordPath(sessionId);
      let existing = null;
      if (fs.existsSync(filePath)) {
        try {
          const raw = fs.readFileSync(filePath, "utf8");
          existing = JSON.parse(raw);
        } catch (parseErr) {
          // 不能把损坏记录静默当成“不存在”再覆盖，否则会丢失
          // retryCount/permanent 等审计状态。先备份，再重建。
          const backup = `${filePath}.corrupt-${Date.now()}`;
          try {
            fs.renameSync(filePath, backup);
            this.logger.warn(
              `Corrupt session record for ${sessionId} moved to ${backup}: ${parseErr?.message || parseErr}`
            );
          } catch (backupErr) {
            this.logger.warn(
              `Corrupt session record for ${sessionId} could not be backed up: ${backupErr?.message || backupErr}`
            );
          }
          existing = null;
        }
      }

      const now = Date.now();
      const payloadHash = computePayloadFingerprint(payload);
      const samePayload = existing?.payloadHash
        ? existing.payloadHash === payloadHash
        : computePayloadFingerprint(existing?.payload) === payloadHash;
      // Preserve a permanent marker only inside the process that observed it.
      // A fresh process gets one fresh attempt after a restart, while startup
      // replay still skips the on-disk permanent record until this write.
      const preservePermanent =
        existing?.permanent === true &&
        samePayload &&
        this._permanentPayloadHashes.has(payloadHash);

      const record = {
        version: 1,
        sessionId: String(sessionId),
        // Preserve the failed status when a permanent marker is carried
        // forward, so a skipped repeat attempt cannot look "pending".
        status: preservePermanent ? (existing?.status || "failed") : status,
        createdAt: existing?.createdAt || now,
        updatedAt: now,
        retryCount: existing?.retryCount || 0,
        lastAttemptAt: existing?.lastAttemptAt || null,
        lastError: existing?.lastError || null,
        lastResult: existing?.lastResult || null,
        permanent: preservePermanent,
        errorCode: preservePermanent ? existing?.errorCode ?? null : null,
        payloadHash,
        payload,
      };

      writeRecordAtomic(filePath, record);
      return record;
    });
  }

  /**
   * Updates record status to success upon confirmed delivery.
   *
   * @param {string} sessionId
   * @param {any} [result] RPC response result
   * @returns {Promise<object|null>}
   */
  async markSuccess(sessionId, result = null) {
    if (!sessionId || typeof sessionId !== "string") return null;
    return this._sessionMutex.runExclusive(sessionId, async () => {
      const filePath = this.getRecordPath(sessionId);
      if (!fs.existsSync(filePath)) return null;

      let record = null;
      try {
        record = JSON.parse(fs.readFileSync(filePath, "utf8"));
      } catch (_) {
        return null;
      }

      const now = Date.now();
      record.status = "success";
      record.permanent = false;
      record.errorCode = null;
      record.updatedAt = now;
      record.lastAttemptAt = now;
      record.lastError = null;
      record.lastResult = result || { ok: true };

      writeRecordAtomic(filePath, record);
      this._permanentPayloadHashes.delete(
        record.payloadHash || computePayloadFingerprint(record.payload)
      );
      return record;
    });
  }

  /**
   * Updates record status to failed upon delivery failure.
   * Record is strictly RETAINED on disk for future replay.
   *
   * @param {string} sessionId
   * @param {Error|string} error
   * @returns {Promise<object|null>}
   */
  async markFailed(sessionId, error) {
    if (!sessionId || typeof sessionId !== "string") return null;
    return this._sessionMutex.runExclusive(sessionId, async () => {
      const filePath = this.getRecordPath(sessionId);
      if (!fs.existsSync(filePath)) return null;

      let record = null;
      try {
        record = JSON.parse(fs.readFileSync(filePath, "utf8"));
      } catch (_) {
        return null;
      }

      const now = Date.now();
      record.status = "failed";
      record.updatedAt = now;
      record.lastAttemptAt = now;
      record.retryCount = (record.retryCount || 0) + 1;
      const errDetail = error?.code ? `[code: ${error.code}] ${error.message}` : (error?.message || String(error));
      record.lastError = errDetail;
      record.permanent = false;
      record.errorCode = error?.code ?? null;
      this._permanentPayloadHashes.delete(
        record.payloadHash || computePayloadFingerprint(record.payload)
      );

      writeRecordAtomic(filePath, record);
      return record;
    });
  }

  /**
   * Marks a record as permanently failed. The record is retained on disk for
   * manual handling/audit but is never replayed automatically.
   *
   * @param {string} sessionId
   * @param {Error|string} error
   * @returns {Promise<object|null>}
   */
  async markPermanent(sessionId, error) {
    if (!sessionId || typeof sessionId !== "string") return null;
    return this._sessionMutex.runExclusive(sessionId, async () => {
      const filePath = this.getRecordPath(sessionId);
      if (!fs.existsSync(filePath)) return null;

      let record = null;
      try {
        record = JSON.parse(fs.readFileSync(filePath, "utf8"));
      } catch (_) {
        return null;
      }

      const now = Date.now();
      record.status = "failed";
      record.permanent = true;
      record.updatedAt = now;
      record.lastAttemptAt = now;
      record.retryCount = (record.retryCount || 0) + 1;
      record.errorCode = error?.code ?? null;
      record.lastError = error?.code
        ? `[code: ${error.code}] ${error.message}`
        : (error?.message || String(error));
      this._permanentPayloadHashes.add(
        record.payloadHash || computePayloadFingerprint(record.payload)
      );

      writeRecordAtomic(filePath, record);
      return record;
    });
  }

  /**
   * Retrieves single session record from disk.
   * @param {string} sessionId
   * @returns {Promise<object|null>}
   */
  async getRecord(sessionId) {
    if (!sessionId) return null;
    return this._sessionMutex.runExclusive(sessionId, async () => {
      const filePath = this.getRecordPath(sessionId);
      if (!fs.existsSync(filePath)) return null;
      try {
        const raw = fs.readFileSync(filePath, "utf8");
        return JSON.parse(raw);
      } catch (err) {
        this.logger.warn(`Failed to read record for session ${sessionId}: ${err.message}`);
        return null;
      }
    });
  }

  /**
   * Lists all persisted session records from storage directory.
   * @param {Function} [filterFn] Optional filter predicate (record) => boolean
   * @returns {Promise<Array<object>>}
   */
  async listRecords(filterFn = null) {
    this._ensureStorageDir();
    if (!fs.existsSync(this.storageDir)) return [];

    let fileNames = [];
    try {
      fileNames = fs.readdirSync(this.storageDir);
    } catch (_) {
      return [];
    }

    const records = [];
    for (const name of fileNames) {
      if (!name.endsWith(".json") || name.startsWith(".")) continue;
      const filePath = path.join(this.storageDir, name);
      try {
        const raw = fs.readFileSync(filePath, "utf8");
        const record = JSON.parse(raw);
        if (record && record.sessionId) {
          if (!filterFn || filterFn(record)) {
            records.push(record);
          }
        }
      } catch (_) {}
    }
    return records;
  }

  /**
   * Replays a single session to AMR via UDS.
   * Idempotent: supports both previously failed records and previously succeeded records.
   *
   * @param {string} sessionId
   * @param {object} [options]
   * @param {boolean} [options.force=false] Replay even if status === "success"
   * @param {number} [options.deadlineMs]
   * @param {object} [options.udsClient]
   * @returns {Promise<object>} Replay result
   */
  async replaySession(sessionId, options = {}) {
    const {
      force = false,
      forcePermanent = false,
      deadlineMs = this.ingestDeadlineMs,
      udsClient = this.udsClient,
    } = options;

    if (!udsClient) {
      throw new Error("AMR_PERSISTENCE_NO_CLIENT: UDS client is required for replay");
    }

    return this._sessionMutex.runExclusive(sessionId, async () => {
      const filePath = this.getRecordPath(sessionId);
      if (!fs.existsSync(filePath)) {
        return { ok: false, sessionId, error: "Session record not found" };
      }

      let record;
      try {
        record = JSON.parse(fs.readFileSync(filePath, "utf8"));
      } catch (err) {
        return { ok: false, sessionId, error: `Corrupted session record: ${err.message}` };
      }

      if (!record.payload || !Array.isArray(record.payload.messages) || record.payload.messages.length === 0) {
        return { ok: false, sessionId, error: "Empty or invalid session payload" };
      }

      // Permanent 2.x failures are retained for manual handling but are never
      // replayed automatically (DOC-AMR-09 section 3). This prevents an
      // auth/protocol error from becoming an infinite retry loop.
      if (record.permanent === true && !forcePermanent) {
        return {
          ok: false,
          sessionId,
          skipped: true,
          permanent: true,
          status: record.status,
          error: record.lastError || "Permanent failure retained for manual handling",
        };
      }

      // If already succeeded and force is false, skip re-sending
      if (record.status === "success" && !force) {
        return { ok: true, sessionId, skipped: true, status: "success" };
      }

      const now = Date.now();
      record.lastAttemptAt = now;

      try {
        // Legacy 1.x spool records may still contain identity fields. Never
        // forward them to a 2.x adapter.
        const outboundPayload = stripIdentityFields(record.payload);
        this.logger.debug(`Replaying session ${sessionId} (${outboundPayload.messages.length} messages, timeout: ${deadlineMs}ms)`);
        const ingestResult = await udsClient.request("session.ingest", outboundPayload, deadlineMs);
        record.status = "success";
        record.permanent = false;
        record.errorCode = null;
        record.updatedAt = Date.now();
        record.lastError = null;
        record.lastResult = ingestResult;

        writeRecordAtomic(filePath, record);
        this._permanentPayloadHashes.delete(
          record.payloadHash || computePayloadFingerprint(record.payload)
        );
        this.logger.info(`Replay succeeded for session ${sessionId}`);
        return {
          ok: true,
          sessionId,
          replayed: true,
          result: ingestResult,
          status: "success",
        };
      } catch (err) {
        record.status = "failed";
        record.updatedAt = Date.now();
        record.retryCount = (record.retryCount || 0) + 1;
        const errDetail = err?.code ? `[code: ${err.code}] ${err.message}` : (err?.message || String(err));
        record.lastError = errDetail;
        record.errorCode = err?.code ?? null;

        const permanent = isPermanentError(err);
        if (permanent) {
          record.permanent = true;
          this._permanentPayloadHashes.add(
            record.payloadHash || computePayloadFingerprint(record.payload)
          );
        }

        // Retain failed record on disk
        writeRecordAtomic(filePath, record);
        if (permanent) {
          this.logger.warn(
            `Replay permanently failed for session ${sessionId} (code: ${record.errorCode ?? "unknown"}); retained for manual handling and will not be retried: ${errDetail}`
          );
        } else {
          this.logger.warn(`Replay failed for session ${sessionId} (retryCount: ${record.retryCount}): ${errDetail}`);
        }
        return {
          ok: false,
          sessionId,
          replayed: true,
          permanent,
          error: errDetail,
          status: "failed",
          retryCount: record.retryCount,
        };
      }
    });
  }

  /**
   * Replays pending and failed sessions in batch.
   *
   * @param {object} [options]
   * @param {boolean} [options.includeSuccess=false] If true, replays both success and failed sessions idempotently
   * @param {number} [options.deadlineMs]
   * @param {object} [options.udsClient]
   * @param {Function} [options.filter]
   * @returns {Promise<object>} Summary { total, replayed, succeeded, failed, items }
   */
  async replayPending(options = {}) {
    const {
      includeSuccess = false,
      deadlineMs = this.ingestDeadlineMs,
      udsClient = this.udsClient,
      filter = null,
    } = options;

    return this._replayLock.runExclusive(async () => {
      this._ensureStorageDir();
      if (!fs.existsSync(this.storageDir)) {
        return { total: 0, replayed: 0, succeeded: 0, failed: 0, skippedPermanent: 0, items: [] };
      }

      let fileNames = [];
      try {
        fileNames = fs.readdirSync(this.storageDir);
      } catch (err) {
        this.logger.error(`Failed to read storage directory: ${err.message}`);
        return { total: 0, replayed: 0, succeeded: 0, failed: 0, skippedPermanent: 0, items: [] };
      }

      const jsonFiles = fileNames.filter(
        (f) => f.endsWith(".json") && !f.startsWith(".")
      );

      const items = [];
      let skippedPermanent = 0;
      for (const fileName of jsonFiles) {
        const filePath = path.join(this.storageDir, fileName);
        let record = null;
        try {
          record = JSON.parse(fs.readFileSync(filePath, "utf8"));
        } catch (_) {
          continue;
        }

        if (!record || !record.sessionId) continue;
        if (filter && typeof filter === "function" && !filter(record)) continue;

        // Permanent failures stay on disk for manual handling but are never
        // retried automatically, no matter how many startups occur.
        if (record.permanent === true) {
          this._permanentPayloadHashes.add(
            record.payloadHash || computePayloadFingerprint(record.payload)
          );
          skippedPermanent++;
          items.push({
            ok: false,
            sessionId: record.sessionId,
            skipped: true,
            permanent: true,
            status: record.status,
            error: record.lastError || "Permanent failure retained for manual handling",
          });
          continue;
        }

        // By default, skip records that already succeeded
        if (!includeSuccess && record.status === "success") {
          continue;
        }

        const replayRes = await this.replaySession(record.sessionId, {
          force: includeSuccess,
          deadlineMs,
          udsClient,
        });
        items.push(replayRes);
      }

      const replayed = items.filter((r) => !r.skipped).length;
      const succeeded = items.filter((r) => r.ok && !r.skipped).length;
      const failed = items.filter((r) => !r.ok && !r.skipped).length;

      return {
        total: jsonFiles.length,
        replayed,
        succeeded,
        failed,
        skippedPermanent,
        items,
      };
    });
  }

  /**
   * Replays ALL sessions (both success and failed) idempotently.
   *
   * @param {object} [options]
   * @returns {Promise<object>}
   */
  async replayAll(options = {}) {
    return this.replayPending({ ...options, includeSuccess: true });
  }

  close() {
    this._isClosed = true;
    this._permanentPayloadHashes.clear();
  }
}

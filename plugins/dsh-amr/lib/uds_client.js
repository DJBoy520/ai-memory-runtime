/**
 * lib/uds_client.js
 * Pure Node.js Standard Library UDS Client for AI Memory Runtime (AMR) 2.x.
 *
 * Red Line Compliance:
 * 1. Zero-VRAM: purely standard library (`node:net`, `node:buffer`).
 * 2. Protocol: 4-byte Big-Endian uint32 length prefix + UTF-8 JSON-RPC 2.0 payload.
 * 3. State Machine: Handles chunks, fragmentation, partial frames, and multiple concatenated frames (sticky packets).
 * 4. Safety: Rejects malformed frames (length 0 or > 4MB: 4 * 1024 * 1024).
 * 5. Hard Deadline: Aborts socket via socket.destroy(new Error("AMR_DEADLINE_EXCEEDED")) to ensure zero ghost callbacks.
 * 6. Identity: the 2.x adapter binds the agent identity to the socket; callers must not send
 *    `source_agent`/`agent_id`.
 * 7. Fail-open: Never crashes the host process.
 */

import net from "node:net";
import { logger } from "./logger.js";

const MAX_FRAME_SIZE = 4 * 1024 * 1024; // 4MB frame safety ceiling
const DEFAULT_SOCKET_BASENAME = "amr2-dsh.sock";

/**
 * AMR 2.x JSON-RPC error codes (DOC-AMR-09 section 3).
 * 1.x daemon method names and frames are unchanged.
 */
export const AMR_ERROR_CODES = Object.freeze({
  AGENT_UNAUTHORIZED: -32001,
  VERSION_CONFLICT: -32002,
  MODEL_LOADING: -32003,
  QUEUE_FULL: -32004,
  NOT_FOUND: -32005,
  IDEMPOTENCY_CONFLICT: -32006,
  ADAPTER_UNAVAILABLE: -32010,
  ADAPTER_PROTOCOL_ERROR: -32011,
  ADAPTER_REMOTE_AUTH_FAILED: -32012,
  METHOD_NOT_FOUND: -32601,
  INVALID_ARGUMENT: -32602,
  INTERNAL: -32603,
});

/** Backwards/forwards friendly alias. */
export const ERROR_CODES = AMR_ERROR_CODES;

const TRANSIENT_NETWORK_CODES = new Set([
  "ETIMEDOUT",
  "ECONNREFUSED",
  "ECONNRESET",
  "EPIPE",
  // Missing/unreachable socket is the local-adapter equivalent of "offline".
  "ENOENT",
  "ENOTFOUND",
  "EHOSTUNREACH",
  "ENETUNREACH",
]);

/**
 * True when an error is worth retrying later (adapter unavailable / timeout /
 * connection failure). Permanent 2.x business errors return false.
 *
 * @param {any} err
 * @returns {boolean}
 */
export function isTransientError(err) {
  if (!err) return false;
  const code = err.code;
  if (
    code === AMR_ERROR_CODES.ADAPTER_UNAVAILABLE ||
    code === AMR_ERROR_CODES.MODEL_LOADING ||
    code === AMR_ERROR_CODES.QUEUE_FULL
  ) {
    return true;
  }
  if (typeof code === "string" && TRANSIENT_NETWORK_CODES.has(code)) {
    return true;
  }
  return false;
}

/**
 * Resolve UDS path for the 2.x local adapter.
 *
 * Priority (DOC-AMR-09 section 2):
 * 1. Explicit `socketPath` config
 * 2. `AMR2_SOCKET_PATH` legacy environment fallback
 * 3. `AMR_SOCKET_PATH` migration fallback (only while AMR2 is unset)
 * 4. `$XDG_RUNTIME_DIR/amr2-dsh.sock`
 * 5. `/run/user/<uid>/amr2-dsh.sock`
 * 6. `/tmp/amr2-dsh.sock`
 *
 * The legacy `qdrant-bge.sock` path is intentionally no longer a default.
 */
export function resolveSocketPath(configuredPath) {
  if (configuredPath && typeof configuredPath === "string" && configuredPath.trim().length > 0) {
    return configuredPath.trim();
  }
  const amr2Env = process.env.AMR2_SOCKET_PATH;
  if (amr2Env && amr2Env.trim().length > 0) {
    return amr2Env.trim();
  }
  const legacyEnv = process.env.AMR_SOCKET_PATH;
  if (legacyEnv && legacyEnv.trim().length > 0) {
    return legacyEnv.trim();
  }
  const xdg = process.env.XDG_RUNTIME_DIR;
  if (xdg && xdg.trim().length > 0) {
    return `${xdg.trim()}/${DEFAULT_SOCKET_BASENAME}`;
  }
  if (process.platform === "linux" && typeof process.getuid === "function") {
    return `/run/user/${process.getuid()}/${DEFAULT_SOCKET_BASENAME}`;
  }
  return `/tmp/${DEFAULT_SOCKET_BASENAME}`;
}

/**
 * Encode message with 4-byte Big-Endian length header.
 * @param {object} obj JSON-serializable object
 * @returns {Buffer}
 */
export function encodeFrame(obj) {
  const jsonStr = JSON.stringify(obj);
  const payloadBuf = Buffer.from(jsonStr, "utf8");
  if (payloadBuf.length === 0 || payloadBuf.length > MAX_FRAME_SIZE) {
    throw new Error(
      `AMR_FRAME_SIZE_INVALID: payload length ${payloadBuf.length} exceeds bounds (1..${MAX_FRAME_SIZE})`
    );
  }
  const headerBuf = Buffer.allocUnsafe(4);
  headerBuf.writeUInt32BE(payloadBuf.length, 0);
  return Buffer.concat([headerBuf, payloadBuf]);
}

/**
 * FrameDecoder: State machine parser for streaming buffer inputs.
 * Accurately extracts complete frames across partial or sticky network chunks.
 */
export class FrameDecoder {
  constructor(maxFrameSize = MAX_FRAME_SIZE) {
    this.maxFrameSize = maxFrameSize;
    this.buffer = Buffer.alloc(0);
  }

  /**
   * Feed new chunk into decoder and extract all available complete payload buffers.
   * @param {Buffer} chunk
   * @returns {Buffer[]} Array of extracted JSON payload buffers
   * @throws {Error} if frame size is invalid or exceeds max limit
   */
  feed(chunk) {
    if (!chunk || chunk.length === 0) {
      return [];
    }
    this.buffer = Buffer.concat([this.buffer, chunk]);
    const frames = [];

    while (this.buffer.length >= 4) {
      const frameLen = this.buffer.readUInt32BE(0);

      if (frameLen === 0 || frameLen > this.maxFrameSize) {
        // Corrupted or oversized frame - reset buffer and throw
        this.buffer = Buffer.alloc(0);
        throw new Error(`AMR_FRAME_SIZE_INVALID: frame length ${frameLen} exceeds bounds (0..${this.maxFrameSize})`);
      }

      const totalExpected = 4 + frameLen;
      if (this.buffer.length < totalExpected) {
        // Incomplete frame, wait for more data
        break;
      }

      // Slice out the exact frame payload
      const payload = this.buffer.subarray(4, totalExpected);
      frames.push(payload);

      // Advance buffer
      this.buffer = this.buffer.subarray(totalExpected);
    }

    return frames;
  }

  reset() {
    this.buffer = Buffer.alloc(0);
  }
}

export class AmrUdsClient {
  /**
   * @param {object} [options]
   * @param {string} [options.socketPath]
   * @param {number} [options.defaultDeadlineMs=80]
   */
  constructor(options = {}) {
    this.configuredPath = options.socketPath;
    this.defaultDeadlineMs = options.defaultDeadlineMs || 80;
    this._requestId = 1;
  }

  getSocketPath() {
    return resolveSocketPath(this.configuredPath);
  }

  /**
   * Execute JSON-RPC 2.0 Request with Hard Deadline.
   * If deadline fires, the socket is immediately destroyed to prevent leaking or ghost returns.
   *
   * @param {string} method
   * @param {object} [params={}]
   * @param {number} [deadlineMs]
   * @returns {Promise<any>}
   */
  async request(method, params = {}, deadlineMs) {
    const timeoutMs = deadlineMs || this.defaultDeadlineMs;
    const socketPath = this.getSocketPath();
    const id = this._requestId++;

    return new Promise((resolve, reject) => {
      let settled = false;
      let timer = null;
      let socket = null;
      const decoder = new FrameDecoder();

      const cleanup = () => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        if (socket) {
          socket.removeAllListeners();
          // destroy() 之后仍可能有异步 error；保留一个空监听器，避免
          // Node 把它升级成未捕获异常并杀掉宿主进程。
          socket.on("error", () => {});
          if (!socket.destroyed) {
            socket.destroy();
          }
          socket = null;
        }
      };

      const finishResolve = (val) => {
        if (!settled) {
          settled = true;
          cleanup();
          resolve(val);
        }
      };

      const finishReject = (err) => {
        if (!settled) {
          settled = true;
          cleanup();
          reject(err);
        }
      };

      // Set Hard Deadline timer
      timer = setTimeout(() => {
        if (!settled) {
          const timeoutErr = new Error(`AMR_DEADLINE_EXCEEDED: method ${method} timed out after ${timeoutMs}ms`);
          timeoutErr.code = "ETIMEDOUT";
          const sock = socket;
          finishReject(timeoutErr);
          if (sock && !sock.destroyed) {
            sock.destroy();
          }
        }
      }, timeoutMs);

      try {
        socket = net.createConnection({ path: socketPath });
      } catch (err) {
        finishReject(err);
        return;
      }

      socket.on("connect", () => {
        if (settled) return;
        try {
          const rpcPayload = {
            jsonrpc: "2.0",
            id,
            method,
            params,
          };
          const frame = encodeFrame(rpcPayload);
          socket.write(frame);
        } catch (err) {
          finishReject(err);
        }
      });

      socket.on("data", (chunk) => {
        if (settled) return;
        let payloads;
        try {
          payloads = decoder.feed(chunk);
        } catch (err) {
          finishReject(err);
          return;
        }

        for (const payload of payloads) {
          try {
            const resp = JSON.parse(payload.toString("utf8"));
            if (resp && resp.id === id) {
              if (resp.error) {
                const err = new Error(resp.error.message || "AMR RPC Error");
                err.code = resp.error.code;
                err.data = resp.error.data;
                finishReject(err);
              } else {
                finishResolve(resp.result);
              }
              return;
            }
          } catch (err) {
            finishReject(err);
            return;
          }
        }
      });

      socket.on("error", (err) => {
        finishReject(err);
      });

      socket.on("end", () => {
        if (!settled) {
          const closedErr = new Error("AMR_CONNECTION_CLOSED: socket closed prematurely before response");
          closedErr.code = "ECONNRESET";
          finishReject(closedErr);
        }
      });

      socket.on("close", () => {
        if (!settled) {
          const closedErr = new Error("AMR_CONNECTION_CLOSED: socket closed prematurely before response");
          closedErr.code = "ECONNRESET";
          finishReject(closedErr);
        }
      });
    });
  }

  /**
   * 2.x control-method health check: `amr.ping` -> `{"status":"ok"}`.
   * Never touches the backend and fails open at the call site.
   *
   * @param {number} [deadlineMs]
   * @returns {Promise<{status?: string}>}
   */
  ping(deadlineMs) {
    return this.request("amr.ping", {}, deadlineMs);
  }

  /**
   * Execute Fire-and-forget notification (No response expected, no ID in JSON-RPC).
   *
   * NOTE: 2.x session ingestion must use request() with an id so a server
   * acknowledgement can be distinguished from packet loss (DOC-AMR-09 §4).
   * notify() is retained only for 1.x-compatible, non-critical calls.
   *
   * @param {string} method
   * @param {object} params
   * @returns {void}
   */
  notify(method, params = {}) {
    const socketPath = this.getSocketPath();
    setImmediate(() => {
      let socket = null;
      let timer = null;

      const cleanup = () => {
        if (timer) clearTimeout(timer);
        if (socket) {
          socket.removeAllListeners();
          if (!socket.destroyed) socket.destroy();
          socket = null;
        }
      };

      try {
        socket = net.createConnection({ path: socketPath });
      } catch (err) {
        logger.debug(`[uds_client] notify connect error: ${err.message}`);
        return;
      }

      // 1000ms safety timeout to prevent hanging sockets
      timer = setTimeout(() => {
        cleanup();
      }, 1000);
      timer.unref?.();

      socket.on("connect", () => {
        try {
          const rpcPayload = {
            jsonrpc: "2.0",
            method,
            params,
          };
          const frame = encodeFrame(rpcPayload);
          socket.end(frame, () => {
            cleanup();
          });
        } catch (err) {
          logger.debug(`[uds_client] notify write error: ${err.message}`);
          cleanup();
        }
      });

      socket.on("error", (err) => {
        logger.debug(`[uds_client] notify socket error: ${err.message}`);
        cleanup();
      });

      socket.once("close", cleanup);
    });
  }
}

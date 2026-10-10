/**
 * lib/uds_client.js
 * Pure Node.js Standard Library UDS Client for AI Memory Runtime (AMR).
 *
 * Red Line Compliance:
 * 1. Zero-VRAM: purely standard library (`node:net`, `node:buffer`, `node:os`).
 * 2. Protocol: 4-byte Big-Endian uint32 length prefix + UTF-8 JSON-RPC 2.0 payload.
 * 3. State Machine: Handles chunks, fragmentation, partial frames, and multiple concatenated frames (sticky packets).
 * 4. Safety: Rejects malformed frames (length 0 or > 4MB: 4 * 1024 * 1024).
 * 5. Hard Deadline: Aborts socket via socket.destroy(new Error("AMR_DEADLINE_EXCEEDED")) to ensure zero ghost callbacks.
 * 6. Fire-and-forget: Supports notify() for session ingestion with 0ms host wait.
 * 7. Fail-open: Never crashes the host process.
 */

import net from "node:net";
import os from "node:os";
import { logger } from "./logger.js";

const MAX_FRAME_SIZE = 4 * 1024 * 1024; // 4MB frame safety ceiling

/**
 * Resolve UDS path with priority:
 * 1. Explicit config path
 * 2. process.env.AMR_SOCKET_PATH
 * 3. process.env.XDG_RUNTIME_DIR/qdrant-bge.sock
 * 4. /run/user/<uid>/qdrant-bge.sock
 * 5. /tmp/qdrant-bge.sock
 */
export function resolveSocketPath(configuredPath) {
  if (configuredPath && typeof configuredPath === "string" && configuredPath.trim().length > 0) {
    return configuredPath.trim();
  }
  if (process.env.AMR_SOCKET_PATH && process.env.AMR_SOCKET_PATH.trim().length > 0) {
    return process.env.AMR_SOCKET_PATH.trim();
  }
  const xdg = process.env.XDG_RUNTIME_DIR;
  if (xdg && xdg.trim().length > 0) {
    return `${xdg.trim()}/qdrant-bge.sock`;
  }
  if (typeof process.getuid === "function") {
    return `/run/user/${process.getuid()}/qdrant-bge.sock`;
  }
  return "/tmp/qdrant-bge.sock";
}

/**
 * Encode message with 4-byte Big-Endian length header.
 * @param {object} obj JSON-serializable object
 * @returns {Buffer}
 */
export function encodeFrame(obj) {
  const jsonStr = JSON.stringify(obj);
  const payloadBuf = Buffer.from(jsonStr, "utf8");
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
          finishReject(new Error("AMR_CONNECTION_CLOSED: socket closed prematurely before response"));
        }
      });
    });
  }

  /**
   * Execute Fire-and-forget notification (No response expected, no ID in JSON-RPC).
   * Perfect for session.ingest.
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
    });
  }
}

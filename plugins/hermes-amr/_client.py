"""Lightweight UNIX Domain Socket (UDS) client for AMR (AI Memory Runtime).

Protocol:
- 4-byte big-endian uint32 payload length prefix
- Followed by utf-8 encoded JSON-RPC 2.0 request/response

Strict zero third-party dependencies (socket, struct, json, os, time, logging only).
Defensive handling against packet fragmentation and sticky packets (_recv_exact).
Fail-open error handling for resilient agent operation.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import struct
import time
from typing import Any, Dict, Optional, Union

logger = logging.getLogger(__name__)

DEFAULT_SOCKET_PATH = "/run/user/1000/qdrant-bge.sock"
DEFAULT_CONNECT_TIMEOUT = 0.3
DEFAULT_REQUEST_TIMEOUT = 1.0


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from the socket, defending against partial chunks."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionResetError(f"Socket closed unexpectedly while receiving {n} bytes (got {len(buf)} bytes)")
        buf.extend(chunk)
    return bytes(buf)


class AmrUdsClient:
    """High-performance lightweight UDS client for AMR."""

    def __init__(
        self,
        socket_path: str = DEFAULT_SOCKET_PATH,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ):
        self.socket_path = socket_path
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

    def is_available(self) -> bool:
        """Fast check to see if the UDS socket exists and is connectable."""
        if not os.path.exists(self.socket_path):
            return False
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.1)  # probe
                sock.connect(self.socket_path)
            return True
        except Exception:
            return False

    def call(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        req_id: Optional[Union[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Send a JSON-RPC 2.0 call over UDS and return the result.
        
        Fail-open: returns None on connection or communication error.
        """
        if req_id is None:
            req_id = int(time.time() * 1000)

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": req_id,
        }

        raw_data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        header = struct.pack(">I", len(raw_data))

        req_timeout = timeout if timeout is not None else self.request_timeout

        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(req_timeout)
                sock.connect(self.socket_path)
                sock.sendall(header + raw_data)

                # Read 4-byte response length
                resp_header = _recv_exact(sock, 4)
                (resp_len,) = struct.unpack(">I", resp_header)
                if resp_len > 16 * 1024 * 1024:  # 16MB sanity safety limit
                    raise ValueError(f"Response too large: {resp_len} bytes")

                resp_body = _recv_exact(sock, resp_len)
                resp_json = json.loads(resp_body.decode("utf-8"))

                if "error" in resp_json and resp_json["error"]:
                    logger.warning("AMR RPC error for %s: %s", method, resp_json["error"])
                    return None
                return resp_json.get("result")
        except socket.timeout:
            logger.warning("AMR UDS timeout (%ss) on method %s", req_timeout, method)
            return None
        except Exception as exc:
            logger.debug("AMR UDS call failed on %s (%s): %s", method, self.socket_path, exc)
            return None

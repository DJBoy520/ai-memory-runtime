"""Lightweight UNIX Domain Socket (UDS) client for AMR (AI Memory Runtime) 2.x.

Protocol:
- 4-byte big-endian uint32 payload length prefix
- Followed by utf-8 encoded JSON-RPC 2.0 request/response

Compliance (DOC-AMR-09):
- The 2.x local adapter binds the agent identity to the socket. Callers must
  not send ``agent_id``/``source_agent``.
- ``request()`` surfaces JSON-RPC/transport failures so callers can distinguish
  acknowledged writes from lost ones (persist-first ingestion).
- ``call()`` keeps the 1.x fail-open contract and returns ``None`` on failure.
- ``ping()`` implements the ``amr.ping`` control method.

Strict zero third-party dependencies (socket, struct, json, os, time, logging only).
Defensive handling against packet fragmentation and sticky packets (_recv_exact).
"""

from __future__ import annotations

import errno as errno_mod
import itertools
import json
import logging
import os
import socket
import struct
import sys
import threading
from typing import Any, Dict, Optional, Union

logger = logging.getLogger(__name__)

DEFAULT_SOCKET_BASENAME = "amr2-hermes.sock"
DEFAULT_CONNECT_TIMEOUT = 0.3
DEFAULT_REQUEST_TIMEOUT = 1.0

MAX_REQUEST_FRAME_SIZE = 4 * 1024 * 1024  # adapter request frame ceiling
MAX_RESPONSE_FRAME_SIZE = 8 * 1024 * 1024  # adapter response frame ceiling

# AMR 2.x error codes (DOC-AMR-09 section 3). Method names are unchanged.
AMR_ERROR_CODES: Dict[str, int] = {
    "AGENT_UNAUTHORIZED": -32001,
    "VERSION_CONFLICT": -32002,
    "MODEL_LOADING": -32003,
    "QUEUE_FULL": -32004,
    "NOT_FOUND": -32005,
    "IDEMPOTENCY_CONFLICT": -32006,
    "ADAPTER_UNAVAILABLE": -32010,
    "ADAPTER_PROTOCOL_ERROR": -32011,
    "ADAPTER_REMOTE_AUTH_FAILED": -32012,
    "METHOD_NOT_FOUND": -32601,
    "INVALID_ARGUMENT": -32602,
    "INTERNAL": -32603,
}

ADAPTER_UNAVAILABLE = AMR_ERROR_CODES["ADAPTER_UNAVAILABLE"]
ADAPTER_PROTOCOL_ERROR = AMR_ERROR_CODES["ADAPTER_PROTOCOL_ERROR"]
ADAPTER_REMOTE_AUTH_FAILED = AMR_ERROR_CODES["ADAPTER_REMOTE_AUTH_FAILED"]

_TRANSIENT_ERROR_CODES = {
    ADAPTER_UNAVAILABLE,
    AMR_ERROR_CODES["MODEL_LOADING"],
    AMR_ERROR_CODES["QUEUE_FULL"],
}

_TRANSIENT_OS_CODES = {
    "ETIMEDOUT",
    "ECONNREFUSED",
    "ECONNRESET",
    "EPIPE",
    "ENOENT",
    "ENOTFOUND",
    "EHOSTUNREACH",
    "ENETUNREACH",
    "EAGAIN",
    "EWOULDBLOCK",
}


def resolve_socket_path(configured_path: Optional[str] = None) -> str:
    """Resolve the 2.x adapter socket path.

    Priority (DOC-AMR-09 section 2):
    1. Explicit ``socket_path`` passed by the caller/config
    2. ``AMR2_SOCKET_PATH`` environment fallback
    3. Legacy ``AMR_SOCKET_PATH`` migration fallback
    4. ``$XDG_RUNTIME_DIR/amr2-hermes.sock``
    5. Linux ``/run/user/<uid>/amr2-hermes.sock``
    6. ``/tmp/amr2-hermes.sock``
    """
    if configured_path is not None:
        configured = str(configured_path).strip()
        if configured:
            return configured

    amr2_env = os.environ.get("AMR2_SOCKET_PATH")
    if amr2_env and amr2_env.strip():
        return amr2_env.strip()

    legacy_env = os.environ.get("AMR_SOCKET_PATH")
    if legacy_env and legacy_env.strip():
        return legacy_env.strip()

    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and xdg.strip():
        return os.path.join(xdg.strip(), DEFAULT_SOCKET_BASENAME)

    if sys.platform.startswith("linux") and hasattr(os, "getuid"):
        return f"/run/user/{os.getuid()}/{DEFAULT_SOCKET_BASENAME}"

    uid = os.getuid() if hasattr(os, "getuid") else "nouser"
    return f"/tmp/amr2-hermes-{uid}.sock"


# Backwards-compatible module constant: the resolved 2.x default at import time.
# The legacy 1.x ``qdrant-bge.sock`` path is intentionally no longer a default.
DEFAULT_SOCKET_PATH = resolve_socket_path()


class AmrError(Exception):
    """Base class for AMR client errors."""


class AmrRpcError(AmrError):
    """A JSON-RPC error returned by the adapter/daemon."""

    def __init__(self, code: Any, message: str, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(message)

    def __str__(self) -> str:
        return f"AMR RPC error {self.code}: {self.message}"


class AmrUnavailableError(AmrRpcError):
    """Transient transport failure (timeout / connection error): code -32010."""

    def __init__(self, message: str, data: Any = None):
        super().__init__(ADAPTER_UNAVAILABLE, message, data)


class AmrProtocolError(AmrRpcError):
    """Malformed/oversized/unexpected adapter response: code -32011."""

    def __init__(self, message: str, data: Any = None):
        super().__init__(ADAPTER_PROTOCOL_ERROR, message, data)


class AmrAuthError(AmrRpcError):
    """Remote adapter authentication failure: code -32012."""

    def __init__(self, message: str, data: Any = None):
        super().__init__(ADAPTER_REMOTE_AUTH_FAILED, message, data)


_id_lock = threading.Lock()
_id_counter = itertools.count(1)


def next_request_id() -> int:
    """Return a process-wide monotonically increasing JSON-RPC id."""
    with _id_lock:
        return next(_id_counter)


# Backwards-compatible private alias.
_next_request_id = next_request_id


def is_transient(exc: BaseException) -> bool:
    """Return True when an error is worth retrying later.

    Adapter unavailability, model loading, queue-full and network/timeout
    failures are transient. Permanent business/auth/protocol errors are not.
    """
    if exc is None:
        return False

    code = getattr(exc, "code", None)
    if code in _TRANSIENT_ERROR_CODES:
        return True
    if isinstance(code, str) and code.upper() in _TRANSIENT_OS_CODES:
        return True
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    # Bare OSError from a socket layer (e.g. FileNotFoundError for a missing
    # adapter socket) has no JSON-RPC code but is still an offline condition.
    if isinstance(exc, OSError) and code is None:
        errno_name = errno_mod.errorcode.get(getattr(exc, "errno", None))
        if errno_name and errno_name in _TRANSIENT_OS_CODES:
            return True
    return False


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from the socket, defending against partial chunks."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionResetError(
                f"Socket closed unexpectedly while receiving {n} bytes (got {len(buf)} bytes)"
            )
        buf.extend(chunk)
    return bytes(buf)


class AmrUdsClient:
    """High-performance lightweight UDS client for AMR 2.x."""

    def __init__(
        self,
        socket_path: Optional[str] = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ):
        self.socket_path = resolve_socket_path(socket_path)
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

    def is_available(self) -> bool:
        """Fast check to see if the UDS socket is connectable."""
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.1)  # probe
                sock.connect(self.socket_path)
            return True
        except Exception:
            return False

    def ping(self, timeout: float = 0.3) -> Any:
        """2.x control-method health check: ``amr.ping``.

        Raises on failure; callers are expected to fail open (log at debug).
        """
        return self.request("amr.ping", {}, timeout=timeout)

    def request(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        req_id: Optional[Union[str, int]] = None,
    ) -> Any:
        """Send a JSON-RPC 2.0 request and return its ``result``.

        Unlike :meth:`call`, this surfaces failures:

        - JSON-RPC error -> :class:`AmrRpcError` (with the server's code)
        - timeout/connection error -> :class:`AmrUnavailableError` (code -32010)
        - malformed/oversized response -> :class:`AmrProtocolError` (code -32011)
        - adapter auth rejection -> :class:`AmrAuthError` (code -32012)
        """
        if req_id is None:
            req_id = next_request_id()

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": req_id,
        }

        try:
            raw_data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise AmrProtocolError(f"Cannot encode request for {method}: {exc}") from exc

        if len(raw_data) == 0 or len(raw_data) > MAX_REQUEST_FRAME_SIZE:
            raise AmrProtocolError(
                f"Request frame for {method} has invalid size {len(raw_data)} "
                f"(1..{MAX_REQUEST_FRAME_SIZE})"
            )
        header = struct.pack(">I", len(raw_data))
        req_timeout = timeout if timeout is not None else self.request_timeout

        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                # 连接与读写使用不同的超时：连接按 connect_timeout，避免
                # 一个挂起的 accept 队列占用完整的 request_timeout。
                sock.settimeout(self.connect_timeout)
                sock.connect(self.socket_path)
                sock.settimeout(req_timeout)
                sock.sendall(header + raw_data)

                resp_header = _recv_exact(sock, 4)
                (resp_len,) = struct.unpack(">I", resp_header)
                if resp_len == 0 or resp_len > MAX_RESPONSE_FRAME_SIZE:
                    raise AmrProtocolError(
                        f"Response frame for {method} has invalid size {resp_len} "
                        f"(1..{MAX_RESPONSE_FRAME_SIZE})"
                    )
                resp_body = _recv_exact(sock, resp_len)
        except AmrRpcError:
            raise
        except socket.timeout as exc:
            raise AmrUnavailableError(
                f"AMR UDS timeout ({req_timeout}s) on method {method}",
                {"os_error": "ETIMEDOUT"},
            ) from exc
        except (ConnectionError, OSError) as exc:
            raise AmrUnavailableError(
                f"AMR UDS connection failed on {method} ({self.socket_path}): {exc}",
                {
                    "errno": getattr(exc, "errno", None),
                    "os_error": getattr(exc, "strerror", None) or exc.__class__.__name__,
                },
            ) from exc
        except Exception as exc:  # defensive: never leak an unexpected socket error
            raise AmrUnavailableError(
                f"AMR UDS failure on {method} ({self.socket_path}): {exc}"
            ) from exc

        try:
            resp_json = json.loads(resp_body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise AmrProtocolError(
                f"Malformed JSON-RPC response for {method}: {exc}"
            ) from exc

        if not isinstance(resp_json, dict):
            raise AmrProtocolError(
                f"Malformed JSON-RPC response for {method}: not an object"
            )

        error = resp_json.get("error")
        if error:
            # JSON-RPC 允许错误响应的 id 为 null；先处理 error，再校验
            # 成功响应的 id，避免把真实的鉴权/参数错误误报为 id mismatch。
            response_id = resp_json.get("id")
            if response_id not in (None, req_id):
                raise AmrProtocolError(
                    f"JSON-RPC error id mismatch for {method}: "
                    f"expected {req_id!r} or null, got {response_id!r}"
                )
            if not isinstance(error, dict) or not isinstance(error.get("code"), int):
                raise AmrProtocolError(
                    f"Malformed JSON-RPC error object for {method}: {error!r}"
                )
            code = error["code"]
            message = str(error.get("message") or "AMR RPC error")
            data = error.get("data")
            if code == ADAPTER_REMOTE_AUTH_FAILED:
                raise AmrAuthError(message, data)
            raise AmrRpcError(code, message, data)

        if resp_json.get("id") != req_id:
            raise AmrProtocolError(
                f"JSON-RPC response id mismatch for {method}: "
                f"expected {req_id!r}, got {resp_json.get('id')!r}"
            )

        if "result" not in resp_json:
            raise AmrProtocolError(
                f"JSON-RPC response for {method} has neither result nor error"
            )
        return resp_json.get("result")

    def call(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        req_id: Optional[Union[str, int]] = None,
    ) -> Optional[Any]:
        """Fail-open JSON-RPC call: returns the result, or ``None`` on any error."""
        try:
            return self.request(method, params, timeout=timeout, req_id=req_id)
        except AmrRpcError as exc:
            logger.warning("AMR RPC error for %s: %s", method, exc)
            return None
        except Exception as exc:
            logger.debug(
                "AMR UDS call failed on %s (%s): %s", method, self.socket_path, exc
            )
            return None

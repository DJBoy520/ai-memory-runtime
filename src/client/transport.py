"""
AI Memory Runtime (AMR) - Client Transport Layer.

Provides decoupled transport abstractions for communicating with AMR:
- BaseTransport: Abstract protocol interface.
- UdsTransport: High-performance Unix Domain Socket transport (sync & async)
  with JSON-RPC 2.0 framing, timeout guards, and error categorization.
- MockTransport: In-memory mock transport for testing and fault injection.
"""

from __future__ import annotations

import abc
import asyncio
import json
import logging
import os
import socket
import struct
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from src.client.boundaries import (
    PermanentRejectionError,
    TransientDeliveryError,
    is_transient_error,
)

logger = logging.getLogger("amr.client.transport")

DEFAULT_UDS_SOCKET_PATH = "/run/user/1000/qdrant-bge.sock"
MAX_FRAME_BYTES = 16 * 1024 * 1024  # 16 MB sanity safety limit
HEADER_STRUCT = struct.Struct(">I")


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


async def _async_read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    """Read exactly n bytes from asyncio StreamReader."""
    try:
        return await reader.readexactly(n)
    except asyncio.IncompleteReadError as e:
        raise ConnectionResetError(
            f"Socket closed unexpectedly while receiving {n} bytes (got {len(e.partial)} bytes)"
        ) from e


class BaseTransport(abc.ABC):
    """Abstract interface for sending session ingestion batches to AMR."""

    @abc.abstractmethod
    def send_ingest(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Synchronously transmit session ingestion batch to AMR."""
        pass

    @abc.abstractmethod
    async def async_send_ingest(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Asynchronously transmit session ingestion batch to AMR."""
        pass


class UdsTransport(BaseTransport):
    """
    Standard UNIX Domain Socket transport for AMR, using 4-byte big-endian
    length-prefixed JSON-RPC 2.0 frames.
    """

    def __init__(
        self,
        socket_path: str = DEFAULT_UDS_SOCKET_PATH,
        connect_timeout: float = 1.0,
        request_timeout: float = 5.0,
    ):
        self.socket_path = socket_path
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

    def _build_payload(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        req_id: Optional[Union[str, int]] = None,
    ) -> bytes:
        if req_id is None:
            req_id = f"spool_{int(time.time() * 1000)}"

        params: Dict[str, Any] = {
            "session_id": session_id,
            "agent_id": agent_id,
            "messages": messages,
        }
        if project_id is not None:
            params["project_id"] = project_id

        rpc_obj = {
            "jsonrpc": "2.0",
            "method": "session.ingest",
            "params": params,
            "id": req_id,
        }
        raw = json.dumps(rpc_obj, ensure_ascii=False).encode("utf-8")
        if len(raw) > MAX_FRAME_BYTES:
            raise ValueError(f"Payload size {len(raw)} exceeds max frame limit {MAX_FRAME_BYTES}")
        return HEADER_STRUCT.pack(len(raw)) + raw

    def _parse_response(self, raw_resp: bytes) -> Dict[str, Any]:
        try:
            resp_json = json.loads(raw_resp.decode("utf-8"))
        except Exception as e:
            raise TransientDeliveryError(f"Failed to decode server JSON response: {e}") from e

        if "error" in resp_json and resp_json["error"]:
            err = resp_json["error"]
            code = err.get("code") if isinstance(err, dict) else None
            msg = (err.get("message") if isinstance(err, dict) else str(err)) or "AMR RPC Error"
            data = err.get("data") if isinstance(err, dict) else None

            if is_transient_error(code=code, message=msg):
                raise TransientDeliveryError(f"AMR Transient Error [{code}]: {msg}", code=code, data=data)
            else:
                raise PermanentRejectionError(f"AMR Permanent Error [{code}]: {msg}", code=code, data=data)

        if "result" in resp_json:
            return resp_json["result"]
        return resp_json

    def send_ingest(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        effective_timeout = timeout if timeout is not None else self.request_timeout
        frame_bytes = self._build_payload(session_id, agent_id, project_id, messages)

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(effective_timeout)
            try:
                sock.connect(self.socket_path)
            except (FileNotFoundError, ConnectionRefusedError, OSError) as e:
                raise TransientDeliveryError(
                    f"Failed to connect to AMR UDS at '{self.socket_path}': {e}"
                ) from e

            # Send frame
            sock.sendall(frame_bytes)

            # Read response header
            try:
                header = _recv_exact(sock, HEADER_STRUCT.size)
            except (socket.timeout, TimeoutError) as e:
                raise TransientDeliveryError(f"AMR UDS read timeout after {effective_timeout}s") from e
            except (ConnectionResetError, BrokenPipeError, ConnectionError) as e:
                raise TransientDeliveryError(f"AMR UDS connection dropped before response: {e}") from e

            (resp_len,) = HEADER_STRUCT.unpack(header)
            if resp_len > MAX_FRAME_BYTES:
                raise PermanentRejectionError(f"AMR response length {resp_len} exceeds limit")

            # Read response body
            body = _recv_exact(sock, resp_len)
            return self._parse_response(body)
        finally:
            try:
                sock.close()
            except OSError:
                pass

    async def async_send_ingest(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        effective_timeout = timeout if timeout is not None else self.request_timeout
        frame_bytes = self._build_payload(session_id, agent_id, project_id, messages)

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(self.socket_path),
                timeout=self.connect_timeout,
            )
        except (FileNotFoundError, ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            raise TransientDeliveryError(
                f"Async failed to connect to AMR UDS at '{self.socket_path}': {e}"
            ) from e

        try:
            writer.write(frame_bytes)
            await writer.drain()

            header = await asyncio.wait_for(
                _async_read_exact(reader, HEADER_STRUCT.size),
                timeout=effective_timeout,
            )
            (resp_len,) = HEADER_STRUCT.unpack(header)
            if resp_len > MAX_FRAME_BYTES:
                raise PermanentRejectionError(f"AMR async response length {resp_len} exceeds limit")

            body = await asyncio.wait_for(
                _async_read_exact(reader, resp_len),
                timeout=effective_timeout,
            )
            return self._parse_response(body)
        except asyncio.TimeoutError as e:
            raise TransientDeliveryError(f"Async AMR UDS timeout after {effective_timeout}s") from e
        except (ConnectionResetError, BrokenPipeError, ConnectionError) as e:
            raise TransientDeliveryError(f"Async AMR UDS connection dropped before response: {e}") from e
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


class MockTransport(BaseTransport):
    """
    Mock transport for tests and simulations.
    Supports fault injection, latency, call tracking, and customizable response handlers.
    """

    def __init__(
        self,
        handler: Optional[Callable[[str, str, Optional[str], List[Dict[str, Any]]], Dict[str, Any]]] = None,
    ):
        self.handler = handler
        self.calls: List[Dict[str, Any]] = []
        self.fail_with: Optional[Exception] = None
        self.timeout_after_commit: bool = False
        self.committed_store: List[Dict[str, Any]] = []

    def set_handler(self, handler: Callable[..., Dict[str, Any]]) -> None:
        self.handler = handler

    def send_ingest(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        call_record = {
            "session_id": session_id,
            "agent_id": agent_id,
            "project_id": project_id,
            "messages": messages,
            "timestamp": time.time(),
        }
        self.calls.append(call_record)

        if self.timeout_after_commit:
            # Simulate server commits to DB but drops connection or times out before ack reaches client
            self.committed_store.append(call_record)
            raise TransientDeliveryError("Simulated socket timeout after server committed to DB")

        if self.fail_with is not None:
            raise self.fail_with

        if self.handler is not None:
            return self.handler(session_id, agent_id, project_id, messages)

        # Default standard AMR success response
        return {
            "session_id": session_id,
            "total_received": len(messages),
            "inserted": len(messages),
            "revision_updated": 0,
            "ignored": 0,
            "status": "success",
        }

    async def async_send_ingest(
        self,
        session_id: str,
        agent_id: str,
        project_id: Optional[str],
        messages: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        return self.send_ingest(session_id, agent_id, project_id, messages, timeout)

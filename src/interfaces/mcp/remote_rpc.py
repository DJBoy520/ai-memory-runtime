"""Remote daemon RPC adapter over an explicit business Unix socket.

The remote MCP side deliberately does not know how the local daemon is
reached.  :class:`RemoteRpc` is the small async callable adapter it uses:

* the caller must pass the business socket path explicitly -- this module
  never guesses a socket and never falls back to a production default;
* connect, write and read operations each have their own timeout;
* the writer is always closed, including on error paths;
* a daemon JSON-RPC ``error`` response is raised as :class:`DaemonRpcError`
  instead of being converted into a successful result.

Only the existing length-prefixed JSON-RPC helpers from
:mod:`src.interfaces.ipc.protocol` are used for framing.
"""

from __future__ import annotations

import asyncio
import itertools
import math
import os
from collections.abc import Mapping
from typing import Any, Optional, Union

from src.interfaces.ipc.protocol import (
    ConnectionClosedError,
    FrameDecodeError,
    FrameTooLargeError,
    MAX_REQUEST_BYTES,
    make_jsonrpc_request,
    read_frame,
    write_frame,
)

__all__ = [
    "DaemonRpcError",
    "RemoteRpc",
    "RemoteRpcAdapter",
    "RemoteRpcConnectionError",
    "RemoteRpcError",
    "RemoteRpcProtocolError",
    "RemoteRpcTimeoutError",
    "make_remote_rpc",
]

DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_WRITE_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 120.0  # BGE-M3 cold starts can take tens of seconds.
DEFAULT_CLOSE_TIMEOUT = 1.0

RequestId = Union[int, str]
Params = Optional[Union[Mapping[str, Any], list, tuple]]


class RemoteRpcError(Exception):
    """Base class for transport/protocol failures in the RPC adapter."""


class RemoteRpcTimeoutError(RemoteRpcError, TimeoutError):
    """Connecting, writing or reading exceeded its configured timeout."""


class RemoteRpcConnectionError(RemoteRpcError, ConnectionError):
    """The socket could not be opened or was closed before a response arrived."""


class RemoteRpcProtocolError(RemoteRpcError):
    """The daemon sent a frame that is not a valid JSON-RPC response."""


class DaemonRpcError(RemoteRpcError):
    """A JSON-RPC error response returned by the daemon.

    ``code``, ``message`` and ``data`` are preserved exactly so callers can
    map them onto their own error contract.  This exception is always raised;
    an error response is never returned as if it were a successful result.
    """

    def __init__(
        self,
        code: int,
        message: str,
        data: Optional[Any] = None,
    ) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.data = data


def _validate_timeout(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    if not math.isfinite(value):
        # NaN/inf would silently disable the timeout in asyncio.wait_for.
        raise ValueError(f"{name} must be a finite number")
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return float(value)


class RemoteRpc:
    """Async ``rpc(method, params)`` callable backed by one business UDS.

    A fresh connection is opened per call, which keeps the adapter safe to use
    from concurrent remote requests without sharing a socket or request stream.
    The path is intentionally required: callers should pass
    ``config.server.business_socket`` (or an isolated test socket), never an
    admin socket or an implicit production default.
    """

    def __init__(
        self,
        socket_path: Union[str, os.PathLike[str]],
        *,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        write_timeout: float = DEFAULT_WRITE_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
        close_timeout: float = DEFAULT_CLOSE_TIMEOUT,
        max_frame_bytes: int = MAX_REQUEST_BYTES,
    ) -> None:
        if isinstance(socket_path, os.PathLike):
            socket_path = os.fspath(socket_path)
        if not isinstance(socket_path, str) or not socket_path.strip():
            raise ValueError("socket_path must be a non-empty business socket path")

        if isinstance(max_frame_bytes, bool) or not isinstance(max_frame_bytes, int):
            raise TypeError("max_frame_bytes must be an integer")
        if max_frame_bytes <= 0:
            raise ValueError("max_frame_bytes must be greater than zero")

        self.socket_path = socket_path
        self.connect_timeout = _validate_timeout("connect_timeout", connect_timeout)
        self.write_timeout = _validate_timeout("write_timeout", write_timeout)
        self.read_timeout = _validate_timeout("read_timeout", read_timeout)
        self.close_timeout = _validate_timeout("close_timeout", close_timeout)
        self.max_frame_bytes = max_frame_bytes
        self._request_ids = itertools.count(1)

    async def __call__(
        self,
        method: str,
        params: Params = None,
        *,
        req_id: Optional[RequestId] = None,
    ) -> Any:
        """Send one JSON-RPC request and return its ``result``.

        Raises:
            ValueError: invalid method or params supplied by the caller.
            RemoteRpcTimeoutError: connect/write/read timeout.
            RemoteRpcConnectionError: socket cannot be opened or was closed.
            RemoteRpcProtocolError: malformed response.
            DaemonRpcError: daemon returned a JSON-RPC ``error`` object.
        """
        if not isinstance(method, str) or not method:
            raise ValueError("method must be a non-empty string")

        request_params: Optional[Union[dict[str, Any], list, tuple]]
        if params is None:
            request_params = None
        elif isinstance(params, Mapping):
            request_params = dict(params)
        elif isinstance(params, (list, tuple)):
            request_params = params
        else:
            raise ValueError("params must be a mapping, list, tuple or None")

        if req_id is None:
            req_id = next(self._request_ids)

        request = make_jsonrpc_request(method, request_params, req_id=req_id)
        reader, writer = await self._connect(method)
        try:
            await self._write_request(writer, request, method)
            response = await self._read_response(reader, method)
            return self._extract_result(response, req_id, method)
        finally:
            await self._close_writer(writer)

    async def _connect(
        self, method: str
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            return await asyncio.wait_for(
                asyncio.open_unix_connection(self.socket_path),
                timeout=self.connect_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise RemoteRpcTimeoutError(
                f"Timed out connecting to business RPC socket "
                f"{self.socket_path!r} after {self.connect_timeout:g}s "
                f"for {method}"
            ) from exc
        except (FileNotFoundError, ConnectionRefusedError, OSError) as exc:
            raise RemoteRpcConnectionError(
                f"Failed to connect to business RPC socket "
                f"{self.socket_path!r} for {method}: {exc}"
            ) from exc

    async def _write_request(
        self,
        writer: asyncio.StreamWriter,
        request: dict[str, Any],
        method: str,
    ) -> None:
        try:
            await asyncio.wait_for(
                write_frame(writer, request, max_bytes=self.max_frame_bytes),
                timeout=self.write_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise RemoteRpcTimeoutError(
                f"Timed out writing {method} request to business RPC socket "
                f"{self.socket_path!r} after {self.write_timeout:g}s"
            ) from exc
        except (FrameTooLargeError, FrameDecodeError) as exc:
            raise RemoteRpcProtocolError(
                f"Invalid request frame for {method} to business RPC socket "
                f"{self.socket_path!r}: {exc}"
            ) from exc
        except (ConnectionError, OSError) as exc:
            raise RemoteRpcConnectionError(
                f"Connection lost while writing {method} request to business "
                f"RPC socket {self.socket_path!r}: {exc}"
            ) from exc

    async def _read_response(
        self, reader: asyncio.StreamReader, method: str
    ) -> dict[str, Any]:
        try:
            response = await asyncio.wait_for(
                read_frame(reader, max_bytes=self.max_frame_bytes),
                timeout=self.read_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise RemoteRpcTimeoutError(
                f"Timed out reading {method} response from business RPC "
                f"socket {self.socket_path!r} after {self.read_timeout:g}s"
            ) from exc
        except ConnectionClosedError as exc:
            raise RemoteRpcConnectionError(
                f"Business RPC socket {self.socket_path!r} closed before "
                f"returning a response for {method}: {exc}"
            ) from exc
        except (FrameDecodeError, FrameTooLargeError) as exc:
            raise RemoteRpcProtocolError(
                f"Invalid response frame for {method} from business RPC "
                f"socket {self.socket_path!r}: {exc}"
            ) from exc
        except (ConnectionError, OSError) as exc:
            raise RemoteRpcConnectionError(
                f"Connection lost while reading {method} response from "
                f"business RPC socket {self.socket_path!r}: {exc}"
            ) from exc

        if not isinstance(response, dict):
            raise RemoteRpcProtocolError(
                f"Expected a JSON-RPC object for {method}, got "
                f"{type(response).__name__}"
            )
        return response

    @staticmethod
    def _extract_result(
        response: dict[str, Any], req_id: RequestId, method: str
    ) -> Any:
        # Errors take precedence: never allow an error response to be returned
        # as an ordinary result, even if its id or other fields are malformed.
        if "error" in response and response["error"] is not None:
            error = response["error"]
            if not isinstance(error, dict):
                raise DaemonRpcError(-32603, str(error))
            code = error.get("code", -32603)
            message = error.get("message", "")
            raise DaemonRpcError(code, str(message), error.get("data"))

        if "result" not in response:
            raise RemoteRpcProtocolError(
                f"JSON-RPC response for {method} has neither result nor error"
            )

        if response.get("jsonrpc") != "2.0":
            raise RemoteRpcProtocolError(
                f"JSON-RPC response for {method} has invalid version "
                f"{response.get('jsonrpc')!r}"
            )

        if response.get("id") != req_id:
            raise RemoteRpcProtocolError(
                f"JSON-RPC response id mismatch for {method}: expected "
                f"{req_id!r}, got {response.get('id')!r}"
            )

        return response["result"]

    async def _close_writer(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        try:
            await asyncio.wait_for(
                writer.wait_closed(), timeout=self.close_timeout
            )
        except (asyncio.TimeoutError, ConnectionError, OSError):
            # Closing is best effort; the original operation failure, if any,
            # must be the exception the caller observes.
            pass


# A descriptive alias for callers that treat this as the backend adapter.
RemoteRpcAdapter = RemoteRpc


def make_remote_rpc(
    socket_path: Union[str, os.PathLike[str]],
    *,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    write_timeout: float = DEFAULT_WRITE_TIMEOUT,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    close_timeout: float = DEFAULT_CLOSE_TIMEOUT,
    max_frame_bytes: int = MAX_REQUEST_BYTES,
) -> RemoteRpc:
    """Build a :class:`RemoteRpc` for an explicitly supplied business socket."""
    return RemoteRpc(
        socket_path,
        connect_timeout=connect_timeout,
        write_timeout=write_timeout,
        read_timeout=read_timeout,
        close_timeout=close_timeout,
        max_frame_bytes=max_frame_bytes,
    )

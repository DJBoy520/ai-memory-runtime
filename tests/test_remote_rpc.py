"""Unix socket RPC adapter tests.

These tests start real ``asyncio`` Unix-domain-socket servers in a temporary
directory.  They never connect to the production daemon socket.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Awaitable, Callable, Dict, List, Optional

import pytest

from src.interfaces.ipc.protocol import (
    FrameTooLargeError,
    make_jsonrpc_request,
    read_frame,
    write_frame,
)
from src.interfaces.mcp.remote_rpc import (
    DaemonRpcError,
    RemoteRpc,
    RemoteRpcError,
    RemoteRpcProtocolError,
    make_remote_rpc,
)

ConnectionHandler = Callable[
    [asyncio.StreamReader, asyncio.StreamWriter], Awaitable[Optional[None]]
]


class _SocketServer:
    """Small real UDS JSON-RPC server used by the adapter tests."""

    def __init__(self, handler: ConnectionHandler) -> None:
        self._handler = handler
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self, path: str) -> None:
        async def _connected(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                await self._handler(reader, writer)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

        self._server = await asyncio.start_unix_server(_connected, path=path)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


@pytest.fixture
async def uds_server(tmp_path):
    servers: List[_SocketServer] = []

    async def _start(handler: ConnectionHandler) -> tuple[_SocketServer, str]:
        path = str(tmp_path / f"business-{len(servers)}.sock")
        server = _SocketServer(handler)
        await server.start(path)
        servers.append(server)
        return server, path

    yield _start

    for server in servers:
        await server.stop()


async def test_call_writes_expected_jsonrpc_frame_and_returns_result(uds_server):
    received: List[Dict[str, Any]] = []

    async def handler(reader, writer):
        request = await read_frame(reader)
        received.append(request)
        await write_frame(
            writer,
            {
                "jsonrpc": "2.0",
                "result": {"items": ["memory-1"], "total": 1},
                "id": request["id"],
            },
        )

    _, path = await uds_server(handler)
    rpc = RemoteRpc(path, connect_timeout=1.0, write_timeout=1.0, read_timeout=1.0)

    result = await rpc("memory.search", {"query": "SM4", "limit": 1})

    assert result == {"items": ["memory-1"], "total": 1}
    assert received == [
        make_jsonrpc_request(
            "memory.search", {"query": "SM4", "limit": 1}, req_id=1
        )
    ]


async def test_daemon_jsonrpc_error_is_raised_not_wrapped_as_success(uds_server):
    async def handler(reader, writer):
        request = await read_frame(reader)
        await write_frame(
            writer,
            {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32002,
                    "message": "VERSION_CONFLICT: stale update",
                    "data": {
                        "memory_id": "memory-1",
                        "current_version": 3,
                        "expected_version": 2,
                    },
                },
                "id": request["id"],
            },
        )

    _, path = await uds_server(handler)
    rpc = RemoteRpc(path, read_timeout=1.0)

    with pytest.raises(DaemonRpcError) as exc_info:
        await rpc("memory.update", {"memory_id": "memory-1", "expected_version": 2})

    error = exc_info.value
    assert error.code == -32002
    assert error.message == "VERSION_CONFLICT: stale update"
    assert error.data == {
        "memory_id": "memory-1",
        "current_version": 3,
        "expected_version": 2,
    }


async def test_read_timeout_raises_and_closes_writer(uds_server):
    server_saw_eof = asyncio.Event()

    async def handler(reader, writer):
        await read_frame(reader)
        # Do not answer.  The adapter's read timeout should close its writer,
        # which the server observes as EOF.
        await reader.read()
        server_saw_eof.set()

    _, path = await uds_server(handler)
    rpc = RemoteRpc(path, read_timeout=0.05, close_timeout=0.5)

    with pytest.raises(TimeoutError):
        await rpc("memory.get", {"memory_id": "memory-1"})

    await asyncio.wait_for(server_saw_eof.wait(), timeout=2.0)


async def test_abnormal_server_close_raises(uds_server):
    async def handler(reader, writer):
        await read_frame(reader)
        # Close without sending a JSON-RPC response.
        writer.close()
        await writer.wait_closed()

    _, path = await uds_server(handler)
    rpc = RemoteRpc(path, read_timeout=1.0)

    with pytest.raises(RemoteRpcError):
        await rpc("memory.get", {"memory_id": "memory-1"})


async def test_socket_path_must_be_explicit(tmp_path):
    with pytest.raises(TypeError):
        RemoteRpc()  # type: ignore[call-arg]

    with pytest.raises(ValueError):
        RemoteRpc("")

    rpc = RemoteRpc(str(tmp_path / "business.sock"))
    with pytest.raises(RemoteRpcError):
        await rpc("memory.get", {"memory_id": "memory-1"})


# --- Regression: non-finite timeouts must be rejected ---------------------

_TIMEOUT_FIELDS = ("connect_timeout", "write_timeout", "read_timeout", "close_timeout")


@pytest.mark.parametrize("bad_timeout", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("field", _TIMEOUT_FIELDS)
def test_non_finite_timeout_is_rejected(tmp_path, field, bad_timeout):
    socket_path = str(tmp_path / "business.sock")

    with pytest.raises(ValueError):
        RemoteRpc(socket_path, **{field: bad_timeout})

    with pytest.raises(ValueError):
        make_remote_rpc(socket_path, **{field: bad_timeout})


@pytest.mark.parametrize("bad_timeout", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_timeout_never_reaches_asyncio(tmp_path, bad_timeout):
    # ``asyncio.wait_for`` silently accepts NaN/inf; the adapter must reject
    # them at construction time instead of deferring an unbounded wait.
    with pytest.raises(ValueError):
        RemoteRpc(str(tmp_path / "business.sock"), read_timeout=bad_timeout)


def test_finite_timeouts_are_still_accepted(tmp_path):
    rpc = RemoteRpc(
        str(tmp_path / "business.sock"),
        connect_timeout=1,
        write_timeout=0.25,
        read_timeout=30.0,
        close_timeout=1.0,
    )
    assert rpc.connect_timeout == 1.0
    assert rpc.write_timeout == 0.25
    assert rpc.read_timeout == 30.0
    assert math.isfinite(rpc.close_timeout)


# --- Regression: oversize request frames map to protocol errors -----------


async def test_oversize_write_frame_maps_to_protocol_error(uds_server):
    async def handler(reader, writer):
        # Consume until the adapter closes; the request is rejected locally
        # before any bytes are written, so this simply observes EOF.
        await reader.read()

    _, path = await uds_server(handler)
    rpc = RemoteRpc(path, max_frame_bytes=32, read_timeout=1.0)

    with pytest.raises(RemoteRpcProtocolError) as exc_info:
        await rpc("memory.search", {"query": "x" * 512})

    assert "frame" in str(exc_info.value).lower()
    assert isinstance(exc_info.value.__cause__, FrameTooLargeError)

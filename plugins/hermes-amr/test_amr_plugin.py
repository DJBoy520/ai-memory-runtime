"""Unit tests for AMR (AI Memory Runtime) Hermes memory plugin."""

import json
import os
import socket
import struct
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    from plugins.memory.amr._client import AmrUdsClient, _recv_exact
    from plugins.memory.amr import AmrMemoryProvider, register_memory_provider
except ImportError:
    import sys
    _pkg_dir = Path(__file__).resolve().parent
    if str(_pkg_dir) not in sys.path:
        sys.path.insert(0, str(_pkg_dir))
    from _client import AmrUdsClient, _recv_exact
    from __init__ import AmrMemoryProvider, register_memory_provider


class MockUdsServer:
    """Mock UDS server for testing JSON-RPC framing."""

    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.running = False
        self.thread = None
        self.received_requests = []
        self.handler = self._default_handler

    def _default_handler(self, req: dict) -> dict:
        method = req.get("method")
        req_id = req.get("id")
        if method == "memory.search":
            return {
                "jsonrpc": "2.0",
                "result": {
                    "results": [
                        {"content": "Standard SM2/SM3/SM4 cryptographic algorithms", "score": 0.95},
                        {"content": "Owner preference: strict 300ms prefetch timeout", "score": 0.91},
                    ],
                    "total": 2,
                },
                "id": req_id,
            }
        elif method == "session.ingest":
            return {"jsonrpc": "2.0", "result": {"status": "ok"}, "id": req_id}
        elif method == "memory.create":
            return {"jsonrpc": "2.0", "result": {"memory_id": "mem_123", "version": 1, "status": "ACTIVE"}, "id": req_id}
        elif method == "memory.record":
            return {"jsonrpc": "2.0", "result": {"id": "rec_123"}, "id": req_id}
        return {"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": req_id}

    def start(self):
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)
        self.sock.bind(self.socket_path)
        self.sock.listen(5)
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            try:
                conn, _ = self.sock.accept()
            except Exception:
                break
            try:
                with conn:
                    header = _recv_exact(conn, 4)
                    (msg_len,) = struct.unpack(">I", header)
                    body = _recv_exact(conn, msg_len)
                    req = json.loads(body.decode("utf-8"))
                    self.received_requests.append(req)

                    resp = self.handler(req)
                    resp_data = json.dumps(resp).encode("utf-8")
                    conn.sendall(struct.pack(">I", len(resp_data)) + resp_data)
            except Exception:
                pass

    def stop(self):
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass
        if os.path.exists(self.socket_path):
            try:
                os.remove(self.socket_path)
            except Exception:
                pass


@pytest.fixture
def mock_server(tmp_path):
    sock_path = str(tmp_path / "test_amr.sock")
    server = MockUdsServer(sock_path)
    server.start()
    yield server
    server.stop()


def test_client_framing_and_exact_recv(mock_server):
    time.sleep(0.05)
    client = AmrUdsClient(socket_path=mock_server.socket_path)
    assert client.is_available() is True

    result = client.call("memory.search", {"query": "国密标准"})
    assert isinstance(result, dict)
    items = result.get("results")
    assert len(items) == 2
    assert "Standard SM2/SM3/SM4" in items[0]["content"]

    assert len(mock_server.received_requests) == 1
    req = mock_server.received_requests[0]
    assert req["method"] == "memory.search"
    assert req["params"]["query"] == "国密标准"


def test_client_fail_open_nonexistent_socket(tmp_path):
    client = AmrUdsClient(socket_path=str(tmp_path / "nonexistent.sock"))
    assert client.is_available() is False
    res = client.call("memory.search", {"query": "test"})
    assert res is None


def test_provider_registration_and_meta():
    provider = register_memory_provider()
    assert isinstance(provider, AmrMemoryProvider)
    assert provider.name == "amr"
    assert provider.get_tool_schemas() == []


def test_provider_lifecycle(mock_server, tmp_path):
    provider = AmrMemoryProvider()
    provider._hermes_home = tmp_path
    provider._socket_path = mock_server.socket_path

    # Initialize
    provider.initialize(session_id="test_sess_01")
    provider._client = AmrUdsClient(socket_path=mock_server.socket_path)

    # Queue prefetch & prefetch
    provider.queue_prefetch("国密算法标准")
    context = provider.prefetch("国密算法标准")

    assert "### [AI Memory Runtime: Recalled Facts]" in context
    assert "Standard SM2/SM3/SM4" in context
    assert provider.recall_status() is not None
    assert provider.recall_status().count == 2
    assert provider.recall_status().provider_label == "amr"

    # Sync turn
    provider.sync_turn(
        user_content="请简述国密SM4",
        assistant_content="SM4是一种分组密码算法...",
        session_id="test_sess_01",
    )

    # Allow worker thread to execute
    time.sleep(0.1)

    methods_called = [r["method"] for r in mock_server.received_requests]
    assert "session.ingest" in methods_called
    assert "memory.create" in methods_called

    # Check payload parameters
    for r in mock_server.received_requests:
        if r["method"] == "session.ingest":
            assert r["params"]["agent_id"] == "hermes"
            assert r["params"]["session_id"] == "test_sess_01"
            assert len(r["params"]["messages"]) == 2
        if r["method"] == "memory.create":
            assert r["params"]["agent_id"] == "hermes"
            assert r["params"]["status"] == "ACTIVE"
            assert "User: 请简述国密SM4" in r["params"]["content"]

    provider.shutdown()


def test_provider_prefetch_timeout(tmp_path):
    # Server that hangs to trigger timeout
    sock_path = str(tmp_path / "hanging.sock")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(sock_path)
    sock.listen(1)

    provider = AmrMemoryProvider()
    provider.initialize(session_id="test_timeout")
    provider._socket_path = sock_path
    provider._client = AmrUdsClient(socket_path=sock_path)

    def hang_handler():
        try:
            conn, _ = sock.accept()
            time.sleep(1.0)
            conn.close()
        except Exception:
            pass

    t = threading.Thread(target=hang_handler, daemon=True)
    t.start()

    provider.queue_prefetch("query")
    start_t = time.time()
    res = provider.prefetch("query")
    elapsed = time.time() - start_t

    # Must finish around 0.3s without blocking indefinitely
    assert res == ""
    assert elapsed < 0.6

    sock.close()
    provider.shutdown()

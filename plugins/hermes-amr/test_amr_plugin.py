"""Unit tests for AMR (AI Memory Runtime) 2.x Hermes memory plugin."""

import json
import os
import socket
import struct
import sys
import threading
import time
from itertools import count
from pathlib import Path

import pytest

try:
    from plugins.memory.amr._client import (  # type: ignore
        AmrProtocolError,
        AmrAuthError,
        AmrRpcError,
        AmrUdsClient,
        DEFAULT_SOCKET_PATH,
        _recv_exact,
        is_transient,
        resolve_socket_path,
    )
    from plugins.memory.amr._spool import IngestSpool, compute_record_key
    from plugins.memory.amr import AmrMemoryProvider, register_memory_provider
except ImportError:
    _pkg_dir = Path(__file__).resolve().parent
    if str(_pkg_dir) not in sys.path:
        sys.path.insert(0, str(_pkg_dir))
    from _client import (  # type: ignore
        AmrProtocolError,
        AmrAuthError,
        AmrRpcError,
        AmrUdsClient,
        DEFAULT_SOCKET_PATH,
        _recv_exact,
        is_transient,
        resolve_socket_path,
    )
    from _spool import IngestSpool, compute_record_key  # type: ignore
    from __init__ import AmrMemoryProvider, register_memory_provider  # type: ignore


def wait_until(predicate, timeout=3.0, interval=0.01):
    """Poll a predicate until it is true or the deadline expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


class MockUdsServer:
    """Mock UDS server for testing JSON-RPC framing."""

    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.running = False
        self.thread = None
        self.received_requests = []
        self.handler = self._default_handler

    def _default_handler(self, req: dict):
        method = req.get("method")
        req_id = req.get("id")
        if method == "amr.ping":
            return {"jsonrpc": "2.0", "result": {"status": "ok"}, "id": req_id}
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
                    if isinstance(resp, (bytes, bytearray)):
                        resp_data = bytes(resp)
                    else:
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


class FakeAmrClient:
    """In-process fake 2.x adapter with injectable ingest failures."""

    def __init__(self):
        self.calls = []
        self.lock = threading.Lock()
        self.ingest_errors = []
        self.ping_result = {"status": "ok"}
        self.ping_error = None
        self.ping_calls = 0
        self.search_result = {
            "results": [
                {"content": "Recalled fact one", "score": 0.9},
                {"content": "Recalled fact two", "score": 0.8},
            ]
        }
        self.create_result = {"memory_id": "mem_fake", "version": 1}
        self.record_result = {"id": "rec_fake"}
        self._ids = count(1)

    def ping(self, timeout=0.3):
        self.ping_calls += 1
        req_id = next(self._ids)
        self._record("amr.ping", {}, timeout, req_id)
        if self.ping_error is not None:
            raise self.ping_error
        return self.ping_result

    def _record(self, method, params, timeout, req_id):
        with self.lock:
            self.calls.append(
                {
                    "method": method,
                    "params": params or {},
                    "timeout": timeout,
                    "id": req_id,
                }
            )

    def _dispatch(self, method, params, timeout, req_id):
        if req_id is None:
            req_id = next(self._ids)
        self._record(method, params, timeout, req_id)
        if method == "amr.ping":
            self.ping_calls += 1
            if self.ping_error is not None:
                raise self.ping_error
            return self.ping_result
        if method == "memory.search":
            return self.search_result
        if method == "session.ingest":
            with self.lock:
                errors = list(self.ingest_errors)
            if errors:
                with self.lock:
                    self.ingest_errors.pop(0)
                raise errors[0]
            return {"status": "ok"}
        if method == "memory.create":
            return self.create_result
        if method == "memory.record":
            return self.record_result
        raise AmrRpcError(-32601, "Method not found")

    def request(self, method, params=None, timeout=None, req_id=None):
        return self._dispatch(method, params or {}, timeout, req_id)

    def call(self, method, params=None, timeout=None, req_id=None):
        try:
            return self._dispatch(method, params or {}, timeout, req_id)
        except Exception:
            return None

    def count(self, method):
        with self.lock:
            return sum(1 for call in self.calls if call["method"] == method)

    def params_for(self, method):
        with self.lock:
            return [call["params"] for call in self.calls if call["method"] == method]


@pytest.fixture(autouse=True)
def isolate_environment(monkeypatch, tmp_path):
    """Keep tests away from the real adapter socket and real spool directory."""
    for var in ("AMR2_SOCKET_PATH", "AMR_SOCKET_PATH", "XDG_RUNTIME_DIR"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AMR_HERMES_SPOOL_DIR", str(tmp_path / "default-spool"))
    yield


@pytest.fixture
def mock_server(tmp_path):
    sock_path = str(tmp_path / "test_amr.sock")
    server = MockUdsServer(sock_path)
    server.start()
    yield server
    server.stop()


# --------------------------------------------------------------------------
# socket resolution / protocol
# --------------------------------------------------------------------------
def test_default_socket_is_amr2_hermes():
    assert DEFAULT_SOCKET_PATH.endswith("amr2-hermes.sock")
    assert "qdrant-bge" not in DEFAULT_SOCKET_PATH


def test_resolve_socket_path_priority(monkeypatch):
    monkeypatch.setenv("AMR_SOCKET_PATH", "/legacy/amr.sock")
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/xdg-runtime")
    assert resolve_socket_path("/explicit.sock") == "/explicit.sock"

    monkeypatch.setenv("AMR2_SOCKET_PATH", "/amr2/override.sock")
    assert resolve_socket_path("/explicit.sock") == "/amr2/override.sock"

    monkeypatch.delenv("AMR2_SOCKET_PATH")
    monkeypatch.setenv("AMR_SOCKET_PATH", "/legacy/amr.sock")
    assert resolve_socket_path(None) == "/legacy/amr.sock"

    monkeypatch.delenv("AMR_SOCKET_PATH")
    assert resolve_socket_path(None) == "/xdg-runtime/amr2-hermes.sock"

    monkeypatch.delenv("XDG_RUNTIME_DIR")
    resolved = resolve_socket_path(None)
    if sys.platform.startswith("linux"):
        assert resolved == f"/run/user/{os.getuid()}/amr2-hermes.sock"
    else:
        assert resolved == "/tmp/amr2-hermes.sock"


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


def test_client_ping(mock_server):
    client = AmrUdsClient(socket_path=mock_server.socket_path)
    assert client.ping() == {"status": "ok"}
    assert mock_server.received_requests[-1]["method"] == "amr.ping"


def test_client_request_error_and_ids(mock_server):
    def handler(req):
        return {
            "jsonrpc": "2.0",
            "error": {"code": -32005, "message": "not found"},
            "id": req.get("id"),
        }

    mock_server.handler = handler
    client = AmrUdsClient(socket_path=mock_server.socket_path)
    with pytest.raises(AmrRpcError) as excinfo:
        client.request("memory.search", {"query": "x"})
    assert excinfo.value.code == -32005

    first = mock_server.received_requests[-1]["id"]
    with pytest.raises(AmrRpcError):
        client.request("memory.search", {"query": "x"})
    second = mock_server.received_requests[-1]["id"]
    assert isinstance(first, int) and isinstance(second, int)
    assert second > first


def test_client_fail_open_nonexistent_socket(tmp_path):
    client = AmrUdsClient(socket_path=str(tmp_path / "nonexistent.sock"))
    assert client.is_available() is False
    res = client.call("memory.search", {"query": "test"})
    assert res is None


def test_client_malformed_response_is_protocol_error(mock_server):
    mock_server.handler = lambda req: b"{this is not json"
    client = AmrUdsClient(socket_path=mock_server.socket_path)
    with pytest.raises(AmrProtocolError) as excinfo:
        client.request("memory.search", {"query": "x"})
    assert excinfo.value.code == -32011
    assert client.call("memory.search", {"query": "x"}) is None


def test_is_transient_classification():
    assert is_transient(AmrRpcError(-32010, "offline")) is True
    assert is_transient(AmrRpcError(-32003, "loading")) is True
    assert is_transient(AmrRpcError(-32004, "queue full")) is True
    assert is_transient(AmrRpcError(-32012, "auth")) is False
    assert is_transient(AmrRpcError(-32011, "protocol")) is False


# --------------------------------------------------------------------------
# provider
# --------------------------------------------------------------------------
def test_provider_registration_and_meta():
    provider = register_memory_provider()
    assert isinstance(provider, AmrMemoryProvider)
    assert provider.name == "amr"
    assert provider.get_tool_schemas() == []


def test_provider_lifecycle(mock_server, tmp_path):
    provider = AmrMemoryProvider()
    provider._hermes_home = tmp_path

    # Initialize
    provider.initialize(
        session_id="test_sess_01",
        socket_path=mock_server.socket_path,
        spool_dir=str(tmp_path / "spool"),
    )

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

    assert wait_until(
        lambda: "session.ingest" in [r["method"] for r in mock_server.received_requests]
        and "memory.create" in [r["method"] for r in mock_server.received_requests]
    )
    assert wait_until(lambda: provider._spool.pending() == [])

    # 2.x: no caller-supplied identity fields anywhere.
    for r in mock_server.received_requests:
        assert "agent_id" not in r["params"]
        assert "source_agent" not in r["params"]
        if r["method"] == "session.ingest":
            assert r["params"]["session_id"] == "test_sess_01"
            assert len(r["params"]["messages"]) == 2
            assert r["id"] is not None
        if r["method"] == "memory.create":
            assert r["params"]["status"] == "ACTIVE"
            assert "User: 请简述国密SM4" in r["params"]["content"]

    assert "agent_id" not in mock_server.received_requests[0]["params"]
    provider.shutdown()


def test_fake_adapter_ping_async(tmp_path):
    fake = FakeAmrClient()
    provider = AmrMemoryProvider()
    provider.initialize(
        session_id="ping_sess",
        client=fake,
        spool_dir=str(tmp_path / "spool"),
    )
    assert wait_until(lambda: fake.ping_calls >= 1)
    provider.shutdown()


def test_fake_adapter_search_has_no_identity_fields(tmp_path):
    fake = FakeAmrClient()
    provider = AmrMemoryProvider()
    provider.initialize(
        session_id="search_sess",
        client=fake,
        spool_dir=str(tmp_path / "spool"),
    )

    provider.queue_prefetch("meaningful query")
    context = provider.prefetch("meaningful query")

    assert "Recalled fact one" in context
    searches = fake.params_for("memory.search")
    assert searches
    for params in searches:
        assert "agent_id" not in params
        assert "source_agent" not in params
        assert params["query"] == "meaningful query"
        assert "project_id" in params
    provider.shutdown()


def test_fake_adapter_ingest_ack_clears_spool(tmp_path):
    fake = FakeAmrClient()
    provider = AmrMemoryProvider()
    provider.initialize(
        session_id="ack_sess",
        client=fake,
        spool_dir=str(tmp_path / "spool"),
    )

    provider.sync_turn(
        user_content="hello",
        assistant_content="world",
        session_id="ack_sess",
    )

    assert wait_until(lambda: fake.count("session.ingest") == 1)
    ingest_calls = [
        call for call in fake.calls if call["method"] == "session.ingest"
    ]
    assert ingest_calls[0]["id"] is not None
    assert "agent_id" not in ingest_calls[0]["params"]
    assert "source_agent" not in ingest_calls[0]["params"]
    assert wait_until(lambda: provider._spool.pending() == [])
    provider.shutdown()


def test_fake_adapter_transient_error_kept_and_replayed(tmp_path):
    fake = FakeAmrClient()
    fake.ingest_errors = [AmrRpcError(-32010, "adapter unavailable")]
    provider = AmrMemoryProvider()
    provider.initialize(
        session_id="transient_sess",
        client=fake,
        spool_dir=str(tmp_path / "spool"),
    )

    provider.sync_turn(
        user_content="persist me",
        assistant_content="despite offline",
        session_id="transient_sess",
    )

    assert wait_until(lambda: len(provider._spool.pending()) == 1)
    pending = provider._spool.pending()
    assert pending[0]["key"].startswith("transient_sess:")
    assert not pending[0].get("permanent")
    first_calls = fake.count("session.ingest")

    # Adapter recovers: a non-blocking replay drains the pending record.
    assert wait_until(lambda: not provider._replay_running)
    for _ in range(50):
        provider._schedule_replay()
        if wait_until(
            lambda: fake.count("session.ingest") > first_calls
            and provider._spool.pending() == [],
            timeout=0.2,
        ):
            break
        time.sleep(0.01)
    assert fake.count("session.ingest") > first_calls
    assert provider._spool.pending() == []
    replayed = [
        call for call in fake.calls if call["method"] == "session.ingest"
    ][-1]
    assert replayed["id"] is not None
    provider.shutdown()


def test_fake_adapter_auth_error_attempted_once(tmp_path):
    fake = FakeAmrClient()
    fake.ingest_errors = [AmrRpcError(-32012, "remote auth failed")]
    provider = AmrMemoryProvider()
    provider.initialize(
        session_id="auth_sess",
        client=fake,
        spool_dir=str(tmp_path / "spool"),
    )

    provider.sync_turn(
        user_content="never accepted",
        assistant_content="permanent",
        session_id="auth_sess",
    )

    assert wait_until(
        lambda: len(provider._spool.pending()) == 1
        and provider._spool.pending()[0].get("permanent") is True
    )
    attempts_after_direct_send = fake.count("session.ingest")

    # A replay sweep must skip the permanently failed record.
    summary = provider._spool.replay(fake, deadline_s=0.5)
    assert summary["attempted"] == 0
    assert summary["skipped_records"] == 1
    assert fake.count("session.ingest") == attempts_after_direct_send
    provider.shutdown()


def test_provider_survives_malformed_adapter(tmp_path):
    sock_path = str(tmp_path / "malformed.sock")
    server = MockUdsServer(sock_path)
    server.handler = lambda req: b"\xff not json"
    server.start()
    try:
        provider = AmrMemoryProvider()
        provider._client = AmrUdsClient(socket_path=sock_path)
        assert provider.prefetch("meaningful query") == ""
        assert provider.recall_status() is None
    finally:
        server.stop()
        provider.shutdown()


def test_provider_survives_jsonrpc_error_response(tmp_path):
    sock_path = str(tmp_path / "rpc-error.sock")
    server = MockUdsServer(sock_path)
    server.handler = lambda req: {
        "jsonrpc": "2.0",
        "error": {"code": -32603, "message": "internal error"},
        "id": req.get("id"),
    }
    server.start()
    try:
        provider = AmrMemoryProvider()
        provider._client = AmrUdsClient(socket_path=sock_path)
        assert provider.prefetch("meaningful query") == ""
    finally:
        server.stop()
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


# --------------------------------------------------------------------------
# spool
# --------------------------------------------------------------------------
def test_spool_key_without_message_ids_is_stable_and_distinct(tmp_path):
    spool = IngestSpool(persistence_dir=str(tmp_path / "spool"))
    payload_a = {
        "session_id": "sess",
        "messages": [{"role": "user", "content": "alpha"}],
    }
    payload_b = {
        "session_id": "sess",
        "messages": [{"role": "user", "content": "beta"}],
    }

    key_a1 = spool.enqueue(payload_a)
    key_a2 = spool.enqueue(payload_a)
    key_b = spool.enqueue(payload_b)

    assert key_a1 is not None
    assert key_a1 == key_a2
    assert key_b != key_a1
    assert key_a1 == compute_record_key(payload_a)
    assert len(spool.pending()) == 2

    # Keys are persisted and stable across instances.
    reopened = IngestSpool(persistence_dir=str(tmp_path / "spool"))
    keys = {record["key"] for record in reopened.pending()}
    assert keys == {key_a1, key_b}


def test_spool_remove_and_mark_permanent(tmp_path):
    spool = IngestSpool(persistence_dir=str(tmp_path / "spool"))
    key = spool.enqueue(
        {
            "session_id": "sess",
            "messages": [{"message_id": "m1", "role": "user", "content": "x"}],
        }
    )
    assert key is not None
    marked = spool.mark_permanent(key, AmrRpcError(-32012, "auth failed"))
    assert marked is not None
    assert marked["permanent"] is True
    assert marked["last_error"]["code"] == -32012

    # Marking again is idempotent and does not bump attempts.
    again = spool.mark_permanent(key, AmrRpcError(-32012, "auth failed"))
    assert again["attempts"] == 1

    assert spool.remove(key) is True
    assert spool.pending() == []


def test_spool_replay_transient_then_success(tmp_path):
    spool = IngestSpool(persistence_dir=str(tmp_path / "spool"))
    spool.enqueue(
        {
            "session_id": "sess",
            "messages": [{"message_id": "m1", "role": "user", "content": "x"}],
        }
    )

    class FlakyClient:
        def __init__(self):
            self.error = AmrRpcError(-32010, "offline")
            self.requests = 0

        def request(self, method, params, timeout=None):
            self.requests += 1
            if self.error is not None:
                raise self.error
            return {"status": "ok"}

    client = FlakyClient()
    summary = spool.replay(client, deadline_s=0.5)
    assert summary["attempted"] == 1
    assert summary["transient"] == 1
    assert summary["delivered"] == 0
    assert len(spool.pending()) == 1

    client.error = None
    summary = spool.replay(client, deadline_s=0.5)
    assert summary["delivered"] == 1
    assert spool.pending() == []


def test_request_accepts_error_response_with_null_id(mock_server):
    mock_server.handler = lambda req: {
        "jsonrpc": "2.0",
        "id": None,
        "error": {"code": -32002, "message": "VERSION_CONFLICT: stale"},
    }
    client = AmrUdsClient(socket_path=mock_server.socket_path)
    with pytest.raises(AmrRpcError) as excinfo:
        client.request("memory.update", {})
    assert excinfo.value.code == -32002


def test_request_maps_remote_auth_failure(mock_server):
    mock_server.handler = lambda req: {
        "jsonrpc": "2.0",
        "id": req.get("id"),
        "error": {"code": -32012, "message": "ADAPTER_REMOTE_AUTH_FAILED"},
    }
    client = AmrUdsClient(socket_path=mock_server.socket_path)
    with pytest.raises(AmrAuthError):
        client.request("memory.search", {"query": "x"})

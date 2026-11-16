"""2.x UDS 适配器端到端测试。

真实链路：临时 UDS 客户端 -> AdapterService -> HTTPS -> RemoteMcpService
-> UDS -> 临时假 daemon。全部材料在 tmp_path；不连接生产。
"""

import asyncio
import contextlib
import datetime as dt
import ipaddress
import json
import os
import stat
import struct

import pytest

pytest.importorskip("mcp")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from src.interfaces.adapter.config import (
    AdapterBinding,
    AdapterConfig,
    load_adapter_config,
)
from src.interfaces.adapter.server import AdapterService
from src.interfaces.ipc.protocol import (
    ConnectionClosedError,
    read_frame,
    write_frame,
)
from src.interfaces.mcp.bridge2 import AdapterBridge
from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_server import RemoteMcpService
from src.interfaces.mcp.remote_service import RemoteServiceConfig

TOKEN_A = "a" * 40
TOKEN_B = "b" * 40
TOKEN_INTRUDER = "c" * 40
AGENT_A = "openclaw"
AGENT_B = "hermes"
SENSITIVE = "sensitive-daemon-marker"
SEARCH_RESULT = {
    "results": [{"memory_id": "mem-1", "content": "adapter e2e", "score": 0.9}],
    "total": 1,
}
TOKENS = {
    TOKEN_A: {
        "agent_id": AGENT_A,
        "tools": ["memory_search", "memory_create", "memory_update", "memory_ingest_session"],
    },
    TOKEN_B: {"agent_id": AGENT_B, "tools": ["memory_search"]},
}


def _utcnow():
    return dt.datetime.now(dt.timezone.utc)


def _new_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _name(cn):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def _write_key(path, key):
    path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    os.chmod(path, 0o600)


def _make_tls(root):
    ca_key = _new_key()
    ca_name = _name("adapter test CA")
    now = _utcnow()
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    server_key = _new_key()
    server = (
        x509.CertificateBuilder()
        .subject_name(_name("localhost"))
        .issuer_name(ca.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_file = root / "ca.pem"
    cert_file = root / "server.crt"
    key_file = root / "server.key"
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    cert_file.write_bytes(server.public_bytes(serialization.Encoding.PEM))
    _write_key(key_file, server_key)
    return ca_file, cert_file, key_file


def _write_json(path, data, mode=None):
    path.write_text(json.dumps(data), encoding="utf-8")
    if mode is not None:
        os.chmod(path, mode)
    return path


class FakeDaemon:
    def __init__(self, socket_path):
        self.socket_path = socket_path
        self.requests = []
        self._server = None

    async def start(self):
        os.makedirs(os.path.dirname(self.socket_path), exist_ok=True)
        self._server = await asyncio.start_unix_server(self._handle, path=self.socket_path)
        return self

    async def _handle(self, reader, writer):
        try:
            request = await read_frame(reader)
            self.requests.append(request)
            method = request.get("method")
            if method == "memory.search":
                if request.get("params", {}).get("query") == "large":
                    result = {"blob": "x" * 5000}
                else:
                    result = SEARCH_RESULT
                await write_frame(
                    writer,
                    {"jsonrpc": "2.0", "id": request["id"], "result": result},
                )
            elif method == "session.ingest":
                await write_frame(
                    writer,
                    {"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}},
                )
            elif method == "system.health":
                await write_frame(writer, {"jsonrpc":"2.0", "id": request["id"], "result":{"status":"healthy", "qdrant_ok":True, "sqlite_ok":True, "model_state":"READY"}})
            elif method == "memory.update":
                await write_frame(
                    writer,
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {
                            "code": -32002,
                            "message": "VERSION_CONFLICT: current version differs",
                            "data": {
                                "memory_id": "mem-1",
                                "current_version": 5,
                                "expected_version": 4,
                                "secret_path": SENSITIVE,
                            },
                        },
                    },
                )
            elif method == "memory.create":
                await write_frame(
                    writer,
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {
                            "code": -32603,
                            "message": f"INTERNAL: {SENSITIVE}",
                            "data": {},
                        },
                    },
                )
            else:
                await write_frame(
                    writer,
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {"code": -32601, "message": "METHOD_NOT_FOUND"},
                    },
                )
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def stop(self):
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None


class Harness:
    def __init__(self, root):
        self.root = root
        self.run_dir = root / "run"
        self.run_dir.mkdir(mode=0o700, exist_ok=True)
        self.ca_file, self.cert_file, self.key_file = _make_tls(root)
        self.daemon = FakeDaemon(str(self.run_dir / "daemon.sock"))
        self.server = None
        self.adapter = None
        self.server_credentials = _write_json(
            root / "service-credentials.json", TOKENS, 0o600
        )

    @property
    def openclaw_socket(self):
        return str(self.run_dir / "openclaw.sock")

    @property
    def hermes_socket(self):
        return str(self.run_dir / "hermes.sock")

    def _adapter_config(
        self,
        name,
        *,
        credentials_file=None,
        bindings=None,
        remote_url=None,
        **overrides,
    ):
        credentials_file = credentials_file or self.server_credentials
        bindings = bindings or [
            {"socket_path": self.openclaw_socket, "agent_id": AGENT_A},
            {"socket_path": self.hermes_socket, "agent_id": AGENT_B},
        ]
        path = _write_json(
            self.root / name,
            {
                "remote_url": remote_url or f"https://localhost:{self.server.bound_port}/mcp",
                "ca_file": str(self.ca_file),
                "credentials_file": str(credentials_file),
                "connect_timeout": 2.0,
                "read_timeout": 10.0,
                "bindings": bindings,
                **overrides,
            },
        )
        return AdapterService(load_adapter_config(path))

    async def start(self):
        await self.daemon.start()
        self.server = RemoteMcpService(
            RemoteServiceConfig(
                business_socket=self.daemon.socket_path,
                admin_socket=str(self.run_dir / "admin.sock"),
                credentials_file=str(self.server_credentials),
                cert_file=str(self.cert_file),
                key_file=str(self.key_file),
                allowed_hosts=["localhost", "localhost:*", "127.0.0.1", "127.0.0.1:*"],
                allowed_origins=["https://localhost", "https://localhost:*"],
                authenticator=TokenAuthenticator(TOKENS),
                bind_port=0,
            )
        )
        await self.server.start()
        self.adapter = self._adapter_config("adapter.json")
        await self.adapter.start()
        return self

    async def stop(self):
        if self.adapter is not None:
            await self.adapter.stop()
            self.adapter = None
        if self.server is not None:
            await self.server.stop()
            self.server = None
        await self.daemon.stop()


async def _rpc(socket_path, method, params=None, req_id=1):
    reader, writer = await asyncio.open_unix_connection(socket_path)
    try:
        token = TOKEN_B if socket_path.endswith('hermes.sock') else (TOKEN_INTRUDER if socket_path.endswith('intruder.sock') else TOKEN_A)
        request = {"jsonrpc": "2.0", "id": req_id, "method": method, "auth_token": token}
        if params is not None:
            request["params"] = params
        await write_frame(writer, request)
        return await asyncio.wait_for(read_frame(reader), timeout=10.0)
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def _notify(socket_path, method, params):
    reader, writer = await asyncio.open_unix_connection(socket_path)
    try:
        await write_frame(writer, {"jsonrpc": "2.0", "method": method, "params": params, "auth_token": TOKEN_A})
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def test_search_round_trip_and_identity_is_bound(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            response = await _rpc(
                harness.openclaw_socket,
                "memory.search",
                {"query": "adapter e2e", "agent_id": "forged", "source_agent": "forged"},
            )
            assert "error" not in response, response
            assert response["result"] == SEARCH_RESULT

            request = harness.daemon.requests[-1]
            assert request["method"] == "memory.search"
            assert request["params"]["agent_id"] == AGENT_A
            assert request["params"]["source_agent"] == AGENT_A
            assert request["params"]["agent_id"] != "forged"
            assert request["params"]["source_agent"] != "forged"
        finally:
            await harness.stop()

    asyncio.run(run())


def test_ping_is_control_only_and_does_not_reach_daemon(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            before = len(harness.daemon.requests)
            response = await _rpc(harness.openclaw_socket, "amr.ping", {})
            assert response["result"] == {"status": "ok"}
            assert len(harness.daemon.requests) == before
        finally:
            await harness.stop()

    asyncio.run(run())


@pytest.mark.parametrize("method", ["memory_spool_status", "admin.shutdown", "unknown.method"])
def test_unknown_or_local_only_methods_are_rejected_without_backend(tmp_path, method):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            before = len(harness.daemon.requests)
            response = await _rpc(harness.openclaw_socket, method, {})
            assert response["error"]["code"] == -32601
            assert len(harness.daemon.requests) == before
        finally:
            await harness.stop()

    asyncio.run(run())


def test_tool_not_in_binding_allowlist_is_rejected_without_backend(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            before = len(harness.daemon.requests)
            response = await _rpc(
                harness.hermes_socket,
                "memory.update",
                {"memory_id": "mem-1", "content": "new content", "expected_version": 1, "change_reason": "x"},
            )
            assert response["error"]["code"] == -32601
            assert len(harness.daemon.requests) == before
        finally:
            await harness.stop()

    asyncio.run(run())


def test_daemon_version_conflict_is_preserved(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            response = await _rpc(
                harness.openclaw_socket,
                "memory.update",
                {"memory_id": "mem-1", "content": "new content", "expected_version": 4, "change_reason": "x"},
            )
            error = response["error"]
            assert error["code"] == -32002
            assert error["message"].startswith("VERSION_CONFLICT:")
            assert error["data"]["current_version"] == 5
            assert error["data"]["expected_version"] == 4
            assert "secret_path" not in error["data"]
            assert SENSITIVE not in json.dumps(response)
        finally:
            await harness.stop()

    asyncio.run(run())


def test_generic_daemon_error_does_not_leak_text(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            response = await _rpc(
                harness.openclaw_socket,
                "memory.create",
                {"content": "new memory content"},
            )
            assert response["error"]["code"] == -32603
            assert SENSITIVE not in json.dumps(response)
        finally:
            await harness.stop()

    asyncio.run(run())


def test_remote_unavailable_maps_to_adapter_unavailable(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            await harness.server.stop()
            response = await _rpc(
                harness.openclaw_socket, "memory.search", {"query": "offline"}
            )
            assert response["error"]["code"] == -32010
        finally:
            await harness.stop()

    asyncio.run(run())


def test_unknown_adapter_token_maps_to_remote_auth_failure(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        intruder_socket = str(harness.run_dir / "intruder.sock")
        intruder_credentials = _write_json(
            tmp_path / "intruder-credentials.json",
            {TOKEN_INTRUDER: {"agent_id": "intruder", "tools": ["memory_search"]}},
            0o600,
        )
        intruder = harness._adapter_config(
            "intruder-adapter.json",
            credentials_file=intruder_credentials,
            bindings=[{"socket_path": intruder_socket, "agent_id": "intruder"}],
        )
        try:
            await intruder.start()
            response = await _rpc(intruder_socket, "memory.search", {"query": "x"})
            assert response["error"]["code"] == -32012
        finally:
            await intruder.stop()
            await harness.stop()

    asyncio.run(run())


def test_notification_still_runs_session_ingest(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            before = len(harness.daemon.requests)
            await _notify(
                harness.openclaw_socket,
                "session.ingest",
                {
                    "session_id": "s-1",
                    "messages": [
                        {"message_id": "m-1", "role": "user", "content": "hello"}
                    ],
                },
            )
            for _ in range(100):
                if len(harness.daemon.requests) > before:
                    break
                await asyncio.sleep(0.01)
            assert len(harness.daemon.requests) == before + 1
            assert harness.daemon.requests[-1]["method"] == "session.ingest"
        finally:
            await harness.stop()

    asyncio.run(run())


def test_multiple_frames_on_one_connection(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            reader, writer = await asyncio.open_unix_connection(harness.openclaw_socket)
            try:
                for req_id in (1, 2):
                    await write_frame(
                        writer,
                        {
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "method": "memory.search",
                            "auth_token": TOKEN_A,
                            "params": {"query": f"q{req_id}"},
                        },
                    )
                    response = await asyncio.wait_for(read_frame(reader), timeout=10.0)
                    assert response["id"] == req_id
                    assert response["result"] == SEARCH_RESULT
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
        finally:
            await harness.stop()

    asyncio.run(run())


def test_two_connections_keep_identity_isolated(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            a, b = await asyncio.gather(
                _rpc(harness.openclaw_socket, "memory.search", {"query": "a"}),
                _rpc(harness.hermes_socket, "memory.search", {"query": "b"}),
            )
            assert "error" not in a and "error" not in b
            identities = {
                request["params"]["agent_id"]
                for request in harness.daemon.requests[-2:]
            }
            assert identities == {AGENT_A, AGENT_B}
        finally:
            await harness.stop()

    asyncio.run(run())


def test_socket_mode_is_0600_and_stop_cleans_it(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            mode = stat.S_IMODE(os.stat(harness.openclaw_socket).st_mode)
            assert mode == 0o600
        finally:
            await harness.stop()

        assert not os.path.exists(harness.openclaw_socket)
        assert not os.path.exists(harness.hermes_socket)

    asyncio.run(run())


def test_start_refuses_regular_file_and_does_not_delete_it(tmp_path):
    async def run():
        harness = Harness(tmp_path)
        placeholder = harness.run_dir / "openclaw.sock"
        placeholder.write_text("do-not-delete", encoding="utf-8")
        adapter = harness._adapter_config(
            "adapter.json",
            remote_url="https://localhost:1/mcp",
            bindings=[{"socket_path": str(placeholder), "agent_id": AGENT_A}],
        )
        with pytest.raises(ValueError):
            await adapter.start()
        assert adapter.is_started is False
        assert placeholder.read_text(encoding="utf-8") == "do-not-delete"
        await adapter.stop()

    asyncio.run(run())


def test_malformed_json_frame_returns_parse_error(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            reader, writer = await asyncio.open_unix_connection(harness.openclaw_socket)
            try:
                payload = b"not-json"
                writer.write(struct.pack(">I", len(payload)) + payload)
                await writer.drain()
                response = await asyncio.wait_for(read_frame(reader), timeout=10.0)
                assert response["error"]["code"] == -32700
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
        finally:
            await harness.stop()

    asyncio.run(run())


def test_concurrent_stop_is_safe(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            await asyncio.gather(harness.adapter.stop(), harness.adapter.stop())
            assert harness.adapter.is_started is False
            assert not os.path.exists(harness.openclaw_socket)
        finally:
            await harness.stop()

    asyncio.run(run())


def test_stop_does_not_remove_another_instances_socket(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        other = harness._adapter_config("other-adapter.json")
        try:
            await other.stop()  # 未启动的实例不得清理别人的 socket
            assert os.path.exists(harness.openclaw_socket)
        finally:
            await harness.stop()

    asyncio.run(run())


def test_response_larger_than_request_limit_is_still_written(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        large_socket = str(harness.run_dir / "large.sock")
        # 单独的 socket，避免与主适配器冲突。
        adapter = harness._adapter_config(
            "large-adapter.json",
            bindings=[{"socket_path": large_socket, "agent_id": AGENT_A}],
            max_request_bytes=1024,
            max_response_bytes=64 * 1024,
        )
        try:
            await adapter.start()
            response = await _rpc(large_socket, "memory.search", {"query": "large"})
            assert response["result"]["blob"] == "x" * 5000
        finally:
            await adapter.stop()
            await harness.stop()

    asyncio.run(run())


def test_adapter_can_restart_after_stop(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            await harness.adapter.stop()
            assert not os.path.exists(harness.openclaw_socket)
            await harness.adapter.start()
            response = await _rpc(
                harness.openclaw_socket, "memory.search", {"query": "after restart"}
            )
            assert response["result"] == SEARCH_RESULT
        finally:
            await harness.stop()

    asyncio.run(run())


def test_oversized_response_returns_clean_error_instead_of_hanging(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        large_socket = str(harness.run_dir / "tiny.sock")
        adapter = harness._adapter_config(
            "tiny-adapter.json",
            bindings=[{"socket_path": large_socket, "agent_id": AGENT_A}],
            max_response_bytes=1024,
        )
        try:
            await adapter.start()
            reader, writer = await asyncio.open_unix_connection(large_socket)
            try:
                await write_frame(
                    writer,
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "memory.search",
                        "params": {"query": "large"},
                        "auth_token": TOKEN_A,
                    },
                )
                response = await asyncio.wait_for(read_frame(reader), timeout=5.0)
                assert response["error"]["code"] == -32011
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
        finally:
            await adapter.stop()
            await harness.stop()

    asyncio.run(run())


def test_duplicate_bindings_are_rejected_before_start(tmp_path):
    async def run():
        harness = Harness(tmp_path)
        socket_path = str(harness.run_dir / "duplicate.sock")
        config = AdapterConfig(
            remote_url="https://localhost:1/mcp",
            credentials_file=str(harness.server_credentials),
            ca_file=str(harness.ca_file),
            bindings=(
                AdapterBinding(socket_path, AGENT_A, ("memory_search",), TOKEN_A),
                AdapterBinding(socket_path, AGENT_B, ("memory_search",), TOKEN_B),
            ),
        )
        service = AdapterService(config)
        with pytest.raises(ValueError):
            await service.start()
        assert service.is_started is False
        await service.stop()

    asyncio.run(run())


def test_second_adapter_cannot_steal_an_active_socket(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        second = harness._adapter_config("second-adapter.json")
        try:
            with pytest.raises(ValueError):
                await second.start()
            # 第一个实例仍然可用。
            response = await _rpc(
                harness.openclaw_socket, "memory.search", {"query": "still alive"}
            )
            assert response["result"] == SEARCH_RESULT
        finally:
            await second.stop()
            await harness.stop()

    asyncio.run(run())


def test_bridge2_round_trip_through_adapter_to_daemon(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            token_path = tmp_path / 'bridge-token'
            token_path.write_text(TOKEN_A, encoding='ascii')
            token_path.chmod(0o600)
            bridge = AdapterBridge(harness.openclaw_socket, token_file=str(token_path))
            listed = await bridge.handle_request({'jsonrpc':'2.0','id':9,'method':'tools/list','params':{}})
            assert {t['name'] for t in listed['result']['tools']} == set(TOKENS[TOKEN_A]['tools'])
            response = await bridge.handle_request(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "memory_search",
                        "arguments": {
                            "query": "bridge2 e2e",
                            "agent_id": "forged",
                            "source_agent": "forged",
                        },
                    },
                }
            )
            assert response["result"]["isError"] is False
            assert json.loads(response["result"]["content"][0]["text"]) == SEARCH_RESULT
            request = harness.daemon.requests[-1]
            assert request["method"] == "memory.search"
            assert request["params"]["agent_id"] == AGENT_A
            assert request["params"]["source_agent"] == AGENT_A
            assert request["params"]["agent_id"] != "forged"

            error = await bridge.handle_request(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "memory_update",
                        "arguments": {
                            "memory_id": "mem-1",
                            "content": "new content",
                            "expected_version": 4,
                            "change_reason": "x",
                        },
                    },
                }
            )
            assert error["result"]["isError"] is True
            assert "[-32002]" in error["result"]["content"][0]["text"]
        finally:
            await harness.stop()

    asyncio.run(run())


def test_authenticated_adapter_through_gateway_to_protected_daemon(tmp_path, monkeypatch):
    from src.interfaces.ipc.server import UDSServer
    internal = tmp_path / 'internal.token'
    internal.write_text('i'*43, encoding='ascii'); internal.chmod(0o600)
    monkeypatch.setenv('AMR_INTERNAL_TOKEN_FILE', str(internal))
    monkeypatch.setenv('AMR_INTERNAL_AUTH_REQUIRED', '1')
    async def run():
        harness = await Harness(tmp_path).start()
        await harness.daemon.stop()
        requests = []
        async def handle(method, params):
            requests.append((method,params)); return SEARCH_RESULT
        daemon = UDSServer('business', str(harness.run_dir / 'daemon.sock'), handle, internal_token='i'*43)
        await daemon.start()
        try:
            reader, writer = await asyncio.open_unix_connection(daemon.socket_path)
            await write_frame(writer, {'jsonrpc':'2.0','id':1,'method':'memory.search','params':{'agent_id':'openclaw'}})
            assert (await read_frame(reader))['error']['code'] == -32012
            writer.close(); await writer.wait_closed()
            assert not requests
            response = await _rpc(harness.openclaw_socket, 'memory.search', {'query':'isolated', 'agent_id':'forged'})
            assert response['result'] == SEARCH_RESULT
            assert requests[-1][1]['agent_id'] == 'openclaw'
            assert requests[-1][1]['source_agent'] == 'openclaw'
        finally:
            await daemon.stop()
            await harness.stop()
    asyncio.run(run())


def test_wrong_agent_token_never_reaches_protected_gateway(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            for token in (None, TOKEN_B, 'bad'):
                reader, writer = await asyncio.open_unix_connection(harness.openclaw_socket)
                request = {'jsonrpc':'2.0','id':1,'method':'memory.search','params':{'query':'x'}}
                if token is not None: request['auth_token'] = token
                await write_frame(writer, request)
                assert (await read_frame(reader))['error']['code'] == -32012
                writer.close(); await writer.wait_closed()
            assert not harness.daemon.requests
            ok = await _rpc(harness.openclaw_socket, 'memory.search', {'query':'ok'})
            assert ok['result'] == SEARCH_RESULT
        finally:
            await harness.stop()
    asyncio.run(run())


def test_activation_readiness_checks_https_and_authenticated_sockets(tmp_path, monkeypatch):
    from src.agent_access.activation import wait_ready
    from src.agent_access.registry import Registry
    import sys
    module = sys.modules[__name__]
    monkeypatch.setattr(module, 'TOKEN_A', 'a' * 43)
    monkeypatch.setattr(module, 'TOKEN_B', 'b' * 43)
    monkeypatch.setattr(module, 'TOKENS', {
        'a'*43: {'agent_id': AGENT_A, 'tools': ['memory_search']},
        'b'*43: {'agent_id': AGENT_B, 'tools': ['memory_search']},
    })
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            reg = Registry(tmp_path / 'registry')
            reg.import_existing(harness.server_credentials, tmp_path / 'adapter.json')
            base = json.loads((tmp_path / 'adapter.json').read_text())
            gen = reg.render(tmp_path / 'generation', base)
            await asyncio.to_thread(wait_ready, gen, 5)
        finally:
            await harness.stop()
    asyncio.run(run())


def test_activation_readiness_fails_when_daemon_is_down(tmp_path, monkeypatch):
    from src.agent_access.activation import wait_ready, ActivationError
    from src.agent_access.registry import Registry
    import sys
    module = sys.modules[__name__]
    monkeypatch.setattr(module, 'TOKEN_A', 'a'*43)
    monkeypatch.setattr(module, 'TOKEN_B', 'b'*43)
    monkeypatch.setattr(module, 'TOKENS', {
        'a'*43: {'agent_id': AGENT_A, 'tools':['memory_search']},
        'b'*43: {'agent_id': AGENT_B, 'tools':['memory_search']},
    })
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            reg = Registry(tmp_path/'registry')
            reg.import_existing(harness.server_credentials, tmp_path/'adapter.json')
            gen = reg.render(tmp_path/'generation', json.loads((tmp_path/'adapter.json').read_text()))
            await harness.daemon.stop()
            with pytest.raises(ActivationError):
                await asyncio.to_thread(wait_ready, gen, 0.5)
        finally:
            await harness.stop()
    asyncio.run(run())


def test_bridge_tool_discovery_requires_credential(tmp_path):
    async def run():
        harness = await Harness(tmp_path).start()
        try:
            bridge = AdapterBridge(harness.hermes_socket)
            result = await bridge.handle_request({'jsonrpc':'2.0','id':1,'method':'tools/list','params':{}})
            assert result['error']['code'] == -32012
        finally:
            await harness.stop()
    asyncio.run(run())


def test_unavailable_active_registry_is_retryable_end_to_end(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from src.interfaces.ipc.auth import authorize_agent
    from src.interfaces.ipc.server import UDSServer
    internal = tmp_path / 'internal.token'; internal.write_text('i'*43); internal.chmod(0o600)
    active = tmp_path / 'active.json'; active.write_text(json.dumps(TOKENS)); active.chmod(0o600)
    monkeypatch.setenv('AMR_INTERNAL_TOKEN_FILE', str(internal))
    monkeypatch.setenv('AMR_AGENT_CREDENTIALS_FILE', str(active))
    monkeypatch.setenv('AMR_INTERNAL_AUTH_REQUIRED', '1')
    cfg = SimpleNamespace(server=SimpleNamespace(allowed_agents=['system']))
    async def run():
        harness = await Harness(tmp_path).start(); await harness.daemon.stop()
        async def handle(method, params):
            authorize_agent(cfg, params)
            return SEARCH_RESULT
        daemon = UDSServer('business', str(harness.run_dir/'daemon.sock'), handle, internal_token='i'*43)
        await daemon.start()
        try:
            ok = await _rpc(harness.openclaw_socket, 'memory.search', {'query':'ok'})
            assert ok['result'] == SEARCH_RESULT
            active.unlink()
            unavailable = await _rpc(harness.openclaw_socket, 'memory.search', {'query':'pending'})
            assert unavailable['error']['code'] == -32010
        finally:
            await daemon.stop(); await harness.stop()
    asyncio.run(run())

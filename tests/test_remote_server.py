"""真实 TLS 的远程 MCP 服务端测试。

所有证书、私钥、凭证、配置文件与假 daemon Unix socket 都在 ``tmp_path``
内生成；不读取生产配置、不连接生产 daemon、不监听固定端口。服务使用
``bind_port=0``，只通过 :attr:`RemoteMcpService.bound_port` 暴露的真实端口
访问。

测试覆盖：TLS 启动后立即可用的 MCP initialize/tools/list/tools/call、
Bearer 认证、明文拒绝、证书链/主机名校验、幂等停止、重复启动拒绝，以及
启动前的 TLS/配置失败关闭。
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import ipaddress
import json
import os
import socket
import ssl
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

pytest.importorskip("mcp")

import httpx  # noqa: E402
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID  # noqa: E402

from src.interfaces.ipc.protocol import read_frame, write_frame  # noqa: E402
from src.interfaces.mcp.remote_auth import TokenAuthenticator  # noqa: E402
from src.interfaces.mcp.remote_server import (  # noqa: E402
    RemoteMcpService,
    RemoteServerError,
    TLSMaterialError,
    load_server_ssl_context,
)
from src.interfaces.mcp.remote_service import RemoteServiceConfig  # noqa: E402

# 仅用于隔离测试的固定假凭证与身份。
TOKEN = "test-only-server-token-0123456789abcdef0123456789abcdef"
AGENT_ID = "tls_test_agent"
FORGED_AGENT = "forged_agent"
DAEMON_RESULT = {
    "results": [
        {
            "memory_id": "mem-tls-1",
            "content": "TLS test memory",
            "score": 0.93,
            "status": "ACTIVE",
        }
    ],
    "total": 1,
}
ALLOWED_HOSTS = ["localhost", "localhost:*", "127.0.0.1", "127.0.0.1:*"]
ALLOWED_ORIGINS = ["https://localhost", "https://localhost:*"]

_MCP_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# TLS material generation (tmp_path only)
# ---------------------------------------------------------------------------


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _new_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _write_key(path: Path, key) -> Path:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    os.chmod(path, 0o600)
    return path


def _write_cert(path: Path, cert: x509.Certificate) -> Path:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return path


def _make_ca(tmp_path: Path, name: str):
    key = _new_key()
    subject = _name(name)
    now = _utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = _write_cert(tmp_path / f"{name.replace(' ', '-')}.pem", cert)
    return key, cert, cert_path


def _make_server_cert(
    ca_key,
    ca_cert: x509.Certificate,
    key,
    *,
    common_name: str,
    san_entries: List[x509.GeneralName],
    not_before: Optional[dt.datetime] = None,
    not_after: Optional[dt.datetime] = None,
) -> x509.Certificate:
    now = _utcnow()
    return (
        x509.CertificateBuilder()
        .subject_name(_name(common_name))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before if not_before is not None else now - dt.timedelta(days=1))
        .not_valid_after(not_after if not_after is not None else now + dt.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .sign(ca_key, hashes.SHA256())
    )


class Material:
    """tmp_path 内的全部 TLS/凭证材料。"""

    def __init__(self, root: Path) -> None:
        self.root = root

        self.ca_key, self.ca_cert, self.ca_file = _make_ca(root, "amr test CA")
        _, _, self.rogue_ca_file = _make_ca(root, "amr untrusted CA")

        self.server_key = _new_key()
        self.server_key_file = _write_key(root / "server.key", self.server_key)
        self.server_cert = _make_server_cert(
            self.ca_key,
            self.ca_cert,
            self.server_key,
            common_name="localhost",
            san_entries=[
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
            ],
        )
        self.server_cert_file = _write_cert(root / "server.crt", self.server_cert)

        # 与证书不匹配的另一把私钥。
        self.other_key_file = _write_key(root / "other.key", _new_key())

        now = _utcnow()
        self.expired_cert = _make_server_cert(
            self.ca_key,
            self.ca_cert,
            self.server_key,
            common_name="localhost",
            san_entries=[
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
            ],
            not_before=now - dt.timedelta(days=30),
            not_after=now - dt.timedelta(days=1),
        )
        self.expired_cert_file = _write_cert(root / "expired.crt", self.expired_cert)

        # 主机名不匹配的证书：由受信任 CA 签发，但只包含其他主机名。
        self.wrong_host_cert_file = _write_cert(
            root / "wrong-host.crt",
            _make_server_cert(
                self.ca_key,
                self.ca_cert,
                self.server_key,
                common_name="not-localhost.example",
                san_entries=[x509.DNSName("not-localhost.example")],
            ),
        )

        self.credentials_file = root / "credentials.json"
        self.credentials_file.write_text(
            json.dumps(
                {TOKEN: {"agent_id": AGENT_ID, "tools": ["memory_search"]}}
            ),
            encoding="utf-8",
        )
        os.chmod(self.credentials_file, 0o600)


@pytest.fixture()
def material(tmp_path: Path) -> Material:
    return Material(tmp_path)


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def _business_socket(root: Path) -> str:
    return str(root / "run" / "business.sock")


def _admin_socket(root: Path) -> str:
    return str(root / "run" / "admin.sock")


def _config(
    material: Material,
    *,
    cert_file: Optional[Path] = None,
    key_file: Optional[Path] = None,
    business_socket: Optional[str] = None,
    admin_socket: Optional[str] = None,
    credentials_file: Optional[Path] = None,
    bind_host: str = "127.0.0.1",
    bind_port: int = 0,
    allowed_origins: Optional[List[str]] = None,
) -> RemoteServiceConfig:
    return RemoteServiceConfig(
        business_socket=business_socket or _business_socket(material.root),
        admin_socket=admin_socket or _admin_socket(material.root),
        credentials_file=str(credentials_file or material.credentials_file),
        cert_file=str(cert_file or material.server_cert_file),
        key_file=str(key_file or material.server_key_file),
        allowed_hosts=list(ALLOWED_HOSTS),
        authenticator=TokenAuthenticator(
            {TOKEN: {"agent_id": AGENT_ID, "tools": ["memory_search"]}}
        ),
        bind_host=bind_host,
        bind_port=bind_port,
        allowed_origins=None if allowed_origins is None else list(allowed_origins),
    )


def _write_config_file(
    tmp_path: Path,
    material: Material,
    **overrides: Any,
) -> Path:
    payload: Dict[str, Any] = {
        "business_socket": _business_socket(tmp_path),
        "admin_socket": _admin_socket(tmp_path),
        "credentials_file": str(material.credentials_file),
        "cert_file": str(material.server_cert_file),
        "key_file": str(material.server_key_file),
        "allowed_hosts": list(ALLOWED_HOSTS),
        "bind_host": "127.0.0.1",
        "bind_port": 0,
        "allowed_origins": list(ALLOWED_ORIGINS),
    }
    payload.update(overrides)
    path = tmp_path / "remote-service.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Fake daemon (tmp_path UDS only)
# ---------------------------------------------------------------------------


class FakeDaemon:
    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self.requests: List[Dict[str, Any]] = []
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> "FakeDaemon":
        os.makedirs(os.path.dirname(self.socket_path), exist_ok=True)
        self._server = await asyncio.start_unix_server(
            self._handle, path=self.socket_path
        )
        return self

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request = await read_frame(reader)
            self.requests.append(request)
            await write_frame(
                writer,
                {"jsonrpc": "2.0", "id": request["id"], "result": DAEMON_RESULT},
            )
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None


# ---------------------------------------------------------------------------
# MCP-over-HTTPS helpers
# ---------------------------------------------------------------------------


def _payload(method: str, params: Any, request_id: int) -> Dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params,
    }


def _initialize_payload() -> Dict[str, Any]:
    return _payload(
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "amr-tls-test", "version": "0.0.0"},
        },
        1,
    )


def _https_client(material: Material, service: RemoteMcpService, **kwargs: Any):
    return httpx.AsyncClient(
        verify=str(material.ca_file),
        base_url=f"https://localhost:{service.bound_port}",
        timeout=10.0,
        trust_env=False,
        **kwargs,
    )


async def _post_mcp(
    client: httpx.AsyncClient,
    payload: Dict[str, Any],
    *,
    token: Optional[str] = TOKEN,
) -> httpx.Response:
    headers = dict(_MCP_HEADERS)
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return await client.post("/mcp", headers=headers, json=payload)


def _jsonrpc_result(response: httpx.Response) -> Any:
    assert response.status_code == 200, response.text
    body = response.json()
    assert "error" not in body, body
    return body["result"]


# ---------------------------------------------------------------------------
# a. TLS 启动后立即可用，且身份由令牌决定
# ---------------------------------------------------------------------------


def test_tls_mcp_initialize_list_and_call_immediately_after_start(
    tmp_path: Path, material: Material
) -> None:
    config_path = _write_config_file(tmp_path, material)

    async def run() -> None:
        daemon = await FakeDaemon(_business_socket(tmp_path)).start()
        service = RemoteMcpService.from_config_file(config_path)
        try:
            await service.start()
            assert service.is_started is True
            assert service.bound_port is not None
            assert service.bound_port > 0

            # 不 sleep、不重试：start() 返回后立即发起真实 TLS 请求。
            async with _https_client(material, service) as client:
                initialize = await _post_mcp(client, _initialize_payload())
                result = _jsonrpc_result(initialize)
                assert result["serverInfo"]["name"]

                listed = await _post_mcp(
                    client, _payload("tools/list", {}, 2)
                )
                listed_result = _jsonrpc_result(listed)
                assert "memory_search" in {
                    tool["name"] for tool in listed_result["tools"]
                }

                called = await _post_mcp(
                    client,
                    _payload(
                        "tools/call",
                        {
                            "name": "memory_search",
                            "arguments": {
                                "query": "TLS round trip",
                                "limit": 3,
                                # 客户端伪造身份必须被 Gateway 覆盖。
                                "agent_id": FORGED_AGENT,
                                "source_agent": FORGED_AGENT,
                            },
                        },
                        3,
                    ),
                )
                called_result = _jsonrpc_result(called)
                assert called_result.get("isError") is not True
                assert json.loads(called_result["content"][0]["text"]) == DAEMON_RESULT
        finally:
            await service.stop()
            await daemon.stop()

        assert len(daemon.requests) == 1
        request = daemon.requests[0]
        assert request["method"] == "memory.search"
        assert request["params"]["query"] == "TLS round trip"
        assert request["params"]["agent_id"] == AGENT_ID
        assert request["params"]["source_agent"] == AGENT_ID
        assert request["params"]["agent_id"] != FORGED_AGENT
        assert request["params"]["source_agent"] != FORGED_AGENT

    asyncio.run(run())


# ---------------------------------------------------------------------------
# b. Bearer 认证
# ---------------------------------------------------------------------------


def test_tls_mcp_rejects_missing_and_wrong_bearer_tokens(
    material: Material,
) -> None:
    async def run() -> None:
        service = RemoteMcpService(_config(material))
        try:
            await service.start()
            async with _https_client(material, service) as client:
                missing = await _post_mcp(
                    client, _initialize_payload(), token=None
                )
                assert missing.status_code == 401

                wrong = await _post_mcp(
                    client, _initialize_payload(), token="not-the-token"
                )
                assert wrong.status_code == 401
        finally:
            await service.stop()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# c. 明文 HTTP 不能得到成功的 MCP 响应
# ---------------------------------------------------------------------------


def test_plaintext_http_against_tls_port_fails(material: Material) -> None:
    async def run() -> None:
        service = RemoteMcpService(_config(material))
        response: Optional[httpx.Response] = None
        failed = False
        try:
            await service.start()
            port = service.bound_port
            assert port is not None
            try:
                async with httpx.AsyncClient(
                    base_url=f"http://localhost:{port}",
                    timeout=5.0,
                    trust_env=False,
                ) as client:
                    response = await client.post(
                        "/mcp",
                        headers=dict(_MCP_HEADERS),
                        json=_initialize_payload(),
                    )
            except Exception:
                failed = True
        finally:
            await service.stop()

        # TLS 端口对明文握手一律失败；即使返回了 HTTP 错误，也绝不能是
        # 带 JSON-RPC result 的成功 MCP 响应。
        assert failed or (response is not None and response.status_code >= 400)
        if not failed and response is not None:
            assert "result" not in response.text

    asyncio.run(run())


# ---------------------------------------------------------------------------
# d. 证书链与主机名校验
# ---------------------------------------------------------------------------


def test_untrusted_ca_is_rejected_by_client(material: Material) -> None:
    async def run() -> None:
        service = RemoteMcpService(_config(material))
        try:
            await service.start()
            async with httpx.AsyncClient(
                verify=str(material.rogue_ca_file),
                base_url=f"https://localhost:{service.bound_port}",
                timeout=10.0,
                trust_env=False,
            ) as client:
                with pytest.raises(httpx.ConnectError):
                    await _post_mcp(client, _initialize_payload())
        finally:
            await service.stop()

    asyncio.run(run())


def test_hostname_mismatch_is_rejected_by_client(material: Material) -> None:
    wrong_host_service = RemoteMcpService(
        _config(
            material,
            cert_file=material.wrong_host_cert_file,
            key_file=material.server_key_file,
        )
    )

    async def run() -> None:
        try:
            await wrong_host_service.start()
            async with _https_client(material, wrong_host_service) as client:
                with pytest.raises(httpx.ConnectError):
                    await _post_mcp(client, _initialize_payload())
        finally:
            await wrong_host_service.stop()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# e/f. 停止幂等、端口释放、重复启动
# ---------------------------------------------------------------------------


def test_stop_is_idempotent_and_releases_port(material: Material) -> None:
    async def run() -> None:
        service = RemoteMcpService(_config(material))
        await service.start()
        port = service.bound_port
        assert port is not None

        await service.stop()
        assert service.is_started is False
        assert service.bound_port is None

        # 第二次 stop 不抛异常。
        await service.stop()
        assert service.is_started is False

        # 监听套接字已关闭：连接必须被拒绝。
        with pytest.raises(OSError):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            del reader

        # 没有遗留的运行中任务（当前任务除外）。
        pending = [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        assert pending == []

    asyncio.run(run())


def test_second_start_raises_runtime_error(material: Material) -> None:
    async def run() -> None:
        service = RemoteMcpService(_config(material))
        try:
            await service.start()
            with pytest.raises(RuntimeError):
                await service.start()
        finally:
            await service.stop()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# g. 启动前失败关闭
# ---------------------------------------------------------------------------


def test_missing_certificate_is_rejected_before_start(
    tmp_path: Path, material: Material
) -> None:
    service_config = _config(
        material, cert_file=tmp_path / "missing.crt"
    )
    with pytest.raises(TLSMaterialError):
        RemoteMcpService(service_config)


def test_certificate_key_mismatch_is_rejected_before_start(
    material: Material,
) -> None:
    service_config = _config(
        material, cert_file=material.server_cert_file, key_file=material.other_key_file
    )
    with pytest.raises(TLSMaterialError):
        RemoteMcpService(service_config)


def test_expired_certificate_is_rejected_before_start(
    material: Material,
) -> None:
    service_config = _config(
        material,
        cert_file=material.expired_cert_file,
        key_file=material.server_key_file,
    )
    with pytest.raises(TLSMaterialError):
        RemoteMcpService(service_config)


def test_credentials_with_0644_permissions_are_rejected_before_start(
    tmp_path: Path, material: Material
) -> None:
    os.chmod(material.credentials_file, 0o644)
    config_path = _write_config_file(tmp_path, material)

    with pytest.raises(ValueError):
        RemoteMcpService.from_config_file(config_path)


def test_identical_business_and_admin_sockets_are_rejected_before_start(
    tmp_path: Path, material: Material
) -> None:
    same = _business_socket(tmp_path)
    config_path = _write_config_file(
        tmp_path,
        material,
        business_socket=same,
        admin_socket=same,
    )

    with pytest.raises(ValueError):
        RemoteMcpService.from_config_file(config_path)


def test_construction_does_not_build_app_or_open_connections(
    material: Material, monkeypatch: pytest.MonkeyPatch
) -> None:
    import src.interfaces.mcp.remote_server as remote_server

    def _boom_build(*args: Any, **kwargs: Any):
        raise AssertionError("construction must not build the ASGI app")

    async def _boom_connect(*args: Any, **kwargs: Any):
        raise AssertionError("construction must not connect to any socket")

    monkeypatch.setattr(remote_server, "build_runtime", _boom_build)
    monkeypatch.setattr(asyncio, "open_unix_connection", _boom_connect)

    service = RemoteMcpService(_config(material))

    assert service.is_started is False
    assert service.bound_port is None
    assert service.config.business_socket.startswith(str(material.root))


def test_load_server_ssl_context_sets_tls12_minimum(material: Material) -> None:
    context = load_server_ssl_context(
        material.server_cert_file, material.server_key_file
    )

    assert isinstance(context, ssl.SSLContext)
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2


def test_remote_server_error_hierarchy() -> None:
    assert issubclass(TLSMaterialError, RemoteServerError)
    assert issubclass(RemoteServerError, ValueError)


# ---------------------------------------------------------------------------
# 生命周期边界：端口占用、重启、serve 与启动期 app 校验失败。
# ---------------------------------------------------------------------------


def test_start_reports_port_in_use_without_killing_the_caller(
    material: Material,
) -> None:
    """uvicorn 在端口占用时会 sys.exit；库必须转成 RemoteServerError。"""

    async def run() -> None:
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        try:
            port = blocker.getsockname()[1]
            service = RemoteMcpService(_config(material, bind_port=port))

            # 关键：SystemExit 绝不能穿透到调用方。
            with pytest.raises(RemoteServerError):
                await service.start()

            assert service.is_started is False
            assert service.bound_port is None
            # 启动失败后 stop 仍然幂等。
            await service.stop()
        finally:
            blocker.close()

    asyncio.run(run())


def test_service_can_restart_after_stop(material: Material) -> None:
    async def run() -> None:
        service = RemoteMcpService(_config(material))
        try:
            await service.start()
            first_port = service.bound_port
            assert first_port
            await service.stop()
            assert service.is_started is False

            await service.start()
            assert service.is_started is True
            assert service.bound_port

            async with _https_client(material, service) as client:
                response = await _post_mcp(client, _initialize_payload())
                _jsonrpc_result(response)
        finally:
            await service.stop()

    asyncio.run(run())


def test_serve_returns_after_stop(material: Material) -> None:
    async def run() -> None:
        service = RemoteMcpService(_config(material))
        serve_task = asyncio.create_task(service.serve())
        try:
            deadline = asyncio.get_running_loop().time() + 10.0
            while not service.is_started:
                if serve_task.done():
                    # 让 serve 的真实异常浮出，而不是误报超时。
                    await serve_task
                    pytest.fail("serve() ended before the service started")
                if asyncio.get_running_loop().time() >= deadline:
                    pytest.fail("service did not start in time")
                await asyncio.sleep(0.005)

            async with _https_client(material, service) as client:
                response = await _post_mcp(client, _initialize_payload())
                _jsonrpc_result(response)

            await service.stop()
            await asyncio.wait_for(serve_task, timeout=10.0)
        finally:
            await service.stop()
            if not serve_task.done():
                serve_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await serve_task

        assert service.is_started is False
        assert service.bound_port is None

    asyncio.run(run())


def test_start_fails_closed_on_invalid_origin_allowlist(
    material: Material,
) -> None:
    """build_app 的严格 Origin 校验必须发生在监听之前。"""
    service = RemoteMcpService(_config(material, allowed_origins=["*"]))

    async def run() -> None:
        with pytest.raises(ValueError):
            await service.start()
        assert service.is_started is False
        assert service.bound_port is None
        await service.stop()

    asyncio.run(run())

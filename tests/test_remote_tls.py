"""远程 MCP 客户端 TLS 配置的隔离行为测试。

安全不变量（全部通过真实行为验证，不 mock ssl）：
1. 组件返回真实的 ``ssl.SSLContext``，强制证书链校验（CERT_REQUIRED）；
2. hostname 校验始终开启，且没有任何关闭校验的 API 开关；
3. 可选自定义 CA 文件成为唯一信任锚，可信握手成功；
4. 未信任的私有 CA、错误 CA、hostname 不匹配均握手失败；
5. CA 文件缺失、是目录、为空或内容无效时必须在构造阶段失败（fail closed）。

测试只在 ``127.0.0.1`` 的临时端口启动一次性 TLS echo 服务，证书由本机
``openssl`` 在临时目录即时生成；不连接 daemon、生产 socket 或外部网络。
"""

import contextlib
import inspect
import shutil
import socket
import ssl
import subprocess
import threading
from pathlib import Path
from typing import Dict, Iterator

import pytest

from src.interfaces.mcp.remote_tls import TLSConfigError, build_client_ssl_context

OPENSSL = shutil.which("openssl")
PROBE = b"amr-tls-probe"


def _openssl(*args: str) -> None:
    subprocess.run(
        [OPENSSL, *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _make_ca(root: Path, name: str) -> Dict[str, Path]:
    key = root / f"{name}.key"
    cert = root / f"{name}.pem"
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-sha256",
        "-nodes",
        "-days",
        "2",
        "-subj",
        f"/CN=AMR Test {name}",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
        str(key),
        "-out",
        str(cert),
    )
    return {"key": key, "cert": cert}


@pytest.fixture(scope="module")
def pki(tmp_path_factory: pytest.TempPathFactory) -> Dict[str, Path]:
    """临时 PKI：可信 CA、无关 CA，以及由可信 CA 签发的 localhost 服务端证书。"""
    if OPENSSL is None:
        pytest.skip("openssl CLI is required for real TLS handshake tests")

    root = tmp_path_factory.mktemp("amr-tls-pki")
    trusted = _make_ca(root, "trusted-ca")
    untrusted = _make_ca(root, "untrusted-ca")

    server_key = root / "server.key"
    server_csr = root / "server.csr"
    server_cert = root / "server.pem"
    extfile = root / "server.ext"
    extfile.write_text(
        "\n".join(
            [
                "basicConstraints=critical,CA:FALSE",
                "keyUsage=critical,digitalSignature,keyEncipherment",
                "extendedKeyUsage=serverAuth",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "",
            ]
        ),
        encoding="ascii",
    )
    _openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-sha256",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-keyout",
        str(server_key),
        "-out",
        str(server_csr),
    )
    _openssl(
        "x509",
        "-req",
        "-in",
        str(server_csr),
        "-CA",
        str(trusted["cert"]),
        "-CAkey",
        str(trusted["key"]),
        "-CAcreateserial",
        "-days",
        "2",
        "-sha256",
        "-extfile",
        str(extfile),
        "-out",
        str(server_cert),
    )

    return {
        "trusted_ca": trusted["cert"],
        "trusted_ca_key": trusted["key"],
        "untrusted_ca": untrusted["cert"],
        "server_key": server_key,
        "server_cert": server_cert,
    }


def _server_context(pki: Dict[str, Path]) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(
        certfile=str(pki["server_cert"]),
        keyfile=str(pki["server_key"]),
    )
    return context


@contextlib.contextmanager
def _tls_echo_server(server_context: ssl.SSLContext) -> Iterator[int]:
    """在 127.0.0.1 临时端口启动一次性 TLS echo 服务。"""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    listener.settimeout(0.2)
    port = listener.getsockname()[1]
    stopped = threading.Event()
    workers = []

    def handle(conn: socket.socket) -> None:
        try:
            conn.settimeout(2)
            with server_context.wrap_socket(conn, server_side=True) as tls:
                data = tls.recv(1024)
                if data:
                    tls.sendall(data)
        except (ssl.SSLError, OSError):
            # 客户端拒绝证书是预期路径，服务端只负责关闭连接。
            pass
        finally:
            with contextlib.suppress(OSError):
                conn.close()

    def serve() -> None:
        while not stopped.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            worker = threading.Thread(target=handle, args=(conn,), daemon=True)
            worker.start()
            workers.append(worker)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        stopped.set()
        listener.close()
        thread.join(timeout=5)
        for worker in workers:
            worker.join(timeout=5)


def _roundtrip(
    context: ssl.SSLContext,
    port: int,
    *,
    hostname: str = "localhost",
    payload: bytes = PROBE,
) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as raw:
        with context.wrap_socket(raw, server_hostname=hostname) as tls:
            tls.settimeout(5)
            tls.sendall(payload)
            return tls.recv(1024)


def test_returns_real_context_with_chain_and_hostname_verification() -> None:
    context = build_client_ssl_context()

    assert isinstance(context, ssl.SSLContext)
    assert context.protocol == ssl.PROTOCOL_TLS_CLIENT
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.minimum_version >= ssl.TLSVersion.TLSv1_2


def test_no_api_allows_disabling_verification() -> None:
    signature = inspect.signature(build_client_ssl_context)
    assert set(signature.parameters) == {"ca_file"}

    with pytest.raises(TypeError):
        build_client_ssl_context(verify=False)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        build_client_ssl_context(check_hostname=False)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        build_client_ssl_context(None, ssl.CERT_NONE)  # type: ignore[misc]


def test_custom_ca_enables_trusted_local_handshake(pki: Dict[str, Path]) -> None:
    with _tls_echo_server(_server_context(pki)) as port:
        from_path = build_client_ssl_context(pki["trusted_ca"])
        assert from_path.verify_mode == ssl.CERT_REQUIRED
        assert from_path.check_hostname is True
        assert _roundtrip(from_path, port) == PROBE

        from_str = build_client_ssl_context(str(pki["trusted_ca"]))
        assert _roundtrip(from_str, port) == PROBE


def test_default_context_rejects_private_ca_chain(pki: Dict[str, Path]) -> None:
    """不带自定义 CA 时必须使用系统信任库，私有 CA 链必须被拒绝。"""
    with _tls_echo_server(_server_context(pki)) as port:
        context = build_client_ssl_context()
        with pytest.raises(ssl.SSLCertVerificationError):
            _roundtrip(context, port)


def test_unrelated_ca_is_rejected(pki: Dict[str, Path]) -> None:
    with _tls_echo_server(_server_context(pki)) as port:
        context = build_client_ssl_context(pki["untrusted_ca"])
        with pytest.raises(ssl.SSLCertVerificationError):
            _roundtrip(context, port)


def test_hostname_mismatch_is_rejected(pki: Dict[str, Path]) -> None:
    with _tls_echo_server(_server_context(pki)) as port:
        context = build_client_ssl_context(pki["trusted_ca"])
        with pytest.raises(ssl.SSLCertVerificationError) as excinfo:
            _roundtrip(context, port, hostname="not-localhost.example")
        # 62 == X509_V_ERR_HOSTNAME_MISMATCH
        assert excinfo.value.verify_code == 62


def test_missing_ca_file_fails_closed(tmp_path: Path) -> None:
    missing = tmp_path / "missing-ca.pem"
    with pytest.raises(TLSConfigError):
        build_client_ssl_context(missing)
    with pytest.raises(TLSConfigError):
        build_client_ssl_context(str(missing))


def test_ca_directory_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(TLSConfigError):
        build_client_ssl_context(tmp_path)


def test_empty_ca_file_fails_closed(tmp_path: Path) -> None:
    empty = tmp_path / "empty-ca.pem"
    empty.write_bytes(b"")
    with pytest.raises(TLSConfigError):
        build_client_ssl_context(empty)


def test_garbage_ca_file_fails_closed(tmp_path: Path) -> None:
    garbage = tmp_path / "garbage-ca.pem"
    garbage.write_text("this is not a certificate\n", encoding="utf-8")
    with pytest.raises(TLSConfigError):
        build_client_ssl_context(garbage)


def test_non_certificate_pem_fails_closed(pki: Dict[str, Path]) -> None:
    """私钥 PEM 不是 CA 证书，必须拒绝而不是静默接受。"""
    with pytest.raises(TLSConfigError):
        build_client_ssl_context(pki["trusted_ca_key"])

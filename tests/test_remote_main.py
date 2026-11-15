"""AMR 2.x remote MCP CLI tests (temp TLS material only)."""

import asyncio
import datetime as dt
import ipaddress
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

pytest.importorskip("mcp")
pytest.importorskip("cryptography")

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from src.interfaces.mcp.remote_main import main

ROOT = Path(__file__).resolve().parent.parent
TOKEN = "r" * 40


def _make_tls(root):
    now = dt.datetime.now(dt.timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "cli test CA")])
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
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
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
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    os.chmod(key_file, 0o600)
    return ca_file, cert_file, key_file


def _config(tmp_path):
    ca_file, cert_file, key_file = _make_tls(tmp_path)
    credentials = tmp_path / "credentials.json"
    credentials.write_text(
        json.dumps({TOKEN: {"agent_id": "cli", "tools": ["memory_search"]}}),
        encoding="utf-8",
    )
    os.chmod(credentials, 0o600)
    config = tmp_path / "remote.json"
    config.write_text(
        json.dumps(
            {
                "business_socket": str(tmp_path / "run" / "business.sock"),
                "admin_socket": str(tmp_path / "run" / "admin.sock"),
                "credentials_file": str(credentials),
                "cert_file": str(cert_file),
                "key_file": str(key_file),
                "allowed_hosts": ["localhost", "localhost:*"],
                "bind_port": 0,
            }
        ),
        encoding="utf-8",
    )
    return config, ca_file


def test_check_validates_tls_and_config(tmp_path):
    config, _ = _config(tmp_path)
    assert main(["--config", str(config), "--check"]) == 0


def test_check_rejects_invalid_certificate(tmp_path):
    config, _ = _config(tmp_path)
    payload = json.loads(config.read_text(encoding="utf-8"))
    bad_cert = tmp_path / "bad.crt"
    bad_cert.write_text("not-a-cert", encoding="utf-8")
    payload["cert_file"] = str(bad_cert)
    config.write_text(json.dumps(payload), encoding="utf-8")
    assert main(["--config", str(config), "--check"]) == 2


def test_cli_serves_tls_and_stops_on_sigterm(tmp_path):
    config, ca_file = _config(tmp_path)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    process = subprocess.Popen(
        [sys.executable, "-m", "src.interfaces.mcp.remote_main", "--config", str(config)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        line = process.stdout.readline()
        assert "amr2-mcp listening on https://" in line, line
        port = int(line.rsplit(":", 1)[1].split("/", 1)[0])

        async def call():
            async with httpx.AsyncClient(
                verify=str(ca_file),
                base_url=f"https://localhost:{port}",
                timeout=10.0,
                trust_env=False,
            ) as client:
                return await client.post(
                    "/mcp",
                    headers={
                        "Authorization": f"Bearer {TOKEN}",
                        "Accept": "application/json, text/event-stream",
                        "Content-Type": "application/json",
                    },
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "clientInfo": {"name": "cli-test", "version": "0"},
                        },
                    },
                )

        response = asyncio.run(call())
        assert response.status_code == 200
        assert response.json()["result"]["serverInfo"]["name"]
    finally:
        process.send_signal(signal.SIGTERM)
        try:
            code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            code = process.wait(timeout=5)
        stdout, stderr = process.communicate()
    assert code == 0, f"stdout={stdout} stderr={stderr}"

"""AMR 2.x adapter CLI tests (temp sockets only)."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from src.interfaces.adapter.main import main
from src.interfaces.ipc.protocol import read_frame, write_frame

ROOT = Path(__file__).resolve().parent.parent
TOKEN = "t" * 40


def _config(tmp_path, *, socket_name="openclaw.sock"):
    credentials = tmp_path / "credentials.json"
    credentials.write_text(
        json.dumps(
            {TOKEN: {"agent_id": "openclaw", "tools": ["memory_search"]}}
        ),
        encoding="utf-8",
    )
    os.chmod(credentials, 0o600)
    socket_path = tmp_path / "run" / socket_name
    config = tmp_path / "adapter.json"
    config.write_text(
        json.dumps(
            {
                "remote_url": "https://localhost:1/mcp",
                "credentials_file": str(credentials),
                "bindings": [
                    {"socket_path": str(socket_path), "agent_id": "openclaw"}
                ],
            }
        ),
        encoding="utf-8",
    )
    return config, socket_path


def test_check_validates_without_opening_socket(tmp_path):
    config, socket_path = _config(tmp_path)
    assert main(["--config", str(config), "--check"]) == 0
    assert not socket_path.exists()


def test_check_rejects_missing_config(tmp_path):
    assert main(["--config", str(tmp_path / "missing.json"), "--check"]) == 2


def test_cli_starts_pings_and_stops_on_sigterm(tmp_path):
    config, socket_path = _config(tmp_path)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    process = subprocess.Popen(
        [sys.executable, "-m", "src.interfaces.adapter.main", "--config", str(config)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        line = process.stdout.readline()
        assert "amr2-adapter listening on" in line, line
        assert str(socket_path) in line

        import asyncio

        async def ping():
            reader, writer = await asyncio.open_unix_connection(str(socket_path))
            try:
                await write_frame(
                    writer, {"jsonrpc": "2.0", "id": 1, "method": "amr.ping", "params": {}}
                )
                return await asyncio.wait_for(read_frame(reader), timeout=5)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

        response = asyncio.run(ping())
        assert response["result"] == {"status": "ok"}
    finally:
        process.send_signal(signal.SIGTERM)
        try:
            code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            code = process.wait(timeout=5)
        stdout, stderr = process.communicate()
    assert code == 0, f"stdout={stdout} stderr={stderr}"
    assert not socket_path.exists()

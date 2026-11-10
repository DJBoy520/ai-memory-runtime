"""Unit tests for AMR Doctor diagnostic tool (scripts/amr_doctor.py).

Tests run in complete isolation with temporary UDS sockets and temporary SQLite DB.
Never connects to production sockets, DB, or GPU.
"""

import asyncio
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent

from scripts.amr_doctor import (
    check_sqlite_integrity,
    format_human_report,
    run_diagnostics,
    sanitize_dead_letters,
)
from src.interfaces.ipc.protocol import (
    make_jsonrpc_response,
    read_frame,
    write_frame,
)


class FakeIpcServer:
    """Lightweight fake Unix Domain Socket server to simulate business or admin RPCs."""

    def __init__(self, socket_path: str, handlers: dict):
        self.socket_path = socket_path
        self.handlers = handlers
        self.server: asyncio.Server | None = None

    async def _client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                try:
                    req = await read_frame(reader)
                except Exception:
                    break
                method = req.get("method")
                req_id = req.get("id", 1)
                handler = self.handlers.get(method)
                if handler is not None:
                    res_val = handler(req.get("params", {})) if callable(handler) else handler
                    resp = make_jsonrpc_response(res_val, req_id=req_id)
                else:
                    resp = {
                        "jsonrpc": "2.0",
                        "error": {"code": -32601, "message": f"Method {method} not found"},
                        "id": req_id,
                    }
                await write_frame(writer, resp)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self):
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        self.server = await asyncio.start_unix_server(self._client_connected, path=self.socket_path)

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if os.path.exists(self.socket_path):
            try:
                os.unlink(self.socket_path)
            except OSError:
                pass


@pytest.fixture
def tmp_sqlite_db(tmp_path):
    """Creates a healthy temporary SQLite database."""
    db_path = tmp_path / "test_sessions.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("CREATE TABLE memories (memory_id TEXT PRIMARY KEY, content TEXT);")
    conn.execute("INSERT INTO memories VALUES ('mem_1', 'test content');")
    conn.commit()
    conn.close()
    return str(db_path)


@pytest.mark.asyncio
async def test_doctor_healthy(tmp_path, tmp_sqlite_db):
    """Doctor reports HEALTHY (0) when all services, SQLite, and Outbox are normal."""
    b_sock = str(tmp_path / "b.sock")
    a_sock = str(tmp_path / "a.sock")

    b_server = FakeIpcServer(b_sock, {
        "system.health": {
            "status": "healthy",
            "service": "ai-memory-runtime",
            "model_state": "READY",
            "qdrant_ok": True,
            "sqlite_ok": True,
        }
    })
    a_server = FakeIpcServer(a_sock, {
        "admin.status": {
            "engine": {"state": "READY", "device": "cuda"},
            "collections": ["ai_memory", "crypto_standards"],
            "sqlite_path": tmp_sqlite_db,
            "status": "healthy",
        },
        "status.metrics": {
            "requests_total": 42,
            "errors_total": 0,
            "outbox": {"processed": 10, "dead_letter": 0},
        },
        "outbox.list_dead_letters": {
            "total": 0,
            "dead_letters": [],
        },
        "admin.collections": {
            "ai_memory": {"points_count": 10},
            "crypto_standards": {"points_count": 5},
        },
    })

    await b_server.start()
    await a_server.start()

    try:
        report = await run_diagnostics(
            admin_socket=a_sock,
            business_socket=b_sock,
            timeout=2.0,
        )
        assert report["status"] == "healthy"
        assert report["exit_code"] == 0
        assert report["checks"]["business_ipc"]["status"] == "ok"
        assert report["checks"]["admin_ipc"]["status"] == "ok"
        assert report["checks"]["sqlite"]["status"] == "ok"
        assert report["checks"]["qdrant"]["collections_count"] == 2
        assert report["checks"]["outbox_dead_letters"]["dead_letters_count"] == 0

        human_text = format_human_report(report)
        assert "Overall Status: HEALTHY (exit code 0)" in human_text
        assert "[  OK  ] Business IPC" in human_text
    finally:
        await b_server.stop()
        await a_server.stop()


@pytest.mark.asyncio
async def test_doctor_degraded_on_dead_letters(tmp_path, tmp_sqlite_db):
    """Doctor reports DEGRADED (1) when outbox dead letters exist."""
    b_sock = str(tmp_path / "b.sock")
    a_sock = str(tmp_path / "a.sock")

    b_server = FakeIpcServer(b_sock, {
        "system.health": {
            "status": "healthy",
            "service": "ai-memory-runtime",
            "model_state": "READY",
            "qdrant_ok": True,
            "sqlite_ok": True,
        }
    })
    a_server = FakeIpcServer(a_sock, {
        "admin.status": {
            "engine": {"state": "READY", "device": "cuda"},
            "collections": ["ai_memory"],
            "sqlite_path": tmp_sqlite_db,
            "status": "healthy",
        },
        "status.metrics": {
            "requests_total": 10,
            "errors_total": 1,
            "outbox": {"processed": 5, "dead_letter": 1},
        },
        "outbox.list_dead_letters": {
            "total": 1,
            "dead_letters": [
                {
                    "id": 101,
                    "target_collection": "ai_memory",
                    "memory_id": "mem_fail_1",
                    "projection_revision": 1,
                    "retry_count": 5,
                    "last_error": "ConnectionRefusedError: Qdrant unreachable",
                    "dead_letter_at": 1791456000,
                }
            ],
        },
        "admin.collections": {"ai_memory": {"points_count": 5}},
    })

    await b_server.start()
    await a_server.start()

    try:
        report = await run_diagnostics(
            admin_socket=a_sock,
            business_socket=b_sock,
            timeout=2.0,
        )
        assert report["status"] == "degraded"
        assert report["exit_code"] == 1
        assert report["checks"]["outbox_dead_letters"]["status"] == "degraded"
        assert report["checks"]["outbox_dead_letters"]["dead_letters_count"] == 1
    finally:
        await b_server.stop()
        await a_server.stop()


@pytest.mark.asyncio
async def test_doctor_degraded_on_model_loading(tmp_path, tmp_sqlite_db):
    """Doctor reports DEGRADED (1) when model is loading/idle."""
    b_sock = str(tmp_path / "b.sock")
    a_sock = str(tmp_path / "a.sock")

    b_server = FakeIpcServer(b_sock, {
        "system.health": {
            "status": "healthy",
            "service": "ai-memory-runtime",
            "model_state": "LOADING",
            "qdrant_ok": True,
            "sqlite_ok": True,
        }
    })
    a_server = FakeIpcServer(a_sock, {
        "admin.status": {
            "engine": {"state": "LOADING", "device": "cuda"},
            "collections": ["ai_memory"],
            "sqlite_path": tmp_sqlite_db,
            "status": "healthy",
        },
        "status.metrics": {"requests_total": 0, "errors_total": 0},
        "outbox.list_dead_letters": {"total": 0, "dead_letters": []},
        "admin.collections": {"ai_memory": {"points_count": 5}},
    })

    await b_server.start()
    await a_server.start()

    try:
        report = await run_diagnostics(
            admin_socket=a_sock,
            business_socket=b_sock,
            timeout=2.0,
        )
        assert report["status"] == "degraded"
        assert report["exit_code"] == 1
        assert report["checks"]["business_ipc"]["status"] == "degraded"
    finally:
        await b_server.stop()
        await a_server.stop()


@pytest.mark.asyncio
async def test_doctor_unhealthy_on_missing_sockets(tmp_path):
    """Doctor reports UNHEALTHY (2) when sockets cannot be reached."""
    missing_b = str(tmp_path / "nonexistent_b.sock")
    missing_a = str(tmp_path / "nonexistent_a.sock")

    report = await run_diagnostics(
        admin_socket=missing_a,
        business_socket=missing_b,
        timeout=1.0,
    )
    assert report["status"] == "unhealthy"
    assert report["exit_code"] == 2
    assert report["checks"]["business_ipc"]["status"] == "unhealthy"
    assert report["checks"]["admin_ipc"]["status"] == "unhealthy"


def test_sqlite_integrity_corrupted_file(tmp_path):
    """Corrupted SQLite file is detected as unhealthy."""
    corrupt_db = tmp_path / "corrupt.db"
    corrupt_db.write_bytes(b"corrupted header and junk data")

    res = check_sqlite_integrity(str(corrupt_db), timeout=2.0)
    assert res["status"] == "unhealthy"
    assert res["quick_check"] in ("failed", "error")


def test_sanitize_dead_letters_excludes_content():
    """Dead letter sanitizer strictly omits memory content, source_refs, and credentials."""
    raw = [
        {
            "id": 1,
            "memory_id": "mem_101",
            "target_collection": "ai_memory",
            "projection_revision": 2,
            "retry_count": 3,
            "last_error": "Connection reset by peer",
            "content": "CONFIDENTIAL SECRET CONTENT",
            "source_refs": ["secret_session_1"],
            "api_key": "sk-123456789",
        }
    ]
    sanitized = sanitize_dead_letters(raw)
    assert len(sanitized) == 1
    item = sanitized[0]
    assert item["id"] == 1
    assert item["memory_id"] == "mem_101"
    assert "content" not in item
    assert "source_refs" not in item
    assert "api_key" not in item
    assert "CONFIDENTIAL" not in json.dumps(item)


def test_cli_execution_with_flags(tmp_path, tmp_sqlite_db):
    """Test invoking scripts/amr_doctor.py via CLI process."""
    cmd = [
        sys.executable,
        str(ROOT_DIR / "scripts" / "amr_doctor.py"),
        "--socket", str(tmp_path / "missing_a.sock"),
        "--business-socket", str(tmp_path / "missing_b.sock"),
        "--json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 2
    data = json.loads(proc.stdout)
    assert data["status"] == "unhealthy"
    assert data["exit_code"] == 2


@pytest.mark.asyncio
async def test_doctor_with_config_file(tmp_path, tmp_sqlite_db):
    """Doctor respects custom configuration file with custom sqlite_path."""
    b_sock = str(tmp_path / "custom_b.sock")
    a_sock = str(tmp_path / "custom_a.sock")

    b_server = FakeIpcServer(b_sock, {
        "system.health": {
            "status": "healthy",
            "service": "ai-memory-runtime",
            "model_state": "READY",
            "qdrant_ok": True,
            "sqlite_ok": True,
        }
    })
    a_server = FakeIpcServer(a_sock, {
        "admin.status": {
            "engine": {"state": "READY", "device": "cuda"},
            "collections": ["ai_memory"],
            "sqlite_path": tmp_sqlite_db,
            "status": "healthy",
        },
        "status.metrics": {"requests_total": 5, "errors_total": 0},
        "outbox.list_dead_letters": {"total": 0, "dead_letters": []},
        "admin.collections": {"ai_memory": {"points_count": 10}},
    })

    cfg_file = tmp_path / "test_config.yaml"
    cfg_file.write_text(f"""
server:
  business_socket: "{b_sock}"
  admin_socket: "{a_sock}"
storage:
  sqlite_path: "{tmp_sqlite_db}"
""")

    await b_server.start()
    await a_server.start()

    try:
        report = await run_diagnostics(
            config_path=str(cfg_file),
            timeout=2.0,
        )
        assert report["status"] == "healthy"
        assert report["exit_code"] == 0
    finally:
        await b_server.stop()
        await a_server.stop()


def test_doctor_human_format_unhealthy():
    """Verify human format renders warning and error tags correctly."""
    fake_report = {
        "status": "unhealthy",
        "exit_code": 2,
        "checks": {
            "business_ipc": {"status": "unhealthy", "socket": "/tmp/b.sock", "error": "ConnectionRefused"},
            "admin_ipc": {"status": "degraded", "socket": "/tmp/a.sock", "engine_state": "LOADING"},
            "sqlite": {"status": "unhealthy", "path": "/tmp/s.db", "error": "file_not_found"},
            "qdrant": {"status": "ok", "collections_count": 1, "collections": {"ai_memory": {}}},
            "metrics": {"status": "ok", "summary": {"requests_total": 0}},
            "outbox_dead_letters": {"status": "degraded", "dead_letters_count": 3, "warning": "Has dead letters"},
        }
    }
    rendered = format_human_report(fake_report)
    assert "[ FAIL ] Business IPC" in rendered
    assert "[ WARN ] Admin IPC" in rendered
    assert "[ FAIL ] SQLite SSOT" in rendered
    assert "[ WARN ] Outbox Dead-Ltr" in rendered
    assert "Overall Status: UNHEALTHY (exit code 2)" in rendered


"""
AMR v5 Usability, Developer Experience & Documentation Consistency Audit Tests.
Track: r5-usability
Validates end-to-end beginner flow, MCP bridge, admin CLI, contract usability,
and solidifies documented discrepancies.
"""

import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from unittest.mock import MagicMock
import pytest
from qdrant_client import QdrantClient

from config.settings import AppConfig, load_config
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore
from src.main import AMRApplication
from src.service.memory_status import normalize_status


class FakeEngine:
    def __init__(self, dim: int = 1024):
        self.dim = dim
        self.state = MagicMock()
        self.state.value = "READY"
        self.model_path = "mock/bge-m3"

    async def embed(self, texts):
        results = []
        for t in texts:
            val = float(abs(hash(t)) % 1000 + 1) / 1000.0
            vec = [val] * self.dim
            norm = sum(x * x for x in vec) ** 0.5 or 1.0
            results.append([x / norm for x in vec])
        return results

    async def get_model_status(self):
        return {
            "state": "READY",
            "allocated_mb": 0.0,
            "reserved_mb": 0.0,
            "driver_used_mb": 0.0,
            "queue_depth": 0,
            "max_queue_size": 64,
            "total_inferences": 0,
            "device": "cpu",
            "idle_timeout_seconds": 0,
            "last_error": None,
            "unhealthy": False,
        }

    async def load_model(self):
        pass

    async def unload_model(self):
        pass

    async def preload(self):
        pass

    def close(self):
        pass


@pytest.fixture
def isolated_amr_env(tmp_path):
    """Sets up an isolated AMR daemon instance on temporary sockets."""
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)

    biz_sock = str(run_dir / "audit_biz.sock")
    adm_sock = str(run_dir / "audit_adm.sock")
    db_path = str(tmp_path / "audit_sessions.db")
    cfg_path = str(tmp_path / "audit_config.yaml")

    cfg_content = f"""server:
  business_socket: "{biz_sock}"
  admin_socket: "{adm_sock}"
  socket_mode: 0o600
  allowed_agents:
    - "openclaw"
    - "system"
    - "zcode"
    - "test_runner"
model:
  name_or_path: "mock/bge-m3"
  device: "cpu"
  fp16: false
storage:
  sqlite_path: "{db_path}"
qdrant:
  url: "http://127.0.0.1:1"
search:
  default_score_threshold: 0.1
"""
    with open(cfg_path, "w", encoding="utf-8") as f:
        f.write(cfg_content)
    os.chmod(cfg_path, 0o600)

    # Reset in-memory Qdrant
    QdrantManager.reset_instance()
    q_client = QdrantClient(":memory:")
    qm = QdrantManager(client=q_client, auto_init_collections=True)
    store = SessionStore(db_path=db_path)
    engine = FakeEngine()

    app = AMRApplication(
        config_path=cfg_path,
        engine=engine,
        qdrant_manager=qm,
        session_store=store,
    )

    daemon_loop = asyncio.new_event_loop()
    stop_event = threading.Event()

    def run_daemon():
        asyncio.set_event_loop(daemon_loop)

        async def _main():
            await app.start()
            while not stop_event.is_set():
                await asyncio.sleep(0.05)
            await app.stop()

        daemon_loop.run_until_complete(_main())

    t = threading.Thread(target=run_daemon, daemon=True)
    t.start()

    # Wait for sockets to be ready
    t0 = time.time()
    while not (os.path.exists(biz_sock) and os.path.exists(adm_sock)):
        if time.time() - t0 > 5.0:
            raise TimeoutError("AMR daemon failed to bind sockets within 5s")
        time.sleep(0.05)

    yield {
        "biz_sock": biz_sock,
        "adm_sock": adm_sock,
        "db_path": db_path,
        "cfg_path": cfg_path,
        "qdrant": qm,
        "store": store,
        "app": app,
    }

    stop_event.set()
    t.join(timeout=4.0)
    QdrantManager.reset_instance()


def test_fresh_agent_minimal_flow_and_doc_inconsistencies(isolated_amr_env):
    """
    Simulates a brand new developer performing the minimal usable flow via scripts/mcp-bridge.sh:
    initialize -> tools/list -> memory_create -> memory_get -> memory_search -> memory_update -> memory_delete.
    Asserts exact field presence, eventual consistency, and captures discrepancies.
    """
    biz_sock = isolated_amr_env["biz_sock"]
    qm = isolated_amr_env["qdrant"]

    env = os.environ.copy()
    env["AMR_SOCKET_PATH"] = biz_sock
    env["AMR_SOURCE_AGENT"] = "openclaw"
    env["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)

    script_path = str(Path(__file__).resolve().parent.parent / "scripts" / "mcp-bridge.sh")

    proc = subprocess.Popen(
        [script_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    def rpc(req):
        proc.stdin.write(json.dumps(req) + "\n")
        proc.stdin.flush()
        line = proc.stdout.readline()
        assert line, f"mcp-bridge exited unexpectedly: {proc.stderr.read()}"
        return json.loads(line)

    # 1. initialize
    r1 = rpc({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    assert r1.get("result", {}).get("serverInfo", {}).get("name") == "ai-memory-runtime-mcp"

    # 2. tools/list: Verify tools list
    r2 = rpc({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    tools = [t["name"] for t in r2["result"]["tools"]]
    # DOC-AMR-06 lines 61-67 claim 5 tools, but code exposes 9 tools
    assert len(tools) == 9
    assert "memory_history" in tools
    assert "memory_delete" in tools

    # 3. memory_create: Note DOC-AMR-04 §1.2 says source_message_ids/session_id, but tool expects content/project_id/type
    r3 = rpc({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "AEP architecture consensus: single writer projection with CAS",
                "project_id": "aep-arch",
                "type": "decision/arch",
            },
        },
    })
    assert not r3["result"]["isError"]
    c3 = json.loads(r3["result"]["content"][0]["text"])
    mid = c3["memory_id"]
    assert c3["version"] == 1
    # Status is canonical uppercase "ACTIVE", while DOC-AMR-04 says lowercase "active"
    assert c3["status"] == "ACTIVE"
    # Returns projection_status = "pending" for async outbox projection
    assert c3["projection_status"] == "pending"
    assert c3["projection_revision"] == 1
    # Because source_refs was not passed, stamped provenance-missing
    assert any("provenance-missing" in s for s in c3.get("source_refs", []))

    # 4. memory_get: Immediate read from SQLite SSOT (strong consistency)
    r4 = rpc({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "memory_get",
            "arguments": {"memory_id": mid},
        },
    })
    assert not r4["result"]["isError"]
    c4 = json.loads(r4["result"]["content"][0]["text"])
    assert c4["memory_id"] == mid
    assert c4["status"] == "ACTIVE"
    assert c4["version"] == 1

    # 5. Wait for OutboxWorker projection to Qdrant (weak consistency)
    projected = False
    for _ in range(50):
        time.sleep(0.1)
        pts = qm.client.scroll(collection_name="ai_memory", limit=10)[0]
        if any(p.payload.get("memory_id") == mid for p in pts):
            projected = True
            break
    assert projected, "OutboxWorker did not project memory to Qdrant within 5s"

    # memory_search: Weakly consistent search
    r5 = rpc({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "memory_search",
            "arguments": {
                "query": "AEP architecture consensus",
                "project_id": "aep-arch",
                "limit": 5,
                "score_threshold": 0.0,
            },
        },
    })
    assert not r5["result"]["isError"]
    c5 = json.loads(r5["result"]["content"][0]["text"])
    assert c5["total"] >= 1
    found_item = next(m for m in c5["results"] if m["memory_id"] == mid)
    assert found_item["status"] == "ACTIVE"
    # v5 fix: projection payload carries created_at so search results are complete.
    assert found_item["created_at"] is not None

    # 6. memory_update:
    # 6a. Omitting expected_version when changing content fails with self-explanatory error
    r6_fail = rpc({
        "jsonrpc": "2.0",
        "id": 61,
        "method": "tools/call",
        "params": {
            "name": "memory_update",
            "arguments": {
                "memory_id": mid,
                "content": "AEP architecture consensus revised",
            },
        },
    })
    assert r6_fail["result"]["isError"]
    assert "expected_version" in r6_fail["result"]["content"][0]["text"]

    # 6b. Wrong expected_version triggers VERSION_CONFLICT (-32002) with current_version in data
    r6_conflict = rpc({
        "jsonrpc": "2.0",
        "id": 62,
        "method": "tools/call",
        "params": {
            "name": "memory_update",
            "arguments": {
                "memory_id": mid,
                "content": "AEP architecture consensus revised",
                "expected_version": 99,
                "change_reason": "revision test",
            },
        },
    })
    assert r6_conflict["result"]["isError"]
    err_text = r6_conflict["result"]["content"][0]["text"]
    assert "[-32002]" in err_text or "VERSION_CONFLICT" in err_text
    assert '"current_version": 1' in err_text

    # 6c. Successful update
    r6 = rpc({
        "jsonrpc": "2.0",
        "id": 63,
        "method": "tools/call",
        "params": {
            "name": "memory_update",
            "arguments": {
                "memory_id": mid,
                "content": "AEP architecture consensus: single writer projection with CAS (v2)",
                "expected_version": 1,
                "change_reason": "version 2 refinement",
            },
        },
    })
    assert not r6["result"]["isError"]
    c6 = json.loads(r6["result"]["content"][0]["text"])
    assert c6["version"] == 2
    assert c6["projection_revision"] == 2
    assert c6["projection_status"] == "pending"

    # 7. memory_delete: Soft delete
    r7 = rpc({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "memory_delete",
            "arguments": {
                "memory_id": mid,
                "reason": "lifecycle cleanup",
            },
        },
    })
    assert not r7["result"]["isError"]
    c7 = json.loads(r7["result"]["content"][0]["text"])
    assert c7["current_status"] == "DELETED"
    assert c7["projection_status"] == "pending"
    assert c7["projection_revision"] == 3

    proc.stdin.close()
    proc.wait(timeout=2)


def test_idempotency_key_replay_and_conflict(isolated_amr_env):
    """
    Validates idempotency contract:
    - Same idempotency_key + same payload: returns identical result with idempotent_replay=True
    - Same idempotency_key + different payload: returns -32006 IDEMPOTENCY_CONFLICT with structured data
    """
    biz_sock = isolated_amr_env["biz_sock"]
    env = os.environ.copy()
    env["AMR_SOCKET_PATH"] = biz_sock
    env["AMR_SOURCE_AGENT"] = "openclaw"
    env["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)

    script_path = str(Path(__file__).resolve().parent.parent / "scripts" / "mcp-bridge.sh")
    proc = subprocess.Popen(
        [script_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    def rpc(req):
        proc.stdin.write(json.dumps(req) + "\n")
        proc.stdin.flush()
        return json.loads(proc.stdout.readline())

    rpc({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})

    idem_key = "idemp-test-key-001"
    # Initial create
    r1 = rpc({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "Unique content for idempotency testing",
                "idempotency_key": idem_key,
            },
        },
    })
    assert not r1["result"]["isError"]
    c1 = json.loads(r1["result"]["content"][0]["text"])
    mid1 = c1["memory_id"]

    # Replay same payload
    r2 = rpc({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "Unique content for idempotency testing",
                "idempotency_key": idem_key,
            },
        },
    })
    assert not r2["result"]["isError"]
    c2 = json.loads(r2["result"]["content"][0]["text"])
    assert c2["memory_id"] == mid1
    assert c2.get("idempotent_replay") is True

    # Replay same key with DIFFERENT payload -> conflict
    r3 = rpc({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "Conflicting different content under same key",
                "idempotency_key": idem_key,
            },
        },
    })
    assert r3["result"]["isError"]
    err_text = r3["result"]["content"][0]["text"]
    assert "[-32006]" in err_text or "IDEMPOTENCY_CONFLICT" in err_text
    assert idem_key in err_text

    proc.stdin.close()
    proc.wait(timeout=2)


def test_admin_cli_operability(isolated_amr_env):
    """
    Validates src/admin/cli.py on isolated admin socket:
    status, metrics, dead-letters, replay, collections.
    """
    adm_sock = isolated_amr_env["adm_sock"]
    py_bin = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"

    def run_cmd(subcmd_args):
        p = subprocess.run(
            [py_bin, "src/admin/cli.py", "-s", adm_sock] + subcmd_args,
            capture_output=True,
            text=True,
        )
        assert p.returncode == 0, f"admin CLI {subcmd_args} failed: {p.stderr}"
        return json.loads(p.stdout)

    status_out = run_cmd(["status"])
    assert status_out["status"] == "healthy"
    assert "engine" in status_out
    assert "collections" in status_out

    metrics_out = run_cmd(["metrics"])
    assert "gauges" in metrics_out
    assert "outbox_lag_seconds" in metrics_out["gauges"]
    assert "outbox_dead_letter" in metrics_out["gauges"]

    dl_out = run_cmd(["dead-letters"])
    assert "dead_letters" in dl_out
    assert dl_out["total"] == 0

    replay_out = run_cmd(["replay"])
    assert replay_out["status"] == "ok"
    assert replay_out["replayed_count"] == 0

    colls_out = run_cmd(["collections"])
    assert "ai_memory" in colls_out


def test_documentation_and_usability_contracts():
    """Regression assertions for the usability fixes (post-fix contract)."""
    root_dir = Path(__file__).resolve().parent.parent

    # 1. The rollout docs must reference a real doctor script.
    doctor_script = root_dir / "scripts" / "amr_doctor.py"
    assert doctor_script.exists(), "scripts/amr_doctor.py must exist"
    rollout = (root_dir / "docs" / "ROLLOUT-AMR-RELIABILITY.md").read_text(encoding="utf-8")
    assert "scripts/amr_doctor.py" in rollout
    assert "amr doctor --json" not in rollout

    py_bin = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"

    # 2/3. Entrypoints import cleanly without PYTHONPATH (sys.path bootstrap).
    clean_env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    for rel in ("src/main.py", "src/interfaces/mcp/bridge.py"):
        p = subprocess.run(
            [py_bin, "-c", f"import runpy; runpy.run_path({rel!r}, run_name='not_main')"],
            cwd=str(root_dir),
            env=clean_env,
            capture_output=True,
            text=True,
        )
        assert p.returncode == 0, f"{rel} failed without PYTHONPATH: {p.stderr}"
        assert "ModuleNotFoundError" not in p.stderr

    # 4. Admin RPC names in docs match the dispatcher.
    api_doc = (root_dir / "docs" / "DOC-AMR-04-API.md").read_text(encoding="utf-8")
    assert "admin.status" in api_doc and "admin.collections" in api_doc
    assert "admin.get_status" not in api_doc and "admin.collection_list" not in api_doc

    # 5. Status normalization + v5 exact-ID redirect use HISTORICAL, not SUPERSEDED.
    assert normalize_status("superseded") == "HISTORICAL"
    svc_src = (root_dir / "src" / "service" / "memory_service.py").read_text(encoding="utf-8")
    assert 'status_val == "SUPERSEDED"' not in svc_src
    assert "normalize_status(status" in svc_src

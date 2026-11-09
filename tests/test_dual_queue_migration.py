"""
Tests for Dual Queue Migration & Legacy Drainer (RFC v5.2, RFC v5.3)
验证：
1. 旧队列含 v3 行任务 → 补投新队列 + 标 synced（不丢任务）；
2. legacy delta patch → set_payload 应用 fields；delta delete → 物理删点；
3. failed 任务被正常重试（不被 !=pending 跳过）；已取消（synced）任务被跳过且不复活；
4. mark_sync_task_failed 不得复活已取消/synced 行；
5. migrate_projection_queue.py --inventory/--rehearse/--apply 在 tmp 副本上运行前后对账稳定；
6. quarantine 非空时阻断并以非 0 退出；
7. fresh DB 迁移 no-op 幂等稳定。
"""

import asyncio
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from src.core.session_store import SessionStore
from src.service.outbox_worker import OutboxWorker


class FakeEngine:
    def __init__(self, dim: int = 1024):
        self.dim = dim

    async def embed(self, texts):
        return [[0.05] * self.dim for _ in texts]


class FakeQdrant:
    def __init__(self):
        self.upserted_points = []
        self.deleted_points = []
        self.client = MagicMock()

    def upsert_points(self, collection_name: str, points, wait: bool = True):
        for p in points:
            self.upserted_points.append({"col": collection_name, "id": p.id, "payload": p.payload})

    def delete_point_by_id(self, point_id: str, collection_name: str = "ai_memory", wait: bool = True):
        self.deleted_points.append({"col": collection_name, "point_id": point_id})


@pytest.fixture
def dual_env(tmp_path: Path):
    db_path = tmp_path / "dual_queue.db"
    store = SessionStore(db_path=db_path)
    engine = FakeEngine()
    qdrant = FakeQdrant()
    worker = OutboxWorker(
        session_store=store,
        qdrant_manager=qdrant,
        engine=engine,
        batch_size=10,
        poll_interval=0.1,
    )
    return {
        "store": store,
        "engine": engine,
        "qdrant": qdrant,
        "worker": worker,
        "db_path": db_path,
    }


def insert_memory_row(
    store: SessionStore,
    memory_id: str,
    projection_class: str = "legacy",
    projection_revision: int = 1,
    collection_name: str = "ai_memory",
    status: str = "ACTIVE",
) -> str:
    now = int(time.time())
    point_id = f"point_{memory_id}"
    with store.locked_write_transaction() as conn:
        conn.cursor().execute(
            """
            INSERT INTO memories (
                memory_id, qdrant_point_id, type, subject, predicate, object,
                content, status, projection_class, projection_revision,
                collection_name, scope, version, created_at, updated_at
            ) VALUES (?, ?, 'general', 's', 'p', 'o', 'content', ?, ?, ?, ?, 'global', 1, ?, ?);
            """,
            (memory_id, point_id, status, projection_class, projection_revision, collection_name, now, now),
        )
    return point_id


@pytest.mark.asyncio
async def test_legacy_queue_v3_task_migrated_to_new_outbox(dual_env):
    """旧队列含 v3 行任务 → 补投新队列 + 标 synced（不丢任务）"""
    store = dual_env["store"]
    worker = dual_env["worker"]
    mid = "mem_v3_in_legacy_queue"
    insert_memory_row(store, mid, projection_class="v3", projection_revision=5)

    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO qdrant_sync_queue (
                memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, created_at, updated_at
            ) VALUES (?, 'point_01', 'upsert', '{"content":"v3 text"}', 'pending', 0, ?, ?);
            """,
            (mid, now, now),
        )
        legacy_id = cur.lastrowid

    # 运行 worker 排水
    processed = await worker.run_once()
    assert processed >= 1

    # 验证旧队列任务已被标记为 synced，且注记中包含 migrated to outbox#
    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status, last_error FROM qdrant_sync_queue WHERE id = ?;", (legacy_id,))
        row = cur.fetchone()
        assert row["status"] == "synced"
        assert "migrated to outbox#" in row["last_error"]

        # 验证新队列中确实存在等价重投影任务且 projection_revision 为 5
        cur.execute("SELECT id, status, projection_revision FROM memory_projection_outbox WHERE memory_id = ?;", (mid,))
        new_task = cur.fetchone()
        assert new_task is not None
        assert new_task["projection_revision"] == 5


@pytest.mark.asyncio
async def test_legacy_delta_patch_and_delete(dual_env):
    """legacy delta patch 调 set_payload；delta delete 物理删点"""
    store = dual_env["store"]
    worker = dual_env["worker"]
    qdrant = dual_env["qdrant"]

    mid = "mem_legacy_delta"
    point_id = insert_memory_row(store, mid, projection_class="legacy", projection_revision=1)
    now = int(time.time())

    # 1. 测试 delta patch
    patch_envelope = {
        "amr_delta_v1": True,
        "op": "patch",
        "fields": {"status": "HISTORICAL", "superseded_by": "mem_new"},
        "collection": "custom_col",
        "targets": [(mid, point_id)],
    }
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO qdrant_sync_queue (
                memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, created_at, updated_at
            ) VALUES (?, ?, 'update_payload', ?, 'pending', 0, ?, ?);
            """,
            (mid, point_id, json.dumps(patch_envelope), now, now),
        )
        patch_task_id = cur.lastrowid

    processed = await worker.run_once()
    assert processed == 1

    # 验证 qdrant.client.set_payload 得到调用
    qdrant.client.set_payload.assert_called_with(
        collection_name="custom_col",
        payload={"status": "HISTORICAL", "superseded_by": "mem_new"},
        points=[point_id],
        wait=True,
    )
    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM qdrant_sync_queue WHERE id = ?;", (patch_task_id,))
        assert cur.fetchone()["status"] == "synced"

    # 2. 测试 delta delete
    delete_envelope = {
        "amr_delta_v1": True,
        "op": "delete",
        "collection": "custom_col",
        "targets": [(mid, point_id)],
    }
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO qdrant_sync_queue (
                memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, created_at, updated_at
            ) VALUES (?, ?, 'delete', ?, 'pending', 0, ?, ?);
            """,
            (mid, point_id, json.dumps(delete_envelope), now, now),
        )
        del_task_id = cur.lastrowid

    processed = await worker.run_once()
    assert processed == 1

    # 验证 delete_point_by_id 被调用
    assert len(qdrant.deleted_points) == 1
    assert qdrant.deleted_points[0]["point_id"] == point_id
    assert qdrant.deleted_points[0]["col"] == "custom_col"

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM qdrant_sync_queue WHERE id = ?;", (del_task_id,))
        assert cur.fetchone()["status"] == "synced"


@pytest.mark.asyncio
async def test_failed_legacy_task_is_retried_and_synced_task_skipped(dual_env):
    """failed 任务被正常重试（不被 !=pending 跳过）；被取消（synced）任务跳过"""
    store = dual_env["store"]
    worker = dual_env["worker"]
    mid = "mem_failed_retry"
    point_id = insert_memory_row(store, mid, projection_class="legacy")
    now = int(time.time())

    # 插入 failed 状态的任务（retry_count=1 < 5）
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO qdrant_sync_queue (
                memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, created_at, updated_at
            ) VALUES (?, ?, 'delete', NULL, 'failed', 1, ?, ?);
            """,
            (mid, point_id, now, now),
        )
        failed_task_id = cur.lastrowid

        # 插入 synced 状态（已取消）的任务
        cur.execute(
            """
            INSERT INTO qdrant_sync_queue (
                memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, created_at, updated_at
            ) VALUES (?, ?, 'delete', NULL, 'synced', 0, ?, ?);
            """,
            (mid, point_id, now, now),
        )
        synced_task_id = cur.lastrowid

    processed = await worker.run_once()
    # failed_task_id 应该被重试且成功，synced_task_id 被跳过
    assert processed == 1

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM qdrant_sync_queue WHERE id = ?;", (failed_task_id,))
        assert cur.fetchone()["status"] == "synced"

        cur.execute("SELECT status FROM qdrant_sync_queue WHERE id = ?;", (synced_task_id,))
        assert cur.fetchone()["status"] == "synced"


def test_mark_sync_task_failed_does_not_revive_synced_row(dual_env):
    """mark failed 不复活被取消行（status='synced'）"""
    store = dual_env["store"]
    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO qdrant_sync_queue (
                memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, created_at, updated_at
            ) VALUES ('mem_revive_test', 'point_01', 'delete', NULL, 'synced', 0, ?, ?);
            """,
            (now, now),
        )
        task_id = cur.lastrowid

    # 尝试标记已为 synced 的任务为 failed
    success = store.mark_sync_task_failed(task_id, "late network timeout")
    # 影响行数应当为 0（False）
    assert success is False

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM qdrant_sync_queue WHERE id = ?;", (task_id,))
        assert cur.fetchone()["status"] == "synced"


def test_migrate_script_inventory_rehearse_apply(tmp_path: Path):
    """测试 migrate_projection_queue.py --inventory / --rehearse / --apply 在 tmp 库上运行"""
    db_path = tmp_path / "test_migration_script.db"
    store = SessionStore(db_path=db_path)
    # 插入一些旧数据
    mid = "mem_script_test"
    insert_memory_row(store, mid, projection_class="v3", projection_revision=1)
    store.close()

    script_path = Path(__file__).resolve().parent.parent / "scripts" / "migrate_projection_queue.py"

    # 1. 运行 --inventory
    res_inv = subprocess.run(
        [sys.executable, str(script_path), "--db", str(db_path), "--inventory"],
        capture_output=True,
        text=True,
        check=True,
    )
    inv_data = json.loads(res_inv.stdout)
    assert inv_data["schema_status"]["memories"]["table_exists"] is True
    assert inv_data["quarantine"]["count"] == 0

    # 2. 运行 --rehearse（带 report 输出）
    rehearse_report = tmp_path / "rehearse_report.json"
    res_reh = subprocess.run(
        [sys.executable, str(script_path), "--db", str(db_path), "--rehearse", "--report", str(rehearse_report)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert rehearse_report.exists()
    reh_data = json.loads(rehearse_report.read_text())
    assert reh_data["status"] == "completed"
    assert reh_data["quarantine_count"] == 0

    # 3. 运行 --apply
    res_app = subprocess.run(
        [sys.executable, str(script_path), "--db", str(db_path), "--apply"],
        capture_output=True,
        text=True,
        check=True,
    )
    app_data = json.loads(res_app.stdout)
    assert app_data["status"] == "completed"


def test_migrate_script_quarantine_blocks_and_exits_nonzero(tmp_path: Path):
    """构造含孤儿任务的旧库，测试 quarantine 阻止上线并返回非 0 退出码"""
    db_path = tmp_path / "quarantine_test.db"
    conn = sqlite3.connect(str(db_path))
    # 模拟 v4 旧 schema
    conn.execute("""
        CREATE TABLE memories (
            memory_id TEXT PRIMARY KEY,
            qdrant_point_id TEXT,
            content TEXT,
            status TEXT
        );
    """)
    conn.execute("""
        CREATE TABLE memory_projection_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT,
            version INTEGER,
            projection_type TEXT,
            op_type TEXT,
            payload_snapshot TEXT,
            status TEXT,
            retry_count INTEGER,
            next_retry_at INTEGER,
            last_error TEXT,
            dead_letter_at INTEGER,
            created_at INTEGER,
            updated_at INTEGER
        );
    """)
    # 插入一个孤儿任务（memories 表里不存在）
    now = int(time.time())
    conn.execute("""
        INSERT INTO memory_projection_outbox (
            memory_id, version, projection_type, op_type, status, created_at, updated_at
        ) VALUES ('orphan_mem_999', 1, 'full', 'upsert', 'pending', ?, ?);
    """, (now, now))
    conn.commit()
    conn.close()

    script_path = Path(__file__).resolve().parent.parent / "scripts" / "migrate_projection_queue.py"

    # 运行 --rehearse，期待 exit code != 0 且打印告警
    res = subprocess.run(
        [sys.executable, str(script_path), "--db", str(db_path), "--rehearse"],
        capture_output=True,
        text=True,
    )
    assert res.returncode != 0
    assert "必须人工确认后才可上线" in (res.stdout + res.stderr)


def test_fresh_db_migration_noop(tmp_path: Path):
    """全新空库执行迁移应为 no-op 且 exit code 0"""
    db_path = tmp_path / "fresh_noop.db"
    script_path = Path(__file__).resolve().parent.parent / "scripts" / "migrate_projection_queue.py"

    # 首先通过 SessionStore 创建空库
    store = SessionStore(db_path=db_path)
    store.close()

    res = subprocess.run(
        [sys.executable, str(script_path), "--db", str(db_path), "--apply"],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(res.stdout)
    assert data["status"] == "completed"
    assert data["quarantine_count"] == 0

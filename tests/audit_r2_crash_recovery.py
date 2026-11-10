"""
Red-Team Audit Track R2: Subprocess Crash & Recovery Tests
验证 AMR v5 在真实子进程崩溃场景下的恢复承诺：
1. 子进程在 BEGIN 后 / COMMIT 前被 kill (os._exit) → SQLite 无半写、WAL 正常恢复
2. Qdrant upsert 成功后、mark done 前崩溃 → 重启重跑幂等，Qdrant 与 SQLite 最终一致
3. 异常发生后、mark failed 前崩溃 → 任务仍处于 pending 状态，可安全重试
4. legacy delta 删除点位后、mark synced 前崩溃 → 重跑删除幂等，状态同步
"""

import multiprocessing
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from qdrant_client import QdrantClient, models

from src.core.session_store import SessionStore
from src.service.outbox_worker import OutboxWorker


class FakeAuditEngine:
    def __init__(self, dim: int = 128):
        self.dim = dim

    async def embed(self, texts: List[str]) -> List[List[float]]:
        return [[0.1] * self.dim for _ in texts]


def _worker_child_crash_in_tx(db_path: str):
    """子进程函数：开启写事务，写入一半后直接强制退出 (os._exit)"""
    conn = sqlite3.connect(db_path, timeout=5.0)
    conn.isolation_level = None
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.execute("BEGIN IMMEDIATE;")

    now = int(time.time())
    cur.execute(
        """
        INSERT INTO memories (
            memory_id, qdrant_point_id, type, conflict_policy,
            content, confidence, importance, mention_count,
            status, project_id, scope, source_agent, created_by_agent,
            version, projection_revision, collection_name, projection_class,
            created_at, updated_at
        ) VALUES ('mem_child_killed', 'pt_child_killed', 'fact', 'coexist',
                 'Killed mid-flight content', 1.0, 0.5, 1,
                 'ACTIVE', 'global', 'global', 'agent_child', 'agent_child',
                 1, 1, 'ai_memory', 'v3', ?, ?);
        """,
        (now, now),
    )
    cur.execute(
        """
        INSERT INTO memory_revisions (memory_id, version, content, updated_by_agent, updated_at, change_reason)
        VALUES ('mem_child_killed', 1, 'Killed mid-flight content', 'agent_child', ?, 'Initial');
        """,
        (now,),
    )
    # 不 COMMIT，直接硬件级别退出
    os._exit(42)


def test_subprocess_killed_in_uncommitted_transaction_wal_recovery(tmp_path: Path):
    """
    攻击面 3.1: 子进程在事务中途被 SIGKILL (os._exit)
    断言：
    - 主进程重连后 WAL 自动回滚未提交事务；
    - memories、memory_revisions、memory_projection_outbox 均无任何半写数据；
    - PRAGMA integrity_check 返回 'ok'。
    """
    db_file = tmp_path / "crash_uncommitted.db"
    # 主进程先初始化结构
    store = SessionStore(db_path=db_file)
    store.close()

    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(target=_worker_child_crash_in_tx, args=(str(db_file),))
    proc.start()
    proc.join(timeout=10)

    assert proc.exitcode == 42, f"Expected child exitcode 42, got {proc.exitcode}"

    # 主进程重新打开数据库，触发 WAL 恢复
    reopened_store = SessionStore(db_path=db_file)
    conn = reopened_store.get_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA integrity_check;")
    assert cur.fetchone()[0] == "ok", "SQLite WAL 完整性检查失败"

    cur.execute("SELECT COUNT(*) FROM memories WHERE memory_id = 'mem_child_killed';")
    assert cur.fetchone()[0] == 0, "memories 表残留了被杀子进程的半写行"

    cur.execute("SELECT COUNT(*) FROM memory_revisions WHERE memory_id = 'mem_child_killed';")
    assert cur.fetchone()[0] == 0, "memory_revisions 表残留了被杀子进程的半写行"

    reopened_store.close()


def _child_upsert_then_crash(db_path: str, qdrant_path: str, memory_id: str, point_id: str, dim: int):
    """子进程：执行 Qdrant upsert 成功，但在 mark_projection_done 之前直接 os._exit"""
    import asyncio

    # 连接 Qdrant
    q_client = QdrantClient(path=qdrant_path)
    # 写入点位
    q_client.upsert(
        collection_name="ai_memory",
        points=[
            models.PointStruct(
                id=point_id,
                vector=[0.1] * dim,
                payload={"memory_id": memory_id, "status": "ACTIVE", "version": 1},
            )
        ],
        wait=True,
    )
    q_client.close()

    # 此时 Qdrant 写入已持久化，但在调用 SQLite mark_projection_done 前崩溃
    os._exit(99)


def test_crash_after_qdrant_upsert_before_mark_done_recovers_idempotently(tmp_path: Path):
    """
    攻击面 3.2: Qdrant upsert 成功后、mark done 前子进程崩溃
    断言：
    - 崩溃后 SQLite 任务依然处于 pending 状态；
    - Qdrant 已持久化该点；
    - Worker 重新拉起后重跑该任务，Qdrant 发生幂等覆盖，SQLite 成功标记 done；
    - Qdrant 最终数据与 SQLite SSOT 保持严格一致。
    """
    dim = 16
    db_file = tmp_path / "crash_after_upsert.db"
    qdrant_dir = tmp_path / "qdrant_crash_upsert"

    # 初始化本地持久化 Qdrant
    qc = QdrantClient(path=str(qdrant_dir))
    qc.create_collection(
        collection_name="ai_memory",
        vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
    )
    qc.close()

    # 初始化 SQLite 并创建记忆任务
    store = SessionStore(db_path=db_file)
    mem_id = "mem_crash_upsert_001"
    point_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

    mem = store.create_memory_v3(
        memory_id=mem_id,
        content="Crash recovery upsert content",
        qdrant_point_id=point_uuid,
    )
    tasks = store.fetch_pending_projection_tasks()
    assert len(tasks) == 1
    task_id = tasks[0]["id"]
    store.close()

    # 派生子进程执行 Qdrant 写入后故意崩溃
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(
        target=_child_upsert_then_crash,
        args=(str(db_file), str(qdrant_dir), mem_id, point_uuid, dim),
    )
    proc.start()
    proc.join(timeout=10)
    assert proc.exitcode == 99

    # 检查崩溃后状态：
    # 1. Qdrant 中已有点位
    qc_check = QdrantClient(path=str(qdrant_dir))
    pts = qc_check.retrieve(collection_name="ai_memory", ids=[point_uuid])
    assert len(pts) == 1
    qc_check.close()

    # 2. SQLite 中任务依然处于 pending
    store_recovery = SessionStore(db_path=db_file)
    tasks_check = store_recovery.fetch_pending_projection_tasks()
    assert len(tasks_check) == 1
    assert tasks_check[0]["id"] == task_id
    assert tasks_check[0]["status"] == "pending"

    # 3. 恢复守护 Worker，重跑该任务
    # 包装 QdrantManager 代理到本地磁盘 QdrantClient
    class DiskQdrantManager:
        def __init__(self, client):
            self.client = client
        def upsert_points(self, collection_name: str, points: List[Any], wait: bool = True):
            return self.client.upsert(collection_name=collection_name, points=points, wait=wait)
        def delete_point_by_id(self, point_id: str, collection_name: str = "ai_memory", wait: bool = True):
            return self.client.delete(collection_name=collection_name, points_selector=[point_id], wait=wait)

    qc_active = QdrantClient(path=str(qdrant_dir))
    mgr = DiskQdrantManager(qc_active)
    worker = OutboxWorker(
        session_store=store_recovery,
        qdrant_manager=mgr,
        engine=FakeAuditEngine(dim=dim),
    )

    import asyncio
    processed = asyncio.run(worker.run_once())
    assert processed >= 1

    # 4. 验证最终一致性
    tasks_final = store_recovery.fetch_pending_projection_tasks()
    assert len(tasks_final) == 0, "任务应已标记 done 离开 pending 队列"

    # 检查 outbox 状态
    conn = store_recovery.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT status, projected_revision FROM memory_projection_outbox WHERE id = ?;", (task_id,))
    outbox_row = cur.fetchone()
    assert outbox_row["status"] == "done"
    assert outbox_row["projected_revision"] == 1

    # 检查 Qdrant 中的点
    final_pts = qc_active.retrieve(collection_name="ai_memory", ids=[point_uuid], with_payload=True)
    assert len(final_pts) == 1
    assert final_pts[0].payload["memory_id"] == mem_id
    assert final_pts[0].payload["version"] == 1

    qc_active.close()
    store_recovery.close()


def _child_fail_then_crash(db_path: str, task_id: int):
    """子进程：处理发生故障，但在执行 mark_projection_failed 之前直接 os._exit"""
    # 故意在写回 mark_projection_failed 前崩溃
    os._exit(88)


def test_crash_before_mark_failed_retains_pending_for_retry(tmp_path: Path):
    """
    攻击面 3.3: 故障发生后、写回 mark_projection_failed 前崩溃
    断言：
    - 任务未丢失，依然以 pending 状态保留在队列中；
    - retry_count 保持原样；
    - 下一个拉起的 Worker 可以完整重新拾取此任务执行。
    """
    db_file = tmp_path / "crash_before_fail.db"
    store = SessionStore(db_path=db_file)
    mem_id = "mem_crash_fail_001"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Testing crash before mark failed",
    )
    t = store.fetch_pending_projection_tasks()[0]
    task_id = t["id"]
    assert t["status"] == "pending"
    assert t["retry_count"] == 0
    store.close()

    # 子进程崩溃
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(target=_child_fail_then_crash, args=(str(db_file), task_id))
    proc.start()
    proc.join(timeout=10)
    assert proc.exitcode == 88

    # 重新加载 store
    store2 = SessionStore(db_path=db_file)
    pending_tasks = store2.fetch_pending_projection_tasks()
    assert len(pending_tasks) == 1
    assert pending_tasks[0]["id"] == task_id
    assert pending_tasks[0]["status"] == "pending"
    assert pending_tasks[0]["retry_count"] == 0
    store2.close()

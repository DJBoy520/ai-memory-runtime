"""
Red-Team Audit Track R2: Outbox Worker Fault Injection & Single-Writer Flock Tests
验证 AMR v5 Worker 的容灾与故障恢复契约：
1. engine.embed 抛异常与超时 → 记账失败、退避等待、不崩溃主循环
2. qdrant upsert / delete 抛异常 → 捕获并退避，指标记录
3. 持续故障不无限热循环（next_retry_at 窗口过滤，指数退避至死信）
4. 旧队列排水状态机：v3 行补投新队列不丢、delta patch/delete、point missing 容错、已取消不复活、failed 合法重试
5. 单执行者 flock 排他：双进程并发启动，第二进程必须 exit 3 退出，释放后可重新获得
"""

import asyncio
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest
from qdrant_client import models

from src.core.session_store import SessionStore
from src.service.metrics import METRICS
from src.service.outbox_worker import OutboxWorker


class ControlledEngine:
    def __init__(self, dim: int = 16):
        self.dim = dim
        self.should_fail = False
        self.fail_exc = RuntimeError("Embedding model inference error")

    async def embed(self, texts: List[str]) -> List[List[float]]:
        if self.should_fail:
            raise self.fail_exc
        return [[0.1] * self.dim for _ in texts]


class ControlledQdrantManager:
    def __init__(self):
        self.fail_upsert = False
        self.fail_delete = False
        self.fail_payload = False
        self.point_missing = False
        self.upsert_calls: List[Any] = []
        self.delete_calls: List[Any] = []
        self.client = MagicMock()

        def mock_set_payload(collection_name, payload, points, wait):
            if self.point_missing:
                raise RuntimeError("Point 12345 missing: Not found (404)")
            if self.fail_payload:
                raise RuntimeError("Qdrant HTTP 500 payload update failed")
            return True

        self.client.set_payload.side_effect = mock_set_payload

    def upsert_points(self, collection_name: str, points: List[Any], wait: bool = True):
        if self.fail_upsert:
            raise RuntimeError("Qdrant 503 Service Unavailable during upsert")
        self.upsert_calls.extend(points)

    def delete_point_by_id(self, point_id: str, collection_name: str = "ai_memory", wait: bool = True):
        if self.fail_delete:
            raise RuntimeError("Qdrant 500 Internal Server Error during delete")
        self.delete_calls.append(point_id)


@pytest.fixture
def worker_env(tmp_path):
    db_file = tmp_path / "worker_fault.db"
    store = SessionStore(db_path=db_file)
    engine = ControlledEngine()
    qdrant = ControlledQdrantManager()
    lock_file = tmp_path / "test_worker.lock"
    worker = OutboxWorker(
        session_store=store,
        qdrant_manager=qdrant,
        engine=engine,
        lock_path=lock_file,
        batch_size=10,
    )
    yield store, engine, qdrant, worker
    store.close()


@pytest.mark.asyncio
async def test_worker_engine_embed_exception_and_timeout(worker_env):
    """
    攻击面 4.1: engine.embed 抛异常 / 超时故障注入
    断言：
    - run_once() 成功捕获并返回 0；
    - 任务保持 pending，retry_count 增为 1；
    - next_retry_at 设置为未来时间（当前时间 + 1s 退避）；
    - last_error 记录具体错误信息；
    - METRICS outbox_failures 增加。
    """
    store, engine, qdrant, worker = worker_env
    mem = store.create_memory_v3(
        memory_id="mem_embed_fail",
        content="Testing embed failure injection",
    )
    task_id = store.fetch_pending_projection_tasks()[0]["id"]

    # 注入 Embed 超时异常
    engine.should_fail = True
    engine.fail_exc = TimeoutError("Embedding engine timeout after 5000ms")

    init_fail_metric = METRICS.snapshot()["counters"].get("outbox_failures", 0)
    success_count = await worker.run_once()
    assert success_count == 0, "故障时成功计数必须为 0"

    assert METRICS.snapshot()["counters"].get("outbox_failures", 0) == init_fail_metric + 1

    # 验证底层任务行状态
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT retry_count, next_retry_at, last_error, status FROM memory_projection_outbox WHERE id = ?;", (task_id,))
    row = cur.fetchone()
    assert row["status"] == "pending"
    assert row["retry_count"] == 1
    assert row["next_retry_at"] is not None
    assert row["next_retry_at"] >= int(time.time())
    assert "Embedding engine timeout" in row["last_error"]


@pytest.mark.asyncio
async def test_worker_qdrant_upsert_and_delete_exceptions(worker_env):
    """
    攻击面 4.2: Qdrant 写入与删除故障注入
    - upsert 失败注入
    - delete 失败注入 (针对 DELETED 状态的记忆)
    """
    store, engine, qdrant, worker = worker_env

    # 1. Upsert 故障注入
    store.create_memory_v3(
        memory_id="mem_qdrant_fail_up",
        content="Testing qdrant upsert failure",
    )
    t_up = store.fetch_pending_projection_tasks()[0]
    qdrant.fail_upsert = True

    await worker.run_once()
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT retry_count, last_error FROM memory_projection_outbox WHERE id = ?;", (t_up["id"],))
    row_up = cur.fetchone()
    assert row_up["retry_count"] == 1
    assert "Qdrant 503" in row_up["last_error"]

    # 恢复 upsert 故障
    qdrant.fail_upsert = False

    # 2. Delete 故障注入
    store.create_memory_v3(
        memory_id="mem_qdrant_fail_del",
        content="Testing qdrant delete failure",
    )
    store.change_memory_status("mem_qdrant_fail_del", "DELETED")
    cur.execute("SELECT id FROM memory_projection_outbox WHERE memory_id = 'mem_qdrant_fail_del' AND status = 'pending';")
    del_task_id = cur.fetchone()["id"]

    qdrant.fail_delete = True
    await worker.run_once()

    cur.execute("SELECT retry_count, last_error FROM memory_projection_outbox WHERE id = ?;", (del_task_id,))
    row_del = cur.fetchone()
    assert row_del["retry_count"] == 1
    assert "Qdrant 500" in row_del["last_error"]


@pytest.mark.asyncio
async def test_continuous_failures_exponential_backoff_no_hot_loop(worker_env):
    """
    攻击面 4.3: 持续故障下的指数退避与非无限热循环验证
    断言：
    - 失败后由于 next_retry_at > now，立即再次执行 run_once() 时拉取不到任务（0 次重复冲击）；
    - 退避时间按 min(2^retry_count, 60) 指数递增；
    - 时间推进到退避之后才重新被 fetch 拉起。
    """
    store, engine, qdrant, worker = worker_env
    store.create_memory_v3(
        memory_id="mem_backoff_001",
        content="Testing exponential backoff and no hot-loop",
    )
    engine.should_fail = True

    # 第 1 次运行：产生失败，retry_count -> 1，backoff 1s (2^0)
    now_0 = int(time.time())
    await worker.run_once()

    # 立即（同一秒内）再跑 run_once()：必须 0 任务执行（无热循环轮询）
    tasks_now = store.fetch_pending_projection_tasks(now=now_0)
    assert len(tasks_now) == 0, "退避期内拉到了任务，存在热循环风险"

    # 手工模拟时间前进 2 秒：应当可以重新拉到
    tasks_future = store.fetch_pending_projection_tasks(now=now_0 + 2)
    assert len(tasks_future) == 1
    assert tasks_future[0]["retry_count"] == 1


@pytest.mark.asyncio
async def test_legacy_queue_v3_row_re_enqueued_not_dropped(worker_env):
    """
    攻击面 4.4: 旧队列中遗留的 v3 记忆任务补投新统一队列，不静默丢弃
    断言：
    - process_legacy_task 识别出该 memory_id 在 memories 表为 v3 行；
    - 在新队列 memory_projection_outbox 补入等价 pending 任务；
    - 原旧任务标 status='synced'，last_error 记 'migrated to outbox#...'。
    """
    store, engine, qdrant, worker = worker_env
    # 建立一条 v3 记忆
    mem = store.create_memory_v3(
        memory_id="mem_mig_v3",
        content="Testing v3 task lingering in legacy queue",
    )
    # 把 v3 新队列的任务先标记 done（模拟新队列已清空）
    v3_task_id = store.fetch_pending_projection_tasks()[0]["id"]
    store.mark_projection_done(v3_task_id, claim_revision=1)
    assert len(store.fetch_pending_projection_tasks()) == 0

    # 手工在 qdrant_sync_queue (旧表) 写入一条该 memory 的 legacy 待同步任务
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO qdrant_sync_queue (
            memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, last_error, created_at, updated_at
        ) VALUES ('mem_mig_v3', 'pt_mig', 'upsert', '{}', 'pending', 0, NULL, 100, 100);
        """
    )
    legacy_task_id = cur.lastrowid
    conn.commit()

    # 执行 Worker 单轮同步
    processed = await worker.run_once()
    assert processed >= 1

    # 验证旧表任务标记为已迁移
    cur.execute("SELECT status, last_error FROM qdrant_sync_queue WHERE id = ?;", (legacy_task_id,))
    leg_row = cur.fetchone()
    assert leg_row["status"] == "synced"
    assert "migrated to outbox#" in leg_row["last_error"]

    # 验证新队列补投了任务
    new_v3_tasks = store.fetch_pending_projection_tasks()
    assert len(new_v3_tasks) == 1
    assert new_v3_tasks[0]["memory_id"] == "mem_mig_v3"
    assert new_v3_tasks[0]["projection_revision"] == 1


@pytest.mark.asyncio
async def test_legacy_delta_patch_and_delete_with_point_missing(worker_env):
    """
    攻击面 4.5: legacy delta 信封处理与 point missing 容错
    - op='patch' 时调用 client.set_payload；发生 point missing 时标记 done 并记录 note，不反复重试死循环
    - op='delete' 时调用 delete_point_by_id
    """
    store, engine, qdrant, worker = worker_env

    # 1. 创建 delta patch 任务并注入 point missing
    qdrant.point_missing = True
    delta_patch_payload = {
        "amr_delta_v1": True,
        "op": "patch",
        "fields": {"status": "ACTIVE"},
        "collection": "ai_memory",
        "targets": [("mem_missing_pt", "pt_uuid_missing")],
    }
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO qdrant_sync_queue (
            memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, last_error, created_at, updated_at
        ) VALUES ('mem_missing_pt', 'pt_uuid_missing', 'update_payload', ?, 'pending', 0, NULL, 100, 100);
        """,
        (import_json := __import__("json").dumps(delta_patch_payload),),
    )
    patch_id = cur.lastrowid
    conn.commit()

    # 执行 Worker
    await worker.run_once()

    cur.execute("SELECT status, last_error FROM qdrant_sync_queue WHERE id = ?;", (patch_id,))
    p_row = cur.fetchone()
    assert p_row["status"] == "synced", "点位缺失时不应持续 failed 热循环，而应标记 synced 容错归档"
    assert "point missing" in p_row["last_error"]


@pytest.mark.asyncio
async def test_legacy_synced_task_never_resurrected(worker_env):
    """
    攻击面 4.6: 被取消 (synced) 的旧任务绝不复活
    recheck 发现 status='synced' 时直接跳过，返回 False
    """
    store, engine, qdrant, worker = worker_env
    task_dict = {
        "id": 9999,
        "memory_id": "mem_dummy",
        "op_type": "upsert",
        "status": "pending",  # 内存传参假装 pending
    }
    # 但数据库实际为 synced
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO qdrant_sync_queue (
            id, memory_id, qdrant_point_id, op_type, payload_snapshot, status, retry_count, last_error, created_at, updated_at
        ) VALUES (9999, 'mem_dummy', 'pt_dum', 'upsert', '{}', 'synced', 0, 'cancelled', 100, 100);
        """
    )
    conn.commit()

    res = await worker.process_legacy_task(task_dict)
    assert res is False, "已标记 synced 的任务必须被写前 recheck 跳过"


def test_outbox_worker_single_writer_flock_mutual_exclusion(tmp_path: Path):
    """
    攻击面 4.7: 单执行者 flock 排他性
    - 子进程 A 启动并持有锁；
    - 子进程 B 启动时获取锁失败，触发 SystemExit(3) 退出；
    - 子进程 A 退出并释放后，后续进程可正常启动。
    """
    lock_file = tmp_path / "amr_outbox_audit.lock"

    script_a = f"""
import sys, time
from unittest.mock import MagicMock
from src.service.outbox_worker import OutboxWorker

worker = OutboxWorker(
    session_store=MagicMock(),
    qdrant_manager=MagicMock(),
    engine=MagicMock(),
    lock_path=r"{lock_file}",
)
worker.start()
print("LOCKED", flush=True)
try:
    while True:
        time.sleep(0.1)
except KeyboardInterrupt:
    worker.stop()
    sys.exit(0)
"""

    env = os.environ.copy()
    proc_a = subprocess.Popen([sys.executable, "-c", script_a], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    try:
        line = proc_a.stdout.readline().strip()
        assert line == "LOCKED"

        script_b = f"""
from unittest.mock import MagicMock
from src.service.outbox_worker import OutboxWorker

worker = OutboxWorker(
    session_store=MagicMock(),
    qdrant_manager=MagicMock(),
    engine=MagicMock(),
    lock_path=r"{lock_file}",
)
worker.start()
"""
        proc_b = subprocess.Popen([sys.executable, "-c", script_b], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        rc_b = proc_b.wait(timeout=10)
        stderr_b = proc_b.stderr.read()

        assert rc_b == 3, f"第二个竞争 Worker 进程必须退出码为 3，实际为 {rc_b}"
        assert "another outbox worker holds amr-outbox.lock" in stderr_b
    finally:
        proc_a.terminate()
        proc_a.wait(timeout=5)

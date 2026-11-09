"""
Tests for Phase A: v3 Unified Projection Ordering, CAS, and Dead-Letter Semantics
验证：
1. v3 任务按 revision 投影 11 字段；
2. DELETED 状态物理删点；
3. 行缺失时标 done 跳过，不假装删点；
4. CAS：处理期间合并刷新导致 0 行更新，任务保持 pending，下轮以最新态重投；
5. 失败 CAS 不污染新 revision（并发合并刷新后 retry_count 不增加）；
6. 重试退避与 8 次失败转死信；
7. 死信不因 stale revision 产生；
8. outbox_lag_seconds 在持续失败下可见增长；
9. Qdrant 写入成功但 mark done 前崩溃，重试具备幂等性。
"""

import asyncio
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.core.session_store import SessionStore
from src.service.memory_status import normalize_status
from src.service.metrics import METRICS
from src.service.outbox_worker import OutboxWorker


class FakeEngine:
    def __init__(self, dim: int = 1024):
        self.dim = dim
        self.embed_calls: List[List[str]] = []
        self.hook = None

    async def embed(self, texts: List[str]) -> List[List[float]]:
        self.embed_calls.append(texts)
        if self.hook:
            await self.hook(texts)
        return [[0.05] * self.dim for _ in texts]


class FakeQdrantManager:
    def __init__(self):
        self.upserted_points: List[Dict[str, Any]] = []
        self.deleted_points: List[Dict[str, Any]] = []
        self.set_payload_calls: List[Dict[str, Any]] = []
        self.client = MagicMock()
        self.fail_upsert = False
        self.fail_delete = False

    def upsert_points(self, collection_name: str, points: List[Any], wait: bool = True):
        if self.fail_upsert:
            raise RuntimeError("Qdrant connection refused during upsert")
        for p in points:
            self.upserted_points.append({
                "collection": collection_name,
                "id": p.id,
                "vector": p.vector,
                "payload": p.payload,
            })

    def delete_point_by_id(self, point_id: str, collection_name: str = "ai_memory", wait: bool = True):
        if self.fail_delete:
            raise RuntimeError("Qdrant connection refused during delete")
        self.deleted_points.append({
            "collection": collection_name,
            "point_id": point_id,
        })


@pytest.fixture
def test_setup(tmp_path: Path):
    db_path = tmp_path / "test_projection.db"
    store = SessionStore(db_path=db_path)
    engine = FakeEngine()
    qdrant = FakeQdrantManager()
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


def insert_test_memory(
    store: SessionStore,
    memory_id: str,
    content: str = "Test memory content",
    status: str = "ACTIVE",
    projection_class: str = "v3",
    projection_revision: int = 1,
    collection_name: str = "ai_memory",
    superseded_by: Optional[str] = None,
    scope: str = "team_workspace",
) -> str:
    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        point_id = f"point_{memory_id}"
        cur.execute(
            """
            INSERT INTO memories (
                memory_id, qdrant_point_id, type, subject, predicate, object,
                content, status, projection_class, projection_revision,
                collection_name, superseded_by, scope, version,
                created_at, updated_at
            ) VALUES (?, ?, 'general', 'user', 'likes', 'coffee',
                      ?, ?, ?, ?,
                      ?, ?, ?, 1,
                      ?, ?);
            """,
            (
                memory_id, point_id, content, status, projection_class, projection_revision,
                collection_name, superseded_by, scope, now, now
            ),
        )
    return point_id


@pytest.mark.asyncio
async def test_v3_projection_11_fields(test_setup):
    """验证 v3 任务按 revision 投影 11 字段且 status 大写规范化"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    qdrant = test_setup["qdrant"]

    # 先插入被引用的目标行以满足外键约束
    insert_test_memory(store, memory_id="mem_v3_002", content="Target", projection_revision=1)

    mid = "mem_v3_001"
    point_id = insert_test_memory(
        store,
        memory_id=mid,
        content="AMR Single-Writer Architecture",
        status="active",  # 传入小写，期待投影时为 ACTIVE
        projection_class="v3",
        projection_revision=1,
        superseded_by="mem_v3_002",
        scope="global",
    )

    with store.locked_write_transaction() as conn:
        task_id = store.enqueue_projection_task(
            conn.cursor(),
            memory_id=mid,
            projection_revision=1,
            target_collection="ai_memory",
        )

    processed = await worker.run_once()
    assert processed == 1

    # 验证 Qdrant upsert 收到 11 字段完整 payload
    assert len(qdrant.upserted_points) == 1
    pt = qdrant.upserted_points[0]
    assert pt["id"] == point_id
    assert pt["collection"] == "ai_memory"

    payload = pt["payload"]
    expected_keys = {
        "memory_id", "version", "content", "project_id", "type", "status",
        "created_by_agent", "updated_by_agent", "updated_at", "superseded_by", "scope"
    }
    assert set(payload.keys()) == expected_keys
    assert payload["memory_id"] == mid
    assert payload["content"] == "AMR Single-Writer Architecture"
    assert payload["status"] == "ACTIVE"  # 规范化大写
    assert payload["superseded_by"] == "mem_v3_002"
    assert payload["scope"] == "global"

    # 验证 outbox 表状态变为 done
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT status, projected_revision FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        row = cur.fetchone()
        assert row["status"] == "done"
        assert row["projected_revision"] == 1


@pytest.mark.asyncio
async def test_v3_deleted_removes_point(test_setup):
    """验证 DELETED 状态物理删点"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    qdrant = test_setup["qdrant"]

    mid = "mem_v3_deleted_001"
    point_id = insert_test_memory(
        store,
        memory_id=mid,
        status="DELETED",
        projection_class="v3",
        projection_revision=2,
        collection_name="custom_collection",
    )

    with store.locked_write_transaction() as conn:
        task_id = store.enqueue_projection_task(
            conn.cursor(),
            memory_id=mid,
            projection_revision=2,
            target_collection="custom_collection",
        )

    processed = await worker.run_once()
    assert processed == 1

    # 验证没有 upsert，只有 delete
    assert len(qdrant.upserted_points) == 0
    assert len(qdrant.deleted_points) == 1
    del_pt = qdrant.deleted_points[0]
    assert del_pt["point_id"] == point_id
    assert del_pt["collection"] == "custom_collection"

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        assert cur.fetchone()["status"] == "done"


@pytest.mark.asyncio
async def test_v3_missing_memory_row_skips(test_setup):
    """验证行缺失时 CAS 标 done 跳过，不假装删点"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    qdrant = test_setup["qdrant"]

    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status, retry_count, created_at, updated_at
            ) VALUES ('ai_memory', 'non_existent_mid', 1, 'pending', 0, ?, ?);
            """,
            (now, now),
        )
        task_id = cur.lastrowid

    processed = await worker.run_once()
    assert processed == 1

    # 没有调用 Qdrant
    assert len(qdrant.upserted_points) == 0
    assert len(qdrant.deleted_points) == 0

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        assert cur.fetchone()["status"] == "done"


@pytest.mark.asyncio
async def test_cas_merge_during_processing_keeps_pending(test_setup):
    """验证 CAS：处理期间合并刷新导致 0 行更新，任务保持 pending，下轮以最新态重投"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    engine = test_setup["engine"]

    mid = "mem_cas_merge_001"
    insert_test_memory(store, memory_id=mid, content="Content V1", projection_revision=1)

    with store.locked_write_transaction() as conn:
        task_id = store.enqueue_projection_task(
            conn.cursor(),
            memory_id=mid,
            projection_revision=1,
        )

    # 注入钩子：在 embedding 过程中模拟并发写入，将 memories 的 projection_revision 增加到 2
    async def concurrent_write_hook(texts):
        with store.locked_write_transaction() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE memories SET projection_revision = 2, content = 'Content V2' WHERE memory_id = ?;", (mid,))
            # 并发写入更新 outbox 任务
            store.enqueue_projection_task(cur, memory_id=mid, projection_revision=2)

    engine.hook = concurrent_write_hook

    # 第一轮执行：由于并发写入修改了 revision，当前正在处理的 rev=1 CAS done 将更新 0 行
    processed = await worker.run_once()
    assert processed == 0  # 本轮 CAS done 失败，未完成

    # 验证任务依然处于 pending 状态，且 projection_revision 保持最新 (2)
    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status, projection_revision FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        row = cur.fetchone()
        assert row["status"] == "pending"
        assert row["projection_revision"] == 2

    # 移除钩子，第二轮执行应当成功以最新态投影
    engine.hook = None
    processed_round2 = await worker.run_once()
    assert processed_round2 == 1

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status, projected_revision FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        row = cur.fetchone()
        assert row["status"] == "done"
        assert row["projected_revision"] == 2


@pytest.mark.asyncio
async def test_failure_cas_spares_merged_revision(test_setup):
    """验证失败分支 CAS：新 revision 已被合并刷新时（0 行更新），绝不给它增加 retry_count 记账"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    engine = test_setup["engine"]
    qdrant = test_setup["qdrant"]

    mid = "mem_failure_merge_001"
    insert_test_memory(store, memory_id=mid, content="Content Fail", projection_revision=1)

    with store.locked_write_transaction() as conn:
        task_id = store.enqueue_projection_task(
            conn.cursor(),
            memory_id=mid,
            projection_revision=1,
        )

    # 在执行过程中并发更新 revision 为 2 并重置 retry_count
    async def concurrent_bump(texts):
        with store.locked_write_transaction() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE memories SET projection_revision = 2 WHERE memory_id = ?;", (mid,))
            store.enqueue_projection_task(cur, memory_id=mid, projection_revision=2)

    engine.hook = concurrent_bump
    # 模拟 Qdrant 失败
    qdrant.fail_upsert = True

    processed = await worker.run_once()
    assert processed == 0

    # 验证任务未被记上失败（retry_count 依然是 0，next_retry_at 为 NULL）
    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status, retry_count, next_retry_at, projection_revision FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        row = cur.fetchone()
        assert row["status"] == "pending"
        assert row["projection_revision"] == 2
        assert row["retry_count"] == 0
        assert row["next_retry_at"] is None


@pytest.mark.asyncio
async def test_retry_backoff_and_dead_letter(test_setup):
    """验证失败退避与 8 次失败后进入死信"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    qdrant = test_setup["qdrant"]
    qdrant.fail_upsert = True

    mid = "mem_dead_letter_001"
    insert_test_memory(store, memory_id=mid, content="Content Error", projection_revision=1)

    with store.locked_write_transaction() as conn:
        task_id = store.enqueue_projection_task(
            conn.cursor(),
            memory_id=mid,
            projection_revision=1,
        )

    # 模拟前 7 次失败重试
    for i in range(7):
        # 清除 next_retry_at 以允许立即重试
        with store.locked_write_transaction() as conn:
            conn.cursor().execute("UPDATE memory_projection_outbox SET next_retry_at = NULL WHERE id = ?;", (task_id,))

        processed = await worker.run_once()
        assert processed == 0

        with store._lock:
            cur = store._get_connection().cursor()
            cur.execute("SELECT status, retry_count, next_retry_at FROM memory_projection_outbox WHERE id = ?;", (task_id,))
            row = cur.fetchone()
            assert row["status"] == "pending"
            assert row["retry_count"] == i + 1
            assert row["next_retry_at"] is not None

    # 第 8 次失败，应该转为 dead_letter
    with store.locked_write_transaction() as conn:
        conn.cursor().execute("UPDATE memory_projection_outbox SET next_retry_at = NULL WHERE id = ?;", (task_id,))

    processed = await worker.run_once()
    assert processed == 0

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status, retry_count, dead_letter_at FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        row = cur.fetchone()
        assert row["status"] == "dead_letter"
        assert row["retry_count"] == 8
        assert row["dead_letter_at"] is not None


@pytest.mark.asyncio
async def test_dead_letter_never_for_stale_revision(test_setup):
    """验证死信不因 stale revision 产生：第 7 次失败后合并刷新，第 8 次失败不进死信"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    engine = test_setup["engine"]
    qdrant = test_setup["qdrant"]
    qdrant.fail_upsert = True

    mid = "mem_dl_stale_001"
    insert_test_memory(store, memory_id=mid, content="Stale DL", projection_revision=1)

    with store.locked_write_transaction() as conn:
        task_id = store.enqueue_projection_task(
            conn.cursor(),
            memory_id=mid,
            projection_revision=1,
        )
        # 手工置 retry_count = 7
        conn.cursor().execute("UPDATE memory_projection_outbox SET retry_count = 7 WHERE id = ?;", (task_id,))

    # 在本次处理时并发刷新为 rev=2
    async def bump_hook(texts):
        with store.locked_write_transaction() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE memories SET projection_revision = 2 WHERE memory_id = ?;", (mid,))
            store.enqueue_projection_task(cur, memory_id=mid, projection_revision=2)

    engine.hook = bump_hook

    processed = await worker.run_once()
    assert processed == 0

    # 验证没有变成 dead_letter，保持 pending 且 retry_count 为 0
    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status, retry_count, projection_revision FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        row = cur.fetchone()
        assert row["status"] == "pending"
        assert row["projection_revision"] == 2
        assert row["retry_count"] == 0


@pytest.mark.asyncio
async def test_outbox_lag_seconds_visible_under_sustained_failure(test_setup):
    """验证 outbox_lag_seconds 在持续失败下可见增长"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    qdrant = test_setup["qdrant"]
    qdrant.fail_upsert = True

    mid = "mem_lag_001"
    insert_test_memory(store, memory_id=mid, content="Lag test", projection_revision=1)

    now = int(time.time())
    old_time = now - 150  # 150 秒前创建的任务
    with store.locked_write_transaction() as conn:
        conn.cursor().execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status, retry_count, created_at, updated_at
            ) VALUES ('ai_memory', ?, 1, 'pending', 0, ?, ?);
            """,
            (mid, old_time, old_time),
        )

    await worker.run_once()
    metrics_snap = METRICS.snapshot()
    lag_seconds = metrics_snap.get("gauges", {}).get("outbox_lag_seconds", 0)
    assert lag_seconds >= 140


@pytest.mark.asyncio
async def test_qdrant_success_crash_before_mark_done_is_idempotent(test_setup):
    """验证 Qdrant 写入成功但 mark done 前崩溃，重跑幂等"""
    store = test_setup["store"]
    worker = test_setup["worker"]
    qdrant = test_setup["qdrant"]

    mid = "mem_crash_idempotent_001"
    point_id = insert_test_memory(store, memory_id=mid, content="Crash idempotent", projection_revision=1)

    with store.locked_write_transaction() as conn:
        task_id = store.enqueue_projection_task(conn.cursor(), memory_id=mid, projection_revision=1)

    # 模拟在 mark_projection_done 前抛出致命崩溃
    original_mark_done = store.mark_projection_done
    call_count = 0

    def crash_mark_done(task_id_arg, claim_rev_arg, proj_rev_arg=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OSError("Simulated process crash / power loss before mark done")
        return original_mark_done(task_id_arg, claim_rev_arg, proj_rev_arg)

    store.mark_projection_done = crash_mark_done

    # 首次执行崩溃
    await worker.run_once()
    assert len(qdrant.upserted_points) == 1

    # 任务仍然为 pending（因为崩溃未提交 done）
    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        # 因为抛出异常，进入 except 分支调用了 mark_projection_failed，status 仍为 pending
        assert cur.fetchone()["status"] == "pending"

    # 清空下一次重试时间模拟重启恢复
    with store.locked_write_transaction() as conn:
        conn.cursor().execute("UPDATE memory_projection_outbox SET next_retry_at = NULL WHERE id = ?;", (task_id,))

    # 第二次执行：恢复正常
    processed = await worker.run_once()
    assert processed == 1

    # Qdrant 进行了相同 point_id 的覆盖 upsert，保持最终一致
    assert len(qdrant.upserted_points) == 2
    assert qdrant.upserted_points[0]["id"] == point_id
    assert qdrant.upserted_points[1]["id"] == point_id

    with store._lock:
        cur = store._get_connection().cursor()
        cur.execute("SELECT status FROM memory_projection_outbox WHERE id = ?;", (task_id,))
        assert cur.fetchone()["status"] == "done"

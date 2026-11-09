"""测试管理 UDS 接口与指标契约（status.metrics / outbox.list_dead_letters / outbox.replay）。

重点断言：
1. status.metrics 暴露进程内轻量指标快照（counters, gauges, summary）；
2. outbox.list_dead_letters 准确筛选 dead_letter 状态任务并支持 limit；
3. outbox.replay 语义（RFC v5 D10）：
   - 以 memories 当前 projection_revision 经统一入队 helper 重新入队；
   - 原 dead_letter 行标记 status='done'，last_error='absorbed by pending#<id>'；
   - 严禁直改 pending 撞唯一索引；
   - 记忆行缺失时标记 status='done'，last_error='memory row missing; skip'；
4. 参数错误与不存在异常分别映射为 RpcInvalidArgument 与 RpcNotFound。
"""
import time
import pytest
from unittest.mock import AsyncMock, MagicMock

from config.settings import AppConfig
from src.core.session_store import SessionStore
from src.interfaces.ipc.errors import RpcInvalidArgument, RpcNotFound
from src.main import AMRApplication
from src.service.metrics import METRICS


@pytest.fixture
def app_with_store(tmp_path):
    db_file = str(tmp_path / "test_admin.db")
    store = SessionStore(db_path=db_file)

    cfg = AppConfig()
    cfg.storage.sqlite_path = db_file

    fake_engine = MagicMock()
    fake_engine.state = MagicMock(value="ready")
    fake_engine.get_model_status = AsyncMock(return_value={"state": "ready"})

    fake_qdrant = MagicMock()
    fake_qdrant.is_healthy.return_value = True

    app = AMRApplication(
        engine=fake_engine,
        qdrant_manager=fake_qdrant,
        session_store=store,
    )
    app.config = cfg

    yield app, store

    store.close()


@pytest.mark.asyncio
async def test_admin_metrics_snapshot(app_with_store):
    """验证 status.metrics 返回当前 METRICS 指标快照。"""
    app, _ = app_with_store

    METRICS.incr("test_counter", 5)
    METRICS.set_gauge("test_gauge", 42.5)
    METRICS.observe("test_latency_ms", 12.3)
    METRICS.observe("test_latency_ms", 45.6)

    snapshot = await app.dispatch_admin_rpc("status.metrics", {})
    assert "counters" in snapshot
    assert snapshot["counters"]["test_counter"] >= 5
    assert snapshot["gauges"]["test_gauge"] == 42.5
    assert "summary" in snapshot
    assert "test_latency_ms" in snapshot["summary"]
    assert snapshot["summary"]["test_latency_ms"]["count"] >= 2
    assert "ts" in snapshot


@pytest.mark.asyncio
async def test_outbox_list_dead_letters(app_with_store):
    """验证 outbox.list_dead_letters 查询死信任务行并支持 limit。"""
    app, store = app_with_store

    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        # 插入测试记忆
        cur.execute(
            """
            INSERT INTO memories (
                memory_id, content, type, status, created_by_agent,
                projection_revision, collection_name, projection_class, qdrant_point_id, created_at, updated_at
            ) VALUES ('mem_dl_1', 'Dead letter content 1', 'fact', 'ACTIVE', 'agent_a', 2, 'ai_memory', 'v3', 'pid_dl_1', ?, ?);
            """,
            (now, now),
        )
        cur.execute(
            """
            INSERT INTO memories (
                memory_id, content, type, status, created_by_agent,
                projection_revision, collection_name, projection_class, qdrant_point_id, created_at, updated_at
            ) VALUES ('mem_dl_2', 'Dead letter content 2', 'fact', 'ACTIVE', 'agent_a', 1, 'ai_memory', 'v3', 'pid_dl_2', ?, ?);
            """,
            (now, now),
        )

        # 插入 2 条 dead_letter，1 条 pending，1 条 done
        cur.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status,
                retry_count, last_error, dead_letter_at, created_at, updated_at
            ) VALUES ('ai_memory', 'mem_dl_1', 1, 'dead_letter', 8, 'Qdrant connection refused', ?, ?, ?);
            """,
            (now - 100, now - 200, now - 100),
        )
        cur.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status,
                retry_count, last_error, dead_letter_at, created_at, updated_at
            ) VALUES ('ai_memory', 'mem_dl_2', 1, 'dead_letter', 8, 'Embed timeout', ?, ?, ?);
            """,
            (now - 50, now - 200, now - 50),
        )
        cur.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status,
                retry_count, last_error, dead_letter_at, created_at, updated_at
            ) VALUES ('ai_memory', 'mem_dl_3', 1, 'pending', 0, NULL, NULL, ?, ?);
            """,
            (now, now),
        )

    # 1. 正常查询死信任务
    res = await app.dispatch_admin_rpc("outbox.list_dead_letters", {"limit": 10})
    assert res["total"] == 2
    dl_list = res["dead_letters"]
    assert len(dl_list) == 2
    mids = {d["memory_id"] for d in dl_list}
    assert mids == {"mem_dl_1", "mem_dl_2"}
    for item in dl_list:
        assert item["retry_count"] == 8
        assert item["dead_letter_at"] is not None
        assert "last_error" in item
        assert item["collections"] == "ai_memory"

    # 2. limit 限制
    res_limited = await app.dispatch_admin_rpc("outbox.list_dead_letters", {"limit": 1})
    assert len(res_limited["dead_letters"]) == 1

    # 3. 参数校验异常
    with pytest.raises(RpcInvalidArgument):
        await app.dispatch_admin_rpc("outbox.list_dead_letters", {"limit": -1})

    with pytest.raises(RpcInvalidArgument):
        await app.dispatch_admin_rpc("outbox.list_dead_letters", {"limit": "not_an_int"})


@pytest.mark.asyncio
async def test_outbox_replay_semantics(app_with_store):
    """验证 outbox.replay 采用统一 helper 重新入队，并将原死信任务标为 absorbed。"""
    app, store = app_with_store

    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        # 记忆当前 revision 为 3
        cur.execute(
            """
            INSERT INTO memories (
                memory_id, content, type, status, created_by_agent,
                projection_revision, collection_name, projection_class, qdrant_point_id, created_at, updated_at
            ) VALUES ('mem_replay_1', 'Replay test card', 'fact', 'ACTIVE', 'agent_a', 3, 'ai_memory', 'v3', 'pid_replay_1', ?, ?);
            """,
            (now, now),
        )
        # 死信任务当时的 revision 为 1
        cur.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status,
                retry_count, last_error, dead_letter_at, created_at, updated_at
            ) VALUES ('ai_memory', 'mem_replay_1', 1, 'dead_letter', 8, 'Fatal timeout', ?, ?, ?);
            """,
            (now - 100, now - 200, now - 100),
        )
        dead_id = cur.lastrowid

    # 重放指定任务
    rep_res = await app.dispatch_admin_rpc("outbox.replay", {"task_ids": [dead_id]})
    assert rep_res["status"] == "ok"
    assert rep_res["replayed_count"] == 1
    assert dead_id in rep_res["task_ids"]

    # 验证底层数据库状态
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        # 1. 原死信任务状态变为 done，且 last_error 注记 absorbed by pending#...
        cur.execute("SELECT status, last_error FROM memory_projection_outbox WHERE id = ?;", (dead_id,))
        old_task = cur.fetchone()
        assert old_task["status"] == "done"
        assert old_task["last_error"].startswith("absorbed by pending#")
        new_pending_id = int(old_task["last_error"].split("#")[1])

        # 2. 新入队的 pending 任务使用 memories 当前最新 projection_revision (3)
        cur.execute("SELECT id, status, projection_revision, retry_count FROM memory_projection_outbox WHERE id = ?;", (new_pending_id,))
        new_task = cur.fetchone()
        assert new_task["status"] == "pending"
        assert new_task["projection_revision"] == 3
        assert new_task["retry_count"] == 0


@pytest.mark.asyncio
async def test_outbox_replay_missing_memory_row(app_with_store):
    """当死信对应的记忆行已从主表中删除时，重放标记 done 且注记 'memory row missing; skip'。"""
    app, store = app_with_store

    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status,
                retry_count, last_error, dead_letter_at, created_at, updated_at
            ) VALUES ('ai_memory', 'mem_ghost_nonexistent', 1, 'dead_letter', 8, 'Error', ?, ?, ?);
            """,
            (now, now, now),
        )
        dead_id = cur.lastrowid

    rep_res = await app.dispatch_admin_rpc("outbox.replay", {"task_ids": [dead_id]})
    assert rep_res["replayed_count"] == 1

    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute("SELECT status, last_error FROM memory_projection_outbox WHERE id = ?;", (dead_id,))
        row = cur.fetchone()
        assert row["status"] == "done"
        assert "memory row missing; skip" in row["last_error"]


@pytest.mark.asyncio
async def test_outbox_replay_error_cases(app_with_store):
    """验证重放参数非法、任务不存在、任务非 dead_letter 时的异常抛出。"""
    app, store = app_with_store

    now = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO memory_projection_outbox (
                target_collection, memory_id, projection_revision, status,
                retry_count, last_error, dead_letter_at, created_at, updated_at
            ) VALUES ('ai_memory', 'mem_dummy', 1, 'pending', 0, NULL, NULL, ?, ?);
            """,
            (now, now),
        )
        pending_id = cur.lastrowid

    # 1. 非法的 task_ids 格式
    with pytest.raises(RpcInvalidArgument):
        await app.dispatch_admin_rpc("outbox.replay", {"task_ids": "invalid"})

    # 2. 任务不存在 -> RpcNotFound
    with pytest.raises(RpcNotFound):
        await app.dispatch_admin_rpc("outbox.replay", {"task_ids": [999999]})

    # 3. 任务并非 dead_letter 状态 -> RpcInvalidArgument
    with pytest.raises(RpcInvalidArgument, match="not in dead_letter status"):
        await app.dispatch_admin_rpc("outbox.replay", {"task_ids": [pending_id]})

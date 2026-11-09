"""测试统一投影队列 (v3 专用): 入队合并、退避重置、跨 collection 独立、CAS 状态机、死信、回退复查。"""
import time
import pytest

from src.core.errors import LegacyPathForbidden
from src.core.session_store import SessionStore


@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_outbox.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


def test_enqueue_merges_pending_and_resets_backoff(store):
    """验证同 (target_collection, memory_id) 的 pending 任务被合并，并重置 retry_count 和 next_retry_at"""
    mem = store.create_memory_v3(
        memory_id="mem_queue_1",
        content="Testing projection queue merge",
        created_by_agent="test_agent",
    )
    assert mem["projection_status"] == "pending"

    # 先拉出任务
    tasks = store.fetch_pending_projection_tasks(limit=10)
    assert len(tasks) == 1
    t1 = tasks[0]
    assert t1["projection_revision"] == 1
    assert t1["retry_count"] == 0
    assert t1["next_retry_at"] is None

    # 模拟一次处理失败，增加 retry_count 和 next_retry_at
    future_time = int(time.time()) + 300
    failed_rows = store.mark_projection_failed(t1["id"], claim_revision=1, error="Qdrant 503", next_retry_at=future_time)
    assert failed_rows == 1

    # 此时 fetch 拉不到（因为 next_retry_at > now）
    assert len(store.fetch_pending_projection_tasks(limit=10)) == 0

    # 发生新写入：update_memory_v3 会重新 enqueue
    with store.locked_write_transaction() as conn:
        rev2 = store.bump_projection_revision(conn.cursor(), "mem_queue_1")
        assert rev2 == 2
        outbox_id = store.enqueue_projection_task(conn.cursor(), memory_id="mem_queue_1", projection_revision=rev2)
        assert outbox_id == t1["id"]  # 同一行合并

    # 验证该行已被重置：retry_count=0, next_retry_at=NULL, projection_revision=2
    tasks_after = store.fetch_pending_projection_tasks(limit=10)
    assert len(tasks_after) == 1
    t2 = tasks_after[0]
    assert t2["id"] == t1["id"]
    assert t2["projection_revision"] == 2
    assert t2["retry_count"] == 0
    assert t2["next_retry_at"] is None


def test_different_collection_independent(store):
    """验证同一 memory_id 在不同 target_collection 下的任务互不干扰"""
    mem = store.create_memory_v3(
        memory_id="mem_multi_col",
        content="Multi-collection test content",
        collection_name="col_a",
    )
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        id_b = store.enqueue_projection_task(cur, memory_id="mem_multi_col", projection_revision=1, target_collection="col_b")

    tasks = store.fetch_pending_projection_tasks(limit=10)
    assert len(tasks) == 2
    collections = {t["target_collection"] for t in tasks}
    assert collections == {"col_a", "col_b"}


def test_enqueue_legacy_forbidden(store):
    """验证对 legacy 行调用 enqueue_projection_task 必须抛出 LegacyPathForbidden"""
    store.create_memory(
        memory_id="mem_leg_1",
        subject="sub",
        predicate="pred",
        content="legacy content",
        type="fact",
    )
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        with pytest.raises(LegacyPathForbidden) as exc_info:
            store.enqueue_projection_task(cur, memory_id="mem_leg_1", projection_revision=1)
        assert exc_info.value.memory_id == "mem_leg_1"
        assert exc_info.value.projection_class == "legacy"


def test_cas_mark_done_and_merged_spares(store):
    """验证 mark_projection_done CAS：处理期间被合并刷新时，旧 claim_revision 更新 0 行保留 pending"""
    mem = store.create_memory_v3(
        memory_id="mem_cas_done",
        content="Testing CAS mark done",
    )
    tasks = store.fetch_pending_projection_tasks()
    t = tasks[0]
    assert t["projection_revision"] == 1

    # 模拟 Worker 在 project 期间，发生新写入，revision 升为 2
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        rev2 = store.bump_projection_revision(cur, "mem_cas_done")
        store.enqueue_projection_task(cur, memory_id="mem_cas_done", projection_revision=rev2)

    # Worker 投影完 v1，尝试按 claim=1 标记完成 -> 应该 CAS 失败 (0 行)
    done_success = store.mark_projection_done(t["id"], claim_revision=1, projected_revision=1)
    assert done_success is False

    # 任务仍然保持 pending，等待下轮以 v2 执行
    tasks_rem = store.fetch_pending_projection_tasks()
    assert len(tasks_rem) == 1
    assert tasks_rem[0]["projection_revision"] == 2

    # 下轮以 v2 执行并 CAS 成功
    done_v2 = store.mark_projection_done(t["id"], claim_revision=2, projected_revision=2)
    assert done_v2 is True
    assert len(store.fetch_pending_projection_tasks()) == 0


def test_cas_mark_failed_spares_merged_revision(store):
    """验证 mark_projection_failed CAS：合并后旧 revision 失败不记账"""
    mem = store.create_memory_v3(
        memory_id="mem_cas_fail",
        content="Testing CAS mark failed",
    )
    tasks = store.fetch_pending_projection_tasks()
    t = tasks[0]

    # 模拟 Worker 尝试 v1 期间，合并刷新至 v2
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        rev2 = store.bump_projection_revision(cur, "mem_cas_fail")
        store.enqueue_projection_task(cur, memory_id="mem_cas_fail", projection_revision=rev2)

    # Worker 报告 v1 失败 -> CAS 失败 (0 行更新)，不增加 retry_count
    updated_rows = store.mark_projection_failed(t["id"], claim_revision=1, error="Timeout", next_retry_at=int(time.time()) + 60)
    assert updated_rows == 0

    t_check = store.fetch_pending_projection_tasks()[0]
    assert t_check["retry_count"] == 0
    assert t_check["next_retry_at"] is None


def test_dead_letter_threshold_and_cas(store):
    """验证 mark_projection_dead_letter：retry_count < 8 不置死信，>= 8 且 revision 匹配才置 dead_letter"""
    mem = store.create_memory_v3(
        memory_id="mem_dl_1",
        content="Testing dead letter",
    )
    t = store.fetch_pending_projection_tasks()[0]

    # retry 7 次
    for i in range(7):
        r = store.mark_projection_failed(t["id"], claim_revision=1, error=f"err {i}", next_retry_at=0)
        assert r == 1

    # 尝试置死信 -> 失败 (retry_count=7 < 8)
    assert store.mark_projection_dead_letter(t["id"], claim_revision=1) is False

    # 第 8 次失败
    store.mark_projection_failed(t["id"], claim_revision=1, error="err 8", next_retry_at=0)

    # 置死信成功
    assert store.mark_projection_dead_letter(t["id"], claim_revision=1) is True

    # 此时 pending 队列为空
    assert len(store.fetch_pending_projection_tasks()) == 0

    # backlog 统计可查
    backlog = store.count_projection_backlog()
    assert backlog["pending"] == 0
    assert backlog["dead_letter"] == 1


def test_reenqueue_if_newer(store):
    """验证 reenqueue_projection_if_newer 兜底机制"""
    mem = store.create_memory_v3(
        memory_id="mem_reenqueue",
        content="Testing re-enqueue if newer",
    )
    # 当前 revision 为 1
    assert store.reenqueue_projection_if_newer("mem_reenqueue", claim_revision=1) is None

    # memories 发生递增至 3
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        store.bump_projection_revision(cur, "mem_reenqueue")
        store.bump_projection_revision(cur, "mem_reenqueue")

    # 当 claim 为 1 时，memories 已经是 3，应当重新入队
    outbox_id = store.reenqueue_projection_if_newer("mem_reenqueue", claim_revision=1)
    assert outbox_id is not None

    tasks = store.fetch_pending_projection_tasks()
    assert len(tasks) == 1
    assert tasks[0]["projection_revision"] == 3

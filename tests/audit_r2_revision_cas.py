"""
Red-Team Audit Track R2: Revision Monotonicity, CAS & Dead-Letter Replay Tests
验证 AMR v5 的 CAS 与统一投影队列契约：
1. 所有写路径 revision 单调 +1（create=1、内容、元数据、状态、删除、冲突联动）
2. Worker 投影处理期间并发写入：done CAS 0 行、任务保持 pending、下一轮按最新态重投
3. 失败分支 CAS 隔离：旧 revision 失败不污染已合并的新 revision（retry_count/next_retry_at 不变）
4. 8 次重试后转死信，阈值未到不入死信
5. 死信 replay 语义（走合并入队、原行 absorbed）
6. 部分唯一索引约束：同一 (collection, memory_id) 仅一条 pending；跨 collection 互不干扰
"""

import time
import pytest

from src.core.session_store import SessionStore


@pytest.fixture
def store(tmp_path):
    db_file = tmp_path / "audit_cas.db"
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


def test_revision_monotonicity_across_all_write_paths(store):
    """
    攻击面 2.1: 验证所有写路径 revision 单调 +1
    1. create_memory_v3: revision = 1
    2. update_memory_v3 (正文变更): revision = 2
    3. update_memory_v3 (纯类型/元数据变更): revision = 3
    4. update_memory_v3 (纯状态变更): revision = 4
    5. change_memory_status (删除 DELETED): revision = 5
    6. 被其它记忆声明 conflicts_with: revision = 6
    """
    mem_id = "mem_rev_mono_001"

    # 1. 创建
    mem1 = store.create_memory_v3(
        memory_id=mem_id,
        content="Version 1 content string",
        type="fact",
    )
    assert mem1["projection_revision"] == 1
    t1 = store.fetch_pending_projection_tasks()[0]
    assert t1["projection_revision"] == 1

    # 2. 修改正文 (需带 expected_version)
    mem2 = store.update_memory_v3(
        memory_id=mem_id,
        content="Version 2 updated content string",
        expected_version=1,
        change_reason="Content update",
    )
    assert mem2["projection_revision"] == 2
    t2 = store.fetch_pending_projection_tasks()[0]
    assert t2["projection_revision"] == 2

    # 3. 仅修改类型/元数据 (正文为 None)
    mem3 = store.update_memory_v3(
        memory_id=mem_id,
        type="architecture",
    )
    assert mem3["projection_revision"] == 3
    t3 = store.fetch_pending_projection_tasks()[0]
    assert t3["projection_revision"] == 3

    # 4. 仅修改状态 (如置为 TEMPORARY)
    mem4 = store.update_memory_v3(
        memory_id=mem_id,
        status="TEMPORARY",
    )
    assert mem4["projection_revision"] == 4
    t4 = store.fetch_pending_projection_tasks()[0]
    assert t4["projection_revision"] == 4

    # 5. 删除 (change_memory_status -> DELETED)
    del_res = store.change_memory_status(
        memory_id=mem_id,
        new_status="DELETED",
        reason="Lifecycle retirement",
    )
    assert del_res["projection_revision"] == 5
    t5 = store.fetch_pending_projection_tasks()[0]
    assert t5["projection_revision"] == 5

    # 6. 新建另一条记忆并与其声明冲突，验证目标记忆 revision 亦被联动递增
    target_id = "mem_rev_target_002"
    store.create_memory_v3(
        memory_id=target_id,
        content="Target conflict content",
    )
    target_mem_init = store.get_memory_v3(target_id)
    assert target_mem_init["projection_revision"] == 1

    store.create_memory_v3(
        memory_id="mem_conflictor_003",
        content="Conflicting content pointing to target",
        conflicts_with=[target_id],
    )
    target_mem_after = store.get_memory_v3(target_id)
    assert target_mem_after["projection_revision"] == 2
    assert target_mem_after["status"] == "CONFLICT"


def test_worker_processing_concurrency_cas_miss_keeps_pending(store):
    """
    攻击面 2.2: 模拟 Worker 处理期间新写入到达，CAS mark done 返回 False，任务保持 pending
    下一轮以新 revision 重投，保证最终一致性。
    """
    mem_id = "mem_worker_cas_001"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Initial content rev 1",
    )
    task = store.fetch_pending_projection_tasks()[0]
    task_id = task["id"]
    claim_rev = task["projection_revision"]
    assert claim_rev == 1

    # 模拟 Worker 在生成向量或向 Qdrant 写入的耗时窗口中：
    # 外部并发写入完成并提交了 revision 2
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        new_rev = store.bump_projection_revision(cur, mem_id)
        assert new_rev == 2
        store.enqueue_projection_task(cur, memory_id=mem_id, projection_revision=new_rev)

    # Worker 处理完成 rev 1，尝试提交 CAS 完成
    done_ok = store.mark_projection_done(task_id, claim_revision=claim_rev, projected_revision=claim_rev)
    assert done_ok is False, "CAS mark done 应当返回 False，因为任务行已被合并更新为 rev 2"

    # 验证任务未被置为 done，依然处于 pending 且当前 revision 是 2
    pending_tasks = store.fetch_pending_projection_tasks()
    assert len(pending_tasks) == 1
    assert pending_tasks[0]["id"] == task_id
    assert pending_tasks[0]["status"] == "pending"
    assert pending_tasks[0]["projection_revision"] == 2

    # 下一轮 Worker 读取 rev 2 快照并以 claim=2 提交，CAS 必须成功
    done_v2 = store.mark_projection_done(task_id, claim_revision=2, projected_revision=2)
    assert done_v2 is True
    assert len(store.fetch_pending_projection_tasks()) == 0


def test_failure_cas_isolation_does_not_pollute_new_revision(store):
    """
    攻击面 2.3: 失败分支 CAS 隔离
    Worker 尝试 rev 1 期间失败，但任务已合并刷新至 rev 2：
    mark_projection_failed(claim=1) 必须更新 0 行，绝不能增加新 revision 的 retry_count，
    也不得给新 revision 设置退避时间。
    """
    mem_id = "mem_fail_cas_001"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Rev 1 content",
    )
    task = store.fetch_pending_projection_tasks()[0]
    task_id = task["id"]

    # 并发合并刷新至 rev 2
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        rev2 = store.bump_projection_revision(cur, mem_id)
        store.enqueue_projection_task(cur, memory_id=mem_id, projection_revision=rev2)

    # Worker 报告 rev 1 失败
    future_ts = int(time.time()) + 120
    failed_rows = store.mark_projection_failed(
        task_id, claim_revision=1, error="Qdrant connection timeout", next_retry_at=future_ts
    )
    assert failed_rows == 0, "更新行数必须为 0，防止把旧任务的失败记在新 revision 账上"

    # 查验新 revision 任务状态：完全未受污染
    t_after = store.fetch_pending_projection_tasks()[0]
    assert t_after["projection_revision"] == 2
    assert t_after["retry_count"] == 0, "新 revision retry_count 被错误递增"
    assert t_after["next_retry_at"] is None, "新 revision 被错误施加了重试退避"


def test_dead_letter_threshold_and_cas_guard(store):
    """
    攻击面 2.4: 死信流转阈值与 CAS 守卫
    - retry_count < 8 时拒绝进入死信
    - retry_count = 8 且 revision 匹配时转移进入 dead_letter
    - 进入死信后从 pending 队列消失，并在 count_projection_backlog 中体现
    """
    mem_id = "mem_dl_guard_001"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Dead letter threshold test content",
    )
    task_id = store.fetch_pending_projection_tasks()[0]["id"]

    # 连续重试 7 次
    for i in range(1, 8):
        updated = store.mark_projection_failed(
            task_id, claim_revision=1, error=f"Transient error {i}", next_retry_at=0
        )
        assert updated == 1
        # 未达到 8 次前，mark_projection_dead_letter 必须返回 False
        assert store.mark_projection_dead_letter(task_id, claim_revision=1) is False

    # 第 8 次失败
    store.mark_projection_failed(task_id, claim_revision=1, error="Persistent crash 8", next_retry_at=0)

    # 此时成功置入死信
    dl_success = store.mark_projection_dead_letter(task_id, claim_revision=1)
    assert dl_success is True

    # 验证 pending 队列已被清空，backlog 显示 1 条 dead_letter
    assert len(store.fetch_pending_projection_tasks()) == 0
    backlog = store.count_projection_backlog()
    assert backlog["pending"] == 0
    assert backlog["dead_letter"] == 1


def test_dead_letter_replay_semantics_and_absorption(store):
    """
    攻击面 2.5: 死信后的重放 (replay) 语义
    模拟 admin CLI / outbox.replay_dead_letters 的核心机制：
    1. 任务死信后，以最新 memories.projection_revision 重新入队；
    2. 生成一条新的 pending 任务（或合并到现有 pending）；
    3. 原 dead_letter 任务标记 status='done'，last_error='absorbed by pending#...'；
    4. 验证 backlog 指标：dead_letter 减为 0，pending 增为 1。
    """
    mem_id = "mem_replay_001"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Content doomed to dead letter then replayed",
    )
    task_id = store.fetch_pending_projection_tasks()[0]["id"]

    # 制造 8 次失败使之死信
    for _ in range(8):
        store.mark_projection_failed(task_id, claim_revision=1, error="Fatal", next_retry_at=0)
    store.mark_projection_dead_letter(task_id, claim_revision=1)

    assert store.count_projection_backlog()["dead_letter"] == 1
    assert store.count_projection_backlog()["pending"] == 0

    # 执行 replay 流程
    now_ts = int(time.time())
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        cur.execute("SELECT projection_revision, collection_name FROM memories WHERE memory_id = ?;", (mem_id,))
        m_row = cur.fetchone()
        curr_rev = m_row["projection_revision"]
        col = m_row["collection_name"] or "ai_memory"

        # 重新入队生成 pending
        new_pending_id = store.enqueue_projection_task(
            cur,
            memory_id=mem_id,
            projection_revision=curr_rev,
            target_collection=col,
        )
        assert new_pending_id != task_id, "死信行与 pending 行 id 应当不同"

        # 原 dead_letter 行标记 absorbed
        cur.execute(
            "UPDATE memory_projection_outbox SET status = 'done', last_error = ?, updated_at = ? WHERE id = ?;",
            (f"absorbed by pending#{new_pending_id}", now_ts, task_id),
        )

    # 验证原死信行已被吸收
    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT status, last_error FROM memory_projection_outbox WHERE id = ?;", (task_id,))
    orig_row = cur.fetchone()
    assert orig_row["status"] == "done"
    assert f"absorbed by pending#{new_pending_id}" in orig_row["last_error"]

    # 验证新任务已处于 pending
    tasks = store.fetch_pending_projection_tasks()
    assert len(tasks) == 1
    assert tasks[0]["id"] == new_pending_id
    assert tasks[0]["status"] == "pending"
    assert tasks[0]["retry_count"] == 0

    # 验证 backlog
    backlog = store.count_projection_backlog()
    assert backlog["dead_letter"] == 0
    assert backlog["pending"] == 1


def test_partial_unique_index_collection_isolation(store):
    """
    攻击面 2.6: 部分唯一索引与多集合隔离验证
    1. 同一 target_collection + memory_id：第二次 pending 入队触发 ON CONFLICT DO UPDATE（行合并）
    2. 不同 target_collection + 同一 memory_id：独立插入两条 pending 记录，互不干扰
    """
    mem_id = "mem_col_iso_001"

    # 1. 相同 collection (default: ai_memory) 重复入队
    store.create_memory_v3(
        memory_id=mem_id,
        content="Multi-collection test content",
        collection_name="ai_memory",
    )
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        id1 = store.enqueue_projection_task(cur, memory_id=mem_id, projection_revision=1, target_collection="ai_memory")
        id2 = store.enqueue_projection_task(cur, memory_id=mem_id, projection_revision=1, target_collection="ai_memory")
    assert id1 == id2, "相同集合相同 memory_id 必须合并入同一行"

    # 2. 不同集合 (finance_docs)
    with store.locked_write_transaction() as conn:
        cur = conn.cursor()
        id_finance = store.enqueue_projection_task(
            cur, memory_id=mem_id, projection_revision=1, target_collection="finance_docs"
        )
    assert id_finance != id1, "不同集合的投影任务必须独立，不能误合并"

    tasks = store.fetch_pending_projection_tasks(limit=10)
    assert len(tasks) == 2
    cols = {t["target_collection"] for t in tasks}
    assert cols == {"ai_memory", "finance_docs"}

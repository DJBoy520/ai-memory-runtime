"""
Red-Team Audit Track R2: Transaction Atomicity & Idempotency Tests
验证 AMR v5 的事务原子性承诺：
1. create / update / delete 业务中途抛异常时全部回滚，无半写
2. 幂等业务失败时 key 一同回滚，可用同 key 干净重试
3. locked_write_transaction 嵌套重入行为与缺陷
4. execute_idempotent 非字典/None 返回值重放缺陷
"""

import json
import sqlite3
import threading
import time
from typing import Any, Dict
from unittest.mock import patch

import pytest

from src.core.errors import IdempotencyConflict, VersionConflictError
from src.core.idempotency import compute_request_digest
from src.core.session_store import SessionStore


@pytest.fixture
def store(tmp_path):
    db_file = tmp_path / "audit_tx.db"
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


def test_create_memory_v3_atomicity_on_outbox_failure(store):
    """
    攻击面 1.1: 创建中途抛异常（outbox 入队失败）
    断言：主表 memories、快照表 memory_revisions、审计日志 memory_audit_log 全量回滚，无残留孤儿数据。
    """
    mem_id = "mem_tx_fail_create"
    orig_enqueue = store.enqueue_projection_task

    def failing_enqueue(cursor, **kwargs):
        raise sqlite3.OperationalError("Simulated disk I/O failure during outbox enqueue")

    with patch.object(store, "enqueue_projection_task", side_effect=failing_enqueue):
        with pytest.raises(sqlite3.OperationalError) as exc_info:
            store.create_memory_v3(
                memory_id=mem_id,
                content="Transaction atomicity test content",
                type="fact",
            )
        assert "Simulated disk I/O failure" in str(exc_info.value)

    # 验证没有任何表残留半写数据
    conn = store.get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM memories WHERE memory_id = ?;", (mem_id,))
    assert cur.fetchone()[0] == 0, "memories 表存在未回滚的半写数据"

    cur.execute("SELECT COUNT(*) FROM memory_revisions WHERE memory_id = ?;", (mem_id,))
    assert cur.fetchone()[0] == 0, "memory_revisions 表存在未回滚的半写数据"

    cur.execute("SELECT COUNT(*) FROM memory_audit_log WHERE memory_id = ?;", (mem_id,))
    assert cur.fetchone()[0] == 0, "memory_audit_log 表存在未回滚的半写数据"

    cur.execute("SELECT COUNT(*) FROM memory_projection_outbox WHERE memory_id = ?;", (mem_id,))
    assert cur.fetchone()[0] == 0, "memory_projection_outbox 表存在未回滚的半写数据"


def test_update_memory_v3_atomicity_on_audit_failure(store):
    """
    攻击面 1.2: 更新中途抛异常（审计日志写入失败）
    断言：正文修改、版本号递增、修订快照、outbox 更新全部回滚，保持原子性。
    """
    mem_id = "mem_tx_fail_update"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Original content v1",
        type="fact",
    )
    init_mem = store.get_memory_v3(mem_id)
    assert init_mem["version"] == 1
    assert init_mem["content"] == "Original content v1"

    # 使用 SQLite 触发器注入错误，模拟向 memory_audit_log 写入时的异常
    conn = store.get_connection()
    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS fail_audit BEFORE INSERT ON memory_audit_log BEGIN SELECT RAISE(ABORT, 'Simulated corruption on audit table'); END;"
    )

    with pytest.raises(sqlite3.IntegrityError) as exc_info:
        store.update_memory_v3(
            memory_id=mem_id,
            content="Updated content v2 that must roll back",
            expected_version=1,
            change_reason="Audit failure test",
        )
    assert "Simulated corruption on audit table" in str(exc_info.value)

    # 清理触发器
    conn.execute("DROP TRIGGER IF EXISTS fail_audit;")

    # 验证回滚后状态完全保持初始状态
    after_mem = store.get_memory_v3(mem_id)
    assert after_mem["version"] == 1, "版本号被错误推进"
    assert after_mem["content"] == "Original content v1", "正文被污染半写"

    revs = store.get_memory_revisions(mem_id)
    assert len(revs) == 1, "memory_revisions 泄露了未提交的快照"
    assert revs[0]["version"] == 1

    tasks = store.fetch_pending_projection_tasks()
    assert len(tasks) == 1
    assert tasks[0]["projection_revision"] == 1, "outbox 投影 revision 被错误递增"


def test_delete_atomicity_on_enqueue_failure(store):
    """
    攻击面 1.3: 状态变更为 DELETED 时，enqueue 失败导致全事务回滚
    断言：状态仍为 ACTIVE，deleted_at 未置位，无半写。
    """
    mem_id = "mem_tx_fail_delete"
    store.create_memory_v3(
        memory_id=mem_id,
        content="Content to delete atomically",
        type="fact",
    )

    with patch.object(store, "enqueue_projection_task", side_effect=RuntimeError("Outbox queue blocked")):
        with pytest.raises(RuntimeError) as exc_info:
            store.change_memory_status(
                memory_id=mem_id,
                new_status="DELETED",
                reason="Red-team fault injection",
            )
        assert "Outbox queue blocked" in str(exc_info.value)

    mem = store.get_memory_v3(mem_id)
    assert mem["status"] == "ACTIVE"
    assert mem["deleted_at"] is None
    assert mem["deletion_reason"] is None

    audit_logs = store.get_audit_logs(mem_id)
    actions = [a["action"] for a in audit_logs]
    assert "delete" not in actions


def test_idempotent_business_failure_rolls_back_key(store):
    """
    攻击面 1.4: 幂等业务函数失败 → idempotency_keys 随事务回滚，不烧掉 key，同 key 可重试
    """
    payload = {"content": "Retryable content payload"}
    digest = compute_request_digest("create", payload)
    idem_key = "key_tx_retry_001"

    attempt = 0

    def business(cur):
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            raise ValueError("First attempt business logic crashed")
        return store.create_memory_v3(
            memory_id="mem_retry_ok",
            content=payload["content"],
            cursor=cur,
        )

    # 首次执行失败
    with pytest.raises(ValueError) as exc_info:
        store.execute_idempotent("create", idem_key, digest, business)
    assert "First attempt business logic crashed" in str(exc_info.value)

    # 确认 key 未被持久化占位
    rec = store.get_idempotency_record("default", "create", idem_key)
    assert rec is None, "失败事务的 idempotency key 未回滚，导致 key 被提前烧掉"

    # 用相同的 key 和 digest 干净重试
    res = store.execute_idempotent("create", idem_key, digest, business)
    assert res["memory_id"] == "mem_retry_ok"
    assert "idempotent_replay" not in res

    # 再次重放验证已生效
    replay = store.execute_idempotent("create", idem_key, digest, business)
    assert replay["memory_id"] == "mem_retry_ok"
    assert replay["idempotent_replay"] is True


def test_idempotent_non_dict_response_replay_defect(store):
    """
    攻击面 1.5: execute_idempotent 在 business_fn 返回 None 或非字典时的重放行为。
    修复后契约：字典返回合并幂等标记，None/非字典包装成 {"result": ..., "idempotent_replay": True} 返回，绝不崩溃。
    """
    idem_key = "key_non_dict_001"
    digest = compute_request_digest("custom_op", {"x": 1})

    # 首次执行返回 None
    res1 = store.execute_idempotent("custom_op", idem_key, digest, lambda cur: None)
    assert res1 is None

    # 重放时：断言返回包装字典并包含 idempotent_replay=True
    res_replay = store.execute_idempotent("custom_op", idem_key, digest, lambda cur: None)
    assert res_replay == {"result": None, "idempotent_replay": True}


def test_locked_write_transaction_nesting_defect(store):
    """
    攻击面 1.6: locked_write_transaction 嵌套重入行为
    修复后契约：支持重入与嵌套事务，内层使用 SAVEPOINT，最外层统一 COMMIT，
    保证原子性与无报错完成。
    """
    with store.locked_write_transaction() as conn1:
        conn1.execute("INSERT INTO sessions (session_id, agent_id, started_at) VALUES ('s1', 'a1', 100);")
        with store.locked_write_transaction() as conn2:
            conn2.execute("INSERT INTO sessions (session_id, agent_id, started_at) VALUES ('s2', 'a2', 200);")

    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT session_id FROM sessions WHERE session_id IN ('s1', 's2') ORDER BY session_id;")
    rows = [r[0] for r in cur.fetchall()]
    assert rows == ["s1", "s2"]


def test_checkpoint_mode_whitelist(store):
    """
    F-R2-06: checkpoint(mode) 参数白名单校验
    仅允许 PASSIVE/FULL/RESTART/TRUNCATE（大小写不敏感），非法值抛 ValueError。
    """
    for valid_mode in ["PASSIVE", "FULL", "RESTART", "TRUNCATE", "passive", "restart"]:
        store.checkpoint(valid_mode)
    for invalid_mode in ["UNKNOWN", "DROP TABLE", "; DROP TABLE memories; --", ""]:
        with pytest.raises(ValueError) as exc:
            store.checkpoint(invalid_mode)
        assert "Invalid checkpoint mode" in str(exc.value)


def test_concurrent_threads_locked_write_transaction_serialized(store):
    """
    攻击面 1.7: 多线程并发 locked_write_transaction 执行序列化，无死锁、无半写
    """
    n_threads = 8
    n_writes_per_thread = 10
    errors = []

    def writer(thread_idx: int):
        for w in range(n_writes_per_thread):
            try:
                sid = f"sess_{thread_idx}_{w}"
                with store.locked_write_transaction() as conn:
                    conn.execute(
                        "INSERT INTO sessions (session_id, agent_id, started_at) VALUES (?, ?, ?);",
                        (sid, f"agent_{thread_idx}", int(time.time())),
                    )
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"并发写操作发生错误: {errors}"

    conn = store.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM sessions;")
    assert cur.fetchone()[0] == n_threads * n_writes_per_thread

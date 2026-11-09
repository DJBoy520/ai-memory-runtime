"""测试写接口幂等控制：同 key 重放、异 digest 冲突、异常回滚、过期覆写、两线程并发。"""
import concurrent.futures
import time
import pytest

from src.core.errors import IdempotencyConflict, VersionConflictError
from src.core.idempotency import compute_request_digest
from src.core.session_store import SessionStore


@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_idem.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


def test_idempotent_replay_same_key_same_digest(store):
    """验证同 key 同 digest 重放首次提交结果，返回 idempotent_replay=True 且 business_fn 仅执行一次"""
    call_count = 0
    payload = {"content": "Same digest test", "type": "fact"}
    digest = compute_request_digest("create", payload)
    idem_key = "key_replay_001"

    def business(cur):
        nonlocal call_count
        call_count += 1
        res = store.create_memory_v3(
            memory_id="mem_idem_1",
            content=payload["content"],
            type=payload["type"],
            cursor=cur,
        )
        return res

    # 首次执行
    r1 = store.execute_idempotent("create", idem_key, digest, business)
    assert call_count == 1
    assert r1["memory_id"] == "mem_idem_1"
    assert "idempotent_replay" not in r1

    # 再次使用同 key 同 digest 执行 -> 重放
    r2 = store.execute_idempotent("create", idem_key, digest, business)
    assert call_count == 1  # 业务函数未再次执行
    assert r2["memory_id"] == "mem_idem_1"
    assert r2["idempotent_replay"] is True

    # 验证底层 idempotency_keys 表记录
    rec = store.get_idempotency_record("default", "create", idem_key)
    assert rec is not None
    assert rec["memory_id"] == "mem_idem_1"
    assert rec["request_digest"] == digest


def test_idempotent_conflict_different_digest(store):
    """验证同 key 异 digest 抛出 IdempotencyConflict，包含 existing_memory_id"""
    payload1 = {"content": "First payload"}
    digest1 = compute_request_digest("create", payload1)
    idem_key = "key_conflict_001"

    def business1(cur):
        return store.create_memory_v3(
            memory_id="mem_conf_1",
            content=payload1["content"],
            cursor=cur,
        )

    store.execute_idempotent("create", idem_key, digest1, business1)

    # 传入不同内容（不同 digest）复用同一个 key
    payload2 = {"content": "Tampered different payload"}
    digest2 = compute_request_digest("create", payload2)

    def business2(cur):
        return store.create_memory_v3(
            memory_id="mem_conf_2",
            content=payload2["content"],
            cursor=cur,
        )

    with pytest.raises(IdempotencyConflict) as exc_info:
        store.execute_idempotent("create", idem_key, digest2, business2)

    assert exc_info.value.op == "create"
    assert exc_info.value.idem_key == idem_key
    assert exc_info.value.existing_memory_id == "mem_conf_1"


def test_business_failure_rolls_back_key(store):
    """验证业务逻辑异常时，整个事务回滚，幂等 key 不被烧掉，客户端修复后可重试"""
    payload = {"content": "Fail then succeed"}
    digest = compute_request_digest("update", payload)
    idem_key = "key_rollback_001"

    attempt = 0

    def failing_business(cur):
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            raise VersionConflictError("VERSION_CONFLICT: Expected version 2, got 1", memory_id="mem_x")
        return {"memory_id": "mem_x", "version": 2, "status": "ACTIVE"}

    # 首次执行抛异常
    with pytest.raises(VersionConflictError):
        store.execute_idempotent("update", idem_key, digest, failing_business)

    # 验证 idempotency_keys 中无该 key 记录（已随事务回滚）
    rec = store.get_idempotency_record("default", "update", idem_key)
    assert rec is None

    # 重试执行（使用同一 key 和 digest）可以干净执行成功
    res = store.execute_idempotent("update", idem_key, digest, failing_business)
    assert res["memory_id"] == "mem_x"
    assert res["version"] == 2

    # 验证最终记录已写入
    rec2 = store.get_idempotency_record("default", "update", idem_key)
    assert rec2 is not None
    assert rec2["memory_id"] == "mem_x"


def test_expired_key_atomic_overwrite(store):
    """验证超过保留期的 key 可以原子覆写占位并重新执行业务逻辑"""
    payload1 = {"content": "Old request"}
    digest1 = compute_request_digest("create", payload1)
    idem_key = "key_expired_001"

    # 执行并写入一条记录
    store.execute_idempotent("create", idem_key, digest1, lambda cur: {"memory_id": "mem_old", "version": 1})

    # 手工篡改 created_at 为 10 天前 (超过 7 天保留期)
    ten_days_ago = int(time.time()) - (10 * 86400)
    with store.locked_write_transaction() as conn:
        conn.execute("UPDATE idempotency_keys SET created_at = ? WHERE idem_key = ?;", (ten_days_ago, idem_key))

    # 使用不同 digest 重新请求同一个已过期的 key
    payload2 = {"content": "New request after expiration"}
    digest2 = compute_request_digest("create", payload2)
    new_call_ran = False

    def business2(cur):
        nonlocal new_call_ran
        new_call_ran = True
        return {"memory_id": "mem_new", "version": 1}

    # 过期 key 应当原子覆写，而非报冲突或重放旧响应
    res = store.execute_idempotent("create", idem_key, digest2, business2, retention_days=7)
    assert new_call_ran is True
    assert res["memory_id"] == "mem_new"

    # 验证记录被更新为最新 digest 和 memory_id
    rec = store.get_idempotency_record("default", "create", idem_key)
    assert rec["request_digest"] == digest2
    assert rec["memory_id"] == "mem_new"


def test_concurrent_same_key_serialized(store):
    """两线程并发同 key 同 digest：恰一次业务提交，另一个重放"""
    payload = {"content": "Concurrent execution test"}
    digest = compute_request_digest("create", payload)
    idem_key = "key_concurrent_001"

    execution_count = 0

    def business(cur):
        nonlocal execution_count
        execution_count += 1
        time.sleep(0.05)  # 模拟微小耗时
        return store.create_memory_v3(
            memory_id="mem_conc_1",
            content=payload["content"],
            cursor=cur,
        )

    def worker():
        return store.execute_idempotent("create", idem_key, digest, business)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(worker)
        f2 = executor.submit(worker)
        r1 = f1.result()
        r2 = f2.result()

    # 业务函数恰执行 1 次
    assert execution_count == 1
    # 两个返回值都是 mem_conc_1
    assert r1["memory_id"] == "mem_conc_1"
    assert r2["memory_id"] == "mem_conc_1"
    # 其中一个必为 idempotent_replay
    replays = [r.get("idempotent_replay", False) for r in (r1, r2)]
    assert replays.count(True) == 1
    assert replays.count(False) == 1


def test_cleanup_expired_idempotency(store):
    """验证 cleanup_expired_idempotency 清理超过 retention_days 的记录"""
    now = int(time.time())
    store.execute_idempotent("create", "key_act", "d1", lambda cur: {"memory_id": "m1"})
    store.execute_idempotent("create", "key_exp", "d2", lambda cur: {"memory_id": "m2"})

    # 将 key_exp 设为 30 天前
    old_ts = now - (30 * 86400)
    with store.locked_write_transaction() as conn:
        conn.execute("UPDATE idempotency_keys SET created_at = ? WHERE idem_key = 'key_exp';", (old_ts,))

    deleted = store.cleanup_expired_idempotency(retention_days=7)
    assert deleted == 1

    assert store.get_idempotency_record("default", "create", "key_act") is not None
    assert store.get_idempotency_record("default", "create", "key_exp") is None

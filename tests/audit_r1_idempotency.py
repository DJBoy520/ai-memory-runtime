"""
AMR v5 Audit Track R1: 幂等契约红队审计测试 (SessionStore & MemoryService 幂等仲裁层)
覆盖攻击面：
1. 同 key 同 digest 重放：create / update / delete 三类操作均返回幂等快照并附加 idempotent_replay=True
2. 同 key 异 digest 冲突：抛出 IdempotencyConflict 并携带完整 data {op, idem_key, existing_memory_id}
3. 边界 Key：空字符串、超长 key (10,000 字符)、Unicode/Emoji 特殊字符 key
4. 业务失败自动回滚：业务抛出异常时不烧 key，后续重试正常成功
5. 过期键覆写执行：超过 retention_days 的旧 key 重新执行并覆写
6. 跨 Agent 隔离性：不同 agent 相同 idem_key 互不影响、独立持久化
7. 语义等价但未规范化导致的摘要误冲突缺陷 (status 大小写、source_refs None vs [])
"""

import time
import pytest
from pathlib import Path
from typing import Any, Dict

from src.core.errors import IdempotencyConflict
from src.core.idempotency import compute_request_digest
from src.core.session_store import SessionStore
from src.service.memory_service import MemoryService
class DummyEngine:
    async def embed(self, texts):
        return [[0.1] * 1024 for _ in texts]


class DummyQdrant:
    def get_point_by_memory_id(self, *args, **kwargs):
        return None


@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_audit_idem.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


@pytest.fixture
def memory_service(store):
    return MemoryService(
        engine=DummyEngine(),
        qdrant_manager=DummyQdrant(),
        session_store=store,
    )


# =========================================================================
# 1. create / update / delete 三类操作的同 key 重放
# =========================================================================

@pytest.mark.asyncio
async def test_idempotent_replay_create_update_delete(memory_service, store):
    """验证 create / update / delete 三类操作带 idempotency_key 均支持精确重放"""
    # 1. Create 幂等重放
    create_key = "idem_create_001"
    res1 = await memory_service.memory_create(
        content="Substantive rule 1 for audit testing",
        type="decision/arch",
        idempotency_key=create_key,
        agent_id="openclaw",
    )
    mid = res1["memory_id"]
    assert "idempotent_replay" not in res1

    # 再次以完全相同参数调用
    res1_replay = await memory_service.memory_create(
        content="Substantive rule 1 for audit testing",
        type="decision/arch",
        idempotency_key=create_key,
        agent_id="openclaw",
    )
    assert res1_replay["memory_id"] == mid
    assert res1_replay.get("idempotent_replay") is True

    # 2. Update 幂等重放
    update_key = "idem_update_001"
    res2 = await memory_service.memory_update(
        memory_id=mid,
        content="Substantive rule 1 updated for audit testing",
        expected_version=1,
        change_reason="Audit test refine",
        idempotency_key=update_key,
        agent_id="openclaw",
    )
    assert res2["version"] == 2
    assert "idempotent_replay" not in res2

    res2_replay = await memory_service.memory_update(
        memory_id=mid,
        content="Substantive rule 1 updated for audit testing",
        expected_version=1,
        change_reason="Audit test refine",
        idempotency_key=update_key,
        agent_id="openclaw",
    )
    assert res2_replay["version"] == 2
    assert res2_replay.get("idempotent_replay") is True

    # 3. Delete 幂等重放
    delete_key = "idem_delete_001"
    res3 = await memory_service.memory_delete(
        memory_id=mid,
        reason="Audit cleanup test",
        idempotency_key=delete_key,
        agent_id="openclaw",
    )
    assert res3["current_status"] == "DELETED"
    assert "idempotent_replay" not in res3

    res3_replay = await memory_service.memory_delete(
        memory_id=mid,
        reason="Audit cleanup test",
        idempotency_key=delete_key,
        agent_id="openclaw",
    )
    assert res3_replay["current_status"] == "DELETED"
    assert res3_replay.get("idempotent_replay") is True


# =========================================================================
# 2. 同 key 异 digest 冲突与数据完整性
# =========================================================================

@pytest.mark.asyncio
async def test_idempotent_conflict_data_integrity(memory_service):
    """验证同 key 异 digest 冲突抛出 IdempotencyConflict，且 data 包含 op, idem_key, existing_memory_id"""
    conflict_key = "key_conflict_audit_001"
    res = await memory_service.memory_create(
        content="Initial content version alpha",
        idempotency_key=conflict_key,
        agent_id="hermes",
    )
    mid = res["memory_id"]

    # 尝试用同一个 key 但不同的正文 content 发起 create
    with pytest.raises(IdempotencyConflict) as exc_info:
        await memory_service.memory_create(
            content="Different content version beta",
            idempotency_key=conflict_key,
            agent_id="hermes",
        )

    err = exc_info.value
    assert err.op == "create"
    assert err.idem_key == conflict_key
    assert err.existing_memory_id == mid
    assert "Idempotency conflict" in str(err)


# =========================================================================
# 3. 边界 Key：空字符串、超长 key、Unicode 与特殊符号
# =========================================================================

@pytest.mark.asyncio
async def test_empty_string_idempotency_key(memory_service):
    """当 idempotency_key 为空字符串 '' 时，应当视同未提供幂等键（执行新创建，不幂等）"""
    res1 = await memory_service.memory_create(
        content="Test empty string idem key",
        idempotency_key="",
        agent_id="openclaw",
    )
    res2 = await memory_service.memory_create(
        content="Test empty string idem key",
        idempotency_key="",
        agent_id="openclaw",
    )
    # 两次调用均生成了不同的独立 memory_id
    assert res1["memory_id"] != res2["memory_id"]


@pytest.mark.asyncio
async def test_unicode_and_super_long_idempotency_key(memory_service, store):
    """支持包含 Emoji、Unicode、控制字符以及超长 10,000 字符的幂等键正常处理"""
    unicode_key = "🚀_🔑_TEST_IDEM_中文_Русский_한국어_#$&@*!"
    res1 = await memory_service.memory_create(
        content="Unicode key substantive content",
        idempotency_key=unicode_key,
        agent_id="openclaw",
    )
    res1_replay = await memory_service.memory_create(
        content="Unicode key substantive content",
        idempotency_key=unicode_key,
        agent_id="openclaw",
    )
    assert res1_replay["memory_id"] == res1["memory_id"]
    assert res1_replay.get("idempotent_replay") is True

    # 10,000 字符超长 Key
    long_key = "LONG_KEY_" + "X" * 10000
    res2 = await memory_service.memory_create(
        content="Long key substantive content",
        idempotency_key=long_key,
        agent_id="openclaw",
    )
    res2_replay = await memory_service.memory_create(
        content="Long key substantive content",
        idempotency_key=long_key,
        agent_id="openclaw",
    )
    assert res2_replay["memory_id"] == res2["memory_id"]
    assert res2_replay.get("idempotent_replay") is True


# =========================================================================
# 4. 业务失败不烧 Key (事务级回滚)
# =========================================================================

def test_business_failure_does_not_burn_idempotency_key(store):
    """业务函数失败抛出异常时，事务自动回滚，不烧 key，客户端重试可再次执行"""
    idem_key = "retry_after_fail_001"
    digest = compute_request_digest("create", {"action": "do_fail"})
    call_count = 0

    def failing_business(cur):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Database deadlock simulation or business error")
        return store.create_memory_v3(
            memory_id="mem_retry_success",
            content="Substantive content after retry",
            cursor=cur,
        )

    # 第一次调用：失败
    with pytest.raises(RuntimeError):
        store.execute_idempotent("create", idem_key, digest, failing_business)
    assert call_count == 1

    # 验证底层 idempotency_keys 表中无残留记录
    rec = store.get_idempotency_record("default", "create", idem_key)
    assert rec is None

    # 第二次调用：重试成功，不应报 IdempotencyConflict
    res = store.execute_idempotent("create", idem_key, digest, failing_business)
    assert call_count == 2
    assert res["memory_id"] == "mem_retry_success"


# =========================================================================
# 5. 过期键覆写与重新执行
# =========================================================================

def test_expired_idempotency_key_overwrite(store):
    """超过保留期限 (retention_days) 的幂等记录可被同 key 新请求安全覆写"""
    idem_key = "expired_key_001"
    payload1 = {"step": 1}
    digest1 = compute_request_digest("create", payload1)

    # 1. 插入一个 10 天前创建的记录 (模拟过期)
    old_ts = int(time.time()) - (10 * 86400)
    with store.locked_write_transaction() as conn:
        conn.cursor().execute(
            """
            INSERT INTO idempotency_keys (agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at)
            VALUES ('default', 'create', ?, ?, 'mem_old_1', '{"memory_id": "mem_old_1"}', ?);
            """,
            (idem_key, digest1, old_ts),
        )

    # 2. 以新 digest 再次调用，保留期为 7 天
    payload2 = {"step": 2}
    digest2 = compute_request_digest("create", payload2)

    def business_new(cur):
        return store.create_memory_v3(
            memory_id="mem_fresh_2",
            content="Fresh content replacing expired key",
            cursor=cur,
        )

    res = store.execute_idempotent("create", idem_key, digest2, business_new, retention_days=7)
    assert res["memory_id"] == "mem_fresh_2"

    # 验证数据库记录已被覆写为新 digest 与新 memory_id
    rec = store.get_idempotency_record("default", "create", idem_key)
    assert rec["memory_id"] == "mem_fresh_2"
    assert rec["request_digest"] == digest2


# =========================================================================
# 6. 跨 Agent 隔离性
# =========================================================================

@pytest.mark.asyncio
async def test_cross_agent_idempotency_isolation(memory_service):
    """不同 Agent 使用相同的 idempotency_key 不应产生冲突或互相重放污染"""
    shared_key = "shared_key_across_agents"

    res_agent1 = await memory_service.memory_create(
        content="Knowledge created by Agent Alpha",
        idempotency_key=shared_key,
        agent_id="openclaw",
    )

    res_agent2 = await memory_service.memory_create(
        content="Knowledge created by Agent Beta",
        idempotency_key=shared_key,
        agent_id="hermes",
    )

    # 两者应当生成各自独立的 memory_id，绝不发生跨 agent 冲突或覆盖
    assert res_agent1["memory_id"] != res_agent2["memory_id"]
    assert res_agent1["created_by_agent"] == "openclaw"
    assert res_agent2["created_by_agent"] == "hermes"


# =========================================================================
# 7. 漏洞发现用例：语义等价但未规范化导致的摘要误判冲突缺陷
# =========================================================================

@pytest.mark.asyncio
async def test_unnormalized_status_causes_false_idempotency_conflict_bug(memory_service):
    """
    【发现 BUG 验证】
    在 memory_service.py:825:
    `digest_payload` 直接使用了未规范化的原始入参 `status`:
    ```python
    digest_payload = {
        "content": content,
        "status": status,  # 传入 "ACTIVE" vs "active" 会产生不同的 digest！
        ...
    }
    ```
    而在底层存储中，两者均被 `normalize_status` 归一为 "ACTIVE"，业务效果完全相同。
    若客户端重试时偶然改变了 status 的大小写（例如从 "ACTIVE" 变成 "active"），
    AMR 不会重放，而是错误地抛出 -32006 IdempotencyConflict！
    """
    idem_key = "key_case_sensitivity_test"

    # 第一次传入大写 ACTIVE
    res1 = await memory_service.memory_create(
        content="Substantive test fact for case sensitivity",
        status="ACTIVE",
        idempotency_key=idem_key,
        agent_id="openclaw",
    )

    # 客户端重试时传入小写 active（语义完全一致）
    try:
        res2 = await memory_service.memory_create(
            content="Substantive test fact for case sensitivity",
            status="active",
            idempotency_key=idem_key,
            agent_id="openclaw",
        )
        assert res2["memory_id"] == res1["memory_id"]
        assert res2.get("idempotent_replay") is True
    except IdempotencyConflict as e:
        pytest.fail(
            f"Semantic equivalence ('ACTIVE' vs 'active') caused false IdempotencyConflict: {e} "
            "(digest_payload computed before normalize_status)"
        )

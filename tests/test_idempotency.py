"""测试服务层幂等控制契约：
- create 同 key 同 digest -> 只建一条 memory、一次 outbox；重放 idempotent_replay=True；
- 同 key 异 digest -> IdempotencyConflict；
- 业务失败（例如 content 太短/版本冲突）-> key/业务行/队列行全无，修参后同 key 成功；
- 过期 key（retention 设 0/很小 + 手动改 created_at）-> 重执行不重放；
- 两线程并发同 key -> 恰一次业务写入。
"""
import asyncio
import pytest

from src.core.errors import IdempotencyConflict, VersionConflictError
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
    db_file = str(tmp_path / "test_service_idempotency.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


@pytest.fixture
def service(store):
    return MemoryService(
        engine=DummyEngine(),
        qdrant_manager=DummyQdrant(),
        session_store=store,
    )


@pytest.mark.asyncio
async def test_idempotent_create_same_key_same_digest(service, store):
    """create 同 key 同 digest -> 只建一条 memory、一次 outbox；重放 idempotent_replay=True"""
    idem_key = "key_create_replay_001"
    content = "Substantive knowledge content for idempotency replay test"

    # 首次提交
    r1 = await service.memory_create(
        content=content,
        project_id="proj_replay",
        type="fact",
        idempotency_key=idem_key,
    )
    assert r1 is not None
    assert r1["projection_status"] == "pending"
    assert "idempotent_replay" not in r1
    mid1 = r1["memory_id"]

    # 二次提交（同 key 同 digest）
    r2 = await service.memory_create(
        content=content,
        project_id="proj_replay",
        type="fact",
        idempotency_key=idem_key,
    )
    assert r2 is not None
    assert r2["memory_id"] == mid1
    assert r2.get("idempotent_replay") is True

    # 验证底层数据库只有 1 条 memory 和 1 条 outbox 记录
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM memories WHERE memory_id = ?;", (mid1,))
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT count(*) FROM memory_projection_outbox WHERE memory_id = ?;", (mid1,))
        assert cur.fetchone()[0] == 1


@pytest.mark.asyncio
async def test_idempotent_create_same_key_different_digest(service):
    """同 key 异 digest -> 抛出 IdempotencyConflict"""
    idem_key = "key_conflict_001"
    content1 = "Initial substantive knowledge for conflict test"

    # 首次提交成功
    await service.memory_create(
        content=content1,
        idempotency_key=idem_key,
    )

    # 传入不同 content（不同 digest）复用同一个 key
    content2 = "Tampered completely different knowledge text"
    with pytest.raises(IdempotencyConflict) as excinfo:
        await service.memory_create(
            content=content2,
            idempotency_key=idem_key,
        )

    assert excinfo.value.op == "create"
    assert excinfo.value.idem_key == idem_key


@pytest.mark.asyncio
async def test_idempotent_business_failure_rolls_back_and_allows_retry(service, store):
    """业务失败（例如 content 太短/版本冲突）-> key/业务行/队列行全无，修参后同 key 成功"""
    idem_key_create = "key_fail_create_001"

    # 1. create 业务失败（内容太短，少于 5 字符）
    with pytest.raises(ValueError, match="too short"):
        await service.memory_create(
            content="abc",
            idempotency_key=idem_key_create,
        )

    # 验证 key 占位、业务行、outbox 队列行全无
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM idempotency_keys WHERE idem_key = ?;", (idem_key_create,))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM memories;")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM memory_projection_outbox;")
        assert cur.fetchone()[0] == 0

    # 修复参数后使用同一个 key 成功创建
    r_fixed = await service.memory_create(
        content="Valid length knowledge content now",
        idempotency_key=idem_key_create,
    )
    assert r_fixed is not None
    assert r_fixed["projection_status"] == "pending"
    assert "idempotent_replay" not in r_fixed
    mid = r_fixed["memory_id"]

    # 2. update 业务失败（乐观锁版本冲突 expected_version 不一致）
    idem_key_update = "key_fail_update_001"
    with pytest.raises(VersionConflictError):
        await service.memory_update(
            memory_id=mid,
            content="Updated content text here",
            expected_version=99,
            change_reason="Typo fix",
            idempotency_key=idem_key_update,
        )

    # 验证 update key 占位未留下
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM idempotency_keys WHERE idem_key = ?;", (idem_key_update,))
        assert cur.fetchone()[0] == 0

    # 修复 expected_version 后同 key 成功
    up_fixed = await service.memory_update(
        memory_id=mid,
        content="Updated content text here",
        expected_version=1,
        change_reason="Typo fix",
        idempotency_key=idem_key_update,
    )
    assert up_fixed["version"] == 2
    assert "idempotent_replay" not in up_fixed


@pytest.mark.asyncio
async def test_idempotent_expired_key_reexecutes(service, store):
    """过期 key（手动改 created_at < cutoff）-> 重执行产生新记忆，不重放旧记忆"""
    idem_key = "key_expiring_001"
    content = "Content that will be submitted again after key expires"

    # 首次执行
    r1 = await service.memory_create(
        content=content,
        idempotency_key=idem_key,
    )
    mid1 = r1["memory_id"]

    # 人工修改 created_at 为远古时间戳（simulate expired）
    with store._lock:
        conn = store._get_connection()
        conn.execute("UPDATE idempotency_keys SET created_at = 1 WHERE idem_key = ?;", (idem_key,))
        conn.commit()

    # 再次提交 -> 应当作为过期键重新执行，而非重放
    r2 = await service.memory_create(
        content=content,
        idempotency_key=idem_key,
    )
    mid2 = r2["memory_id"]

    assert "idempotent_replay" not in r2
    assert mid2 != mid1


@pytest.mark.asyncio
async def test_idempotent_concurrent_two_threads(service, store):
    """两并发协程同 key -> 恰一次业务写入，一建一放或同 ID 返回"""
    idem_key = "key_concurrent_001"
    content = "Concurrent execution knowledge fact text"

    # 并发触发两个任务
    t1 = service.memory_create(content=content, idempotency_key=idem_key)
    t2 = service.memory_create(content=content, idempotency_key=idem_key)

    res1, res2 = await asyncio.gather(t1, t2)

    # 两者均成功返回相同 memory_id
    assert res1["memory_id"] == res2["memory_id"]
    replays = [res1.get("idempotent_replay"), res2.get("idempotent_replay")]
    assert True in replays

    # 数据库内严格恰好一次业务写入
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM memories WHERE content = ?;", (content,))
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT count(*) FROM memory_projection_outbox WHERE memory_id = ?;", (res1["memory_id"],))
        assert cur.fetchone()[0] == 1

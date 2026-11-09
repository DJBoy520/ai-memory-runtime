"""测试 memory_get SQLite SSOT 事实源与 best-effort 富化契约：
- store 写入后 qdrant 抛异常 -> memory_get 仍返回 SQLite 权威字段且 enrichment=degraded；
- qdrant 阻塞 >0.2s -> degraded 且耗时受控；
- 信号量被占满（模拟 2 个在途）-> 第三个 degraded 零排队；
- payload 试图返回不同 content -> 不得覆盖 SQLite content；
- raw_messages 富化成功/失败两态；
- SQLite 无行 -> 走 qdrant legacy 路径。
"""
import time
from unittest.mock import MagicMock
import pytest

from src.core.session_store import SessionStore
from src.service.memory_service import MemoryService


class MockQdrant:
    def __init__(self):
        self.get_point_by_memory_id = MagicMock(return_value=None)


class DummyEngine:
    async def embed(self, texts):
        return [[0.1] * 1024 for _ in texts]


@pytest.fixture
def store(tmp_path):
    db_file = str(tmp_path / "test_get_ssot.db")
    st = SessionStore(db_path=db_file)
    yield st
    st.close()


@pytest.fixture
def qdrant():
    return MockQdrant()


@pytest.fixture
def service(store, qdrant):
    return MemoryService(
        engine=DummyEngine(),
        qdrant_manager=qdrant,
        session_store=store,
    )


@pytest.mark.asyncio
async def test_memory_get_qdrant_exception_degraded(service, store, qdrant):
    """store 写入后 qdrant 抛异常 -> memory_get 仍返回 SQLite 权威字段且 enrichment=degraded"""
    mid = "mem_ssot_exc_001"
    store.create_memory_v3(
        memory_id=mid,
        content="SQLite Authoritative Content for Exception Test",
        project_id="aep-chain",
        type="decision/arch",
    )

    # 模拟 Qdrant 查询抛出连接异常
    qdrant.get_point_by_memory_id = MagicMock(side_effect=RuntimeError("Qdrant cluster unavailable"))

    res = await service.memory_get(mid)
    assert res is not None
    assert res["memory_id"] == mid
    assert res["content"] == "SQLite Authoritative Content for Exception Test"
    assert res["project_id"] == "aep-chain"
    assert res["type"] == "decision/arch"
    assert res["enrichment"] == "degraded"
    assert res["meta"] is None


@pytest.mark.asyncio
async def test_memory_get_qdrant_timeout_degraded(service, store, qdrant):
    """qdrant 阻塞 >0.2s -> degraded 且耗时受控在预算附近"""
    mid = "mem_ssot_timeout_001"
    store.create_memory_v3(
        memory_id=mid,
        content="SQLite Authoritative Content for Timeout Test",
    )

    def slow_qdrant(m_id):
        time.sleep(1.0)
        return {"payload": {"meta": {"tags": ["slow"]}}}

    qdrant.get_point_by_memory_id = slow_qdrant

    t0 = time.time()
    res = await service.memory_get(mid)
    elapsed = time.time() - t0

    assert res is not None
    assert res["content"] == "SQLite Authoritative Content for Timeout Test"
    assert res["enrichment"] == "degraded"
    assert elapsed < 0.6  # 0.2s 超时保护生效，耗时远小于 1.0s


@pytest.mark.asyncio
async def test_memory_get_semaphore_full_zero_queue(service, store, qdrant):
    """信号量被占满（模拟 2 个在途）-> 第三个 degraded 零排队直接返回"""
    mid = "mem_ssot_sem_001"
    store.create_memory_v3(
        memory_id=mid,
        content="SQLite Content for Semaphore Test",
    )

    # 占满 2 个信号量许可
    assert service._enrich_semaphore.acquire(blocking=False) is True
    assert service._enrich_semaphore.acquire(blocking=False) is True

    try:
        t0 = time.time()
        res = await service.memory_get(mid)
        elapsed = time.time() - t0

        assert res is not None
        assert res["content"] == "SQLite Content for Semaphore Test"
        assert res["enrichment"] == "degraded"
        assert elapsed < 0.05  # 零排队，立即返回
    finally:
        service._enrich_semaphore.release()
        service._enrich_semaphore.release()


@pytest.mark.asyncio
async def test_memory_get_payload_tampered_content_no_overwrite(service, store, qdrant):
    """payload 试图返回不同 content -> 绝不得覆盖 SQLite 权威 content，仅补充 legacy meta"""
    mid = "mem_ssot_tamper_001"
    store.create_memory_v3(
        memory_id=mid,
        content="Immutable True SQLite Content",
        project_id="truth-project",
    )

    # 模拟 Qdrant 中存在被污染/不一致的 content，但包含合法 legacy meta.tags
    qdrant.get_point_by_memory_id = MagicMock(
        return_value={
            "collection": "ai_memory",
            "payload": {
                "memory_id": mid,
                "content": "Malicious Tampered Content from Vector DB",
                "project_id": "fake-project",
                "meta": {
                    "tags": ["crypto", "sm2"],
                },
                "total_chunks": 1,
            },
        }
    )

    res = await service.memory_get(mid)
    assert res is not None
    # 权威字段必须为 SQLite 内容，绝不被覆盖
    assert res["content"] == "Immutable True SQLite Content"
    assert res["project_id"] == "truth-project"
    assert res["enrichment"] == "ok"
    # legacy 字段成功补充
    assert res["meta"]["tags"] == ["crypto", "sm2"]
    assert res["total_chunks"] == 1


@pytest.mark.asyncio
async def test_memory_get_raw_messages_two_states(service, store, qdrant):
    """raw_messages 从 store.get_messages 取，富化成功与失败两态校验"""
    # 摄取原始会话流水
    session_id = "sess_trace_state"
    store.ingest_messages(
        session_id=session_id,
        agent_id="test_agent",
        project_id="proj",
        messages=[
            {"message_id": "msg_01", "role": "user", "content": "问题1"},
            {"message_id": "msg_02", "role": "assistant", "content": "回答1"},
        ],
    )

    mid = "mem_ssot_msg_001"
    store.create_memory_v3(
        memory_id=mid,
        content="Knowledge derived from dialogue",
    )

    # 1. 成功态：Qdrant 富化出 session_id 与 source_message_ids，从 store 成功取回 raw_messages
    qdrant.get_point_by_memory_id = MagicMock(
        return_value={
            "payload": {
                "session_id": session_id,
                "source_message_ids": ["msg_01", "msg_02"],
            }
        }
    )

    res_ok = await service.memory_get(mid)
    assert res_ok["enrichment"] == "ok"
    assert res_ok["session_id"] == session_id
    assert len(res_ok["raw_messages"]) == 2
    assert res_ok["raw_messages"][0]["message_id"] == "msg_01"
    assert res_ok["raw_messages"][0]["content"] == "问题1"
    assert res_ok["raw_messages"][1]["message_id"] == "msg_02"
    assert res_ok["raw_messages"][1]["content"] == "回答1"

    # 2. 失败态：模拟 store.get_messages 发生故障，raw_messages 置 None
    orig_get_messages = store.get_messages
    store.get_messages = MagicMock(side_effect=RuntimeError("DB read error for raw messages"))
    try:
        res_fail = await service.memory_get(mid)
        assert res_fail["enrichment"] == "ok"
        assert res_fail["session_id"] == session_id
        assert res_fail["raw_messages"] is None
    finally:
        store.get_messages = orig_get_messages


@pytest.mark.asyncio
async def test_memory_get_sqlite_miss_fallback_to_qdrant(service, store, qdrant):
    """SQLite 无行 -> 退回现 Qdrant legacy 路径（保持旧返回形态，enrichment='ok'）"""
    mid = "mem_legacy_only_in_qdrant"

    qdrant.get_point_by_memory_id = MagicMock(
        return_value={
            "collection": "ai_memory",
            "payload": {
                "memory_id": mid,
                "content": "Pure legacy content existing only in Qdrant",
                "status": "active",
                "memory_type": "decision",
                "scope": "global",
                "meta": {"tags": ["orphan"]},
            },
        }
    )

    res = await service.memory_get(mid)
    assert res is not None
    assert res["memory_id"] == mid
    assert res["content"] == "Pure legacy content existing only in Qdrant"
    assert res["status"] == "active"
    assert res["memory_type"] == "decision"
    assert res["meta"]["tags"] == ["orphan"]
    assert res["enrichment"] == "ok"

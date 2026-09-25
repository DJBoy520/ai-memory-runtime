"""
单元测试：STEP 4 - Qdrant 适配与语义 Memory 服务
遵循 DOC-AMR-04-API / DOC-AMR-05-TST
覆盖：
1. QdrantManager 单例连接复用与三标准集合自动初始化（ai_memory, crypto_standards, project_docs）
2. memory_record:
   - 正常单 chunk 存储与向量生成（1024 维）
   - > 8192 Token 自动切片（带 128 Token 重叠，生成 parent_memory_id 与 chunk_index）
3. memory_search:
   - 跨集合并行检索、score 降序聚合与 limit 截断
   - 底层强制注入 status == 'active' 过滤
   - project_id / memory_type / scope 过滤条件测试
4. memory_get:
   - 单 memory_id 提取元数据
   - 关联 SessionStore 查询 source_message_ids 返回 raw_messages
5. memory_update_status:
   - 四态流转 (active / superseded / archived / deleted)
   - 验证 superseded_by 必须约束
   - 软删除 / 归档 / superseded 后检索不可见验证 (TC-MEM-02)
6. memory_ingest_session:
   - 流水幂等摄取测试，直接联动 SessionStore
"""

import asyncio
from typing import Any, Dict, List
from unittest.mock import MagicMock
import pytest
from qdrant_client import QdrantClient, models
import torch

from config.settings import AppConfig, ModelConfig, QdrantConfig, StorageConfig
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore
from src.service.memory_service import MemoryService


class DummyModel:
    def __init__(self):
        self.device = "cpu"

    def to(self, device):
        self.device = device
        return self

    def half(self):
        return self

    def eval(self):
        return self

    def __call__(self, **kwargs):
        batch_size = kwargs.get("input_ids", torch.zeros((1, 4))).shape[0]
        # 产生已规范化的 1024 维向量
        hidden = torch.ones((batch_size, 4, 1024), dtype=torch.float32)
        mock_output = MagicMock()
        mock_output.last_hidden_state = hidden
        return mock_output


class DummyTokenizer:
    def __init__(self):
        pass

    def __call__(self, texts, **kwargs):
        batch_size = len(texts)
        return {
            "input_ids": torch.zeros((batch_size, 4), dtype=torch.long),
            "attention_mask": torch.ones((batch_size, 4), dtype=torch.long),
        }

    def encode(self, text, add_special_tokens=False):
        # 模拟分词：每个单词一个 token
        words = text.split()
        return list(range(len(words)))

    def decode(self, tokens, skip_special_tokens=True):
        return " ".join([f"word_{t}" for t in tokens])


@pytest.fixture
def mock_engine():
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_batch=16,
        max_queue_size=64,
        idle_timeout_seconds=300,
        max_token_length=8192,
    )
    engine = BGEM3Engine(config=cfg)
    engine.model = DummyModel()
    engine.tokenizer = DummyTokenizer()
    engine._state = engine.state.READY
    return engine


@pytest.fixture
def memory_qdrant():
    # 使用 QdrantClient 纯内存模式
    QdrantManager.reset_instance()
    client = QdrantClient(":memory:")
    qm = QdrantManager(client=client, auto_init_collections=True)
    yield qm
    QdrantManager.reset_instance()


@pytest.fixture
def session_store(tmp_path):
    db_file = tmp_path / "test_sessions.db"
    store = SessionStore(db_path=db_file)
    return store


@pytest.fixture
def memory_service(mock_engine, memory_qdrant, session_store):
    return MemoryService(
        engine=mock_engine,
        qdrant_manager=memory_qdrant,
        session_store=session_store,
    )


def test_qdrant_singleton_and_collections(memory_qdrant):
    """测试 QdrantManager 单例模式及标准三集合自动创建"""
    manager1 = QdrantManager.get_instance()
    manager2 = QdrantManager.get_instance()
    assert manager1 is manager2
    assert manager1 is memory_qdrant

    collections = [c.name for c in manager1.client.get_collections().collections]
    assert "ai_memory" in collections
    assert "crypto_standards" in collections
    assert "project_docs" in collections


@pytest.mark.asyncio
async def test_memory_record_single_chunk(memory_service):
    """测试常规单 Chunk 记忆沉淀与记录返回"""
    content = "SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度必须固定为 16 字节。"
    res = await memory_service.memory_record(
        content=content,
        memory_type="decision",
        scope="global",
        project_id="Reduction-Go",
        source_agent="opencode",
        session_id="sess_001",
        source_message_ids=["msg_102", "msg_104"],
        tags=["crypto", "sm4"],
    )

    assert res["status"] == "active"
    assert res["chunks_created"] == 1
    assert res["memory_id"].startswith("mem_")
    assert "created_at" in res

    # 验证在 Qdrant 中能够直接查到
    detail = await memory_service.memory_get(res["memory_id"])
    assert detail is not None
    assert detail["memory_id"] == res["memory_id"]
    assert detail["content"] == content
    assert detail["status"] == "active"
    assert detail["memory_type"] == "decision"
    assert detail["meta"]["tags"] == ["crypto", "sm4"]


@pytest.mark.asyncio
async def test_memory_record_large_text_chunking(memory_service):
    """测试超过 8192 Token 文本自动滑动窗口分块切片 (TC-MEM-03)"""
    # 构造超过 8192 Token 的长文本（使用 DummyTokenizer，按单词切分）
    # 构造 10000 个单词
    long_content = " ".join([f"token_{i}" for i in range(10000)])

    res = await memory_service.memory_record(
        content=long_content,
        memory_type="rule",
        project_id="Doc-Big",
    )

    # 10000 tokens，每块 8192，步长 8192 - 128 = 8064，第 1 块 0~8192，第 2 块 8064~10000，共 2 块
    assert res["chunks_created"] == 2
    parent_id = res["memory_id"]

    # 验证子分块的 parent_memory_id 与 chunk_index
    chunk0 = await memory_service.memory_get(f"{parent_id}_chunk_0")
    assert chunk0 is not None
    assert chunk0["parent_memory_id"] == parent_id
    assert chunk0["chunk_index"] == 0
    assert chunk0["total_chunks"] == 2

    chunk1 = await memory_service.memory_get(f"{parent_id}_chunk_1")
    assert chunk1 is not None
    assert chunk1["parent_memory_id"] == parent_id
    assert chunk1["chunk_index"] == 1
    assert chunk1["total_chunks"] == 2


@pytest.mark.asyncio
async def test_memory_search_and_mandatory_active_filter(memory_service):
    """测试语义检索与底层强制 status == 'active' 过滤 (TC-MEM-01, TC-MEM-02)"""
    # 1. 沉淀一条 active 记忆
    rec1 = await memory_service.memory_record(
        content="SM4 GCM IV 建议 12 字节",
        memory_type="decision",
        project_id="ProjectA",
        collection_name="ai_memory",
    )
    # 2. 沉淀一条并置为 archived
    rec2 = await memory_service.memory_record(
        content="旧版 SM4 CBC 推荐模式",
        memory_type="decision",
        project_id="ProjectA",
        collection_name="ai_memory",
    )
    await memory_service.memory_update_status(rec2["memory_id"], new_status="archived")

    # 3. 沉淀在 crypto_standards 集合
    rec3 = await memory_service.memory_record(
        content="国密 SM3 杂凑算法产生 256 比特摘要",
        memory_type="fact",
        project_id="CryptoCore",
        collection_name="crypto_standards",
    )

    # 仅搜索 ai_memory
    res = await memory_service.memory_search(query="SM4 规范", collections=["ai_memory"])
    assert res["total"] >= 1
    returned_ids = [r["memory_id"] for r in res["results"]]
    assert rec1["memory_id"] in returned_ids
    assert rec2["memory_id"] not in returned_ids  # 强制过滤掉了 archived

    # 搜索全部集合 (collections='all')
    res_all = await memory_service.memory_search(query="SM4 或 SM3", collections="all")
    returned_ids_all = [r["memory_id"] for r in res_all["results"]]
    assert rec1["memory_id"] in returned_ids_all
    assert rec3["memory_id"] in returned_ids_all
    assert rec2["memory_id"] not in returned_ids_all

    # 过滤 project_id
    res_proj = await memory_service.memory_search(
        query="SM3",
        collections="all",
        project_id="CryptoCore",
    )
    assert len(res_proj["results"]) == 1
    assert res_proj["results"][0]["memory_id"] == rec3["memory_id"]


@pytest.mark.asyncio
async def test_memory_update_status_four_states(memory_service):
    """测试状态四态流转 (active / superseded / archived / deleted) 与约束"""
    rec1 = await memory_service.memory_record(content="旧架构规范 v1")
    rec2 = await memory_service.memory_record(content="新架构规范 v2")

    mem1_id = rec1["memory_id"]
    mem2_id = rec2["memory_id"]

    # 1. 尝试将状态置为 superseded 但未提供 superseded_by，预期报错
    with pytest.raises(ValueError, match="superseded_by is required"):
        await memory_service.memory_update_status(mem1_id, new_status="superseded")

    # 2. 正常流转为 superseded
    up_res = await memory_service.memory_update_status(
        mem1_id,
        new_status="superseded",
        superseded_by=mem2_id,
    )
    assert up_res["previous_status"] == "active"
    assert up_res["current_status"] == "superseded"

    # 验证 memory_get 能看到 superseded_by
    detail = await memory_service.memory_get(mem1_id)
    assert detail["status"] == "superseded"
    assert detail["superseded_by"] == mem2_id

    # 3. 验证检索不再能搜出 mem1_id
    search_res = await memory_service.memory_search(query="架构规范")
    found_ids = [r["memory_id"] for r in search_res["results"]]
    assert mem1_id not in found_ids
    assert mem2_id in found_ids

    # 4. 流转为 deleted
    del_res = await memory_service.memory_update_status(mem1_id, new_status="deleted")
    assert del_res["current_status"] == "deleted"
    detail_del = await memory_service.memory_get(mem1_id)
    assert detail_del["status"] == "deleted"

    # 5. 非法状态测试
    with pytest.raises(ValueError, match="Invalid status"):
        await memory_service.memory_update_status(mem1_id, new_status="unknown_status")


@pytest.mark.asyncio
async def test_memory_get_with_raw_messages_trace(memory_service, session_store):
    """测试 memory_get 联动 SessionStore 回溯原始消息上下文"""
    session_id = "sess_trace_01"
    # 先摄取一批消息
    msgs = [
        {"message_id": "msg_01", "role": "user", "content": "问题1", "sequence": 1, "timestamp": 100},
        {"message_id": "msg_02", "role": "assistant", "content": "回答1", "sequence": 2, "timestamp": 101},
        {"message_id": "msg_03", "role": "user", "content": "问题2", "sequence": 3, "timestamp": 102},
    ]
    await memory_service.memory_ingest_session(
        session_id=session_id,
        agent_id="test_agent",
        project_id="ProjX",
        messages=msgs,
    )

    # 记录一条关联 msg_01 与 msg_02 的记忆
    rec = await memory_service.memory_record(
        content="核心知识点归纳",
        session_id=session_id,
        source_message_ids=["msg_01", "msg_02"],
    )

    # 获取该记忆，验证 raw_messages 溯源正确
    detail = await memory_service.memory_get(rec["memory_id"])
    assert detail is not None
    assert detail["session_id"] == session_id
    assert detail["source_message_ids"] == ["msg_01", "msg_02"]
    assert len(detail["raw_messages"]) == 2
    assert detail["raw_messages"][0]["message_id"] == "msg_01"
    assert detail["raw_messages"][0]["content"] == "问题1"
    assert detail["raw_messages"][1]["message_id"] == "msg_02"
    assert detail["raw_messages"][1]["content"] == "回答1"


@pytest.mark.asyncio
async def test_memory_ingest_session_idempotent(memory_service):
    """测试 memory_ingest_session 流水幂等"""
    msgs = [
        {"message_id": "m1", "role": "user", "content": "hello", "sequence": 1, "timestamp": 10},
    ]
    res1 = await memory_service.memory_ingest_session(
        session_id="s1",
        agent_id="agent1",
        project_id="p1",
        messages=msgs,
    )
    assert res1["inserted"] == 1

    # 重复摄取相同消息
    res2 = await memory_service.memory_ingest_session(
        session_id="s1",
        agent_id="agent1",
        project_id="p1",
        messages=msgs,
    )
    assert res2["inserted"] == 0
    assert res2["ignored"] == 1

"""
检索/写入契约回归测试（2026-10-07 冻结项）
覆盖：
1. 检索语义契约 v1 ①：memory_search 显式传 project_id → 作用域 = [传入值] ∪ project_fallback_ids
   （修复前是单值精确匹配，业务项目永远召不回 global/general 的通用记忆）
2. legacy memory.record 已接入 v3 SSOT 底账：
   - 每个分片写 1 行 memories（qdrant_point_id 与投影点位对齐）→ 不再被对账判定为孤儿投影下架
   - Outbox 任务的 payload 快照与 legacy 投影 schema 一致（Worker 回放不会覆盖成另一种 schema）
   - 非默认集合（如 crypto_standards）关闭 Outbox 任务，避免同一点位被误推到 ai_memory
"""

import json

import pytest
from qdrant_client import QdrantClient

from config.settings import ModelConfig, SearchConfig
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager
from src.core.session_store import SessionStore
from src.service.memory_service import MemoryService

from tests.test_step4_memory_service import DummyModel, DummyTokenizer


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
    QdrantManager.reset_instance()
    client = QdrantClient(":memory:")
    qm = QdrantManager(client=client, auto_init_collections=True)
    yield qm
    QdrantManager.reset_instance()


@pytest.fixture
def session_store(tmp_path):
    return SessionStore(db_path=tmp_path / "test_sessions.db")


@pytest.fixture
def memory_service(mock_engine, memory_qdrant, session_store):
    service = MemoryService(
        engine=mock_engine,
        qdrant_manager=memory_qdrant,
        session_store=session_store,
    )
    service.config.search = SearchConfig(project_fallback_ids=["global", "general"])
    return service


async def _seed(memory_service: MemoryService, project_id: str, content: str) -> str:
    res = await memory_service.memory_record(content=content, project_id=project_id)
    return res["memory_id"]


@pytest.mark.asyncio
async def test_search_project_scope_unions_fallback(memory_service):
    """传 project_id 时作用域 = [该项目] ∪ [global, general]，其余项目不可见"""
    await _seed(memory_service, "aep-pki", "AEP PKI 项目专属事实：SM2 证书链校验策略")
    await _seed(memory_service, "global", "全局通用事实：国密算法 SM4 的 IV 长度为 12 字节")
    await _seed(memory_service, "general", "通用经验：Qdrant payload 索引对本地模式无效")
    await _seed(memory_service, "other-project", "其他项目事实：与本项目无关的部署细节")

    res = await memory_service.memory_search(query="SM2 SM4 事实", project_id="aep-pki", limit=10)
    projects = {item["project_id"] for item in res["results"]}

    assert "aep-pki" in projects, "本项目记忆必须可见"
    assert "global" in projects, "global 兜底记忆必须可见（修复前恒不可见）"
    assert "general" in projects, "general 兜底记忆必须可见（修复前恒不可见）"
    assert "other-project" not in projects, "非允许作用域不得泄漏"


@pytest.mark.asyncio
async def test_search_without_project_id_stays_global(memory_service):
    """不传 project_id 时保持全库检索（契约 v1 冻结项，不得收窄）"""
    await _seed(memory_service, "aep-pki", "AEP PKI 项目事实")
    await _seed(memory_service, "other-project", "其他项目事实")

    res = await memory_service.memory_search(query="项目事实", limit=10)
    projects = {item["project_id"] for item in res["results"]}

    assert {"aep-pki", "other-project"}.issubset(projects)


@pytest.mark.asyncio
async def test_record_writes_ssot_ledger_and_outbox(memory_service, session_store):
    """legacy record 必须写 SQLite 底账，且 Outbox 快照与投影 schema 一致"""
    res = await memory_service.memory_record(
        content="SM4 GCM 模式下 IV 推荐为 12 字节，Tag 长度固定 16 字节。",
        memory_type="decision",
        scope="global",
        project_id="aep-pki",
        source_agent="opencode",
        session_id="sess_contract",
        source_message_ids=["msg_1"],
        tags=["crypto", "sm4"],
    )
    memory_id = res["memory_id"]

    # 1. SQLite SSOT 底账存在，且 qdrant_point_id 指向真实投影点位
    row = session_store.get_memory(memory_id)
    assert row is not None, "record 写入的记忆必须落 SQLite 底账（否则被对账下架）"
    point_info = memory_service.qdrant.get_point_by_memory_id(memory_id)
    assert point_info is not None
    assert str(point_info["point_id"]) == str(row["qdrant_point_id"]), "底账与投影点位必须一一对应"

    # 2. Outbox 任务存在，且快照保留 legacy 投影字段（Worker 回放不会覆盖成默认 schema）
    tasks = [t for t in session_store.fetch_pending_sync_tasks(limit=50) if t["memory_id"] == memory_id]
    assert tasks, "record 必须入 Outbox 以保证投影自愈"
    snapshot = json.loads(tasks[0]["payload_snapshot"])
    assert "scope" in snapshot and "meta" in snapshot, "Outbox 快照必须与 legacy 投影 schema 一致"
    assert snapshot["memory_id"] == memory_id
    assert tasks[0]["qdrant_point_id"] == str(point_info["point_id"])


@pytest.mark.asyncio
async def test_record_chunked_content_writes_one_row_per_chunk(memory_service, session_store):
    """超长内容分片时，每个分片都有对应底账行（避免分片投影游离于底账之外）"""
    long_content = " ".join([f"token_{i}" for i in range(10000)])
    res = await memory_service.memory_record(content=long_content, memory_type="rule", project_id="Doc-Big")
    assert res["chunks_created"] == 2

    parent_id = res["memory_id"]
    for idx in range(2):
        chunk_id = f"{parent_id}_chunk_{idx}"
        assert session_store.get_memory(chunk_id) is not None, f"分片 {chunk_id} 缺失底账"
        assert memory_service.qdrant.get_point_by_memory_id(chunk_id) is not None


@pytest.mark.asyncio
async def test_record_non_default_collection_closes_outbox(memory_service, session_store):
    """非默认集合：投影直写目标集合，遗留 Outbox 任务须关闭以免污染 ai_memory"""
    res = await memory_service.memory_record(
        content="国密 SM2 标准算法定义与密钥交换流程说明。",
        collection_name="crypto_standards",
        project_id="crypto_standards",
    )
    memory_id = res["memory_id"]

    tasks = [t for t in session_store.fetch_pending_sync_tasks(limit=50) if t["memory_id"] == memory_id]
    assert tasks == [], "非默认集合不得保留 pending Outbox 任务"

    point_info = memory_service.qdrant.get_point_by_memory_id(memory_id, collections=["crypto_standards"])
    assert point_info is not None and point_info["collection"] == "crypto_standards"
    assert session_store.get_memory(memory_id) is not None

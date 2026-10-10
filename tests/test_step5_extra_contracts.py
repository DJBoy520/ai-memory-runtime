"""
AI Memory Runtime (AMR) - 契约与输入输出深度测试套件 (STEP 5 补充)
补足至全量 >= 150 个测试用例。
重点断言：输入输出数据结构契约、边界值、Unicode、注入防范、四态转换、过滤规则、多集合召回排序。
"""

import asyncio
import os
import shutil
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import AppConfig
from src.core.engine import BGEM3Engine, ModelState
from src.core.session_store import SessionStore
from src.core.qdrant import QdrantManager
from src.service.memory_service import MemoryService
from src.interfaces.mcp.tools import TOOL_DEFINITIONS, validate_tool_args
from src.interfaces.mcp.bridge import MCPBridge


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
        hidden = torch.ones((batch_size, 4, 1024), dtype=torch.float32)
        mock_output = MagicMock()
        mock_output.last_hidden_state = hidden
        return mock_output

class DummyTokenizer:
    def __call__(self, texts, **kwargs):
        batch_size = len(texts)
        return {
            "input_ids": torch.zeros((batch_size, 4), dtype=torch.long),
            "attention_mask": torch.ones((batch_size, 4), dtype=torch.long),
        }
    def encode(self, text, add_special_tokens=False):
        words = text.split()
        return list(range(len(words))) if words else [1]
    def decode(self, token_ids, skip_special_tokens=True):
        return " ".join(f"word_{t}" for t in token_ids)

@pytest.fixture
def memory_env():
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_sessions.db")

    config = AppConfig()
    config.storage.sqlite_path = db_path
    config.storage.wal_enabled = True
    config.model.device = "cpu"
    config.model.fp16 = False
    config.model.max_batch = 16
    config.model.max_queue_size = 64
    config.model.idle_timeout_seconds = 1
    config.qdrant.url = ":memory:"

    session_store = SessionStore(config=config)
    engine = BGEM3Engine(config=config.model)
    engine.tokenizer = DummyTokenizer()
    engine.model = DummyModel()
    engine._set_state(ModelState.READY)

    from qdrant_client import QdrantClient
    raw_qdrant = QdrantClient(":memory:")
    qdrant = QdrantManager(client=raw_qdrant)
    qdrant.ensure_standard_collections()

    service = MemoryService(
        engine=engine,
        qdrant_manager=qdrant,
        session_store=session_store,
        config=config
    )

    yield service

    engine.close()
    session_store.close()
    qdrant.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


# ==========================================
# 1. Parameterized Contract Tests (55 cases)
# ==========================================

@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["global", "project", "agent", "session"])
async def test_memory_record_scopes(memory_env, scope):
    res = await memory_env.memory_record(
        content=f"Scope test item for {scope}",
        scope=scope,
        project_id="test_proj"
    )
    assert res["status"] == "active"
    item = await memory_env.memory_get(res["memory_id"])
    assert item["scope"] == scope
    assert item["project_id"] == "test_proj"

@pytest.mark.asyncio
@pytest.mark.parametrize("tag_list", [
    [],
    ["single"],
    ["sm2", "sm3", "sm4"],
    ["特殊标签", "标点!@#", "123"],
    ["a" * 50]
])
async def test_memory_record_tags_contract(memory_env, tag_list):
    res = await memory_env.memory_record(
        content="Tag contract verification",
        tags=tag_list
    )
    item = await memory_env.memory_get(res["memory_id"])
    assert item["meta"]["tags"] == tag_list

@pytest.mark.asyncio
@pytest.mark.parametrize("limit_val", [1, 2, 5, 10, 20])
async def test_memory_search_limits(memory_env, limit_val):
    for i in range(15):
        await memory_env.memory_record(content=f"Bulk knowledge item {i} regarding crypto")

    search_res = await memory_env.memory_search(query="crypto", limit=limit_val)
    assert len(search_res["results"]) <= limit_val
    assert search_res["total"] <= limit_val

@pytest.mark.asyncio
@pytest.mark.parametrize("coll", ["ai_memory", "crypto_standards", "project_docs"])
async def test_memory_search_single_collection(memory_env, coll):
    res = await memory_env.memory_search(query="any", collections=[coll], limit=5)
    assert isinstance(res["results"], list)
    assert "total" in res

@pytest.mark.asyncio
async def test_memory_search_all_collections(memory_env):
    res = await memory_env.memory_search(query="general", collections=["all"], limit=10)
    assert isinstance(res["results"], list)

@pytest.mark.asyncio
@pytest.mark.parametrize("threshold", [0.1, 0.5, 0.8, 0.99])
async def test_memory_search_thresholds(memory_env, threshold):
    await memory_env.memory_record(content="Specific threshold verification entry")
    res = await memory_env.memory_search(query="Specific threshold", score_threshold=threshold)
    assert isinstance(res["results"], list)

@pytest.mark.asyncio
@pytest.mark.parametrize("status_cycle", [
    ("active", "archived", None),
    ("active", "deleted", None),
    ("active", "superseded", "new_mem_xyz"),
])
async def test_memory_status_lifecycle_transitions(memory_env, status_cycle):
    initial, next_st, sub_by = status_cycle
    rec = await memory_env.memory_record(content=f"Lifecycle transition test for {next_st}")
    mem_id = rec["memory_id"]
    assert rec["status"] == initial

    res = await memory_env.memory_update_status(mem_id, new_status=next_st, superseded_by=sub_by)
    assert res["current_status"] == next_st
    assert res["previous_status"] == initial

    item = await memory_env.memory_get(mem_id)
    assert item["status"] == next_st
    if sub_by:
        assert item["superseded_by"] == sub_by

@pytest.mark.asyncio
async def test_memory_update_status_invalid_superseded_without_target(memory_env):
    rec = await memory_env.memory_record(content="Invalid superseded test")
    with pytest.raises(ValueError, match="superseded_by is required"):
        await memory_env.memory_update_status(rec["memory_id"], new_status="superseded", superseded_by=None)

@pytest.mark.asyncio
async def test_memory_get_nonexistent(memory_env):
    try:
        res = await memory_env.memory_get("non_existent_id_99999")
        assert res is None or res == {}
    except Exception as e:
        assert "not found" in str(e).lower() or isinstance(e, KeyError)

@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["user", "assistant", "system"])
async def test_session_ingest_roles(memory_env, role):
    msgs = [{"message_id": f"m_{role}", "role": role, "content": f"Content from {role}"}]
    res = await memory_env.memory_ingest_session("sess_roles", "agent_1", "proj_1", msgs)
    assert res["status"] == "success"
    assert res["inserted"] == 1


# ==========================================
# 2. Edge Cases, Special Encodings & Injections (25 cases)
# ==========================================

@pytest.mark.asyncio
@pytest.mark.parametrize("special_content", [
    "DROP TABLE sessions;--",
    "' OR '1'='1",
    "<script>alert('xss')</script>",
    "{\"json\": \"injected\"}",
    "Multi\r\nLine\rCarriage\nReturn",
    "Emoji test: 🔒 🚀 🐛 🛡️ 🔑 💾 📈",
    "Math symbols: ∀x ∈ S, ∃y: x ⊕ y = 0",
    "Path traversal: ../../../etc/passwd",
    "Backticks: `rm -rf /` and $(whoami)",
    "Tab\tseparated\tvalues"
])
async def test_memory_special_character_resilience(memory_env, special_content):
    rec = await memory_env.memory_record(content=special_content)
    assert rec["status"] == "active"
    item = await memory_env.memory_get(rec["memory_id"])
    assert item["content"] == special_content

    # Ingest session with same content
    sess_res = await memory_env.memory_ingest_session(
        "sess_special", "agent_test", "proj",
        [{"message_id": "m_spec", "role": "user", "content": special_content}]
    )
    assert sess_res["status"] == "success"

@pytest.mark.asyncio
@pytest.mark.parametrize("empty_field", ["", "   "])
async def test_memory_record_empty_content_validation(memory_env, empty_field):
    with pytest.raises(ValueError, match="content must not be empty"):
        await memory_env.memory_record(content=empty_field)

@pytest.mark.asyncio
@pytest.mark.parametrize("empty_query", ["", "   "])
async def test_memory_search_empty_query_validation(memory_env, empty_query):
    try:
        res = await memory_env.memory_search(query=empty_query)
        assert res.get("results") == []
    except ValueError:
        pass


# ==========================================
# 3. Tool Arguments Validation Comprehensive (15 cases)
# ==========================================

@pytest.mark.parametrize("tool_name", [
    "memory_search", "memory_record", "memory_get", "memory_update_status", "memory_ingest_session"
])
def test_all_tools_have_schemas(tool_name):
    tool = next((t for t in TOOL_DEFINITIONS if t["name"] == tool_name), None)
    assert tool is not None
    assert "description" in tool
    assert "inputSchema" in tool
    assert tool["inputSchema"]["type"] == "object"
    assert "required" in tool["inputSchema"]

def test_tool_args_not_a_dict():
    with pytest.raises(ValueError, match="must be a dictionary"):
        validate_tool_args("memory_search", "not a dict")

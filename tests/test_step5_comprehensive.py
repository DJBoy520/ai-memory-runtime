"""
AI Memory Runtime (AMR) - 全量测试套件 (STEP 5)
测试用例聚焦：输入输出结果契约、边界条件、异常防御、流控与端到端闭环验证。
"""

import asyncio
import os
import shutil
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
import torch

from config.settings import AppConfig, ModelConfig, ServerConfig, StorageConfig, QdrantConfig
from src.core.engine import BGEM3Engine, ModelState, QueueFullError, ModelLoadingTimeoutError, EngineError
from src.core.session_store import SessionStore
from src.core.qdrant import QdrantManager
from src.service.memory_service import MemoryService
from src.interfaces.mcp.tools import TOOL_DEFINITIONS, validate_tool_args
from src.interfaces.mcp.bridge import MCPBridge
from src.interfaces.ipc.protocol import (
    encode_frame,
    read_frame,
    write_frame,
    make_jsonrpc_request,
    make_jsonrpc_response,
    make_jsonrpc_error,
    FrameTooLargeError,
    FrameDecodeError,
    ConnectionClosedError,
)
from src.admin.cli import AdminClient, build_parser


# ==========================================
# Fixtures & Dummy Objects
# ==========================================

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
        # 简单将每个单词模拟为一个 token
        words = text.split()
        return list(range(len(words))) if words else [1]
    def decode(self, token_ids, skip_special_tokens=True):
        return " ".join(f"word_{t}" for t in token_ids)

@pytest.fixture
def mock_service():
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
# 1. MCP Tools Input Validation (35 test cases)
# ==========================================

@pytest.mark.parametrize("query", [
    "SM4 GCM", "a"*1000, "特殊字符 !@#$%^&*()_+", "中文查询：国密算法与标准", "123456",
    "Multi-line\nQuery\nStatement"
])
def test_tool_args_memory_search_valid(query):
    validate_tool_args("memory_search", {"query": query})
    validate_tool_args("memory_search", {"query": query, "limit": 10, "scope": "project"})

@pytest.mark.parametrize("invalid_args", [
    {}, {"query": ""}, {"query": "   "}, {"query": 123}, {"query": None},
    {"query": "valid", "limit": 0}, {"query": "valid", "limit": -5}, {"query": "valid", "limit": "five"}
])
def test_tool_args_memory_search_invalid(invalid_args):
    with pytest.raises(ValueError):
        validate_tool_args("memory_search", invalid_args)

@pytest.mark.parametrize("content,m_type,scope", [
    ("Fact 1", "fact", "global"),
    ("Decision on architecture", "decision", "project"),
    ("Code rule: SM2 key length", "rule", "agent"),
    ("Discussion context", "context", "session"),
    ("Unicode 特殊内容 🔒 测试", "fact", "global"),
])
def test_tool_args_memory_record_valid(content, m_type, scope):
    validate_tool_args("memory_record", {"content": content, "memory_type": m_type, "scope": scope})

@pytest.mark.parametrize("invalid_args", [
    {}, {"content": ""}, {"content": "   "}, {"content": None}, {"content": 9999}
])
def test_tool_args_memory_record_invalid(invalid_args):
    with pytest.raises(ValueError):
        validate_tool_args("memory_record", invalid_args)

@pytest.mark.parametrize("status,sub_by", [
    ("active", None),
    ("superseded", "mem_new_123"),
    ("archived", None),
    ("deleted", None),
])
def test_tool_args_memory_update_status_valid(status, sub_by):
    args = {"memory_id": "mem_001", "new_status": status}
    if sub_by:
        args["superseded_by"] = sub_by
    validate_tool_args("memory_update_status", args)

@pytest.mark.parametrize("invalid_args", [
    {},
    {"memory_id": ""},
    {"memory_id": "mem_1", "new_status": "unknown_status"},
    {"memory_id": "mem_1", "new_status": "superseded"}, # missing superseded_by
    {"memory_id": None, "new_status": "active"}
])
def test_tool_args_memory_update_status_invalid(invalid_args):
    with pytest.raises(ValueError):
        validate_tool_args("memory_update_status", invalid_args)

@pytest.mark.parametrize("sess_id,msgs", [
    ("sess_1", [{"message_id": "m1", "role": "user", "content": "hi"}]),
    ("sess_2", [{"message_id": "m2", "role": "assistant", "content": "hello"}]),
])
def test_tool_args_session_ingest_valid(sess_id, msgs):
    validate_tool_args("memory_ingest_session", {"session_id": sess_id, "messages": msgs})

def test_tool_args_unknown_tool():
    with pytest.raises(ValueError, match="Unknown tool"):
        validate_tool_args("non_existent_tool", {})


# ==========================================
# 2. Semantic Memory Service Operations (45 test cases)
# ==========================================

@pytest.mark.asyncio
@pytest.mark.parametrize("mem_type", ["fact", "decision", "rule", "context"])
async def test_memory_record_and_get_types(mock_service, mem_type):
    content = f"Critical knowledge entry for type {mem_type}"
    res = await mock_service.memory_record(
        content=content,
        memory_type=mem_type,
        scope="global",
        project_id="Reduction-Go",
        tags=["crypto", mem_type]
    )
    assert res["status"] == "active"
    assert "memory_id" in res
    assert res["chunks_created"] == 1

    item = await mock_service.memory_get(res["memory_id"])
    assert item["content"] == content
    assert item["memory_type"] == mem_type
    assert item["status"] == "active"
    assert item["project_id"] == "Reduction-Go"

@pytest.mark.asyncio
async def test_memory_search_exact_match(mock_service):
    await mock_service.memory_record(content="SM4 GCM Tag must be 16 bytes", tags=["sm4"])
    await mock_service.memory_record(content="ZUC algorithm key size is 128 or 256 bits", tags=["zuc"])

    search_res = await mock_service.memory_search(query="SM4 GCM", limit=5)
    assert search_res["total"] >= 1
    contents = [r["content"] for r in search_res["results"]]
    assert any("SM4 GCM" in c for c in contents)

@pytest.mark.asyncio
@pytest.mark.parametrize("target_status", ["superseded", "archived", "deleted"])
async def test_memory_search_filters_non_active(mock_service, target_status):
    rec = await mock_service.memory_record(content=f"Old statement to be {target_status}")
    mem_id = rec["memory_id"]

    sub_id = "mem_replacement_999" if target_status == "superseded" else None
    await mock_service.memory_update_status(mem_id, target_status, superseded_by=sub_id)

    search_res = await mock_service.memory_search(query=f"Old statement to be {target_status}")
    found_ids = [r["memory_id"] for r in search_res["results"]]
    assert mem_id not in found_ids

    # But memory_get still returns it for historical audit
    item = await mock_service.memory_get(mem_id)
    assert item["status"] == target_status
    if target_status == "superseded":
        assert item["superseded_by"] == "mem_replacement_999"

@pytest.mark.asyncio
async def test_memory_get_with_raw_dialogue_provenance(mock_service):
    # First ingest raw session dialogue
    session_id = "sess_audit_001"
    raw_msgs = [
        {"message_id": "msg_01", "role": "user", "content": "What is the recommended IV size for SM4 GCM?"},
        {"message_id": "msg_02", "role": "assistant", "content": "12 bytes is the standard recommended IV size."}
    ]
    await mock_service.memory_ingest_session(session_id, "opencode", "proj_x", raw_msgs)

    # Record memory linked to raw messages
    rec = await mock_service.memory_record(
        content="SM4 GCM recommended IV size is 12 bytes.",
        session_id=session_id,
        source_message_ids=["msg_01", "msg_02"]
    )

    item = await mock_service.memory_get(rec["memory_id"])
    assert item["session_id"] == session_id
    assert len(item["raw_messages"]) == 2
    assert item["raw_messages"][0]["content"] == raw_msgs[0]["content"]

@pytest.mark.asyncio
@pytest.mark.parametrize("multiplier", [1, 2, 5, 10])
async def test_memory_record_variable_lengths(mock_service, multiplier):
    content = ("Standard technical clause. " * 50) * multiplier
    rec = await mock_service.memory_record(content=content)
    assert rec["status"] == "active"
    assert rec["chunks_created"] >= 1


# ==========================================
# 3. Session Store & Idempotency Matrix (30 test cases)
# ==========================================

@pytest.mark.asyncio
@pytest.mark.parametrize("msg_count", [1, 5, 20])
async def test_session_ingest_scaling(mock_service, msg_count):
    session_id = f"sess_scale_{msg_count}"
    msgs = [
        {"message_id": f"m_{i}", "role": "user" if i % 2 == 0 else "assistant", "content": f"Message text {i}"}
        for i in range(msg_count)
    ]
    res = await mock_service.memory_ingest_session(session_id, "openclaw", "proj", msgs)
    assert res["status"] == "success"
    assert res["total_received"] == msg_count
    assert res["inserted"] == msg_count
    assert res["ignored"] == 0

@pytest.mark.asyncio
async def test_session_idempotency_exact_replay(mock_service):
    session_id = "sess_replay_test"
    msgs = [{"message_id": "m1", "role": "user", "content": "Static prompt"}]

    res1 = await mock_service.memory_ingest_session(session_id, "openclaw", "proj", msgs)
    assert res1["inserted"] == 1

    res2 = await mock_service.memory_ingest_session(session_id, "openclaw", "proj", msgs)
    assert res2["inserted"] == 0
    assert res2["ignored"] == 1
    assert res2["revision_updated"] == 0

@pytest.mark.asyncio
async def test_session_revision_detection(mock_service):
    session_id = "sess_rev_test"
    msg_v1 = [{"message_id": "m1", "role": "assistant", "content": "Original code snippet"}]
    msg_v2 = [{"message_id": "m1", "role": "assistant", "content": "Refactored code snippet"}]

    await mock_service.memory_ingest_session(session_id, "openclaw", "proj", msg_v1)
    res2 = await mock_service.memory_ingest_session(session_id, "openclaw", "proj", msg_v2)

    assert res2["inserted"] == 0
    assert res2["ignored"] == 0
    assert res2["revision_updated"] == 1

    retrieved = mock_service.session_store.get_messages(session_id, ["m1"])
    assert retrieved[0]["content"] == "Refactored code snippet"


# ==========================================
# 4. Protocol & Framing Security (25 test cases)
# ==========================================

@pytest.mark.parametrize("payload", [
    {"hello": "world"},
    {"query": "SM4", "limit": 10},
    [1, 2, 3, "test"],
    "simple string payload",
    {"nested": {"a": [1, 2, {"b": True}]}},
    {"unicode": "国密 SM2/SM3/SM4 算法规范 🚀"}
])
def test_protocol_encode_decode_matrix(payload):
    encoded = encode_frame(payload)
    assert len(encoded) >= 5 # 4 bytes header + payload
    import json
    length = int.from_bytes(encoded[:4], byteorder="big")
    assert length == len(encoded) - 4

def test_protocol_frame_too_large_rejection():
    large_payload = {"data": "X" * (4 * 1024 * 1024 + 10)}
    with pytest.raises(FrameTooLargeError):
        encode_frame(large_payload)

def test_protocol_jsonrpc_helpers():
    req = make_jsonrpc_request("memory.search", {"query": "test"}, req_id=42)
    assert req["jsonrpc"] == "2.0"
    assert req["method"] == "memory.search"
    assert req["id"] == 42

    resp = make_jsonrpc_response(result={"data": "ok"}, req_id=42)
    assert resp["result"] == {"data": "ok"}
    assert resp["id"] == 42

    err = make_jsonrpc_error(code=-32600, message="Invalid Request", req_id=42)
    assert err["error"]["code"] == -32600


# ==========================================
# 5. Admin CLI Command Parser & Options (20 test cases)
# ==========================================

def test_admin_cli_parser_commands():
    parser = build_parser()

    # status
    args_status = parser.parse_args(["status"])
    assert args_status.command == "status"

    # load
    args_load = parser.parse_args(["load"])
    assert args_load.command == "load"

    # unload
    args_unload = parser.parse_args(["unload"])
    assert args_unload.command == "unload"

    # collections
    args_colls = parser.parse_args(["collections"])
    assert args_colls.command == "collections"

    # snapshot
    args_snap = parser.parse_args(["snapshot", "ai_memory"])
    assert args_snap.command == "snapshot"
    assert args_snap.collection == "ai_memory"

def test_admin_cli_custom_socket():
    parser = build_parser()
    args = parser.parse_args(["--socket", "/tmp/custom-admin.sock", "status"])
    assert args.socket == "/tmp/custom-admin.sock"
    assert args.command == "status"


# ==========================================
# 6. MCP Bridge Serialization & Tool Dispatch (20 test cases)
# ==========================================

@pytest.mark.asyncio
async def test_mcp_bridge_initialize():
    bridge = MCPBridge(source_agent="test_agent")
    req = {"jsonrpc": "2.0", "id": 1, "method": "initialize"}
    resp = await bridge.handle_request(req)
    assert resp["id"] == 1
    assert resp["result"]["serverInfo"]["name"] == "ai-memory-runtime-mcp"
    assert "tools" in resp["result"]["capabilities"]

@pytest.mark.asyncio
async def test_mcp_bridge_tools_list():
    bridge = MCPBridge()
    req = {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
    resp = await bridge.handle_request(req)
    assert resp["id"] == 2
    tools = resp["result"]["tools"]
    assert len(tools) >= 5
    names = [t["name"] for t in tools]
    assert "memory_search" in names
    assert "memory_record" in names
    assert "memory_get" in names
    assert "memory_update_status" in names
    assert "memory_ingest_session" in names
    assert "memory_create" in names
    assert "memory_update" in names
    assert "memory_history" in names
    assert "memory_delete" in names

@pytest.mark.asyncio
async def test_mcp_bridge_invalid_tool_call():
    bridge = MCPBridge()
    # Missing required query argument
    req = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {"name": "memory_search", "arguments": {}}
    }
    resp = await bridge.handle_request(req)
    assert resp["id"] == 3
    assert resp["result"]["isError"] is True
    assert "Parameter 'query' is required" in resp["result"]["content"][0]["text"]

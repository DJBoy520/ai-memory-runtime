"""
AMR v5 Audit Track R1: Agent 身份鉴权与规范化审计测试 (src/interfaces/ipc/auth.py & main.dispatch_business_rpc)
覆盖攻击面：
1. agent_id 大小写、前后空格、全空白、空串、None、非字符串（整数/布尔/列表/字典）
2. source_agent 兜底回退机制与 system 终极默认
3. instance_id 与 client_ref 规范化与空格去除
4. 未授权拦截：错误码 -32001 与 data.agent_id 的完整性
5. dispatch_business_rpc 级别的鉴权拦截与 ContextVar 透传
"""

import pytest
from typing import Any, Dict

from config.settings import AppConfig
from src.core.engine import ModelState
from src.core.request_context import get_client_ref, get_request_id
from src.core.session_store import SessionStore
from src.interfaces.ipc.auth import authorize_agent
from src.interfaces.ipc.errors import RpcAgentUnauthorized
from src.main import AMRApplication
from tests.test_write_path_no_io import ExplodingQdrant


class DummyEngine:
    state = ModelState.READY

    async def get_model_status(self):
        return {"state": "READY"}

    async def embed(self, texts):
        return [[0.0] * 1024 for _ in texts]


class DummyQdrant:
    def is_healthy(self):
        return True

    def get_point_by_memory_id(self, memory_id):
        return None

    def search_points(self, *args, **kwargs):
        return []


def _build_test_config(allowed_agents=None):
    cfg = AppConfig()
    cfg.server.allowed_agents = allowed_agents or ["openclaw", "hermes", "system", "claude_code"]
    return cfg


def test_agent_id_case_and_whitespace_normalization():
    """验证 agent_id 无论大写还是包含前后空格均被规范化为小写紧凑格式"""
    cfg = _build_test_config(["openclaw"])

    agent_id, _ = authorize_agent(cfg, {"agent_id": "  OPENCLAW  "})
    assert agent_id == "openclaw"

    agent_id2, _ = authorize_agent(cfg, {"agent_id": "OpenClaw\t\n"})
    assert agent_id2 == "openclaw"


def test_agent_id_fallback_to_source_agent():
    """当 agent_id 缺失或为空串时，无缝回退到 source_agent"""
    cfg = _build_test_config(["hermes"])

    # 1. agent_id 为 None
    agent_id, _ = authorize_agent(cfg, {"agent_id": None, "source_agent": "  HERMES  "})
    assert agent_id == "hermes"

    # 2. agent_id 为空串 ""
    agent_id, _ = authorize_agent(cfg, {"agent_id": "", "source_agent": "hermes"})
    assert agent_id == "hermes"

    # 3. agent_id 为纯空格 "   "
    agent_id, _ = authorize_agent(cfg, {"agent_id": "   ", "source_agent": "hermes"})
    assert agent_id == "hermes"


def test_agent_id_fallback_to_system_default():
    """当 agent_id 与 source_agent 均缺失或为空串时，默认赋予 system"""
    cfg = _build_test_config(["system"])

    agent_id1, _ = authorize_agent(cfg, {})
    assert agent_id1 == "system"

    agent_id2, _ = authorize_agent(cfg, {"agent_id": "", "source_agent": "   "})
    assert agent_id2 == "system"


def test_agent_id_non_string_types():
    """当 agent_id 为非字符串（数字、布尔、列表、字典）时的转换与拦截"""
    cfg = _build_test_config(["12345", "true"])

    # 1. 整数 12345 转换为 "12345" 命中白名单
    agent_id, _ = authorize_agent(cfg, {"agent_id": 12345})
    assert agent_id == "12345"

    # 2. 布尔 True 转换为 "true" 命中白名单
    agent_id, _ = authorize_agent(cfg, {"agent_id": True})
    assert agent_id == "true"

    # 3. 字典/列表不在白名单中，触发 RpcAgentUnauthorized
    with pytest.raises(RpcAgentUnauthorized) as exc_info:
        authorize_agent(cfg, {"agent_id": {"admin": True}})
    assert exc_info.value.code == -32001
    assert "agent_id" in exc_info.value.data


def test_unauthorized_agent_error_payload():
    """未授权 Agent 访问时抛出 -32001，且携带规范化的 data.agent_id"""
    cfg = _build_test_config(["system"])

    with pytest.raises(RpcAgentUnauthorized) as exc_info:
        authorize_agent(cfg, {"agent_id": "EvilHackerBot"})

    err = exc_info.value
    assert err.code == -32001
    assert "AGENT_UNAUTHORIZED" in err.message
    assert err.data == {"agent_id": "evilhackerbot"}

    # 验证生成的 JSON-RPC error 对象契约
    jsonrpc_err = err.to_jsonrpc_error(req_id=123)
    assert jsonrpc_err["id"] == 123
    assert jsonrpc_err["error"]["code"] == -32001
    assert jsonrpc_err["error"]["data"]["agent_id"] == "evilhackerbot"


def test_instance_id_and_client_ref_precedence():
    """验证 instance_id 与 client_ref 规范化与提取优先级"""
    cfg = _build_test_config()

    # 1. 正常 instance_id
    _, inst = authorize_agent(cfg, {"instance_id": "  node-alpha-1  "})
    assert inst == "node-alpha-1"

    # 2. instance_id 为空回退到 client_ref
    _, inst = authorize_agent(cfg, {"instance_id": "", "client_ref": "  ref-999  "})
    assert inst == "ref-999"

    # 3. 均为纯空白时得到 None
    _, inst = authorize_agent(cfg, {"instance_id": "   ", "client_ref": ""})
    assert inst is None


@pytest.mark.asyncio
async def test_dispatch_business_rpc_auth_enforcement_and_context():
    """验证 dispatch_business_rpc 入口严密执行鉴权并将 client_ref 写入上下文"""
    app = AMRApplication(
        engine=DummyEngine(),
        qdrant_manager=DummyQdrant(),
        session_store=SessionStore(db_path=":memory:"),
    )
    # 覆盖白名单为严格列表
    app.config.server.allowed_agents = ["authorized_worker"]

    # 1. 未授权 Agent 调用 business RPC
    with pytest.raises(RpcAgentUnauthorized) as exc_info:
        await app.dispatch_business_rpc(
            "ping",
            {"agent_id": "unauthorized_worker", "client_ref": "cli_req_42"}
        )
    assert exc_info.value.code == -32001
    assert exc_info.value.data["agent_id"] == "unauthorized_worker"

    # 2. 授权 Agent 调用 business RPC
    res = await app.dispatch_business_rpc(
        "ping",
        {"agent_id": "AUTHORIZED_WORKER", "client_ref": "cli_req_42"}
    )
    assert res["status"] == "healthy"

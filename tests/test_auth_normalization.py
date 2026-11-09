"""测试 Agent 授权与身份规范化（src/interfaces/ipc/auth.py）。

重点断言：
1. 大小写与空格归一化；
2. agent_id 回退链：agent_id -> source_agent -> 'system'；
3. 未授权抛出 RpcAgentUnauthorized，包含 AGENT_UNAUTHORIZED 与 data.agent_id；
4. instance_id / client_ref 规范化提取。
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

from config.settings import AppConfig, ServerConfig
from src.interfaces.ipc.auth import authorize_agent
from src.interfaces.ipc.errors import RpcAgentUnauthorized, RpcError
from src.main import AMRApplication


@pytest.fixture
def sample_config():
    cfg = AppConfig()
    cfg.server.allowed_agents = [
        "system",
        "hermes",
        "openclaw",
        "qwenwork",
        "doubao",
        "zcode",
    ]
    return cfg


def test_auth_case_and_whitespace_normalization(sample_config):
    """验证大小写和首尾空格的归一化处理。"""
    agent, inst = authorize_agent(sample_config, {"agent_id": "  OpenClaw  "})
    assert agent == "openclaw"
    assert inst is None

    agent2, _ = authorize_agent(sample_config, {"agent_id": "HERMES"})
    assert agent2 == "hermes"

    agent3, _ = authorize_agent(sample_config, {"agent_id": "  qwenwork \t\n"})
    assert agent3 == "qwenwork"


def test_auth_fallback_chain(sample_config):
    """验证 agent_id -> source_agent -> 'system' 顺序回退。"""
    # 仅提供 source_agent
    agent1, _ = authorize_agent(sample_config, {"source_agent": "  DouBao  "})
    assert agent1 == "doubao"

    # agent_id 为空串/纯空格时回退到 source_agent
    agent2, _ = authorize_agent(sample_config, {"agent_id": "   ", "source_agent": "ZCode"})
    assert agent2 == "zcode"

    # 两者均为空或缺失时，默认回退到 system
    agent3, _ = authorize_agent(sample_config, {})
    assert agent3 == "system"

    agent4, _ = authorize_agent(sample_config, {"agent_id": "", "source_agent": "  "})
    assert agent4 == "system"


def test_auth_unauthorized_agent_data(sample_config):
    """验证未授权 Agent 抛出 RpcAgentUnauthorized，消息含 AGENT_UNAUTHORIZED 且 data.agent_id 准确。"""
    with pytest.raises(RpcAgentUnauthorized) as exc_info:
        authorize_agent(sample_config, {"agent_id": "  Malicious_Hacker_Agent  "})

    exc = exc_info.value
    assert isinstance(exc, RpcError)
    assert isinstance(exc, ValueError)
    assert "AGENT_UNAUTHORIZED" in str(exc)
    assert exc.data == {"agent_id": "malicious_hacker_agent"}
    assert exc.code == -32001


def test_instance_id_and_client_ref_extraction(sample_config):
    """验证 instance_id 与 client_ref 的提取与去空。"""
    # 显式 instance_id
    _, inst1 = authorize_agent(sample_config, {"agent_id": "hermes", "instance_id": "  inst-node-42  "})
    assert inst1 == "inst-node-42"

    # 回退到 client_ref
    _, inst2 = authorize_agent(sample_config, {"agent_id": "hermes", "client_ref": "ref-worker-99"})
    assert inst2 == "ref-worker-99"

    # instance_id 优先于 client_ref
    _, inst3 = authorize_agent(sample_config, {
        "agent_id": "hermes",
        "instance_id": "primary-inst",
        "client_ref": "secondary-ref",
    })
    assert inst3 == "primary-inst"

    # 空白字符串视为 None
    _, inst4 = authorize_agent(sample_config, {"agent_id": "hermes", "instance_id": "   "})
    assert inst4 is None


@pytest.mark.asyncio
async def test_auth_integration_in_main_dispatch(sample_config):
    """验证 main.AMRApplication.dispatch_business_rpc 接入 authorize_agent。"""
    fake_engine = MagicMock()
    fake_engine.state = MagicMock(value="ready")
    fake_engine.get_model_status = AsyncMock(return_value={"state": "ready"})

    fake_qdrant = MagicMock()
    fake_qdrant.is_healthy.return_value = True

    app = AMRApplication(
        engine=fake_engine,
        qdrant_manager=fake_qdrant,
        session_store=MagicMock(),
    )
    app.config = sample_config

    # 1. 授权 Agent 成功
    res = await app.dispatch_business_rpc("system.health", {
        "agent_id": "  OpenClaw  ",
        "instance_id": "test-box-1",
    })
    assert res["status"] == "healthy"

    # 2. 未授权 Agent 抛出异常且兼容既有测试的 match='AGENT_UNAUTHORIZED'
    with pytest.raises(ValueError, match="AGENT_UNAUTHORIZED") as exc_info:
        await app.dispatch_business_rpc("system.health", {"agent_id": "unknown_ai"})

    assert isinstance(exc_info.value, RpcAgentUnauthorized)
    assert exc_info.value.data == {"agent_id": "unknown_ai"}

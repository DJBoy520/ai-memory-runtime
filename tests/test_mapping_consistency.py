"""映射单一真源与别名一致性守卫测试（复杂度审计 Phase1 修复，2026-11-21）"""
import pytest
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]


def test_method_to_tool_matches_tool_rpc_methods():
    """METHOD_TO_TOOL 每个规范条目必须与 TOOL_RPC_METHODS（唯一真源）一致（守卫在导入时也会运行）"""
    from src.interfaces.adapter.mapping import METHOD_TO_TOOL
    from src.interfaces.mcp.remote_backend import TOOL_RPC_METHODS
    for method, tool in METHOD_TO_TOOL.items():
        assert TOOL_RPC_METHODS.get(tool) == method, f"drift: {tool}"


def test_alias_tools_map_to_same_method_as_canonical():
    """别名工具必须与规范工具映射到同一 RPC 方法（别名不产生第二套语义）"""
    from src.interfaces.mcp.remote_backend import TOOL_RPC_METHODS
    for alias, canonical in (("memory_session_checkpoint", "session_checkpoint"),
                             ("memory_get_handoff", "session_get_handoff")):
        assert TOOL_RPC_METHODS.get(alias) == TOOL_RPC_METHODS.get(canonical), f"alias drift: {alias}"


def test_bridge_no_second_mapping_table():
    """bridge.py 不得再内置第二份映射表（复杂度审计 codex#1/DeepSeek#3）"""
    src = (REPO/"src/interfaces/mcp/bridge.py").read_text()
    assert "rpc_method_map" not in src, "bridge.py reintroduced a local mapping table"


def test_bridge_rejects_mismatched_response_id():
    """bridge.py/bridge2 必须校验响应 id（审计 DeepSeek#4：此前不校验）"""
    b = (REPO/"src/interfaces/mcp/bridge.py").read_text()
    b2 = (REPO/"src/interfaces/mcp/bridge2.py").read_text()
    assert "MISMATCHED_RESPONSE_ID" in b and "MISMATCHED_RESPONSE_ID" in b2

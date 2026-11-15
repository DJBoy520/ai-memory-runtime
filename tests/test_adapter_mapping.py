"""2.x adapter 方法映射契约测试。"""

import pytest

from src.interfaces.adapter import mapping
from src.interfaces.mcp.remote_backend import TOOL_RPC_METHODS


def test_every_method_maps_to_a_known_tool():
    for method, tool in mapping.METHOD_TO_TOOL.items():
        assert TOOL_RPC_METHODS[tool] == method
        assert mapping.resolve_tool(method) == tool


def test_every_remote_tool_has_a_canonical_method():
    aliases = {"memory_session_checkpoint", "memory_get_handoff"}
    assert set(mapping.METHOD_TO_TOOL.values()) | aliases == set(TOOL_RPC_METHODS)
    # 别名工具共享同一个 daemon 方法，不产生第二套映射。
    for alias in aliases:
        assert TOOL_RPC_METHODS[alias] in mapping.METHOD_TO_TOOL


def test_alias_tools_are_not_canonical_adapter_methods():
    with pytest.raises(mapping.UnknownMethodError):
        mapping.tool_to_method("memory_session_checkpoint")
    with pytest.raises(mapping.UnknownMethodError):
        mapping.tool_to_method("memory_get_handoff")


@pytest.mark.parametrize(
    "method",
    [
        "memory_spool_status",
        "memory_spool_retry",
        "admin.shutdown",
        "session.delete_all",
        "memory.unknown",
        "",
        None,
        123,
    ],
)
def test_unknown_or_local_only_methods_are_rejected(method):
    with pytest.raises(mapping.UnknownMethodError):
        mapping.resolve_tool(method)


def test_tool_to_method_round_trips_canonical_tools():
    for tool, method in mapping.TOOL_TO_METHOD.items():
        assert mapping.resolve_tool(method) == tool

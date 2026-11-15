"""2.x UDS 适配器：daemon 方法 <-> MCP 工具映射。

适配器对插件保持 1.x 的 daemon 方法名；服务端只接受 MCP 工具名。本模块是
两侧的唯一映射来源，未知/本机专用/管理方法一律拒绝。

别名策略：`memory_session_checkpoint` 与 `memory_get_handoff` 是 1.x 的工具
别名；适配器对外只使用规范方法 `session.checkpoint` / `session.get_handoff`。
"""

from __future__ import annotations

from typing import Any, Dict

from src.interfaces.mcp.remote_backend import TOOL_RPC_METHODS

__all__ = [
    "METHOD_TO_TOOL",
    "UnknownMethodError",
    "resolve_tool",
    "tool_to_method",
]

# daemon 方法 -> 规范 MCP 工具。必须与 TOOL_RPC_METHODS 保持一致；
# 测试会逐项核对，防止新增工具时只改一边。
METHOD_TO_TOOL: Dict[str, str] = {
    "memory.search": "memory_search",
    "memory.create": "memory_create",
    "memory.update": "memory_update",
    "memory.history": "memory_history",
    "memory.delete": "memory_delete",
    "memory.get": "memory_get",
    "memory.record": "memory_record",
    "memory.update_status": "memory_update_status",
    "session.ingest": "memory_ingest_session",
    "session.checkpoint": "session_checkpoint",
    "session.get_handoff": "session_get_handoff",
    "memory.candidates": "memory_candidates",
    "memory.approve_candidate": "memory_approve_candidate",
}

# 工具 -> 方法（仅规范工具名有值；别名工具不产生第二套方法）。
TOOL_TO_METHOD: Dict[str, str] = {
    tool: method for method, tool in METHOD_TO_TOOL.items()
}


class UnknownMethodError(PermissionError):
    """方法不在适配器业务白名单内（未知、本机专用或管理方法）。"""


def resolve_tool(method: Any) -> str:
    """把 daemon 方法解析为 MCP 工具；未知方法失败关闭。"""
    if not isinstance(method, str) or not method:
        raise UnknownMethodError("unknown method")
    tool = METHOD_TO_TOOL.get(method)
    if tool is None:
        # 不区分“未知”和“未授权”，避免暴露方法白名单。
        raise UnknownMethodError("unknown method")
    return tool


def tool_to_method(tool: Any) -> str:
    """把规范 MCP 工具解析为 daemon 方法；别名/未知工具失败关闭。"""
    if not isinstance(tool, str) or not tool:
        raise UnknownMethodError("unknown tool")
    method = TOOL_TO_METHOD.get(tool)
    if method is None:
        raise UnknownMethodError("unknown tool")
    return method


def _assert_mapping_is_consistent() -> None:
    for method, tool in METHOD_TO_TOOL.items():
        if TOOL_RPC_METHODS.get(tool) != method:
            raise RuntimeError(
                f"adapter mapping drift: {method!r} -> {tool!r} not in TOOL_RPC_METHODS"
            )
    # 反向检查：新增工具必须显式进入适配器映射，否则启动即失败，避免
    # “服务端新增工具、适配器静默不可达”的漂移。
    for tool, method in TOOL_RPC_METHODS.items():
        if method not in METHOD_TO_TOOL:
            raise RuntimeError(
                f"adapter mapping missing tool {tool!r} (method {method!r})"
            )


_assert_mapping_is_consistent()

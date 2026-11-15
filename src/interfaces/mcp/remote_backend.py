"""远程 MCP 业务工具到 daemon RPC 方法的隔离适配层。

职责边界：
- 只负责把合法 MCP 业务工具名映射为 daemon JSON-RPC 方法名；
- RPC 传输通过构造函数注入的 ``rpc(method, params)`` 完成，本模块不创建连接；
- 未知工具拒绝；``memory_spool_status``/``memory_spool_retry`` 属于本机专用
  能力，远程路径一律拒绝，即使调用方绕过工具定义直接传入名称；
- 参数使用浅拷贝转发，不修改调用方传入的字典；
- 本模块不导入 ``remote_gateway``：``LOCAL_ONLY_TOOLS`` 与
  ``TOOL_RPC_METHODS`` 在此定义，Gateway 只单向引用，避免循环依赖。
"""

from typing import Any, Awaitable, Callable, Dict, FrozenSet, Mapping

__all__ = [
    "LOCAL_ONLY_TOOLS",
    "TOOL_RPC_METHODS",
    "LocalOnlyToolError",
    "RemoteBackend",
    "RemoteBackendError",
    "UnknownToolError",
]

RpcCall = Callable[[str, Mapping[str, Any]], Awaitable[Any]]

# 仅在本机进程中才有意义的工具：它们直接读取/驱动本机会话 spool，
# 通过远程网关调用会把本地文件系统状态暴露给远端或触发本地副作用，
# 因此无论令牌允许列表如何配置都一律拒绝。
LOCAL_ONLY_TOOLS: FrozenSet[str] = frozenset(
    {"memory_spool_status", "memory_spool_retry"}
)

# 与 src/interfaces/mcp/bridge.py 中的 business tool -> daemon RPC 映射保持一致。
TOOL_RPC_METHODS: Dict[str, str] = {
    "memory_search": "memory.search",
    "memory_create": "memory.create",
    "memory_update": "memory.update",
    "memory_history": "memory.history",
    "memory_delete": "memory.delete",
    "memory_get": "memory.get",
    "memory_record": "memory.record",
    "memory_update_status": "memory.update_status",
    "memory_ingest_session": "session.ingest",
    "session_checkpoint": "session.checkpoint",
    "memory_session_checkpoint": "session.checkpoint",
    "session_get_handoff": "session.get_handoff",
    "memory_get_handoff": "session.get_handoff",
    "memory_candidates": "memory.candidates",
    "memory_approve_candidate": "memory.approve_candidate",
}


class RemoteBackendError(Exception):
    """远程后端拒绝调用或无法分发工具。"""


class UnknownToolError(RemoteBackendError, ValueError):
    """工具名不在合法业务映射内。"""


class LocalOnlyToolError(RemoteBackendError, PermissionError):
    """本机专用工具不得通过远程后端调用。"""


def resolve_rpc_method(tool_name: Any) -> str:
    """解析工具名对应的 daemon RPC 方法，未知/本机专用工具直接拒绝。"""
    if isinstance(tool_name, str) and tool_name in LOCAL_ONLY_TOOLS:
        raise LocalOnlyToolError(
            f"Tool '{tool_name}' is only available to the local MCP bridge"
        )

    if not isinstance(tool_name, str) or tool_name not in TOOL_RPC_METHODS:
        raise UnknownToolError(f"Unknown tool name: {tool_name!r}")

    return TOOL_RPC_METHODS[tool_name]


class RemoteBackend:
    """把 MCP 工具调用转换为注入的异步 daemon RPC 调用。"""

    def __init__(self, rpc: RpcCall) -> None:
        if not callable(rpc):
            raise TypeError("rpc must be callable as rpc(method, params)")
        self._rpc = rpc

    async def call(self, tool_name: Any, arguments: Any) -> Any:
        """映射工具名并调用 ``rpc(method, params)``。"""
        rpc_method = resolve_rpc_method(tool_name)

        if not isinstance(arguments, Mapping):
            raise ValueError(
                "Tool arguments must be a mapping, got "
                f"{type(arguments).__name__}"
            )

        return await self._rpc(rpc_method, dict(arguments))

    async def handle(self, tool_name: Any, arguments: Any) -> Any:
        """Gateway 兼容别名。"""
        return await self.call(tool_name, arguments)

    async def __call__(self, tool_name: Any, arguments: Any) -> Any:
        """可直接作为 ``Gateway(..., backend=RemoteBackend(rpc))`` 使用。"""
        return await self.call(tool_name, arguments)

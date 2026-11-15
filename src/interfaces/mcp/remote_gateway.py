"""远程 MCP Gateway：鉴权、工具授权与身份注入。

安全不变量：
- 未知/非法令牌在 Gateway 层即被拒绝，绝不转发给后端；
- 只允许令牌身份允许列表内的工具，且该工具必须被 ``remote_backend.TOOL_RPC_METHODS``
  显式登记为远程业务工具；``TOOL_DEFINITIONS`` 只是 MCP 工具清单，新增工具
  不会自动获得远程调用权限；
- 本机专用工具（如 ``memory_spool_status``/``memory_spool_retry``）一律拒绝，
  即使令牌允许列表显式包含它们，也不得通过远程网关暴露本地 spool 能力；
- 身份字段 ``agent_id``/``source_agent`` 一律由令牌身份覆盖，客户端无法伪造；
- 业务参数必须在转发前通过 ``validate_tool_args`` 校验，客户端无法绕过工具契约；
- :meth:`Gateway.handle_identity` 只接受认证中间件产出的、类型精确为
  :class:`TokenIdentity` 的不可变身份；``None``、字典、鸭子类型对象及子类
  一律拒绝。身份是独立参数，不能由客户端在 ``arguments`` 中注入。
"""

from typing import Any, Awaitable, Callable, Dict, FrozenSet, Mapping

from src.interfaces.mcp.remote_auth import TokenAuthenticator, TokenIdentity
from src.interfaces.mcp.remote_backend import LOCAL_ONLY_TOOLS, TOOL_RPC_METHODS
from src.interfaces.mcp.tools import validate_tool_args

__all__ = ["Gateway", "LOCAL_ONLY_TOOLS"]

Backend = Callable[[str, Mapping[str, Any]], Awaitable[Any]]


def _remote_tool_names() -> FrozenSet[str]:
    """远程可调用工具白名单：只取 ``TOOL_RPC_METHODS`` 显式登记的业务工具。

    刻意不参考 ``TOOL_DEFINITIONS``：MCP 工具清单可能新增本机专用工具，
    它不应因此自动成为远程可调用工具。
    """
    return frozenset(TOOL_RPC_METHODS)


class Gateway:
    """在把调用转发给后端之前完成鉴权与参数加固的最小网关。"""

    def __init__(self, authenticator: TokenAuthenticator, backend: Backend) -> None:
        self._authenticator = authenticator
        self._backend = backend
        # 白名单快照在构造时确定，来源是显式的远程业务工具映射。
        self._remote_tools = _remote_tool_names()

    async def handle(self, token: Any, tool: Any, arguments: Any) -> Any:
        """兼容入口：先用令牌鉴权，再委托给 :meth:`handle_identity`。"""
        identity = self._authenticator.authenticate(token)
        if identity is None:
            raise PermissionError("invalid or missing token")

        return await self.handle_identity(identity, tool, arguments)

    async def handle_identity(self, identity: Any, tool: Any, arguments: Any) -> Any:
        """使用已完成鉴权的身份执行与 :meth:`handle` 相同的授权和校验。

        身份只能由认证中间件以独立参数传入。这里严格要求类型精确为
        :class:`TokenIdentity`：拒绝 ``None``、非身份对象和子类，防止通过
        覆盖 ``allows_tool`` 等方式放宽工具授权。客户端提交的 ``arguments``
        不参与身份判定，其中的 ``agent_id``/``source_agent`` 始终被覆盖。
        """
        if type(identity) is not TokenIdentity:
            raise PermissionError("invalid identity")

        # 本机专用工具在任何允许列表之前被拒绝：允许列表无法解除该限制。
        if isinstance(tool, str) and tool in LOCAL_ONLY_TOOLS:
            raise PermissionError("tool not available over remote gateway")

        # 未在 TOOL_RPC_METHODS 显式登记的工具一律拒绝，即使令牌允许列表包含它。
        if tool not in self._remote_tools:
            raise PermissionError("tool not available over remote gateway")

        if not identity.allows_tool(tool):
            raise PermissionError("tool not permitted for this token")

        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be a dictionary")

        # 在注入服务端身份前校验客户端提交的真实业务参数，避免身份字段掩盖
        # 缺失或类型错误的业务参数。
        validate_tool_args(tool, arguments)

        params: Dict[str, Any] = dict(arguments)
        params["agent_id"] = identity.agent_id
        params["source_agent"] = identity.agent_id

        return await self._backend(tool, params)

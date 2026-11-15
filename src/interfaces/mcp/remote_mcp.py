"""远程 MCP SDK ASGI 应用：令牌认证、身份隔离与 Gateway 调用。

本模块只构造内部 ASGI 应用，不监听端口、不读取生产配置、不创建网络连接。
调用方必须把返回的应用挂载到已有的内部服务端（或直接用于 ASGI 测试）；
本模块不提供 ``uvicorn``/``hypercorn`` 启动入口。

安全不变量：
- MCP 端点由 :class:`BearerAuthMiddleware` 保护；缺失、重复、非法或未知
  Bearer 凭据在进入 MCP 前即被拒绝；``/healthz`` 是唯一无认证的存活探针，
  只返回固定状态，不触达后端；
- 每个 MCP 请求都从 ``server.request_context.request.scope`` 的
  :data:`IDENTITY_SCOPE_KEY` 读取认证中间件写入的不可变
  :class:`TokenIdentity`，不复用、不缓存，也不从客户端参数推断身份；
- 身份缺失或不是精确的 :class:`TokenIdentity` 时失败关闭，绝不调用后端；
- ``tools/list`` 只返回 ``identity.tools`` 与 ``TOOL_RPC_METHODS`` 的交集，
  客户端无法通过协议请求扩大可见工具范围；
- ``tools/call`` 一律通过 :meth:`Gateway.handle_identity` 完成远程白名单、
  令牌允许列表、业务参数校验和 ``agent_id``/``source_agent`` 身份注入；
- 客户端提交的 ``agent_id``/``source_agent`` 不参与授权，且会被 Gateway
  覆盖；本模块不读取或不信任这些字段；
- 请求令牌只用于中间件鉴权，鉴权后 ``Authorization`` 头被中间件移除；
  本模块不保存令牌、不记录令牌，也不把令牌写入错误响应；
- 工具调用失败时只返回固定的 MCP 错误文本，不记录也不透露原始异常，
  避免后端内部信息（标识、路径、凭据片段等）通过错误响应外泄；2.x 例外：
  daemon 的稳定 JSON-RPC 错误码会以 ``structuredContent.amr_error`` 暴露
  ``code``/``message_class``/``retryable`` 与少量白名单字段，供本地适配器
  还原错误语义，原始异常文本仍不暴露。
"""

from __future__ import annotations

import contextlib
import json
from typing import Any, AsyncIterator, Callable, Iterable
from urllib.parse import urlsplit

from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, TextContent, Tool
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.types import Receive, Scope, Send

from src.interfaces.mcp.http_auth import IDENTITY_SCOPE_KEY, BearerAuthMiddleware
from src.interfaces.mcp.remote_auth import TokenAuthenticator, TokenIdentity
from src.interfaces.mcp.remote_backend import TOOL_RPC_METHODS
from src.interfaces.mcp.remote_gateway import Backend, Gateway
from src.interfaces.mcp.remote_rpc import DaemonRpcError
from src.interfaces.mcp.tools import TOOL_DEFINITIONS

__all__ = ["build_app"]

_DEFAULT_PATH = "/mcp"
_SERVER_NAME = "ai-memory-runtime-remote-mcp"
_SERVER_VERSION = "1.0.0"

# 2.x 结构化错误信封：daemon 的稳定错误码 -> 公开分类/可重试性/允许的 data
# 字段。只白名单标量字段；原始异常文本、路径、SQL、令牌一律不进入响应。
_AMR_ERROR_MAP: dict[int, tuple[str, bool, tuple[str, ...]]] = {
    -32001: ("AGENT_UNAUTHORIZED", False, ()),
    -32002: ("VERSION_CONFLICT", False, ("memory_id", "current_version", "expected_version")),
    -32003: ("MODEL_LOADING", True, ("model_state", "retry_after_ms")),
    -32004: ("QUEUE_FULL", True, ("active_requests", "max_queue_size")),
    -32005: ("NOT_FOUND", False, ("memory_id",)),
    -32006: ("IDEMPOTENCY_CONFLICT", False, ("op", "existing_memory_id")),
    -32601: ("METHOD_NOT_FOUND", False, ()),
    -32602: ("INVALID_ARGUMENT", False, ("field",)),
    -32603: ("INTERNAL", False, ()),
}

_AMR_ERROR_FALLBACK = ("INTERNAL", False, ())

# 默认仅允许本机回环主机。``:*`` 只放宽端口，不放宽主机名；这样可以
# 让本地测试/开发使用任意监听端口，同时继续拒绝未知域名。
# ``test`` 是 httpx ``ASGITransport`` 惯用的内存测试主机（RFC 6761 保留
# 的测试名），仅作精确匹配以支持仓库内的隔离测试，绝不构成域名通配。
_DEFAULT_ALLOWED_HOSTS: tuple[str, ...] = (
    "localhost",
    "localhost:*",
    "127.0.0.1",
    "127.0.0.1:*",
    "[::1]",
    "[::1]:*",
    "test",
)

# Origin 允许本机回环的 http/https（含任意端口）。缺少 Origin 的
# 非浏览器客户端由 SDK 视为同源请求；显式提供的 Origin 必须精确匹配。
_DEFAULT_ALLOWED_ORIGINS: tuple[str, ...] = (
    "http://localhost",
    "http://localhost:*",
    "https://localhost",
    "https://localhost:*",
    "http://127.0.0.1",
    "http://127.0.0.1:*",
    "https://127.0.0.1",
    "https://127.0.0.1:*",
    "http://[::1]",
    "http://[::1]:*",
    "https://[::1]",
    "https://[::1]:*",
)

_ALLOWED_ORIGIN_SCHEMES = frozenset({"http", "https"})


def _has_unsafe_allowlist_chars(value: str) -> bool:
    """拒绝控制字符与空白，避免头值被日志/代理解释出多种含义。"""
    return any(ord(char) < 0x20 or ord(char) == 0x7F or char.isspace() for char in value)


def _validate_host_pattern(value: str) -> None:
    """校验一个 Host 允许项；只允许精确值或显式 ``host:*`` 端口范围。"""
    if _has_unsafe_allowlist_chars(value):
        raise ValueError("allowed_hosts entries must not contain whitespace or control characters")
    if any(char in value for char in "/?#@"):
        raise ValueError("allowed_hosts entries must be bare host[:port] values")

    if "*" not in value:
        return

    if not value.endswith(":*") or value.count("*") != 1:
        raise ValueError("allowed_hosts only supports the explicit 'host:*' port pattern")

    base = value[:-2]
    if not base or "*" in base or any(char in base for char in "/?#@"):
        raise ValueError("allowed_hosts only supports the explicit 'host:*' port pattern")


def _validate_origin_pattern(value: str) -> None:
    """校验一个 Origin 允许项；只允许 http/https 源或显式 ``origin:*``。"""
    if _has_unsafe_allowlist_chars(value):
        raise ValueError("allowed_origins entries must not contain whitespace or control characters")

    base = value
    if "*" in value:
        if not value.endswith(":*") or value.count("*") != 1:
            raise ValueError("allowed_origins only supports the explicit 'origin:*' port pattern")
        base = value[:-2]
        if not base or "*" in base:
            raise ValueError("allowed_origins only supports the explicit 'origin:*' port pattern")

    parsed = urlsplit(base)
    if (
        parsed.scheme not in _ALLOWED_ORIGIN_SCHEMES
        or not parsed.netloc
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("allowed_origins entries must be http(s) origins without path, query or credentials")


def _normalize_allowlist(
    values: Iterable[str] | None,
    *,
    field: str,
    default: tuple[str, ...],
    validator: Callable[[str], None],
) -> tuple[str, ...]:
    """校验并冻结允许列表；``None`` 使用安全默认值，显式空列表表示全部拒绝。"""
    if values is None:
        return default
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{field} must be an iterable of strings, not a string")

    entries = list(values)
    normalized: list[str] = []
    for entry in entries:
        if not isinstance(entry, str) or not entry:
            raise ValueError(f"{field} entries must be non-empty strings")
        validator(entry)
        normalized.append(entry)

    # 去重并保持顺序；调用方之后的修改不会影响已构造的应用。
    return tuple(dict.fromkeys(normalized))


class _StreamableHTTPASGIApp:
    """把 session manager 暴露为纯 ASGI callable。

    Starlette 的 ``Route`` 会把普通函数/方法当作 ``func(request) -> response``，
    因此这里使用可调用对象，确保请求原样交给 Streamable HTTP 传输层处理。
    """

    def __init__(self, session_manager: StreamableHTTPSessionManager) -> None:
        self._session_manager = session_manager

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._session_manager.handle_request(scope, receive, send)


def _identity_from_context(server: Server[Any, Any]) -> TokenIdentity:
    """读取当前 MCP 请求的认证身份；任何缺失或伪造都失败关闭。"""
    try:
        request = server.request_context.request
    except LookupError:
        raise PermissionError("authenticated identity is required") from None

    if request is None:
        raise PermissionError("authenticated identity is required")

    scope = getattr(request, "scope", None)
    if not isinstance(scope, dict):
        raise PermissionError("authenticated identity is required")

    try:
        identity = scope[IDENTITY_SCOPE_KEY]
    except KeyError:
        raise PermissionError("authenticated identity is required") from None

    # 与 Gateway 保持一致：只接受认证中间件产出的精确类型，拒绝子类和
    # 鸭子类型对象，避免通过覆盖 allows_tool 等方式放宽授权。
    if type(identity) is not TokenIdentity:
        raise PermissionError("authenticated identity is required")

    return identity


def _tools_for_identity(identity: TokenIdentity) -> list[Tool]:
    """返回该身份可见工具：令牌允许列表与远程 RPC 白名单的交集。"""
    allowed = frozenset(identity.tools).intersection(TOOL_RPC_METHODS)

    tools: list[Tool] = []
    for definition in TOOL_DEFINITIONS:
        name = definition.get("name")
        if name not in allowed:
            continue
        tools.append(
            Tool(
                name=name,
                description=definition.get("description"),
                inputSchema=definition.get("inputSchema") or {"type": "object"},
            )
        )
    return tools


def _json_text(value: Any) -> str:
    """把后端结果转换为安全的文本内容，不包含任何请求令牌。"""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(value)


def _tool_result(value: Any, *, is_error: bool = False) -> CallToolResult:
    if isinstance(value, CallToolResult):
        return value
    return CallToolResult(
        content=[TextContent(type="text", text=_json_text(value))],
        isError=is_error,
    )


def _error_result(message: str) -> CallToolResult:
    return _tool_result(message, is_error=True)


def _amr_error_result(error: DaemonRpcError) -> CallToolResult:
    """把 daemon 稳定错误码转换为 MCP 结构化错误信封（2.x）。

    只暴露稳定 ``code``、``message_class``、``retryable`` 以及每种错误码
    白名单内的标量字段；原始异常文本、路径、SQL 与令牌不进入响应。适配器
    依赖这些字段还原 1.x JSON-RPC 错误码，标准客户端可忽略 ``structuredContent``。
    """
    raw_code = getattr(error, "code", None)
    if isinstance(raw_code, bool) or not isinstance(raw_code, int):
        code = -32603
    else:
        code = raw_code

    message_class, retryable, allowed_keys = _AMR_ERROR_MAP.get(
        code, _AMR_ERROR_FALLBACK
    )

    payload: dict[str, Any] = {
        "code": code,
        "message_class": message_class,
        "retryable": retryable,
    }
    safe_data: dict[str, Any] = {}
    if allowed_keys and isinstance(getattr(error, "data", None), dict):
        for key in allowed_keys:
            value = error.data.get(key)
            # bool 是 int 的子类：显式排除，避免 True/False 混入版本号等
            # 整型字段；只放行可安全 JSON 化的 str/int 标量。
            if isinstance(value, bool):
                continue
            if isinstance(value, (str, int)):
                safe_data[key] = value
    if safe_data:
        payload["data"] = safe_data

    return CallToolResult(
        content=[
            TextContent(
                type="text",
                text=f"Error: {message_class.replace('_', ' ').lower()}",
            )
        ],
        structuredContent={"amr_error": payload},
        isError=True,
    )


IdentityReader = Callable[[Server[Any, Any]], TokenIdentity]


def _build_server(
    identity_reader: IdentityReader,
    gateway: Gateway,
) -> Server[Any, Any]:
    """构造只依赖每请求身份的 lowlevel MCP Server。"""
    server: Server[Any, Any] = Server(name=_SERVER_NAME, version=_SERVER_VERSION)

    @server.list_tools()
    async def _list_tools(_request: Any = None) -> Iterable[Tool]:
        identity = identity_reader(server)
        return _tools_for_identity(identity)

    @server.call_tool(validate_input=False)
    async def _call_tool(name: str, arguments: Any) -> CallToolResult:
        try:
            identity = identity_reader(server)
        except PermissionError:
            # 身份不可用时失败关闭，不把任何后端能力暴露给匿名调用。
            return _error_result("Error: authenticated identity is required")

        try:
            result = await gateway.handle_identity(
                identity,
                name,
                arguments if arguments is not None else {},
            )
        except PermissionError:
            # Gateway 已负责授权与身份注入。这里只返回固定的错误分类，
            # 不复用异常文本，避免后端或内部细节通过 MCP 响应外泄。
            return _error_result("Error: tool call not permitted")
        except ValueError:
            # 参数校验失败同样只返回固定文本，不透露原始异常内容。
            return _error_result("Error: invalid tool arguments")
        except DaemonRpcError as exc:
            # 2.x：保留 daemon 稳定错误码给本地适配器；只暴露白名单字段。
            return _amr_error_result(exc)
        except Exception:
            # 后端异常可能包含内部标识、路径或凭据片段：保持 MCP 工具
            # 错误结构（isError + 文本内容），但绝不记录或返回异常原文。
            return _error_result("Error: tool call failed")

        return _tool_result(result)

    return server



def build_app(
    authenticator: TokenAuthenticator,
    backend: Backend,
    *,
    path: str = _DEFAULT_PATH,
    allowed_hosts: Iterable[str] | None = None,
    allowed_origins: Iterable[str] | None = None,
) -> Starlette:
    """构造仅内部使用的远程 MCP ASGI 应用。

    DNS rebinding 防护始终开启，Host/Origin 只接受显式允许值。默认仅允许
    本机回环主机与本机回环来源（端口可用 ``:*`` 显式放宽），适合本地测试；
    面向其他主机/来源部署时必须通过参数明确配置，不能使用宽泛通配符。

    Args:
        authenticator: 令牌到不可变身份的鉴权器；只由中间件用于建立身份。
        backend: 与 :class:`Gateway` 兼容的异步后端 ``(tool, params)``。
        path: MCP 端点路径，默认 ``/mcp``。
        allowed_hosts: 允许的 Host 头值；省略时仅允许本机回环。每项必须是
            精确的 ``host[:port]``，或显式端口模式 ``host:*``；拒绝 ``*``、
            ``*.example`` 等宽泛通配符。显式空列表表示拒绝所有 Host。
        allowed_origins: 允许的 Origin 头值；省略时仅允许本机回环 http/https。
            每项必须是 ``scheme://host[:port]``，或显式端口模式
            ``scheme://host:*``；拒绝宽泛通配符。显式空列表仍允许省略
            Origin 的非浏览器客户端，但拒绝任何显式 Origin。

    Returns:
        带 lifespan 的 Starlette 应用。调用方负责在内部 ASGI 服务中运行它；
        本函数不会打开套接字或监听公网地址。
    """
    if not path.startswith("/"):
        raise ValueError("path must start with '/'")

    hosts = _normalize_allowlist(
        allowed_hosts,
        field="allowed_hosts",
        default=_DEFAULT_ALLOWED_HOSTS,
        validator=_validate_host_pattern,
    )
    origins = _normalize_allowlist(
        allowed_origins,
        field="allowed_origins",
        default=_DEFAULT_ALLOWED_ORIGINS,
        validator=_validate_origin_pattern,
    )
    security_settings = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(hosts),
        allowed_origins=list(origins),
    )

    gateway = Gateway(authenticator=authenticator, backend=backend)
    server = _build_server(_identity_from_context, gateway)
    session_manager = StreamableHTTPSessionManager(
        app=server,
        stateless=True,
        json_response=True,
        event_store=None,
        security_settings=security_settings,
    )

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        async with session_manager.run():
            yield

    async def _healthz(_request: Any) -> JSONResponse:
        # 只暴露存活状态；不包含版本、路径、依赖或凭证信息，也不触达后端。
        return JSONResponse({"status": "ok"})

    app = Starlette(
        routes=[
            Route(path, endpoint=_StreamableHTTPASGIApp(session_manager)),
            Route("/healthz", endpoint=_healthz, methods=["GET"]),
        ],
        lifespan=lifespan,
    )
    # 只保护 MCP 端点；/healthz 明确作为无认证存活探针。其余未来路由默认
    # 不在此路径集合内，新增路由时必须显式决定是否加入保护列表。
    app.add_middleware(
        BearerAuthMiddleware,
        authenticator=authenticator,
        protected_paths=[path],
    )
    return app

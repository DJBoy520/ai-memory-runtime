"""远程 MCP Gateway 的隔离单元测试。

安全不变量：错误的令牌必须在 Gateway 层被拒绝，且不得把请求转发给后端。
本测试只使用测试专用假凭证和内存后端，不连接任何服务或数据库。
"""

import pytest

from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_gateway import Gateway

# 仅用于单元测试的固定假凭证；真实凭证必须由 CSPRNG 生成且不得写入仓库。
VALID_TOKEN = "test-only-gateway-token-0123456789abcdef0123456789abcdef"
WRONG_TOKEN = "test-only-wrong-token-fedcba9876543210fedcba9876543210"

# 后端应看到的服务端身份；客户端传入的同名字段必须被覆盖。
SERVER_AGENT_ID = "gateway_test_agent"
FORGED_AGENT_ID = "forged_agent"
FORGED_SOURCE_AGENT = "forged_source_agent"
FORGED_UNKNOWN_TOOL = "forged_admin_tool"


class _RecordingBackend:
    """记录所有后端调用的异步后端桩。"""

    def __init__(self) -> None:
        self.calls = []

    async def _record(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return {"ok": True}

    async def handle(self, *args, **kwargs):
        return await self._record(*args, **kwargs)

    async def __call__(self, *args, **kwargs):
        return await self._record(*args, **kwargs)


@pytest.mark.asyncio
async def test_handle_rejects_wrong_token_without_calling_backend():
    """错误令牌必须导致拒绝，且后端调用次数保持为零。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search"],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(Exception) as exc_info:
        await gateway.handle(WRONG_TOKEN, "memory_search", {"query": "test query"})

    # 拒绝必须来自鉴权路径，而不是 Gateway/后端接口不匹配等实现错误。
    assert not isinstance(exc_info.value, (TypeError, AttributeError)), (
        "错误令牌应以鉴权错误被拒绝，而不是触发接口调用错误"
    )
    assert backend.calls == []


@pytest.mark.asyncio
async def test_valid_token_forwards_with_server_identity_and_preserves_arguments():
    """有效令牌放行；身份字段由服务端覆盖，其余参数原样转发且入参不被修改。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search"],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    arguments = {
        "query": "test query",
        "limit": 3,
        "project_id": "test-project",
        # 客户端伪造的身份字段，必须被服务端身份覆盖。
        "agent_id": FORGED_AGENT_ID,
        "source_agent": FORGED_SOURCE_AGENT,
    }
    original_arguments = dict(arguments)

    result = await gateway.handle(VALID_TOKEN, "memory_search", arguments)

    assert result == {"ok": True}
    assert len(backend.calls) == 1

    (tool, forwarded), kwargs = backend.calls[0]
    assert kwargs == {}
    assert tool == "memory_search"
    assert forwarded["agent_id"] == SERVER_AGENT_ID
    assert forwarded["source_agent"] == SERVER_AGENT_ID
    assert forwarded["query"] == "test query"
    assert forwarded["limit"] == 3
    assert forwarded["project_id"] == "test-project"

    # 后端看到的是网关新建的参数字典，调用方传入的原始参数不得被改写。
    assert forwarded is not arguments
    assert arguments == original_arguments


@pytest.mark.asyncio
async def test_handle_rejects_tool_outside_allowlist_without_calling_backend():
    """令牌允许列表之外的合法工具必须被拒绝，且后端不被调用。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search"],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(PermissionError):
        await gateway.handle(VALID_TOKEN, "memory_delete", {"memory_id": "mem-1"})

    assert backend.calls == []


@pytest.mark.asyncio
async def test_handle_rejects_forged_unknown_tool_in_allowlist_without_calling_backend():
    """允许列表中被伪造、但并未定义的工具同样必须被拒绝，且后端不被调用。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search", FORGED_UNKNOWN_TOOL],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(PermissionError):
        await gateway.handle(VALID_TOKEN, FORGED_UNKNOWN_TOOL, {"query": "test query"})

    assert backend.calls == []


# 本机专用工具：直接操作服务器本地会话 spool，远程网关必须一律拒绝，
# 即使令牌允许列表显式包含它们也不得放行。
LOCAL_ONLY_TOOLS = ("memory_spool_status", "memory_spool_retry")


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", LOCAL_ONLY_TOOLS)
async def test_handle_rejects_local_only_tool_even_if_allowlisted(tool_name):
    """本机专用工具即使在令牌允许列表内也必须被拒绝，且后端不被调用。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search", *LOCAL_ONLY_TOOLS],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(PermissionError):
        await gateway.handle(VALID_TOKEN, tool_name, {})

    assert backend.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", LOCAL_ONLY_TOOLS)
async def test_handle_rejects_local_only_tool_with_non_empty_arguments(tool_name):
    """携带参数的本机专用工具调用同样必须被拒绝，不能因参数不同而绕过。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": [tool_name],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(PermissionError):
        await gateway.handle(
            VALID_TOKEN,
            tool_name,
            {"agent_id": FORGED_AGENT_ID, "force": True},
        )

    assert backend.calls == []


@pytest.mark.asyncio
async def test_handle_forwards_valid_memory_search_arguments():
    """有效 memory_search 参数应通过校验并调用后端。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search"],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    result = await gateway.handle(
        VALID_TOKEN, "memory_search", {"query": "test query", "limit": 5}
    )

    assert result == {"ok": True}
    assert len(backend.calls) == 1
    (tool, forwarded), kwargs = backend.calls[0]
    assert kwargs == {}
    assert tool == "memory_search"
    assert forwarded["query"] == "test query"
    assert forwarded["limit"] == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_query", ["", "   "])
async def test_handle_rejects_empty_query_without_calling_backend(empty_query):
    """空 query 必须在调用后端前被业务参数校验拒绝。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search"],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(ValueError):
        await gateway.handle(VALID_TOKEN, "memory_search", {"query": empty_query})

    assert backend.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        {"query": 123},
        {"query": "test query", "limit": "3"},
        {"query": "test query", "limit": True},
    ],
)
async def test_handle_rejects_wrong_argument_types_without_calling_backend(arguments):
    """错误参数类型必须在调用后端前被业务参数校验拒绝。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search"],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(ValueError):
        await gateway.handle(VALID_TOKEN, "memory_search", arguments)

    assert backend.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        [("query", "test query")],
        "query=test query",
        None,
    ],
)
async def test_handle_rejects_non_mapping_arguments_without_calling_backend(arguments):
    """arguments 不是字典时必须在调用后端前拒绝。"""
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search"],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(ValueError):
        await gateway.handle(VALID_TOKEN, "memory_search", arguments)

    assert backend.calls == []


# --- 回归：远程可调用工具白名单必须来自 TOOL_RPC_METHODS，而不是 TOOL_DEFINITIONS ---
# 审计发现：Gateway 曾把 TOOL_DEFINITIONS 当作可用工具来源，导致任何新增的
# 本地工具只要令牌允许列表包含它就会被远程放行。这里模拟"新增本地工具"，
# 断言即使令牌允许也必须拒绝且不触达后端。

import src.interfaces.mcp.tools as tools_module
from src.interfaces.mcp.remote_backend import TOOL_RPC_METHODS

# 模拟未来新增、但只应在本机使用的工具（未登记到 TOOL_RPC_METHODS）。
NEW_LOCAL_TOOL = "memory_local_admin_probe"


@pytest.fixture
def new_local_tool_in_definitions():
    """把新工具条目加入 TOOL_DEFINITIONS，测试结束后恢复。"""
    entry = {
        "name": NEW_LOCAL_TOOL,
        "description": "Local-only probe tool used by the regression test.",
        "inputSchema": {"type": "object", "properties": {}},
    }
    definitions = tools_module.TOOL_DEFINITIONS
    definitions.append(entry)
    try:
        yield NEW_LOCAL_TOOL
    finally:
        definitions.remove(entry)


@pytest.mark.asyncio
async def test_new_tool_added_to_definitions_is_rejected_even_if_allowlisted(
    new_local_tool_in_definitions,
):
    """TOOL_DEFINITIONS 新增工具不得自动成为远程可调用工具。

    令牌允许列表显式包含该工具也必须拒绝，且不调用后端。
    """
    tool_name = new_local_tool_in_definitions
    assert tool_name not in TOOL_RPC_METHODS

    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": ["memory_search", tool_name],
            }
        }
    )
    backend = _RecordingBackend()
    gateway = Gateway(authenticator=authenticator, backend=backend)

    with pytest.raises(PermissionError):
        await gateway.handle(VALID_TOKEN, tool_name, {})

    assert backend.calls == []


def test_gateway_remote_allowlist_is_derived_from_tool_rpc_methods():
    """远程可调用工具集合必须等于 TOOL_RPC_METHODS 的显式列表。"""
    authenticator = TokenAuthenticator(
        {VALID_TOKEN: {"agent_id": SERVER_AGENT_ID, "tools": ["memory_search"]}}
    )
    gateway = Gateway(authenticator=authenticator, backend=_RecordingBackend())

    assert gateway._remote_tools == frozenset(TOOL_RPC_METHODS)


# --- handle_identity: 供已完成鉴权的认证中间件直接调用 ---
# 认证中间件在 scope 中写入的 TokenIdentity 可绕过令牌字符串，但不能绕过
# 工具授权、参数校验或身份注入；身份必须由调用方以独立参数提供，客户端
# 无法通过 arguments 或其他可构造参数注入身份。

from src.interfaces.mcp.remote_auth import TokenIdentity


def _identity(tools=("memory_search",)) -> TokenIdentity:
    return TokenIdentity(agent_id=SERVER_AGENT_ID, tools=tuple(tools))


def _gateway(tools=("memory_search",)):
    authenticator = TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": SERVER_AGENT_ID,
                "tools": list(tools),
            }
        }
    )
    backend = _RecordingBackend()
    return Gateway(authenticator=authenticator, backend=backend), backend


@pytest.mark.asyncio
async def test_handle_identity_forwards_with_server_identity_and_preserves_arguments():
    """已认证身份直接调用时，身份字段统一由服务端覆盖且不修改原始参数。"""
    gateway, backend = _gateway()
    arguments = {
        "query": "test query",
        "limit": 3,
        "project_id": "test-project",
        "agent_id": FORGED_AGENT_ID,
        "source_agent": FORGED_SOURCE_AGENT,
    }
    original_arguments = dict(arguments)

    result = await gateway.handle_identity(_identity(), "memory_search", arguments)

    assert result == {"ok": True}
    assert len(backend.calls) == 1
    (tool, forwarded), kwargs = backend.calls[0]
    assert kwargs == {}
    assert tool == "memory_search"
    assert forwarded["agent_id"] == SERVER_AGENT_ID
    assert forwarded["source_agent"] == SERVER_AGENT_ID
    assert forwarded["query"] == "test query"
    assert forwarded is not arguments
    assert arguments == original_arguments


class _ForgedIdentity:
    """模拟客户端自造的身份对象：即使接口形状一致也必须被拒绝。"""

    agent_id = FORGED_AGENT_ID
    tools = ("memory_search",)

    def allows_tool(self, tool):
        return True


class _PrivilegedTokenIdentity(TokenIdentity):
    """模拟通过子类化放宽工具授权的伪造身份。"""

    def allows_tool(self, tool):
        return True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity",
    [
        None,
        VALID_TOKEN,
        "",
        {"agent_id": SERVER_AGENT_ID, "tools": ["memory_search"]},
        _ForgedIdentity(),
        _PrivilegedTokenIdentity(agent_id=SERVER_AGENT_ID, tools=("memory_search",)),
    ],
)
async def test_handle_identity_rejects_none_and_non_token_identity(identity):
    """None、原始令牌、字典、鸭子类型对象及身份子类一律拒绝，且不调用后端。"""
    gateway, backend = _gateway()

    with pytest.raises(PermissionError):
        await gateway.handle_identity(identity, "memory_search", {"query": "test query"})

    assert backend.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identity_tools", "tool", "arguments"),
    [
        (("memory_search",), "memory_delete", {"memory_id": "mem-1"}),
        (("memory_search", FORGED_UNKNOWN_TOOL), FORGED_UNKNOWN_TOOL, {"query": "test query"}),
        (("memory_search", "memory_spool_status"), "memory_spool_status", {}),
        (("memory_search", "memory_spool_retry"), "memory_spool_retry", {"force": True}),
    ],
)
async def test_handle_identity_applies_tool_authorization(
    identity_tools, tool, arguments
):
    """handle_identity 必须复用远程白名单、本机专用工具和令牌允许列表检查。"""
    gateway, backend = _gateway(tools=identity_tools)

    with pytest.raises(PermissionError):
        await gateway.handle_identity(_identity(identity_tools), tool, arguments)

    assert backend.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        {"query": ""},
        {"query": "   "},
        {"query": 123},
        {"query": "test query", "limit": "3"},
        {"query": "test query", "limit": True},
        [("query", "test query")],
        "query=test query",
        None,
    ],
)
async def test_handle_identity_applies_argument_validation(arguments):
    """handle_identity 必须复用业务参数校验，非法参数不会触达后端。"""
    gateway, backend = _gateway()

    with pytest.raises(ValueError):
        await gateway.handle_identity(_identity(), "memory_search", arguments)

    assert backend.calls == []


@pytest.mark.asyncio
async def test_handle_delegates_to_handle_identity_after_authentication(monkeypatch):
    """兼容入口 handle(token, ...) 必须先鉴权，再以独立身份参数委托。"""
    gateway, backend = _gateway()
    delegated = {}

    async def fake_handle_identity(identity, tool, arguments):
        delegated["identity"] = identity
        delegated["tool"] = tool
        delegated["arguments"] = arguments
        return "delegated-result"

    monkeypatch.setattr(gateway, "handle_identity", fake_handle_identity)

    arguments = {"query": "test query", "agent_id": FORGED_AGENT_ID}
    result = await gateway.handle(VALID_TOKEN, "memory_search", arguments)

    assert result == "delegated-result"
    assert delegated["identity"] == _identity()
    assert delegated["tool"] == "memory_search"
    assert delegated["arguments"] is arguments
    assert backend.calls == []


@pytest.mark.asyncio
async def test_handle_wrong_token_does_not_delegate_to_handle_identity(monkeypatch):
    """鉴权失败时不得进入 handle_identity，身份也不得由参数伪造。"""
    gateway, backend = _gateway()
    delegated = []

    async def fake_handle_identity(identity, tool, arguments):
        delegated.append(identity)
        return "delegated-result"

    monkeypatch.setattr(gateway, "handle_identity", fake_handle_identity)

    with pytest.raises(PermissionError):
        await gateway.handle(WRONG_TOKEN, "memory_search", {"query": "test query"})

    assert delegated == []
    assert backend.calls == []

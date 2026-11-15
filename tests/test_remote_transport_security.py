"""远程 MCP 传输层 Origin/Host 防护的隔离测试。

安全不变量：
1. 默认开启 DNS rebinding 防护，未知 Host 返回 421，未知 Origin 返回 403；
2. 主机/来源必须精确匹配明确配置，不能用后缀或未配置的通配符绕过；
3. 默认允许本机回环主机（含任意端口），便于本地测试而不暴露宽泛入口；
4. 显式配置的主机/来源可通过真实 MCP 客户端完成初始化与工具调用；
5. 被拒绝的请求在进入 Gateway/后端之前终止。

测试只使用 httpx 的 ASGITransport 在内存中调用应用，不监听端口、不打开
socket、不连接 daemon 或任何外部网络。
"""

import asyncio

import httpx
import pytest

pytest.importorskip("mcp")

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_mcp import build_app

# 仅用于隔离测试的固定假凭证。
TOKEN = "test-only-transport-token-0123456789abcdef0123456789abcdef"
AGENT_ID = "transport_security_test_agent"


class RecordingBackend:
    """记录后端调用；被防护层拒绝的请求不应出现在这里。"""

    def __init__(self) -> None:
        self.calls = []

    async def __call__(self, tool, params):
        self.calls.append({"tool": tool, "params": dict(params)})
        return {"results": [], "total": 0}


def _authenticator() -> TokenAuthenticator:
    return TokenAuthenticator(
        {TOKEN: {"agent_id": AGENT_ID, "tools": ["memory_search"]}}
    )


def _factory(app, base_url):
    def factory(**kwargs):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=base_url,
            **kwargs,
        )

    return factory


def _init_body():
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "transport-security-test", "version": "0"},
        },
    }


async def _raw_initialize(app, *, host, origin=None, token=TOKEN):
    headers = {
        "Host": host,
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    if origin is not None:
        headers["Origin"] = origin

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            return await client.post("/mcp", headers=headers, json=_init_body())


async def _call_memory_search(app, *, base_url, extra_headers=None):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    headers.update(extra_headers or {})

    async with app.router.lifespan_context(app):
        async with streamablehttp_client(
            f"{base_url}/mcp",
            headers=headers,
            httpx_client_factory=_factory(app, base_url),
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return await session.call_tool(
                    "memory_search", {"query": "isolated transport security"}
                )


def test_default_allows_localhost_with_any_port():
    """默认配置应允许本地回环主机，且真实 MCP 客户端可正常调用。"""

    async def run():
        backend = RecordingBackend()
        app = build_app(_authenticator(), backend)
        result = await _call_memory_search(
            app, base_url="http://localhost:8765"
        )
        assert not result.isError
        assert backend.calls == [
            {
                "tool": "memory_search",
                "params": {
                    "query": "isolated transport security",
                    "agent_id": AGENT_ID,
                    "source_agent": AGENT_ID,
                },
            }
        ]

    asyncio.run(run())


def test_default_rejects_unknown_host_before_backend():
    """未配置的 Host 必须返回 421，且不能进入后端。"""

    async def run():
        backend = RecordingBackend()
        app = build_app(_authenticator(), backend)
        response = await _raw_initialize(app, host="evil.example")
        assert response.status_code == 421
        assert response.text == "Invalid Host header"
        assert backend.calls == []

    asyncio.run(run())


def test_default_rejects_host_that_only_starts_with_localhost():
    """后缀/前缀相似主机不得借 localhost 的配置通过 Host 校验。"""

    async def run():
        backend = RecordingBackend()
        app = build_app(_authenticator(), backend)
        response = await _raw_initialize(
            app, host="localhost.evil.example", origin="http://localhost:8765"
        )
        assert response.status_code == 421
        assert backend.calls == []

    asyncio.run(run())


def test_default_rejects_unknown_origin_before_backend():
    """未配置的浏览器 Origin 必须返回 403，即使 Host 合法且令牌有效。"""

    async def run():
        backend = RecordingBackend()
        app = build_app(_authenticator(), backend)
        response = await _raw_initialize(
            app,
            host="localhost:8765",
            origin="https://localhost.evil.example",
        )
        assert response.status_code == 403
        assert response.text == "Invalid Origin header"
        assert backend.calls == []

    asyncio.run(run())


def test_explicit_host_and_origin_allow_real_client_calls():
    """显式配置的主机/来源必须放行真实 MCP 客户端调用。"""

    async def run():
        backend = RecordingBackend()
        app = build_app(
            _authenticator(),
            backend,
            allowed_hosts=["api.example.test:8443"],
            allowed_origins=["https://app.example.test:5173"],
        )
        result = await _call_memory_search(
            app,
            base_url="http://api.example.test:8443",
            extra_headers={"Origin": "https://app.example.test:5173"},
        )
        assert not result.isError
        assert [call["tool"] for call in backend.calls] == ["memory_search"]

    asyncio.run(run())


def test_explicit_allowlist_rejects_unlisted_host_and_origin():
    """显式配置不会隐式放宽到其他主机或来源。"""

    async def run():
        backend = RecordingBackend()
        # 每个应用实例的 session manager 只能 run 一次，因此分别构造。
        bad_host_app = build_app(
            _authenticator(),
            backend,
            allowed_hosts=["api.example.test:8443"],
            allowed_origins=["https://app.example.test:5173"],
        )
        bad_origin_app = build_app(
            _authenticator(),
            backend,
            allowed_hosts=["api.example.test:8443"],
            allowed_origins=["https://app.example.test:5173"],
        )

        bad_host = await _raw_initialize(
            bad_host_app,
            host="other.example.test:8443",
            origin="https://app.example.test:5173",
        )
        assert bad_host.status_code == 421

        bad_origin = await _raw_initialize(
            bad_origin_app,
            host="api.example.test:8443",
            origin="https://evil.example.test:5173",
        )
        assert bad_origin.status_code == 403

        assert backend.calls == []

    asyncio.run(run())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"allowed_hosts": ["*"]},
        {"allowed_hosts": ["*:*"]},
        {"allowed_hosts": ["*.example.test"]},
        {"allowed_hosts": ["api.*.example.test"]},
        {"allowed_origins": ["*"]},
        {"allowed_origins": ["https://*.example.test"]},
    ],
)
def test_build_app_rejects_broad_wildcard_allowlists(kwargs):
    """拒绝宽泛通配符；主机/来源只能精确配置或显式限定端口。"""

    with pytest.raises(ValueError):
        build_app(_authenticator(), RecordingBackend(), **kwargs)

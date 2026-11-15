"""Isolated integration contract for the remote MCP SDK ASGI app (no real services)."""

import asyncio

import httpx
import pytest

pytest.importorskip("mcp")

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_mcp import build_app

TOKEN = "token-alpha"
AGENT = "agent-alpha"
ALLOWED_TOOLS = ["memory_search"]

BETA_TOKEN = "token-beta"
BETA_AGENT = "agent-beta"
BETA_ALLOWED_TOOLS = ["memory_search", "memory_get"]

FORGED_AGENT = "forged-agent"


def build_authenticator() -> TokenAuthenticator:
    """Real authenticator: each token -> immutable identity + tool allow-list."""
    return TokenAuthenticator(
        {
            TOKEN: {"agent_id": AGENT, "tools": ALLOWED_TOOLS},
            BETA_TOKEN: {"agent_id": BETA_AGENT, "tools": BETA_ALLOWED_TOOLS},
        }
    )


class RecordingBackend:
    """Async callable matching ``Gateway``'s ``Backend = (tool, params)`` contract.

    Records the server-injected identity so the test can prove the client cannot
    forge ``agent_id``/``source_agent``.
    """

    def __init__(self) -> None:
        self.calls = []

    async def __call__(self, tool, params):
        snapshot = dict(params)
        self.calls.append(
            {
                "tool": tool,
                "agent_id": snapshot.get("agent_id"),
                "source_agent": snapshot.get("source_agent"),
                "params": snapshot,
            }
        )
        return {"results": [], "total": 0}


def test_remote_mcp_requires_bearer_token_and_binds_agent_identity():
    async def run():
        backend = RecordingBackend()
        app = build_app(build_authenticator(), backend)

        def factory(**kwargs):
            return httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                **kwargs,
            )

        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    "/mcp",
                    headers={
                        "Accept": "application/json, text/event-stream",
                        "Content-Type": "application/json",
                    },
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "clientInfo": {"name": "test", "version": "0"},
                        },
                    },
                )
                assert response.status_code == 401

            async with streamablehttp_client(
                "http://test/mcp",
                headers={"Authorization": f"Bearer {TOKEN}"},
                httpx_client_factory=factory,
            ) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    assert "memory_search" in {tool.name for tool in tools.tools}

                    result = await session.call_tool(
                        "memory_search", {"query": "isolated contract"}
                    )
                    assert not result.isError

        assert backend.calls
        record = backend.calls[-1]
        assert record["tool"] == "memory_search"
        assert record["agent_id"] == AGENT
        assert record["source_agent"] == AGENT
        assert record["params"]["query"] == "isolated contract"

    asyncio.run(run())


def test_concurrent_sdk_clients_keep_identity_and_tool_allowlists_isolated():
    """Two real SDK clients use separate tokens concurrently.

    Each session must see only its token's tools, every backend call must carry
    the identity bound to that session's token, and a tool outside the token's
    allow-list must be rejected without reaching the backend.
    """

    async def run():
        backend = RecordingBackend()
        app = build_app(build_authenticator(), backend)

        def factory(**kwargs):
            return httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                **kwargs,
            )

        async def exercise_client(token, expected_tools, query, allowed_extra_tool):
            async with streamablehttp_client(
                "http://test/mcp",
                headers={"Authorization": f"Bearer {token}"},
                httpx_client_factory=factory,
            ) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()

                    tools = await session.list_tools()
                    discovered = {tool.name for tool in tools.tools}
                    assert discovered == set(expected_tools)

                    # Client-supplied identity fields must be ignored/overwritten.
                    search_result = await session.call_tool(
                        "memory_search",
                        {
                            "query": query,
                            "agent_id": FORGED_AGENT,
                            "source_agent": FORGED_AGENT,
                        },
                    )
                    assert not search_result.isError

                    # The extra tool is allowed for beta (memory_get) but denied
                    # for alpha (memory_search only).
                    extra_result = await session.call_tool(
                        allowed_extra_tool,
                        {"memory_id": f"mem-{query}"},
                    )

                    return discovered, extra_result

        async with app.router.lifespan_context(app):
            # Both clients issue memory_search at the same time.
            alpha_result, beta_result = await asyncio.gather(
                exercise_client(TOKEN, ALLOWED_TOOLS, "query-alpha", "memory_get"),
                exercise_client(
                    BETA_TOKEN, BETA_ALLOWED_TOOLS, "query-beta", "memory_get"
                ),
            )

        alpha_discovered, alpha_extra = alpha_result
        beta_discovered, beta_extra = beta_result
        assert alpha_discovered == {"memory_search"}
        assert beta_discovered == {"memory_search", "memory_get"}
        assert alpha_extra.isError, "alpha must not call memory_get"
        assert not beta_extra.isError, "beta may call its allow-listed memory_get"

        searches = [
            call for call in backend.calls if call["tool"] == "memory_search"
        ]
        assert len(searches) == 2
        by_query = {call["params"]["query"]: call for call in searches}

        alpha_search = by_query["query-alpha"]
        assert alpha_search["agent_id"] == AGENT
        assert alpha_search["source_agent"] == AGENT

        beta_search = by_query["query-beta"]
        assert beta_search["agent_id"] == BETA_AGENT
        assert beta_search["source_agent"] == BETA_AGENT

        # No call may carry a client-forged identity or cross-tenant identity.
        assert all(
            call["agent_id"] != FORGED_AGENT
            and call["source_agent"] != FORGED_AGENT
            for call in backend.calls
        )
        assert all(
            not (
                call["tool"] == "memory_get"
                and (
                    call["agent_id"] == AGENT
                    or call["source_agent"] == AGENT
                )
            )
            for call in backend.calls
        )

        # memory_get reached the backend only for beta's allow-listed call.
        gets = [call for call in backend.calls if call["tool"] == "memory_get"]
        assert len(gets) == 1
        assert gets[0]["agent_id"] == BETA_AGENT
        assert gets[0]["source_agent"] == BETA_AGENT

        # The alpha memory_get call must have been rejected before the backend.
        assert not any(
            call["tool"] == "memory_get"
            and call["params"].get("memory_id") == "mem-query-alpha"
            for call in backend.calls
        )

    asyncio.run(run())

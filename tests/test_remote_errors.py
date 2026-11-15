"""Isolated contract: backend exception details must never reach MCP clients.

This test uses the real MCP SDK client against the in-process ASGI app. It does
not start a server, open a socket, or touch production services.
"""

import asyncio
import json

import httpx
import pytest

pytest.importorskip("mcp")

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_mcp import build_app

TOKEN = "token-errors"
AGENT = "agent-errors"
ALLOWED_TOOLS = ["memory_search"]

# Marker that must never appear in an MCP error response or in server logs.
SENSITIVE_MARKER = "sensitive-internal-marker"


def build_authenticator() -> TokenAuthenticator:
    return TokenAuthenticator(
        {
            TOKEN: {"agent_id": AGENT, "tools": ALLOWED_TOOLS},
        }
    )


class ExplodingBackend:
    """Backend that fails with an exception carrying sensitive internal text."""

    def __init__(self) -> None:
        self.calls = []

    async def __call__(self, tool, params):
        self.calls.append({"tool": tool, "params": dict(params)})
        raise RuntimeError(SENSITIVE_MARKER)


def _serialized(value):
    if hasattr(value, "model_dump"):
        return json.dumps(value.model_dump(), default=str, ensure_ascii=False)
    return repr(value)


def test_backend_exception_text_is_not_exposed_to_mcp_client(caplog):
    """A backend RuntimeError must become a generic MCP tool error.

    The error structure (``isError`` + text content) is preserved, but neither
    the exception message nor sensitive internal text may be returned to the
    client or written to logs.
    """

    async def run():
        backend = ExplodingBackend()
        app = build_app(build_authenticator(), backend)

        def factory(**kwargs):
            return httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                **kwargs,
            )

        async with app.router.lifespan_context(app):
            async with streamablehttp_client(
                "http://test/mcp",
                headers={"Authorization": f"Bearer {TOKEN}"},
                httpx_client_factory=factory,
            ) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()

                    result = await session.call_tool(
                        "memory_search", {"query": "trigger backend failure"}
                    )

        # The call really reached the backend; the failure is not masked before it.
        assert backend.calls
        assert backend.calls[-1]["tool"] == "memory_search"

        # Error structure is preserved.
        assert result.isError is True
        assert result.content
        text = result.content[0].text
        assert isinstance(text, str)
        assert text.startswith("Error:")

        # The sensitive marker must not leak through the MCP response.
        rendered = _serialized(result)
        assert SENSITIVE_MARKER not in text
        assert SENSITIVE_MARKER not in rendered

    with caplog.at_level("DEBUG"):
        asyncio.run(run())

    # And the original exception must not be written to logs either.
    assert SENSITIVE_MARKER not in caplog.text
    assert all(
        SENSITIVE_MARKER not in record.getMessage()
        for record in caplog.records
    )

"""2.x remote MCP health endpoint contract."""

import asyncio

import httpx
import pytest

pytest.importorskip("mcp")

from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_mcp import build_app

TOKEN = "h" * 40


class _Backend:
    calls = 0

    async def __call__(self, tool, params):
        self.calls += 1
        return {"ok": True}


def _app():
    backend = _Backend()
    app = build_app(
        TokenAuthenticator({TOKEN: {"agent_id": "health", "tools": ["memory_search"]}}),
        backend,
    )
    return app, backend


def test_healthz_is_unauthenticated_and_does_not_touch_backend():
    async def run():
        app, backend = _app()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/healthz")
        return response, backend

    response, backend = asyncio.run(run())
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert backend.calls == 0


def test_mcp_still_requires_bearer_auth():
    async def run():
        app, _ = _app()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                return await client.post(
                    "/mcp",
                    headers={"Accept": "application/json, text/event-stream"},
                    json={"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {}},
                )

    response = asyncio.run(run())
    assert response.status_code == 401

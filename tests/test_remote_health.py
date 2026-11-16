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


def test_readyz_requires_bearer_and_propagates_backend_failure():
    async def run():
        ready_calls = []
        async def ready(identity):
            ready_calls.append(identity.agent_id)
            if len(ready_calls) > 1: raise OSError('internal detail')
        app = build_app(TokenAuthenticator({TOKEN: {'agent_id':'health','tools':['memory_search']}}), _Backend(), readiness=ready)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                missing = await client.get('/readyz')
                ok = await client.get('/readyz',headers={'Authorization':'Bearer '+TOKEN})
                failed = await client.get('/readyz',headers={'Authorization':'Bearer '+TOKEN})
        assert missing.status_code == 401
        assert ok.status_code == 200
        assert failed.status_code == 503 and 'internal detail' not in failed.text
        assert ready_calls == ['health','health']
    asyncio.run(run())


def test_runtime_readiness_rejects_unhealthy_qdrant(monkeypatch, tmp_path):
    from src.interfaces.mcp import remote_runtime
    class Rpc:
        def __init__(self, _): pass
        async def __call__(self, method, params):
            assert method == 'system.health'
            return {'qdrant_ok':False,'sqlite_ok':True,'model_state':'READY'}
    monkeypatch.setattr(remote_runtime, 'RemoteRpc', Rpc)
    async def run():
        auth = TokenAuthenticator({TOKEN:{'agent_id':'health','tools':['memory_search']}})
        app = remote_runtime.build_runtime(str(tmp_path/'business'),str(tmp_path/'admin'),auth)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                result = await client.get('/readyz',headers={'Authorization':'Bearer '+TOKEN})
                assert result.status_code == 503
    asyncio.run(run())

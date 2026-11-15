"""真实 MCP 客户端 -> 内存 ASGI -> RemoteRpc -> 临时 UDS daemon 的隔离端到端测试。

仅使用 tmp_path 下的临时 Unix socket 模拟 daemon，不连接生产 daemon、
不读取生产配置，也不导入模型/Qdrant 依赖。
"""

import asyncio
import json

import pytest

pytest.importorskip("mcp")

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from src.interfaces.ipc.protocol import read_frame, write_frame
from src.interfaces.mcp.remote_auth import TokenAuthenticator
from src.interfaces.mcp.remote_backend import RemoteBackend
from src.interfaces.mcp.remote_mcp import build_app
from src.interfaces.mcp.remote_rpc import RemoteRpc

# 仅用于隔离测试的固定假凭证与身份。
TOKEN = "test-only-e2e-token-0123456789abcdef0123456789abcdef"
AGENT_ID = "e2e_test_agent"
FORGED_AGENT = "forged_agent"
DAEMON_RESULT = {
    "results": [
        {
            "memory_id": "mem-e2e-1",
            "content": "SM4 GCM uses a 12-byte IV",
            "score": 0.91,
            "status": "ACTIVE",
        }
    ],
    "total": 1,
}


def test_memory_search_round_trips_through_temp_daemon(tmp_path):
    async def run():
        socket_path = str(tmp_path / "daemon.sock")
        requests = []

        async def daemon(reader, writer):
            try:
                request = await read_frame(reader)
                requests.append(request)
                await write_frame(
                    writer,
                    {"jsonrpc": "2.0", "id": request["id"], "result": DAEMON_RESULT},
                )
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

        server = await asyncio.start_unix_server(daemon, path=socket_path)
        backend = RemoteBackend(RemoteRpc(socket_path))
        authenticator = TokenAuthenticator(
            {TOKEN: {"agent_id": AGENT_ID, "tools": ["memory_search"]}}
        )
        app = build_app(authenticator, backend)

        def factory(**kwargs):
            return httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                **kwargs,
            )

        try:
            async with app.router.lifespan_context(app):
                async with streamablehttp_client(
                    "http://test/mcp",
                    headers={"Authorization": f"Bearer {TOKEN}"},
                    httpx_client_factory=factory,
                ) as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        result = await session.call_tool(
                            "memory_search",
                            {
                                "query": "SM4 GCM IV",
                                "limit": 3,
                                # 客户端伪造身份必须被 Gateway 覆盖。
                                "agent_id": FORGED_AGENT,
                                "source_agent": FORGED_AGENT,
                            },
                        )
        finally:
            server.close()
            await server.wait_closed()

        assert not result.isError
        assert json.loads(result.content[0].text) == DAEMON_RESULT

        assert len(requests) == 1
        assert requests[0]["method"] == "memory.search"
        assert requests[0]["params"] == {
            "query": "SM4 GCM IV",
            "limit": 3,
            "agent_id": AGENT_ID,
            "source_agent": AGENT_ID,
        }

    asyncio.run(run())

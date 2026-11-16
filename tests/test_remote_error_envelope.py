"""2.x 结构化 daemon 错误信封的隔离契约测试。

使用真实 MCP SDK 客户端访问进程内 ASGI 应用，不启动监听、不连接生产。
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
from src.interfaces.mcp.remote_rpc import DaemonRpcError

TOKEN = "token-envelope"
AGENT = "agent-envelope"
SENSITIVE = "sensitive-internal-marker"


def _authenticator() -> TokenAuthenticator:
    return TokenAuthenticator(
        {TOKEN: {"agent_id": AGENT, "tools": ["memory_search"]}}
    )


class _DaemonErrorBackend:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.calls = 0

    async def __call__(self, tool, params):
        self.calls += 1
        raise self.error


def _call(error: BaseException):
    async def run():
        backend = _DaemonErrorBackend(error)
        app = build_app(_authenticator(), backend)

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
                        "memory_search", {"query": "trigger envelope"}
                    )
        return result, backend

    return asyncio.run(run())


def _rendered(result) -> str:
    if hasattr(result, "model_dump"):
        return json.dumps(result.model_dump(), default=str, ensure_ascii=False)
    return repr(result)


@pytest.mark.parametrize(
    "code, expected_class, retryable",
    [
        (-32001, "AGENT_UNAUTHORIZED", False),
        (-32002, "VERSION_CONFLICT", False),
        (-32003, "MODEL_LOADING", True),
        (-32004, "QUEUE_FULL", True),
        (-32005, "NOT_FOUND", False),
        (-32006, "IDEMPOTENCY_CONFLICT", False),
        (-32010, "REGISTRY_UNAVAILABLE", True),
        (-32601, "METHOD_NOT_FOUND", False),
        (-32602, "INVALID_ARGUMENT", False),
        (-32603, "INTERNAL", False),
    ],
)
def test_daemon_error_code_becomes_structured_envelope(
    code, expected_class, retryable
):
    result, backend = _call(DaemonRpcError(code, f"{SENSITIVE}: raw text", {}))

    assert backend.calls == 1
    assert result.isError is True
    assert result.structuredContent is not None
    envelope = result.structuredContent["amr_error"]
    assert envelope["code"] == code
    assert envelope["message_class"] == expected_class
    assert envelope["retryable"] is retryable

    # 文本只保留安全分类，不含原始错误文本。
    assert result.content[0].text == f"Error: {expected_class.replace('_', ' ').lower()}"
    assert SENSITIVE not in _rendered(result)


def test_whitelisted_data_is_preserved_and_other_fields_are_dropped():
    error = DaemonRpcError(
        -32002,
        f"VERSION_CONFLICT: {SENSITIVE}",
        {
            "memory_id": "mem-1",
            "current_version": 7,
            "expected_version": 6,
            "secret_path": SENSITIVE,
            "nested": {"token": SENSITIVE},
            "bad_list": [SENSITIVE],
            "current_version_bool": True,
        },
    )
    result, _ = _call(error)

    envelope = result.structuredContent["amr_error"]
    assert envelope["data"] == {
        "memory_id": "mem-1",
        "current_version": 7,
        "expected_version": 6,
    }
    # bool 不得作为整型字段进入信封。
    assert "current_version_bool" not in envelope["data"]
    assert SENSITIVE not in _rendered(result)


def test_unknown_code_falls_back_to_internal_without_data():
    result, _ = _call(
        DaemonRpcError(-32099, f"{SENSITIVE}", {"memory_id": SENSITIVE})
    )

    envelope = result.structuredContent["amr_error"]
    assert envelope["code"] == -32099
    assert envelope["message_class"] == "INTERNAL"
    assert envelope["retryable"] is False
    assert "data" not in envelope
    assert SENSITIVE not in _rendered(result)


def test_non_integer_code_falls_back_to_internal():
    result, _ = _call(DaemonRpcError("not-a-code", SENSITIVE, None))

    envelope = result.structuredContent["amr_error"]
    assert envelope["code"] == -32603
    assert envelope["message_class"] == "INTERNAL"


def test_generic_backend_error_has_no_structured_envelope():
    result, _ = _call(RuntimeError(SENSITIVE))

    assert result.isError is True
    assert result.structuredContent is None
    assert result.content[0].text == "Error: tool call failed"
    assert SENSITIVE not in _rendered(result)

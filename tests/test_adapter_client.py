"""2.x adapter 远程客户端契约测试（httpx MockTransport，无网络）。"""

import asyncio
import json
import ssl

import httpx
import pytest

from src.interfaces.adapter.client import (
    AdapterProtocolError,
    AdapterRemoteAuthError,
    RemoteAmrError,
    RemoteToolClient,
)

URL = "https://amr.test/mcp"


def _client(handler):
    client = RemoteToolClient(URL, "t" * 40, ssl.create_default_context())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


def _run(client, tool="memory_search", args=None):
    async def run():
        try:
            return await client.call_tool(tool, args or {"query": "x"})
        finally:
            await client.aclose()

    return asyncio.run(run())


def _ok(request):
    body = json.loads(request.content)
    return httpx.Response(
        200,
        json={
            "jsonrpc": "2.0",
            "id": body["id"],
            "result": {
                "content": [{"type": "text", "text": '{"ok": true}'}],
                "isError": False,
            },
        },
    )


def test_client_rejects_non_https_url():
    with pytest.raises(ValueError):
        RemoteToolClient("http://amr.test/mcp", "t" * 40, ssl.create_default_context())


def test_success_result_is_unwrapped_to_daemon_value():
    assert _run(_client(_ok)) == {"ok": True}


def test_http_401_maps_to_remote_auth_error():
    client = _client(lambda request: httpx.Response(401))
    with pytest.raises(AdapterRemoteAuthError):
        _run(client)


def test_response_id_mismatch_is_rejected():
    def handler(request):
        body = json.loads(request.content)
        response = json.loads(_ok(request).content)
        response["id"] = body["id"] + 1
        return httpx.Response(200, json=response)

    with pytest.raises(AdapterProtocolError):
        _run(_client(handler))


def test_structured_daemon_error_is_preserved():
    def handler(request):
        body = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": {
                    "content": [{"type": "text", "text": "Error: version conflict"}],
                    "isError": True,
                    "structuredContent": {
                        "amr_error": {
                            "code": -32002,
                            "message_class": "VERSION_CONFLICT",
                            "retryable": False,
                            "data": {"current_version": 5, "secret": "x"},
                        }
                    },
                },
            },
        )

    with pytest.raises(RemoteAmrError) as excinfo:
        _run(_client(handler))
    error = excinfo.value
    assert error.code == -32002
    assert error.message_class == "VERSION_CONFLICT"
    assert error.data == {"current_version": 5}


def test_sse_response_is_rejected_instead_of_hanging():
    def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=b"data: {}\n\n",
        )

    with pytest.raises(AdapterProtocolError):
        _run(_client(handler))


def test_oversized_response_is_rejected():
    client = RemoteToolClient(URL, "t" * 40, ssl.create_default_context(), max_response_bytes=16)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"content-type": "application/json"},
                content=b'{"jsonrpc":"2.0","id":1,"result":{"content":[]}}',
            )
        )
    )

    async def run():
        try:
            await client.call_tool("memory_search", {"query": "x"})
        finally:
            await client.aclose()

    with pytest.raises(AdapterProtocolError):
        asyncio.run(run())

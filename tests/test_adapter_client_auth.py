"""Local adapters must authenticate the caller, not just the socket path."""
import asyncio

from src.interfaces.adapter.config import AdapterBinding
from src.interfaces.adapter.server import AdapterService

TOKEN = "a" * 43
OTHER = "b" * 43


class Client:
    def __init__(self):
        self.calls = []

    async def call_tool(self, name, params):
        self.calls.append((name, params))
        return {"results": []}


def dispatch(token, method="memory.search"):
    async def run():
        service = object.__new__(AdapterService)
        responses = []
        async def write(_writer, payload):
            responses.append(payload)
            return True
        async def error(_writer, code, message, req_id):
            responses.append({"error": {"code": code, "message": message}, "id": req_id})
            return True
        service._write = write
        service._write_error = error
        binding = AdapterBinding(socket_path="/unused", agent_id="openclaw", tools=("memory_search",), token=TOKEN)
        client = Client()
        request = {"jsonrpc": "2.0", "id": 1, "method": method, "params": {"query": "test", "agent_id": "forged"}}
        if token is not None:
            request["auth_token"] = token
        await service._dispatch(binding, client, request, None)
        return responses[0], client.calls
    return asyncio.run(run())


def test_missing_wrong_and_other_agent_tokens_are_denied_without_forwarding():
    for token in (None, "wrong", OTHER, 123, "é"):
        response, calls = dispatch(token)
        assert response["error"]["code"] == -32012
        assert not calls
        assert TOKEN not in str(response)


def test_correct_token_allows_permitted_tool_and_does_not_forward_claimed_identity():
    response, calls = dispatch(TOKEN)
    assert "result" in response
    assert calls == [("memory_search", {"query": "test"})]


def test_correct_token_cannot_call_ungranted_tool():
    response, calls = dispatch(TOKEN, "memory.delete")
    assert "error" in response
    assert not calls


def test_ping_requires_client_authentication():
    response, calls = dispatch(None, "amr.ping")
    assert response["error"]["code"] == -32012
    assert not calls
    response, calls = dispatch(TOKEN, "amr.ping")
    assert response["result"] == {"status": "ok"}
    assert not calls


def test_authenticated_client_can_discover_only_granted_tools():
    response, calls = dispatch(TOKEN, 'amr.tools')
    assert response['result'] == {'tools':['memory_search']}
    assert not calls
    denied, calls = dispatch(None, 'amr.tools')
    assert denied['error']['code'] == -32012
    assert not calls

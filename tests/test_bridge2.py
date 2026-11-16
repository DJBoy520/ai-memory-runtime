"""2.x stdio MCP shim contract tests (temp adapter UDS only)."""

import asyncio
import json

from src.interfaces.ipc.protocol import read_frame, write_frame
from src.interfaces.mcp.bridge2 import AdapterBridge, AdapterRpcError, resolve_socket_path

TOKEN_RESULT = {"results": [{"memory_id": "m-1", "content": "shim"}]}


class FakeAdapter:
    def __init__(self, path):
        self.path = path
        self.requests = []
        self.error = None
        self.server = None

    async def start(self):
        self.server = await asyncio.start_unix_server(self._handle, path=self.path)
        return self

    async def _handle(self, reader, writer):
        try:
            request = await read_frame(reader)
            self.requests.append(request)
            if request.get('method') == 'amr.tools':
                payload = {'jsonrpc':'2.0','id':request['id'],'result':{'tools':['memory_search']}}
            elif self.error is not None:
                payload = {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "error": self.error,
                }
            else:
                payload = {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": TOKEN_RESULT,
                }
            await write_frame(writer, payload)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None


def test_resolve_socket_path_precedence(monkeypatch, tmp_path):
    monkeypatch.setenv("AMR2_SOCKET_PATH", str(tmp_path / "env.sock"))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    assert resolve_socket_path("/explicit.sock") == "/explicit.sock"
    assert resolve_socket_path() == str(tmp_path / "env.sock")
    monkeypatch.delenv("AMR2_SOCKET_PATH")
    monkeypatch.setenv("AMR2_AGENT", "zcode")
    assert resolve_socket_path() == str(tmp_path / "amr2-zcode.sock")
    monkeypatch.setenv("AMR2_AGENT", "../../etc/passwd")
    assert resolve_socket_path() == str(tmp_path / "amr2-etcpasswd.sock")


def test_initialize_ping_and_notifications(tmp_path):
    async def run():
        adapter = await FakeAdapter(str(tmp_path / "adapter.sock")).start()
        bridge = AdapterBridge(str(tmp_path / "adapter.sock"))
        try:
            init = await bridge.handle_request(
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
            )
            assert init["result"]["serverInfo"]["name"]
            ping = await bridge.handle_request(
                {"jsonrpc": "2.0", "id": 2, "method": "ping", "params": {}}
            )
            assert ping["result"] == {}
            notify = await bridge.handle_request(
                {"jsonrpc": "2.0", "method": "notifications/initialized"}
            )
            assert notify is None
            assert adapter.requests == []
        finally:
            await adapter.stop()

    asyncio.run(run())


def test_tools_list_hides_local_only_tools(tmp_path):
    async def run():
        adapter = await FakeAdapter(str(tmp_path / "adapter.sock")).start()
        bridge = AdapterBridge(str(tmp_path / "adapter.sock"))
        try:
            response = await bridge.handle_request(
                {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
            )
            names = {tool["name"] for tool in response["result"]["tools"]}
            assert "memory_search" in names
            assert names == {'memory_search'}
            assert "memory_spool_status" not in names
            assert "memory_spool_retry" not in names
        finally:
            await adapter.stop()

    asyncio.run(run())


def test_tool_call_forwards_method_and_strips_identity(tmp_path):
    async def run():
        adapter = await FakeAdapter(str(tmp_path / "adapter.sock")).start()
        bridge = AdapterBridge(str(tmp_path / "adapter.sock"))
        try:
            response = await bridge.handle_request(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "memory_search",
                        "arguments": {
                            "query": "x",
                            "agent_id": "forged",
                            "source_agent": "forged",
                        },
                    },
                }
            )
            assert response["result"]["isError"] is False
            assert json.loads(response["result"]["content"][0]["text"]) == TOKEN_RESULT
            request = adapter.requests[-1]
            assert request["method"] == "memory.search"
            assert request["params"] == {"query": "x"}
        finally:
            await adapter.stop()

    asyncio.run(run())


def test_adapter_error_is_preserved_as_tool_error(tmp_path):
    async def run():
        adapter = FakeAdapter(str(tmp_path / "adapter.sock"))
        adapter.error = {"code": -32002, "message": "VERSION_CONFLICT: x"}
        await adapter.start()
        bridge = AdapterBridge(str(tmp_path / "adapter.sock"))
        try:
            response = await bridge.handle_request(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "memory_search", "arguments": {"query": "x"}},
                }
            )
            assert response["result"]["isError"] is True
            assert response["result"]["content"][0]["text"].startswith("[-32002]")
        finally:
            await adapter.stop()

    asyncio.run(run())


def test_unknown_tool_and_invalid_args_do_not_touch_backend(tmp_path):
    async def run():
        adapter = await FakeAdapter(str(tmp_path / "adapter.sock")).start()
        bridge = AdapterBridge(str(tmp_path / "adapter.sock"))
        try:
            unknown = await bridge.handle_request(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "admin.shutdown", "arguments": {}},
                }
            )
            assert unknown["result"]["isError"] is True
            invalid = await bridge.handle_request(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "memory_search", "arguments": {}},
                }
            )
            assert invalid["result"]["isError"] is True
            assert adapter.requests == []
        finally:
            await adapter.stop()

    asyncio.run(run())


def test_unavailable_adapter_returns_tool_error(tmp_path):
    async def run():
        bridge = AdapterBridge(str(tmp_path / "missing.sock"))
        response = await bridge.handle_request(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "memory_search", "arguments": {"query": "x"}},
            }
        )
        assert response["result"]["isError"] is True
        assert "unavailable" in response["result"]["content"][0]["text"]

    asyncio.run(run())


def test_non_object_request_is_rejected(tmp_path):
    async def run():
        bridge = AdapterBridge(str(tmp_path / "missing.sock"))
        response = await bridge.handle_request(["not", "an", "object"])
        assert response["error"]["code"] == -32600

    asyncio.run(run())


def test_bridge_forwards_private_agent_token(tmp_path):
    async def run():
        credential = tmp_path / 'token'
        credential.write_text('a' * 43, encoding='ascii')
        credential.chmod(0o600)
        adapter = await FakeAdapter(str(tmp_path / 'adapter.sock')).start()
        try:
            bridge = AdapterBridge(str(tmp_path / 'adapter.sock'), token_file=str(credential))
            result = await bridge._call_adapter('memory.search', {'query':'test'}, 1)
            assert result == TOKEN_RESULT
            assert adapter.requests[-1]['auth_token'] == 'a' * 43
            assert 'auth_token' not in adapter.requests[-1]['params']
        finally:
            await adapter.stop()
    asyncio.run(run())

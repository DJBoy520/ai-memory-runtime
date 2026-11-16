"""Internal legacy clients must present the configured local bearer token."""
import asyncio
import json
from pathlib import Path

import pytest

from src.admin.cli import AdminClient
from src.client.transport import UdsTransport
from src.client.boundaries import TransientDeliveryError
from src.interfaces.mcp.bridge import MCPBridge
from src.interfaces.ipc.protocol import make_jsonrpc_response, read_frame, write_frame

TOKEN = "t" * 43


async def _serve(path: Path, requests: list, result):
    async def handle(reader, writer):
        try:
            request = await read_frame(reader)
            requests.append(request)
            await write_frame(writer, make_jsonrpc_response(result, request.get("id")))
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(handle, path=str(path))
    return server


def test_uds_transport_includes_token_and_missing_token_is_transient(tmp_path):
    token_file = tmp_path / "credential"
    token_file.write_text(TOKEN + "\n", encoding="ascii")
    token_file.chmod(0o600)
    transport = UdsTransport(socket_path=str(tmp_path / "unused.sock"), auth_token_file=str(token_file))
    request = json.loads(transport._build_payload("s", "agent", None, [])[4:])
    assert request["auth_token"] == TOKEN

    token_file.unlink()
    with pytest.raises(TransientDeliveryError):
        transport._build_payload("s", "agent", None, [])


@pytest.mark.asyncio
async def test_admin_client_presents_admin_token(tmp_path):
    token_file = tmp_path / "admin"
    token_file.write_text(TOKEN + "\n", encoding="ascii")
    token_file.chmod(0o600)
    requests = []
    server = await _serve(tmp_path / "admin.sock", requests, {"ok": True})
    try:
        client = AdminClient(socket_path=str(tmp_path / "admin.sock"), token_file=str(token_file))
        assert await client.status() == {"ok": True}
        assert requests[0]["auth_token"] == TOKEN
        assert requests[0]["method"] == "admin.status"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_legacy_bridge_presents_agent_token(tmp_path):
    token_file = tmp_path / "agent"
    token_file.write_text(TOKEN + "\n", encoding="ascii")
    token_file.chmod(0o600)
    requests = []
    server = await _serve(tmp_path / "bridge.sock", requests, {"status": "ok"})
    try:
        bridge = MCPBridge(
            socket_path=str(tmp_path / "bridge.sock"),
            auth_token_file=str(token_file),
        )
        assert await bridge._call_daemon("memory.search", {"query": "x"}, 1) == {"status": "ok"}
        assert requests[0]["auth_token"] == TOKEN
        assert requests[0]["method"] == "memory.search"
    finally:
        server.close()
        await server.wait_closed()

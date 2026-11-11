import json

import pytest

from config.settings import AppConfig
from src.core.session_store import SessionStore
from src.interfaces.ipc.server import UDSServer
from src.interfaces.mcp.bridge import MCPBridge


@pytest.mark.asyncio
async def test_cross_agent_handoff_through_mcp_and_uds(tmp_path):
    cfg = AppConfig()
    cfg.storage.sqlite_path = str(tmp_path / "sessions.db")
    store = SessionStore(cfg)

    async def handler(method, params):
        if method == "session.checkpoint":
            return store.save_checkpoint(**{k: v for k, v in params.items() if k != "source_agent"})
        if method == "session.get_handoff":
            return store.get_handoff(project_id=params["project_id"])
        raise ValueError("unknown method")

    path = str(tmp_path / "amr.sock")
    server = UDSServer(name="test", socket_path=path, handler=handler)
    await server.start()
    try:
        agent_a = MCPBridge(socket_path=path, source_agent="agent-a")
        agent_b = MCPBridge(socket_path=path, source_agent="agent-b")

        async def call(bridge, name, args):
            return await bridge.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": args}})

        payload = {"session_id": "s-a", "project_id": "p-a", "completed": ["implemented"], "next_steps": ["verify"], "idempotency_key": "key-1", "messages": [{"message_id": "m-1", "role": "user", "content": "original"}]}
        first = await call(agent_a, "session_checkpoint", dict(payload))
        assert not first["result"].get("isError")
        second = await call(agent_a, "session_checkpoint", dict(payload))
        assert not second["result"].get("isError")
        result = await call(agent_b, "session_get_handoff", {"project_id": "p-a"})
        data = json.loads(result["result"]["content"][0]["text"])
        assert data["handoffs"][0]["next_steps"] == ["verify"]
        isolated = await call(agent_b, "session_get_handoff", {"project_id": "p-b"})
        assert json.loads(isolated["result"]["content"][0]["text"])["handoffs"] == []
        altered = dict(payload, messages=[{"message_id": "m-1", "role": "user", "content": "different"}])
        rejected = await call(agent_a, "session_checkpoint", altered)
        assert rejected["result"].get("isError")
        assert store.get_messages("s-a")[0]["content"] == "original"
        checkpoint_id = json.loads(first["result"]["content"][0]["text"])["checkpoint_id"]
        collision = await call(agent_b, "session_checkpoint", {"session_id": "s-b", "project_id": "p-b", "checkpoint_id": checkpoint_id, "completed": ["wrong"], "next_steps": []})
        assert collision["result"].get("isError")
        assert store.get_session("s-b") is None
    finally:
        await server.stop()
        store.close()

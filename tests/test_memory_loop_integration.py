import asyncio
import json
import time

import pytest

from config.settings import AppConfig
from src.client import AmrSessionClient
from src.cognitive.scheduler import CognitiveScheduler
from src.core.session_store import SessionStore
from src.interfaces.ipc.server import UDSServer
from src.interfaces.mcp.bridge import MCPBridge
from src.main import AMRApplication
from src.service.memory_service import MemoryService


@pytest.mark.asyncio
async def test_offline_spool_distill_and_cross_agent_handoff(tmp_path):
    cfg = AppConfig()
    cfg.storage.sqlite_path = str(tmp_path / "sessions.db")
    store = SessionStore(cfg)
    socket = str(tmp_path / "amr.sock")
    text = "The project uses PostgreSQL 16 for the primary database."
    client = AmrSessionClient(spool_dir=tmp_path / "spool", socket_path=socket, agent_id="agent-a", auto_replay=False)
    queued = await asyncio.to_thread(client.record_messages, "sess-a", [{"message_id": "msg-a", "role": "user", "content": text, "timestamp": int(time.time())}], "project-a")
    assert not queued.ok
    assert client.pending_count("sess-a") == 1

    cfg.server.allowed_agents = ["agent-a", "agent-b"]
    app = AMRApplication.__new__(AMRApplication)
    app.config = cfg
    app.memory_service = MemoryService(engine=object(), qdrant_manager=object(), session_store=store, config=cfg)
    server = UDSServer(name="test", socket_path=socket, handler=app.dispatch_business_rpc)
    await server.start()
    try:
        replay = await asyncio.to_thread(client.replay_pending, "sess-a")
        assert len(replay) == 1 and replay[0].ok
        assert client.pending_count("sess-a") == 0
        assert store.get_messages("sess-a")[0]["content"] == text

        llm = {"candidates": [{"proposal_id": "candidate-a", "operation": "EXTRACT", "subject": "database", "predicate": "uses", "content": text, "evidence_source_ids": ["msg-a"], "extracted_spans": [{"message_id": "msg-a", "span": text}]}], "message_disposition": [{"message_id": "msg-a", "disposition": "EXTRACTED"}]}
        distill = await CognitiveScheduler(session_store=store).run_pipeline_for_session("sess-a", batch_id="batch-a", mock_llm_result=llm)
        assert distill["project_id"] == "project-a"
        assert distill["written_candidates"] == 1
        row = store.get_connection().execute("SELECT state FROM curation_candidates WHERE candidate_id = 'candidate-a'").fetchone()
        assert row["state"] == "PROPOSED"

        a = MCPBridge(socket_path=socket, source_agent="agent-a")
        b = MCPBridge(socket_path=socket, source_agent="agent-b")
        async def call(bridge, name, arguments):
            return await bridge.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}})
        saved = await call(a, "session_checkpoint", {"session_id": "sess-a", "project_id": "project-a", "completed": ["database selected"], "next_steps": ["apply migration"], "idempotency_key": "cp-a"})
        assert not saved["result"].get("isError")
        result = await call(b, "session_get_handoff", {"project_id": "project-a"})
        assert json.loads(result["result"]["content"][0]["text"])["handoffs"][0]["next_steps"] == ["apply migration"]
        other = await call(b, "session_get_handoff", {"project_id": "project-b"})
        assert json.loads(other["result"]["content"][0]["text"])["handoffs"] == []
    finally:
        await server.stop()
        app.memory_service.close()
        store.close()

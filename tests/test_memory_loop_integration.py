import asyncio
import json
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import time

import pytest

from config.settings import AppConfig
from src.client import AmrSessionClient
from src.cognitive.scheduler import CognitiveScheduler
from src.cognitive.evidence_validator import EvidenceValidator
from src.core.session_store import SessionStore
from src.interfaces.ipc.server import UDSServer
from src.interfaces.mcp.bridge import MCPBridge
from src.main import AMRApplication
from src.service.memory_service import MemoryService
from src.core.errors import IdempotencyConflict
from src.service.candidate_review import approve_candidate
from src.client import UdsTransport, TransientDeliveryError


def _offline_spool_worker(spool, socket_path, worker):
    client = AmrSessionClient(spool_dir=spool, socket_path=socket_path, agent_id="agent-a", auto_replay=False)
    for n in range(4):
        client.record_messages("shared-session", [{"message_id": f"{worker}-{n}", "role": "user", "content": "value"}])


def _crash_during_send(spool):
    import os
    from src.client import MockTransport
    class CrashTransport(MockTransport):
        def send_ingest(self, *args, **kwargs):
            os._exit(17)
    client = AmrSessionClient(spool_dir=spool, transport=CrashTransport(), auto_replay=False)
    client.record_messages("crash-session", [{"message_id": "m", "role": "user", "content": "value"}])


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
    app.session_store = store
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
        listed = await call(b, "memory_candidates", {"project_id": "project-a"})
        assert json.loads(listed["result"]["content"][0]["text"])["candidates"][0]["candidate_id"] == "candidate-a"
        wrong = await call(b, "memory_approve_candidate", {"project_id": "project-b", "candidate_id": "candidate-a"})
        assert wrong["result"].get("isError")
        approved = await call(b, "memory_approve_candidate", {"project_id": "project-a", "candidate_id": "candidate-a"})
        assert not approved["result"].get("isError")
        approved_data = json.loads(approved["result"]["content"][0]["text"])
        memory_id = approved_data["memory_id"]
        again = await call(b, "memory_approve_candidate", {"project_id": "project-a", "candidate_id": "candidate-a"})
        assert json.loads(again["result"]["content"][0]["text"])["memory_id"] == memory_id
        assert store.get_connection().execute("SELECT count(*) FROM memory_projection_outbox WHERE memory_id = ?", (memory_id,)).fetchone()[0] == 1
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


@pytest.mark.asyncio
async def test_mcp_bridge_replays_after_process_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("AMR_SPOOL_DIR", str(tmp_path / "mcp-spool"))
    cfg = AppConfig()
    cfg.storage.sqlite_path = str(tmp_path / "bridge.db")
    cfg.server.allowed_agents = ["agent-a"]
    store = SessionStore(cfg)
    path = str(tmp_path / "offline.sock")
    first = MCPBridge(socket_path=path, source_agent="agent-a")
    request = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "memory_ingest_session", "arguments": {"session_id": "s", "project_id": "p", "messages": [{"message_id": "m", "role": "user", "content": "saved offline"}]}}}
    queued = await first.handle_request(request)
    assert json.loads(queued["result"]["content"][0]["text"])["status"] == "queued"
    checkpoint = {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "session_checkpoint", "arguments": {"session_id": "s", "project_id": "p", "completed": ["done"], "next_steps": ["continue"], "idempotency_key": "checkpoint-key"}}}
    queued_checkpoint = await first.handle_request(checkpoint)
    assert json.loads(queued_checkpoint["result"]["content"][0]["text"])["status"] == "queued"
    status_request = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "memory_spool_status", "arguments": {}}}
    status = await first.handle_request(status_request)
    assert json.loads(status["result"]["content"][0]["text"])["pending"] == 2
    app = AMRApplication.__new__(AMRApplication)
    app.config = cfg
    app.session_store = store
    app.memory_service = MemoryService(engine=object(), qdrant_manager=object(), session_store=store, config=cfg)
    server = UDSServer(name="test", socket_path=path, handler=app.dispatch_business_rpc)
    await server.start()
    try:
        restarted = MCPBridge(socket_path=path, source_agent="agent-a")
        await restarted._session_spool()
        assert store.get_messages("s")[0]["content"] == "saved offline"
        assert restarted._spool_client.pending_count("s") == 0
        assert store.get_handoff("p")["handoffs"][0]["next_steps"] == ["continue"]
    finally:
        await server.stop()
        app.memory_service.close()
        store.close()


def test_ingest_checkpoint_idempotent_retry(tmp_path):
    cfg = AppConfig()
    cfg.storage.sqlite_path = str(tmp_path / "idempotency.db")
    store = SessionStore(cfg)
    try:
        args = {"session_id": "s", "agent_id": "a", "project_id": "p", "messages": [{"message_id": "m", "role": "user", "content": "value"}], "checkpoint": {"completed": ["done"], "next_steps": ["next"]}, "idempotency_key": "key"}
        first = store.ingest_messages(**args)
        again = store.ingest_messages(**args)
        assert again["idempotent_replay"] is True
        assert again["checkpoint"]["checkpoint_id"] == first["checkpoint"]["checkpoint_id"]
        assert len(store.get_session_checkpoints("s")) == 1
        with pytest.raises(IdempotencyConflict):
            store.ingest_messages(**dict(args, checkpoint={"completed": ["changed"]}))
    finally:
        store.close()


def test_spool_rejects_response_with_wrong_rpc_id():
    response = json.dumps({"jsonrpc": "2.0", "id": "other", "result": {"status": "success", "session_id": "s"}}).encode()
    with pytest.raises(TransientDeliveryError, match="response"):
        UdsTransport()._parse_response(response, "expected")


def test_candidate_approval_is_atomic_and_concurrent(tmp_path, monkeypatch):
    cfg = AppConfig()
    cfg.storage.sqlite_path = str(tmp_path / "review.db")
    store = SessionStore(cfg)
    try:
        store.ingest_messages("s", "a", "p", [{"message_id": "m", "role": "user", "content": "Evidence for this approved fact"}])
        conn = store.get_connection()
        conn.execute("INSERT INTO curation_candidates (candidate_id, batch_id, session_window_json, operation, matched_rule_id, content, evidence_source_ids, extracted_spans, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("c", "b", json.dumps({"session_ids": ["s"]}), "EXTRACT", "COGNITIVE_DISTILL", "Evidence for this approved fact", json.dumps(["m"]), json.dumps([{"message_id": "m", "span": "Evidence for this approved fact"}]), "PROPOSED", 1))
        conn.commit()
        original = store.enqueue_projection_task
        def fail(*args, **kwargs):
            raise RuntimeError("projection enqueue failed")
        monkeypatch.setattr(store, "enqueue_projection_task", fail)
        with pytest.raises(RuntimeError, match="enqueue"):
            approve_candidate(store, "c", "p", "reviewer")
        assert conn.execute("SELECT state FROM curation_candidates WHERE candidate_id = 'c'").fetchone()[0] == "PROPOSED"
        assert conn.execute("SELECT count(*) FROM memories").fetchone()[0] == 0
        monkeypatch.setattr(store, "enqueue_projection_task", original)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: approve_candidate(store, "c", "p", "reviewer"), range(2)))
        assert results[0]["memory_id"] == results[1]["memory_id"]
        assert conn.execute("SELECT count(*) FROM memories").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM memory_projection_outbox").fetchone()[0] == 1
    finally:
        store.close()


def test_empty_evidence_is_rejected():
    proposal = {"evidence_source_ids": ["m"], "extracted_spans": [{"message_id": "m", "span": " "}]}
    valid, reason = EvidenceValidator().validate_proposal(proposal, {"m": "real evidence"})
    assert not valid
    assert "INVALID_EVIDENCE" in reason


def test_spool_preserves_concurrent_process_batches(tmp_path):
    context = multiprocessing.get_context("spawn")
    spool = str(tmp_path / "spool")
    socket_path = str(tmp_path / "missing.sock")
    workers = [context.Process(target=_offline_spool_worker, args=(spool, socket_path, i)) for i in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(15)
        if worker.is_alive():
            worker.terminate()
            worker.join()
        assert worker.exitcode == 0
    client = AmrSessionClient(spool_dir=spool, socket_path=socket_path, auto_replay=False)
    batches = client.storage.list_batches_for_session("shared-session")
    assert len(batches) == 16
    assert len({b.messages[0].message_id for b in batches}) == 16


def test_crash_after_spooling_preserves_batch_for_replay(tmp_path):
    from src.client import MockTransport
    spool = str(tmp_path / "crash-spool")
    worker = multiprocessing.get_context("spawn").Process(target=_crash_during_send, args=(spool,))
    worker.start()
    worker.join(15)
    if worker.is_alive():
        worker.terminate()
        worker.join()
    assert worker.exitcode == 17
    transport = MockTransport()
    client = AmrSessionClient(spool_dir=spool, transport=transport, auto_replay=False)
    assert client.pending_count("crash-session") == 1
    assert client.replay_pending("crash-session")[0].ok
    assert transport.calls[0]["messages"][0]["message_id"] == "m"
    assert client.pending_count("crash-session") == 0

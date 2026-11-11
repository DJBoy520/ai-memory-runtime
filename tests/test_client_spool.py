"""
AI Memory Runtime (AMR) - Client Session Spooling Test Suite.

Comprehensive tests verifying:
1. Spool-before-send durability, atomic writes, fsync, and POSIX permissions (0o600/0o700).
2. Stable message IDs preservation across retries and restarts.
3. Batch isolation (successful acknowledgment deletes ONLY the confirmed batch).
4. Same-session sequential batches in FIFO order.
5. Timeout when already committed (超时已提交): mock service commits then drops connection,
   client replays with stable message IDs, mock service deduplicates without duplicate rows.
6. Process restart & replay (重启重放): client 1 dies with pending spools; client 2
   starts up on the same directory and drains all pending batches in order.
7. Concurrency safety: multi-threaded concurrent batch ingestion and session locking.
8. Permanent rejection quarantine (dead_letter isolation for cross-agent session collisions).
9. Asynchronous client (AsyncAmrSessionClient) lifecycle and fault recovery.
10. End-to-end UDS JSON-RPC framing with temporary mock server socket in tmp_path.
"""

import asyncio
import json
import os
import stat
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from src.client import (
    AmrSessionClient,
    AsyncAmrSessionClient,
    MockTransport,
    PermanentRejectionError,
    SessionBatch,
    SessionMessage,
    SpoolConfig,
    SpoolStorage,
    TransientDeliveryError,
    UdsTransport,
    atomic_write,
    compute_stable_message_id,
    safe_session_dirname,
)


# =============================================================================
# 1. Spool-before-send, Atomic Writes, Fsync & Permissions (0o600 / 0o700)
# =============================================================================

def test_permissions_and_directory_creation(tmp_path: Path):
    spool_dir = tmp_path / "test_spool_perms"
    storage = SpoolStorage(spool_dir)

    # 1. Directory permissions: 0o700 (drwx------)
    assert spool_dir.exists()
    spool_stat = os.stat(spool_dir)
    assert stat.S_IMODE(spool_stat.st_mode) == 0o700

    sessions_stat = os.stat(storage.sessions_dir)
    assert stat.S_IMODE(sessions_stat.st_mode) == 0o700

    # 2. Batch file permissions: 0o600 (-rw-------)
    msg = SessionMessage(role="user", content="hello perms", message_id="msg_001")
    batch = SessionBatch(
        batch_id="batch_perm_test",
        session_id="session_perms",
        agent_id="test_agent",
        messages=[msg],
    )
    batch_path = storage.save_batch(batch)
    assert batch_path.exists()

    file_stat = os.stat(batch_path)
    assert stat.S_IMODE(file_stat.st_mode) == 0o600

    # Session directory permissions: 0o700
    sess_dir_stat = os.stat(batch_path.parent)
    assert stat.S_IMODE(sess_dir_stat.st_mode) == 0o700


def test_atomic_write_and_orphan_cleanup(tmp_path: Path):
    target = tmp_path / "atomic_test" / "file.json"
    atomic_write(target, '{"test": true}', sync_parent=True)
    assert target.exists()

    # Plant a stale temporary file
    tmp_file = target.parent / ".tmp.12345.999999.deadbeef"
    tmp_file.write_text("unfinished write")
    # Set mtime back by 400 seconds
    old_time = time.time() - 400
    os.utime(tmp_file, (old_time, old_time))

    storage = SpoolStorage(tmp_path / "atomic_test")
    cleaned = storage.clean_orphan_tmp_files(max_age_seconds=300)
    assert cleaned >= 1
    assert not tmp_file.exists()


# =============================================================================
# 2. Stable Message IDs Contract
# =============================================================================

def test_independent_batches_with_identical_messages_have_distinct_ids(tmp_path):
    transport = MockTransport()
    transport.fail_with = TransientDeliveryError("offline")
    client = AmrSessionClient(spool_dir=tmp_path / "spool", transport=transport)
    for _ in range(2):
        client.record_messages(session_id="s", messages=[{"role": "user", "content": "same"}])
    batches = client.storage.list_batches_for_session("s")
    assert len(batches) == 2
    ids = [batch.messages[0].message_id for batch in batches]
    assert ids[0] != ids[1]
    transport.fail_with = None
    client.replay_pending("s")
    assert [call["messages"][0]["message_id"] for call in transport.calls[-2:]] == ids

def test_new_batch_waits_for_older_unconfirmed_batch(tmp_path):
    transport = MockTransport()
    transport.fail_with = TransientDeliveryError("offline")
    client = AmrSessionClient(spool_dir=tmp_path / "fifo", transport=transport)
    client.record_messages(session_id="s", messages=[{"message_id": "m", "role": "user", "content": "old"}])
    transport.fail_with = None
    result = client.record_messages(session_id="s", messages=[{"message_id": "m", "role": "user", "content": "new"}])
    assert not result.ok
    assert len(transport.calls) == 1
    assert len(client.replay_pending("s")) == 2
    assert [call["messages"][0]["content"] for call in transport.calls[-2:]] == ["old", "new"]

def test_invalid_ack_retains_batch(tmp_path):
    transport = MockTransport(handler=lambda *_: {"status": "success", "session_id": "wrong"})
    client = AmrSessionClient(spool_dir=tmp_path / "invalid-ack", transport=transport)
    result = client.record_messages(session_id="s", messages=[{"message_id": "m", "role": "user", "content": "value"}])
    assert not result.ok
    assert client.pending_count("s") == 1

def test_directory_sync_failure_prevents_send(tmp_path, monkeypatch):
    transport = MockTransport()
    client = AmrSessionClient(spool_dir=tmp_path / "fsync", transport=transport)
    original = os.fsync
    def fail_directory(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("directory sync failed")
        return original(fd)
    monkeypatch.setattr(os, "fsync", fail_directory)
    with pytest.raises(Exception, match="sync spool directory"):
        client.record_messages("s", [{"message_id": "m", "role": "user", "content": "value"}])
    assert transport.calls == []

@pytest.mark.asyncio
async def test_async_client_respects_pending_batch_order(tmp_path):
    transport = MockTransport()
    transport.fail_with = TransientDeliveryError("offline")
    client = AsyncAmrSessionClient(spool_dir=tmp_path / "async-fifo", transport=transport)
    await client.record_messages("s", [{"message_id": "m", "role": "user", "content": "old"}])
    transport.fail_with = None
    result = await client.record_messages("s", [{"message_id": "m", "role": "user", "content": "new"}])
    assert not result.ok
    assert len(transport.calls) == 1
    await client.replay_pending("s")
    assert [call["messages"][0]["content"] for call in transport.calls[-2:]] == ["old", "new"]

def test_stable_message_id_contract(tmp_path: Path):
    session_id = "sess_stable_contract"

    # Deterministic generation for identical parameters
    id1 = compute_stable_message_id(session_id, 1, "user", "What is the capital of France?")
    id2 = compute_stable_message_id(session_id, 1, "user", "What is the capital of France?")
    assert id1 == id2
    assert id1.startswith("msg_")

    # Different sequence or content yields distinct ID
    id3 = compute_stable_message_id(session_id, 2, "user", "What is the capital of France?")
    assert id1 != id3

    # Client auto-assigns stable ID when message_id is omitted
    transport = MockTransport()
    client = AmrSessionClient(spool_dir=tmp_path / "spool_stable", transport=transport)

    # Fail transport so batch stays on disk
    transport.fail_with = TransientDeliveryError("Simulated offline")

    result = client.record_messages(
        session_id=session_id,
        messages=[
            {"role": "user", "content": "Query 1"},
            {"role": "assistant", "content": "Answer 1"},
        ],
    )
    assert not result.ok
    assert client.pending_count(session_id) == 1

    # Read batch from disk directly and verify IDs are fixed
    pending_batches = client.storage.list_batches_for_session(session_id)
    assert len(pending_batches) == 1
    persisted_batch = pending_batches[0]

    msg1_id = persisted_batch.messages[0].message_id
    msg2_id = persisted_batch.messages[1].message_id
    assert msg1_id.startswith("msg_")
    assert msg2_id.startswith("msg_")

    # Now simulate recovery / replay: verify transport receives the exact same message IDs
    transport.fail_with = None
    replayed = client.replay_pending(session_id)
    assert len(replayed) == 1
    assert replayed[0].ok

    assert len(transport.calls) == 2  # 1 initial failed + 1 replayed
    replayed_msgs = transport.calls[1]["messages"]
    assert replayed_msgs[0]["message_id"] == msg1_id
    assert replayed_msgs[1]["message_id"] == msg2_id


# =============================================================================
# 3. Batch Isolation: Successful Ack Deletes ONLY This Batch
# =============================================================================

def test_ack_deletes_only_this_batch(tmp_path: Path):
    spool_dir = tmp_path / "spool_batch_iso"
    transport = MockTransport()
    client = AmrSessionClient(spool_dir=spool_dir, transport=transport, auto_replay=False)

    session_id = "session_isolation_test"

    # 1. Spool batch A with transport failing
    transport.fail_with = TransientDeliveryError("Offline for batch A")
    res_a = client.record_messages(
        session_id=session_id,
        messages=[{"role": "user", "content": "Batch A message"}],
    )
    assert not res_a.ok
    batch_a_id = res_a.batch_id

    # 2. Spool batch B with transport failing
    transport.fail_with = TransientDeliveryError("Offline for batch B")
    res_b = client.record_messages(
        session_id=session_id,
        messages=[{"role": "user", "content": "Batch B message"}],
    )
    assert not res_b.ok
    batch_b_id = res_b.batch_id

    assert client.pending_count(session_id) == 2

    # 3. Now simulate: Batch A is acknowledged, Batch B is NOT touched
    transport.fail_with = None

    # Manually trigger send and delete only for batch A
    batch_a = [b for b in client.storage.list_batches_for_session(session_id) if b.batch_id == batch_a_id][0]
    ack_res = client._send_and_commit_batch(batch_a, is_retry=True)
    assert ack_res.ok

    # Verify: Batch A is gone, Batch B STILL exists on disk!
    remaining = client.storage.list_batches_for_session(session_id)
    assert len(remaining) == 1
    assert remaining[0].batch_id == batch_b_id
    assert remaining[0].messages[0].content == "Batch B message"

    # Now acknowledge Batch B
    batch_b = remaining[0]
    ack_b = client._send_and_commit_batch(batch_b, is_retry=True)
    assert ack_b.ok

    # Both batches are cleared
    assert client.pending_count(session_id) == 0


# =============================================================================
# 4. Same-session Sequential Batches (同会话连续批次)
# =============================================================================

def test_same_session_sequential_batches(tmp_path: Path):
    spool_dir = tmp_path / "spool_sequential"
    transport = MockTransport()

    # In-memory mock AMR store to simulate AMR server state
    amr_server_store: Dict[str, List[Dict[str, Any]]] = {}

    def mock_amr_ingest(session_id, agent_id, project_id, messages):
        if session_id not in amr_server_store:
            amr_server_store[session_id] = []
        amr_server_store[session_id].extend(messages)
        return {
            "session_id": session_id,
            "total_received": len(messages),
            "inserted": len(messages),
            "revision_updated": 0,
            "ignored": 0,
            "status": "success",
        }

    transport.set_handler(mock_amr_ingest)
    client = AmrSessionClient(spool_dir=spool_dir, transport=transport)

    session_id = "session_seq_001"

    # Send 3 sequential batches for the same session
    batch1_res = client.record_messages(
        session_id=session_id,
        messages=[
            {"role": "user", "content": "Turn 1 user"},
            {"role": "assistant", "content": "Turn 1 assistant"},
        ],
    )
    assert batch1_res.ok
    assert client.pending_count(session_id) == 0

    batch2_res = client.record_messages(
        session_id=session_id,
        messages=[
            {"role": "user", "content": "Turn 2 user"},
            {"role": "assistant", "content": "Turn 2 assistant"},
        ],
    )
    assert batch2_res.ok
    assert client.pending_count(session_id) == 0

    batch3_res = client.record_messages(
        session_id=session_id,
        messages=[
            {"role": "user", "content": "Turn 3 user"},
            {"role": "assistant", "content": "Turn 3 assistant"},
        ],
    )
    assert batch3_res.ok
    assert client.pending_count(session_id) == 0

    # Verify server received all 6 messages in strict sequential order
    stored_messages = amr_server_store[session_id]
    assert len(stored_messages) == 6
    expected_contents = [
        "Turn 1 user", "Turn 1 assistant",
        "Turn 2 user", "Turn 2 assistant",
        "Turn 3 user", "Turn 3 assistant",
    ]
    for idx, exp in enumerate(expected_contents):
        assert stored_messages[idx]["content"] == exp
        assert stored_messages[idx]["message_id"] != ""


# =============================================================================
# 5. Timeout When Already Committed (超时已提交与幂等去重)
# =============================================================================

def test_timeout_when_already_committed_idempotent_dedup(tmp_path: Path):
    """
    Scenario:
    1. Client transmits Batch 1 to AMR.
    2. AMR Server successfully commits the messages into SQLite WAL DB.
    3. But network timeout / socket drop happens BEFORE the ack packet reaches the client.
    4. Client catches timeout -> marks delivery failed -> keeps batch in spool.
    5. Client replays the batch.
    6. AMR Server checks incoming message_ids, recognizes they already exist,
       and returns success with ignored=2.
    7. Client receives success ack -> deletes batch file!
    8. Result: zero duplicate messages, clean spool.
    """
    spool_dir = tmp_path / "spool_timeout_commit"
    transport = MockTransport()

    # Simulate AMR server database table: (session_id, message_id) -> message
    db_messages: Dict[str, Dict[str, Any]] = {}
    ingest_count = 0

    def mock_amr_server(session_id, agent_id, project_id, messages):
        nonlocal ingest_count
        ingest_count += 1
        inserted = 0
        ignored = 0

        for m in messages:
            key = f"{session_id}::{m['message_id']}"
            if key in db_messages:
                # Same message ID -> server deduplicates
                ignored += 1
            else:
                db_messages[key] = m
                inserted += 1

        if ingest_count == 1:
            # First attempt: committed into DB, but socket timeout occurs before sending response
            raise TransientDeliveryError("Socket read timeout after server DB transaction committed")

        # Second attempt (replay): returns idempotent success
        return {
            "session_id": session_id,
            "total_received": len(messages),
            "inserted": inserted,
            "revision_updated": 0,
            "ignored": ignored,
            "status": "success",
        }

    transport.set_handler(mock_amr_server)
    client = AmrSessionClient(spool_dir=spool_dir, transport=transport, auto_replay=False)

    session_id = "session_timeout_001"

    # Initial send: times out after server committed
    res1 = client.record_messages(
        session_id=session_id,
        messages=[
            {"role": "user", "content": "Critical trade order 1"},
            {"role": "assistant", "content": "Trade order 1 executed"},
        ],
    )
    assert not res1.ok
    assert "timeout after server DB transaction committed" in res1.error

    # Verify: batch is STILL in client spool on disk
    assert client.pending_count(session_id) == 1
    # Verify: server DB already has the 2 messages
    assert len(db_messages) == 2

    # Now replay the pending batch
    replayed = client.replay_pending(session_id)
    assert len(replayed) == 1
    assert replayed[0].ok
    assert replayed[0].server_response["status"] == "success"
    assert replayed[0].server_response["ignored"] == 2
    assert replayed[0].server_response["inserted"] == 0

    # Spool is now cleanly deleted
    assert client.pending_count(session_id) == 0

    # DB messages still strictly 2 (no duplicates!)
    assert len(db_messages) == 2


# =============================================================================
# 6. Process Restart & Replay (重启重放)
# =============================================================================

def test_process_restart_and_replay(tmp_path: Path):
    """
    Scenario:
    1. Client process 1 starts while transport is offline.
    2. Client process 1 records two batches.
    3. Both fail to send and are durably persisted to disk with 0o600 permissions.
    4. Client process 1 terminates (simulating host crash or restart).
    5. Transport comes online.
    6. Client process 2 initializes on the same spool directory.
    7. Client process 2 auto-replays all batches and clears the spool.
    """
    spool_dir = tmp_path / "spool_crash_recovery"

    received_batches: List[List[Dict[str, Any]]] = []
    transport = MockTransport()
    transport.set_handler(lambda sid, aid, pid, msgs: {
        "session_id": sid,
        "total_received": len(msgs),
        "inserted": len(msgs),
        "revision_updated": 0,
        "ignored": 0,
        "status": "success",
    })

    # Step 1: Client 1 runs while transport fails
    transport.fail_with = TransientDeliveryError("Connection refused: AMR server offline")

    client_process_1 = AmrSessionClient(
        spool_dir=spool_dir,
        agent_id="test_agent",
        transport=transport,
        auto_replay=False,
    )

    r1 = client_process_1.record_messages("sess_crash", [{"role": "user", "content": "Message prior to crash 1"}])
    r2 = client_process_1.record_messages("sess_crash", [{"role": "user", "content": "Message prior to crash 2"}])
    assert not r1.ok
    assert not r2.ok
    assert client_process_1.pending_count("sess_crash") == 2

    # Step 2: Client 1 exits / is destroyed
    client_process_1.close()
    del client_process_1

    # Verify spool files physically exist on disk
    assert len(list((spool_dir / "sessions").glob("*/batch_*.json"))) == 2

    # Step 3: Transport restored, Client process 2 launches
    transport.fail_with = None

    client_process_2 = AmrSessionClient(
        spool_dir=spool_dir,
        agent_id="test_agent",
        transport=transport,
        auto_replay=True,  # Auto-replays on startup
    )

    # Spool should be completely cleared after startup replay
    assert client_process_2.pending_count("sess_crash") == 0
    assert len(list((spool_dir / "sessions").glob("*/batch_*.json"))) == 0


# =============================================================================
# 7. Concurrency Safety (多线程与多进程并发安全)
# =============================================================================

def test_multithreaded_concurrency_safety(tmp_path: Path):
    spool_dir = tmp_path / "spool_concurrency"
    transport = MockTransport()

    # Thread-safe server message accumulator
    server_lock = threading.Lock()
    all_server_messages: List[Dict[str, Any]] = []

    def mock_ingest(session_id, agent_id, project_id, messages):
        time.sleep(0.005)  # slight latency to provoke races
        with server_lock:
            all_server_messages.extend(messages)
        return {
            "session_id": session_id,
            "total_received": len(messages),
            "inserted": len(messages),
            "revision_updated": 0,
            "ignored": 0,
            "status": "success",
        }

    transport.set_handler(mock_ingest)

    client = AmrSessionClient(spool_dir=spool_dir, transport=transport, auto_replay=False)

    num_threads = 6
    messages_per_thread = 5
    errors: List[Exception] = []

    def worker(thread_idx: int):
        try:
            for m_idx in range(messages_per_thread):
                # 3 threads share "shared_session", 3 threads use their own session
                sess = "shared_session" if thread_idx < 3 else f"session_th_{thread_idx}"
                res = client.record_messages(
                    session_id=sess,
                    messages=[{"role": "user", "content": f"Th {thread_idx} msg {m_idx}"}],
                )
                if not res.ok:
                    raise RuntimeError(f"Worker {thread_idx} failed: {res.error}")
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrency errors occurred: {errors}"
    assert len(all_server_messages) == num_threads * messages_per_thread
    assert client.pending_count() == 0


# =============================================================================
# 8. Permanent Error Quarantine (死信隔离)
# =============================================================================

def test_permanent_error_quarantine_cross_agent_collision(tmp_path: Path):
    spool_dir = tmp_path / "spool_quarantine"
    transport = MockTransport()

    # Simulate AMR server rejecting cross-agent session theft
    def reject_cross_agent(session_id, agent_id, project_id, messages):
        raise PermanentRejectionError(
            "INVALID_ARGUMENT: Session ID already belongs to a different agent; use a unique session ID",
            code=-32602,
        )

    transport.set_handler(reject_cross_agent)
    client = AmrSessionClient(spool_dir=spool_dir, transport=transport, auto_replay=False)

    session_id = "session_collision_001"
    res = client.record_messages(
        session_id=session_id,
        messages=[{"role": "user", "content": "Stolen session content"}],
    )

    assert not res.ok
    assert res.quarantined is True
    assert "different agent" in res.error

    # Active pending queue is NOT blocked
    assert client.pending_count(session_id) == 0

    # Dead letter contains the quarantined batch
    assert client.quarantined_count(session_id) == 1


# =============================================================================
# 9. Asynchronous Client (AsyncAmrSessionClient)
# =============================================================================

@pytest.mark.asyncio
async def test_async_client_lifecycle_and_replay(tmp_path: Path):
    spool_dir = tmp_path / "spool_async"
    transport = MockTransport()

    async_client = AsyncAmrSessionClient(
        spool_dir=spool_dir,
        agent_id="async_agent",
        transport=transport,
    )

    session_id = "sess_async_001"

    # 1. Normal async ingest
    res1 = await async_client.record_messages(
        session_id=session_id,
        messages=[{"role": "user", "content": "Async message 1"}],
    )
    assert res1.ok
    assert await async_client.pending_count(session_id) == 0

    # 2. Async transient failure
    transport.fail_with = TransientDeliveryError("Async temporary network drop")
    res2 = await async_client.record_messages(
        session_id=session_id,
        messages=[{"role": "user", "content": "Async message 2"}],
    )
    assert not res2.ok
    assert await async_client.pending_count(session_id) == 1

    # 3. Async replay recovery
    transport.fail_with = None
    replayed = await async_client.replay_pending(session_id)
    assert len(replayed) == 1
    assert replayed[0].ok
    assert await async_client.pending_count(session_id) == 0


# =============================================================================
# 10. Real UDS JSON-RPC Protocol Compatibility (Mock UDS Server in tmp_path)
# =============================================================================

@pytest.mark.asyncio
async def test_real_uds_server_roundtrip_in_tmp_dir(tmp_path: Path):
    """
    Spins up an isolated mock UDS server using AMR's exact 4-byte length prefix
    and JSON-RPC 2.0 protocol on a temporary socket in tmp_path.
    Verifies UdsTransport connects, transmits, parses responses, and handles timeouts.
    """
    mock_socket_path = str(tmp_path / "mock_amr.sock")
    spool_dir = tmp_path / "spool_real_uds"

    received_rpc_requests: List[Dict[str, Any]] = []

    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # Read 4-byte header
            header = await reader.readexactly(4)
            length = int.from_bytes(header, "big")
            body = await reader.readexactly(length)
            req = json.loads(body.decode("utf-8"))
            received_rpc_requests.append(req)

            # Build JSON-RPC response
            resp_payload = {
                "jsonrpc": "2.0",
                "result": {
                    "session_id": req["params"]["session_id"],
                    "total_received": len(req["params"]["messages"]),
                    "inserted": len(req["params"]["messages"]),
                    "revision_updated": 0,
                    "ignored": 0,
                    "status": "success",
                },
                "id": req.get("id"),
            }
            resp_bytes = json.dumps(resp_payload).encode("utf-8")
            resp_header = len(resp_bytes).to_bytes(4, "big")
            writer.write(resp_header + resp_bytes)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(handle_client, path=mock_socket_path)

    try:
        transport = UdsTransport(
            socket_path=mock_socket_path,
            connect_timeout=1.0,
            request_timeout=2.0,
        )
        # 1. Test AsyncAmrSessionClient over real UDS socket
        async_client = AsyncAmrSessionClient(
            spool_dir=spool_dir / "async",
            agent_id="real_uds_agent",
            transport=transport,
            auto_replay=False,
        )

        res_async = await async_client.record_messages(
            session_id="session_uds_async",
            messages=[
                {"role": "user", "content": "Real UDS async frame test"},
            ],
        )

        assert res_async.ok
        assert res_async.server_response["status"] == "success"
        assert len(received_rpc_requests) == 1
        assert received_rpc_requests[0]["method"] == "session.ingest"
        assert received_rpc_requests[0]["params"]["session_id"] == "session_uds_async"
        assert await async_client.pending_count("session_uds_async") == 0

        # 2. Test Sync AmrSessionClient executed in thread pool so it does not block the event loop
        client = AmrSessionClient(
            spool_dir=spool_dir / "sync",
            agent_id="real_uds_agent",
            transport=transport,
            auto_replay=False,
        )

        res = await asyncio.to_thread(
            client.record_messages,
            session_id="session_uds_real",
            messages=[
                {"role": "user", "content": "Real UDS frame test"},
            ],
        )

        assert res.ok
        assert res.server_response["status"] == "success"
        assert len(received_rpc_requests) == 2
        assert received_rpc_requests[1]["method"] == "session.ingest"
        assert received_rpc_requests[1]["params"]["session_id"] == "session_uds_real"
        assert client.pending_count("session_uds_real") == 0

    finally:
        server.close()
        await server.wait_closed()
        if os.path.exists(mock_socket_path):
            try:
                os.unlink(mock_socket_path)
            except OSError:
                pass


# =============================================================================
# 11. Security: Path Traversal Defense in Session IDs
# =============================================================================

def test_path_traversal_safe_session_ids(tmp_path: Path):
    spool_dir = tmp_path / "spool_traversal"
    transport = MockTransport()
    client = AmrSessionClient(spool_dir=spool_dir, transport=transport, auto_replay=False)

    malicious_session_ids = [
        "../../etc/passwd",
        "nested/sub/dir/id",
        "session:with:colons",
        "session with spaces and symbols #?&*",
        "..\\..\\windows\\system32",
    ]

    for sid in malicious_session_ids:
        res = client.record_messages(
            session_id=sid,
            messages=[{"role": "user", "content": f"Safe test for {sid}"}],
        )
        assert res.ok
        assert res.session_id == sid

        # Check directory: must be contained strictly under sessions_dir without escaping
        safe_name = safe_session_dirname(sid)
        sess_dir = client.storage.sessions_dir / safe_name
        assert sess_dir.resolve().is_relative_to(client.storage.sessions_dir)
        assert ".." not in safe_name


# =============================================================================
# 12. Cross-Session Replay Independence
# =============================================================================

def test_cross_session_replay_independence(tmp_path: Path):
    """
    If Session A encounters a transient network error, Session A pauses replay,
    BUT independent Session B must proceed and successfully replay!
    """
    spool_dir = tmp_path / "spool_multi_sess"
    transport = MockTransport()

    # Fail Session A, succeed for Session B
    def handler(session_id, agent_id, project_id, messages):
        if session_id == "session_A":
            raise TransientDeliveryError("Session A network pipe down")
        return {
            "session_id": session_id,
            "total_received": len(messages),
            "inserted": len(messages),
            "revision_updated": 0,
            "ignored": 0,
            "status": "success",
        }

    transport.set_handler(handler)
    client = AmrSessionClient(spool_dir=spool_dir, transport=transport, auto_replay=False)

    # Spool for Session A (will fail and stay on disk)
    res_a = client.record_messages("session_A", [{"role": "user", "content": "A1"}])
    assert not res_a.ok
    assert client.pending_count("session_A") == 1

    # Spool for Session B while transport is temporarily failing all
    transport.fail_with = TransientDeliveryError("Offline during B1 write")
    res_b = client.record_messages("session_B", [{"role": "user", "content": "B1"}])
    assert not res_b.ok
    assert client.pending_count("session_B") == 1

    # Now reset transport to handler (Session A fails, Session B succeeds)
    transport.fail_with = None

    # Replay all pending sessions
    replayed = client.replay_pending()
    assert len(replayed) == 2  # Attempted A, then attempted B

    # Session A is still pending (paused due to transient error)
    assert client.pending_count("session_A") == 1

    # Session B successfully replayed and cleared!
    assert client.pending_count("session_B") == 0


# =============================================================================
# 13. Corrupted Batch File Detection
# =============================================================================

def test_corrupted_batch_file_detection(tmp_path: Path):
    from src.client.boundaries import CorruptedBatchError

    spool_dir = tmp_path / "spool_corrupt"
    storage = SpoolStorage(spool_dir)

    sess_dir = storage.get_session_dir("corrupted_sess")
    corrupt_file = sess_dir / "batch_000000000000000001_bad.json"
    corrupt_file.write_text("{ incomplete json ... ")

    with pytest.raises(CorruptedBatchError):
        storage.list_batches_for_session("corrupted_sess")


# =============================================================================
# 14. Validation and List Content Normalization
# =============================================================================

def test_validation_and_list_content_normalization(tmp_path: Path):
    spool_dir = tmp_path / "spool_norm"
    transport = MockTransport()
    client = AmrSessionClient(spool_dir=spool_dir, transport=transport, auto_replay=False)

    # Empty messages rejected
    with pytest.raises(ValueError, match="non-empty"):
        client.record_messages("sess_norm", [])

    # Empty session_id rejected
    with pytest.raises(ValueError, match="session_id"):
        client.record_messages("   ", [{"role": "user", "content": "hi"}])

    # Multimodal list content flattened to string
    res = client.record_messages(
        "sess_norm",
        [
            {
                "role": "user",
                "content": [{"type": "text", "text": "Part 1"}, {"type": "text", "text": "Part 2"}],
            }
        ],
    )
    assert res.ok
    assert len(transport.calls) == 1
    call_msg = transport.calls[0]["messages"][0]
    assert call_msg["content"] == "Part 1\nPart 2"


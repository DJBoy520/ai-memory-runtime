import os
import shutil
import tempfile
import pytest
from src.core.session_store import SessionStore
from config.settings import AppConfig, StorageConfig, DatabaseConfig

@pytest.fixture
def temp_store():
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_sessions.db")
    config = AppConfig()
    config.storage.sqlite_path = db_path
    config.storage.wal_enabled = True
    
    store = SessionStore(config)
    yield store
    store.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def _watermark(store, session_id, message_id="m"):
    with store.get_connection() as conn:
        row = conn.execute(
            "SELECT source_revision, content_hash FROM message_source_revisions "
            "WHERE session_id = ? AND message_id = ?",
            (session_id, message_id),
        ).fetchone()
    return None if row is None else (row["source_revision"], row["content_hash"])

def test_db_initialization_and_wal(temp_store):
    with temp_store.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode;")
        mode = cursor.fetchone()[0]
        assert mode.lower() == "wal"
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        assert "sessions" in tables
        assert "messages" in tables
        assert "ingest_log" in tables

def test_ingest_messages_basic(temp_store):
    messages = [
        {"message_id": "msg_001", "role": "user", "content": "Hello", "sequence": 1, "timestamp": 1000},
        {"message_id": "msg_002", "role": "assistant", "content": "Hi there", "sequence": 2, "timestamp": 1001}
    ]
    res = temp_store.ingest_messages("sess_001", "agent_claw", "proj_test", messages)
    assert res["status"] == "success"
    assert res["total_received"] == 2
    assert res["inserted"] == 2
    assert res["ignored"] == 0
    assert res["revision_updated"] == 0
    
    sess = temp_store.get_session("sess_001")
    assert sess is not None
    assert sess["message_count"] == 2

def test_ingest_duplicate_ignored(temp_store):
    messages = [
        {"message_id": "msg_001", "role": "user", "content": "Hello", "sequence": 1, "timestamp": 1000}
    ]
    temp_store.ingest_messages("sess_001", "agent_claw", "proj_test", messages)
    
    res = temp_store.ingest_messages("sess_001", "agent_claw", "proj_test", messages)
    assert res["inserted"] == 0
    assert res["ignored"] == 1
    assert res["revision_updated"] == 0

def test_session_identity_collision_does_not_modify_existing_session(temp_store):
    first = [{"message_id": "m1", "role": "user", "content": "original", "timestamp": 1000}]
    temp_store.ingest_messages("shared-id", "agent-a", "project-a", first)
    for agent, project in [("agent-b", "project-a"), ("agent-a", "project-b")]:
        with pytest.raises(ValueError, match="different"):
            temp_store.ingest_messages("shared-id", agent, project, [{"message_id": "m1", "role": "user", "content": "overwrite"}])
    assert temp_store.get_session("shared-id")["project_id"] == "project-a"
    assert temp_store.get_messages("shared-id")[0]["content"] == "original"

def test_ingest_revision_updated(temp_store):
    messages_v1 = [
        {"message_id": "msg_001", "role": "user", "content": "Use Qdrant", "sequence": 1, "timestamp": 1000}
    ]
    temp_store.ingest_messages("sess_001", "agent_claw", "proj_test", messages_v1)
    
    messages_v2 = [
        {"message_id": "msg_001", "role": "user", "content": "Use Milvus", "sequence": 1, "timestamp": 1005}
    ]
    res = temp_store.ingest_messages("sess_001", "agent_claw", "proj_test", messages_v2)
    assert res["inserted"] == 0
    assert res["ignored"] == 0
    assert res["revision_updated"] == 1
    
    msgs = temp_store.get_messages("sess_001", ["msg_001"])
    assert len(msgs) == 1
    assert msgs[0]["content"] == "Use Milvus"


def test_versioned_messages_reject_late_or_unversioned_overwrite(temp_store):
    def message(content, revision):
        item = {"message_id": "m1", "role": "user", "content": content, "sequence": 1, "timestamp": 1000}
        if revision is not None:
            item["source_revision"] = revision
        return item

    temp_store.ingest_messages("versioned", "agent-a", "p", [message("new", 20)])
    temp_store.ingest_messages("versioned", "agent-a", "p", [message("old", 10)])
    assert temp_store.get_messages("versioned")[0]["content"] == "new"
    temp_store.ingest_messages("versioned", "agent-a", "p", [message("new", None)])
    with pytest.raises(ValueError, match="revision"):
        temp_store.ingest_messages("versioned", "agent-a", "p", [message("old", None)])
    with pytest.raises(ValueError, match="revision"):
        temp_store.ingest_messages("versioned", "agent-a", "p", [message("different", 20)])
    temp_store.ingest_messages("versioned", "agent-a", "p", [message("newer", 21)])
    assert temp_store.get_messages("versioned")[0]["content"] == "newer"


@pytest.mark.parametrize("revision", [-1, True, 1.5, "3", 2**63])
def test_invalid_source_revision_is_rejected(temp_store, revision):
    with pytest.raises(ValueError, match="source_revision"):
        temp_store.ingest_messages("v", "agent-a", "p", [{"message_id": "m", "role": "user", "content": "x", "source_revision": revision}])


def test_versioned_message_requires_stable_message_id(temp_store):
    with pytest.raises(ValueError, match="source_revision.*message"):
        temp_store.ingest_messages("v", "agent-a", "p", [{"role": "user", "content": "x", "source_revision": 1}])


def test_newer_unchanged_snapshot_advances_version(temp_store):
    def ingest(text, rev):
        return temp_store.ingest_messages("watermark", "agent-a", "p", [{"message_id": "m", "role": "user", "content": text, "source_revision": rev}])
    ingest("same", 10)
    ingest("same", 20)
    assert _watermark(temp_store, "watermark")[0] == 20
    ingest("stale revision", 15)
    assert temp_store.get_messages("watermark")[0]["content"] == "same"
    assert _watermark(temp_store, "watermark")[0] == 20


def test_later_revision_applies_in_order(temp_store):
    def ingest(text, rev):
        temp_store.ingest_messages("in-order", "agent-a", "p", [{"message_id": "m", "role": "user", "content": text, "source_revision": rev}])

    ingest("v10", 10)
    ingest("v20", 20)
    assert temp_store.get_messages("in-order")[0]["content"] == "v20"
    assert _watermark(temp_store, "in-order")[0] == 20


def test_versioned_input_cannot_overwrite_unversioned_baseline(temp_store):
    msg = {"message_id": "m", "role": "user", "content": "current"}
    temp_store.ingest_messages("legacy", "agent-a", "p", [msg])
    with pytest.raises(ValueError, match="revision"):
        temp_store.ingest_messages("legacy", "agent-a", "p", [{**msg, "content": "unknown age", "source_revision": 1}])
    temp_store.ingest_messages("legacy", "agent-a", "p", [{**msg, "source_revision": 1}])
    temp_store.ingest_messages("legacy", "agent-a", "p", [{**msg, "content": "new", "source_revision": 2}])
    assert temp_store.get_messages("legacy")[0]["content"] == "new"


def test_source_revision_detects_out_of_band_legacy_write(temp_store):
    msg = {"message_id": "m", "role": "user", "content": "current", "source_revision": 1}
    temp_store.ingest_messages("protected", "agent-a", "p", [msg])
    with temp_store.get_connection() as conn:
        conn.execute("UPDATE messages SET content_hash='legacy-modified' WHERE session_id='protected'")
    with pytest.raises(ValueError, match="revision baseline"):
        temp_store.ingest_messages("protected", "agent-a", "p", [{**msg, "source_revision": 2}])


def test_source_revision_conflict_rolls_back_whole_batch(temp_store):
    temp_store.ingest_messages("atomic", "agent-a", "p", [{"message_id": "m", "role": "user", "content": "original", "source_revision": 1}])
    with pytest.raises(ValueError, match="revision"):
        temp_store.ingest_messages("atomic", "agent-a", "p", [
            {"message_id": "new", "role": "user", "content": "new", "source_revision": 2},
            {"message_id": "m", "role": "user", "content": "conflict", "source_revision": 1},
        ])
    assert [m["message_id"] for m in temp_store.get_messages("atomic")] == ["m"]


def test_source_revision_conflict_rolls_back_prior_update(temp_store):
    temp_store.ingest_messages("atomic-update", "agent-a", "p", [
        {"message_id": "m", "role": "user", "content": "original", "source_revision": 1},
        {"message_id": "n", "role": "user", "content": "stable", "source_revision": 1},
    ])
    with pytest.raises(ValueError, match="revision"):
        temp_store.ingest_messages("atomic-update", "agent-a", "p", [
            {"message_id": "m", "role": "user", "content": "updated", "source_revision": 2},
            {"message_id": "n", "role": "user", "content": "conflict", "source_revision": 1},
        ])
    by_id = {m["message_id"]: m["content"] for m in temp_store.get_messages("atomic-update")}
    assert by_id == {"m": "original", "n": "stable"}
    assert _watermark(temp_store, "atomic-update")[0] == 1


def test_checkpoint_messages_obey_source_revision(temp_store):
    msg = {"message_id": "m", "role": "user", "content": "new", "source_revision": 10}
    temp_store.ingest_messages("checkpoint", "agent-a", "p", [msg])
    temp_store.save_checkpoint("checkpoint", "agent-a", "p", completed=[], messages=[{**msg, "content": "old", "source_revision": 9}])
    assert temp_store.get_messages("checkpoint")[0]["content"] == "new"

def test_get_messages_filter(temp_store):
    messages = [
        {"message_id": f"msg_{i:03d}", "role": "user", "content": f"Text {i}", "sequence": i, "timestamp": 1000+i}
        for i in range(10)
    ]
    temp_store.ingest_messages("sess_filter", "agent_claw", "proj_test", messages)
    
    filtered = temp_store.get_messages("sess_filter", ["msg_002", "msg_005"])
    assert len(filtered) == 2
    ids = [m["message_id"] for m in filtered]
    assert "msg_002" in ids
    assert "msg_005" in ids

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

"""
Tests for scripts/ingest_incremental_sessions.py
Verifies:
1. Incremental append to existing sessions (no whole-session skip, message_id idempotency).
2. Cross-session duplicate text (canceling cross-session content hash deduplication).
3. Exact duplicate re-import (INSERT OR IGNORE rowcount tracking).
4. Preservation of original message ID format.
"""

import gzip
import json
import sqlite3
import time
from pathlib import Path

import pytest

from scripts.ingest_incremental_sessions import (
    SessionHarvester,
    compute_content_hash,
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT,
    project_id TEXT,
    started_at INTEGER,
    ended_at INTEGER,
    status TEXT
);

CREATE TABLE IF NOT EXISTS raw_messages (
    message_id TEXT PRIMARY KEY,
    session_id TEXT,
    role TEXT,
    content TEXT,
    content_hash TEXT,
    sequence INTEGER,
    source_type TEXT,
    is_synthetic INTEGER,
    created_at INTEGER
);
"""


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    db_file = tmp_path / "test_sessions.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    return db_file


def test_cross_session_duplicate_text(temp_db: Path):
    """
    Different sessions sending the exact same text must NOT be deduplicated across sessions.
    Both messages must be inserted and counted via rowcount.
    """
    harvester = SessionHarvester(db_path=temp_db)

    identical_text = "Deploy the latest changes to staging immediately."
    content_hash = compute_content_hash("user", identical_text)

    session_a = {
        "session_id": "sess_agent_alpha_001",
        "agent_id": "hermes",
        "project_id": "general",
        "started_at": 1700000000,
        "ended_at": 1700000100,
        "status": "archived",
        "messages": [
            {
                "message_id": "msg_hermes_sess_age_1",
                "role": "user",
                "content": identical_text,
                "content_hash": content_hash,
                "sequence": 1,
                "created_at": 1700000010,
            }
        ],
    }

    session_b = {
        "session_id": "sess_agent_beta_002",
        "agent_id": "dsh",
        "project_id": "general",
        "started_at": 1700000200,
        "ended_at": 1700000300,
        "status": "archived",
        "messages": [
            {
                "message_id": "msg_dsh_sess_age_1",
                "role": "user",
                "content": identical_text,
                "content_hash": content_hash,
                "sequence": 1,
                "created_at": 1700000210,
            }
        ],
    }

    result = harvester.commit_sessions([session_a, session_b])

    assert result["sessions"] == 2
    assert result["messages"] == 2
    assert result["skipped_msgs"] == 0

    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM raw_messages")
    assert cur.fetchone()[0] == 2

    cur.execute("SELECT session_id, message_id, content_hash FROM raw_messages ORDER BY session_id")
    rows = cur.fetchall()
    assert rows[0][0] == "sess_agent_alpha_001"
    assert rows[0][1] == "msg_hermes_sess_age_1"
    assert rows[1][0] == "sess_agent_beta_002"
    assert rows[1][1] == "msg_dsh_sess_age_1"
    assert rows[0][2] == rows[1][2]  # Same content hash across different sessions
    conn.close()


def test_append_messages_to_existing_session(temp_db: Path):
    """
    When new messages are added to an existing session, existing messages are skipped
    via message_id INSERT OR IGNORE and new messages are inserted.
    """
    harvester = SessionHarvester(db_path=temp_db)
    sid = "sess_append_test_100"

    msg1 = {
        "message_id": f"msg_hermes_{sid[:8]}_1",
        "role": "user",
        "content": "Initial message in session",
        "content_hash": compute_content_hash("user", "Initial message in session"),
        "sequence": 1,
        "created_at": 1700001000,
    }

    # Batch 1: Ingest session with 1 message
    batch_1 = [{
        "session_id": sid,
        "agent_id": "hermes",
        "project_id": "general",
        "started_at": 1700001000,
        "ended_at": 1700001050,
        "status": "active",
        "messages": [msg1],
    }]
    res1 = harvester.commit_sessions(batch_1)
    assert res1["sessions"] == 1
    assert res1["messages"] == 1
    assert res1["skipped_msgs"] == 0

    # Batch 2: Session now has message 1 and new message 2 appended
    msg2 = {
        "message_id": f"msg_hermes_{sid[:8]}_2",
        "role": "assistant",
        "content": "Follow-up response appended later",
        "content_hash": compute_content_hash("assistant", "Follow-up response appended later"),
        "sequence": 2,
        "created_at": 1700001060,
    }
    batch_2 = [{
        "session_id": sid,
        "agent_id": "hermes",
        "project_id": "general",
        "started_at": 1700001000,
        "ended_at": 1700001100,
        "status": "archived",
        "messages": [msg1, msg2],
    }]
    res2 = harvester.commit_sessions(batch_2)
    assert res2["sessions"] == 0  # Session already existed in raw_sessions
    assert res2["messages"] == 1  # Only msg2 is inserted
    assert res2["skipped_msgs"] == 1  # msg1 skipped by rowcount == 0

    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    cur.execute("SELECT message_id, sequence, content FROM raw_messages WHERE session_id = ? ORDER BY sequence", (sid,))
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == f"msg_hermes_{sid[:8]}_1"
    assert rows[0][1] == 1
    assert rows[1][0] == f"msg_hermes_{sid[:8]}_2"
    assert rows[1][1] == 2
    conn.close()


def test_duplicate_reimport(temp_db: Path):
    """
    Re-importing the same sessions and messages produces 0 new inserts,
    counts all existing messages as skipped, and keeps database state clean.
    """
    harvester = SessionHarvester(db_path=temp_db)

    batch = [
        {
            "session_id": "sess_dup_001",
            "agent_id": "hermes",
            "project_id": "general",
            "started_at": 1700002000,
            "ended_at": 1700002100,
            "status": "archived",
            "messages": [
                {
                    "message_id": "msg_hermes_sess_dup_1",
                    "role": "user",
                    "content": "Hello from session 1",
                    "content_hash": compute_content_hash("user", "Hello from session 1"),
                    "sequence": 1,
                    "created_at": 1700002010,
                },
                {
                    "message_id": "msg_hermes_sess_dup_2",
                    "role": "assistant",
                    "content": "Hi there! How can I help?",
                    "content_hash": compute_content_hash("assistant", "Hi there! How can I help?"),
                    "sequence": 2,
                    "created_at": 1700002020,
                },
            ],
        },
        {
            "session_id": "sess_dup_002",
            "agent_id": "dsh",
            "project_id": "general",
            "started_at": 1700002200,
            "ended_at": 1700002300,
            "status": "archived",
            "messages": [
                {
                    "message_id": "msg_dsh_sess_dup_1",
                    "role": "user",
                    "content": "Run pipeline diagnostics",
                    "content_hash": compute_content_hash("user", "Run pipeline diagnostics"),
                    "sequence": 1,
                    "created_at": 1700002210,
                }
            ],
        },
    ]

    # First import
    res1 = harvester.commit_sessions(batch)
    assert res1["sessions"] == 2
    assert res1["messages"] == 3
    assert res1["skipped_msgs"] == 0

    # Second import (exact duplicate on same harvester instance)
    res2 = harvester.commit_sessions(batch)
    assert res2["sessions"] == 0
    assert res2["messages"] == 0
    assert res2["skipped_msgs"] == 3

    # Third import (with freshly initialized harvester instance)
    harvester_fresh = SessionHarvester(db_path=temp_db)
    res3 = harvester_fresh.commit_sessions(batch)
    assert res3["sessions"] == 0
    assert res3["messages"] == 0
    assert res3["skipped_msgs"] == 3

    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM raw_sessions")
    assert cur.fetchone()[0] == 2
    cur.execute("SELECT count(*) FROM raw_messages")
    assert cur.fetchone()[0] == 3
    conn.close()


def test_harvest_functions_no_whole_session_skip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Verify that harvest_hermes, harvest_dsh, and harvest_openclaw do not skip
    sessions that are already present in existing_session_ids.
    """
    amr_db = tmp_path / "amr.db"
    conn = sqlite3.connect(amr_db)
    conn.executescript(SCHEMA_SQL)
    conn.close()

    harvester = SessionHarvester(db_path=amr_db)
    existing_sid = "hermes_sess_existing_999"
    harvester.existing_session_ids.add(existing_sid)

    # 1. Hermes harvest test
    hermes_db_file = tmp_path / "hermes_state.db"
    h_conn = sqlite3.connect(hermes_db_file)
    h_conn.execute("""
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, started_at INTEGER, ended_at INTEGER,
            message_count INTEGER, title TEXT, archived INTEGER
        )
    """)
    h_conn.execute("""
        CREATE TABLE messages (
            session_id TEXT, role TEXT, content TEXT, timestamp INTEGER
        )
    """)
    h_conn.execute(
        "INSERT INTO sessions VALUES (?, 1700000000, 1700000100, 1, 'Hermes Test', 1)",
        (existing_sid,),
    )
    h_conn.execute(
        "INSERT INTO messages VALUES (?, 'user', 'Hermes appended message content', 1700000010)",
        (existing_sid,),
    )
    h_conn.commit()
    h_conn.close()

    monkeypatch.setattr(
        "os.path.expanduser",
        lambda p: str(hermes_db_file) if p == "~/.hermes/state.db" else p,
    )

    hermes_batch = harvester.harvest_hermes()
    assert len(hermes_batch) == 1
    assert hermes_batch[0]["session_id"] == existing_sid
    assert hermes_batch[0]["messages"][0]["message_id"] == f"msg_hermes_{existing_sid[:8]}_1"

    # 2. DSH harvest test
    dsh_dir = tmp_path / "dsh_sessions"
    sess_dsh_id = "dsh_sess_existing_888"
    harvester.existing_session_ids.add(sess_dsh_id)
    sdir = dsh_dir / f"session-{sess_dsh_id}"
    sdir.mkdir(parents=True)
    # Put a dummy zstd/file or mock subprocess check_output
    zst_dummy = sdir / "session.zstd"
    zst_dummy.write_text("dummy")

    dsh_data = (
        json.dumps({"type": "session", "createdAt": 1700000000000}) + "\n" +
        json.dumps({
            "type": "user/message",
            "time": 1700000010000,
            "data": {"content": "DSH appended message content"}
        }) + "\n"
    )

    import subprocess
    orig_check_output = subprocess.check_output

    def fake_check_output(cmd, **kwargs):
        if cmd[0] == "zstdcat":
            return dsh_data.encode("utf-8")
        return orig_check_output(cmd, **kwargs)

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)
    monkeypatch.setattr(
        "os.path.expanduser",
        lambda p: str(dsh_dir) if p == "~/.dsh/sessions" else (
            str(hermes_db_file) if p == "~/.hermes/state.db" else p
        ),
    )

    dsh_batch = harvester.harvest_dsh()
    assert len(dsh_batch) == 1
    assert dsh_batch[0]["session_id"] == sess_dsh_id
    assert dsh_batch[0]["messages"][0]["message_id"] == f"msg_dsh_{sess_dsh_id[:8]}_1"

    # 3. OpenClaw harvest test
    claw_dir = tmp_path / "claw_agents" / "agent1" / "sessions"
    claw_dir.mkdir(parents=True)
    sess_claw_id = "claw_sess_existing_777"
    harvester.existing_session_ids.add(sess_claw_id)
    traj_file = claw_dir / f"{sess_claw_id}.trajectory.jsonl"
    traj_lines = [
        json.dumps({"type": "prompt.submitted", "ts": 1700000010000, "data": {"prompt": "Claw user prompt"}}),
        json.dumps({"type": "model.completed", "ts": 1700000020000, "data": {"assistantTexts": ["Claw assistant response"]}}),
    ]
    traj_file.write_text("\n".join(traj_lines))

    monkeypatch.setattr(
        "os.path.expanduser",
        lambda p: str(tmp_path / "claw_agents" / "*" / "sessions" / "*.trajectory.jsonl*") if "openclaw" in p else (
            str(dsh_dir) if p == "~/.dsh/sessions" else (
                str(hermes_db_file) if p == "~/.hermes/state.db" else p
            )
        ),
    )

    claw_batch = harvester.harvest_openclaw()
    assert len(claw_batch) == 1
    assert claw_batch[0]["session_id"] == sess_claw_id
    assert claw_batch[0]["messages"][0]["message_id"] == f"msg_claw_{sess_claw_id[:8]}_1"
    assert claw_batch[0]["messages"][1]["message_id"] == f"msg_claw_{sess_claw_id[:8]}_2"

"""Review and atomically promote evidence-backed session memory proposals."""
import json
import time
import uuid

from src.cognitive.evidence_validator import EvidenceValidator


def _source(cursor, row, project_id):
    try:
        window = json.loads(row["session_window_json"] or "{}")
        session_ids = window.get("session_ids", [])
        evidence = json.loads(row["evidence_source_ids"] or "[]")
        spans = json.loads(row["extracted_spans"] or "[]")
    except (TypeError, ValueError):
        raise ValueError("Invalid candidate provenance")
    if not isinstance(session_ids, list) or len(session_ids) != 1 or not isinstance(session_ids[0], str):
        raise ValueError("Candidate must reference one session")
    if not isinstance(evidence, list) or not evidence or not all(isinstance(x, str) and x for x in evidence):
        raise ValueError("Candidate requires source message IDs")
    session_id = session_ids[0]
    cursor.execute("SELECT agent_id, project_id FROM sessions WHERE session_id = ?", (session_id,))
    current = cursor.fetchone()
    cursor.execute("SELECT agent_id, project_id FROM raw_sessions WHERE session_id = ?", (session_id,))
    raw = cursor.fetchone()
    if current and raw and tuple(current) != tuple(raw):
        raise ValueError("Conflicting session provenance")
    owner = current or raw
    if not owner or owner["project_id"] != project_id:
        raise ValueError("Candidate is not in the requested project")
    messages = {}
    for table in ("messages", "raw_messages"):
        cursor.execute(f"SELECT message_id, content FROM {table} WHERE session_id = ?", (session_id,))
        for item in cursor.fetchall():
            mid = item["message_id"]
            if mid in messages and messages[mid] != item["content"]:
                raise ValueError("Conflicting message provenance")
            messages[mid] = item["content"]
    valid, reason = EvidenceValidator().validate_proposal({"evidence_source_ids": evidence, "extracted_spans": spans}, messages)
    if not valid:
        raise ValueError("Candidate evidence no longer validates")
    return session_id, evidence


def list_candidates(store, project_id, limit=20):
    if not isinstance(project_id, str) or not project_id.strip():
        raise ValueError("project_id is required")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    with store._lock:
        cursor = store.get_connection().cursor()
        cursor.execute("SELECT * FROM curation_candidates WHERE state = 'PROPOSED' AND matched_rule_id = 'COGNITIVE_DISTILL' AND operation = 'EXTRACT' ORDER BY created_at DESC")
        rows = cursor.fetchall()
        results = []
        for row in rows:
            try:
                session_id, evidence = _source(cursor, row, project_id.strip())
            except ValueError:
                continue
            results.append({"candidate_id": row["candidate_id"], "project_id": project_id.strip(), "session_id": session_id, "content": row["content"], "evidence_source_ids": evidence, "extracted_spans": json.loads(row["extracted_spans"]), "state": row["state"]})
            if len(results) >= limit:
                break
        return {"candidates": results}


def approve_candidate(store, candidate_id, project_id, agent_id):
    if not all(isinstance(x, str) and x.strip() for x in (candidate_id, project_id, agent_id)):
        raise ValueError("candidate_id, project_id and agent_id are required")
    with store.locked_write_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM curation_candidates WHERE candidate_id = ?", (candidate_id,))
        row = cursor.fetchone()
        if not row or row["matched_rule_id"] != "COGNITIVE_DISTILL" or row["operation"] != "EXTRACT":
            raise ValueError("Candidate not found or unsupported")
        session_id, evidence = _source(cursor, row, project_id.strip())
        if row["state"] == "APPLIED" and row["memory_id"]:
            cursor.execute("SELECT project_id FROM memories WHERE memory_id = ?", (row["memory_id"],))
            memory = cursor.fetchone()
            if not memory or memory["project_id"] != project_id.strip():
                raise ValueError("Invalid applied candidate")
            return {"candidate_id": candidate_id, "memory_id": row["memory_id"], "status": "APPLIED", "idempotent_replay": True}
        if row["state"] != "PROPOSED" or row["memory_id"]:
            raise ValueError("Candidate is not an eligible proposal")
        memory_id = "mem_" + uuid.uuid4().hex
        result = store.create_memory_v3(memory_id=memory_id, content=row["content"], project_id=project_id.strip(), type="general", status="ACTIVE", created_by_agent=agent_id, source_refs=[f"{session_id}:{mid}" for mid in evidence], cursor=cursor)
        cursor.execute("UPDATE curation_candidates SET state = 'APPLIED', memory_id = ?, processed_at = ? WHERE candidate_id = ? AND state = 'PROPOSED'", (memory_id, int(time.time()), candidate_id))
        if cursor.rowcount != 1:
            raise ValueError("Candidate was changed concurrently")
        return {"candidate_id": candidate_id, "memory_id": memory_id, "status": "APPLIED", "projection_status": result.get("projection_status", "pending")}

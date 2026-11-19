"""Review and atomically promote evidence-backed session memory proposals."""
import hashlib
import hmac
import json
import re
import time
import uuid

from src.cognitive.evidence_validator import EvidenceValidator
from src.core.errors import VersionConflictError
from src.service.memory_status import normalize_status

_MAX_EVIDENCE_MESSAGES = 8
_MAX_EVIDENCE_CHARS = 8192
_MAX_REVIEW_JSON_BYTES = 500_000


def _review_digest(row, session_id, evidence_messages):
    reviewed = {"candidate_id": row["candidate_id"], "session_id": session_id,
                "operation": row["operation"], "target_memory_id": row["memory_id"],
                "content": row["content"], "extracted_spans": row["extracted_spans"],
                "evidence_messages": evidence_messages}
    return hashlib.sha256(json.dumps(reviewed, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


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
    if not isinstance(evidence, list) or not all(isinstance(x, str) and x for x in evidence):
        raise ValueError("Candidate requires valid source message IDs")
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
    roles = {}
    for table in ("messages", "raw_messages"):
        cursor.execute(f"SELECT message_id, role, content FROM {table} WHERE session_id = ?", (session_id,))
        for item in cursor.fetchall():
            mid = item["message_id"]
            if mid in messages and (messages[mid], roles[mid]) != (item["content"], item["role"]):
                raise ValueError("Conflicting message provenance")
            messages[mid] = item["content"]
            roles[mid] = item["role"]
    if not evidence:
        # Infer sources only if each span is attributable without ambiguity.
        if not isinstance(spans, list) or not spans:
            raise ValueError("Candidate requires attributable evidence")
        inferred = []
        for item in spans:
            if isinstance(item, dict) and isinstance(item.get("message_id"), str) and item["message_id"].strip():
                inferred.append(item["message_id"])
            else:
                span = item if isinstance(item, str) else item.get("span") if isinstance(item, dict) else None
                if not isinstance(span, str) or not span.strip():
                    raise ValueError("Candidate requires attributable evidence")
                matches = [mid for mid, content in messages.items() if span.strip() in content]
                if len(matches) != 1:
                    raise ValueError("Candidate evidence source is ambiguous")
                inferred.append(matches[0])
        evidence = list(dict.fromkeys(inferred))
    valid, reason = EvidenceValidator().validate_proposal({"evidence_source_ids": evidence, "extracted_spans": spans}, messages)
    if not valid:
        raise ValueError("Candidate evidence no longer validates")
    evidence_messages = [{"message_id": mid, "role": roles[mid], "content": messages[mid]} for mid in dict.fromkeys(evidence)]
    return session_id, evidence, evidence_messages


def list_candidates(store, project_id, limit=20):
    if not isinstance(project_id, str) or not project_id.strip():
        raise ValueError("project_id is required")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    with store._lock:
        cursor = store.get_connection().cursor()
        cursor.execute("SELECT * FROM curation_candidates WHERE state = 'PROPOSED' AND matched_rule_id = 'COGNITIVE_DISTILL' AND operation IN ('EXTRACT', 'UPDATE') ORDER BY created_at DESC")
        rows = cursor.fetchall()
        results = []
        results_truncated = False
        for row in rows:
            try:
                session_id, evidence, evidence_messages = _source(cursor, row, project_id.strip())
            except ValueError:
                continue
            target = None
            if row["operation"] == "UPDATE":
                cursor.execute("SELECT memory_id, project_id, content, version, status, projection_class FROM memories WHERE memory_id = ?", (row["memory_id"],))
                target_row = cursor.fetchone()
                if not target_row or target_row["project_id"] != project_id.strip() or normalize_status(target_row["status"]) != "ACTIVE" or target_row["projection_class"] != "v3":
                    continue
                target = {key: target_row[key] for key in ("memory_id", "content", "version", "status")}
            excerpts = []
            for message in evidence_messages[:_MAX_EVIDENCE_MESSAGES]:
                if len(message["content"]) > _MAX_EVIDENCE_CHARS:
                    message = {**message, "content": message["content"][:_MAX_EVIDENCE_CHARS],
                               "content_truncated": True, "original_characters": len(message["content"])}
                excerpts.append(message)
            item = {"candidate_id": row["candidate_id"], "project_id": project_id.strip(), "session_id": session_id, "operation": row["operation"], "target_memory": target, "content": row["content"], "evidence_source_ids": evidence, "evidence_messages": excerpts, "evidence_digest": _review_digest(row, session_id, evidence_messages), "extracted_spans": json.loads(row["extracted_spans"]), "state": row["state"]}
            omitted = len(evidence_messages) - len(excerpts)
            if omitted:
                item["omitted_evidence_messages"] = omitted
            if len(json.dumps({"candidates": results + [item]}, ensure_ascii=True).encode("utf-8")) > _MAX_REVIEW_JSON_BYTES:
                results_truncated = True
                continue
            results.append(item)
            if len(results) >= limit:
                break
        return {"candidates": results, "results_truncated": results_truncated}


def approve_candidate(store, candidate_id, project_id, agent_id, expected_version=None, change_reason=None, expected_evidence_digest=None):
    if not all(isinstance(x, str) and x.strip() for x in (candidate_id, project_id, agent_id)):
        raise ValueError("candidate_id, project_id and agent_id are required")
    with store.locked_write_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM curation_candidates WHERE candidate_id = ?", (candidate_id,))
        row = cursor.fetchone()
        if not row or row["matched_rule_id"] != "COGNITIVE_DISTILL" or row["operation"] not in ("EXTRACT", "UPDATE"):
            raise ValueError("Candidate not found or unsupported")
        session_id, evidence, evidence_messages = _source(cursor, row, project_id.strip())
        if row["state"] == "APPLIED" and row["memory_id"]:
            cursor.execute("SELECT project_id FROM memories WHERE memory_id = ?", (row["memory_id"],))
            memory = cursor.fetchone()
            if not memory or memory["project_id"] != project_id.strip():
                raise ValueError("Invalid applied candidate")
            return {"candidate_id": candidate_id, "memory_id": row["memory_id"], "status": "APPLIED", "idempotent_replay": True}
        if row["state"] != "PROPOSED" or (row["operation"] == "EXTRACT" and row["memory_id"]):
            raise ValueError("Candidate is not an eligible proposal")
        if len(evidence_messages) > _MAX_EVIDENCE_MESSAGES or any(len(message["content"]) > _MAX_EVIDENCE_CHARS for message in evidence_messages):
            raise ValueError("Candidate evidence view is incomplete; approval is unavailable")
        if not isinstance(expected_evidence_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_evidence_digest) or not hmac.compare_digest(expected_evidence_digest, _review_digest(row, session_id, evidence_messages)):
            raise ValueError("Candidate evidence changed or was not reviewed; list it again")
        source_refs = [f"{session_id}:{mid}" for mid in evidence]
        if row["operation"] == "UPDATE":
            if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < 1 or not isinstance(change_reason, str) or not change_reason.strip():
                raise ValueError("UPDATE approval requires expected_version and change_reason")
            memory_id = row["memory_id"]
            cursor.execute("SELECT project_id, version, status, projection_class, source_refs FROM memories WHERE memory_id = ?", (memory_id,))
            target = cursor.fetchone()
            if not target or target["project_id"] != project_id.strip() or normalize_status(target["status"]) != "ACTIVE" or target["projection_class"] != "v3":
                raise ValueError("Target is not an active project memory")
            if target["version"] != expected_version:
                raise VersionConflictError("VERSION_CONFLICT: Target changed after review", memory_id=memory_id, current_version=target["version"], expected_version=expected_version)
            try:
                old_refs = json.loads(target["source_refs"] or "[]")
                if not isinstance(old_refs, list) or not all(isinstance(ref, str) for ref in old_refs):
                    raise ValueError
            except (TypeError, ValueError):
                raise ValueError("Target has invalid provenance")
            cursor.execute("UPDATE memories SET source_refs = ? WHERE memory_id = ?", (json.dumps(list(dict.fromkeys(old_refs + source_refs)), ensure_ascii=False), memory_id))
            result = store.update_memory_v3(memory_id=memory_id, content=row["content"], expected_version=expected_version, change_reason=change_reason.strip(), agent_id=agent_id, cursor=cursor)
        else:
            memory_id = "mem_" + uuid.uuid4().hex
            result = store.create_memory_v3(memory_id=memory_id, content=row["content"], project_id=project_id.strip(), type="general", status="ACTIVE", created_by_agent=agent_id, source_refs=source_refs, cursor=cursor)
        cursor.execute("UPDATE curation_candidates SET state = 'APPLIED', memory_id = ?, processed_at = ? WHERE candidate_id = ? AND state = 'PROPOSED'", (memory_id, int(time.time()), candidate_id))
        if cursor.rowcount != 1:
            raise ValueError("Candidate was changed concurrently")
        return {"candidate_id": candidate_id, "memory_id": memory_id, "status": "APPLIED", "projection_status": result.get("projection_status", "pending")}

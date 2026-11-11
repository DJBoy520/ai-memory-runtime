"""
AI Memory Runtime (AMR) - Client Spool Data Models & Payloads.

Defines the core data structures for messages, batches, configurations,
and ingestion results, ensuring stable message IDs and deterministic serialization.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def compute_stable_message_id(
    session_id: str,
    sequence: int,
    role: str,
    content: str,
    prefix: str = "msg",
) -> str:
    """
    Generate a deterministic, stable message ID from session and content parameters.
    Only safe when sequence is a stable, unique position across the entire session;
    do not use batch-local sequence numbers for independently submitted batches.
    """
    norm_content = content.strip()
    raw = f"{session_id}:{sequence}:{role}:{norm_content}".encode("utf-8")
    content_digest = hashlib.sha256(raw).hexdigest()[:20]
    safe_sess = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}_{safe_sess}_{sequence:04d}_{content_digest}"


@dataclass
class SessionMessage:
    """
    Representation of an individual conversation message within an AMR session.
    """
    role: str
    content: str
    message_id: str
    sequence: int = 1
    timestamp: float = field(default_factory=time.time)
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not isinstance(self.role, str) or not self.role.strip():
            raise ValueError("SessionMessage requires a non-empty string 'role'")
        if not isinstance(self.content, str):
            if isinstance(self.content, list):
                # Flatten multimodal or segmented contents to text
                self.content = "\n".join(
                    str(p.get("text", p) if isinstance(p, dict) else p)
                    for p in self.content
                )
            else:
                self.content = str(self.content or "")
        if not isinstance(self.message_id, str) or not self.message_id.strip():
            raise ValueError("SessionMessage requires a non-empty string 'message_id'")
        if not isinstance(self.sequence, int):
            try:
                self.sequence = int(self.sequence)
            except (TypeError, ValueError):
                self.sequence = 1
        if not isinstance(self.timestamp, (int, float)):
            try:
                self.timestamp = float(self.timestamp)
            except (TypeError, ValueError):
                self.timestamp = time.time()

    def to_ingest_dict(self) -> Dict[str, Any]:
        """Convert to dict formatted for AMR session.ingest."""
        return {
            "message_id": self.message_id,
            "role": self.role,
            "content": self.content,
            "sequence": self.sequence,
            "timestamp": int(self.timestamp),
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "role": self.role,
            "content": self.content,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        session_id: str,
        index: int = 0,
        default_timestamp: Optional[float] = None,
    ) -> SessionMessage:
        """
        Create a SessionMessage from an arbitrary dict, generating a stable message_id
        if omitted.
        """
        role = str(data.get("role", "user")).strip()
        raw_content = data.get("content", "")
        if isinstance(raw_content, list):
            content = "\n".join(
                str(p.get("text", p) if isinstance(p, dict) else p)
                for p in raw_content
            )
        else:
            content = str(raw_content)

        seq = data.get("sequence")
        if seq is None:
            seq = index + 1
        else:
            try:
                seq = int(seq)
            except (TypeError, ValueError):
                seq = index + 1

        msg_id = data.get("message_id") or data.get("id")
        if not msg_id or not str(msg_id).strip():
            # A batch can contain only a slice of a conversation. Position and
            # text cannot distinguish identical messages in different batches.
            # Persist this random ID before sending; replay uses the stored ID.
            msg_id = f"msg_{uuid.uuid4().hex}"
        else:
            msg_id = str(msg_id).strip()

        ts = data.get("timestamp")
        if ts is None:
            ts = default_timestamp if default_timestamp is not None else time.time()
        else:
            try:
                ts = float(ts)
            except (TypeError, ValueError):
                ts = time.time()

        return cls(
            role=role,
            content=content,
            message_id=msg_id,
            sequence=seq,
            timestamp=ts,
            metadata=data.get("metadata"),
        )


def generate_batch_id(created_at_ns: Optional[int] = None, sequence: int = 1) -> str:
    """Generate a chronologically sortable batch identifier."""
    ns = created_at_ns if created_at_ns is not None else time.time_ns()
    rand = uuid.uuid4().hex[:8]
    return f"{ns:020d}_{sequence:06d}_{rand}"


@dataclass
class SessionBatch:
    """
    A persistent, atomic batch of messages for a specific session spooled to disk.
    """
    batch_id: str
    session_id: str
    agent_id: str
    messages: List[SessionMessage]
    project_id: Optional[str] = None
    checkpoint: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    created_at_ns: int = field(default_factory=time.time_ns)
    attempts: int = 0
    last_attempt_at: Optional[float] = None
    last_error: Optional[str] = None
    status: str = "pending"  # pending | in_flight | quarantined

    def __post_init__(self):
        if not self.session_id or not str(self.session_id).strip():
            raise ValueError("SessionBatch requires a non-empty 'session_id'")
        if not self.agent_id or not str(self.agent_id).strip():
            raise ValueError("SessionBatch requires a non-empty 'agent_id'")
        if not self.messages and not self.checkpoint:
            raise ValueError("SessionBatch requires at least one message")
        if not self.created_at_ns:
            self.created_at_ns = int(self.created_at * 1e9)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "session_id": self.session_id,
            "agent_id": self.agent_id,
            "project_id": self.project_id,
            "checkpoint": self.checkpoint,
            "idempotency_key": self.idempotency_key,
            "created_at": self.created_at,
            "created_at_ns": self.created_at_ns,
            "attempts": self.attempts,
            "last_attempt_at": self.last_attempt_at,
            "last_error": self.last_error,
            "status": self.status,
            "messages": [m.to_dict() for m in self.messages],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SessionBatch:
        msgs = [
            SessionMessage(
                role=m["role"],
                content=m["content"],
                message_id=m["message_id"],
                sequence=m.get("sequence", 1),
                timestamp=m.get("timestamp", time.time()),
                metadata=m.get("metadata"),
            )
            for m in data.get("messages", [])
        ]
        return cls(
            batch_id=data["batch_id"],
            session_id=data["session_id"],
            agent_id=data["agent_id"],
            project_id=data.get("project_id"),
            checkpoint=data.get("checkpoint"),
            idempotency_key=data.get("idempotency_key"),
            messages=msgs,
            created_at=data.get("created_at", time.time()),
            created_at_ns=data.get("created_at_ns", int(time.time() * 1e9)),
            attempts=data.get("attempts", 0),
            last_attempt_at=data.get("last_attempt_at"),
            last_error=data.get("last_error"),
            status=data.get("status", "pending"),
        )

    @classmethod
    def from_json(cls, text: str) -> SessionBatch:
        return cls.from_dict(json.loads(text))


@dataclass
class IngestResult:
    """Result of an ingestion or replay attempt."""
    ok: bool
    session_id: str
    batch_id: str
    message_count: int
    server_response: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    is_retry: bool = False
    quarantined: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SpoolConfig:
    """
    Configuration options for AmrSessionClient spooling and transmission.
    """
    spool_dir: Union[str, Path]
    agent_id: str = "default_agent"
    project_id: Optional[str] = None
    socket_path: Optional[str] = None
    file_mode: int = 0o600  # POSIX rw-------
    dir_mode: int = 0o700   # POSIX rwx------
    sync_parent_dir: bool = True
    request_timeout: float = 5.0
    connect_timeout: float = 1.0
    max_retries: int = 10
    auto_replay: bool = True
    quarantine_permanent_errors: bool = True

    def __post_init__(self):
        self.spool_dir = Path(self.spool_dir).resolve()

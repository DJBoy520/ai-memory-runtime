"""Durable JSONL spool for AMR 2.x ``session.ingest`` acknowledgements.

Design (DOC-AMR-09 section 4), mirroring the verified DSH plugin semantics:

- Persist-first: a payload is appended to the spool before the request is
  attempted, and removed only after the adapter acknowledges it.
- Transient failures (-32010 / timeout / connection errors) stay pending and
  are replayed later; permanent failures are marked once and retained for
  manual handling (never retried forever).
- Replay is non-blocking for callers, serial and re-entrancy guarded.
- Bounded capacity: at most 1000 records and 8MB on disk. Over-capacity drops
  the oldest records with a warning instead of growing without bound.
- Every filesystem operation is wrapped: IO errors are logged and never crash
  the host process. If the spool cannot be read, writes/rewrites are refused
  rather than overwriting unread history.

Standard library only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from ._client import is_transient
except ImportError:  # pragma: no cover - top-level import fallback for tests
    from _client import is_transient

logger = logging.getLogger(__name__)

MAX_SPOOL_BYTES = 8 * 1024 * 1024
MAX_SPOOL_RECORDS = 1000
SPOOL_FILE_NAME = "ingest-spool.jsonl"


def resolve_spool_dir(configured_dir: Optional[str] = None) -> str:
    """Resolve the spool directory.

    Priority: explicit ``persistence_dir``/``spool_dir`` ->
    ``AMR_HERMES_SPOOL_DIR`` -> ``~/.local/share/amr-spool/hermes`` ->
    a per-uid temporary directory.
    """
    if configured_dir is not None:
        configured = str(configured_dir).strip()
        if configured:
            return os.path.abspath(os.path.expanduser(configured))

    env_dir = os.environ.get("AMR_HERMES_SPOOL_DIR")
    if env_dir and env_dir.strip():
        return os.path.abspath(os.path.expanduser(env_dir.strip()))

    try:
        home = str(Path.home())
    except Exception:
        home = ""
    if home and home.strip():
        return os.path.join(home, ".local", "share", "amr-spool", "hermes")

    uid = os.getuid() if hasattr(os, "getuid") else "nouser"
    return os.path.join(tempfile.gettempdir(), f"amr-spool-hermes-{uid}")


DEFAULT_SPOOL_DIR = resolve_spool_dir()


def compute_record_key(payload: Dict[str, Any]) -> str:
    """Stable record key: ``session_id:first_message_id:last_message_id``.

    Identical payloads map to the same key and re-enqueue is idempotent. When
    no message id is available, a content hash is used instead of a fixed
    ``session_id::`` key so distinct messages are not collapsed.
    """
    session_id = "" if payload.get("session_id") is None else str(payload.get("session_id"))
    messages = payload.get("messages")
    if not isinstance(messages, list):
        messages = []

    def _message_id(message: Any) -> str:
        if not isinstance(message, dict):
            return ""
        value = message.get("message_id")
        return "" if value is None else str(value)

    first = _message_id(messages[0]) if messages else ""
    last = _message_id(messages[-1]) if messages else ""
    if not first and not last:
        canonical = json.dumps(
            messages if messages else payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        return f"{session_id}::h:{digest}"
    return f"{session_id}:{first}:{last}"


class IngestSpool:
    """Bounded, append-only JSONL spool for pending session ingestion."""

    def __init__(
        self,
        persistence_dir: Optional[str] = None,
        spool_dir: Optional[str] = None,
        max_bytes: int = MAX_SPOOL_BYTES,
        max_records: int = MAX_SPOOL_RECORDS,
        logger_: Optional[logging.Logger] = None,
    ):
        self.dir = resolve_spool_dir(spool_dir or persistence_dir)
        self.file_path = os.path.join(self.dir, SPOOL_FILE_NAME)
        self.max_bytes = max_bytes
        self.max_records = max_records
        self.logger = logger_ or logger
        self._records: List[Dict[str, Any]] = []
        self._loaded = False
        self._load_failed = False
        self._replaying = False
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ load
    def _load(self) -> None:
        """Load the JSONL file once. Malformed/partial lines are skipped."""
        if self._loaded:
            return
        self._loaded = True

        try:
            with open(self.file_path, "r", encoding="utf-8") as handle:
                text = handle.read()
        except FileNotFoundError:
            return
        except Exception as exc:
            # Do not treat an empty in-memory state as truth: a later rewrite
            # would silently destroy records we could not read.
            self._load_failed = True
            self.logger.warning(
                "[ingest_spool] unable to read spool %s: %s", self.file_path, exc
            )
            return

        seen = set()
        malformed = 0
        for line in text.split("\n"):
            if not line.strip():
                continue
            try:
                parsed = json.loads(line)
                if (
                    not isinstance(parsed, dict)
                    or not isinstance(parsed.get("payload"), dict)
                    or not parsed.get("payload")
                ):
                    malformed += 1
                    continue
                key = parsed.get("key")
                if not (isinstance(key, str) and key):
                    key = compute_record_key(parsed["payload"])
                if key in seen:
                    continue  # de-duplicate a repeated append after a crash
                seen.add(key)
                record = dict(parsed)
                record["key"] = key
                self._records.append(record)
            except Exception:
                malformed += 1

        if malformed > 0:
            self.logger.warning(
                "[ingest_spool] skipped %s malformed spool record(s) in %s",
                malformed,
                self.file_path,
            )

    def _ensure_dir(self) -> None:
        os.makedirs(self.dir, mode=0o700, exist_ok=True)
        try:
            os.chmod(self.dir, 0o700)
        except OSError:
            pass

    @staticmethod
    def _serialize(record: Dict[str, Any]) -> str:
        return json.dumps(record, ensure_ascii=False) + "\n"

    # --------------------------------------------------------------- public
    def enqueue(self, payload: Dict[str, Any]) -> Optional[str]:
        """Append one payload to the spool (persist-first).

        Duplicate keys are idempotent and return the existing key. Returns the
        record key, or ``None`` when invalid/not writable. Never raises.
        """
        if not isinstance(payload, dict):
            self.logger.warning("[ingest_spool] refusing to enqueue non-object payload")
            return None
        session_id = payload.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            self.logger.warning(
                "[ingest_spool] refusing to enqueue payload without session_id"
            )
            return None

        with self._lock:
            try:
                self._load()
            except Exception as exc:
                self.logger.warning(
                    "[ingest_spool] load failed before enqueue: %s", exc
                )
            if self._load_failed:
                self.logger.warning(
                    "[ingest_spool] refusing to enqueue: spool could not be loaded"
                )
                return None

            key = compute_record_key(payload)
            for record in self._records:
                if record.get("key") == key:
                    return key

            record = {
                "key": key,
                "payload": payload,
                "enqueued_at": int(time.time() * 1000),
            }
            try:
                self._ensure_dir()
                # Open/append with 0600 without relying on umask.
                fd = os.open(
                    self.file_path,
                    os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                    0o600,
                )
                try:
                    data = self._serialize(record).encode("utf-8")
                    while data:
                        written = os.write(fd, data)
                        data = data[written:]
                    # Persist-first 承诺：确认写入落到磁盘后再返回。
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except Exception as exc:
                self.logger.warning(
                    "[ingest_spool] append failed; record not persisted: %s", exc
                )
                return None

            self._records.append(record)
            try:
                self._enforce_capacity()
            except Exception as exc:
                self.logger.warning(
                    "[ingest_spool] capacity enforcement failed: %s", exc
                )
            return key

    def pending(self) -> List[Dict[str, Any]]:
        """Return a snapshot of all pending records (including permanent ones)."""
        with self._lock:
            try:
                self._load()
            except Exception as exc:
                self.logger.warning(
                    "[ingest_spool] load failed in pending(): %s", exc
                )
                return []
            if self._load_failed:
                return []
            return [dict(record) for record in self._records]

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Find one record by key, or ``None``."""
        with self._lock:
            try:
                self._load()
            except Exception:
                return None
            if self._load_failed:
                return None
            for record in self._records:
                if record.get("key") == key:
                    return dict(record)
            return None

    def remove(self, key: str) -> bool:
        """Remove one record by key and rewrite atomically. Never raises."""
        return self.remove_many([key]) > 0

    def remove_many(self, keys: Iterable[str]) -> int:
        """Remove several records in one atomic rewrite. Returns removed count."""
        key_set = {str(key) for key in keys}
        if not key_set:
            return 0
        with self._lock:
            try:
                self._load()
            except Exception as exc:
                self.logger.warning(
                    "[ingest_spool] load failed in remove(): %s", exc
                )
                return 0
            if self._load_failed:
                return 0

            before = len(self._records)
            self._records = [
                record
                for record in self._records
                if str(record.get("key")) not in key_set
            ]
            removed = before - len(self._records)
            if removed > 0:
                self._rewrite()
            return removed

    def mark_permanent(self, key: str, err: Any) -> Optional[Dict[str, Any]]:
        """Mark a record as permanently failed and retain it for manual handling.

        Idempotent: an already-permanent record is not modified again.
        """
        with self._lock:
            try:
                self._load()
            except Exception as exc:
                self.logger.warning(
                    "[ingest_spool] load failed in mark_permanent: %s", exc
                )
                return None
            if self._load_failed:
                return None

            for record in self._records:
                if record.get("key") != key:
                    continue
                if record.get("permanent"):
                    return dict(record)
                if isinstance(err, dict):
                    error_code = err.get("code")
                    error_message = err.get("message") or str(err)
                else:
                    error_code = getattr(err, "code", None)
                    error_message = getattr(err, "message", None) or err or "unknown error"
                record["permanent"] = True
                record["attempts"] = (int(record.get("attempts") or 0)) + 1
                record["last_error"] = {
                    "code": error_code,
                    "message": str(error_message),
                }
                record["failed_at"] = int(time.time() * 1000)
                self._rewrite()
                return dict(record)
            return None

    def replay(
        self,
        client: Any,
        deadline_s: float = 1.5,
        on_permanent: Optional[Any] = None,
        skip_keys: Optional[Iterable[str]] = None,
    ) -> Dict[str, int]:
        """Replay pending, non-permanent records in FIFO order.

        - Success -> remove from the spool.
        - Transient error -> leave pending and stop this sweep (the adapter is
          down; avoids one deadline stall per record).
        - Permanent error -> mark once, callback, retain.

        Returns a summary dict. Never raises.
        """
        summary = {
            "attempted": 0,
            "delivered": 0,
            "permanent": 0,
            "transient": 0,
            "skipped": 0,
            "skipped_records": 0,
        }

        with self._lock:
            if self._replaying:
                summary["skipped"] = 1
                return summary
            if client is None or not hasattr(client, "request"):
                self.logger.warning("[ingest_spool] replay skipped: invalid client")
                summary["skipped"] = 1
                return summary
            self._replaying = True

        try:
            skip_set = {str(key) for key in (skip_keys or ())}
            all_records = self.pending()
            records = [
                record
                for record in all_records
                if not record.get("permanent") and str(record.get("key")) not in skip_set
            ]
            summary["skipped_records"] = len(all_records) - len(records)

            for record in records:
                summary["attempted"] += 1
                try:
                    client.request(
                        "session.ingest",
                        record.get("payload") or {},
                        timeout=deadline_s,
                    )
                    self.remove(str(record.get("key")))
                    summary["delivered"] += 1
                except Exception as exc:
                    if is_transient(exc):
                        summary["transient"] += 1
                        # Adapter still unavailable: keep this and later records.
                        break

                    summary["permanent"] += 1
                    updated = self.mark_permanent(str(record.get("key")), exc)
                    if on_permanent is not None:
                        try:
                            on_permanent(updated or record, exc)
                        except Exception as callback_exc:
                            self.logger.warning(
                                "[ingest_spool] on_permanent callback failed: %s",
                                callback_exc,
                            )
        except Exception as exc:
            self.logger.warning("[ingest_spool] replay sweep failed open: %s", exc)
        finally:
            with self._lock:
                self._replaying = False

        return summary

    # -------------------------------------------------------------- internal
    def _file_size(self) -> int:
        try:
            return os.stat(self.file_path).st_size
        except FileNotFoundError:
            return 0
        except Exception as exc:
            self.logger.warning(
                "[ingest_spool] stat failed during capacity check: %s", exc
            )
            return 0

    def _enforce_capacity(self) -> None:
        """Drop oldest records until both the count and byte caps fit."""
        file_bytes = self._file_size()
        over_records = len(self._records) > self.max_records
        over_bytes = file_bytes > self.max_bytes
        if not over_records and not over_bytes:
            return

        drop_count = 0
        if over_records:
            # Batch trim to 90% of the cap to avoid rewriting on every append.
            target = max(1, int(self.max_records * 0.9))
            drop_count = max(0, len(self._records) - target)

        sizes = [
            len(self._serialize(record).encode("utf-8")) for record in self._records
        ]
        total = sum(sizes[drop_count:])
        while drop_count < len(self._records) and total > self.max_bytes:
            total -= sizes[drop_count]
            drop_count += 1

        if drop_count <= 0:
            # Oversized solely because of malformed/duplicate lines: compact
            # without dropping valid records.
            if over_bytes:
                self._rewrite()
            return

        dropped = self._records[:drop_count]
        del self._records[:drop_count]
        self._rewrite()
        self.logger.warning(
            "[ingest_spool] capacity exceeded (records=%s, bytes=%s); "
            "dropped %s oldest record(s)",
            len(dropped) + len(self._records),
            file_bytes,
            len(dropped),
        )

    def _rewrite(self) -> None:
        """Atomically rewrite the spool from in-memory records. Never raises."""
        if self._load_failed:
            self.logger.warning(
                "[ingest_spool] rewrite skipped: spool could not be loaded"
            )
            return

        tmp_path = None
        try:
            self._ensure_dir()
            body = "".join(self._serialize(record) for record in self._records)
            fd, tmp_path = tempfile.mkstemp(
                prefix=".ingest-spool-", suffix=".tmp", dir=self.dir
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(tmp_path, 0o600)
                os.replace(tmp_path, self.file_path)
                tmp_path = None
            finally:
                if tmp_path is not None:
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
        except Exception as exc:
            self.logger.warning(
                "[ingest_spool] rewrite failed; spool may be stale: %s", exc
            )
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass


# Convenience aliases for callers that prefer a shorter/explicit name.
Spool = IngestSpool
AmrIngestSpool = IngestSpool


"""
AI Memory Runtime (AMR) - Client Spool Storage Engine.

Handles atomic file writes, fsync, directory creation with 0o700,
file permissions with 0o600, process/thread file locking,
and isolation of confirmed batches.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import stat
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from src.client.boundaries import (
    CorruptedBatchError,
    SpoolLockError,
    SpoolStorageError,
)
from src.client.models import SessionBatch

logger = logging.getLogger("amr.client.storage")

_THREAD_LOCKS: Dict[str, threading.RLock] = {}
_THREAD_LOCKS_MUTEX = threading.Lock()


def _get_thread_lock(lock_key: str) -> threading.RLock:
    with _THREAD_LOCKS_MUTEX:
        if lock_key not in _THREAD_LOCKS:
            _THREAD_LOCKS[lock_key] = threading.RLock()
        return _THREAD_LOCKS[lock_key]


def safe_session_dirname(session_id: str) -> str:
    """
    Sanitize session ID into a safe, collision-free filesystem directory name.
    Guards against path traversal (../, /, \\, null bytes).
    """
    cleaned = re.sub(r"[^a-zA-Z0-9_\-]", "_", session_id.strip())
    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:12]
    prefix = cleaned[:32] if cleaned else "sess"
    return f"{prefix}__{digest}"


def ensure_directory(path: Path, mode: int = 0o700) -> None:
    """
    Ensure directory exists with strictly enforced POSIX permissions.
    """
    try:
        path.mkdir(parents=True, exist_ok=True, mode=mode)
        os.chmod(path, mode)
    except Exception as e:
        raise SpoolStorageError(f"Failed to create directory '{path}': {e}") from e


def atomic_write(
    target_path: Path,
    content: str | bytes,
    dir_mode: int = 0o700,
    file_mode: int = 0o600,
    sync_parent: bool = True,
) -> None:
    """
    Atomically write content to target_path using a temporary file,
    enforcing fsync, POSIX permissions (file_mode), and atomic replace.
    """
    target_path = target_path.resolve()
    parent = target_path.parent
    ensure_directory(parent, mode=dir_mode)

    raw_bytes = content.encode("utf-8") if isinstance(content, str) else content

    # Unique temp file in the same directory guarantees same filesystem for atomic rename
    tmp_name = f".tmp.{os.getpid()}.{time.time_ns()}.{uuid.uuid4().hex[:8]}"
    tmp_path = parent / tmp_name

    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
    if hasattr(os, "O_EXCL"):
        flags |= os.O_EXCL

    fd = -1
    try:
        fd = os.open(str(tmp_path), flags, file_mode)
        os.fchmod(fd, file_mode)

        # Write all bytes
        total_written = 0
        while total_written < len(raw_bytes):
            written = os.write(fd, raw_bytes[total_written:])
            if written == 0:
                raise SpoolStorageError(f"Zero bytes written to {tmp_path}")
            total_written += written

        # Fsync to ensure durability to hardware/storage cache
        os.fsync(fd)
    except Exception as e:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise SpoolStorageError(f"Failed to write spooled data to '{target_path}': {e}") from e
    else:
        try:
            os.close(fd)
        except OSError:
            pass

    # Double check file permission
    os.chmod(tmp_path, file_mode)

    # Atomic rename (POSIX guarantees replacement is atomic)
    try:
        os.replace(tmp_path, target_path)
    except Exception as e:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise SpoolStorageError(f"Atomic rename failed for '{target_path}': {e}") from e

    # Fsync parent directory to ensure directory entry is durably recorded
    if sync_parent and hasattr(os, "O_DIRECTORY"):
        dir_fd = -1
        try:
            dir_fd = os.open(str(parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            os.fsync(dir_fd)
        except OSError as e:
            raise SpoolStorageError(f"Failed to sync spool directory '{parent}': {e}") from e
        finally:
            if dir_fd >= 0:
                try:
                    os.close(dir_fd)
                except OSError:
                    pass


class FileLock:
    """
    Combined process-level (fcntl.flock) and thread-level (threading.RLock) lock.
    """
    def __init__(self, lock_path: Path, timeout: float = 10.0, mode: int = 0o600):
        self.lock_path = lock_path.resolve()
        self.timeout = timeout
        self.mode = mode
        self._fd: Optional[int] = None
        self._thread_lock = _get_thread_lock(str(self.lock_path))

    def acquire(self, timeout: Optional[float] = None) -> None:
        effective_timeout = timeout if timeout is not None else self.timeout
        deadline = time.monotonic() + effective_timeout

        # 1. Acquire thread lock first
        acquired_thread = self._thread_lock.acquire(timeout=effective_timeout)
        if not acquired_thread:
            raise SpoolLockError(
                f"Thread lock timeout ({effective_timeout}s) for '{self.lock_path}'"
            )

        try:
            ensure_directory(self.lock_path.parent)
            flags = os.O_CREAT | os.O_RDWR
            fd = os.open(str(self.lock_path), flags, self.mode)
            try:
                os.fchmod(fd, self.mode)
            except (AttributeError, OSError):
                pass

            # 2. Acquire process-level flock
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._fd = fd
                    return
                except (BlockingIOError, OSError):
                    if time.monotonic() >= deadline:
                        os.close(fd)
                        raise SpoolLockError(
                            f"Process file lock timeout ({effective_timeout}s) for '{self.lock_path}'"
                        )
                    time.sleep(0.01)
        except Exception:
            self._thread_lock.release()
            raise

    def release(self) -> None:
        try:
            if self._fd is not None:
                try:
                    fcntl.flock(self._fd, fcntl.LOCK_UN)
                except OSError:
                    pass
                try:
                    os.close(self._fd)
                except OSError:
                    pass
                self._fd = None
        finally:
            self._thread_lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class SpoolStorage:
    """
    Manages local filesystem storage for spooled session batches.
    Guarantees isolation:
    - Sessions live under `spool_dir / sessions / {safe_session_id} /`
    - Quarantined dead-letters live under `spool_dir / dead_letter / {safe_session_id} /`
    - Successful acks delete ONLY the specific acknowledged batch.
    """

    def __init__(
        self,
        spool_dir: Path,
        dir_mode: int = 0o700,
        file_mode: int = 0o600,
        sync_parent_dir: bool = True,
    ):
        self.spool_dir = Path(spool_dir).resolve()
        self.dir_mode = dir_mode
        self.file_mode = file_mode
        self.sync_parent_dir = sync_parent_dir

        self.sessions_dir = self.spool_dir / "sessions"
        self.dead_letter_dir = self.spool_dir / "dead_letter"

        ensure_directory(self.spool_dir, mode=self.dir_mode)
        ensure_directory(self.sessions_dir, mode=self.dir_mode)
        ensure_directory(self.dead_letter_dir, mode=self.dir_mode)

    def get_session_dir(self, session_id: str) -> Path:
        safe_name = safe_session_dirname(session_id)
        d = self.sessions_dir / safe_name
        ensure_directory(d, mode=self.dir_mode)
        return d

    def get_session_lock(self, session_id: str, timeout: float = 10.0) -> FileLock:
        sess_dir = self.get_session_dir(session_id)
        lock_path = sess_dir / ".session.lock"
        return FileLock(lock_path, timeout=timeout, mode=self.file_mode)

    def get_global_lock(self, timeout: float = 10.0) -> FileLock:
        lock_path = self.spool_dir / ".spool.lock"
        return FileLock(lock_path, timeout=timeout, mode=self.file_mode)

    def _batch_file_path(self, session_id: str, batch: SessionBatch) -> Path:
        sess_dir = self.get_session_dir(session_id)
        # Sortable by created_at_ns, then batch_id
        file_name = f"batch_{batch.created_at_ns:020d}_{batch.batch_id}.json"
        return sess_dir / file_name

    def save_batch(self, batch: SessionBatch) -> Path:
        """
        Persist batch atomically with fsync and 0o600 permissions.
        """
        path = self._batch_file_path(batch.session_id, batch)
        data = batch.to_json()
        atomic_write(
            path,
            data,
            dir_mode=self.dir_mode,
            file_mode=self.file_mode,
            sync_parent=self.sync_parent_dir,
        )
        logger.debug("Persisted batch %s for session %s to %s", batch.batch_id, batch.session_id, path)
        return path

    def update_batch(self, batch: SessionBatch) -> None:
        """
        Update batch state (attempts, errors, status) atomically in-place.
        """
        sess_dir = self.get_session_dir(batch.session_id)
        # Search for existing batch file with matching batch_id
        target_file = None
        for p in sess_dir.glob(f"batch_*_{batch.batch_id}.json"):
            target_file = p
            break

        if target_file is None:
            target_file = self._batch_file_path(batch.session_id, batch)

        atomic_write(
            target_file,
            batch.to_json(),
            dir_mode=self.dir_mode,
            file_mode=self.file_mode,
            sync_parent=self.sync_parent_dir,
        )

    def delete_batch(self, session_id: str, batch_id: str) -> bool:
        """
        Delete ONLY this batch file upon successful server acknowledgment.
        Does NOT touch any other batch file.
        """
        safe_name = safe_session_dirname(session_id)
        sess_dir = self.sessions_dir / safe_name
        if not sess_dir.exists():
            return False

        deleted = False
        for p in sess_dir.glob(f"batch_*_{batch_id}.json"):
            try:
                p.unlink(missing_ok=True)
                deleted = True
                logger.debug("Deleted acknowledged batch %s from %s", batch_id, p)
            except OSError as e:
                logger.warning("Failed to unlink batch file %s: %s", p, e)

        # Clean up empty session directory if no other JSON batches remain
        if deleted:
            try:
                remaining = [f for f in sess_dir.glob("batch_*.json")]
                if not remaining:
                    # Session is completely drained
                    lock_file = sess_dir / ".session.lock"
                    # Keep the lock inode and directory: unlinking a held flock
                    # allows another process to create and lock a different inode.
                    pass
            except Exception:
                pass

        return deleted

    def quarantine_batch(self, batch: SessionBatch, reason: str) -> Path:
        """
        Move a permanently rejected batch to dead_letter/{safe_session_id}/.
        Removes the original pending file from sessions/{safe_session_id}/.
        """
        safe_name = safe_session_dirname(batch.session_id)
        dead_dir = self.dead_letter_dir / safe_name
        ensure_directory(dead_dir, mode=self.dir_mode)

        batch.status = "quarantined"
        batch.last_error = reason
        dead_path = dead_dir / f"quarantine_{batch.created_at_ns:020d}_{batch.batch_id}.json"

        atomic_write(
            dead_path,
            batch.to_json(),
            dir_mode=self.dir_mode,
            file_mode=self.file_mode,
            sync_parent=self.sync_parent_dir,
        )

        # Remove from active pending
        self.delete_batch(batch.session_id, batch.batch_id)
        logger.warning(
            "Quarantined batch %s for session %s to %s: %s",
            batch.batch_id,
            batch.session_id,
            dead_path,
            reason,
        )
        return dead_path

    def list_active_session_ids(self) -> List[str]:
        """
        List all distinct session directories that contain pending batches.
        """
        if not self.sessions_dir.exists():
            return []

        session_dirs = []
        for p in self.sessions_dir.iterdir():
            if p.is_dir() and not p.name.startswith("."):
                # Check if it has any batch files
                batches = list(p.glob("batch_*.json"))
                if batches:
                    session_dirs.append(p)

        # Read one batch from each dir to get the exact original session_id
        session_ids: List[str] = []
        for sd in sorted(session_dirs, key=lambda x: x.name):
            batches = sorted(sd.glob("batch_*.json"))
            if batches:
                try:
                    with open(batches[0], "r", encoding="utf-8") as f:
                        data = json.load(f)
                        sid = data.get("session_id")
                        if sid:
                            session_ids.append(sid)
                except Exception:
                    pass
        return session_ids

    def list_batches_for_session(self, session_id: str) -> List[SessionBatch]:
        """
        List all pending batches for a session, strictly sorted in chronological FIFO order.
        """
        safe_name = safe_session_dirname(session_id)
        sess_dir = self.sessions_dir / safe_name
        if not sess_dir.exists():
            return []

        files = sorted(sess_dir.glob("batch_*.json"), key=lambda p: p.name)
        batches: List[SessionBatch] = []

        for p in files:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    content = f.read()
                b = SessionBatch.from_json(content)
                batches.append(b)
            except Exception as e:
                logger.error("Failed to parse batch file %s: %s", p, e)
                raise CorruptedBatchError(f"Corrupted batch file at '{p}': {e}", file_path=str(p)) from e

        return batches

    def list_all_pending_batches(self) -> List[SessionBatch]:
        """
        List all pending batches across all sessions, ordered by session and creation time.
        """
        all_batches: List[SessionBatch] = []
        for sid in self.list_active_session_ids():
            try:
                all_batches.extend(self.list_batches_for_session(sid))
            except Exception as e:
                logger.warning("Error reading batches for session %s: %s", sid, e)
        return all_batches

    def count_pending(self, session_id: Optional[str] = None) -> int:
        """Count pending batch files."""
        if session_id is not None:
            safe_name = safe_session_dirname(session_id)
            sess_dir = self.sessions_dir / safe_name
            if not sess_dir.exists():
                return 0
            return len(list(sess_dir.glob("batch_*.json")))

        total = 0
        if not self.sessions_dir.exists():
            return 0
        for p in self.sessions_dir.glob("*/batch_*.json"):
            if not p.name.startswith(".tmp"):
                total += 1
        return total

    def count_quarantined(self, session_id: Optional[str] = None) -> int:
        """Count quarantined batch files."""
        if session_id is not None:
            safe_name = safe_session_dirname(session_id)
            d = self.dead_letter_dir / safe_name
            if not d.exists():
                return 0
            return len(list(d.glob("quarantine_*.json")))

        if not self.dead_letter_dir.exists():
            return 0
        return len(list(self.dead_letter_dir.glob("*/quarantine_*.json")))

    def clean_orphan_tmp_files(self, max_age_seconds: float = 300.0) -> int:
        """
        Clean up any stale temporary files left behind by interrupted process crashes.
        """
        cleaned = 0
        now = time.time()
        for root, _, files in os.walk(str(self.spool_dir)):
            for f in files:
                if f.startswith(".tmp."):
                    full_p = Path(root) / f
                    try:
                        mtime = full_p.stat().st_mtime
                        if (now - mtime) >= max_age_seconds:
                            full_p.unlink(missing_ok=True)
                            cleaned += 1
                    except OSError:
                        pass
        return cleaned

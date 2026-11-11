"""
AI Memory Runtime (AMR) - Client Spooling Boundaries & Exception Hierarchy.

This module defines the formal guarantees, boundary conditions, and exception
classifications for reliable AMR client session spooling.

Guarantees & Non-Guarantees (明确作用域与不可保障的边界):
======================================================
1. Guarantees (保障能力):
   - Spool-before-send: Every message batch is persisted atomically to local disk
     with fsync prior to attempting network/UDS transmission.
   - At-least-once delivery: Spooled batches survive process restarts, sudden
     termination (post-fsync), and network partitions.
   - Per-session FIFO ordering: Replay strictly preserves chronological sequence
     per session; transient errors suspend that session to avoid out-of-order execution.
   - Stable message IDs: Message IDs are fixed at creation time and persisted,
     enabling AMR server-side idempotency and deduplication across retries.
   - Batch-level isolation: On successful server acknowledgment, only the confirmed
     batch file is deleted. Concurrent or subsequent batches remain intact.
   - Atomic disk I/O & strict permissions: Files written via temp-file + fsync + replace.
     Files enforce 0o600 (POSIX rw-------) and directories 0o700 (POSIX rwx------).
   - Concurrency safety: Session-level and directory-level file locking (fcntl.flock)
     combined with thread locks guard against race conditions in multi-threaded
     or multi-process client deployments.
   - Poison-pill quarantine: Batches rejected permanently by AMR (e.g., session
     identity collisions across agents) are isolated to a dead_letter folder
     so they do not endlessly block the transmission queue.

2. Non-Guarantees / Boundaries (不可保障的边界):
   - Hard power loss / disk controller write-cache drop: Without battery-backed
     write-cache on hardware, catastrophic system power failure before hardware
     platter/flash commit cannot be prevented by application-level software.
   - Disk exhaustion (ENOSPC): When local storage is completely full, batch spooling
     will raise SpoolStorageError; the client cannot store unbounded messages without disk space.
   - Fatal server-side rejections: If a session ID is already owned by a different
     agent or project on the AMR server, retries will never succeed. Such batches
     are quarantined rather than infinitely retried.
   - Downstream cognitive distillation: The client guarantees ingestion into AMR's
     raw WAL storage (SessionStore). It cannot guarantee whether or when background
     cognitive distillation, embedding generation, or Qdrant vector indexing completes.
   - Host SIGKILL during write before fsync: If the host OS kills the client process
     before fsync returns, that specific uncommitted batch is lost.
   - Cross-host clock skew: If multiple distributed machines spool to a shared path
     with desynchronized system clocks, timestamps may not be strictly monotonic.
"""

from __future__ import annotations

import socket
from typing import Any, Dict, Optional, Set, Union


class SpoolError(Exception):
    """Base exception for all AMR client spooling errors."""
    pass


class SpoolStorageError(SpoolError):
    """Raised when local filesystem operations (create, write, fsync, delete) fail."""
    pass


class SpoolLockError(SpoolStorageError):
    """Raised when acquiring a file or session lock fails or times out."""
    pass


class CorruptedBatchError(SpoolStorageError):
    """Raised when a spooled batch file cannot be parsed or contains invalid data."""
    def __init__(self, message: str, file_path: Optional[str] = None):
        super().__init__(message)
        self.file_path = file_path


class DeliveryError(SpoolError):
    """Base exception for network/UDS transmission errors to AMR."""
    def __init__(
        self,
        message: str,
        code: Optional[int] = None,
        data: Optional[Any] = None,
        is_transient: bool = True,
    ):
        super().__init__(message)
        self.code = code
        self.data = data
        self.is_transient = is_transient


class TransientDeliveryError(DeliveryError):
    """
    Transient network or server failure where retrying the batch is safe and expected.
    Examples: Socket timeout, connection refused, connection reset, server model loading, queue full.
    """
    def __init__(self, message: str, code: Optional[int] = None, data: Optional[Any] = None):
        super().__init__(message, code=code, data=data, is_transient=True)


class PermanentRejectionError(DeliveryError):
    """
    Permanent semantic rejection by AMR server where retrying identical payload will fail.
    Examples: Session collision across agents/projects, unauthorized agent, invalid arguments.
    Such batches are quarantined into the dead_letter directory.
    """
    def __init__(self, message: str, code: Optional[int] = None, data: Optional[Any] = None):
        super().__init__(message, code=code, data=data, is_transient=False)


# JSON-RPC error codes recognized as permanent rejections
PERMANENT_ERROR_CODES: Set[int] = {
    -32001,  # AGENT_UNAUTHORIZED
    -32600,  # INVALID_REQUEST
    -32601,  # METHOD_NOT_FOUND
    -32602,  # INVALID_ARGUMENT (e.g. session collision across agents, invalid role)
}

# JSON-RPC error codes recognized as transient / retryable
TRANSIENT_ERROR_CODES: Set[int] = {
    -32003,  # MODEL_LOADING
    -32004,  # QUEUE_FULL
    -32000,  # SERVER_BUSY
}

# Substrings in error message that indicate permanent rejection
PERMANENT_ERROR_PATTERNS = (
    "Session ID already belongs to a different agent",
    "Session ID already belongs to a different project",
    "AGENT_UNAUTHORIZED",
    "INVALID_ARGUMENT",
    "Unknown or untrusted agent_id",
    "Each message requires",
)


def is_transient_error(
    exc: Optional[Exception] = None,
    code: Optional[int] = None,
    message: Optional[str] = None,
) -> bool:
    """
    Determine whether an error is transient (retryable) or permanent (quarantine).
    """
    if isinstance(exc, PermanentRejectionError):
        return False
    if isinstance(exc, TransientDeliveryError):
        return True

    # Check error code
    if code is not None:
        if code in PERMANENT_ERROR_CODES:
            return False
        if code in TRANSIENT_ERROR_CODES:
            return True

    # Check error message patterns
    err_str = (message or (str(exc) if exc else "")).strip()
    for pat in PERMANENT_ERROR_PATTERNS:
        if pat in err_str:
            return False

    # Check exception types
    if exc is not None:
        if isinstance(exc, (TimeoutError, socket.timeout, ConnectionRefusedError,
                            ConnectionResetError, BrokenPipeError, ConnectionError,
                            BlockingIOError, InterruptedError)):
            return True
        if isinstance(exc, ValueError):
            return False

    # Default to transient for unexpected network/IO issues
    return True


CLIENT_GUARANTEES: Dict[str, str] = {
    "spool_before_send": "Messages are serialized and fsynced to disk prior to any network attempt.",
    "at_least_once_delivery": "Spooled batches survive process restarts and network drops until acknowledged.",
    "stable_message_ids": "Every message receives a deterministic or persisted ID, enabling server deduplication.",
    "batch_isolation": "Acknowledgment deletes ONLY the confirmed batch file; other batches remain untouched.",
    "fifo_ordering": "Pending batches are replayed strictly in chronological sequence per session.",
    "atomic_file_ops": "Writes use temp-files + fsync + atomic replace; files use 0o600, dirs 0o700.",
    "concurrency_safety": "File locks (fcntl.flock) and in-memory locks guard concurrent multi-process access.",
    "poison_pill_quarantine": "Permanent server rejections are isolated to dead_letter/ without stalling the queue.",
}

CLIENT_NON_GUARANTEES: Dict[str, str] = {
    "hardware_power_cut": "Data in battery-less hardware drive write cache before media flush cannot be guaranteed.",
    "disk_full_enospc": "Client cannot store new batches if the host filesystem has zero free disk space.",
    "server_identity_conflicts": "If a session ID is registered to another agent, client retries cannot force ownership.",
    "downstream_distillation": "Ingestion into SQLite WAL is guaranteed; background cognitive distillation is asynchronous.",
    "pre_fsync_sigkill": "Process terminated by SIGKILL before write+fsync finishes will lose the in-memory batch.",
    "distributed_clock_skew": "Timestamps reflect local system clock; cross-host skew is outside client control.",
}

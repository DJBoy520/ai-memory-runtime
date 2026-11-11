"""
Universal AMR Client Reliable Session Spooling Library.

A general-purpose, agent-independent library for reliable conversation session
spooling and retransmission to AI Memory Runtime (AMR).
"""

from src.client.boundaries import (
    CLIENT_GUARANTEES,
    CLIENT_NON_GUARANTEES,
    CorruptedBatchError,
    DeliveryError,
    PermanentRejectionError,
    SpoolError,
    SpoolLockError,
    SpoolStorageError,
    TransientDeliveryError,
    is_transient_error,
)
from src.client.client import (
    AmrSessionClient,
    AsyncAmrSessionClient,
)
from src.client.models import (
    IngestResult,
    SessionBatch,
    SessionMessage,
    SpoolConfig,
    compute_stable_message_id,
    generate_batch_id,
)
from src.client.storage import (
    FileLock,
    SpoolStorage,
    atomic_write,
    safe_session_dirname,
)
from src.client.transport import (
    BaseTransport,
    MockTransport,
    UdsTransport,
)

__version__ = "1.0.0"

__all__ = [
    "AmrSessionClient",
    "AsyncAmrSessionClient",
    "SessionMessage",
    "SessionBatch",
    "SpoolConfig",
    "IngestResult",
    "compute_stable_message_id",
    "generate_batch_id",
    "SpoolStorage",
    "FileLock",
    "atomic_write",
    "safe_session_dirname",
    "BaseTransport",
    "UdsTransport",
    "MockTransport",
    "SpoolError",
    "SpoolStorageError",
    "SpoolLockError",
    "CorruptedBatchError",
    "DeliveryError",
    "TransientDeliveryError",
    "PermanentRejectionError",
    "is_transient_error",
    "CLIENT_GUARANTEES",
    "CLIENT_NON_GUARANTEES",
]

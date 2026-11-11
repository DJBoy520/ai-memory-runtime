"""
AI Memory Runtime (AMR) - Client Session Spooling Library.

Provides universal, reliable session ingestion for any Agent:
- AmrSessionClient: Synchronous client (suitable for sync agent frameworks, scripts, threads).
- AsyncAmrSessionClient: Asynchronous client (suitable for asyncio loops, FastAPI, MCP servers).

Guarantees:
- Spool-before-send with fsync and 0o600 permissions.
- Stable message IDs across all retries.
- Ack deletes only the confirmed batch.
- Retains unconfirmed batches on failure, replays on restart.
- Thread and process concurrency safe via file locks.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from src.client.boundaries import (
    PermanentRejectionError,
    SpoolError,
    TransientDeliveryError,
    is_transient_error,
)
from src.client.models import (
    IngestResult,
    SessionBatch,
    SessionMessage,
    SpoolConfig,
    generate_batch_id,
)
from src.client.storage import SpoolStorage
from src.client.transport import BaseTransport, UdsTransport

logger = logging.getLogger("amr.client")


class AmrSessionClient:
    """
    Synchronous Universal AMR Session Spooling Client.
    Decoupled from any specific Agent framework.
    """

    def __init__(
        self,
        spool_dir: Union[str, Path],
        agent_id: str = "default_agent",
        project_id: Optional[str] = None,
        socket_path: Optional[str] = None,
        transport: Optional[BaseTransport] = None,
        config: Optional[SpoolConfig] = None,
        auto_replay: bool = True,
        raise_on_failure: bool = False,
    ):
        if config is not None:
            self.config = config
        else:
            self.config = SpoolConfig(
                spool_dir=spool_dir,
                agent_id=agent_id,
                project_id=project_id,
                socket_path=socket_path,
                auto_replay=auto_replay,
            )

        self.storage = SpoolStorage(
            spool_dir=self.config.spool_dir,
            dir_mode=self.config.dir_mode,
            file_mode=self.config.file_mode,
            sync_parent_dir=self.config.sync_parent_dir,
        )

        if transport is not None:
            self.transport = transport
        else:
            sock = self.config.socket_path or "/run/user/1000/qdrant-bge.sock"
            self.transport = UdsTransport(
                socket_path=sock,
                connect_timeout=self.config.connect_timeout,
                request_timeout=self.config.request_timeout,
            )

        self.raise_on_failure = raise_on_failure

        # Clean stale tmp files
        try:
            self.storage.clean_orphan_tmp_files()
        except Exception:
            pass

        # Process restart auto-replay: drain pending batches from prior runs
        if self.config.auto_replay:
            try:
                self.replay_pending()
            except Exception as e:
                logger.warning("Auto-replay on client startup encountered error: %s", e)

    def _prepare_messages(
        self,
        session_id: str,
        messages: Sequence[Union[Dict[str, Any], SessionMessage]],
    ) -> List[SessionMessage]:
        if not messages:
            raise ValueError("Parameter 'messages' must be a non-empty sequence")

        prepared: List[SessionMessage] = []
        for idx, m in enumerate(messages):
            if isinstance(m, SessionMessage):
                prepared.append(m)
            elif isinstance(m, dict):
                prepared.append(SessionMessage.from_dict(m, session_id=session_id, index=idx))
            else:
                raise ValueError(f"Each message must be a dict or SessionMessage, got {type(m).__name__}")
        return prepared

    def record_messages(
        self,
        session_id: str,
        messages: Sequence[Union[Dict[str, Any], SessionMessage]],
        project_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> IngestResult:
        """
        Record and ingest conversation messages for a session.

        Guarantees:
        1. Messages are normalized and assigned stable message_ids.
        2. Spool-before-send: batch is persisted and fsynced to disk before network call.
        3. If send succeeds: deletes ONLY this batch file.
        4. If send fails (transient): keeps batch on disk for future replay.
        5. If send fails (permanent): quarantines batch to dead_letter/.
        """
        if not session_id or not str(session_id).strip():
            raise ValueError("Parameter 'session_id' is required and cannot be empty")

        norm_messages = self._prepare_messages(session_id, messages)
        effective_agent = (agent_id or self.config.agent_id or "default_agent").strip()
        effective_project = project_id if project_id is not None else self.config.project_id

        # Generate unique, sortable batch ID
        batch_id = generate_batch_id()
        batch = SessionBatch(
            batch_id=batch_id,
            session_id=session_id,
            agent_id=effective_agent,
            project_id=effective_project,
            messages=norm_messages,
            created_at=time.time(),
            created_at_ns=time.time_ns(),
        )

        # Acquire session lock to preserve per-session sequential consistency
        with self.storage.get_session_lock(session_id):
            # Step 1: PERSISTENCE FIRST (Spool-before-send)
            self.storage.save_batch(batch)

            # Step 2: Attempt network delivery
            return self._send_and_commit_batch(batch, timeout=timeout, is_retry=False)

    def _send_and_commit_batch(
        self,
        batch: SessionBatch,
        timeout: Optional[float] = None,
        is_retry: bool = False,
    ) -> IngestResult:
        payload_messages = [m.to_ingest_dict() for m in batch.messages]
        req_timeout = timeout if timeout is not None else self.config.request_timeout

        try:
            resp = self.transport.send_ingest(
                session_id=batch.session_id,
                agent_id=batch.agent_id,
                project_id=batch.project_id,
                messages=payload_messages,
                timeout=req_timeout,
            )

            # Step 3: On verified success ack -> Delete ONLY this batch file!
            self.storage.delete_batch(batch.session_id, batch.batch_id)

            return IngestResult(
                ok=True,
                session_id=batch.session_id,
                batch_id=batch.batch_id,
                message_count=len(batch.messages),
                server_response=resp,
                is_retry=is_retry,
            )

        except PermanentRejectionError as ex:
            err_msg = str(ex)
            batch.attempts += 1
            batch.last_attempt_at = time.time()
            batch.last_error = err_msg

            quarantined = False
            if self.config.quarantine_permanent_errors:
                self.storage.quarantine_batch(batch, err_msg)
                quarantined = True
            else:
                self.storage.update_batch(batch)

            if self.raise_on_failure:
                raise

            return IngestResult(
                ok=False,
                session_id=batch.session_id,
                batch_id=batch.batch_id,
                message_count=len(batch.messages),
                error=err_msg,
                is_retry=is_retry,
                quarantined=quarantined,
            )

        except Exception as ex:
            # Transient delivery failure: retain batch in spool
            err_msg = str(ex)
            batch.attempts += 1
            batch.last_attempt_at = time.time()
            batch.last_error = err_msg
            self.storage.update_batch(batch)

            logger.debug(
                "Batch %s for session %s delivery failed (retained in spool): %s",
                batch.batch_id,
                batch.session_id,
                err_msg,
            )

            if self.raise_on_failure:
                raise TransientDeliveryError(
                    f"Transient delivery error for batch {batch.batch_id}: {err_msg}"
                ) from ex

            return IngestResult(
                ok=False,
                session_id=batch.session_id,
                batch_id=batch.batch_id,
                message_count=len(batch.messages),
                error=err_msg,
                is_retry=is_retry,
                quarantined=False,
            )

    def replay_pending(
        self,
        session_id: Optional[str] = None,
        max_batches: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> List[IngestResult]:
        """
        Replay unacknowledged pending batches from local spool storage.
        Strictly preserves FIFO order per session.
        If a transient error occurs on batch N, replay pauses for that session
        so subsequent batches are not sent out of order.
        """
        target_sessions = [session_id] if session_id is not None else self.storage.list_active_session_ids()
        results: List[IngestResult] = []
        batches_processed = 0

        for sid in target_sessions:
            if max_batches is not None and batches_processed >= max_batches:
                break

            with self.storage.get_session_lock(sid):
                pending_batches = self.storage.list_batches_for_session(sid)
                for batch in pending_batches:
                    if max_batches is not None and batches_processed >= max_batches:
                        break

                    res = self._send_and_commit_batch(batch, timeout=timeout, is_retry=True)
                    results.append(res)
                    batches_processed += 1

                    if not res.ok and not res.quarantined:
                        # Transient error encountered: STOP replaying this session
                        # to preserve in-order message delivery
                        logger.info(
                            "Replay paused for session %s at batch %s due to transient error: %s",
                            sid,
                            batch.batch_id,
                            res.error,
                        )
                        break

        return results

    def pending_count(self, session_id: Optional[str] = None) -> int:
        """Return the number of unacknowledged spooled batches."""
        return self.storage.count_pending(session_id)

    def quarantined_count(self, session_id: Optional[str] = None) -> int:
        """Return the number of quarantined batches."""
        return self.storage.count_quarantined(session_id)

    def close(self) -> None:
        """Clean up resources."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AsyncAmrSessionClient:
    """
    Asynchronous Universal AMR Session Spooling Client for asyncio applications.
    Offloads local disk I/O to thread pool (zero event loop stalling)
    and utilizes async network transport.
    """

    def __init__(
        self,
        spool_dir: Union[str, Path],
        agent_id: str = "default_agent",
        project_id: Optional[str] = None,
        socket_path: Optional[str] = None,
        transport: Optional[BaseTransport] = None,
        config: Optional[SpoolConfig] = None,
        auto_replay: bool = False,
        raise_on_failure: bool = False,
    ):
        if config is not None:
            self.config = config
        else:
            self.config = SpoolConfig(
                spool_dir=spool_dir,
                agent_id=agent_id,
                project_id=project_id,
                socket_path=socket_path,
                auto_replay=auto_replay,
            )

        self.storage = SpoolStorage(
            spool_dir=self.config.spool_dir,
            dir_mode=self.config.dir_mode,
            file_mode=self.config.file_mode,
            sync_parent_dir=self.config.sync_parent_dir,
        )

        if transport is not None:
            self.transport = transport
        else:
            sock = self.config.socket_path or "/run/user/1000/qdrant-bge.sock"
            self.transport = UdsTransport(
                socket_path=sock,
                connect_timeout=self.config.connect_timeout,
                request_timeout=self.config.request_timeout,
            )

        self.raise_on_failure = raise_on_failure

    def _prepare_messages(
        self,
        session_id: str,
        messages: Sequence[Union[Dict[str, Any], SessionMessage]],
    ) -> List[SessionMessage]:
        if not messages:
            raise ValueError("Parameter 'messages' must be a non-empty sequence")

        prepared: List[SessionMessage] = []
        for idx, m in enumerate(messages):
            if isinstance(m, SessionMessage):
                prepared.append(m)
            elif isinstance(m, dict):
                prepared.append(SessionMessage.from_dict(m, session_id=session_id, index=idx))
            else:
                raise ValueError(f"Each message must be a dict or SessionMessage, got {type(m).__name__}")
        return prepared

    async def record_messages(
        self,
        session_id: str,
        messages: Sequence[Union[Dict[str, Any], SessionMessage]],
        project_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> IngestResult:
        """
        Record messages asynchronously with spool-before-send and stable IDs.
        """
        if not session_id or not str(session_id).strip():
            raise ValueError("Parameter 'session_id' is required and cannot be empty")

        norm_messages = self._prepare_messages(session_id, messages)
        effective_agent = (agent_id or self.config.agent_id or "default_agent").strip()
        effective_project = project_id if project_id is not None else self.config.project_id

        batch_id = generate_batch_id()
        batch = SessionBatch(
            batch_id=batch_id,
            session_id=session_id,
            agent_id=effective_agent,
            project_id=effective_project,
            messages=norm_messages,
            created_at=time.time(),
            created_at_ns=time.time_ns(),
        )

        # Spool to disk inside thread pool
        def _persist():
            with self.storage.get_session_lock(session_id):
                self.storage.save_batch(batch)

        await asyncio.to_thread(_persist)

        # Send asynchronously
        return await self._async_send_and_commit(batch, timeout=timeout, is_retry=False)

    async def _async_send_and_commit(
        self,
        batch: SessionBatch,
        timeout: Optional[float] = None,
        is_retry: bool = False,
    ) -> IngestResult:
        payload_messages = [m.to_ingest_dict() for m in batch.messages]
        req_timeout = timeout if timeout is not None else self.config.request_timeout

        try:
            resp = await self.transport.async_send_ingest(
                session_id=batch.session_id,
                agent_id=batch.agent_id,
                project_id=batch.project_id,
                messages=payload_messages,
                timeout=req_timeout,
            )

            # On verified ack -> delete ONLY this batch
            await asyncio.to_thread(self.storage.delete_batch, batch.session_id, batch.batch_id)

            return IngestResult(
                ok=True,
                session_id=batch.session_id,
                batch_id=batch.batch_id,
                message_count=len(batch.messages),
                server_response=resp,
                is_retry=is_retry,
            )

        except PermanentRejectionError as ex:
            err_msg = str(ex)
            batch.attempts += 1
            batch.last_attempt_at = time.time()
            batch.last_error = err_msg

            quarantined = False
            if self.config.quarantine_permanent_errors:
                await asyncio.to_thread(self.storage.quarantine_batch, batch, err_msg)
                quarantined = True
            else:
                await asyncio.to_thread(self.storage.update_batch, batch)

            if self.raise_on_failure:
                raise

            return IngestResult(
                ok=False,
                session_id=batch.session_id,
                batch_id=batch.batch_id,
                message_count=len(batch.messages),
                error=err_msg,
                is_retry=is_retry,
                quarantined=quarantined,
            )

        except Exception as ex:
            err_msg = str(ex)
            batch.attempts += 1
            batch.last_attempt_at = time.time()
            batch.last_error = err_msg
            await asyncio.to_thread(self.storage.update_batch, batch)

            if self.raise_on_failure:
                raise TransientDeliveryError(
                    f"Transient delivery error for batch {batch.batch_id}: {err_msg}"
                ) from ex

            return IngestResult(
                ok=False,
                session_id=batch.session_id,
                batch_id=batch.batch_id,
                message_count=len(batch.messages),
                error=err_msg,
                is_retry=is_retry,
                quarantined=False,
            )

    async def replay_pending(
        self,
        session_id: Optional[str] = None,
        max_batches: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> List[IngestResult]:
        """Asynchronously replay pending batches in FIFO order."""
        if session_id is not None:
            target_sessions = [session_id]
        else:
            target_sessions = await asyncio.to_thread(self.storage.list_active_session_ids)

        results: List[IngestResult] = []
        batches_processed = 0

        for sid in target_sessions:
            if max_batches is not None and batches_processed >= max_batches:
                break

            pending_batches = await asyncio.to_thread(self.storage.list_batches_for_session, sid)
            for batch in pending_batches:
                if max_batches is not None and batches_processed >= max_batches:
                    break

                res = await self._async_send_and_commit(batch, timeout=timeout, is_retry=True)
                results.append(res)
                batches_processed += 1

                if not res.ok and not res.quarantined:
                    break

        return results

    async def pending_count(self, session_id: Optional[str] = None) -> int:
        return await asyncio.to_thread(self.storage.count_pending, session_id)

    async def quarantined_count(self, session_id: Optional[str] = None) -> int:
        return await asyncio.to_thread(self.storage.count_quarantined, session_id)

    async def close(self) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

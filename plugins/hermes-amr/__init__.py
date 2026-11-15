"""AMR (AI Memory Runtime) memory provider for Hermes (2.x).

Connects via Unix Domain Socket (UDS) to the local AMR 2.x adapter, supporting
zero-overhead prefetch, persist-first background turn ingestion
(``session.ingest`` with an id + local spool), and memory fact persistence
(``memory.create``).

DOC-AMR-09 compliance:
- The adapter binds the agent identity to the socket; no ``agent_id`` or
  ``source_agent`` is ever sent.
- ``session.ingest`` is persist-first: spool -> request -> remove on ack.
  Transient failures stay pending; permanent failures are marked once and
  retained. Replay is non-blocking and skips in-flight keys.
- ``amr.ping`` runs once asynchronously at initialization and fails open.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

try:
    from agent.memory_provider import (
        INDICATOR_GLYPH,
        MemoryProvider,
        RecallStatus,
        is_trivial_prompt,
        spawn_context_thread,
    )
except ImportError:
    # Graceful fallback mock for test isolation in non-hermes hosts
    INDICATOR_GLYPH = "🧠"

    class RecallStatus:
        def __init__(self, provider_label: str = "amr", count: int = 0, glyph: str = "🧠"):
            self.provider_label = provider_label
            self.count = count
            self.glyph = glyph

    class MemoryProvider:
        def __init__(self, *args, **kwargs):
            pass

    def is_trivial_prompt(prompt: str) -> bool:
        if not prompt or len(prompt.strip()) < 3:
            return True
        return prompt.strip().lower() in {"hi", "hello", "ping", "pong", "test"}

    def spawn_context_thread(target, args=(), **kwargs):
        daemon = kwargs.get("daemon", True)
        name = kwargs.get("name")
        t = threading.Thread(target=target, args=args, name=name, daemon=daemon)
        return t

try:
    from ._client import (
        AmrUdsClient,
        DEFAULT_SOCKET_PATH,
        is_transient,
        next_request_id,
        resolve_socket_path,
    )
    from ._spool import IngestSpool, compute_record_key, resolve_spool_dir
except ImportError:
    from _client import (  # type: ignore
        AmrUdsClient,
        DEFAULT_SOCKET_PATH,
        is_transient,
        next_request_id,
        resolve_socket_path,
    )
    from _spool import (  # type: ignore
        IngestSpool,
        compute_record_key,
        resolve_spool_dir,
    )

logger = logging.getLogger(__name__)

_DEFAULT_PROJECT_ID = "crypto-infrastructure"
_PREFETCH_TIMEOUT = 0.3  # Strict 300ms requirement per prompt/owner instructions
_INGEST_TIMEOUT = 2.0
_REPLAY_DEADLINE = 2.0
_PROBE_TIMEOUT = 0.3


def _load_amr_config(hermes_home: Optional[Path] = None) -> Dict[str, Any]:
    """Load config from ~/.hermes/amr.json or config.yaml memory.amr."""
    if hermes_home is None:
        try:
            from hermes_constants import get_hermes_home
            hermes_home = get_hermes_home()
        except Exception:
            hermes_home = Path.home() / ".hermes"

    config_file = hermes_home / "amr.json"
    if config_file.exists():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", config_file, exc)

    try:
        from hermes_cli.config import load_config
        cfg = load_config().get("memory", {}).get("amr", {})
        if isinstance(cfg, dict):
            return dict(cfg)
    except Exception:
        pass

    return {}


class AmrMemoryProvider(MemoryProvider):
    """Hermes MemoryProvider implementation backed by AMR 2.x."""

    def __init__(self) -> None:
        self._config: Dict[str, Any] = {}
        self._client: Optional[AmrUdsClient] = None
        self._socket_path = resolve_socket_path()
        self._project_id = _DEFAULT_PROJECT_ID
        self._top_k = 3
        self._auto_extract = True
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="amr_prefetch"
        )
        self._pending_future: Optional[concurrent.futures.Future] = None
        self._prefetch_lock = threading.Lock()
        self._last_recall_status: Optional[RecallStatus] = None
        self._session_id: str = ""
        self._hermes_home: Optional[Path] = None

        # Persist-first spool + in-flight/replay coordination.
        self._spool: Optional[IngestSpool] = None
        self._spool_dir: Optional[str] = None
        self._spool_lock = threading.Lock()
        self._in_flight: Set[str] = set()
        self._in_flight_lock = threading.Lock()
        self._active_archives = 0
        self._active_lock = threading.Lock()
        self._replay_lock = threading.Lock()
        self._replay_running = False
        self._closed = False
        self._message_seq = 0
        self._message_seq_lock = threading.Lock()

    @property
    def name(self) -> str:
        return "amr"

    def is_available(self) -> bool:
        """Fast check if the UDS socket exists and is connectable."""
        cfg = _load_amr_config(self._hermes_home)
        configured = cfg.get("socket_path") or cfg.get("socketPath")
        if not configured:
            configured = self._socket_path
        client = AmrUdsClient(socket_path=configured)
        return client.is_available()

    def unavailable_reason(self) -> str:
        return f"AMR socket at {self._socket_path} is unavailable or not running (amr2 adapter)."

    # ------------------------------------------------------------- lifecycle
    def initialize(self, session_id: str, **kwargs) -> None:
        """Initialize AMR provider with profile-scoped config."""
        self._session_id = session_id
        self._closed = False
        home_arg = kwargs.get("hermes_home")
        if home_arg:
            self._hermes_home = Path(home_arg)

        self._config = _load_amr_config(self._hermes_home)
        configured_socket = (
            kwargs.get("socket_path")
            or kwargs.get("socketPath")
            or self._config.get("socket_path")
            or self._config.get("socketPath")
        )
        self._socket_path = resolve_socket_path(configured_socket)
        self._project_id = (
            kwargs.get("project_id")
            or self._config.get("project_id")
            or _DEFAULT_PROJECT_ID
        )
        self._top_k = int(kwargs.get("top_k") or self._config.get("top_k", 3))
        self._auto_extract = bool(
            kwargs.get("auto_extract", self._config.get("auto_extract", True))
        )

        # Allow tests/callers to inject a spool or an explicit spool dir.
        spool_arg = kwargs.get("spool")
        spool_dir = (
            kwargs.get("spool_dir")
            or kwargs.get("persistence_dir")
            or kwargs.get("persistenceDir")
            or self._config.get("spool_dir")
            or self._config.get("persistence_dir")
            or self._config.get("persistenceDir")
        )
        if spool_arg is not None:
            self._spool = spool_arg
        elif spool_dir:
            target_dir = resolve_spool_dir(spool_dir)
            current_dir = getattr(self._spool, "dir", None) if self._spool else None
            if current_dir is None or os.path.abspath(current_dir) != target_dir:
                self._spool = IngestSpool(persistence_dir=target_dir)
        if spool_dir:
            self._spool_dir = spool_dir

        client_arg = kwargs.get("client")
        if client_arg:
            self._client = client_arg
        else:
            self._client = AmrUdsClient(
                socket_path=self._socket_path, request_timeout=1.0
            )

        logger.info(
            "AMR MemoryProvider initialized (socket=%s, project=%s, spool=%s)",
            self._socket_path,
            self._project_id,
            self._spool.dir if self._spool is not None else resolve_spool_dir(
                self._spool_dir
            ),
        )

        # Non-blocking 2.x health check + spool replay. Both fail open.
        self._start_health_check()

    def _ensure_spool(self) -> IngestSpool:
        with self._spool_lock:
            if self._spool is None:
                self._spool = IngestSpool(persistence_dir=self._spool_dir)
            return self._spool

    def _fail_open_call(
        self, client: Any, method: str, params: Dict[str, Any], timeout: float
    ) -> Optional[Any]:
        """Call a non-critical method without ever raising into the host."""
        try:
            call = getattr(client, "call", None)
            if call is not None:
                return call(method, params, timeout=timeout)
            return client.request(method, params, timeout=timeout)
        except Exception as exc:
            logger.debug("AMR %s failed open: %s", method, exc)
            return None

    def _start_health_check(self) -> None:
        def _probe() -> None:
            client = self._client
            try:
                if client is not None:
                    try:
                        res = client.ping(_PROBE_TIMEOUT)
                    except TypeError:
                        # Tolerate minimal test doubles with a zero-arg ping.
                        res = client.ping()
                else:
                    res = None
                logger.debug("AMR amr.ping ok: %s", res)
            except Exception as exc:
                logger.debug("AMR amr.ping failed (fail-open): %s", exc)
            self._schedule_replay()

        try:
            thread = spawn_context_thread(_probe, name="amr_health", daemon=True)
            thread.start()
        except Exception as exc:
            logger.debug("AMR health-check thread failed (fail-open): %s", exc)

    # ------------------------------------------------------------- spool I/O
    def _next_message_seq(self) -> int:
        with self._message_seq_lock:
            self._message_seq += 1
            return self._message_seq

    def _in_flight_add(self, key: str) -> None:
        with self._in_flight_lock:
            self._in_flight.add(key)

    def _in_flight_discard(self, key: str) -> None:
        with self._in_flight_lock:
            self._in_flight.discard(key)

    def _in_flight_snapshot(self) -> Set[str]:
        with self._in_flight_lock:
            return set(self._in_flight)

    def _report_permanent(self, record: Dict[str, Any], err: BaseException) -> None:
        code = getattr(err, "code", "unknown")
        key = (record or {}).get("key", "(unspooled)")
        logger.warning(
            "AMR session.ingest permanently failed (code=%s); "
            "record retained for manual handling: %s",
            code,
            key,
        )

    def _schedule_replay(self) -> None:
        """Start a non-blocking spool replay, guarded against concurrent sweeps."""
        if self._closed:
            return
        with self._active_lock:
            if self._active_archives > 0:
                # A direct archive is in flight; a replay now could double-send
                # the same key. It will be scheduled after the ack instead.
                return
        with self._replay_lock:
            if self._replay_running:
                return
            self._replay_running = True

        def _worker() -> None:
            try:
                client = self._client
                if client is None:
                    return
                with self._active_lock:
                    if self._active_archives > 0:
                        return
                summary = self._ensure_spool().replay(
                    client,
                    deadline_s=_REPLAY_DEADLINE,
                    on_permanent=self._report_permanent,
                    skip_keys=self._in_flight_snapshot(),
                )
                logger.debug("AMR spool replay: %s", summary)
            except Exception as exc:
                logger.debug("AMR spool replay failed open: %s", exc)
            finally:
                with self._replay_lock:
                    self._replay_running = False

        try:
            thread = spawn_context_thread(_worker, name="amr_spool_replay", daemon=True)
            thread.start()
        except Exception as exc:
            with self._replay_lock:
                self._replay_running = False
            logger.debug("AMR spool replay thread failed open: %s", exc)

    # --------------------------------------------------------------- recall
    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        """Queue an asynchronous prefetch over UDS."""
        if is_trivial_prompt(query):
            with self._prefetch_lock:
                self._pending_future = None
            return

        if not self._client:
            self._client = AmrUdsClient(socket_path=self._socket_path)

        def _do_search() -> Any:
            params = {
                "query": query,
                "project_id": self._project_id,
                "limit": self._top_k,
            }
            return self._fail_open_call(self._client, "memory.search", params, 0.3)

        future = self._executor.submit(_do_search)
        with self._prefetch_lock:
            self._pending_future = future

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Retrieve prefetch results within 0.3s (300ms) and format as Markdown context."""
        self._last_recall_status = None
        with self._prefetch_lock:
            fut = self._pending_future
            self._pending_future = None

        if fut is None:
            if is_trivial_prompt(query):
                return ""
            if not self._client:
                self._client = AmrUdsClient(socket_path=self._socket_path)
            try:
                params = {
                    "query": query,
                    "project_id": self._project_id,
                    "limit": self._top_k,
                }
                res = self._fail_open_call(
                    self._client, "memory.search", params, _PREFETCH_TIMEOUT
                )
            except Exception as exc:
                logger.debug("AMR sync prefetch fallback error: %s", exc)
                return ""
        else:
            try:
                res = fut.result(timeout=_PREFETCH_TIMEOUT)
            except concurrent.futures.TimeoutError:
                logger.warning("AMR prefetch timed out after %ss", _PREFETCH_TIMEOUT)
                return ""
            except Exception as exc:
                logger.debug("AMR prefetch error: %s", exc)
                return ""

        if isinstance(res, dict):
            res = res.get("results") or res.get("matches") or []

        if not res or not isinstance(res, list):
            return ""

        # Extract items
        items = []
        for item in res:
            if isinstance(item, dict):
                content = (
                    item.get("content") or item.get("text") or item.get("fact")
                )
                if content and str(content).strip():
                    items.append(str(content).strip())
            elif isinstance(item, str) and item.strip():
                items.append(item.strip())

        if not items:
            return ""

        self._last_recall_status = RecallStatus(
            provider_label="amr",
            count=len(items),
            glyph=INDICATOR_GLYPH,
        )

        lines = ["### [AI Memory Runtime: Recalled Facts]"]
        for fact in items:
            lines.append(f"- {fact}")
        return "\n".join(lines)

    def recall_status(self) -> Optional[RecallStatus]:
        return self._last_recall_status

    # --------------------------------------------------------------- ingest
    def _ingest_turn(
        self, client: Any, sid: str, ingest_params: Dict[str, Any]
    ) -> None:
        """Persist-first ingest: spool -> request(id) -> remove on ack."""
        spool = self._ensure_spool()
        record_key: Optional[str] = None
        desired_key: Optional[str] = None
        acknowledged = False
        with self._active_lock:
            self._active_archives += 1
        try:
            # Reserve the key before enqueue so an already-running replay sweep
            # cannot send the same payload concurrently.
            try:
                desired_key = compute_record_key(ingest_params)
                self._in_flight_add(desired_key)
            except Exception:
                desired_key = None

            record_key = spool.enqueue(ingest_params)
            if record_key:
                existing = spool.get(record_key)
                if existing and existing.get("permanent"):
                    logger.debug(
                        "AMR skipping permanently failed spool record %s", record_key
                    )
                    return
                self._in_flight_add(record_key)
            elif desired_key:
                self._in_flight_discard(desired_key)

            # request() (not notify()) gives us a server acknowledgement that
            # lets us distinguish "committed" from "lost in transit". An
            # explicit id makes that acknowledgement unambiguous.
            client.request(
                "session.ingest",
                ingest_params,
                timeout=_INGEST_TIMEOUT,
                req_id=next_request_id(),
            )
            acknowledged = True
            if record_key:
                spool.remove(record_key)
            logger.debug("AMR session.ingest acknowledged for session %s", sid)
        except Exception as exc:
            if is_transient(exc):
                logger.debug(
                    "AMR session.ingest deferred (transient %s); retained in spool: %s",
                    getattr(exc, "code", "error"),
                    record_key or "unspooled",
                )
            elif record_key:
                existing = spool.get(record_key)
                if not (existing and existing.get("permanent")):
                    retained = spool.mark_permanent(record_key, exc)
                    if retained:
                        self._report_permanent(retained, exc)
            else:
                self._report_permanent(
                    {"key": f"{sid}:(unspooled)"}, exc
                )
        finally:
            if record_key:
                self._in_flight_discard(record_key)
            if desired_key:
                self._in_flight_discard(desired_key)
            with self._active_lock:
                self._active_archives -= 1
                idle = self._active_archives <= 0
            # Only drain the backlog once no direct archive is in flight.
            if acknowledged and idle:
                self._schedule_replay()

    def sync_turn(
        self,
        user_content: str,
        assistant_content: str,
        *,
        session_id: str = "",
        messages: Optional[List[Dict[str, Any]]] = None,
        turn_author: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persist turn via background daemon thread."""
        sid = session_id or self._session_id or "default"
        now_ts = int(time.time())
        seq = self._next_message_seq()
        uid = f"{sid}_{now_ts}_{seq}"
        aid = f"{sid}_{now_ts}_{seq}_a"

        def _worker() -> None:
            client = self._client or AmrUdsClient(socket_path=self._socket_path)

            # 1. session.ingest (raw transcript into sessions.db), persist-first.
            try:
                ingest_params = {
                    "session_id": sid,
                    "source": "hermes_plugin",
                    "project_id": self._project_id,
                    "messages": [
                        {
                            "message_id": uid,
                            "role": "user",
                            "content": user_content,
                            "timestamp": now_ts,
                        },
                        {
                            "message_id": aid,
                            "role": "assistant",
                            "content": assistant_content,
                            "timestamp": now_ts,
                        },
                    ],
                }
                self._ingest_turn(client, sid, ingest_params)
            except Exception as exc:
                logger.debug("AMR session.ingest failed open: %s", exc)

            # 2. memory.create (v3.0 standard) if auto_extract is enabled and
            # substantial content. Fail-open: never blocks the hook.
            if self._auto_extract:
                fact_text = f"User: {user_content}\nAssistant: {assistant_content}"
                create_params = {
                    "content": fact_text,
                    "project_id": self._project_id,
                    "type": "general",
                    "status": "ACTIVE",
                    "source_refs": [f"session:{sid}"],
                }
                created = False
                try:
                    created = bool(
                        self._fail_open_call(
                            client, "memory.create", create_params, 15.0
                        )
                    )
                except Exception as exc:
                    logger.debug("AMR memory.create raised: %s", exc)

                if not created:
                    # v3 通道无响应/失败时兜底到 legacy memory.record（该通道已接入
                    # SSOT 底账 + Outbox，不再产生被每日对账下架的孤儿投影），
                    # 保证本轮事实不丢。
                    try:
                        legacy_params = {
                            "content": fact_text,
                            "project_id": self._project_id,
                            "memory_type": "fact",
                            "session_id": sid,
                        }
                        self._fail_open_call(
                            client, "memory.record", legacy_params, 15.0
                        )
                    except Exception as legacy_exc:
                        logger.debug(
                            "AMR memory.record fallback failed: %s", legacy_exc
                        )

        t = spawn_context_thread(_worker, name="amr_sync_turn", daemon=True)
        t.start()

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return []

    def shutdown(self) -> None:
        """Clean shutdown executor and stop scheduling new replays."""
        self._closed = True
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass


def register_memory_provider() -> MemoryProvider:
    """Entry point for dynamic provider discovery."""
    return AmrMemoryProvider()

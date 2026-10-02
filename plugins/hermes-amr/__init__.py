"""AMR (AI Memory Runtime) memory provider for Hermes.

Directly connects via Unix Domain Socket (UDS) to local AMR (amr.service),
supporting zero-overhead prefetch, background turn ingestion (session.ingest),
and memory fact persistence (memory.create).
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent.memory_provider import (
    INDICATOR_GLYPH,
    MemoryProvider,
    RecallStatus,
    is_trivial_prompt,
    spawn_context_thread,
)

try:
    from ._client import AmrUdsClient, DEFAULT_SOCKET_PATH
except ImportError:
    from _client import AmrUdsClient, DEFAULT_SOCKET_PATH

logger = logging.getLogger(__name__)

_DEFAULT_PROJECT_ID = "crypto-infrastructure"
_PREFETCH_TIMEOUT = 0.3  # Strict 300ms requirement per prompt/owner instructions


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
    """Hermes MemoryProvider implementation backed by AMR."""

    def __init__(self) -> None:
        self._config: Dict[str, Any] = {}
        self._client: Optional[AmrUdsClient] = None
        self._socket_path = DEFAULT_SOCKET_PATH
        self._project_id = _DEFAULT_PROJECT_ID
        self._top_k = 3
        self._auto_extract = True
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="amr_prefetch")
        self._pending_future: Optional[concurrent.futures.Future] = None
        self._last_recall_status: Optional[RecallStatus] = None
        self._session_id: str = ""
        self._hermes_home: Optional[Path] = None

    @property
    def name(self) -> str:
        return "amr"

    def is_available(self) -> bool:
        """Fast check if the UDS socket exists and is connectable."""
        cfg = _load_amr_config(self._hermes_home)
        socket_path = cfg.get("socket_path", self._socket_path)
        client = AmrUdsClient(socket_path=socket_path)
        return client.is_available()

    def unavailable_reason(self) -> str:
        return f"AMR socket at {self._socket_path} is unavailable or not running (amr.service)."

    def initialize(self, session_id: str, **kwargs) -> None:
        """Initialize AMR provider with profile-scoped config."""
        self._session_id = session_id
        home_arg = kwargs.get("hermes_home")
        if home_arg:
            self._hermes_home = Path(home_arg)

        self._config = _load_amr_config(self._hermes_home)
        self._socket_path = kwargs.get("socket_path") or self._config.get("socket_path", DEFAULT_SOCKET_PATH)
        self._project_id = kwargs.get("project_id") or self._config.get("project_id", _DEFAULT_PROJECT_ID)
        self._top_k = int(kwargs.get("top_k") or self._config.get("top_k", 3))
        self._auto_extract = bool(kwargs.get("auto_extract", self._config.get("auto_extract", True)))

        client_arg = kwargs.get("client")
        if client_arg:
            self._client = client_arg
        else:
            self._client = AmrUdsClient(socket_path=self._socket_path, request_timeout=1.0)
        logger.info("AMR MemoryProvider initialized (socket=%s, project=%s)", self._socket_path, self._project_id)

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        """Queue an asynchronous prefetch over UDS."""
        if is_trivial_prompt(query):
            self._pending_future = None
            return

        if not self._client:
            self._client = AmrUdsClient(socket_path=self._socket_path)

        def _do_search() -> Any:
            params = {
                "query": query,
                "project_id": self._project_id,
                "limit": self._top_k,
                "agent_id": "hermes",
            }
            return self._client.call("memory.search", params, timeout=0.3)

        self._pending_future = self._executor.submit(_do_search)

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Retrieve prefetch results within 0.3s (300ms) and format as Markdown context."""
        self._last_recall_status = None
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
                    "agent_id": "hermes",
                }
                res = self._client.call("memory.search", params, timeout=_PREFETCH_TIMEOUT)
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
                content = item.get("content") or item.get("text") or item.get("fact")
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

        def _worker() -> None:
            client = self._client or AmrUdsClient(socket_path=self._socket_path)

            # 1. session.ingest (raw transcript into sessions.db)
            try:
                ingest_params = {
                    "session_id": sid,
                    "agent_id": "hermes",
                    "source": "hermes_plugin",
                    "project_id": self._project_id,
                    "messages": [
                        {
                            "message_id": f"{sid}_u_{now_ts}",
                            "role": "user",
                            "content": user_content,
                            "timestamp": now_ts,
                        },
                        {
                            "message_id": f"{sid}_a_{now_ts}",
                            "role": "assistant",
                            "content": assistant_content,
                            "timestamp": now_ts,
                        },
                    ],
                }
                client.call("session.ingest", ingest_params, timeout=2.0)
            except Exception as exc:
                logger.debug("AMR session.ingest failed: %s", exc)

            # 2. memory.create (v3.0 standard) if auto_extract is enabled and substantial content
            if self._auto_extract:
                try:
                    create_params = {
                        "content": f"User: {user_content}\nAssistant: {assistant_content}",
                        "project_id": self._project_id,
                        "agent_id": "hermes",
                        "type": "general",
                        "status": "ACTIVE",
                        "source_refs": [f"session:{sid}"],
                    }
                    res = client.call("memory.create", create_params, timeout=15.0)
                    if res is None:
                        # Fallback to memory.record if needed
                        legacy_params = {
                            "content": f"User: {user_content}\nAssistant: {assistant_content}",
                            "project_id": self._project_id,
                            "agent_id": "hermes",
                            "source": "hermes_plugin",
                            "memory_type": "fact",
                        }
                        client.call("memory.record", legacy_params, timeout=15.0)
                except Exception as exc:
                    logger.debug("AMR memory.create failed: %s", exc)

        t = spawn_context_thread(_worker, name="amr_sync_turn", daemon=True)
        t.start()

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return []

    def shutdown(self) -> None:
        """Clean shutdown executor."""
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass


def register_memory_provider() -> MemoryProvider:
    """Entry point for dynamic provider discovery."""
    return AmrMemoryProvider()

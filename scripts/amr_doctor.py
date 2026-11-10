#!/usr/bin/env python3
"""AI Memory Runtime (AMR) - Doctor Diagnostic Tool.

Performs health and consistency checks on:
- Business Unix Domain Socket (system.health)
- Admin Unix Domain Socket (admin.status, status.metrics, outbox.list_dead_letters)
- SQLite database integrity (PRAGMA quick_check via read-only URI)
- Qdrant vector collections status

Exits with:
- 0: HEALTHY
- 1: DEGRADED
- 2: UNHEALTHY

Security guarantee:
NEVER outputs memory content, credentials/API keys, or source_refs.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.interfaces.ipc.protocol import (
    make_jsonrpc_request,
    read_frame,
    write_frame,
)


def _get_default_runtime_dir() -> str:
    return os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}" if hasattr(os, "getuid") else "/tmp")


async def send_ipc_rpc(
    socket_path: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 5.0,
) -> Dict[str, Any]:
    """Send JSON-RPC over Unix Domain Socket with timeout."""
    if not os.path.exists(socket_path):
        raise FileNotFoundError(f"Socket '{socket_path}' does not exist")

    reader, writer = await asyncio.wait_for(
        asyncio.open_unix_connection(socket_path),
        timeout=timeout,
    )
    try:
        req = make_jsonrpc_request(method=method, params=params or {}, req_id=1)
        await asyncio.wait_for(write_frame(writer, req), timeout=timeout)
        resp = await asyncio.wait_for(read_frame(reader), timeout=timeout)
        if "error" in resp and resp["error"]:
            err = resp["error"]
            raise RuntimeError(f"RPC Error ({err.get('code')}): {err.get('message')}")
        return resp.get("result", {})
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


def check_sqlite_integrity(sqlite_path: str, timeout: float = 5.0) -> Dict[str, Any]:
    """Check SQLite database integrity using read-only URI mode and PRAGMA quick_check."""
    resolved_path = Path(sqlite_path).resolve()
    if not resolved_path.exists():
        return {
            "status": "unhealthy",
            "path": str(resolved_path),
            "quick_check": "file_not_found",
            "error": f"Database file does not exist at '{resolved_path}'",
        }

    uri = f"file:{resolved_path}?mode=ro"
    conn = None
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=timeout)
        cur = conn.cursor()
        cur.execute("PRAGMA quick_check;")
        rows = cur.fetchall()
        if rows and len(rows) == 1 and rows[0][0] == "ok":
            return {
                "status": "ok",
                "path": str(resolved_path),
                "quick_check": "ok",
            }
        else:
            errors = [str(r[0]) for r in rows] if rows else ["unknown integrity failure"]
            return {
                "status": "unhealthy",
                "path": str(resolved_path),
                "quick_check": "failed",
                "error": "; ".join(errors[:5]),
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "path": str(resolved_path),
            "quick_check": "error",
            "error": str(e),
        }
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def sanitize_dead_letters(dead_letters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sanitize dead letter records to guarantee no content/secrets/source_refs are leaked."""
    sanitized = []
    for dl in dead_letters:
        sanitized.append({
            "id": dl.get("id"),
            "target_collection": dl.get("target_collection") or dl.get("collections"),
            "memory_id": dl.get("memory_id"),
            "projection_revision": dl.get("projection_revision") or dl.get("revision"),
            "retry_count": dl.get("retry_count"),
            "last_error": str(dl.get("last_error", ""))[:200],
            "dead_letter_at": dl.get("dead_letter_at"),
        })
    return sanitized


def _resolve_db_path(raw_path: str, config_path: Optional[str] = None) -> str:
    """Resolve a possibly-relative SQLite path.

    Order: absolute -> cwd -> config file dir -> config repo root -> script repo root.
    Falls back to cwd-relative when nothing exists yet.
    """
    p = Path(raw_path)
    if p.is_absolute():
        return str(p)
    candidates = [Path.cwd() / p]
    if config_path:
        cfg = Path(config_path).resolve()
        candidates.extend([cfg.parent / p, cfg.parent.parent / p])
    candidates.append(ROOT_DIR / p)
    for cand in candidates:
        if cand.exists():
            return str(cand.resolve())
    return str((Path.cwd() / p).resolve())


async def run_diagnostics(
    admin_socket: Optional[str] = None,
    business_socket: Optional[str] = None,
    config_path: Optional[str] = None,
    timeout: float = 5.0,
) -> Dict[str, Any]:
    """Run all AMR doctor diagnostic checks independently and produce structured report."""
    # 1. Resolve configuration
    app_config = None
    resolved_sqlite_path = "data/sessions.db"
    try:
        from config.settings import load_config
        app_config = load_config(config_path)
        resolved_sqlite_path = app_config.storage.sqlite_path
        if not admin_socket:
            admin_socket = os.getenv("AMR_ADMIN_SOCKET_PATH", app_config.server.admin_socket)
        if not business_socket:
            business_socket = os.getenv("AMR_SOCKET_PATH", app_config.server.business_socket)
    except Exception:
        pass

    runtime_dir = _get_default_runtime_dir()
    admin_socket = admin_socket or os.getenv("AMR_ADMIN_SOCKET_PATH", os.path.join(runtime_dir, "qdrant-bge-admin.sock"))
    business_socket = business_socket or os.getenv("AMR_SOCKET_PATH", os.path.join(runtime_dir, "qdrant-bge.sock"))

    # Resolve relative sqlite_path against cwd / config location / repo root
    resolved_sqlite_path = _resolve_db_path(resolved_sqlite_path, config_path)

    checks: Dict[str, Any] = {}

    # 2. Check Business Socket (system.health)
    business_res: Dict[str, Any] = {"status": "unhealthy", "socket": business_socket}
    try:
        b_data = await send_ipc_rpc(business_socket, "system.health", {"agent_id": "system"}, timeout=timeout)
        b_status = b_data.get("status", "unknown")
        model_state = b_data.get("model_state", "unknown")
        qdrant_ok = b_data.get("qdrant_ok", True)
        sqlite_ok = b_data.get("sqlite_ok", True)

        business_res["service"] = b_data.get("service", "ai-memory-runtime")
        business_res["model_state"] = model_state
        business_res["qdrant_ok"] = qdrant_ok
        business_res["sqlite_ok"] = sqlite_ok

        b_status_lower = str(b_status).lower()
        model_state_lower = str(model_state).lower()

        if b_status_lower == "healthy" and sqlite_ok and qdrant_ok:
            if model_state_lower in ("ready", "loaded"):
                business_res["status"] = "ok"
            elif model_state_lower in ("loading", "idle", "unloaded"):
                business_res["status"] = "degraded"
                business_res["warning"] = f"Model state is '{model_state}'"
            else:
                business_res["status"] = "degraded"
        else:
            business_res["status"] = "degraded" if (sqlite_ok and not qdrant_ok) else "unhealthy"
    except Exception as e:
        business_res["status"] = "unhealthy"
        business_res["error"] = str(e)
    checks["business_ipc"] = business_res

    # 3. Check Admin Socket (admin.status)
    admin_res: Dict[str, Any] = {"status": "unhealthy", "socket": admin_socket}
    admin_data: Optional[Dict[str, Any]] = None
    try:
        admin_data = await send_ipc_rpc(admin_socket, "admin.status", timeout=timeout)
        admin_res["status"] = "ok"
        if isinstance(admin_data, dict):
            engine_info = admin_data.get("engine", {})
            admin_res["engine_state"] = engine_info.get("state", "unknown")
            admin_res["device"] = engine_info.get("device", "unknown")
            if "sqlite_path" in admin_data and admin_data["sqlite_path"]:
                resolved_sqlite_path = _resolve_db_path(admin_data["sqlite_path"], config_path)
    except Exception as e:
        admin_res["status"] = "unhealthy"
        admin_res["error"] = str(e)
    checks["admin_ipc"] = admin_res

    # 4. Check Status Metrics (status.metrics)
    metrics_res: Dict[str, Any] = {"status": "unknown"}
    try:
        m_data = await send_ipc_rpc(admin_socket, "status.metrics", timeout=timeout)
        metrics_res["status"] = "ok"
        # Extract high-level summary counters safely
        metrics_res["summary"] = {
            "requests_total": m_data.get("requests_total", 0),
            "errors_total": m_data.get("errors_total", 0),
            "outbox_processed": m_data.get("outbox", {}).get("processed", 0) if isinstance(m_data.get("outbox"), dict) else m_data.get("outbox_processed", 0),
            "outbox_dead_letter": m_data.get("outbox", {}).get("dead_letter", 0) if isinstance(m_data.get("outbox"), dict) else m_data.get("outbox_dead_letter", 0),
        }
    except Exception as e:
        metrics_res["status"] = "degraded" if admin_res["status"] == "ok" else "unhealthy"
        metrics_res["error"] = str(e)
    checks["metrics"] = metrics_res

    # 5. Check Dead Letters (outbox.list_dead_letters)
    dead_letters_res: Dict[str, Any] = {"status": "unknown"}
    try:
        dl_data = await send_ipc_rpc(admin_socket, "outbox.list_dead_letters", {"limit": 10}, timeout=timeout)
        total_dead = dl_data.get("total", 0)
        raw_dl = dl_data.get("dead_letters", [])
        dead_letters_res["dead_letters_count"] = total_dead
        dead_letters_res["samples"] = sanitize_dead_letters(raw_dl[:5])
        if total_dead == 0:
            dead_letters_res["status"] = "ok"
        else:
            dead_letters_res["status"] = "degraded"
            dead_letters_res["warning"] = f"Outbox has {total_dead} dead letter task(s)"
    except Exception as e:
        dead_letters_res["status"] = "degraded" if admin_res["status"] == "ok" else "unhealthy"
        dead_letters_res["error"] = str(e)
    checks["outbox_dead_letters"] = dead_letters_res

    # 6. Check Qdrant Collections (admin.collections)
    qdrant_res: Dict[str, Any] = {"status": "unknown"}
    try:
        c_data = await send_ipc_rpc(admin_socket, "admin.collections", timeout=timeout)
        if isinstance(c_data, dict):
            qdrant_res["status"] = "ok"
            qdrant_res["collections_count"] = len(c_data)
            qdrant_res["collections"] = {k: {"points_count": v.get("points_count", 0)} for k, v in c_data.items() if isinstance(v, dict)}
        elif isinstance(c_data, list):
            qdrant_res["status"] = "ok"
            qdrant_res["collections_count"] = len(c_data)
            qdrant_res["collections"] = {c: {} for c in c_data}
        else:
            qdrant_res["status"] = "ok"
            qdrant_res["collections_count"] = 0
    except Exception as e:
        # Fallback to collections from admin.status if available
        if admin_data and isinstance(admin_data, dict) and "collections" in admin_data:
            colls = admin_data.get("collections", [])
            qdrant_res["status"] = "ok"
            qdrant_res["collections_count"] = len(colls)
            qdrant_res["collections"] = {c: {} for c in colls}
        else:
            qdrant_res["status"] = "degraded" if business_res.get("qdrant_ok") else "unhealthy"
            qdrant_res["error"] = str(e)
    checks["qdrant"] = qdrant_res

    # 7. Check SQLite Integrity (quick_check)
    sqlite_res = check_sqlite_integrity(resolved_sqlite_path, timeout=timeout)
    checks["sqlite"] = sqlite_res

    # 8. Compute overall evaluation
    # Exit codes: 0 = healthy, 1 = degraded, 2 = unhealthy
    critical_checks = ["business_ipc", "admin_ipc", "sqlite"]
    is_unhealthy = any(checks[k].get("status") == "unhealthy" for k in critical_checks)

    if is_unhealthy:
        overall_status = "unhealthy"
        exit_code = 2
    else:
        # Check if anything is degraded or secondary component is unhealthy
        is_degraded = (
            any(v.get("status") in ("degraded", "unhealthy") for v in checks.values())
            or checks["outbox_dead_letters"].get("dead_letters_count", 0) > 0
            or str(business_res.get("model_state", "")).lower() not in ("ready", "loaded")
        )
        if is_degraded:
            overall_status = "degraded"
            exit_code = 1
        else:
            overall_status = "healthy"
            exit_code = 0

    return {
        "status": overall_status,
        "exit_code": exit_code,
        "timestamp": int(time.time()),
        "checks": checks,
    }


def format_human_report(report: Dict[str, Any]) -> str:
    """Format doctor report into readable terminal output."""
    lines = []
    lines.append("=" * 66)
    lines.append("     AI Memory Runtime (AMR) - System Doctor Diagnostic Report")
    lines.append("=" * 66)

    def status_tag(st: str) -> str:
        if st == "ok":
            return "[  OK  ]"
        elif st == "degraded":
            return "[ WARN ]"
        else:
            return "[ FAIL ]"

    checks = report.get("checks", {})

    # Business Socket
    b = checks.get("business_ipc", {})
    b_tag = status_tag(b.get("status", "unhealthy"))
    b_info = f"socket={b.get('socket', '-')} (model={b.get('model_state', '-')})"
    if "error" in b:
        b_info += f" Error: {b['error']}"
    lines.append(f"{b_tag} Business IPC   : {b_info}")

    # Admin Socket
    a = checks.get("admin_ipc", {})
    a_tag = status_tag(a.get("status", "unhealthy"))
    a_info = f"socket={a.get('socket', '-')} (engine={a.get('engine_state', '-')}, device={a.get('device', '-')})"
    if "error" in a:
        a_info += f" Error: {a['error']}"
    lines.append(f"{a_tag} Admin IPC      : {a_info}")

    # SQLite
    sq = checks.get("sqlite", {})
    sq_tag = status_tag(sq.get("status", "unhealthy"))
    sq_info = f"path={sq.get('path', '-')} (PRAGMA quick_check: {sq.get('quick_check', '-')})"
    if "error" in sq:
        sq_info += f" Error: {sq['error']}"
    lines.append(f"{sq_tag} SQLite SSOT    : {sq_info}")

    # Qdrant
    q = checks.get("qdrant", {})
    q_tag = status_tag(q.get("status", "unhealthy"))
    coll_names = list(q.get("collections", {}).keys())
    q_info = f"collections_count={q.get('collections_count', 0)} ({', '.join(coll_names) if coll_names else 'none'})"
    if "error" in q:
        q_info += f" Error: {q['error']}"
    lines.append(f"{q_tag} Qdrant Index   : {q_info}")

    # Metrics
    m = checks.get("metrics", {})
    m_tag = status_tag(m.get("status", "unhealthy"))
    sm = m.get("summary", {})
    m_info = f"requests={sm.get('requests_total', 0)}, errors={sm.get('errors_total', 0)}, outbox_processed={sm.get('outbox_processed', 0)}"
    lines.append(f"{m_tag} Metrics Core   : {m_info}")

    # Dead Letters
    dl = checks.get("outbox_dead_letters", {})
    dl_tag = status_tag(dl.get("status", "unhealthy"))
    dl_count = dl.get("dead_letters_count", 0)
    dl_info = f"count={dl_count}"
    if dl.get("warning"):
        dl_info += f" ({dl['warning']})"
    lines.append(f"{dl_tag} Outbox Dead-Ltr: {dl_info}")

    lines.append("-" * 66)
    st = report.get("status", "unknown").upper()
    code = report.get("exit_code", 2)
    lines.append(f"Overall Status: {st} (exit code {code})")
    lines.append("=" * 66)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        prog="amr_doctor",
        description="Diagnostic doctor tool for AI Memory Runtime (AMR). Checks IPC sockets, SQLite SSOT, Outbox, and Qdrant."
    )
    parser.add_argument(
        "--socket",
        "-s",
        dest="admin_socket",
        default=None,
        help="Path to dedicated Admin Unix Domain Socket (admin_socket)."
    )
    parser.add_argument(
        "--business-socket",
        "-b",
        dest="business_socket",
        default=None,
        help="Path to Business Unix Domain Socket."
    )
    parser.add_argument(
        "--config",
        "-c",
        dest="config_path",
        default=None,
        help="Path to YAML configuration file (config.yaml)."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output diagnostics in machine-readable JSON format."
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=5.0,
        help="Timeout in seconds for socket/database operations (default: 5.0s)."
    )

    args = parser.parse_args()

    report = asyncio.run(run_diagnostics(
        admin_socket=args.admin_socket,
        business_socket=args.business_socket,
        config_path=args.config_path,
        timeout=args.timeout,
    ))

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(format_human_report(report))

    sys.exit(report["exit_code"])


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Install per-application AMR 2.0 MCP entries into CC Switch.

CC Switch stores MCP servers in its SQLite database. A single shared entry
cannot preserve per-agent identities, so this script upserts one entry per
application and enables it only for that application:

  amr          -> claude_code
  ai-memory    -> opencode
  amr-codex    -> codex
  amr-gemini   -> gemini_cli
  amr-pi       -> pi

Only file paths are written; token values are never read or printed. Run with
`--dry-run` first, then without it while CC Switch is closed or after a DB
backup. Re-running is idempotent.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys

FLAGS = (
    "enabled_claude",
    "enabled_codex",
    "enabled_gemini",
    "enabled_grokbuild",
    "enabled_opencode",
    "enabled_mcode",
    "enabled_agy",
    "enabled_kimi",
    "enabled_dsh",
    "enabled_hermes",
    "enabled_pi",
    "enabled_omp",
    "enabled_copilot",
)

ENTRIES = {
    "amr": ("amr", "claude_code", "enabled_claude", "AMR 2.0 local adapter (claude_code)"),
    "ai-memory": ("ai-memory", "opencode", "enabled_opencode", "AMR 2.0 local adapter (opencode)"),
    "amr-codex": ("amr-codex", "codex", "enabled_codex", "AMR 2.0 local adapter (codex)"),
    "amr-gemini": ("amr-gemini", "gemini_cli", "enabled_gemini", "AMR 2.0 local adapter (gemini_cli)"),
    "amr-pi": ("amr-pi", "pi", "enabled_pi", "AMR 2.0 local adapter (pi)"),
}


def server_config(amr_root: Path, token_root: Path, agent: str) -> str:
    env = {
        "PYTHONPATH": str(amr_root),
        "AMR_SOURCE_AGENT": agent,
        "AMR_SOCKET_PATH": f"/run/user/{os.getuid()}/amr2-{agent}.sock",
        "AMR2_TOKEN_FILE": str(token_root / f"{agent}.token"),
    }
    return json.dumps(
        {
            "type": "stdio",
            "command": str(amr_root / ".venv" / "bin" / "python"),
            "args": [str(amr_root / "src" / "interfaces" / "mcp" / "bridge.py")],
            "env": env,
        },
        separators=(",", ":"),
    )


def apply(db: Path, amr_root: Path, token_root: Path, dry_run: bool) -> None:
    if not db.is_file():
        raise SystemExit(f"CC Switch database not found: {db}")
    con = sqlite3.connect(db)
    try:
        con.execute("begin immediate")
        for row_id, (name, agent, enabled, description) in ENTRIES.items():
            cfg = server_config(amr_root, token_root, agent)
            row = con.execute("select id from mcp_servers where id=?", (row_id,)).fetchone()
            if dry_run:
                print(f"{row_id}: {'update' if row else 'insert'} {agent} -> {enabled}")
                continue
            if row:
                con.execute(
                    "update mcp_servers set name=?, server_config=?, description=? where id=?",
                    (name, cfg, description, row_id),
                )
            else:
                con.execute(
                    "insert into mcp_servers (id,name,server_config,description,tags) values (?,?,?,?,?)",
                    (row_id, name, cfg, description, "[]"),
                )
            assignments = ",".join(f"{flag}=0" for flag in FLAGS)
            con.execute(f"update mcp_servers set {assignments} where id=?", (row_id,))
            con.execute(f"update mcp_servers set {enabled}=1 where id=?", (row_id,))
        if dry_run:
            con.rollback()
        else:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.path.expanduser("~/.cc-switch/cc-switch.db"))
    parser.add_argument("--amr-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument(
        "--token-root",
        default=os.path.expanduser("~/.config/amr/agents/current/tokens"),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    apply(Path(args.db), Path(args.amr_root), Path(args.token_root), args.dry_run)
    if not args.dry_run:
        print("CC Switch AMR MCP entries updated; reopen/refresh CC Switch to apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

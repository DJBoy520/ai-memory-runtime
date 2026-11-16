"""Validate checked-in client integration templates and the CC Switch installer."""
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "deploy" / "clients"


def test_json_client_templates_reference_token_files_without_secrets():
    expected = {
        "pi.mcp.json": "pi",
        "gemini.settings.json": "gemini_cli",
        "claude.mcp.json": "claude_code",
        "zcode.mcp.json": "zcode",
    }
    for name, agent in expected.items():
        data = json.loads((CLIENTS / name).read_text())
        if name == "zcode.mcp.json":
            args = data["mcp"]["servers"]["amr"]["args"]
            assert "--token-file" in args
            token = args[args.index("--token-file") + 1]
        else:
            env = data["mcpServers"]["amr"]["env"]
            assert env["AMR_SOURCE_AGENT"] == agent
            token = env["AMR2_TOKEN_FILE"]
        assert token.endswith(f"/{agent}.token")
        # A token file path is expected; a 43-character token value is not.
        assert len(token) > 40 and token.startswith("/") and "\n" not in token


def _create_db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute(
        """
        create table mcp_servers (
            id text primary key, name text not null, server_config text not null,
            description text, homepage text, docs text, tags text not null default '[]',
            enabled_claude boolean not null default 0,
            enabled_codex boolean not null default 0,
            enabled_gemini boolean not null default 0,
            enabled_grokbuild boolean not null default 0,
            enabled_opencode boolean not null default 0,
            enabled_mcode boolean not null default 0,
            enabled_agy boolean not null default 0,
            enabled_kimi boolean not null default 0,
            enabled_dsh boolean not null default 0,
            enabled_hermes boolean not null default 0,
            enabled_pi boolean not null default 0,
            enabled_omp boolean not null default 0,
            enabled_copilot boolean not null default 0
        )
        """
    )
    con.commit()
    con.close()


def test_ccswitch_installer_is_idempotent_and_writes_paths_only(tmp_path):
    db = tmp_path / "cc-switch.db"
    _create_db(db)
    token_root = tmp_path / "tokens"
    cmd = [
        sys.executable,
        str(CLIENTS / "ccswitch-amr.py"),
        "--db",
        str(db),
        "--amr-root",
        str(ROOT),
        "--token-root",
        str(token_root),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    subprocess.run(cmd, check=True, capture_output=True, text=True)

    con = sqlite3.connect(db)
    rows = con.execute(
        "select id, name, server_config, enabled_claude, enabled_codex, enabled_gemini, enabled_opencode, enabled_pi from mcp_servers order by id"
    ).fetchall()
    con.close()
    assert [row[0] for row in rows] == ["ai-memory", "amr", "amr-codex", "amr-gemini", "amr-pi"]
    by_id = {row[0]: row for row in rows}
    assert by_id["amr"][3] == 1 and by_id["amr"][4:] == (0, 0, 0, 0)
    assert by_id["ai-memory"][6] == 1 and by_id["ai-memory"][3:6] == (0, 0, 0)
    assert by_id["amr-codex"][4] == 1
    assert by_id["amr-gemini"][5] == 1
    assert by_id["amr-pi"][7] == 1
    for row in rows:
        config = json.loads(row[2])
        token = config["env"]["AMR2_TOKEN_FILE"]
        assert token.startswith(str(token_root))
        assert len(token) < 200  # path, not a credential

#!/usr/bin/env python3
"""
AI Memory Runtime - Universal Multi-Agent Ingestion Script
涵盖:
1. ZCode: ~/.zcode/cli/db/db.sqlite 中的真实交互会话
2. DSH: ~/.dsh/sessions/ 中的 v3/v4 压缩会话
3. Hermes: ~/.hermes/state.db 增量会话
4. OpenClaw: ~/.openclaw/agents/*/sessions/*.trajectory.jsonl* 增量会话
严格执行去重、内容规范清洗、双表(raw_sessions/raw_messages)入库。
"""

import os
import sys
import glob
import time
import json
import sqlite3
import hashlib
import subprocess
import logging
from pathlib import Path
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingest_all_agents")

ROOT_DIR = Path(__file__).resolve().parent.parent
AMR_DB_PATH = ROOT_DIR / "data" / "sessions.db"

def compute_hash(role: str, content: str) -> str:
    return hashlib.sha256(f"{role}:{content}".encode("utf-8")).hexdigest()

def clean_text(text: str) -> str:
    if not text:
        return ""
    if len(text) > 30000:
        text = text[:10000] + "\n\n... [TRUNCATED MASSIVE LOG STREAM BY AMR] ...\n\n" + text[-5000:]
    return text.strip()

def is_meaningful(role: str, content: str) -> bool:
    if not content or len(content.strip()) < 3:
        return False
    lower = content.lower().strip()
    if lower in ("ping", "pong", "heartbeat", "ok", "ack", "healthcheck"):
        return False
    if lower.startswith("heartbeat:") and len(lower) < 60:
        return False
    if "Available agent types for the Agent tool:" in content and len(content) < 1000:
        return False
    return True

def normalize_project(directory: str, title: str = "") -> str:
    d = (directory or "").lower()
    t = (title or "").lower()
    if "aep-pki" in d or "aep-pki" in t: return "aep-pki"
    if "aep-tsa" in d or "aep-tsa" in t: return "aep-tsa"
    if "aep-chain" in d or "aep-chain" in t: return "aep-chain"
    if "aep-iam" in d or "aep-iam" in t: return "aep-iam"
    if "aep-website" in d or "aep-website" in t: return "aep-website"
    if "aep-refimpl" in d or "aep-refimpl" in t or "aep-release" in t: return "aep-refimpl"
    if "ai-memory-runtime" in d or "amr" in d or "amr" in t: return "ai-memory-runtime"
    if "opnsense-mihomo" in d or "mihomo" in t: return "opnsense-mihomo"
    if "signalkeepers" in d or "signalkeepers" in t: return "signalkeepers"
    if "vikey" in t or "加密狗" in t: return "vikey"
    if "ubuntu" in d or "ubuntu" in t: return "general"
    if "aep" in d or "aep" in t: return "aep"
    return "general"

class UniversalHarvester:
    def __init__(self, db_path: Path = AMR_DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._load_existing()

    def _load_existing(self):
        c = self.conn.cursor()
        c.execute("SELECT session_id FROM raw_sessions UNION SELECT session_id FROM sessions")
        self.existing_sids = set(r[0] for r in c.fetchall())
        c.execute("SELECT DISTINCT content_hash FROM raw_messages")
        self.existing_hashes = set(r[0] for r in c.fetchall())
        logger.info(f"Loaded existing AMR: {len(self.existing_sids)} sessions, {len(self.existing_hashes)} message hashes")

    def harvest_zcode(self) -> List[Dict[str, Any]]:
        zcode_db = os.path.expanduser("~/.zcode/cli/db/db.sqlite")
        if not os.path.exists(zcode_db):
            logger.warning(f"ZCode db not found at {zcode_db}")
            return []

        zconn = sqlite3.connect(f"file:{zcode_db}?mode=ro", uri=True)
        zc = zconn.cursor()

        # 排除当前会话以及自动化子 agent 会话
        current_sess = "session_b745d944516a263f9315e6fed22152fe"
        zc.execute("""
            SELECT id, title, directory, time_created, time_updated
            FROM session
            WHERE id NOT LIKE 'sess_subagent_%'
              AND id NOT LIKE 'sess_dwf-%'
              AND id != ?
            ORDER BY time_created ASC
        """, (current_sess,))
        sessions = zc.fetchall()

        harvested = []
        for s in sessions:
            sid, title, directory, tc, tu = s
            if sid in self.existing_sids:
                continue

            proj = normalize_project(directory, title)
            # 提取消息
            zc.execute("""
                SELECT m.id, json_extract(m.data, '$.role') as role, m.time_created
                FROM message m
                WHERE m.session_id = ?
                ORDER BY m.sequence ASC, m.time_created ASC
            """, (sid,))
            raw_msgs = zc.fetchall()

            clean_msgs = []
            seq = 1
            for mid, role, mtc in raw_msgs:
                role = role or "user"
                zc.execute("""
                    SELECT json_extract(p.data, '$.text')
                    FROM part p
                    WHERE p.message_id = ? AND json_extract(p.data, '$.type') = 'text'
                    ORDER BY p.sequence ASC
                """, (mid,))
                text_parts = [r[0] for r in zc.fetchall() if r[0]]
                if not text_parts:
                    continue
                content = clean_text("\n".join(text_parts))
                if not is_meaningful(role, content):
                    continue

                chash = compute_hash(role, content)
                ts = int((mtc or tc or time.time() * 1000) / 1000)
                clean_msgs.append({
                    "message_id": mid,
                    "role": role,
                    "content": content,
                    "content_hash": chash,
                    "sequence": seq,
                    "created_at": ts
                })
                seq += 1

            if clean_msgs:
                started_at = int((tc or clean_msgs[0]["created_at"] * 1000) / 1000)
                ended_at = int((tu or clean_msgs[-1]["created_at"] * 1000) / 1000)
                harvested.append({
                    "session_id": sid,
                    "agent_id": "zcode",
                    "project_id": proj,
                    "title": title or "",
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "status": "archived",
                    "messages": clean_msgs
                })

        logger.info(f"Harvested {len(harvested)} new sessions from ZCode")
        return harvested

    def harvest_dsh(self) -> List[Dict[str, Any]]:
        dsh_root = os.path.expanduser("~/.dsh/sessions")
        if not os.path.exists(dsh_root):
            return []

        harvested = []
        for root, dirs, files in os.walk(dsh_root):
            for d in dirs:
                if not d.startswith("session-"):
                    continue
                sid = d.replace("session-", "")
                if sid in self.existing_sids:
                    continue

                sdir = os.path.join(root, d)
                zst_files = [f for f in os.listdir(sdir) if f.endswith(".zstd")]
                if not zst_files:
                    continue
                
                # 优先选择 session.v4，其次其它
                zst_files.sort(key=lambda x: ("v4" in x, "v3" in x), reverse=True)
                zst_path = os.path.join(sdir, zst_files[0])
                try:
                    out = subprocess.check_output(["zstdcat", zst_path], stderr=subprocess.DEVNULL).decode("utf-8", errors="ignore")
                    lines = [json.loads(l) for l in out.strip().split("\n") if l.strip()]
                except Exception as e:
                    logger.warning(f"Failed to decompress {zst_path}: {e}")
                    continue

                clean_msgs = []
                started_at = None
                cwd = root
                seq = 1
                for item in lines:
                    itype = item.get("type", "")
                    if itype == "session":
                        started_at = int(item.get("createdAt", time.time() * 1000) / 1000)
                        if item.get("cwd"):
                            cwd = item.get("cwd")
                    elif itype in ("user/message", "assistant/message", "system/message"):
                        data = item.get("data", {})
                        msg_obj = data.get("message") if isinstance(data.get("message"), dict) else data
                        role = msg_obj.get("role") or itype.split("/")[0]
                        content = ""
                        raw_c = msg_obj.get("content")
                        if isinstance(raw_c, list):
                            parts = []
                            for p in raw_c:
                                if isinstance(p, dict) and p.get("type") == "text":
                                    parts.append(p.get("text", ""))
                                elif isinstance(p, str):
                                    parts.append(p)
                            content = "\n".join(parts)
                        elif isinstance(raw_c, str):
                            content = raw_c

                        content = clean_text(content)
                        if not is_meaningful(role, content):
                            continue

                        ts = int((item.get("time") or time.time() * 1000) / 1000)
                        chash = compute_hash(role, content)
                        clean_msgs.append({
                            "message_id": f"msg_dsh_{sid[:8]}_{seq}",
                            "role": role,
                            "content": content,
                            "content_hash": chash,
                            "sequence": seq,
                            "created_at": ts
                        })
                        seq += 1

                if clean_msgs:
                    proj = normalize_project(cwd)
                    harvested.append({
                        "session_id": sid,
                        "agent_id": "dsh",
                        "project_id": proj,
                        "title": "",
                        "started_at": started_at or clean_msgs[0]["created_at"],
                        "ended_at": clean_msgs[-1]["created_at"],
                        "status": "closed",
                        "messages": clean_msgs
                    })

        logger.info(f"Harvested {len(harvested)} new sessions from DSH")
        return harvested

    def harvest_hermes(self) -> List[Dict[str, Any]]:
        hermes_db = os.path.expanduser("~/.hermes/state.db")
        if not os.path.exists(hermes_db):
            return []

        h_conn = sqlite3.connect(f"file:{hermes_db}?mode=ro", uri=True)
        h_conn.row_factory = sqlite3.Row
        h_cur = h_conn.cursor()

        h_cur.execute("SELECT id, started_at, ended_at, message_count, title, archived FROM sessions")
        sessions = h_cur.fetchall()
        harvested = []
        for s in sessions:
            sid = s["id"]
            if sid in self.existing_sids:
                continue
            if not s["message_count"] or s["message_count"] == 0:
                continue

            h_cur.execute(
                "SELECT role, content, timestamp FROM messages WHERE session_id = ? ORDER BY timestamp ASC",
                (sid,)
            )
            raw_msgs = h_cur.fetchall()
            clean_msgs = []
            seq = 1
            for m in raw_msgs:
                role = "user" if m["role"] == "user" else ("assistant" if m["role"] == "assistant" else "system")
                content = clean_text(m["content"] or "")
                if not is_meaningful(role, content):
                    continue

                chash = compute_hash(role, content)
                clean_msgs.append({
                    "message_id": f"msg_hermes_{sid[:8]}_{seq}",
                    "role": role,
                    "content": content,
                    "content_hash": chash,
                    "sequence": seq,
                    "created_at": int(m["timestamp"] or s["started_at"] or time.time())
                })
                seq += 1

            if clean_msgs:
                proj = normalize_project(s["title"] or "")
                harvested.append({
                    "session_id": sid,
                    "agent_id": "hermes",
                    "project_id": proj,
                    "title": s["title"] or "",
                    "started_at": int(s["started_at"] or time.time()),
                    "ended_at": int(s["ended_at"] or time.time()) if s["ended_at"] else None,
                    "status": "archived" if s["archived"] else "active",
                    "messages": clean_msgs
                })

        logger.info(f"Harvested {len(harvested)} new sessions from Hermes")
        return harvested

    def commit(self, sessions_list: List[Dict[str, Any]]) -> Dict[str, int]:
        if not sessions_list:
            return {"sessions": 0, "messages": 0, "skipped_msgs": 0}

        cur = self.conn.cursor()
        ins_sess = 0
        ins_msg = 0
        skip_msg = 0

        for s in sessions_list:
            sid = s["session_id"]
            cur.execute("""
                INSERT OR IGNORE INTO raw_sessions (session_id, agent_id, project_id, started_at, ended_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (sid, s["agent_id"], s["project_id"], s["started_at"], s["ended_at"], s["status"]))
            ins_sess += 1

            for m in s["messages"]:
                chash = m["content_hash"]
                if chash in self.existing_hashes:
                    skip_msg += 1
                    continue

                cur.execute("""
                    INSERT OR IGNORE INTO raw_messages 
                    (message_id, session_id, role, content, content_hash, sequence, source_type, is_synthetic, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'imported', 0, ?)
                """, (m["message_id"], sid, m["role"], m["content"], chash, m["sequence"], m["created_at"]))
                self.existing_hashes.add(chash)
                ins_msg += 1

        self.conn.commit()
        logger.info(f"Committed {ins_sess} sessions, {ins_msg} messages, skipped {skip_msg} duplicate messages")
        return {"sessions": ins_sess, "messages": ins_msg, "skipped_msgs": skip_msg}

if __name__ == "__main__":
    harvester = UniversalHarvester()
    zcode_b = harvester.harvest_zcode()
    dsh_b = harvester.harvest_dsh()
    hermes_b = harvester.harvest_hermes()

    total = zcode_b + dsh_b + hermes_b
    logger.info(f"Total incremental sessions ready to ingest: {len(total)}")
    res = harvester.commit(total)
    print("\n--- Ingest Summary ---")
    print(f"Sessions: {res['sessions']}")
    print(f"Messages: {res['messages']}")
    print(f"Skipped Dups: {res['skipped_msgs']}")

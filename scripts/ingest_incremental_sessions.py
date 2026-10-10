#!/usr/bin/env python3
"""
AI Memory Runtime - Incremental Multi-Agent Session Harvester & Ingestion Engine
搜集 Hermes、DSH、OpenCode、OpenClaw 的新会话与归档流水，
遵循 AMR 规范（去重、清洗、断点排重、时序与内容哈希保障）。
"""

import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingest_incremental_sessions")

AMR_DB_PATH = Path("data/sessions.db")

def compute_content_hash(role: str, content: str) -> str:
    return hashlib.sha256(f"{role}:{content}".encode("utf-8")).hexdigest()

def clean_and_sanitize_text(text: str) -> str:
    if not text:
        return ""
    # 截断巨型日志或崩溃转储 (如超 30000 字符)
    if len(text) > 30000:
        text = text[:10000] + "\n\n... [TRUNCATED MASSIVE LOG STREAM BY AMR] ...\n\n" + text[-5000:]
    return text.strip()

def is_meaningful_message(role: str, content: str) -> bool:
    if not content or len(content.strip()) < 3:
        return False
    lower = content.lower().strip()
    # 过滤纯系统心跳、极简探测
    if lower in ("ping", "pong", "heartbeat", "ok", "ack", "healthcheck"):
        return False
    if lower.startswith("heartbeat:") and len(lower) < 60:
        return False
    return True

class SessionHarvester:
    def __init__(self, db_path: Path = AMR_DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._load_existing_state()

    def _load_existing_state(self):
        c = self.conn.cursor()
        c.execute("SELECT session_id FROM raw_sessions")
        self.existing_session_ids = set(r[0] for r in c.fetchall())
        c.execute("SELECT DISTINCT content_hash FROM raw_messages")
        self.existing_content_hashes = set(r[0] for r in c.fetchall())
        logger.info(f"Loaded existing AMR state: {len(self.existing_session_ids)} raw_sessions, {len(self.existing_content_hashes)} raw_messages hashes")

    def harvest_hermes(self) -> List[Dict[str, Any]]:
        """从 ~/.hermes/state.db 搜集未入库的新会话"""
        hermes_db = os.path.expanduser("~/.hermes/state.db")
        if not os.path.exists(hermes_db):
            logger.warning(f"Hermes state.db not found at {hermes_db}")
            return []

        h_conn = sqlite3.connect(f"file:{hermes_db}?mode=ro", uri=True)
        h_conn.row_factory = sqlite3.Row
        h_cur = h_conn.cursor()

        h_cur.execute("SELECT id, started_at, ended_at, message_count, title, archived FROM sessions")
        sessions = h_cur.fetchall()
        
        harvested = []
        for s in sessions:
            sid = s["id"]
            if sid in self.existing_session_ids:
                continue
            if not s["message_count"] or s["message_count"] == 0:
                continue

            # 读取该会话下的所有消息
            h_cur.execute(
                "SELECT role, content, timestamp FROM messages WHERE session_id = ? ORDER BY timestamp ASC",
                (sid,)
            )
            raw_msgs = h_cur.fetchall()
            
            clean_msgs = []
            seq = 1
            for m in raw_msgs:
                role = "user" if m["role"] == "user" else ("assistant" if m["role"] == "assistant" else "system")
                content = clean_and_sanitize_text(m["content"] or "")
                if not is_meaningful_message(role, content):
                    continue
                
                chash = compute_content_hash(role, content)
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
                harvested.append({
                    "session_id": sid,
                    "agent_id": "hermes",
                    "project_id": "general",
                    "started_at": int(s["started_at"] or time.time()),
                    "ended_at": int(s["ended_at"] or time.time()) if s["ended_at"] else None,
                    "status": "archived" if s["archived"] else "active",
                    "messages": clean_msgs
                })

        logger.info(f"Harvested {len(harvested)} new sessions from Hermes state.db")
        return harvested

    def harvest_dsh(self) -> List[Dict[str, Any]]:
        """从 ~/.dsh/sessions/ 搜集未入库的新会话"""
        dsh_root = os.path.expanduser("~/.dsh/sessions")
        if not os.path.exists(dsh_root):
            return []

        harvested = []
        for root, dirs, files in os.walk(dsh_root):
            for d in dirs:
                if not d.startswith("session-"):
                    continue
                sid = d.replace("session-", "")
                if sid in self.existing_session_ids:
                    continue
                
                sdir = os.path.join(root, d)
                zst_files = [f for f in os.listdir(sdir) if f.endswith(".zstd")]
                if not zst_files:
                    continue
                
                zst_path = os.path.join(sdir, zst_files[0])
                try:
                    out = subprocess.check_output(["zstdcat", zst_path], stderr=subprocess.DEVNULL).decode("utf-8", errors="ignore")
                    lines = [json.loads(l) for l in out.strip().split("\n") if l.strip()]
                except Exception as e:
                    logger.warning(f"Failed to decompress {zst_path}: {e}")
                    continue

                clean_msgs = []
                started_at = None
                seq = 1
                for item in lines:
                    itype = item.get("type", "")
                    if itype == "session":
                        started_at = int(item.get("createdAt", time.time() * 1000) / 1000)
                    elif itype in ("user/message", "assistant/message", "system/message"):
                        data = item.get("data", {})
                        role = itype.split("/")[0]
                        content = ""
                        raw_c = data.get("content")
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
                        
                        content = clean_and_sanitize_text(content)
                        if not is_meaningful_message(role, content):
                            continue
                        
                        ts = int((item.get("time") or time.time() * 1000) / 1000)
                        chash = compute_content_hash(role, content)
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
                    harvested.append({
                        "session_id": sid,
                        "agent_id": "dsh",
                        "project_id": "general",
                        "started_at": started_at or clean_msgs[0]["created_at"],
                        "ended_at": clean_msgs[-1]["created_at"],
                        "status": "closed",
                        "messages": clean_msgs
                    })

        logger.info(f"Harvested {len(harvested)} new sessions from DSH")
        return harvested

    def harvest_openclaw(self) -> List[Dict[str, Any]]:
        """从 ~/.openclaw/agents/*/sessions/*.trajectory.jsonl* 提取交互会话"""
        claw_pattern = os.path.expanduser("~/.openclaw/agents/*/sessions/*.trajectory.jsonl*")
        import glob
        trajs = glob.glob(claw_pattern)
        harvested = []

        for t in trajs:
            fname = os.path.basename(t)
            sid = fname.split(".")[0]
            if sid in self.existing_session_ids:
                continue

            lines = []
            try:
                if t.endswith(".zst"):
                    out = subprocess.check_output(["zstdcat", t], stderr=subprocess.DEVNULL).decode("utf-8", errors="ignore")
                    lines = [json.loads(l) for l in out.strip().split("\n") if l.strip()]
                else:
                    with open(t, "r", errors="ignore") as fp:
                        for line in fp:
                            if line.strip():
                                lines.append(json.loads(line))
            except Exception:
                continue

            clean_msgs = []
            seq = 1
            started_at = None
            for item in lines:
                itype = item.get("type")
                raw_ts = item.get("ts")
                if isinstance(raw_ts, (int, float)):
                    ts = int(raw_ts / 1000)
                else:
                    ts = int(time.time())
                if not started_at:
                    started_at = ts
                
                # 用户 Prompt
                if itype == "prompt.submitted":
                    data = item.get("data", {})
                    p = clean_and_sanitize_text(data.get("prompt", ""))
                    if is_meaningful_message("user", p):
                        clean_msgs.append({
                            "message_id": f"msg_claw_{sid[:8]}_{seq}",
                            "role": "user",
                            "content": p,
                            "content_hash": compute_content_hash("user", p),
                            "sequence": seq,
                            "created_at": ts
                        })
                        seq += 1
                elif itype == "model.completed":
                    data = item.get("data", {})
                    texts = data.get("assistantTexts", [])
                    if texts and isinstance(texts, list):
                        c = clean_and_sanitize_text("\n\n".join(texts))
                        if is_meaningful_message("assistant", c):
                            clean_msgs.append({
                                "message_id": f"msg_claw_{sid[:8]}_{seq}",
                                "role": "assistant",
                                "content": c,
                                "content_hash": compute_content_hash("assistant", c),
                                "sequence": seq,
                                "created_at": ts
                            })
                            seq += 1

            if clean_msgs:
                harvested.append({
                    "session_id": sid,
                    "agent_id": "openclaw",
                    "project_id": "general",
                    "started_at": started_at or int(time.time()),
                    "ended_at": clean_msgs[-1]["created_at"],
                    "status": "archived",
                    "messages": clean_msgs
                })

        logger.info(f"Harvested {len(harvested)} new sessions from OpenClaw trajectories")
        return harvested

    def commit_sessions(self, session_list: List[Dict[str, Any]]) -> Dict[str, int]:
        """将清洗后的新会话与消息批量持久化到 AMR SQLite (raw_sessions & raw_messages)"""
        if not session_list:
            return {"sessions": 0, "messages": 0, "skipped_msgs": 0}

        cur = self.conn.cursor()
        inserted_sessions = 0
        inserted_messages = 0
        skipped_messages = 0

        for s in session_list:
            sid = s["session_id"]
            # 1. 插入 raw_sessions
            cur.execute(
                """
                INSERT OR IGNORE INTO raw_sessions (session_id, agent_id, project_id, started_at, ended_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (sid, s["agent_id"], s["project_id"], s["started_at"], s["ended_at"], s["status"])
            )
            inserted_sessions += 1

            # 2. 插入 raw_messages
            for m in s["messages"]:
                chash = m["content_hash"]
                # 检查内容哈希去重
                if chash in self.existing_content_hashes:
                    skipped_messages += 1
                    continue
                
                cur.execute(
                    """
                    INSERT OR IGNORE INTO raw_messages 
                    (message_id, session_id, role, content, content_hash, sequence, source_type, is_synthetic, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'imported', 0, ?)
                    """,
                    (m["message_id"], sid, m["role"], m["content"], chash, m["sequence"], m["created_at"])
                )
                self.existing_content_hashes.add(chash)
                inserted_messages += 1

        self.conn.commit()
        logger.info(f"Committed batch: {inserted_sessions} sessions, {inserted_messages} messages, {skipped_messages} skipped dups")
        return {
            "sessions": inserted_sessions,
            "messages": inserted_messages,
            "skipped_msgs": skipped_messages
        }

if __name__ == "__main__":
    harvester = SessionHarvester()
    hermes_batch = harvester.harvest_hermes()
    dsh_batch = harvester.harvest_dsh()
    claw_batch = harvester.harvest_openclaw()

    total_batch = hermes_batch + dsh_batch + claw_batch
    logger.info(f"Total sessions to ingest across all agents: {len(total_batch)}")
    
    result = harvester.commit_sessions(total_batch)
    print("\n--- Ingest Summary ---")
    print(f"Total Sessions Ingested: {result['sessions']}")
    print(f"Total Messages Ingested: {result['messages']}")
    print(f"Duplicate Messages Skipped: {result['skipped_msgs']}")

#!/usr/bin/env python3
"""
scripts/adopt_orphan_points.py
反向补登脚本：将 Qdrant 中存在但 SQLite memories 表中不存在的孤儿点位补登至 SQLite SSOT。

技术依据：RFC-003 第 2.2 节 & opencode_task_reconciliation_p1.md
- 保持原有 memory_id 与 qdrant_point_id 不变
- status = 'active'
- created_at / updated_at 复用原有时间戳
- source_agent = 'openclaw'
- 计算 SHA256 存入 content_hash
- 幂等执行（重复执行不报错、不重复插入）
"""

import hashlib
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from qdrant_client import QdrantClient
from config.settings import load_config
from src.core.session_store import SessionStore


def adopt_orphans(db_path: str = "data/sessions.db", collection_name: str = "ai_memory") -> int:
    config = load_config()
    client = QdrantClient(
        url=config.qdrant.url,
        api_key=config.qdrant.api_key,
        timeout=config.qdrant.timeout or 10.0,
    )
    store = SessionStore(db_path=db_path)
    conn = store.get_connection()
    cursor = conn.cursor()

    # 获取当前 SQLite 已有的 qdrant_point_id 与 memory_id
    cursor.execute("SELECT memory_id, qdrant_point_id FROM memories")
    sqlite_rows = cursor.fetchall()
    existing_mids = set(r[0] for r in sqlite_rows)
    existing_pids = set(r[1] for r in sqlite_rows)

    print(f"[Adopt] 当前 SQLite memories 记录数: {len(existing_mids)}")

    # 从 Qdrant 全量 scroll 点位
    records = []
    offset = None
    while True:
        res, next_offset = client.scroll(
            collection_name=collection_name,
            limit=500,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        records.extend(res)
        if next_offset is None:
            break
        offset = next_offset

    print(f"[Adopt] Qdrant '{collection_name}' 集合总点位数: {len(records)}")

    # 找出孤儿点位 (既不在 point_id 也不在 memory_id)
    orphan_points = []
    for p in records:
        pid = str(p.id)
        mid = p.payload.get("memory_id") if p.payload else None
        if pid not in existing_pids and mid not in existing_mids:
            orphan_points.append(p)

    print(f"[Adopt] 待补登的孤儿点位数: {len(orphan_points)}")

    if not orphan_points:
        print("[Adopt] 无孤儿点位需要补登，系统已完全一致。")
        return 0

    inserted_count = 0
    # 按照 created_at 排序以便时间序稳定
    orphan_points.sort(key=lambda p: (p.payload or {}).get("created_at", 0))

    for p in orphan_points:
        payload = p.payload or {}
        pid = str(p.id)
        mid = payload.get("memory_id") or f"mem_{pid[:12]}"
        content = payload.get("content", "")
        c_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        
        m_type = payload.get("memory_type", "fact")
        subject = payload.get("subject", "system")
        predicate = payload.get("predicate", "states_fact")
        obj = payload.get("object", None)
        project_id = payload.get("project_id", "crypto-infrastructure")
        scope = payload.get("scope", "global")
        source_agent = payload.get("source_agent", "openclaw")
        status = payload.get("status", "active")
        confidence = float(payload.get("confidence", 0.8))
        importance = float(payload.get("importance", 0.5))
        mention_count = int(payload.get("mention_count", 1))
        created_at = int(payload.get("created_at") or 1790483490)
        updated_at = int(payload.get("updated_at") or created_at)

        cursor.execute(
            """
            INSERT OR IGNORE INTO memories (
                memory_id, qdrant_point_id, type, conflict_policy,
                subject, predicate, object, content,
                valid_from, valid_to, validity_type,
                confidence, importance, mention_count,
                status, superseded_by,
                project_id, scope, source_agent, version,
                content_hash, curation_batch_id, last_reconciled_at,
                deleted_at, deleted_by, deletion_reason,
                created_at, updated_at
            )
            VALUES (
                ?, ?, ?, 'coexist',
                ?, ?, ?, ?,
                ?, NULL, 'open_ended',
                ?, ?, ?,
                ?, NULL,
                ?, ?, ?, 1,
                ?, NULL, NULL,
                NULL, NULL, NULL,
                ?, ?
            )
            """,
            (
                mid, pid, m_type,
                subject, predicate, obj, content,
                created_at,
                confidence, importance, mention_count,
                status,
                project_id, scope, source_agent,
                c_hash,
                created_at, updated_at,
            ),
        )
        if cursor.rowcount > 0:
            inserted_count += 1

    conn.commit()

    # 补充：为库中任何尚缺 content_hash 的历史记录补齐哈希
    cursor.execute("SELECT memory_id, content FROM memories WHERE content_hash IS NULL")
    missing_hash_rows = cursor.fetchall()
    if missing_hash_rows:
        for mid, content in missing_hash_rows:
            ch = hashlib.sha256((content or "").encode("utf-8")).hexdigest()
            cursor.execute("UPDATE memories SET content_hash = ? WHERE memory_id = ?", (ch, mid))
        conn.commit()
        print(f"[Adopt] 已为 {len(missing_hash_rows)} 条旧记录补充生成 content_hash。")

    # 验证最终数量
    cursor.execute("SELECT count(*) FROM memories")
    final_count = cursor.fetchone()[0]
    print(f"[Adopt] 成功补登: {inserted_count} 条点位。当前 SQLite memories 总行数: {final_count}")
    return inserted_count


if __name__ == "__main__":
    adopt_orphans()

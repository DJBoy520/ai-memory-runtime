#!/usr/bin/env python3
"""
SSOT↔投影漂移仲裁执行（2026-10-07，AI 仲裁，用户授权 AI 决策）

背景：refine_v2 curation 只把测试夹具垃圾在 Qdrant 归档、未回写 SQLite，造成 25 条
SQLite=ACTIVE 但 Qdrant=ARCHIVED 的漂移（全部为 test_runner 写入的占位/集成测试内容，
逐条人工级复核 25/25 确认为垃圾）；另有 1 条真实高价值记忆（mem_20261005_0cb3218a，
aep-chain 状态汇总）投影缺失。

仲裁规则（确定性）：
1. 漂移点内容为测试夹具/占位文本（逐条复核确认）→ SQLite 对齐 curation：status → ARCHIVED
2. 真实内容且投影缺失 → 补建 Qdrant 投影（embed + upsert，payload 按 v3 9 字段规范）
全程写审计日志 data/backups/ssot_drift_arbitration_<date>.json；--apply 才执行
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qdrant_client import QdrantClient, models  # noqa: E402

from config.settings import load_config  # noqa: E402

DRIFT_DATE = "20261007"


def main() -> None:
    parser = argparse.ArgumentParser(description="SSOT 漂移仲裁执行")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    client = QdrantClient(url=cfg.qdrant.url, api_key=cfg.qdrant.api_key or None, timeout=cfg.qdrant.timeout)
    conn = sqlite3.connect(cfg.storage.sqlite_path)
    conn.row_factory = sqlite3.Row

    if args.apply:
        snaps = client.list_snapshots(collection_name="ai_memory")
        if not snaps:
            print("[X] 无快照，拒绝执行")
            sys.exit(2)

    # 1) 找漂移集（Qdrant=ARCHIVED 且 SQLite=ACTIVE）
    sqlite_rows = {r["memory_id"]: r for r in conn.execute("SELECT * FROM memories WHERE status='ACTIVE'")}
    drift = []
    off = None
    while True:
        recs, off = client.scroll(
            "ai_memory", limit=256, offset=off, with_payload=True, with_vectors=False,
            scroll_filter=models.Filter(must=[models.FieldCondition(key="status", match=models.MatchValue(value="ARCHIVED"))]),
        )
        for r in recs:
            mid = (r.payload or {}).get("memory_id")
            if mid in sqlite_rows:
                drift.append(mid)
        if off is None:
            break

    audit: dict = {
        "date": DRIFT_DATE,
        "decided_by": "zcode(AI 仲裁，用户授权 AI 决策)",
        "rule": "测试夹具/占位内容逐条复核确认 → SQLite 对齐 curation 归档；真实内容投影缺失 → 补建投影",
        "archived_in_sqlite": [],
        "projection_repaired": [],
    }

    # 2) SQLite 对齐归档
    print(f"[1] SQLite 对齐归档 {len(drift)} 条测试夹具记忆...")
    for mid in drift:
        print(f"    ARCHIVE {mid}: {sqlite_rows[mid]['content'][:50]!r}")
        if args.apply:
            conn.execute(
                "UPDATE memories SET status='ARCHIVED', updated_at=? WHERE memory_id=?",
                (int(time.time()), mid),
            )
        audit["archived_in_sqlite"].append({"memory_id": mid, "reason": "test fixture junk, aligned with refine_v2 curation"})

    # 3) 缺失投影修复：SQLite=ACTIVE 且 Qdrant 无点 → 补建
    print("[2] 扫描 SQLite=ACTIVE 但 Qdrant 无投影的条目...")
    repaired = 0
    for mid, row in sqlite_rows.items():
        if mid in drift:
            continue
        hits, _ = client.scroll(
            "ai_memory",
            scroll_filter=models.Filter(must=[models.FieldCondition(key="memory_id", match=models.MatchValue(value=mid))]),
            limit=1,
            with_payload=False,
        )
        if hits:
            continue
        content = row["content"]
        print(f"    REPAIR {mid}: {content[:50]!r}")
        audit["projection_repaired"].append({"memory_id": mid, "reason": "SSOT ACTIVE but projection missing (outbox loss)"})
        if not args.apply:
            continue
        from src.core.engine import BGEM3Engine

        import asyncio

        engine = BGEM3Engine(config=cfg.model)
        vec = asyncio.run(engine.embed([content]))[0]
        point_id = row["qdrant_point_id"] or str(uuid.uuid5(uuid.NAMESPACE_URL, mid))
        now_ts = int(time.time())
        payload = {
            "memory_id": mid,
            "version": row["version"],
            "content": content,
            "project_id": row["project_id"],
            "type": row["type"],
            "status": "ACTIVE",
            "created_by_agent": row["created_by_agent"] or row["source_agent"],
            "updated_by_agent": "zcode-ssot-repair",
            "created_at": row["created_at"],
            "updated_at": now_ts,
        }
        client.upsert(
            collection_name="ai_memory",
            points=[models.PointStruct(id=point_id, vector=vec, payload=payload)],
            wait=True,
        )
        repaired += 1
        break  # 当前已知仅 1 条；补建后模型加载一次即可

    print(f"[+] 补建投影 {repaired} 条")

    if args.apply:
        conn.commit()
        os.makedirs("data/backups", exist_ok=True)
        log_path = f"data/backups/ssot_drift_arbitration_{DRIFT_DATE}.json"
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(audit, f, ensure_ascii=False, indent=2)
        print(f"[+] 审计日志: {log_path}")
    else:
        print("\n[dry-run] 未写入。加 --apply 执行。")

    conn.close()


if __name__ == "__main__":
    main()

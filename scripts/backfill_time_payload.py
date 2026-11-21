#!/usr/bin/env python3
"""回填 ai_memory 点位 payload 的 created_at（时间检索三层修复 · 数据层，v2.2.0 配套）。

权威源：SQLite memories.created_at（epoch 秒，生产实测零缺失）。
目标：Qdrant ai_memory 集合中 payload 缺 created_at 的点位（2026-11-21 实测 712/1247）。

设计约束（多模型共识终版方案 §2）：
- 按 payload.memory_id 关联 SQLite，显式 point UUID 逐点 set_payload，仅新增 created_at 键，
  不触碰其他字段（merge-only，幂等可重跑）；
- memory_id 不在 SQLite 的点位视为孤儿投影，不硬补，计入报告交由对账引擎处置；
- 完成后可选创建 created_at integer payload index；
- 输出对账报告：backfilled / orphan / remaining / coverage。

用法（Unraid 生产容器内）：
  docker exec amr-daemon python3 /data/amr/scripts/backfill_time_payload.py \
      --sqlite /data/amr/sessions.db --create-index
也可在开发机直接运行（--sqlite 指向任意 dev 库）。
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

from qdrant_client import QdrantClient
from qdrant_client.http import models

COLLECTION = "ai_memory"
SCROLL_PAGE = 256


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill created_at payload on ai_memory points from SQLite")
    p.add_argument("--qdrant-url", default=os.environ.get("AMR_QDRANT_URL", "http://192.168.30.161:6333"))
    p.add_argument("--api-key", default=os.environ.get("AMR_QDRANT_API_KEY", ""))
    p.add_argument("--sqlite", required=True, help="Path to AMR SQLite (sessions.db)")
    p.add_argument("--collection", default=COLLECTION)
    p.add_argument("--create-index", action="store_true", help="Create integer payload index on created_at")
    p.add_argument("--dry-run", action="store_true", help="Only report, do not write")
    return p.parse_args()


def load_sqlite_created_at(sqlite_path: str) -> dict:
    conn = sqlite3.connect(sqlite_path)
    try:
        rows = conn.execute("SELECT memory_id, created_at FROM memories").fetchall()
    finally:
        conn.close()
    return {mid: ts for mid, ts in rows if mid and ts}


def iter_points_missing_created(client: QdrantClient, collection: str):
    offset = None
    while True:
        kwargs = {
            "collection_name": collection,
            "scroll_filter": models.Filter(must=[models.IsEmptyCondition(is_empty=models.PayloadField(key="created_at"))]),
            "limit": SCROLL_PAGE,
            "with_payload": True,
            "with_vectors": False,
        }
        if offset is not None:
            kwargs["offset"] = offset
        points, next_offset = client.scroll(**kwargs)
        for pt in points or []:
            yield pt
        if not next_offset:
            return
        offset = next_offset


def main() -> int:
    args = parse_args()
    client = QdrantClient(url=args.qdrant_url, api_key=args.api_key or None, timeout=60)
    created_map = load_sqlite_created_at(args.sqlite)
    print(f"[source] SQLite memories with created_at: {len(created_map)}")

    to_backfill: list[tuple[str, str, int]] = []  # (point_id, memory_id, created_at)
    orphans: list[tuple[str, str]] = []
    scanned = 0
    for pt in iter_points_missing_created(client, args.collection):
        scanned += 1
        payload = pt.payload or {}
        mid = payload.get("memory_id")
        ts = created_map.get(mid)
        if ts is None:
            orphans.append((str(pt.id), str(mid)))
        else:
            to_backfill.append((str(pt.id), str(mid), int(ts)))
    print(f"[scan] points missing created_at: {scanned}; resolvable: {len(to_backfill)}; orphan: {len(orphans)}")

    if args.dry_run:
        print("[dry-run] no writes performed")
        for pid, mid, _ in to_backfill[:10]:
            print(f"  would backfill {pid} ({mid})")
        for pid, mid in orphans[:10]:
            print(f"  orphan: {pid} ({mid})")
        return 0

    ok, failed = 0, 0
    # created_at 各点不同 → 逐点 set_payload（显式 ID；仅写 created_at 键，merge-only，幂等）
    for pid, mid, ts in to_backfill:
        try:
            client.set_payload(
                collection_name=args.collection,
                points=[pid],
                payload={"created_at": ts},
                wait=True,
            )
            ok += 1
        except Exception as e:
            failed += 1
            print(f"[error] {pid} ({mid}): {e}")
    print(f"[write] backfilled: {ok}; failed: {failed}")

    if args.create_index:
        client.create_payload_index(
            collection_name=args.collection,
            field_name="created_at",
            field_schema=models.PayloadSchemaType.INTEGER,
        )
        print("[index] created_at integer payload index created")

    remaining = sum(1 for _ in iter_points_missing_created(client, args.collection))
    total_pts = client.count(args.collection, exact=True).count
    print(
        "[report] " + json.dumps({
            "collection": args.collection,
            "total_points": total_pts,
            "scanned_missing": scanned,
            "backfilled": ok,
            "failed": failed,
            "orphans": len(orphans),
            "remaining_missing": remaining,
            "coverage_ok": remaining == 0,
        }, ensure_ascii=False)
    )
    if orphans:
        print("[orphans] memory_id not in SQLite (left untouched, reconcile separately):")
        for pid, mid in orphans[:20]:
            print(f"  {pid} {mid}")
    return 0 if remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

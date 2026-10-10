#!/usr/bin/env python3
"""
F1-4: ai_memory 集合状态值与 payload schema 一次性归一化迁移（AMR-RETRIEVAL-IMPLEMENTATION-PLAN.md）

变更内容：
1. status 统一大写映射（active→ACTIVE / archived→ARCHIVED / deleted→DELETED / historical→HISTORICAL /
   pending_verify→PENDING_VERIFY / conflict→CONFLICT / temporary→TEMPORARY）
2. legacy 点补齐 schema：type = type or memory_type；created_by_agent = created_by_agent or source_agent
3. created_at = created_at or updated_at；能按 memory_id 回表 SQLite memories.created_at 的以 SQLite 为准

安全机制：
- 默认 dry-run（只统计计划变更，不写任何数据）；--apply 才执行
- --apply 前置检查集合快照存在（无快照拒绝执行，除非 --force）
- 分批逐点 set_payload，只改 payload 不动向量；只读访问 SQLite

用法：
  python3 scripts/normalize_status_schema.py                # dry-run
  python3 scripts/normalize_status_schema.py --apply        # 执行迁移
"""

import argparse
import os
import sqlite3
import sys
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qdrant_client import QdrantClient, models  # noqa: E402

from config.settings import load_config  # noqa: E402

STATUS_MAP = {
    "active": "ACTIVE",
    "archived": "ARCHIVED",
    "deleted": "DELETED",
    "historical": "HISTORICAL",
    "superseded": "SUPERSEDED",
    "pending_verify": "PENDING_VERIFY",
    "conflict": "CONFLICT",
    "temporary": "TEMPORARY",
}

BATCH_SIZE = 100


def load_sqlite_created_at(sqlite_path: str) -> Dict[str, int]:
    """只读拉取 SQLite memories 表的 memory_id → created_at 映射（回填优先数据源）"""
    mapping: Dict[str, int] = {}
    if not os.path.exists(sqlite_path):
        print(f"[!] SQLite 文件不存在: {sqlite_path}（created_at 将回退用 updated_at）")
        return mapping
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT memory_id, created_at FROM memories")
        for mid, created_at in cur.fetchall():
            if mid and isinstance(created_at, int):
                mapping[mid] = created_at
    finally:
        conn.close()
    return mapping


def scroll_all_points(client: QdrantClient, collection: str) -> List[Tuple[str, Dict[str, Any]]]:
    """全量 scroll（带 payload，不带向量）"""
    points: List[Tuple[str, Dict[str, Any]]] = []
    offset = None
    while True:
        records, offset = client.scroll(
            collection_name=collection,
            limit=256,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        for rec in records:
            points.append((str(rec.id), rec.payload or {}))
        if offset is None:
            break
    return points


def plan_changes(
    points: List[Tuple[str, Dict[str, Any]]],
    sqlite_created: Dict[str, int],
) -> Tuple[Dict[str, Dict[str, Any]], Counter, int]:
    """计算每个点位的 payload 变更；返回 (point_id→changes, 状态分布, 无 status 点数)"""
    changes: Dict[str, Dict[str, Any]] = {}
    status_before: Counter = Counter()
    no_status = 0
    for pid, payload in points:
        status_val = payload.get("status")
        status_before[status_val if isinstance(status_val, str) else "<missing>"] += 1
        point_changes: Dict[str, Any] = {}

        if not isinstance(status_val, str) or not status_val:
            no_status += 1
        else:
            normalized = STATUS_MAP.get(status_val.lower())
            if normalized and status_val != normalized:
                point_changes["status"] = normalized

        if not payload.get("type") and payload.get("memory_type"):
            point_changes["type"] = payload["memory_type"]

        if not payload.get("created_by_agent") and payload.get("source_agent"):
            point_changes["created_by_agent"] = payload["source_agent"]

        if payload.get("created_at") is None:
            memory_id = payload.get("memory_id")
            fallback_created = sqlite_created.get(memory_id) if isinstance(memory_id, str) else None
            point_changes["created_at"] = fallback_created or payload.get("updated_at")

        if point_changes:
            changes[pid] = point_changes
    return changes, status_before, no_status


def apply_changes(client: QdrantClient, collection: str, changes: Dict[str, Dict[str, Any]]) -> None:
    """逐点 set_payload（wait=True），每 BATCH_SIZE 打一次进度"""
    done = 0
    items = list(changes.items())
    for pid, payload_changes in items:
        client.set_payload(
            collection_name=collection,
            payload=payload_changes,
            points=[pid],
            wait=True,
        )
        done += 1
        if done % BATCH_SIZE == 0:
            print(f"    进度 {done}/{len(items)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="ai_memory 状态/schema 归一化迁移（F1-4）")
    parser.add_argument("--collection", default="ai_memory")
    parser.add_argument("--apply", action="store_true", help="执行迁移（默认 dry-run）")
    parser.add_argument("--force", action="store_true", help="跳过快照存在性检查")
    args = parser.parse_args()

    cfg = load_config()
    client = QdrantClient(url=cfg.qdrant.url, api_key=cfg.qdrant.api_key or None, timeout=cfg.qdrant.timeout)

    if args.apply and not args.force:
        snapshots = client.list_snapshots(collection_name=args.collection)
        if not snapshots:
            print("[X] 拒绝执行：集合没有任何快照。请先 POST /collections/ai_memory/snapshots 或加 --force")
            sys.exit(2)
        print(f"[+] 快照检查通过：{len(snapshots)} 个快照存在")

    print(f"[*] 全量拉取 {args.collection} 点位...")
    points = scroll_all_points(client, args.collection)
    print(f"[+] 共 {len(points)} 点")

    sqlite_created = load_sqlite_created_at(cfg.storage.sqlite_path)
    print(f"[+] SQLite created_at 映射 {len(sqlite_created)} 条")

    changes, status_before, no_status = plan_changes(points, sqlite_created)
    print("\n===== 迁移前状态分布 =====")
    for k, v in status_before.most_common():
        print(f"  {k}: {v}")
    print(f"计划变更点数: {len(changes)} / {len(points)}（无 status 字段: {no_status}）")

    change_field_counter: Counter = Counter()
    for pc in changes.values():
        for field in pc:
            change_field_counter[field] += 1
    for field, cnt in change_field_counter.most_common():
        print(f"  字段 {field}: {cnt} 点")

    if not args.apply:
        print("\n[dry-run] 未写入任何数据。确认无误后加 --apply 执行。")
        return

    print(f"\n[*] 开始写入 {len(changes)} 点...")
    apply_changes(client, args.collection, changes)
    print("[+] 写入完成")

    # 迁移后复核：重新全量 scroll 统计
    points_after = scroll_all_points(client, args.collection)
    after: Counter = Counter()
    for _pid, payload in points_after:
        sv = payload.get("status")
        after[sv if isinstance(sv, str) else "<missing>"] += 1
    print("\n===== 迁移后状态分布 =====")
    for k, v in after.most_common():
        print(f"  {k}: {v}")

    # 一致性断言：每个旧状态值的数量应等于其映射目标 + 该目标原有数量
    expected: Counter = Counter()
    for old_val, cnt in status_before.items():
        if old_val == "<missing>":
            expected["<missing>"] += cnt
        else:
            expected[STATUS_MAP.get(old_val.lower(), old_val)] += cnt
    mismatch = {k: (expected[k], after.get(k, 0)) for k in expected if expected[k] != after.get(k, 0)}
    if mismatch:
        print(f"\n[X] 迁移后计数不一致（期望, 实际）: {mismatch}")
        sys.exit(1)
    print("\n[+] 逐状态计数映射一致，迁移校验通过")


if __name__ == "__main__":
    main()

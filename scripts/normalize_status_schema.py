#!/usr/bin/env python3
"""
F1-4: ai_memory 集合状态值与 payload schema 归一化迁移（AMR-RETRIEVAL-IMPLEMENTATION-PLAN.md / RFC v5.3）

变更内容：
1. status 统一大写标准映射（active→ACTIVE / archived→HISTORICAL / superseded→HISTORICAL / deleted→DELETED /
   historical→HISTORICAL / pending_verify→PENDING_VERIFY / conflict→CONFLICT / temporary→TEMPORARY）
   严格与 src.service.memory_status 对齐，禁止将 HISTORICAL 映射为 SUPERSEDED/ARCHIVED。
2. legacy 点补齐 schema：type = type or memory_type；created_by_agent = created_by_agent or source_agent
3. created_at = created_at or updated_at；能按 memory_id 回表 SQLite memories.created_at 的以 SQLite 为准

安全机制：
- 默认 dry-run（只统计计划变更，不写任何数据）；--apply 才执行
- --inventory: 只读统计（各状态计数、将变更计数、歧义清单）
- --rehearse: 用 sqlite3 backup API 复制到临时副本演练全流程迁移，绝不改源库，输出前后对账 JSON；
  顺序说明：先 outbox/分类迁移，再状态枚举迁移。
- --apply 前置强制备份 SQLite 副本，并检查集合快照存在（无快照拒绝执行，除非 --force）
"""

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qdrant_client import QdrantClient, models  # noqa: E402

from config.settings import load_config  # noqa: E402
from src.core.session_store import SessionStore  # noqa: E402
from src.service.memory_status import CANONICAL_STATUSES, normalize_status  # noqa: E402

STATUS_MAP = {
    "active": "ACTIVE",
    "archived": "HISTORICAL",
    "deleted": "DELETED",
    "historical": "HISTORICAL",
    "superseded": "HISTORICAL",
    "pending_verify": "PENDING_VERIFY",
    "conflict": "CONFLICT",
    "temporary": "TEMPORARY",
}

BATCH_SIZE = 100


def get_normalized_status(val: Optional[str]) -> str:
    """使用统一标准规范化状态枚举，保证与 memory_status.normalize_status 严格对齐"""
    return normalize_status(val)


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
            normalized = get_normalized_status(status_val)
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


def run_inventory(sqlite_path: str, client: Optional[QdrantClient] = None, collection: str = "ai_memory") -> Dict[str, Any]:
    """只读统计：各状态计数、将变更计数、歧义清单"""
    print(f"[*] 执行只读状态盘点 (Inventory)...")
    res: Dict[str, Any] = {
        "sqlite_path": sqlite_path,
        "sqlite_status_counts": {},
        "sqlite_will_change_count": 0,
        "sqlite_ambiguities": [],
        "qdrant_status_counts": {},
        "qdrant_will_change_count": 0,
        "qdrant_ambiguities": [],
    }

    # 1. SQLite 盘点
    if os.path.exists(sqlite_path):
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        try:
            cur = conn.cursor()
            cur.execute("SELECT memory_id, status FROM memories")
            rows = cur.fetchall()
            status_counter: Counter = Counter()
            will_change = 0
            ambiguities = []
            for mid, st in rows:
                raw_st = str(st) if st is not None else "<null>"
                status_counter[raw_st] += 1
                norm = get_normalized_status(st)
                if st is None or st == "" or norm not in CANONICAL_STATUSES:
                    ambiguities.append({"memory_id": mid, "raw_status": raw_st, "normalized": norm})
                if st != norm:
                    will_change += 1
            res["sqlite_total"] = len(rows)
            res["sqlite_status_counts"] = dict(status_counter.most_common())
            res["sqlite_will_change_count"] = will_change
            res["sqlite_ambiguities"] = ambiguities
        except Exception as e:
            res["sqlite_error"] = str(e)
        finally:
            conn.close()
    else:
        res["sqlite_error"] = "File not found"

    # 2. Qdrant 盘点 (若连通)
    if client is not None:
        try:
            points = scroll_all_points(client, collection)
            q_counter: Counter = Counter()
            q_will_change = 0
            q_ambiguities = []
            for pid, payload in points:
                st = payload.get("status")
                raw_st = str(st) if st is not None else "<missing>"
                q_counter[raw_st] += 1
                norm = get_normalized_status(st)
                if not isinstance(st, str) or not st or norm not in CANONICAL_STATUSES:
                    q_ambiguities.append({"point_id": pid, "raw_status": raw_st, "normalized": norm})
                if st != norm:
                    q_will_change += 1
            res["qdrant_total"] = len(points)
            res["qdrant_status_counts"] = dict(q_counter.most_common())
            res["qdrant_will_change_count"] = q_will_change
            res["qdrant_ambiguities"] = q_ambiguities
        except Exception as e:
            res["qdrant_error"] = str(e)

    print("\n===== SQLite 状态分布 =====")
    for k, v in res.get("sqlite_status_counts", {}).items():
        print(f"  {k}: {v}")
    print(f"SQLite 将变更行数: {res.get('sqlite_will_change_count', 0)} / {res.get('sqlite_total', 0)}")
    print(f"SQLite 歧义状态行数: {len(res.get('sqlite_ambiguities', []))}")

    if "qdrant_total" in res:
        print("\n===== Qdrant 状态分布 =====")
        for k, v in res.get("qdrant_status_counts", {}).items():
            print(f"  {k}: {v}")
        print(f"Qdrant 将变更点数: {res.get('qdrant_will_change_count', 0)} / {res.get('qdrant_total', 0)}")
        print(f"Qdrant 歧义状态点数: {len(res.get('qdrant_ambiguities', []))}")

    return res


def run_rehearse(sqlite_path: str) -> Dict[str, Any]:
    """
    用 sqlite3 backup API 复制到临时副本演练迁移，输出前后对账 JSON；绝不改源库。
    顺序说明：先 outbox/分类迁移，再状态枚举迁移。
    """
    print("[*] 启动迁移演练 (Rehearse)...")
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"Source database not found: {sqlite_path}")

    # 1. 创建临时副本并用 sqlite3 backup API 复制
    tmp_fd, tmp_db = tempfile.mkstemp(prefix="amr_rehearse_", suffix=".db")
    os.close(tmp_fd)

    src_conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    dst_conn = sqlite3.connect(tmp_db)
    try:
        src_conn.backup(dst_conn, pages=200)
    finally:
        src_conn.close()

    print(f"[+] 已安全复制到临时副本: {tmp_db} (源库保持只读未触碰)")

    rehearsal_report: Dict[str, Any] = {
        "status": "rehearsal_completed",
        "order_explanation": "先 outbox/分类迁移，再状态枚举迁移",
        "source_db": sqlite_path,
        "replica_db": tmp_db,
        "source_untouched": True,
    }

    try:
        # 统计迁移前分布
        cur = dst_conn.cursor()
        cur.execute("SELECT status, count(*) FROM memories GROUP BY status")
        before_counts = {str(r[0]): r[1] for r in cur.fetchall()}
        rehearsal_report["before_inventory"] = before_counts

        # 阶段 1: 触发 schema 幂等迁移 (先 outbox/分类迁移)
        print("[1/2 演练阶段] 执行 outbox 重建与存量分类回填迁移 (v5_outbox_rebuild & v5_projection_class_backfill)...")
        # 实例化 SessionStore 会自动触发 _migrate_schema
        store_replica = SessionStore(db_path=tmp_db)
        rehearsal_report["schema_migration"] = "applied_on_replica"

        # 阶段 2: 状态枚举迁移
        print("[2/2 演练阶段] 执行状态枚举大写与规范映射迁移...")
        cur.execute("SELECT memory_id, status FROM memories")
        rows = cur.fetchall()
        migrated_records = []
        for mid, st in rows:
            norm = get_normalized_status(st)
            if norm != st:
                cur.execute("UPDATE memories SET status = ? WHERE memory_id = ?", (norm, mid))
                migrated_records.append({"memory_id": mid, "from": st, "to": norm})
        dst_conn.commit()

        # 统计迁移后分布
        cur.execute("SELECT status, count(*) FROM memories GROUP BY status")
        after_counts = {str(r[0]): r[1] for r in cur.fetchall()}
        rehearsal_report["after_inventory"] = after_counts
        rehearsal_report["total_memories"] = len(rows)
        rehearsal_report["modified_count"] = len(migrated_records)
        rehearsal_report["sample_changes"] = migrated_records[:10]

    finally:
        dst_conn.close()
        try:
            os.remove(tmp_db)
        except OSError:
            pass

    print("\n===== 演练前后对账报告 (Reconciliation JSON) =====")
    report_json = json.dumps(rehearsal_report, ensure_ascii=False, indent=2)
    print(report_json)
    return rehearsal_report


def backup_database(src_path: str) -> str:
    """迁移前使用 sqlite3 backup API 备份源数据库"""
    backup_file = f"{src_path}.bak.{int(time.time())}"
    src_conn = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
    dst_conn = sqlite3.connect(backup_file)
    try:
        src_conn.backup(dst_conn, pages=200)
    finally:
        src_conn.close()
        dst_conn.close()
    print(f"[+] 迁移前强制备份已完成: {backup_file}")
    return backup_file


def main() -> None:
    parser = argparse.ArgumentParser(description="ai_memory 状态/schema 归一化迁移（F1-4）")
    parser.add_argument("--collection", default="ai_memory")
    parser.add_argument("--inventory", action="store_true", help="只读统计各状态计数、将变更计数、歧义清单")
    parser.add_argument("--rehearse", action="store_true", help="在临时副本上安全演练迁移全流程并输出前后对账 JSON")
    parser.add_argument("--apply", action="store_true", help="执行迁移（默认 dry-run）")
    parser.add_argument("--force", action="store_true", help="跳过快照存在性检查")
    args = parser.parse_args()

    cfg = load_config()

    client: Optional[QdrantClient] = None
    try:
        client = QdrantClient(url=cfg.qdrant.url, api_key=cfg.qdrant.api_key or None, timeout=cfg.qdrant.timeout)
    except Exception:
        client = None

    if args.inventory:
        run_inventory(cfg.storage.sqlite_path, client, args.collection)
        return

    if args.rehearse:
        run_rehearse(cfg.storage.sqlite_path)
        return

    # 默认/Apply 模式
    if args.apply:
        backup_database(cfg.storage.sqlite_path)

        if client is not None and not args.force:
            snapshots = client.list_snapshots(collection_name=args.collection)
            if not snapshots:
                print("[X] 拒绝执行：集合没有任何快照。请先 POST /collections/ai_memory/snapshots 或加 --force")
                sys.exit(2)
            print(f"[+] 快照检查通过：{len(snapshots)} 个快照存在")

    print(f"[*] 全量拉取 {args.collection} 点位...")
    points = scroll_all_points(client, args.collection) if client else []
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

    # Apply 写入 SQLite 状态
    conn = sqlite3.connect(cfg.storage.sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT memory_id, status FROM memories")
        for mid, st in cur.fetchall():
            norm = get_normalized_status(st)
            if norm != st:
                cur.execute("UPDATE memories SET status = ? WHERE memory_id = ?", (norm, mid))
        conn.commit()
        print(f"[+] SQLite 状态枚举归一化完成")
    finally:
        conn.close()

    # Apply 写入 Qdrant payload
    if client and changes:
        print(f"\n[*] 开始写入 Qdrant {len(changes)} 点...")
        apply_changes(client, args.collection, changes)
        print("[+] Qdrant 写入完成")


if __name__ == "__main__":
    main()

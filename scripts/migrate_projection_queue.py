#!/usr/bin/env python3
"""
AI Memory Runtime - 投影队列迁移与预演脚本 (Phase A: Projection Queue Migration & Rehearsal)
功能：
1. --inventory: 对指定 SQLite 库进行资产盘点（旧/新队列按状态/op_type 计数、memories 分类计数、schema 状态、生产者清单与 legacy 例外）；
2. --apply: 调用 SessionStore 触发统一幂等迁移 (_migrate_schema)，输出前后对账 JSON；
3. --rehearse: 利用 sqlite3 backup API 备份源库到临时副本，在副本上执行盘点+迁移+对账，绝不修改源库；
4. --report PATH: 指定输出报告文件路径；
5. 若存在 quarantine 隔离任务，输出“必须人工确认后才可上线”并以非 0 退出码退出。
遵循 RFC v5 §A3, V5.1 §3, V5.2 §3, V5.3 §4 规范。
"""

import argparse
import json
import logging
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from typing import Any, Dict, Optional

# 允许从项目根目录直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import load_config
from src.core.errors import MigrationBlocked
from src.core.session_store import SessionStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("migrate_projection_queue")


def get_inventory(db_path: Path) -> Dict[str, Any]:
    """对目标 SQLite 数据库进行只读盘点"""
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    inventory: Dict[str, Any] = {
        "db_path": str(db_path.resolve()),
        "inspected_at": int(time.time()),
        "schema_status": {},
        "old_queue": {},
        "new_queue": {},
        "memories_classification": {},
        "quarantine": {
            "exists": False,
            "count": 0,
            "tasks": [],
        },
        "producers": {
            "v3_producers": [
                "src/service/memory_service.py: memory_create (统一入队 helper)",
                "src/service/memory_service.py: memory_update (统一入队 helper)",
                "src/service/memory_service.py: memory_update_status (统一入队 helper)",
                "src/service/memory_service.py: memory_delete (统一入队 helper)",
                "src/reconciliation/engine.py: reconcile_status (统一入队 helper)",
            ],
            "legacy_exceptions": [
                "src/service/memory_service.py: memory_record (直写 Qdrant，point 归 legacy 队列管理，无 v3 生命周期)",
            ],
            "scope_boundary": "单写者口径仅覆盖 v3 memories 的 Qdrant 投影；单机单 Worker，多实例不在当前范围",
        },
    }

    try:
        # 1. 检查 schema_status
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row["name"] for row in cursor.fetchall()}

        # 检查 memories 列
        memories_cols = set()
        if "memories" in tables:
            cursor.execute("PRAGMA table_info(memories);")
            memories_cols = {row["name"] for row in cursor.fetchall()}

        inventory["schema_status"]["memories"] = {
            "table_exists": "memories" in tables,
            "has_projection_revision": "projection_revision" in memories_cols,
            "has_collection_name": "collection_name" in memories_cols,
            "has_projection_class": "projection_class" in memories_cols,
        }

        # 检查 memory_projection_outbox 列与状态
        outbox_cols = set()
        outbox_schema_type = "missing"
        if "memory_projection_outbox" in tables:
            cursor.execute("PRAGMA table_info(memory_projection_outbox);")
            outbox_cols = {row["name"] for row in cursor.fetchall()}
            if "target_collection" in outbox_cols and "version" not in outbox_cols:
                outbox_schema_type = "v5_new"
            else:
                outbox_schema_type = "legacy_old"

        inventory["schema_status"]["memory_projection_outbox"] = {
            "table_exists": "memory_projection_outbox" in tables,
            "schema_type": outbox_schema_type,
            "columns": sorted(list(outbox_cols)),
        }

        # 检查 _amr_migrations
        applied_migrations = []
        if "_amr_migrations" in tables:
            cursor.execute("SELECT migration_id, applied_at, details FROM _amr_migrations;")
            applied_migrations = [dict(r) for r in cursor.fetchall()]

        inventory["schema_status"]["migrations"] = {
            "table_exists": "_amr_migrations" in tables,
            "applied": applied_migrations,
        }

        # 检查隔离表
        if "memory_projection_outbox_quarantine" in tables:
            cursor.execute("SELECT COUNT(*) FROM memory_projection_outbox_quarantine;")
            q_cnt = cursor.fetchone()[0]
            cursor.execute("SELECT id, memory_id, reason, observed_at FROM memory_projection_outbox_quarantine LIMIT 50;")
            q_tasks = [dict(r) for r in cursor.fetchall()]
            inventory["quarantine"] = {
                "exists": True,
                "count": q_cnt,
                "tasks": q_tasks,
            }

        # 2. 盘点旧队列 qdrant_sync_queue
        if "qdrant_sync_queue" in tables:
            cursor.execute("SELECT COUNT(*) FROM qdrant_sync_queue;")
            total_old = cursor.fetchone()[0]

            cursor.execute("SELECT status, COUNT(*) as cnt FROM qdrant_sync_queue GROUP BY status;")
            by_status = {row["status"]: row["cnt"] for row in cursor.fetchall()}

            cursor.execute("SELECT op_type, COUNT(*) as cnt FROM qdrant_sync_queue GROUP BY op_type;")
            by_op_type = {row["op_type"]: row["cnt"] for row in cursor.fetchall()}

            # 统计旧队列中关联到 v3 行的任务数
            v3_in_old = 0
            if "memories" in tables and "projection_class" in memories_cols:
                cursor.execute("""
                    SELECT COUNT(*) FROM qdrant_sync_queue q
                    JOIN memories m ON q.memory_id = m.memory_id
                    WHERE m.projection_class = 'v3';
                """)
                v3_in_old = cursor.fetchone()[0]

            inventory["old_queue"] = {
                "exists": True,
                "total": total_old,
                "by_status": by_status,
                "by_op_type": by_op_type,
                "v3_tasks_count": v3_in_old,
            }
        else:
            inventory["old_queue"] = {"exists": False, "total": 0}

        # 3. 盘点新队列 memory_projection_outbox
        if "memory_projection_outbox" in tables and outbox_schema_type == "v5_new":
            cursor.execute("SELECT COUNT(*) FROM memory_projection_outbox;")
            total_new = cursor.fetchone()[0]

            cursor.execute("SELECT status, COUNT(*) as cnt FROM memory_projection_outbox GROUP BY status;")
            by_status_new = {row["status"]: row["cnt"] for row in cursor.fetchall()}

            inventory["new_queue"] = {
                "exists": True,
                "total": total_new,
                "by_status": by_status_new,
            }
        else:
            inventory["new_queue"] = {
                "exists": "memory_projection_outbox" in tables,
                "schema_type": outbox_schema_type,
                "total": 0,
            }

        # 4. 盘点 memories 分类
        if "memories" in tables:
            cursor.execute("SELECT COUNT(*) FROM memories;")
            total_mem = cursor.fetchone()[0]

            if "projection_class" in memories_cols:
                cursor.execute("SELECT projection_class, COUNT(*) as cnt FROM memories GROUP BY projection_class;")
                by_class = {row["projection_class"]: row["cnt"] for row in cursor.fetchall()}
            else:
                by_class = {"unclassified": total_mem}

            inventory["memories_classification"] = {
                "total": total_mem,
                "by_projection_class": by_class,
            }
        else:
            inventory["memories_classification"] = {"total": 0}

    finally:
        conn.close()

    return inventory


def run_inventory_action(db_path: Path, report_path: Optional[Path] = None) -> int:
    """执行只读资产盘点"""
    inventory = get_inventory(db_path)
    output_json = json.dumps(inventory, ensure_ascii=False, indent=2)
    print(output_json)

    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(output_json, encoding="utf-8")
        logger.info("Inventory report written to %s", report_path)

    q_count = inventory.get("quarantine", {}).get("count", 0)
    if q_count > 0:
        print(f"\n[ALERT] 隔离区存在 {q_count} 条任务，必须人工确认后才可上线！", file=sys.stderr)
        return 1

    return 0


def run_apply_action(db_path: Path, report_path: Optional[Path] = None) -> int:
    """对目标库执行迁移"""
    logger.info("Starting migration apply on %s...", db_path)
    before_inventory = get_inventory(db_path)

    blocked_err = None
    try:
        # 触发 SessionStore 初始化的增量幂等迁移流程
        store = SessionStore(db_path=db_path)
        store.close()
    except MigrationBlocked as mb:
        blocked_err = mb
        logger.error("Migration blocked by quarantine tasks: %s", mb)
    except Exception as e:
        logger.error("Unexpected error during migration apply: %s", e)
        raise

    after_inventory = get_inventory(db_path)
    q_count = after_inventory.get("quarantine", {}).get("count", 0)

    report = {
        "action": "apply",
        "status": "blocked" if (blocked_err or q_count > 0) else "completed",
        "before": before_inventory,
        "after": after_inventory,
        "quarantine_count": q_count,
        "timestamp": int(time.time()),
    }

    output_json = json.dumps(report, ensure_ascii=False, indent=2)
    print(output_json)

    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(output_json, encoding="utf-8")
        logger.info("Apply reconciliation report written to %s", report_path)

    if blocked_err or q_count > 0:
        print(f"\n[ALERT] 迁移阻断或隔离区存在 {q_count} 条任务，必须人工确认后才可上线！", file=sys.stderr)
        return 1

    logger.info("Migration apply completed successfully.")
    return 0


def run_rehearse_action(db_path: Path, report_path: Optional[Path] = None) -> int:
    """使用 sqlite3 backup API 将源库复制到临时副本，在副本上执行演练与对账，绝不修改源库"""
    if not db_path.exists():
        raise FileNotFoundError(f"Source database not found: {db_path}")

    logger.info("Starting migration rehearsal from %s...", db_path)
    temp_dir = tempfile.mkdtemp(prefix="amr_rehearse_")
    replica_path = Path(temp_dir) / "rehearsal_replica.db"

    # 1. 使用 sqlite3 在线 backup API 创建完整副本
    src_conn = sqlite3.connect(str(db_path))
    dst_conn = sqlite3.connect(str(replica_path))
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()

    logger.info("Created backup replica at %s using sqlite3 backup API.", replica_path)

    before_inventory = get_inventory(replica_path)
    blocked_err = None

    try:
        # 2. 在副本上跑迁移
        store = SessionStore(db_path=replica_path)
        store.close()
    except MigrationBlocked as mb:
        blocked_err = mb
        logger.warning("Rehearsal migration encountered quarantine: %s", mb)
    except Exception as e:
        logger.error("Error during rehearsal migration: %s", e)
        raise

    after_inventory = get_inventory(replica_path)
    q_count = after_inventory.get("quarantine", {}).get("count", 0)

    report = {
        "action": "rehearse",
        "status": "blocked" if (blocked_err or q_count > 0) else "completed",
        "source_db": str(db_path.resolve()),
        "replica_db": str(replica_path.resolve()),
        "before": before_inventory,
        "after": after_inventory,
        "quarantine_count": q_count,
        "timestamp": int(time.time()),
    }

    output_json = json.dumps(report, ensure_ascii=False, indent=2)
    print(output_json)

    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(output_json, encoding="utf-8")
        logger.info("Rehearsal report written to %s", report_path)

    if blocked_err or q_count > 0:
        print(f"\n[ALERT] 预演发现隔离任务 (count={q_count})，必须人工确认后才可上线！", file=sys.stderr)
        return 1

    logger.info("Rehearsal completed successfully without modifying source database.")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="AMR Phase A 投影队列迁移与预演工具 (scripts/migrate_projection_queue.py)"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="目标 SQLite 数据库路径 (默认取 config.storage.sqlite_path)",
    )
    parser.add_argument(
        "--inventory",
        action="store_true",
        help="只读盘点旧/新队列、memories 分类、schema 与生产者清单",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="对指定数据库触发统一模式迁移并输出前后对账",
    )
    parser.add_argument(
        "--rehearse",
        action="store_true",
        help="通过 sqlite3 backup API 备份源库到临时副本并进行无害迁移预演",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="对账报告 JSON 输出文件路径",
    )

    args = parser.parse_args()

    # 确定目标数据库路径
    if args.db:
        target_db = args.db
    else:
        cfg = load_config()
        target_db = Path(cfg.storage.sqlite_path)

    if not any([args.inventory, args.apply, args.rehearse]):
        parser.print_help()
        sys.exit(0)

    exit_code = 0
    if args.inventory:
        code = run_inventory_action(target_db, report_path=args.report)
        exit_code = exit_code or code

    if args.rehearse:
        code = run_rehearse_action(target_db, report_path=args.report)
        exit_code = exit_code or code

    if args.apply:
        code = run_apply_action(target_db, report_path=args.report)
        exit_code = exit_code or code

    sys.exit(exit_code)


if __name__ == "__main__":
    main()

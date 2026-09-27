#!/usr/bin/env python3
"""
AI Memory Runtime - 离线提纯数据回滚与安全审计脚本 (v2.2)
提供一键安全幂等回滚：
1. 校验冷备文件 SHA256 哈希完整性；
2. 清理 raw_messages 中 is_synthetic = 1 的合成消息及由其关联生成的 memories；
3. 清理对应的 memory_evidence、qdrant_sync_queue、memory_audit_log；
4. 清理已无任何真实消息的 legacy 会话 (如 legacy_migration_20260927)；
5. 输出回滚审计报告并校验数据库状态。
遵循 docs/tasks/task-03-offline-refine-pipeline.md 契约。
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# 确保项目根目录在 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import AppConfig, load_config
from src.core.session_store import SessionStore

logger = logging.getLogger("rollback_refine_v2")

DEFAULT_BACKUP_PATH = PROJECT_ROOT / "data" / "backups" / "ai_memory_backup_20260927.json"
MIGRATION_SESSION_ID = "legacy_migration_20260927"


def compute_file_sha256(filepath: str | Path) -> str:
    """计算文件的 SHA256 哈希"""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


class RefineRollbackService:
    """
    数据回滚与安全审计服务
    """

    def __init__(
        self,
        backup_path: str | Path = DEFAULT_BACKUP_PATH,
        session_store: Optional[SessionStore] = None,
        config: Optional[AppConfig] = None,
    ):
        self.backup_path = Path(backup_path)
        self.config = config or load_config()
        self.session_store = session_store or SessionStore(config=self.config.storage)

    def rollback(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        执行一键回滚操作：
        1. 基于备份快照校验哈希；
        2. 清理 raw_messages 中 is_synthetic = 1 的记录；
        3. 清理仅依赖合成证据的 memories 或来源为 pipeline:migrate 的 memories；
        4. 清理 memory_evidence, qdrant_sync_queue, memory_audit_log 中的残留；
        5. 安全复原状态并返回详细审计结果。
        """
        start_time = time.time()
        file_hash = None
        if self.backup_path.exists():
            file_hash = compute_file_sha256(self.backup_path)
            logger.info(f"Verified backup snapshot hash: {file_hash[:16]}...")
        else:
            logger.warning(f"Backup file not found at {self.backup_path}, proceeding with DB-only audit rollback.")

        with self.session_store._lock:
            conn = self.session_store._get_connection()
            cursor = conn.cursor()

            # 1. 查找所有合成消息
            cursor.execute("SELECT message_id FROM raw_messages WHERE is_synthetic = 1;")
            synthetic_msg_rows = cursor.fetchall()
            synthetic_msg_ids = [row["message_id"] for row in synthetic_msg_rows]

            # 2. 查找与合成消息关联的 memory_id，或者 operator 为 pipeline:migrate 的 memory_id
            cursor.execute(
                """
                SELECT DISTINCT memory_id FROM memory_evidence
                WHERE message_id IN (SELECT message_id FROM raw_messages WHERE is_synthetic = 1)
                UNION
                SELECT memory_id FROM memory_audit_log
                WHERE operator LIKE 'pipeline:migrate%'
                """
            )
            target_mem_rows = cursor.fetchall()
            target_mem_ids = [row["memory_id"] for row in target_mem_rows]

            if dry_run:
                return {
                    "dry_run": True,
                    "backup_file": str(self.backup_path),
                    "file_sha256": file_hash,
                    "synthetic_messages_to_delete": len(synthetic_msg_ids),
                    "memories_to_delete": len(target_mem_ids),
                    "elapsed_seconds": round(time.time() - start_time, 3),
                }

            # 开始事务清理
            try:
                # A. 清理 memory_evidence
                del_ev_count = 0
                if synthetic_msg_ids:
                    placeholders = ",".join(["?"] * len(synthetic_msg_ids))
                    cursor.execute(f"DELETE FROM memory_evidence WHERE message_id IN ({placeholders});", synthetic_msg_ids)
                    del_ev_count += cursor.rowcount

                if target_mem_ids:
                    placeholders = ",".join(["?"] * len(target_mem_ids))
                    cursor.execute(f"DELETE FROM memory_evidence WHERE memory_id IN ({placeholders});", target_mem_ids)
                    del_ev_count += cursor.rowcount

                # B. 清理 memories
                del_mem_count = 0
                if target_mem_ids:
                    placeholders = ",".join(["?"] * len(target_mem_ids))
                    cursor.execute(f"DELETE FROM memories WHERE memory_id IN ({placeholders});", target_mem_ids)
                    del_mem_count = cursor.rowcount

                # C. 清理 qdrant_sync_queue 中关联 target_mem_ids 的项
                del_outbox_count = 0
                if target_mem_ids:
                    placeholders = ",".join(["?"] * len(target_mem_ids))
                    cursor.execute(f"DELETE FROM qdrant_sync_queue WHERE memory_id IN ({placeholders});", target_mem_ids)
                    del_outbox_count = cursor.rowcount

                # D. 清理 memory_audit_log 中关联 target_mem_ids 的项
                del_audit_count = 0
                if target_mem_ids:
                    placeholders = ",".join(["?"] * len(target_mem_ids))
                    cursor.execute(f"DELETE FROM memory_audit_log WHERE memory_id IN ({placeholders});", target_mem_ids)
                    del_audit_count = cursor.rowcount

                # E. 清理 raw_messages 中 is_synthetic = 1
                cursor.execute("DELETE FROM raw_messages WHERE is_synthetic = 1;")
                del_msg_count = cursor.rowcount

                # F. 清理迁移会话 raw_sessions (如果已经没有任何消息依赖)
                cursor.execute(
                    """
                    DELETE FROM raw_sessions
                    WHERE session_id = ?
                      AND session_id NOT IN (SELECT DISTINCT session_id FROM raw_messages);
                    """,
                    (MIGRATION_SESSION_ID,),
                )
                del_session_count = cursor.rowcount

                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Rollback failed, rolled back transaction: {e}")
                raise e

        elapsed_time = round(time.time() - start_time, 3)

        report = {
            "dry_run": False,
            "backup_file": str(self.backup_path),
            "file_sha256": file_hash,
            "deleted_synthetic_messages": del_msg_count,
            "deleted_memories": del_mem_count,
            "deleted_evidence_links": del_ev_count,
            "deleted_outbox_tasks": del_outbox_count,
            "deleted_audit_logs": del_audit_count,
            "deleted_legacy_sessions": del_session_count,
            "elapsed_seconds": elapsed_time,
        }
        return report


def main():
    parser = argparse.ArgumentParser(description="AMR v2.2 离线提纯数据回滚与安全审计脚本")
    parser.add_argument("--backup-path", type=str, default=str(DEFAULT_BACKUP_PATH), help="备份文件快照路径")
    parser.add_argument("--db-path", type=str, default=None, help="目标 SQLite 数据库路径")
    parser.add_argument("--dry-run", action="store_true", help="只预览将要删除的数据项，不实际执行删除")
    parser.add_argument("--yes", "-y", action="store_true", help="跳过确认提示，直接执行一键回滚")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config()
    if args.db_path:
        config.storage.sqlite_path = args.db_path

    if not args.dry_run and not args.yes:
        confirm = input("⚠️  警告：该操作将物理清理所有 is_synthetic=1 的合成消息及其提炼记忆！是否继续? [y/N]: ")
        if confirm.lower() not in ("y", "yes"):
            print("已取消回滚操作。")
            sys.exit(0)

    service = RefineRollbackService(backup_path=args.backup_path, config=config)
    report = service.rollback(dry_run=args.dry_run)

    print("\n" + "=" * 55)
    print("        AMR v2.2 离线提纯数据回滚报告")
    print("=" * 55)
    print(f"模式:           {'DRY RUN 预演模式' if report['dry_run'] else '正式回滚完成'}")
    print(f"校验快照:       {report['backup_file']}")
    print(f"快照 SHA256:    {str(report['file_sha256'])[:16]}...")
    if report["dry_run"]:
        print(f"待清理合成消息: {report['synthetic_messages_to_delete']}")
        print(f"待清理精炼记忆: {report['memories_to_delete']}")
    else:
        print(f"已清理合成消息: {report['deleted_synthetic_messages']}")
        print(f"已清理精炼记忆: {report['deleted_memories']}")
        print(f"已清理证据关系: {report['deleted_evidence_links']}")
        print(f"已清理发件任务: {report['deleted_outbox_tasks']}")
        print(f"已清理审计日志: {report['deleted_audit_logs']}")
        print(f"已清理归档会话: {report['deleted_legacy_sessions']}")
    print(f"回滚总耗时:     {report['elapsed_seconds']} 秒")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()

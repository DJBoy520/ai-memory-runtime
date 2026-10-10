#!/usr/bin/env python3
"""
scripts/daily_memory_reconciliation.py
AMR 每日记忆对账与生命周期治理 CLI 入口包装器
支持 --dry-run, --batch-id 参数
"""

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.reconciliation.engine import ReconciliationEngine


def main():
    parser = argparse.ArgumentParser(description="AMR 每日记忆对账与生命周期治理驱动程序 (RFC-003)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="以只读模式运行，仅生成 curation_candidates 报告，不修改 memories.status 且不写 Outbox",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="指定自定义对账批次 ID (默认自动根据当前时间生成)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/sessions.db",
        help="SQLite 数据库文件路径 (默认: data/sessions.db)",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default="ai_memory",
        help="目标 Qdrant 检索集合名称 (默认: ai_memory)",
    )

    args = parser.parse_args()

    engine = ReconciliationEngine(
        dry_run=args.dry_run,
        batch_id=args.batch_id,
        db_path=args.db_path,
        collection_name=args.collection,
    )

    result = engine.run()
    print("\n--- 对账执行摘要 (JSON) ---")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

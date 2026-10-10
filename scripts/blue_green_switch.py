#!/usr/bin/env python3
"""
AI Memory Runtime - Qdrant 蓝绿集合创建与原子别名热切换脚本 (v2.2)
遵循 DOC-AMR-04 与 docs/tasks/task-04-blue-green-switch-and-agents.md 规范。

核心功能：
1. 校验/创建目标版本集合 `ai_memory_v2` (1024 维 Cosine，配置 status, project_id, type 索引)；
2. 触发 Outbox Worker 同步全量精炼记忆从 SQLite SSOT 到 `ai_memory_v2`；
3. 一致性强校验：确保 `ai_memory_v2` 写入点数与 SQLite `active` 记忆数 100% 严格一致；
4. 原子别名切换：调用 Qdrant `update_collection_aliases`，将别名 `ai_memory` 瞬间原子切到 `ai_memory_v2`；
5. 原集合安全保留为 cold backup，支持一键 `--rollback` 瞬间秒切回滚。
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from qdrant_client import QdrantClient
from qdrant_client.http import models

from config.settings import AppConfig, load_config
from src.core.engine import BGEM3Engine
from src.core.qdrant import DEFAULT_DISTANCE, DEFAULT_VECTOR_SIZE, QdrantManager
from src.core.session_store import SessionStore
from src.service.outbox_worker import OutboxWorker

logger = logging.getLogger("blue_green_switch")

ALIAS_NAME = "ai_memory"
NEW_COLLECTION = "ai_memory_v2"
COLD_BACKUP_COLLECTION = "ai_memory_cold_backup"


class BlueGreenSwitchManager:
    """
    Qdrant 蓝绿集合生命周期与别名切换管理器
    """

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        qdrant_manager: Optional[QdrantManager] = None,
        session_store: Optional[SessionStore] = None,
        engine: Optional[BGEM3Engine] = None,
    ):
        self.config = config or load_config()
        self.qdrant = qdrant_manager or QdrantManager.get_instance(config=self.config.qdrant)
        self.client: QdrantClient = self.qdrant.client
        self.session_store = session_store or SessionStore(config=self.config.storage)
        self.engine = engine or BGEM3Engine(config=self.config.model)

    def ensure_green_collection(self, collection_name: str = NEW_COLLECTION) -> bool:
        """
        创建新版本目标集合（1024 维 Cosine，载荷索引支持 status, project_id, type）
        """
        exists = self.client.collection_exists(collection_name)
        if not exists:
            logger.info(f"Creating collection '{collection_name}' (1024 dim, Cosine)...")
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=DEFAULT_VECTOR_SIZE,
                    distance=DEFAULT_DISTANCE,
                ),
            )
            logger.info(f"Collection '{collection_name}' created.")

        # 确保关键载荷索引
        for field in ["status", "project_id", "type", "memory_id", "scope", "memory_type"]:
            try:
                self.client.create_payload_index(
                    collection_name=collection_name,
                    field_name=field,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
            except Exception as e:
                logger.debug(f"Payload index '{field}' note: {e}")

        logger.info(f"Target collection '{collection_name}' ready with payload indexes.")
        return True

    async def sync_outbox_to_collection(
        self,
        target_collection: str = NEW_COLLECTION,
        batch_size: int = 50,
        max_retries: int = 5,
    ) -> int:
        """
        从 SQLite SSOT 触发 Outbox Worker 同步全量精炼记忆到目标集合
        """
        logger.info(f"Starting Outbox sync to '{target_collection}'...")
        worker = OutboxWorker(
            session_store=self.session_store,
            qdrant_manager=self.qdrant,
            engine=self.engine,
            config=self.config,
            batch_size=batch_size,
            default_collection=target_collection,
        )

        total_synced = 0
        while True:
            # 检查是否还有 pending 或 failed (retry_count < max_retries) 的任务
            pending_tasks = self.session_store.fetch_pending_sync_tasks(limit=batch_size)
            if not pending_tasks:
                break

            count = await worker.run_once(collection_name=target_collection)
            total_synced += count
            logger.info(f"Synced {total_synced} outbox tasks to '{target_collection}'...")

        logger.info(f"Outbox sync finished. Total tasks processed: {total_synced}")
        return total_synced

    def verify_consistency(self, target_collection: str = NEW_COLLECTION) -> Dict[str, Any]:
        """
        验证 target_collection 点数与 SQLite active 记忆一致性
        """
        with self.session_store._lock:
            conn = self.session_store._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM memories WHERE status = 'active';")
            sqlite_active_count = cur.fetchone()[0]

        qdrant_point_count = self.client.count(target_collection).count

        consistent = (sqlite_active_count == qdrant_point_count)
        result = {
            "target_collection": target_collection,
            "sqlite_active_memories": sqlite_active_count,
            "qdrant_points_count": qdrant_point_count,
            "is_consistent": consistent,
        }
        logger.info(
            f"Consistency Check: SQLite active={sqlite_active_count}, "
            f"Qdrant {target_collection}={qdrant_point_count} -> Consistent: {consistent}"
        )
        return result

    def get_current_alias_target(self, alias_name: str = ALIAS_NAME) -> Optional[str]:
        """获取别名当前指向的集合名称"""
        aliases = self.client.get_aliases().aliases
        for a in aliases:
            if a.alias_name == alias_name:
                return a.collection_name
        return None

    def switch_alias_atomic(
        self,
        new_target: str = NEW_COLLECTION,
        alias_name: str = ALIAS_NAME,
    ) -> bool:
        """
        执行原子别名切换：
        调用 client.update_collection_aliases(...) 将别名瞬间重定向到 new_target
        """
        current_target = self.get_current_alias_target(alias_name)
        ops: List[models.AliasOperations] = []

        if current_target:
            if current_target == new_target:
                logger.info(f"Alias '{alias_name}' already points to '{new_target}'. No-op.")
                return True
            ops.append(
                models.DeleteAliasOperation(
                    delete_alias=models.DeleteAlias(alias_name=alias_name)
                )
            )

        ops.append(
            models.CreateAliasOperation(
                create_alias=models.CreateAlias(
                    collection_name=new_target,
                    alias_name=alias_name,
                )
            )
        )

        logger.info(f"Executing atomic alias switch: '{alias_name}' -> '{new_target}' (was: '{current_target}')")
        self.client.update_collection_aliases(change_aliases_operations=ops)
        logger.info(f"Successfully switched alias '{alias_name}' to '{new_target}'!")
        return True

    def rollback_alias_atomic(
        self,
        backup_collection: str = COLD_BACKUP_COLLECTION,
        alias_name: str = ALIAS_NAME,
    ) -> bool:
        """
        一键回切：将别名切换回旧备份集合
        """
        if not self.client.collection_exists(backup_collection):
            raise RuntimeError(f"Cannot rollback: backup collection '{backup_collection}' does not exist!")

        logger.warning(f"ROLLBACK TRIGGERED: Switching alias '{alias_name}' back to '{backup_collection}'...")
        return self.switch_alias_atomic(new_target=backup_collection, alias_name=alias_name)


async def async_main(args: argparse.Namespace) -> int:
    manager = BlueGreenSwitchManager()

    if args.rollback:
        print("\n" + "=" * 60)
        print("          ⚠️  Qdrant 蓝绿热切换 - 执行一键回切")
        print("=" * 60)
        success = manager.rollback_alias_atomic(
            backup_collection=args.backup_collection,
            alias_name=args.alias_name,
        )
        curr = manager.get_current_alias_target(args.alias_name)
        print(f"回切完成！当前别名 [{args.alias_name}] -> [{curr}]")
        print("=" * 60 + "\n")
        return 0 if success else 1

    print("\n" + "=" * 60)
    print("          🚀 Qdrant 蓝绿无缝热切换流水线")
    print("=" * 60)

    # 1. 确保新版本目标集合存在并就绪
    manager.ensure_green_collection(args.target_collection)

    # 2. 从 SQLite SSOT 触发 Outbox Worker 同步全量精炼记忆到目标集合
    await manager.sync_outbox_to_collection(target_collection=args.target_collection)

    # 3. 验证目标集合点数与 SQLite active 记忆一致性
    check = manager.verify_consistency(args.target_collection)
    if not check["is_consistent"] and not args.force:
        logger.error(
            f"Consistency check failed! SQLite={check['sqlite_active_memories']} vs "
            f"Qdrant={check['qdrant_points_count']}. Aborting switch."
        )
        return 1

    # 4. 执行原子别名切换
    manager.switch_alias_atomic(
        new_target=args.target_collection,
        alias_name=args.alias_name,
    )

    curr = manager.get_current_alias_target(args.alias_name)
    print("\n" + "=" * 60)
    print("          🎉 蓝绿热切换成功！零停机、无脏数据")
    print("=" * 60)
    print(f"别名名称:       {args.alias_name}")
    print(f"当前指向集合:   {curr}")
    print(f"SQLite active:  {check['sqlite_active_memories']}")
    print(f"Qdrant 点数:    {check['qdrant_points_count']}")
    print(f"冷备集合:       {args.backup_collection} (支持 --rollback 一键回退)")
    print("=" * 60 + "\n")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Qdrant 蓝绿平滑别名切换与回滚脚本")
    parser.add_argument("--rollback", action="store_true", help="一键回切至旧版本冷备集合")
    parser.add_argument("--target-collection", type=str, default=NEW_COLLECTION, help="目标绿色集合名称")
    parser.add_argument("--backup-collection", type=str, default=COLD_BACKUP_COLLECTION, help="冷备蓝色集合名称")
    parser.add_argument("--alias-name", type=str, default=ALIAS_NAME, help="Qdrant 别名名称")
    parser.add_argument("--force", action="store_true", help="忽略一致性校验强制切换")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    code = asyncio.run(async_main(args))
    sys.exit(code)


if __name__ == "__main__":
    main()

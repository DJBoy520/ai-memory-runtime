#!/usr/bin/env python3
"""
AI Memory Runtime - 离线批处理提纯合并流水线脚本 (v2.2)
执行历史冷备数据提取、归一化、防语义漂移合并、贝叶斯累加及入库。
遵循 docs/tasks/task-03-offline-refine-pipeline.md 契约。
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 确保项目根目录在 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import AppConfig, load_config
from src.core.session_store import SessionStore
from src.service.cognitive_engine import CognitiveEngine
from src.service.entity_normalizer import EntityNormalizer

logger = logging.getLogger("migrate_refine_v2")

DEFAULT_BACKUP_PATH = PROJECT_ROOT / "data" / "backups" / "ai_memory_backup_20260927.json"
MIGRATION_SESSION_ID = "legacy_migration_20260927"
MIGRATION_AGENT_ID = "system_migrator"


def compute_file_sha256(filepath: str | Path) -> str:
    """计算文件的 SHA256 哈希"""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


class LegacyMemoryParser:
    """
    负责将冷备历史数据中的长文本/问答结构化解析提炼出：
    - (subject, predicate, object) 三元组
    - content: 原子化纯净陈述句
    - type: fact, preference, decision, task, episode
    """

    @staticmethod
    def classify_type(raw_type: str, content: str) -> str:
        """根据原始类型与内容关键字自动判定或纠正记忆类型"""
        raw_type = (raw_type or "").lower().strip()
        lower_content = content.lower()

        # 1. 关键历史事件 (Episode) 判定：保持不可变，不盲目合并 (优先判断崩溃、复盘等)
        if raw_type == "episode" or any(
            w in content[:80] for w in ["事件", "事故", "崩溃原因", "故障复盘", "复盘", "episode"]
        ):
            return "episode"

        # 2. 任务类型判定
        if (
            "task" in raw_type
            or "任务" in content[:60]
            or "【任务" in content[:60]
            or "状态为" in content
            or "status_is" in content
            or "state:" in lower_content
        ):
            if any(w in content for w in ["进行中", "已完成", "待办", "in_progress", "completed", "pending", "failed"]):
                return "task"

        # 3. 偏好类型判定
        if raw_type == "preference" or any(w in content[:80] for w in ["偏好", "喜欢", "习惯", "prefer", "like", "不要总是"]):
            return "preference"

        # 4. 决策类型判定
        if raw_type in ("decision", "standard", "architecture") or any(
            w in content[:80] for w in ["决策", "规范", "方案", "指令", "采用", "只读审计", "原则", "policy"]
        ):
            return "decision"

        # 5. 事实与知识
        if raw_type in ("fact", "knowledge"):
            return "fact"

        return "fact"

    @staticmethod
    def extract_triple_and_statement(content: str, mem_type: str) -> Tuple[str, str, Optional[str], str]:
        """
        结构化提取：提炼 (subject, predicate, object) 及原子陈述句 content。
        """
        text = content.strip()

        # 去除开头包装标识
        # 如 【用户指令/需求】: ... 【结论/解决方案】: ...
        user_req_match = re.search(r"【(?:用户指令/需求|用户审核/指令|用户开发指令/需求|用户DSH需求/指令)】:\s*(.+?)(?:\n【|$)", text, re.DOTALL)
        conclusion_match = re.search(r"【(?:结论/解决方案|OpenClaw执行/审核结论)】:\s*(.+)", text, re.DOTALL)

        user_req = user_req_match.group(1).strip() if user_req_match else ""
        conclusion = conclusion_match.group(1).strip() if conclusion_match else ""

        # 1. 优先从第一句/关键行提炼
        lines = [line.strip() for line in text.split("\n") if line.strip() and not line.startswith("```")]
        first_line = lines[0] if lines else text

        subject = "user"
        predicate = "prefers" if mem_type == "preference" else "status_is" if mem_type == "task" else "specifies"
        object_val: Optional[str] = None
        statement = text

        # 针对 task 的解析
        if mem_type == "task":
            predicate = "status_is"
            # 尝试提取任务主体与状态
            task_match = re.search(r"(?:任务|task)[:：\s]*([^\s,，。]+)", text, re.IGNORECASE)
            if task_match:
                subject = task_match.group(1).strip()
            else:
                subject = "task_item"

            if any(w in text for w in ["已完成", "completed", "done", "100%"]):
                object_val = "completed"
            elif any(w in text for w in ["进行中", "in_progress", "running"]):
                object_val = "in_progress"
            elif any(w in text for w in ["阻塞", "blocked"]):
                object_val = "blocked"
            elif any(w in text for w in ["失败", "failed"]):
                object_val = "failed"
            elif any(w in text for w in ["取消", "cancelled"]):
                object_val = "cancelled"
            else:
                object_val = "pending"

            statement = f"任务 {subject} 的状态为 {object_val}"

        # 针对 preference 的解析
        elif mem_type == "preference":
            subject = "user"
            # 判断喜欢/不喜欢
            if any(w in text for w in ["不喜欢", "讨厌", "不要", "禁止", "dislike", "avoid"]):
                predicate = "dislikes"
            else:
                predicate = "prefers"

            # 优先从需求正文中截取
            target_str = user_req if user_req else text
            # 去除前缀
            target_str = re.sub(r"^【[^】]+】:\s*", "", target_str)
            # 寻找宾语匹配 (避免把 前缀匹配进去)
            pref_match = re.search(r"(?:喜欢喝|喜欢用|喜欢|偏好|不喜欢喝|不喜欢|讨厌|喝|用|采用|prefer|love|dislike)[:：\s]*([^\s,，。!！\n]+)", target_str, re.IGNORECASE)
            if pref_match:
                object_val = pref_match.group(1).strip()
            else:
                object_val = target_str[:30].strip()
            statement = f"用户 {predicate} {object_val}"

        # 针对 decision 的解析
        elif mem_type == "decision":
            predicate = "adopts_decision"
            # 尝试从第一行或标题中提取主体
            subj_match = re.search(r"(?:项目|系统|模块|组件|指令|方案)[:：\s>]*([^\s,，。>\n]+)", text)
            if subj_match:
                subject = subj_match.group(1).strip()
            elif "aep" in text.lower():
                subject = "aep"
            elif "amr" in text.lower():
                subject = "ai_memory_runtime"
            else:
                subject = "system"

            object_val = "approved" if any(w in text for w in ["通过", "批准", "已完成", "落地"]) else "specified"
            # 陈述句保留结论或核心行
            if conclusion:
                first_concl = conclusion.split("\n")[0].strip()
                statement = f"关于 {subject} 的决策: {first_concl[:200]}"
            else:
                statement = f"关于 {subject} 的决策规范: {first_line[:200]}"

        # 针对 episode 的解析
        elif mem_type == "episode":
            subject = "event"
            predicate = "occurred"
            object_val = "recorded"
            statement = text[:250]

        # 针对 fact 的解析
        else:
            predicate = "states_fact"
            if "aep" in text.lower():
                subject = "aep"
            elif "openclaw" in text.lower():
                subject = "openclaw"
            elif "hermes" in text.lower():
                subject = "hermes"
            else:
                subject = "system"

            if conclusion:
                first_concl = conclusion.split("\n")[0].strip()
                statement = f"{subject} 事实: {first_concl[:200]}"
            else:
                statement = f"{subject} 事实: {first_line[:200]}"

        # 清洗三元组主谓宾
        subject = re.sub(r"^[#\s>]+", "", subject).strip() or "system"
        predicate = predicate.strip() or "relates_to"
        if object_val:
            object_val = re.sub(r"^[#\s>]+", "", object_val).strip()

        return subject, predicate, object_val, statement


class MigrationRefinePipeline:
    """
    离线批处理提纯合并流水线
    """

    def __init__(
        self,
        backup_path: str | Path = DEFAULT_BACKUP_PATH,
        session_store: Optional[SessionStore] = None,
        cognitive_engine: Optional[CognitiveEngine] = None,
        normalizer: Optional[EntityNormalizer] = None,
        config: Optional[AppConfig] = None,
    ):
        self.backup_path = Path(backup_path)
        self.config = config or load_config()
        self.session_store = session_store or SessionStore(config=self.config.storage)
        self.normalizer = normalizer or EntityNormalizer()
        self.cognitive_engine = cognitive_engine or CognitiveEngine(
            session_store=self.session_store,
            normalizer=self.normalizer,
            config=self.config,
        )
        self.parser = LegacyMemoryParser()

    def run(self, sample_limit: Optional[int] = None) -> Dict[str, Any]:
        """
        运行流水线：
        1. 校验与读取冷备文件
        2. 注入 raw_sessions 与 raw_messages (source_type='legacy_memory', is_synthetic=1)
        3. 结构化提取与归一化
        4. 语义防漂移判定与合并/创建
        5. 生成并返回审计报告
        """
        start_time = time.time()
        if not self.backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {self.backup_path}")

        file_hash = compute_file_sha256(self.backup_path)
        logger.info(f"Loading backup file: {self.backup_path} (SHA256: {file_hash[:12]}...)")

        with open(self.backup_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if not isinstance(raw_data, list):
            raise ValueError("Expected list of items in backup JSON")

        total_available = len(raw_data)
        items_to_process = raw_data[:sample_limit] if sample_limit is not None else raw_data
        total_raw = len(items_to_process)
        logger.info(f"Total available: {total_available}, processing: {total_raw} items (sample={sample_limit})")

        # 1. 初始化合成归档会话
        now = int(time.time())
        self.session_store.record_raw_session(
            session_id=MIGRATION_SESSION_ID,
            agent_id=MIGRATION_AGENT_ID,
            project_id="general",
            started_at=now,
            status="active",
        )

        merged_count = 0
        created_count = 0
        episode_count = 0
        active_memory_ids: List[str] = []
        # 按 (norm_subj, norm_pred) 分组维护当前活跃记忆 ID 列表，用于高效候选匹配
        subject_pred_index: Dict[Tuple[str, str], List[str]] = {}

        for seq, item in enumerate(items_to_process, start=1):
            payload = item.get("payload", {})
            orig_mem_id = payload.get("memory_id") or f"legacy_{seq}"
            raw_content = payload.get("content") or ""
            raw_type = payload.get("memory_type") or "fact"
            project_id = payload.get("project_id") or "general"
            scope = payload.get("scope") or "global"
            source_agent = payload.get("agent_id") or payload.get("assistant_id") or "legacy_agent"
            raw_session_id = payload.get("session_id") or MIGRATION_SESSION_ID

            # 如果原始 session 存在，确保其在 raw_sessions 中登记，防止外键报错
            if raw_session_id != MIGRATION_SESSION_ID:
                self.session_store.record_raw_session(
                    session_id=raw_session_id,
                    agent_id=source_agent,
                    project_id=project_id,
                    started_at=now,
                    status="active",
                )

            # 2. 注入 raw_messages (显式标记 source_type='legacy_memory', is_synthetic=1)
            msg_id = f"raw_msg_legacy_{seq}_{orig_mem_id[-8:]}"
            self.session_store.record_raw_message(
                message_id=msg_id,
                session_id=raw_session_id,
                role="user",
                content=raw_content,
                sequence=seq,
                source_type="legacy_memory",
                is_synthetic=1,
                created_at=now,
            )

            # 3. 结构化解析
            mem_type = self.parser.classify_type(raw_type, raw_content)
            subject, predicate, object_val, statement = self.parser.extract_triple_and_statement(raw_content, mem_type)

            # 4. 实体与谓词归一化
            norm_res = self.normalizer.normalize_triple(subject, predicate, object_val)
            norm_subj = norm_res["subject"]
            norm_pred = norm_res["predicate"]
            norm_obj = norm_res["object"]

            evidence_item = {
                "message_id": msg_id,
                "session_id": raw_session_id,
                "evidence_strength": 0.5,
                "linked_at": now,
            }

            # 5. 合并与冲突消解逻辑
            # 特殊规则：Episode 记忆保持不可变，不盲目合并，完整保全关键事件
            if mem_type == "episode":
                created = self.session_store.create_memory(
                    memory_id=self.cognitive_engine._generate_memory_id(),
                    subject=norm_subj,
                    predicate=norm_pred,
                    object=norm_obj,
                    content=statement,
                    type="episode",
                    conflict_policy="immutable",
                    confidence=0.9,
                    importance=0.8,
                    mention_count=1,
                    status="active",
                    project_id=project_id,
                    scope=scope,
                    source_agent=source_agent,
                    evidence=[evidence_item],
                    operator="pipeline:migrate",
                    audit_detail={"action": "migrate_episode", "orig_id": orig_mem_id},
                )
                active_memory_ids.append(created["memory_id"])
                created_count += 1
                episode_count += 1
                continue

            # 尝试在已建立的 (norm_subj, norm_pred) 索引中寻找候选记忆尝试合并
            candidates = subject_pred_index.get((norm_subj, norm_pred), [])
            merged = False

            for cand_id in candidates:
                merge_res = self.cognitive_engine.merge_memory(
                    existing_memory_id=cand_id,
                    new_subject=norm_subj,
                    new_predicate=norm_pred,
                    new_content=statement,
                    new_object=norm_obj,
                    evidence=evidence_item,
                    evidence_strength=0.5,
                )
                if merge_res.get("merged"):
                    merged = True
                    merged_count += 1
                    break

            if not merged:
                # 无法合并（或无冲突候选），作为新记忆创建
                conflict_policy = (
                    "state_machine" if mem_type == "task"
                    else "coexist" if mem_type == "preference"
                    else "overwrite" if mem_type == "decision"
                    else "coexist"
                )

                created = self.session_store.create_memory(
                    memory_id=self.cognitive_engine._generate_memory_id(),
                    subject=norm_subj,
                    predicate=norm_pred,
                    object=norm_obj,
                    content=statement,
                    type=mem_type,
                    conflict_policy=conflict_policy,
                    confidence=0.8,
                    importance=0.5,
                    mention_count=1,
                    status="active",
                    project_id=project_id,
                    scope=scope,
                    source_agent=source_agent,
                    evidence=[evidence_item],
                    operator="pipeline:migrate",
                    audit_detail={"action": "migrate_create", "orig_id": orig_mem_id},
                )
                mem_id = created["memory_id"]
                active_memory_ids.append(mem_id)
                created_count += 1
                subject_pred_index.setdefault((norm_subj, norm_pred), []).append(mem_id)

            if seq % 20 == 0 or seq == total_raw:
                logger.info(f"Progress: [{seq}/{total_raw}] processed, created: {created_count}, merged: {merged_count}")

        elapsed_time = round(time.time() - start_time, 3)

        # 统计平均置信度
        total_confidence = 0.0
        refined_cards_count = len(active_memory_ids)
        for mid in active_memory_ids:
            mem = self.session_store.get_memory(mid)
            if mem:
                total_confidence += float(mem.get("confidence", 0.0))
        avg_confidence = round(total_confidence / refined_cards_count, 4) if refined_cards_count > 0 else 0.0

        report = {
            "backup_file": str(self.backup_path),
            "file_sha256": file_hash,
            "raw_records_count": total_raw,
            "merged_count": merged_count,
            "refined_cards_count": refined_cards_count,
            "episode_cards_count": episode_count,
            "avg_confidence": avg_confidence,
            "elapsed_seconds": elapsed_time,
        }
        return report


def main():
    parser = argparse.ArgumentParser(description="AMR v2.2 离线批处理提纯合并流水线")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sample", type=int, help="处理前 N 条样本（例如 100）")
    group.add_argument("--full", action="store_true", help="全量处理所有样本")
    parser.add_argument("--backup-path", type=str, default=str(DEFAULT_BACKUP_PATH), help="备份文件路径")
    parser.add_argument("--db-path", type=str, default=None, help="目标 SQLite 数据库路径")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config()
    if args.db_path:
        config.storage.sqlite_path = args.db_path

    limit = 100 if args.sample is None and not args.full else args.sample
    if args.full:
        limit = None

    pipeline = MigrationRefinePipeline(backup_path=args.backup_path, config=config)
    report = pipeline.run(sample_limit=limit)

    print("\n" + "=" * 55)
    print("        AMR v2.2 离线提纯合并处理报告")
    print("=" * 55)
    print(f"原始备份文件:   {report['backup_file']}")
    print(f"文件 SHA256:    {report['file_sha256'][:16]}...")
    print(f"原始记录总数:   {report['raw_records_count']}")
    print(f"去重合并次数:   {report['merged_count']}")
    print(f"生成精炼卡片:   {report['refined_cards_count']} (含不可变 Episode: {report['episode_cards_count']})")
    print(f"卡片平均置信度: {report['avg_confidence']}")
    print(f"流水线总耗时:   {report['elapsed_seconds']} 秒")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()

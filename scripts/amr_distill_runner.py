#!/usr/bin/env python3
"""
AI Memory Runtime - Cognitive Distillation CLI Runner
执行增量会话认知提炼的命令行调度入口：
1. 默认安全：默认不发送真实会话、不自动启用外部模型（执行安全 Dry-Run）；
2. 显式授权：仅当传入 --enable-external-model 或设置 AMR_DISTILL_ENABLE_EXTERNAL=true 时，
   才允许调用指定的 OpenAI 兼容端点；
3. 严格超时与重试保留：执行失败时错误原因清晰可见，并在状态表中持久化以供精准重试；
4. 绝对隐私安全：严禁在任何日志输出中打印密钥、凭据或会话原始内容；
5. 14 天 Dry-Run 铁律：所有提炼提案仅作为 PROPOSED 状态写入 curation_candidates 隔离表，绝对不得直接修改 memories SSOT。
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# 动态将项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 自动处理多 Python 环境容错（如系统默认 python3 缺少环境依赖时切换到 python3.12）
try:
    import pydantic
except ModuleNotFoundError:
    import shutil
    py312 = shutil.which("python3.12")
    if py312 and py312 != sys.executable:
        os.execv(py312, [py312] + sys.argv)
    raise

from config.settings import load_config
from src.core.session_store import SessionStore
from src.cognitive.model_worker import (
    DryRunWorker,
    OpenAICompatibleWorker,
    mask_secret,
    sanitize_text,
)
from src.cognitive.distill_runner import (
    DistillationRunner,
    resolve_runner_config,
)

logger = logging.getLogger("amr_distill_cli")


class SafePrivacyFilter(logging.Filter):
    """全局日志隐私过滤器：确保即使任何模块意外输出敏感信息，也被彻底拦截与脱敏"""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.msg and isinstance(record.msg, str):
            record.msg = sanitize_text(record.msg)
        if record.args:
            if isinstance(record.args, tuple):
                record.args = tuple(
                    sanitize_text(str(a)) if isinstance(a, str) else a for a in record.args
                )
            elif isinstance(record.args, dict):
                record.args = {
                    k: sanitize_text(str(v)) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
        return True


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [distill_runner] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    handler.addFilter(SafePrivacyFilter())

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    # 清空已有 handlers 防止重复打印
    root_logger.handlers = [handler]


def print_text_summary(summary: Dict[str, Any]) -> None:
    """以清晰的终端格式输出提炼执行报表"""
    print("=" * 64)
    print("      AI Memory Runtime - 会话认知提纯批处理审计报表")
    print("=" * 64)
    print(f"批次 ID           : {summary.get('batch_id')}")
    print(f"安全 Dry-Run 模式 : {'是 (未调用外部模型)' if summary.get('dry_run') else '否'}")
    print(f"外部模型启用状态  : {'已显式启用' if summary.get('external_model_enabled') else '已禁用 (安全默认)'}")
    print(f"使用模型名称      : {summary.get('model_name')}")
    print("-" * 64)
    print(f"扫描会话总数      : {summary.get('sessions_scanned', 0)}")
    print(f"成功处理会话数    : {summary.get('sessions_succeeded', 0)}")
    print(f"失败保留待重试数  : {summary.get('sessions_failed', 0)}")
    print(f"跳过空消息会话数  : {summary.get('sessions_skipped', 0)}")
    print(f"提纯候选生成条数  : {summary.get('total_written_candidates', 0)} (状态: PROPOSED)")
    print("-" * 64)

    failed = summary.get("failed_sessions", [])
    if failed:
        print("\n【失败会话清单及原因（保留待重试）】:")
        for idx, f in enumerate(failed, 1):
            sid = f.get("session_id", "unknown")
            st = f.get("status", "UNKNOWN")
            attempts = f.get("attempts", 1)
            err = f.get("error", "")
            print(f"  {idx}. 会话: {sid}")
            print(f"     状态: {st} | 尝试次数: {attempts}")
            print(f"     原因: {err}")
        print("\n提示: 可通过 `python scripts/amr_distill_runner.py --retry-failed` 仅对失败会话执行重试。")
    else:
        print("\n所有扫描会话均已成功处理完成或无需处理。")
    print("=" * 64)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AMR 会话认知提炼与生命周期调度入口 CLI (RFC-004 & RFC-005)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # 基础运行与数据库配置
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="SQLite 数据库文件路径 (默认读取 config.yaml 或 data/sessions.db)",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=None,
        help="增量扫描回溯窗口秒数 (默认 86400 秒即 24 小时; 传 0 表示全量)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="单批次最大扫描会话数限制 (默认 50)",
    )
    parser.add_argument(
        "--session-id",
        type=str,
        default=None,
        help="仅针对单个指定 session_id 进行提炼",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="自定义批处理 ID (默认自动按时间戳生成)",
    )

    # 失败重试与状态查看
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        default=False,
        help="仅挑选历史执行失败 (FAILED) 的会话进行针对性重试",
    )
    parser.add_argument(
        "--list-failed",
        action="store_true",
        default=False,
        help="查看当前数据库中记录的所有失败会话及其原因并退出",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        default=False,
        help="查看提纯总览状态与已提炼候选统计并退出",
    )
    parser.add_argument(
        "--reset-failed",
        action="store_true",
        default=False,
        help="重置所有失败会话状态为 PENDING 待重试状态并退出",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="强制重新提炼即使该会话历史已经 SUCCESS 成功处理过",
    )

    # 模型端点与安全门禁配置
    parser.add_argument(
        "--enable-external-model",
        action="store_true",
        default=False,
        help="【显式授权门禁】允许向配置的 OpenAI 兼容外部端点发送会话内容。未显式开启时默认运行安全 Dry-Run！",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="显式指定 Dry-Run 安全模式（绝对不调用外部网络，自动保全消息）",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=None,
        help="OpenAI 兼容服务端点基础 URL (如 http://localhost:8000/v1 或 https://api.openai.com/v1)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="提炼模型名称 (如 gpt-4o, qwen2.5-72b-instruct 等)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="API 鉴权密钥 (建议优先通过环境变量 AMR_DISTILL_API_KEY 传递)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="HTTP 请求严格超时时间 (秒, 默认 30.0)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help="单会话提纯失败单次运行最大重试次数 (默认 3)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=None,
        help="重试间隔退避秒数 (默认 1.0)",
    )

    # 输出格式
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="报表输出格式 (text 或 json)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="开启详细调试日志 (仍严格遵守零凭据、零会话内容输出原则)",
    )

    return parser.parse_args()


async def main_async() -> int:
    args = parse_arguments()
    setup_logging(verbose=args.verbose)

    # 1. 加载系统配置
    app_cfg = load_config()
    cfg_dict = app_cfg.model_dump() if hasattr(app_cfg, "model_dump") else {}

    # 2. 分级解析仲裁提纯配置
    resolved_cfg = resolve_runner_config(cli_args=args, config_dict=cfg_dict)

    # 确定数据库路径
    db_path_str = args.db_path or app_cfg.storage.sqlite_path
    db_path = Path(db_path_str)
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path

    # 初始化存储仓储
    session_store = SessionStore(db_path=str(db_path))

    # 安全门禁判定
    is_external_enabled = bool(resolved_cfg["enable_external_model"] and not resolved_cfg["dry_run"])

    # 3. 构造工作器
    if is_external_enabled:
        logger.info(
            "External model explicitly enabled: model=%s, timeout=%.1fs",
            resolved_cfg["model"],
            resolved_cfg["timeout"],
        )
        worker = OpenAICompatibleWorker(
            base_url=resolved_cfg["base_url"],
            model=resolved_cfg["model"],
            api_key=resolved_cfg["api_key"],
            timeout=resolved_cfg["timeout"],
            allow_external=True,
        )
    else:
        logger.info(
            "External model is disabled by default. Running in safe Dry-Run mode (no real session content sent externally)."
        )
        worker = DryRunWorker()

    # 4. 构造执行器
    runner = DistillationRunner(
        session_store=session_store,
        model_worker=worker,
        allow_external=is_external_enabled,
        dry_run=not is_external_enabled,
        default_max_retries=resolved_cfg["max_retries"],
        default_retry_delay=resolved_cfg["retry_delay"],
    )

    # 处理只读状态查询命令
    if args.status:
        stat = runner.get_distillation_summary()
        if args.format == "json":
            print(json.dumps(stat, indent=2, ensure_ascii=False))
        else:
            print("\n--- AMR 会话提纯全貌状态 ---")
            for k, v in stat.get("statuses", {}).items():
                print(f"  状态 {k:10s} : {v} 条会话")
            print(f"  当前 PROPOSED 候选总数: {stat.get('total_proposed_candidates', 0)}")
        return 0

    if args.list_failed:
        failed_list = runner.get_failed_sessions(limit=args.limit or 50)
        if args.format == "json":
            print(json.dumps(failed_list, indent=2, ensure_ascii=False))
        else:
            print(f"\n--- 当前失败保留待重试会话 (共 {len(failed_list)} 条) ---")
            for idx, item in enumerate(failed_list, 1):
                print(f"{idx}. 会话: {item['session_id']}")
                print(f"   批次: {item['batch_id']} | 尝试次数: {item['attempts']}")
                print(f"   类型: {item['last_error_type']} | 原因: {item['last_error']}")
                print("-" * 50)
        return 0

    if args.reset_failed:
        count = runner.reset_failed_sessions()
        logger.info("Reset %d failed sessions to PENDING state.", count)
        print(f"已成功将 {count} 个失败会话重置为 PENDING 状态。")
        return 0

    # 5. 执行批处理提炼
    summary = await runner.run_batch(
        lookback_seconds=resolved_cfg["lookback_seconds"],
        limit=resolved_cfg["limit"],
        batch_id=args.batch_id,
        retry_failed_only=args.retry_failed,
        specific_session_id=args.session_id,
        force=args.force,
        max_retries=resolved_cfg["max_retries"],
        retry_delay=resolved_cfg["retry_delay"],
    )

    # 6. 输出结果
    if args.format == "json":
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print_text_summary(summary)

    # 返回状态码：若有会话执行失败，返回 1 提醒调度系统/监控
    if summary.get("sessions_failed", 0) > 0:
        return 1
    return 0


def main() -> None:
    try:
        exit_code = asyncio.run(main_async())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n[!] 用户中断提纯流程。")
        sys.exit(130)
    except Exception as ex:
        logger.error("Distillation runner failed fatally: %s", type(ex).__name__)
        sys.exit(2)


if __name__ == "__main__":
    main()

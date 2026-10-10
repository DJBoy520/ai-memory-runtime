"""
F3-2 回归测试：MCP bridge stdin reader 上限

背景：bridge.run_stdio 此前构造 StreamReader 未传 limit（asyncio 默认 64KB），
超长单条 JSON-RPC（如 memory_ingest_session 批量摄取）会抛 LimitOverrunError，
与 config.server.max_request_bytes=4MB 的声明不一致。
修复后 reader 上限应等于 config.server.max_request_bytes。
"""

import asyncio

from src.interfaces.mcp.bridge import MCPBridge
from config.settings import load_config


def test_bridge_reader_limit_matches_config():
    """run_stdio 构造的 StreamReader 上限必须等于 config.server.max_request_bytes（而非 asyncio 默认 64KB）"""
    captured: dict = {}
    original_reader_cls = asyncio.StreamReader

    class _CaptureReader(original_reader_cls):
        def __init__(self, *args, **kwargs):
            captured["limit"] = kwargs.get("limit")
            super().__init__(*args, **kwargs)

    asyncio.StreamReader = _CaptureReader
    try:
        bridge = MCPBridge(source_agent="test_runner")
        try:
            asyncio.run(bridge.run_stdio())
        except Exception:
            pass  # 无 stdin 管道环境里 connect_read_pipe 会失败，reader 在此前已构造完毕
    finally:
        asyncio.StreamReader = original_reader_cls

    expected = load_config().server.max_request_bytes
    assert captured.get("limit") == expected, (
        f"StreamReader limit 应为 config.server.max_request_bytes={expected}，实际 {captured.get('limit')}"
    )
    # 必须显著大于 asyncio 旧默认 64KB（本修复的目标场景：100KB 单条请求）
    assert expected >= 100 * 1024


def test_configured_limit_reads_100kb_single_line():
    """语义验证：按 config 上限构造的 reader 能读完 100KB 单行请求，默认 64KB 上限则读不下"""
    limit = load_config().server.max_request_bytes
    payload = (
        '{"jsonrpc":"2.0","method":"session.ingest","params":{"blob":"'
        + "x" * (100 * 1024)
        + '"}}\n'
    )

    async def _read_with(reader_limit: int) -> bytes:
        reader = asyncio.StreamReader(limit=reader_limit)
        reader.feed_data(payload.encode("utf-8"))
        reader.feed_eof()
        return await reader.readline()

    assert asyncio.run(_read_with(limit)).decode("utf-8") == payload

    hit_default_ceiling = False
    try:
        asyncio.run(_read_with(64 * 1024))
    except ValueError:
        hit_default_ceiling = True
    assert hit_default_ceiling, "asyncio 默认 64KB 上限读不下 100KB 单行（这正是本修复要解决的场景）"

#!/usr/bin/env python3
"""Run the isolated quickstart with a single-threaded local Qdrant client."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import shlex
import signal
import sys
import threading
import uuid

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from qdrant_client import QdrantClient, models
from src.core.engine import BGEM3Engine
from src.core.qdrant import QdrantManager
from src.interfaces.ipc.protocol import make_jsonrpc_request, read_frame, write_frame
from src.main import AMRApplication


class SerialQdrant:
    """Create and access local Qdrant exclusively on one dedicated thread."""
    def __init__(self, path):
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tryout-qdrant")
        self.thread_id = self.executor.submit(threading.get_ident).result()
        try:
            self.client = self.executor.submit(QdrantClient, path=str(path)).result()
        except BaseException:
            self.executor.shutdown(wait=True)
            raise

    def __getattr__(self, name):
        def call(*args, **kwargs):
            return self.executor.submit(getattr(self.client, name), *args, **kwargs).result()
        return call

    def create_payload_index(self, *args, **kwargs):
        # Qdrant Local evaluates filters without payload indexes.
        return None

    def close(self):
        try:
            self.executor.submit(self.client.close).result()
        finally:
            self.executor.shutdown(wait=True)


async def run():
    root = Path(os.environ['AMR_TRYOUT_DIR'])
    config = root / 'config.json'
    model = os.environ['AMR_TRYOUT_MODEL']
    from config.settings import load_config
    cfg = load_config(config)
    runtime = Path(os.environ['AMR_TRYOUT_RUNTIME'])
    if (cfg.server.business_socket != str(runtime / 'business.sock')
            or cfg.server.admin_socket != str(runtime / 'admin.sock')
            or cfg.storage.sqlite_path != str(root / 'memories.db')):
        raise RuntimeError('试用配置中的 socket 或数据库路径已改变；请恢复试用目录内的路径。')
    try:
        local = SerialQdrant(root / 'qdrant')
        manager = QdrantManager(config=cfg.qdrant, client=local)
        manager.ensure_standard_collections()
        point_id = str(uuid.uuid4())
        local.upsert(collection_name='ai_memory', points=[models.PointStruct(id=point_id, vector=[1.0] + [0.0] * 1023, payload={'tryout_probe': point_id})], wait=True)
        probe_filter = models.Filter(must=[models.FieldCondition(key='tryout_probe', match=models.MatchValue(value=point_id))])
        hits = local.query_points(collection_name='ai_memory', query=[1.0] + [0.0] * 1023, query_filter=probe_filter, limit=1).points
        if not any(str(point.id) == point_id for point in hits):
            raise RuntimeError('write/search probe did not return the test point')
        local.delete(collection_name='ai_memory', points_selector=models.PointIdsList(points=[point_id]), wait=True)
    except Exception as exc:
        print(f'Qdrant 初始化或读写自检失败: {exc}', file=sys.stderr, flush=True)
        raise

    engine = BGEM3Engine(config=cfg.model, model_path=model, crash_count_path=root / 'crash.count')
    app = AMRApplication(config_path=str(config), engine=engine, qdrant_manager=manager)
    try:
        try:
            await engine.embed(['AMR CPU quickstart readiness check'])
        except Exception as exc:
            raise RuntimeError(f'模型加载或推理失败（请检查模型下载和内存）: {exc}') from exc
        try:
            await app.start()
            reader, writer = await asyncio.open_unix_connection(cfg.server.business_socket)
            try:
                await write_frame(writer, make_jsonrpc_request(method='system.health', params={'agent_id': 'claude_code'}, req_id=1))
                reply = await asyncio.wait_for(read_frame(reader), timeout=10)
                if reply.get('error') or not reply.get('result', {}).get('sqlite_ok'):
                    raise RuntimeError(f'health check failed: {reply.get("error", "SQLite unavailable")}')
            finally:
                writer.close()
                await writer.wait_closed()
        except Exception as exc:
            raise RuntimeError(f'业务 socket 不可达或自检失败: {exc}') from exc

        socket = shlex.quote(cfg.server.business_socket)
        bridge = shlex.quote(str(ROOT / 'src/interfaces/mcp/bridge.py'))
        python = shlex.quote(sys.executable)
        spool = shlex.quote(str(root / 'spool'))
        print('\nREADY — 模型、Qdrant 和业务 socket 已就绪。', flush=True)
        print('保持此窗口运行，在另一终端执行以下两条接入命令：', flush=True)
        print(f'claude mcp add --transport stdio --scope user amr-tryout --env AMR_SOCKET_PATH={socket} --env AMR_SOURCE_AGENT=claude_code --env AMR_SPOOL_DIR={spool} -- {python} {bridge}', flush=True)
        print(f'codex mcp add amr-tryout --env AMR_SOCKET_PATH={socket} --env AMR_SOURCE_AGENT=codex --env AMR_SPOOL_DIR={spool} -- {python} {bridge}', flush=True)
        print('\nClaude Code 提示词：调用 AMR 的 memory_create，在 amr-demo 项目保存这条我确认的决定：导出功能首版支持 CSV，JSON 放到下一版。返回 memory_id 和 projection_status。', flush=True)
        print('Codex 新会话提示词：用 AMR 的 memory_search 在 amr-demo 项目查询导出功能首版选择，返回工具结果中的 memory_id。若无结果，等待 1 秒并重试，最多 3 次。', flush=True)
        print('pending 表示已落盘、索引进行中；可用 memory_get 按 ID 立即确认。Ctrl-C 停止。', flush=True)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
    finally:
        await app.stop()


if __name__ == '__main__':
    try:
        asyncio.run(run())
    except Exception as exc:
        print(f'启动失败: {exc}', file=sys.stderr)
        sys.exit(1)

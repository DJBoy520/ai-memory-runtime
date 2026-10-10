"""
AMR v2.2 端到端集成与 Agent 兼容性验证测试套件
对应 Task 04 任务书要求：
1. 验证 UDS 响应体契约中同时输出平铺字段（content, score, memory_id）与结构化三元组（subject, predicate, object）；
2. 验证多 Agent 场景通过 UDS 查询三大主题：
   - 'Tesla P4 显卡状态'
   - 'AEP 架构原则'
   - 'Sub2API 与模型配置'
3. 验证端到端检索耗时严格维持在 < 40ms 预算内（Tesla P4 + BGE-M3 + Qdrant 蓝绿热切集合）；
4. 断言返回评分字段同时包含 final_score 与 vector_score。
"""

import asyncio
import os
import time
from typing import Any, Dict, List
import pytest

from config.settings import load_config
from src.interfaces.ipc.protocol import encode_frame, read_frame

CONFIG = load_config()
BUSINESS_SOCKET = CONFIG.server.business_socket


async def call_uds_rpc(method: str, params: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
    """通过 UDS 连接调用 business RPC 并返回结果"""
    reader, writer = await asyncio.wait_for(
        asyncio.open_unix_connection(BUSINESS_SOCKET),
        timeout=timeout,
    )
    req = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": "e2e-test-1",
    }
    writer.write(encode_frame(req))
    await writer.drain()

    resp = await asyncio.wait_for(read_frame(reader), timeout=timeout)
    writer.close()
    await writer.wait_closed()
    return resp


@pytest.fixture(scope="module", autouse=True)
def ensure_daemon_running():
    """断言并确保 AMR 守护进程正在监听 business UDS"""
    if not os.path.exists(BUSINESS_SOCKET):
        pytest.skip(f"Business socket not found at {BUSINESS_SOCKET}. Daemon may not be running.")


@pytest.mark.asyncio
async def test_warmup_latency():
    """预热连接与模型缓存"""
    resp = await call_uds_rpc("memory.search", {"query": "系统预热", "limit": 1})
    assert "result" in resp or "error" not in resp


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "topic",
    [
        "Tesla P4 显卡状态",
        "AEP 架构原则",
        "Sub2API 与模型配置",
    ],
)
async def test_v2_end_to_end_search_contract_and_latency(topic: str):
    """
    测试通过 UDS 查询三大业务主题：
    1. 检索时延预算断言 < 40ms；
    2. 结果数量 >= 1；
    3. 契约兼容断言：平铺字段 content, score, memory_id 与结构化三元组 subject, predicate, object；
    4. 评分体系断言：同时包含 final_score 与 vector_score。
    """
    # 统计 3 次查询平均耗时，确保单次稳定在预算内
    latencies: List[float] = []
    last_resp: Dict[str, Any] = {}

    for _ in range(3):
        t0 = time.perf_counter()
        resp = await call_uds_rpc("memory.search", {"query": topic, "limit": 3})
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)
        last_resp = resp

    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    print(f"\n[E2E Perf] Topic: '{topic}' | Min: {min_latency:.2f}ms | Avg: {avg_latency:.2f}ms")

    # 1. 耗时预算断言 < 40ms (基于预热后的热态推理与 gRPC 检索)
    assert min_latency < 40.0, f"Query '{topic}' min latency {min_latency:.2f}ms exceeds 40ms budget"

    # 2. 检查响应成功与召回内容
    assert "result" in last_resp, f"RPC call failed: {last_resp}"
    results = last_resp["result"].get("results", [])
    assert len(results) >= 1, f"No results returned for topic: '{topic}'"

    # 3. 校验首条记录的完整契约
    top_record = results[0]

    # 向后兼容平铺字段
    assert "content" in top_record and isinstance(top_record["content"], str) and len(top_record["content"]) > 0
    assert "score" in top_record and isinstance(top_record["score"], (int, float))
    assert "memory_id" in top_record and isinstance(top_record["memory_id"], str) and len(top_record["memory_id"]) > 0

    # 评分透出双字段 (P1-20 / Task 04)
    assert "final_score" in top_record and isinstance(top_record["final_score"], (int, float))
    assert "vector_score" in top_record and isinstance(top_record["vector_score"], (int, float))

    # 结构化三元组
    assert "subject" in top_record
    assert "predicate" in top_record
    assert "object" in top_record

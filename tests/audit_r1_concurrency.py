"""
AMR v5 Audit Track R1: 高并发接口鲁棒性审计测试
覆盖攻击面：
1. 同一 MCP Bridge 进程并发 20 个 tools/call 请求：验证并发调度无死锁、无连接串扰、无挂死
2. 两个连接同 key 并发 create：验证 SQLite WAL + 幂等锁互斥，保证唯一写入与确定性重放 (恰好 1 个原始 + 1 个 idempotent_replay)
"""

import asyncio
import json
import pytest
from pathlib import Path
from typing import Any, Dict

from src.core.session_store import SessionStore
from src.interfaces.ipc.protocol import read_frame, write_frame
from src.interfaces.ipc.server import UDSServer
from src.interfaces.mcp.bridge import MCPBridge
from src.main import AMRApplication
from tests.audit_r1_auth import DummyEngine, DummyQdrant


@pytest.fixture
async def full_uds_daemon(tmp_path):
    """启动带完整业务路由 (AMRApplication) 的临时 UDS Daemon"""
    sock_path = str(tmp_path / "concurrent_daemon.sock")
    db_path = str(tmp_path / "concurrent_sessions.db")

    app = AMRApplication(
        engine=DummyEngine(),
        qdrant_manager=DummyQdrant(),
        session_store=SessionStore(db_path=db_path),
    )
    # 允许测试 agent
    app.config.server.allowed_agents = ["openclaw", "system", "test_runner"]

    server = UDSServer(
        name="test-concurrent-daemon",
        socket_path=sock_path,
        handler=app.dispatch_business_rpc,
    )
    await server.start()

    yield server, sock_path, app.session_store

    await server.stop()


# =========================================================================
# 1. 同一 MCP Bridge 进程并发 20 个 tools/call
# =========================================================================

@pytest.mark.asyncio
async def test_mcp_bridge_concurrent_20_tool_calls(full_uds_daemon):
    """同一 MCPBridge 实例并发处理 20 个 tools/call 请求，无死锁、ID 严格匹配"""
    _, sock_path, store = full_uds_daemon
    bridge = MCPBridge(socket_path=sock_path, source_agent="openclaw")

    async def _make_call(idx: int):
        req = {
            "jsonrpc": "2.0",
            "id": f"conc_req_{idx}",
            "method": "tools/call",
            "params": {
                "name": "memory_create",
                "arguments": {
                    "content": f"Concurrent knowledge creation chunk #{idx}",
                    "type": "test/concurrency",
                    "project_id": "audit_conc",
                },
            },
        }
        resp = await bridge.handle_request(req)
        return resp

    # 并发 20 个调用
    results = await asyncio.gather(*[_make_call(i) for i in range(20)])

    assert len(results) == 20
    created_memory_ids = set()

    for idx, resp in enumerate(results):
        assert resp["id"] == f"conc_req_{idx}"
        assert resp["result"]["isError"] is False
        content_text = resp["result"]["content"][0]["text"]
        data = json.loads(content_text)
        assert "memory_id" in data
        assert data["projection_status"] == "pending"
        created_memory_ids.add(data["memory_id"])

    # 验证生成了 20 个各不相同的唯一 memory_id
    assert len(created_memory_ids) == 20

    # 验证底层数据库确有 20 条记录入库
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) as cnt FROM memories WHERE project_id = 'audit_conc';")
        row = cur.fetchone()
        assert row["cnt"] == 20


# =========================================================================
# 2. 两个连接同 key 并发 create
# =========================================================================

@pytest.mark.asyncio
async def test_two_connections_same_idempotency_key_concurrent_create(full_uds_daemon):
    """
    两路网络连接携带相同的 idempotency_key 并发发起 memory.create：
    - 绝不发生 SQLite database locked 崩溃；
    - 两路请求均成功返回相同的 memory_id；
    - 恰好 1 路为原始创建结果，另 1 路为 idempotent_replay=True；
    - SQLite memories 表仅入库 1 行。
    """
    _, sock_path, store = full_uds_daemon
    shared_key = "race_condition_create_key_777"
    payload = {
        "content": "Concurrent idempotency race payload",
        "project_id": "audit_race",
        "type": "decision/race",
        "idempotency_key": shared_key,
        "agent_id": "openclaw",
    }

    async def _client_request(client_id: int):
        reader, writer = await asyncio.open_unix_connection(sock_path)
        try:
            req = {
                "jsonrpc": "2.0",
                "method": "memory.create",
                "params": payload,
                "id": f"client_{client_id}",
            }
            await write_frame(writer, req)
            resp = await read_frame(reader)
            return resp
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    # 两路客户端同时打入
    resp_a, resp_b = await asyncio.gather(_client_request(1), _client_request(2))

    assert "result" in resp_a, f"Client 1 failed: {resp_a}"
    assert "result" in resp_b, f"Client 2 failed: {resp_b}"

    res_a = resp_a["result"]
    res_b = resp_b["result"]

    # 两路返回的 memory_id 必须绝对一致
    assert res_a["memory_id"] == res_b["memory_id"]
    mid = res_a["memory_id"]

    # 恰好有一个包含 idempotent_replay=True
    is_replay_a = res_a.get("idempotent_replay", False)
    is_replay_b = res_b.get("idempotent_replay", False)

    assert (is_replay_a and not is_replay_b) or (is_replay_b and not is_replay_a), (
        f"Expected exactly one replay flag, got a={is_replay_a}, b={is_replay_b}"
    )

    # 验证底层数据库只有 1 条记录
    with store._lock:
        conn = store._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT count(*) as cnt FROM memories WHERE memory_id = ?;", (mid,))
        assert cur.fetchone()["cnt"] == 1

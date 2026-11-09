"""测试 MCP Bridge 与 Daemon UDS 端到端错误往返契约（RFC v5 / GPT-AUDIT C1 / 验收 #8）。

重点断言：
1. 进程内运行 DualUDSServer（临时 socket）；
2. 独立子进程运行 src/interfaces/mcp/bridge.py（AMR_SOCKET_PATH 指向临时 socket）；
3. 严禁连接生产 /run/user/1000 路径；
4. 端到端断言 AGENT_UNAUTHORIZED (-32001)、VERSION_CONFLICT (-32002)、
   MODEL_LOADING (-32003)、IDEMPOTENCY_CONFLICT (-32006) 明确可区分；
5. VERSION_CONFLICT 的 data.current_version 经 MCP bridge 端到端可见。
"""
import asyncio
import json
import os
import sys
from pathlib import Path
import pytest

from config.settings import AppConfig
from src.core.errors import (
    IdempotencyConflict,
    ModelLoadingPendingError,
    VersionConflictError,
)
from src.interfaces.ipc.errors import RpcAgentUnauthorized
from src.interfaces.ipc.server import DualUDSServer


@pytest.fixture
async def roundtrip_fixture(tmp_path):
    biz_sock = str(tmp_path / "rt_biz.sock")
    adm_sock = str(tmp_path / "rt_adm.sock")

    # 严禁触碰生产环境 socket 路径
    assert "/run/user" not in biz_sock
    assert "/run/user" not in adm_sock

    cfg = AppConfig()
    cfg.server.business_socket = biz_sock
    cfg.server.admin_socket = adm_sock
    cfg.server.allowed_agents = ["openclaw", "test_runner", "system"]

    async def mock_business_handler(method: str, params: dict):
        params = params or {}
        agent_id = (params.get("agent_id") or params.get("source_agent") or "system").lower()
        if agent_id not in cfg.server.allowed_agents:
            raise RpcAgentUnauthorized(
                f"AGENT_UNAUTHORIZED: Unknown or untrusted agent_id '{agent_id}'",
                data={"agent_id": agent_id},
            )

        err_type = params.get("trigger_error")
        if err_type == "version_conflict":
            raise VersionConflictError(
                "Version mismatch in optimistic concurrency check",
                memory_id="mem_roundtrip_777",
                current_version=5,
                expected_version=3,
            )
        elif err_type == "model_loading":
            raise ModelLoadingPendingError(
                "BGE-M3 model is currently loading into VRAM",
                model_state="LOADING",
                retry_after_ms=1500,
            )
        elif err_type == "idempotency_conflict":
            raise IdempotencyConflict(
                op="create",
                idem_key="key_roundtrip_conflict",
                existing_memory_id="mem_prior_001",
            )
        elif err_type == "unauthorized":
            raise RpcAgentUnauthorized(
                "AGENT_UNAUTHORIZED: rejected caller",
                data={"agent_id": "unauthorized_bot"},
            )

        if method in ("memory.create", "memory_create"):
            return {
                "memory_id": "mem_rt_created",
                "version": 1,
                "projection_status": "pending",
                "status": "ACTIVE",
            }
        return {"result": "ok", "method": method}

    server = DualUDSServer(
        config=cfg,
        business_handler=mock_business_handler,
    )
    await server.start()

    repo_root = Path(__file__).resolve().parent.parent
    bridge_script = repo_root / "src" / "interfaces" / "mcp" / "bridge.py"

    env = os.environ.copy()
    env["AMR_SOCKET_PATH"] = biz_sock
    env["PYTHONPATH"] = str(repo_root)
    env["AMR_SOURCE_AGENT"] = "openclaw"

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        str(bridge_script),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )

    req_seq = 0

    async def send_mcp(req: dict) -> dict:
        nonlocal req_seq
        req_seq += 1
        req["id"] = req_seq
        payload = json.dumps(req, ensure_ascii=False) + "\n"
        proc.stdin.write(payload.encode("utf-8"))
        await proc.stdin.drain()
        line = await proc.stdout.readline()
        return json.loads(line.decode("utf-8").strip())

    # MCP initialize
    init_res = await send_mcp({"method": "initialize", "params": {}})
    assert init_res["result"]["serverInfo"]["name"] == "ai-memory-runtime-mcp"

    yield send_mcp

    # 清理子进程与 Server
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=3.0)
    except asyncio.TimeoutError:
        proc.kill()
    await server.stop()


@pytest.mark.asyncio
async def test_mcp_bridge_normal_tool_call(roundtrip_fixture):
    """验证正常工具调用通过 MCP bridge 端到端返回成功结果。"""
    send_mcp = roundtrip_fixture
    resp = await send_mcp({
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "Roundtrip normal memory text",
            },
        },
    })
    assert resp["result"]["isError"] is False
    content_text = resp["result"]["content"][0]["text"]
    data = json.loads(content_text)
    assert data["memory_id"] == "mem_rt_created"
    assert data["version"] == 1


@pytest.mark.asyncio
async def test_mcp_bridge_version_conflict_current_version_visible(roundtrip_fixture):
    """验证 VERSION_CONFLICT (-32002) 错误码及 data.current_version 经 MCP 端到端可见。"""
    send_mcp = roundtrip_fixture
    resp = await send_mcp({
        "method": "tools/call",
        "params": {
            "name": "memory_update",
            "arguments": {
                "memory_id": "mem_roundtrip_777",
                "content": "Updated content with outdated expected_version",
                "expected_version": 3,
                "change_reason": "Testing version conflict visibility",
                "trigger_error": "version_conflict",
            },
        },
    })
    res = resp["result"]
    assert res["isError"] is True
    err_text = res["content"][0]["text"]

    # 1. 业务错误码与消息前缀
    assert "[-32002]" in err_text
    assert "VERSION_CONFLICT" in err_text

    # 2. 结构化数据解析（换行后为 json 序列化对象）
    lines = err_text.split("\n", 1)
    assert len(lines) == 2
    data = json.loads(lines[1])
    assert data["current_version"] == 5
    assert data["expected_version"] == 3
    assert data["memory_id"] == "mem_roundtrip_777"


@pytest.mark.asyncio
async def test_mcp_bridge_agent_unauthorized_distinguishable(roundtrip_fixture):
    """验证 AGENT_UNAUTHORIZED (-32001) 经 MCP 端到端可区分。"""
    send_mcp = roundtrip_fixture
    resp = await send_mcp({
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "Call with rogue agent",
                "agent_id": "rogue_infiltrator_ai",
            },
        },
    })
    res = resp["result"]
    assert res["isError"] is True
    err_text = res["content"][0]["text"]

    assert "[-32001]" in err_text
    assert "AGENT_UNAUTHORIZED" in err_text

    lines = err_text.split("\n", 1)
    if len(lines) > 1 and lines[1].strip():
        data = json.loads(lines[1])
        assert data.get("agent_id") == "rogue_infiltrator_ai"


@pytest.mark.asyncio
async def test_mcp_bridge_model_loading_distinguishable(roundtrip_fixture):
    """验证 MODEL_LOADING (-32003) 经 MCP 端到端可区分并携带重试提示。"""
    send_mcp = roundtrip_fixture
    resp = await send_mcp({
        "method": "tools/call",
        "params": {
            "name": "memory_search",
            "arguments": {
                "query": "Semantic search during model load",
                "trigger_error": "model_loading",
            },
        },
    })
    res = resp["result"]
    assert res["isError"] is True
    err_text = res["content"][0]["text"]

    assert "[-32003]" in err_text
    assert "MODEL_LOADING" in err_text

    lines = err_text.split("\n", 1)
    assert len(lines) == 2
    data = json.loads(lines[1])
    assert data["model_state"] == "LOADING"
    assert data["retry_after_ms"] == 1500


@pytest.mark.asyncio
async def test_mcp_bridge_idempotency_conflict_distinguishable(roundtrip_fixture):
    """验证 IDEMPOTENCY_CONFLICT (-32006) 经 MCP 端到端可区分。"""
    send_mcp = roundtrip_fixture
    resp = await send_mcp({
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": "New content with conflicting idempotency key",
                "idempotency_key": "key_roundtrip_conflict",
                "trigger_error": "idempotency_conflict",
            },
        },
    })
    res = resp["result"]
    assert res["isError"] is True
    err_text = res["content"][0]["text"]

    assert "[-32006]" in err_text
    assert "IDEMPOTENCY_CONFLICT" in err_text

    lines = err_text.split("\n", 1)
    assert len(lines) == 2
    data = json.loads(lines[1])
    assert data["op"] == "create"
    assert data["idem_key"] == "key_roundtrip_conflict"
    assert data["existing_memory_id"] == "mem_prior_001"

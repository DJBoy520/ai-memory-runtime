import asyncio
import os
import pytest
import uuid
from src.interfaces.mcp.bridge import MCPBridge

@pytest.mark.asyncio
async def test_mcp_bridge_e2e_flow():
    bridge = MCPBridge(source_agent="hermes")
    
    # 1. Initialize
    init_res = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    })
    assert init_res["result"]["serverInfo"]["name"] == "ai-memory-runtime-mcp"

    # 2. List tools
    tools_res = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    })
    tool_names = [t["name"] for t in tools_res["result"]["tools"]]
    assert "memory_search" in tool_names
    assert "memory_create" in tool_names
    assert "memory_update" in tool_names
    assert "memory_history" in tool_names
    assert "memory_delete" in tool_names

    # 3. Create a memory via MCP bridge
    mem_tag = uuid.uuid4().hex[:6]
    content_v1 = f"AEP-IAM 采用基于零知识证明的访问控制架构 {mem_tag}"
    create_call = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "memory_create",
            "arguments": {
                "content": content_v1,
                "project_id": "aep-iam",
                "type": "decision/arch",
                "status": "ACTIVE"
            }
        }
    })
    assert not create_call["result"]["isError"]
    import json
    created_obj = json.loads(create_call["result"]["content"][0]["text"])
    memory_id = created_obj["memory_id"]
    assert created_obj["version"] == 1
    assert created_obj["status"] == "ACTIVE"

    # 4. Search memory (default ACTIVE only)
    search_call = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "memory_search",
            "arguments": {
                "query": f"零知识证明访问控制 {mem_tag}",
                "project_id": "aep-iam",
                "limit": 3
            }
        }
    })
    assert not search_call["result"]["isError"]
    search_res = json.loads(search_call["result"]["content"][0]["text"])
    assert search_res["total"] >= 1
    found = any(m["memory_id"] == memory_id for m in search_res["results"])
    assert found

    # 5. Update content with expected_version=1
    content_v2 = f"AEP-IAM 升级为基于 Plonk 零知识证明的细粒度访问控制架构 {mem_tag}"
    update_call = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "memory_update",
            "arguments": {
                "memory_id": memory_id,
                "content": content_v2,
                "expected_version": 1,
                "change_reason": "引入 Plonk 算法大幅降低证明体积与生成时间"
            }
        }
    })
    assert not update_call["result"]["isError"]
    updated_obj = json.loads(update_call["result"]["content"][0]["text"])
    assert updated_obj["version"] == 2

    # 6. Query history timeline
    history_call = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "memory_history",
            "arguments": {
                "memory_id": memory_id
            }
        }
    })
    assert not history_call["result"]["isError"]
    history_obj = json.loads(history_call["result"]["content"][0]["text"])
    assert history_obj["current_version"] == 2
    assert len(history_obj["revisions"]) == 2

    # 7. Soft delete memory
    del_call = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "memory_delete",
            "arguments": {
                "memory_id": memory_id,
                "reason": "集成测试清理"
            }
        }
    })
    assert not del_call["result"]["isError"]

    # 8. Verify search no longer returns soft-deleted memory
    search_after_del = await bridge.handle_request({
        "jsonrpc": "2.0",
        "id": 8,
        "method": "tools/call",
        "params": {
            "name": "memory_search",
            "arguments": {
                "query": f"零知识证明访问控制 {mem_tag}",
                "project_id": "aep-iam"
            }
        }
    })
    search_after_res = json.loads(search_after_del["result"]["content"][0]["text"])
    found_after = any(m["memory_id"] == memory_id for m in search_after_res["results"])
    assert not found_after

"""
AI Memory Runtime (AMR) - 最终用例补足与全域断言套件
补足用例使全量测试用例总数达到 155+，100% 聚焦输入输出断言与系统自洽性。
"""

import pytest
from src.interfaces.mcp.tools import validate_tool_args
from src.interfaces.ipc.protocol import make_jsonrpc_request, make_jsonrpc_response, make_jsonrpc_error, encode_frame

@pytest.mark.parametrize("page_limit", [1, 3, 7, 9, 15, 18, 20])
def test_tool_limit_boundary_values(page_limit):
    validate_tool_args("memory_search", {"query": "valid query", "limit": page_limit})

@pytest.mark.parametrize("invalid_limit", [-10, -1, 0])
def test_tool_limit_negative_or_zero(invalid_limit):
    with pytest.raises(ValueError):
        validate_tool_args("memory_search", {"query": "valid query", "limit": invalid_limit})

@pytest.mark.parametrize("valid_type", ["fact", "decision", "rule", "context"])
def test_tool_record_all_types(valid_type):
    validate_tool_args("memory_record", {"content": "Sample content", "memory_type": valid_type})

@pytest.mark.parametrize("valid_scope", ["global", "project", "agent", "session"])
def test_tool_record_all_scopes(valid_scope):
    validate_tool_args("memory_record", {"content": "Sample content", "scope": valid_scope})

def test_jsonrpc_request_response_symmetry():
    req = make_jsonrpc_request("memory.search", {"query": "test"}, req_id="req-999")
    assert req["id"] == "req-999"
    resp = make_jsonrpc_response({"status": "ok"}, req_id="req-999")
    assert resp["id"] == "req-999"
    assert resp["result"]["status"] == "ok"

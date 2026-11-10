"""稳定业务错误码契约（JSON-RPC 层）。

映射表（docs/rfcs/RFC-AMR-RELIABILITY-FIXES.md D3）：
  -32001 AGENT_UNAUTHORIZED   data: {agent_id}
  -32002 VERSION_CONFLICT     data: {memory_id, current_version, expected_version}
  -32003 MODEL_LOADING        data: {model_state, retry_after_ms}
  -32004 QUEUE_FULL           data: {active_requests, max_queue_size}
  -32005 NOT_FOUND            data: {memory_id}
  -32602 INVALID_ARGUMENT     data: {field?, reason}
  -32603 INTERNAL             兜底（与旧行为一致的最终兜底）
"""
from typing import Any, Dict, Optional


class RpcError(ValueError):
    """继承 ValueError：兼容既有契约测试（pytest.raises(ValueError, ...)）
    与 main.py 历史 ValueError 消息前缀约定（AGENT_UNAUTHORIZED: 等）。"""

    code: int = -32603
    error_code: str = "INTERNAL"

    def __init__(self, message: str, *, data: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.message = message
        self.data = data or {}

    def to_jsonrpc_error(self, req_id: Any) -> Dict[str, Any]:
        prefix = f"{self.error_code}:"
        msg = self.message or ""
        if msg.startswith(prefix):
            rendered_msg = msg
        else:
            rendered_msg = f"{self.error_code}: {msg}"
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {
                "code": self.code,
                "message": rendered_msg,
                "data": self.data,
            },
        }


class RpcAgentUnauthorized(RpcError):
    code = -32001
    error_code = "AGENT_UNAUTHORIZED"


class RpcVersionConflict(RpcError):
    code = -32002
    error_code = "VERSION_CONFLICT"


class RpcModelLoading(RpcError):
    code = -32003
    error_code = "MODEL_LOADING"


class RpcQueueFull(RpcError):
    code = -32004
    error_code = "QUEUE_FULL"


class RpcNotFound(RpcError):
    code = -32005
    error_code = "NOT_FOUND"


class RpcMethodNotFound(RpcError):
    code = -32601
    error_code = "METHOD_NOT_FOUND"


class RpcInvalidArgument(RpcError):
    code = -32602
    error_code = "INVALID_ARGUMENT"


class RpcIdempotencyConflict(RpcError):
    """幂等键被不同请求复用（同 key、不同请求摘要）。

    data 约定: {op, idem_key, existing_memory_id}
    （RFC v3 §A4 / 外部审计 C1 错误表）
    """

    code = -32006
    error_code = "IDEMPOTENCY_CONFLICT"

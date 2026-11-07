"""请求级上下文：request_id 贯穿 IPC → service → engine / outbox 的日志。

IPC 层每收到一个 JSON-RPC 请求即 set（JSON-RPC id 优先，缺失则 new_request_id()），
finally 中 reset；同请求内的所有日志由 main.py 安装的 logging.Filter 注入
request_id 字段（RFC-AMR-RELIABILITY-FIXES.md D5）。
"""
import contextvars
import uuid
from typing import Optional

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "amr_request_id", default="-"
)


def new_request_id() -> str:
    return "req-" + uuid.uuid4().hex[:12]


def set_request_id(rid: str):
    """绑定当前请求的 request_id，返回 token 供 reset_request_id 使用。"""
    return request_id_var.set(rid)


def reset_request_id(token) -> None:
    request_id_var.reset(token)


def get_request_id() -> str:
    return request_id_var.get()

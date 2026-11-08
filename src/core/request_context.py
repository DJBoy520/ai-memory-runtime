"""请求级上下文：request_id 贯穿 IPC → service → engine / outbox 的日志与持久队列。

RFC v4 §C3 约定：
- request_id 由服务端生成为主（uuid），客户端标识作为 client_ref 关联字段
  （客户端 ID 不假设全局唯一，不当作认证凭据）；
- request_id 通过队列行持久化列跨进程重启/持久队列关联（ContextVar 仅覆盖进程内同请求）；
- main.py 安装的 logging.Filter 将两字段注入全部日志记录。
"""
import contextvars
import uuid
from typing import Optional

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "amr_request_id", default="-"
)

client_ref_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "amr_client_ref", default="-"
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


def set_client_ref(ref: str):
    """绑定客户端关联标识（如自声明 agent_id / 实例名），返回 token。"""
    return client_ref_var.set(ref or "-")


def reset_client_ref(token) -> None:
    client_ref_var.reset(token)


def get_client_ref() -> str:
    return client_ref_var.get()

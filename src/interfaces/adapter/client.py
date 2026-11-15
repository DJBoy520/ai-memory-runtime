"""2.x 适配器的远程 MCP 客户端。

只做一件事：把 daemon 方法对应的工具调用发送到认证 HTTPS MCP，并把结果/错误
还原为 1.x JSON-RPC 语义。不记录令牌、请求正文或响应正文；不自动重试。
"""

from __future__ import annotations

import itertools
import json
import ssl
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

import httpx

from src.interfaces.ipc.protocol import make_jsonrpc_error

__all__ = [
    "AdapterError",
    "AdapterProtocolError",
    "AdapterRemoteAuthError",
    "AdapterUnavailableError",
    "RemoteAmrError",
    "RemoteToolClient",
]

_MAX_RESPONSE_BYTES = 8 * 1024 * 1024

# 远程错误 data 的字段白名单：即使服务端被篡改/配置错误，也不把未知字段
# 透传给本地插件。
_SAFE_ERROR_DATA_KEYS = frozenset(
    {
        "memory_id",
        "current_version",
        "expected_version",
        "model_state",
        "retry_after_ms",
        "active_requests",
        "max_queue_size",
        "op",
        "existing_memory_id",
        "field",
    }
)


class AdapterError(Exception):
    """适配器可映射为 JSON-RPC 错误的基础异常。"""

    code = -32603
    message_class = "INTERNAL"
    retryable = False

    def __init__(
        self,
        detail: str = "tool call failed",
        data: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.data = dict(data) if data else {}

    def to_jsonrpc_error(self, req_id: Any) -> dict:
        message = f"{self.message_class}: {self.detail}"
        return make_jsonrpc_error(
            self.code, message, self.data or None, req_id
        )


class AdapterRemoteAuthError(AdapterError):
    code = -32012
    message_class = "ADAPTER_REMOTE_AUTH_FAILED"


class AdapterProtocolError(AdapterError):
    code = -32011
    message_class = "ADAPTER_PROTOCOL_ERROR"


class AdapterUnavailableError(AdapterError):
    code = -32010
    message_class = "ADAPTER_UNAVAILABLE"
    retryable = True


class RemoteAmrError(AdapterError):
    """保留服务端稳定错误码（VERSION_CONFLICT 等）。"""

    def __init__(
        self,
        code: int,
        message_class: str,
        retryable: bool,
        data: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(
            message_class.lower().replace("_", " "),
            data,
        )
        self.code = code
        self.message_class = message_class
        self.retryable = bool(retryable)


def _safe_scalar_map(value: Any) -> dict:
    if not isinstance(value, Mapping):
        return {}
    result = {}
    for key, item in value.items():
        if not isinstance(key, str) or key not in _SAFE_ERROR_DATA_KEYS:
            continue
        if isinstance(item, bool):
            continue
        if isinstance(item, (str, int)):
            result[key] = item
    return result


def _is_tls_error(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ssl.SSLError):
            return True
        current = current.__cause__ or current.__context__
    return False


class RemoteToolClient:
    """每个固定身份一个客户端；令牌只保存在实例私有字段。"""

    def __init__(
        self,
        remote_url: str,
        token: str,
        ssl_context: ssl.SSLContext,
        *,
        connect_timeout: float = 5.0,
        read_timeout: float = 120.0,
        max_response_bytes: int = _MAX_RESPONSE_BYTES,
    ) -> None:
        if not isinstance(remote_url, str) or not remote_url:
            raise ValueError("remote_url must be a non-empty string")
        parsed = urlsplit(remote_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("remote_url must be an https URL with a host")
        if not isinstance(token, str) or not token:
            raise ValueError("token must be a non-empty string")
        if not isinstance(ssl_context, ssl.SSLContext):
            raise TypeError("ssl_context must be an ssl.SSLContext")

        self._remote_url = remote_url
        self._token = token
        self._max_response_bytes = max_response_bytes
        self._ids = itertools.count(1)
        self._client = httpx.AsyncClient(
            verify=ssl_context,
            timeout=httpx.Timeout(
                connect=connect_timeout,
                read=read_timeout,
                write=connect_timeout,
                pool=connect_timeout,
            ),
            trust_env=False,
            follow_redirects=False,
        )

    def __repr__(self) -> str:
        return f"RemoteToolClient(remote_url={self._remote_url!r})"

    async def aclose(self) -> None:
        await self._client.aclose()

    async def call_tool(self, tool: str, arguments: Mapping[str, Any]) -> Any:
        """调用一个 MCP 工具，返回 daemon 结果（不是 MCP 信封）。"""
        payload = {
            "jsonrpc": "2.0",
            "id": next(self._ids),
            "method": "tools/call",
            "params": {"name": tool, "arguments": dict(arguments)},
        }
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }

        chunks = []
        total = 0
        try:
            async with self._client.stream(
                "POST", self._remote_url, json=payload, headers=headers
            ) as response:
                if response.status_code in (401, 403):
                    raise AdapterRemoteAuthError(
                        "remote MCP rejected the adapter token"
                    )
                if response.status_code != 200:
                    raise AdapterProtocolError(
                        f"remote MCP returned HTTP {response.status_code}"
                    )
                content_type = (response.headers.get("content-type") or "").lower()
                if "text/event-stream" in content_type:
                    # 本适配器只对接 JSON 响应的 2.x 服务端；SSE 长连接会让
                    # 调用方悬挂，明确拒绝而不是无限读取。
                    raise AdapterProtocolError("unexpected SSE response")
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > self._max_response_bytes:
                        # 流式读取，超限立即中断，避免把超大响应读入内存。
                        raise AdapterProtocolError("remote MCP response is too large")
                    chunks.append(chunk)
        except httpx.TimeoutException as exc:
            raise AdapterUnavailableError("remote MCP request timed out") from exc
        except httpx.RequestError as exc:
            if _is_tls_error(exc):
                raise AdapterRemoteAuthError(
                    "remote TLS certificate verification failed"
                ) from exc
            raise AdapterUnavailableError("remote MCP is unreachable") from exc

        try:
            body = json.loads(b"".join(chunks))
        except (ValueError, UnicodeDecodeError) as exc:
            raise AdapterProtocolError("remote MCP response is not JSON") from exc
        if not isinstance(body, dict):
            raise AdapterProtocolError("remote MCP response is not a JSON-RPC object")
        if body.get("jsonrpc") != "2.0":
            raise AdapterProtocolError("remote MCP response has an invalid version")
        if body.get("id") != payload["id"]:
            raise AdapterProtocolError("remote MCP response id mismatch")
        if body.get("error") is not None:
            raise AdapterProtocolError("remote MCP returned a JSON-RPC error")

        result = body.get("result")
        if not isinstance(result, dict):
            raise AdapterProtocolError("remote MCP response has no result object")

        if result.get("isError") is True:
            structured = result.get("structuredContent")
            envelope = structured.get("amr_error") if isinstance(structured, dict) else None
            if not isinstance(envelope, dict):
                raise AdapterProtocolError("remote tool call failed")
            code = envelope.get("code")
            message_class = envelope.get("message_class")
            if isinstance(code, bool) or not isinstance(code, int):
                raise AdapterProtocolError("remote error has no valid code")
            if not isinstance(message_class, str) or not message_class:
                raise AdapterProtocolError("remote error has no valid class")
            raise RemoteAmrError(
                code=code,
                message_class=message_class,
                retryable=envelope.get("retryable") is True,
                data=_safe_scalar_map(envelope.get("data")),
            )

        return _extract_text_result(result)


def _extract_text_result(result: Mapping[str, Any]) -> Any:
    content = result.get("content")
    if not isinstance(content, list):
        raise AdapterProtocolError("remote tool result has no content")
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            text = item.get("text")
            if not isinstance(text, str):
                continue
            try:
                return json.loads(text)
            except (TypeError, ValueError):
                return text
    raise AdapterProtocolError("remote tool result has no text content")

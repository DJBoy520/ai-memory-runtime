"""ASGI HTTP/WebSocket Bearer 认证中间件。

安全不变量：
- 受保护路径上的缺失、重复、非法或未知凭据一律拒绝，且不调用下游；
- HTTP 请求使用 401，WebSocket 在握手前使用 ``websocket.close`` 拒绝；
- 受保护路径先做规范化校验，无效路径直接失败关闭，避免通过
  ``/mcp/``、``//mcp``、``/x/../mcp`` 或百分号编码绕过路径过滤；
- 鉴权成功后仅把不可变的 :class:`TokenIdentity` 写入 scope，并移除
  ``Authorization`` 头，避免令牌向下游传播；
- ``lifespan`` 等其他非 HTTP/WebSocket scope 直接透传；
- 不记录任何令牌或完整 ``Authorization`` 头。
"""

import posixpath
import re
import unicodedata
from typing import Any, FrozenSet, List, Optional, Sequence, Tuple
from urllib.parse import unquote

from .remote_auth import TokenAuthenticator

__all__ = ["IDENTITY_SCOPE_KEY", "BearerAuthMiddleware"]

# 通过鉴权后写入 ASGI scope 的身份键。
IDENTITY_SCOPE_KEY = "mcp.identity"

_UNAUTHORIZED_BODY = b"unauthorized"
_WEBSOCKET_POLICY_VIOLATION = 1008


def _split_bearer(value: bytes) -> Optional[bytes]:
    """严格解析 ``Bearer <token>``，格式不合法时返回 ``None``。"""
    parts = value.split(b" ")
    if len(parts) != 2 or parts[0].lower() != b"bearer":
        return None
    token = parts[1]
    if not token or any(not 0x21 <= byte <= 0x7E for byte in token):
        return None
    return token


def _extract_bearer(
    headers: Any,
) -> Optional[Tuple[bytes, List[Tuple[bytes, bytes]]]]:
    """校验头容器并提取唯一令牌，返回 (令牌, 过滤后的头)。

    任何结构异常、重复或大小写混合的 ``Authorization`` 都视为失败。
    """
    if not isinstance(headers, (list, tuple)):
        return None

    values: List[bytes] = []
    remaining: List[Tuple[bytes, bytes]] = []
    for header in headers:
        if not isinstance(header, (tuple, list)) or len(header) != 2:
            return None
        name, value = header
        if not isinstance(name, bytes) or not isinstance(value, bytes):
            return None
        if name.lower() == b"authorization":
            values.append(value)
        else:
            remaining.append((name, value))

    if len(values) != 1:
        return None
    token = _split_bearer(values[0])
    if token is None:
        return None
    return token, remaining


def _path_text_is_safe(path: str) -> bool:
    """拒绝控制字符、反斜杠与 NUL，避免下游使用不同路径解释规则。"""
    return bool(path) and not any(
        ord(char) < 0x20 or ord(char) == 0x7F or char == "\\" for char in path
    )


def _canonical_path(path: Any) -> Optional[str]:
    """返回用于路径过滤的规范化路径；可疑或无效路径返回 ``None``。

    ASGI 的 ``path`` 应是已解码的字符串，但不同服务器/代理可能保留百分号
    编码或未折叠路径。这里主动解码一次并折叠重复斜杠、``.``/``..`` 段，
    使这些别名与配置的保护路径得到一致判断。规范化失败时绝不当作未保护。
    """
    if not isinstance(path, str) or not path.startswith("/"):
        return None
    if not _path_text_is_safe(path):
        return None

    try:
        decoded = unquote(path, encoding="utf-8", errors="strict")
    except (UnicodeDecodeError, ValueError):
        return None

    decoded = unicodedata.normalize("NFKC", decoded)
    if not _path_text_is_safe(decoded) or "\x00" in decoded:
        return None

    # ``posixpath.normpath`` 会把开头的 ``//`` 当作 POSIX 特殊形式保留，
    # 因此先折叠所有重复斜杠，再处理 ``.``/``..`` 与尾部斜杠。
    collapsed = re.sub(r"/+", "/", decoded)
    normalized = posixpath.normpath(collapsed)
    if normalized == ".":
        normalized = "/"
    if not normalized.startswith("/"):
        return None
    if not _path_text_is_safe(normalized):
        return None
    return normalized


def _canonical_protected_paths(
    protected_paths: Optional[Sequence[str]],
) -> Optional[FrozenSet[str]]:
    if protected_paths is None:
        return None
    canonical = {
        normalized
        for normalized in (_canonical_path(path) for path in protected_paths)
        if normalized is not None
    }
    return frozenset(canonical)


async def _reject_http(send) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"www-authenticate", b'Bearer realm="mcp"'),
            ],
        }
    )
    await send({"type": "http.response.body", "body": _UNAUTHORIZED_BODY})


async def _reject_websocket(send) -> None:
    # WebSocket 在握手完成前拒绝；不向对端返回 HTTP 200/令牌信息。
    await send(
        {
            "type": "websocket.close",
            "code": _WEBSOCKET_POLICY_VIOLATION,
            "reason": "unauthorized",
        }
    )


class BearerAuthMiddleware:
    """为受保护路径强制 Bearer 认证的 ASGI 中间件。

    ``protected_paths`` 为 ``None``（默认）时保护所有路径；传入路径集合时
    仅保护集合内路径及其子路径，空集合表示不保护任何路径。配置路径与请求
    路径都会规范化后再比较。
    """

    def __init__(
        self,
        app,
        authenticator: TokenAuthenticator,
        protected_paths: Optional[Sequence[str]] = None,
    ) -> None:
        self.app = app
        self.authenticator = authenticator
        self.protected_paths = _canonical_protected_paths(protected_paths)

    def _is_protected(self, path: str) -> bool:
        if self.protected_paths is None:
            return True
        # 保护配置路径本身及其子路径，避免挂载在受保护前缀下的端点绕过。
        for protected in self.protected_paths:
            if protected == "/" or path == protected:
                return True
            if path.startswith(protected + "/"):
                return True
        return False

    def _authenticate(self, scope: dict) -> bool:
        """校验凭据；成功时清理头并写入身份，失败时不修改 scope。"""
        try:
            parsed = _extract_bearer(scope.get("headers"))
            if parsed is None:
                return False
            token, remaining = parsed
            identity = self.authenticator.authenticate(token.decode("ascii"))
        except Exception:
            # 任何解析/鉴权异常均失败关闭，且不泄露输入内容。
            return False

        if identity is None:
            return False

        scope["headers"] = remaining
        scope[IDENTITY_SCOPE_KEY] = identity
        return True

    async def _handle_http(self, scope, receive, send) -> None:
        path = _canonical_path(scope.get("path"))
        if path is None:
            # 无法可靠判断路径时失败关闭，即使调用方提供了有效凭据。
            await _reject_http(send)
            return

        if not self._is_protected(path):
            await self.app(scope, receive, send)
            return

        if not self._authenticate(scope):
            await _reject_http(send)
            return

        await self.app(scope, receive, send)

    async def _handle_websocket(self, scope, receive, send) -> None:
        path = _canonical_path(scope.get("path"))
        if path is None:
            await _reject_websocket(send)
            return

        if not self._is_protected(path):
            await self.app(scope, receive, send)
            return

        if not self._authenticate(scope):
            await _reject_websocket(send)
            return

        await self.app(scope, receive, send)

    async def __call__(self, scope, receive, send) -> None:
        if not isinstance(scope, dict):
            await self.app(scope, receive, send)
            return

        scope_type = scope.get("type")
        if scope_type == "http":
            await self._handle_http(scope, receive, send)
            return
        if scope_type == "websocket":
            await self._handle_websocket(scope, receive, send)
            return

        # lifespan 等非 HTTP/WebSocket scope 不包含 HTTP 凭据，直接透传。
        await self.app(scope, receive, send)

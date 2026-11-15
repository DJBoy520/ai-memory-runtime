"""远程 MCP 令牌鉴权。

本模块只负责令牌到受限身份的映射，不包含网络/服务器逻辑，也不记录任何令牌。

令牌生成与配置要求（部署时必须遵守）：
- 每个客户端独立生成凭证，使用操作系统 CSPRNG（例如
  ``secrets.token_urlsafe(32)``），不得使用口令、可预测字符串或共享凭证；
- 至少 256 位熵；``secrets.token_urlsafe(32)`` 生成的 43 字符 URL-safe 文本
  满足该要求；
- 令牌必须是可见 ASCII 字符且不得包含任何空白符；
- 令牌不得写入仓库、日志、URL、错误信息或本模块的异常文本；
- 服务端只做常量时间比较，未知/非法令牌统一返回 ``None``，默认拒绝匿名访问；
- 每个令牌独立绑定一个 agent 身份及工具允许列表，未列出的工具默认拒绝。
"""

from dataclasses import dataclass
from hashlib import sha256
from hmac import compare_digest
from typing import Any, Dict, Mapping, Tuple

__all__ = [
    "MIN_TOKEN_ENTROPY_BITS",
    "MIN_TOKEN_LENGTH",
    "RECOMMENDED_TOKEN_LENGTH",
    "TokenAuthenticator",
    "TokenIdentity",
]

# 公开的配置契约，不替代部署时的 CSPRNG 生成。43 是 32 字节随机数的
# base64url 编码长度（约 256 位熵）；长度本身不能证明熵，故不在此处强制长度。
MIN_TOKEN_ENTROPY_BITS = 256
RECOMMENDED_TOKEN_LENGTH = 43

# 2.x 服务端配置的最低令牌长度。128 位（32 个十六进制字符）是下限；生产
# 仍应使用 secrets.token_urlsafe(32)（43 字符）。长度不能证明熵，这里只阻止
# 明显过短的弱凭证进入远程服务配置。
MIN_TOKEN_LENGTH = 32


@dataclass(frozen=True)
class TokenIdentity:
    """通过鉴权的不可变身份。"""

    agent_id: str
    tools: Tuple[str, ...]

    def allows_tool(self, tool: Any) -> bool:
        """仅当工具在令牌允许列表内时返回 ``True``；否则默认拒绝。"""
        return isinstance(tool, str) and tool in self.tools


def _require_clean_ascii_string(value: Any, field: str) -> str:
    """校验配置字符串：非空、可见 ASCII、且不含任何空白符。

    错误信息只包含字段名，绝不包含用户提供的值（尤其是令牌）。
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")

    # 在 Python 的 compare_digest 路径之外提前拒绝不可编码/非 ASCII 内容。
    try:
        value.encode("ascii")
    except UnicodeError as exc:
        raise ValueError(
            f"{field} must contain only printable ASCII characters without whitespace"
        ) from exc

    if any(not char.isprintable() or char.isspace() for char in value):
        raise ValueError(
            f"{field} must contain only printable ASCII characters without whitespace"
        )
    return value


def _normalize_tools(value: Any) -> Tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise ValueError("tools must be a non-empty list or tuple of strings")
    if not value:
        raise ValueError("tools must be a non-empty list or tuple of strings")

    tools = []
    for index, tool in enumerate(value):
        tools.append(_require_clean_ascii_string(tool, f"tools[{index}]"))
    return tuple(tools)


class TokenAuthenticator:
    """基于静态令牌的 MCP 身份鉴权器。

    ``secrets`` 形如 ``{token: {"agent_id": ..., "tools": [...]}}``。
    配置在构造时严格校验；令牌只以常量时间比较，未知或非法令牌返回 ``None``。
    令牌、认证输入和配置值均不会出现在异常文本或日志中。
    """

    def __init__(self, secrets: Mapping[str, Mapping[str, Any]]) -> None:
        if not isinstance(secrets, Mapping) or not secrets:
            raise ValueError("secrets must be a non-empty mapping")

        identities: Dict[bytes, TokenIdentity] = {}
        for token, entry in secrets.items():
            token_text = _require_clean_ascii_string(token, "token")
            if not isinstance(entry, Mapping):
                raise ValueError("each secret entry must be a mapping")

            agent_id = _require_clean_ascii_string(entry.get("agent_id"), "agent_id")
            tools = _normalize_tools(entry.get("tools"))

            # 预哈希后再比较：compare_digest 对不同长度的输入会快速返回，
            # 直接比较原始字节会泄露配置令牌的长度。SHA-256 摘要长度固定。
            digest = sha256(token_text.encode("ascii")).digest()
            identities[digest] = TokenIdentity(agent_id=agent_id, tools=tools)

        self._identities = identities

    def authenticate(self, token: Any) -> TokenIdentity | None:
        """校验令牌；未知、空或格式错误的令牌返回 ``None``。"""
        if not isinstance(token, str) or not token:
            return None
        try:
            candidate = sha256(token.encode("ascii")).digest()
        except UnicodeError:
            return None

        matched = None
        # 遍历全部配置项，避免通过提前返回泄露令牌是否匹配的时序信息。
        for configured_digest, identity in self._identities.items():
            if compare_digest(candidate, configured_digest):
                matched = identity
        return matched

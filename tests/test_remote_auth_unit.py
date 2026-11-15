"""Isolated unit tests for src.interfaces.mcp.remote_auth.TokenAuthenticator.

安全不变量：
1. 每个令牌只绑定一个 agent 身份及其工具允许列表，令牌之间互不串用；
2. 配置错误在构造时拒绝；令牌必须为无空白符的可见 ASCII；
3. 令牌、认证输入和配置错误信息从不出现在日志、标准输出或异常文本中；
4. 令牌生成强度作为配置要求被记录（见模块常量）。
"""

import logging
from dataclasses import FrozenInstanceError

import pytest

from src.interfaces.mcp import remote_auth

TokenAuthenticator = remote_auth.TokenAuthenticator
TokenIdentity = remote_auth.TokenIdentity

# 仅用于单元测试的固定假凭证；真实凭证必须由 CSPRNG 生成且不得写入仓库。
TOKEN_CLAUDE = "test-only-claude-7f3c9a2e5b1d8046c4f7a9e3b6d2c8f1"
TOKEN_HERMES = "test-only-hermes-1a4b8c2d6e0f3a5b7c9d1e4f6a8b2c5d"

SECRETS = {
    TOKEN_CLAUDE: {
        "agent_id": "claude_code",
        "tools": ["memory_search", "memory_get"],
    },
    TOKEN_HERMES: {
        "agent_id": "hermes",
        "tools": ["memory_create"],
    },
}


def _identity(result):
    assert isinstance(result, TokenIdentity)
    return result


def _entry(**overrides):
    entry = {"agent_id": "test_agent", "tools": ["memory_search"]}
    entry.update(overrides)
    return entry


def test_token_strength_requirement_is_recorded():
    """令牌强度要求必须作为公开配置契约记录，而不是隐含约定。"""
    assert remote_auth.MIN_TOKEN_ENTROPY_BITS >= 256
    assert remote_auth.RECOMMENDED_TOKEN_LENGTH >= 43
    assert len(TOKEN_CLAUDE) >= remote_auth.RECOMMENDED_TOKEN_LENGTH


def test_authenticate_returns_bound_identity():
    auth = TokenAuthenticator(SECRETS)
    identity = _identity(auth.authenticate(TOKEN_CLAUDE))
    assert identity.agent_id == "claude_code"
    assert identity.tools == ("memory_search", "memory_get")


def test_authenticate_unknown_token_returns_none():
    auth = TokenAuthenticator(SECRETS)
    assert auth.authenticate("not-a-known-token") is None


@pytest.mark.parametrize("token", ["", " ", "\t", "\n", None, b"bytes", 123, object()])
def test_authenticate_invalid_or_empty_input_returns_none(token):
    auth = TokenAuthenticator(SECRETS)
    assert auth.authenticate(token) is None


@pytest.mark.parametrize(
    "token",
    [
        "tökén",
        "令牌" * 32,
        "token-with-é",
    ],
)
def test_authenticate_non_ascii_input_returns_none(token):
    auth = TokenAuthenticator(SECRETS)
    assert auth.authenticate(token) is None


@pytest.mark.parametrize(
    "token",
    [
        TOKEN_CLAUDE + " ",
        " " + TOKEN_CLAUDE,
        TOKEN_CLAUDE + "\t",
        TOKEN_CLAUDE + "\n",
        TOKEN_CLAUDE.replace("-", " -", 1),
    ],
)
def test_authenticate_whitespace_variant_does_not_match(token):
    auth = TokenAuthenticator(SECRETS)
    assert auth.authenticate(token) is None


def test_multiple_tokens_bind_distinct_agents_and_tools():
    auth = TokenAuthenticator(SECRETS)

    claude = _identity(auth.authenticate(TOKEN_CLAUDE))
    hermes = _identity(auth.authenticate(TOKEN_HERMES))

    assert claude.agent_id == "claude_code"
    assert claude.tools == ("memory_search", "memory_get")
    assert hermes.agent_id == "hermes"
    assert hermes.tools == ("memory_create",)

    # 每个令牌的工具允许列表独立，不得因多令牌配置而合并或串用。
    assert claude.allows_tool("memory_search") is True
    assert claude.allows_tool("memory_create") is False
    assert hermes.allows_tool("memory_create") is True
    assert hermes.allows_tool("memory_search") is False
    assert hermes.allows_tool("memory_get") is False


def test_identity_denies_unknown_or_non_string_tools():
    auth = TokenAuthenticator(SECRETS)
    identity = _identity(auth.authenticate(TOKEN_CLAUDE))
    assert identity.allows_tool("memory_delete") is False
    assert identity.allows_tool("") is False
    assert identity.allows_tool(None) is False


def test_identity_is_immutable():
    auth = TokenAuthenticator(SECRETS)
    identity = _identity(auth.authenticate(TOKEN_CLAUDE))
    with pytest.raises(FrozenInstanceError):
        identity.agent_id = "other"  # type: ignore[misc]


@pytest.mark.parametrize(
    "secrets",
    [
        None,
        [],
        "not-a-mapping",
        {},
    ],
)
def test_invalid_secrets_container_is_rejected(secrets):
    with pytest.raises(ValueError):
        TokenAuthenticator(secrets)


@pytest.mark.parametrize(
    "token",
    [
        "",
        " ",
        "\t",
        "\n",
        "   ",
        " leading",
        "trailing ",
        "inner space",
        "tab\tseparated",
        "newline\nseparated",
        "nonbreaking\u00a0space",
        "em\u2003space",
        "tökén",
        "令牌" * 32,
        b"bytes-token",
        None,
        123,
    ],
)
def test_invalid_token_config_is_rejected(token):
    with pytest.raises(ValueError):
        TokenAuthenticator({token: _entry()})


@pytest.mark.parametrize(
    "entry",
    [
        None,
        [],
        "not-a-mapping",
        {},
        {"tools": ["memory_search"]},
        {"agent_id": None, "tools": ["memory_search"]},
        {"agent_id": "", "tools": ["memory_search"]},
        {"agent_id": "   ", "tools": ["memory_search"]},
        {"agent_id": " agent", "tools": ["memory_search"]},
        {"agent_id": "agent ", "tools": ["memory_search"]},
        {"agent_id": "agent name", "tools": ["memory_search"]},
        {"agent_id": "智能体", "tools": ["memory_search"]},
        {"agent_id": "test_agent"},
        {"agent_id": "test_agent", "tools": None},
        {"agent_id": "test_agent", "tools": []},
        {"agent_id": "test_agent", "tools": "memory_search"},
        {"agent_id": "test_agent", "tools": b"memory_search"},
        {"agent_id": "test_agent", "tools": [None]},
        {"agent_id": "test_agent", "tools": [""]},
        {"agent_id": "test_agent", "tools": ["   "]},
        {"agent_id": "test_agent", "tools": [" memory_search"]},
        {"agent_id": "test_agent", "tools": ["memory_search "]},
        {"agent_id": "test_agent", "tools": ["memory search"]},
        {"agent_id": "test_agent", "tools": ["记忆检索"]},
    ],
)
def test_invalid_entry_config_is_rejected(entry):
    with pytest.raises(ValueError):
        TokenAuthenticator({TOKEN_CLAUDE: entry})


@pytest.mark.parametrize(
    "token",
    [
        " leading",
        "trailing ",
        "inner space",
        "tökén",
        "令牌" * 32,
    ],
)
def test_configuration_error_does_not_echo_token(token):
    with pytest.raises(ValueError) as exc_info:
        TokenAuthenticator({token: _entry()})
    message = str(exc_info.value)
    assert token not in message
    assert repr(token) not in message


def test_authentication_and_configuration_never_log_or_print_tokens(capsys, caplog):
    caplog.set_level(logging.DEBUG)

    auth = TokenAuthenticator(SECRETS)
    assert auth.authenticate(TOKEN_CLAUDE) is not None
    assert auth.authenticate(TOKEN_HERMES) is not None
    assert auth.authenticate("wrong-token") is None
    assert auth.authenticate(TOKEN_CLAUDE + " ") is None

    with pytest.raises(ValueError):
        TokenAuthenticator({TOKEN_CLAUDE + " ": _entry()})

    captured = capsys.readouterr()
    emitted = captured.out + captured.err + caplog.text
    assert TOKEN_CLAUDE not in emitted
    assert TOKEN_HERMES not in emitted

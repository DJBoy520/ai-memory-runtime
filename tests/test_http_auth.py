"""隔离的 ASGI HTTP Bearer 认证中间件行为测试。

安全不变量：
1. 受保护路径上的缺失、非法或未知 Bearer 凭据一律返回 401，且不调用下游应用；
2. 重复的 ``Authorization`` 头一律拒绝，即使其中包含有效令牌；
3. 鉴权成功后只把不可变身份写入 scope，绝不把令牌传播给下游；
4. 路径过滤只影响是否鉴权，不处理 TLS；lifespan 透传，WebSocket 未认证拒绝；
5. 中间件不记录任何令牌或完整 Authorization 头。

测试使用纯内存 ASGI 应用，不连接 daemon、生产 socket 或外部网络。
"""

import logging

import pytest

from src.interfaces.mcp.http_auth import IDENTITY_SCOPE_KEY, BearerAuthMiddleware
from src.interfaces.mcp.remote_auth import TokenAuthenticator, TokenIdentity

# 仅用于单元测试的固定假凭证；真实凭证必须由 CSPRNG 生成且不得写入仓库。
VALID_TOKEN = "test-only-http-token-0123456789abcdef0123456789abcdef"
OTHER_TOKEN = "test-only-http-other-fedcba9876543210fedcba9876543210"
WRONG_TOKEN = "test-only-wrong-token-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
AGENT_ID = "http_auth_test_agent"


class _Downstream:
    """记录下游调用的内存 ASGI 应用。"""

    def __init__(self) -> None:
        self.calls = []
        self.scopes = []

    async def __call__(self, scope, receive, send):
        self.calls.append((scope, receive, send))
        self.scopes.append(scope)
        if scope["type"] == "lifespan":
            # 合法的 ASGI lifespan 响应，不发送 HTTP 消息。
            await send({"type": "lifespan.startup.complete"})
            return
        if scope["type"] == "websocket":
            await send({"type": "websocket.accept"})
            return
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"downstream-ok"})


def _authenticator() -> TokenAuthenticator:
    return TokenAuthenticator(
        {
            VALID_TOKEN: {
                "agent_id": AGENT_ID,
                "tools": ["memory_search"],
            },
            OTHER_TOKEN: {
                "agent_id": "other_agent",
                "tools": ["memory_get"],
            },
        }
    )


def _scope(headers=(), *, path="/mcp", scope_type="http", scheme="https"):
    return {
        "type": scope_type,
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": scheme,
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": "",
        "headers": list(headers),
        "client": ("127.0.0.1", 54321),
        "server": ("127.0.0.1", 8000),
    }


def _bearer(token: str, *, name: bytes = b"authorization") -> tuple[bytes, bytes]:
    return (name, b"Bearer " + token.encode("ascii"))


async def _invoke(middleware, scope):
    sent = []

    async def receive():
        if scope.get("type") == "lifespan":
            return {"type": "lifespan.startup"}
        if scope.get("type") == "websocket":
            return {"type": "websocket.connect"}
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await middleware(scope, receive, send)
    return sent


def _status(sent):
    start = next(message for message in sent if message["type"] == "http.response.start")
    return start["status"]


def _assert_401_without_downstream(sent, downstream):
    assert _status(sent) == 401
    assert downstream.calls == []
    start = next(message for message in sent if message["type"] == "http.response.start")
    headers = dict(start.get("headers", []))
    assert headers.get(b"www-authenticate", b"").lower().startswith(b"bearer")
    body = b"".join(
        message.get("body", b"")
        for message in sent
        if message["type"] == "http.response.body"
    )
    assert VALID_TOKEN.encode("ascii") not in body
    assert WRONG_TOKEN.encode("ascii") not in body


@pytest.mark.asyncio
async def test_valid_bearer_identity_is_written_to_scope():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope([_bearer(VALID_TOKEN)])

    sent = await _invoke(middleware, scope)

    assert _status(sent) == 200
    assert len(downstream.calls) == 1
    passed_scope = downstream.scopes[0]
    identity = passed_scope[IDENTITY_SCOPE_KEY]
    assert isinstance(identity, TokenIdentity)
    assert identity.agent_id == AGENT_ID
    assert identity.tools == ("memory_search",)


@pytest.mark.asyncio
async def test_valid_bearer_token_is_removed_from_downstream_scope():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope(
        [
            (b"content-type", b"application/json"),
            _bearer(VALID_TOKEN),
            (b"x-trace-id", b"trace-1"),
        ]
    )

    await _invoke(middleware, scope)

    passed_scope = downstream.scopes[0]
    header_names = [name.lower() for name, _ in passed_scope["headers"]]
    assert b"authorization" not in header_names
    assert (b"content-type", b"application/json") in passed_scope["headers"]
    assert (b"x-trace-id", b"trace-1") in passed_scope["headers"]

    # 即使调用方保留原 scope，成功路径也不得把令牌留在其中。
    serialized = repr(scope).encode("utf-8", errors="ignore")
    assert VALID_TOKEN.encode("ascii") not in serialized
    assert b"Bearer " not in serialized


@pytest.mark.asyncio
async def test_missing_authorization_returns_401_without_downstream():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    sent = await _invoke(middleware, _scope([(b"content-type", b"application/json")]))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_unknown_token_returns_401_without_downstream():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    sent = await _invoke(middleware, _scope([_bearer(WRONG_TOKEN)]))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "header",
    [
        (b"authorization", b"Basic " + VALID_TOKEN.encode("ascii")),
        (b"authorization", b"Bearer"),
        (b"authorization", b"Bearer "),
        (b"authorization", b"Bearer  " + VALID_TOKEN.encode("ascii")),
        (b"authorization", b"Bearer\t" + VALID_TOKEN.encode("ascii")),
        (b"authorization", b"Bearer " + VALID_TOKEN.encode("ascii") + b" extra"),
        (b"authorization", b"Bearer " + VALID_TOKEN.encode("ascii") + b" "),
        (b"authorization", b" Bearer " + VALID_TOKEN.encode("ascii")),
        (b"authorization", b"Bearer \x7f"),
        (b"authorization", b"Bearer t\xc3\xb6k\xc3\xa9n"),
        (b"authorization", b"Bearer " + VALID_TOKEN.encode("ascii") + b",Bearer x"),
        (b"authorization", b""),
    ],
)
async def test_malformed_authorization_header_returns_401(header):
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    sent = await _invoke(middleware, _scope([header]))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_duplicate_authorization_headers_are_rejected_even_if_valid():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope([_bearer(VALID_TOKEN), _bearer(VALID_TOKEN)])

    sent = await _invoke(middleware, scope)

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_duplicate_mixed_authorization_headers_are_rejected():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope(
        [
            _bearer(VALID_TOKEN),
            (b"authorization", b"Basic " + OTHER_TOKEN.encode("ascii")),
        ]
    )

    sent = await _invoke(middleware, scope)

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [(b"authorization", 123)],
        [(b"authorization", None)],
        [("authorization", b"Bearer " + VALID_TOKEN.encode("ascii"))],
        [(b"authorization",)],
        [(b"authorization", b"Bearer " + VALID_TOKEN.encode("ascii"), b"extra")],
        [None],
        "not-a-header-list",
    ],
)
async def test_malformed_header_container_fails_closed(headers):
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    sent = await _invoke(middleware, _scope(headers))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_authorization_header_name_is_case_insensitive():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope([_bearer(VALID_TOKEN, name=b"Authorization")])

    sent = await _invoke(middleware, scope)

    assert _status(sent) == 200
    assert len(downstream.calls) == 1
    assert downstream.scopes[0][IDENTITY_SCOPE_KEY].agent_id == AGENT_ID


@pytest.mark.asyncio
async def test_bearer_scheme_is_case_insensitive_but_token_is_not():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    ok_scope = _scope([(b"authorization", b"bEaReR " + VALID_TOKEN.encode("ascii"))])
    sent = await _invoke(middleware, ok_scope)
    assert _status(sent) == 200
    assert len(downstream.calls) == 1

    case_changed_token = VALID_TOKEN.swapcase()
    bad_sent = await _invoke(
        middleware, _scope([_bearer(case_changed_token)])
    )
    assert _status(bad_sent) == 401
    assert len(downstream.calls) == 1


@pytest.mark.asyncio
async def test_unprotected_path_passes_through_without_authentication():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(
        downstream, _authenticator(), protected_paths={"/mcp"}
    )

    sent = await _invoke(middleware, _scope([], path="/health"))

    assert _status(sent) == 200
    assert len(downstream.calls) == 1
    assert IDENTITY_SCOPE_KEY not in downstream.scopes[0]


@pytest.mark.asyncio
async def test_protected_path_requires_authentication_when_filter_configured():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(
        downstream, _authenticator(), protected_paths={"/mcp"}
    )

    sent = await _invoke(middleware, _scope([], path="/mcp"))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_all_paths_are_protected_by_default():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    sent = await _invoke(middleware, _scope([], path="/anything/else"))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_empty_protected_paths_protects_nothing():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(
        downstream, _authenticator(), protected_paths=()
    )

    sent = await _invoke(middleware, _scope([], path="/mcp"))

    assert _status(sent) == 200
    assert len(downstream.calls) == 1


@pytest.mark.asyncio
async def test_lifespan_scope_passes_through_with_valid_asgi_messages():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope([], scope_type="lifespan")

    sent = await _invoke(middleware, scope)

    assert sent == [{"type": "lifespan.startup.complete"}]
    assert len(downstream.calls) == 1
    assert downstream.scopes[0] is scope
    assert IDENTITY_SCOPE_KEY not in scope


def _assert_websocket_rejected(sent, downstream):
    assert downstream.calls == []
    close = next(message for message in sent if message["type"] == "websocket.close")
    assert close["code"] == 1008
    assert not any(message["type"] == "websocket.accept" for message in sent)


@pytest.mark.asyncio
async def test_unauthenticated_websocket_is_rejected_not_passed_through():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    sent = await _invoke(middleware, _scope([], scope_type="websocket"))

    _assert_websocket_rejected(sent, downstream)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [_bearer(WRONG_TOKEN)],
        [_bearer(VALID_TOKEN), _bearer(VALID_TOKEN)],
        [(b"authorization", b"Basic " + VALID_TOKEN.encode("ascii"))],
    ],
)
async def test_invalid_websocket_credentials_are_rejected(headers):
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    sent = await _invoke(
        middleware, _scope(headers, scope_type="websocket")
    )

    _assert_websocket_rejected(sent, downstream)


@pytest.mark.asyncio
async def test_authenticated_websocket_passes_through_with_identity():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope([_bearer(VALID_TOKEN)], scope_type="websocket")

    sent = await _invoke(middleware, scope)

    assert sent == [{"type": "websocket.accept"}]
    assert len(downstream.calls) == 1
    assert downstream.scopes[0][IDENTITY_SCOPE_KEY].agent_id == AGENT_ID
    assert all(name.lower() != b"authorization" for name, _ in scope["headers"])


@pytest.mark.asyncio
@pytest.mark.parametrize("scheme", ["http", "https"])
async def test_middleware_does_not_handle_tls_or_depend_on_scheme(scheme):
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    no_auth = await _invoke(middleware, _scope([], scheme=scheme))
    assert _status(no_auth) == 401
    assert downstream.calls == []

    with_auth = await _invoke(
        middleware, _scope([_bearer(VALID_TOKEN)], scheme=scheme)
    )
    assert _status(with_auth) == 200
    assert len(downstream.calls) == 1
    assert downstream.scopes[0][IDENTITY_SCOPE_KEY].agent_id == AGENT_ID


@pytest.mark.asyncio
async def test_missing_or_invalid_path_fails_closed_even_with_valid_credentials():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    for invalid_path in (None, b"/mcp", 123, "", "mcp", "/mcp\x00suffix"):
        scope = _scope([_bearer(VALID_TOKEN)])
        scope["path"] = invalid_path
        sent = await _invoke(middleware, scope)
        assert _status(sent) == 401
    assert downstream.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/mcp/",
        "/mcp//",
        "//mcp",
        "/./mcp",
        "/mcp/.",
        "/x/../mcp",
        "/mcp/../mcp",
        "/%6dcp",
        "/mcp%2F",
        "/%2e/mcp",
    ],
)
async def test_protected_path_cannot_be_bypassed_with_path_aliases(path):
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(
        downstream, _authenticator(), protected_paths={"/mcp"}
    )

    sent = await _invoke(middleware, _scope([], path=path))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_protected_path_descendants_require_authentication():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(
        downstream, _authenticator(), protected_paths={"/mcp"}
    )

    sent = await _invoke(middleware, _scope([], path="/mcp/messages"))

    _assert_401_without_downstream(sent, downstream)


@pytest.mark.asyncio
async def test_protected_path_sibling_is_not_overprotected():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(
        downstream, _authenticator(), protected_paths={"/mcp"}
    )

    sent = await _invoke(middleware, _scope([], path="/mcp-public"))

    assert _status(sent) == 200
    assert len(downstream.calls) == 1
    assert IDENTITY_SCOPE_KEY not in downstream.scopes[0]


@pytest.mark.asyncio
async def test_no_token_or_authorization_header_is_logged(caplog, capsys):
    caplog.set_level(logging.DEBUG)
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())

    await _invoke(middleware, _scope([_bearer(VALID_TOKEN)]))
    await _invoke(middleware, _scope([_bearer(WRONG_TOKEN)]))
    await _invoke(middleware, _scope([_bearer(VALID_TOKEN), _bearer(VALID_TOKEN)]))
    await _invoke(
        middleware, _scope([(b"authorization", b"Bearer " + VALID_TOKEN.encode())])
    )

    captured = capsys.readouterr()
    emitted = captured.out + captured.err + caplog.text
    assert VALID_TOKEN not in emitted
    assert OTHER_TOKEN not in emitted
    assert WRONG_TOKEN not in emitted
    assert "Bearer " not in emitted


@pytest.mark.asyncio
async def test_mutating_upstream_authorization_after_auth_cannot_change_identity():
    downstream = _Downstream()
    middleware = BearerAuthMiddleware(downstream, _authenticator())
    scope = _scope([_bearer(VALID_TOKEN)])

    await _invoke(middleware, scope)

    identity = downstream.scopes[0][IDENTITY_SCOPE_KEY]
    # 成功路径已移除敏感头，上游无法在下游读取前替换令牌。
    assert all(name.lower() != b"authorization" for name, _ in scope["headers"])
    assert identity.agent_id == AGENT_ID

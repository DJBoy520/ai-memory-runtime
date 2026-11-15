"""Smoke test for wiring up the remote MCP runtime (no socket I/O)."""

import os
import socket

import pytest

pytest.importorskip("mcp")

from src.interfaces.mcp.remote_auth import TokenAuthenticator  # noqa: E402
from src.interfaces.mcp.remote_runtime import build_runtime  # noqa: E402

TOKENS = {
    "test-token": {
        "agent_id": "claude_code",
        "tools": ["memory_search"],
    }
}


def _authenticator():
    return TokenAuthenticator(TOKENS)


def _sockets(tmp_path):
    """返回一对不同的绝对套接字路径（仅路径，不创建 socket）。"""
    return str(tmp_path / "business.sock"), str(tmp_path / "admin.sock")


def test_build_runtime_returns_callable_asgi_app():
    app = build_runtime(
        business_socket="/tmp/amr-business.sock",
        admin_socket="/tmp/amr-admin.sock",
        authenticator=_authenticator(),
    )

    assert app is not None
    # ASGI apps are callables invoked as app(scope, receive, send).
    assert callable(app), f"expected a callable ASGI app, got {type(app)!r}"


def test_build_runtime_accepts_distinct_tmp_paths(tmp_path):
    business, admin = _sockets(tmp_path)

    app = build_runtime(
        business_socket=business,
        admin_socket=admin,
        authenticator=_authenticator(),
    )

    assert callable(app)


def test_build_runtime_passes_the_same_authenticator_to_build_app(monkeypatch):
    authenticator = _authenticator()
    captured = {}

    import src.interfaces.mcp.remote_runtime as remote_runtime

    def fake_build_app(given_authenticator, backend, **kwargs):
        captured["authenticator"] = given_authenticator
        captured["backend"] = backend
        captured["kwargs"] = kwargs
        return "app-sentinel"

    monkeypatch.setattr(remote_runtime, "build_app", fake_build_app)

    result = remote_runtime.build_runtime(
        business_socket="/tmp/amr-business.sock",
        admin_socket="/tmp/amr-admin.sock",
        authenticator=authenticator,
    )

    assert result == "app-sentinel"
    assert captured["authenticator"] is authenticator


@pytest.mark.parametrize("authenticator", [None, {}, "not-an-authenticator", object()])
def test_build_runtime_rejects_non_token_authenticator(tmp_path, authenticator):
    business, admin = _sockets(tmp_path)

    with pytest.raises(TypeError, match="TokenAuthenticator"):
        build_runtime(
            business_socket=business,
            admin_socket=admin,
            authenticator=authenticator,
        )


def test_build_runtime_rejects_identical_sockets(tmp_path):
    same = str(tmp_path / "shared.sock")

    with pytest.raises(ValueError):
        build_runtime(
            business_socket=same,
            admin_socket=same,
            authenticator=_authenticator(),
        )


def test_build_runtime_rejects_symlink_alias_sockets(tmp_path):
    business = tmp_path / "business.sock"
    business.write_text("")
    admin = tmp_path / "admin.sock"
    try:
        os.symlink(business, admin)
    except (OSError, NotImplementedError):  # pragma: no cover - platform dependent
        pytest.skip("symlinks are not supported on this platform")

    with pytest.raises(ValueError):
        build_runtime(
            business_socket=str(business),
            admin_socket=str(admin),
            authenticator=_authenticator(),
        )


@pytest.mark.parametrize("relative_field", ["business_socket", "admin_socket"])
@pytest.mark.parametrize("relative_value", ["business.sock", "./admin.sock"])
def test_build_runtime_rejects_relative_socket_paths(
    tmp_path, relative_field, relative_value
):
    business, admin = _sockets(tmp_path)
    kwargs = {"business_socket": business, "admin_socket": admin}
    kwargs[relative_field] = relative_value

    with pytest.raises(ValueError):
        build_runtime(authenticator=_authenticator(), **kwargs)


@pytest.mark.parametrize("field", ["business_socket", "admin_socket"])
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_build_runtime_rejects_blank_socket_paths(tmp_path, field, blank):
    """Socket 路径必须显式提供，空值/纯空白不得推断默认值。"""
    business, admin = _sockets(tmp_path)
    kwargs = {"business_socket": business, "admin_socket": admin}
    kwargs[field] = blank

    with pytest.raises(ValueError):
        build_runtime(authenticator=_authenticator(), **kwargs)


def test_build_runtime_does_not_connect_during_assembly(tmp_path, monkeypatch):
    """装配阶段只构造对象，不得建立任何 socket 连接。"""
    business, admin = _sockets(tmp_path)

    def _boom(*args, **kwargs):
        raise AssertionError("build_runtime must not open a socket connection")

    monkeypatch.setattr(socket.socket, "connect", _boom)
    monkeypatch.setattr(socket.socket, "connect_ex", _boom)

    app = build_runtime(
        business_socket=business,
        admin_socket=admin,
        authenticator=_authenticator(),
    )

    assert callable(app)

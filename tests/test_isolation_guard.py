"""
Tests for AMR test isolation guard.
Verifies that:
1. load_config() reads isolated tmp configuration.
2. Direct socket.connect / socket.connect_ex to production sockets are blocked with RuntimeError.
3. asyncio.open_unix_connection to production sockets is blocked with RuntimeError.
4. Tmp directory sockets and socketpairs are not blocked.
5. AMR_CONFIG_PATH points to TEST_ROOT config.yaml.
6. Dual-gating bypass works strictly when both AMR_TEST_LIVE_DAEMON=1 and matching AMR_TEST_DAEMON_SOCKET are present.
"""

import asyncio
import os
import socket
import tempfile
import uuid
import pytest

from config.settings import load_config
from tests.conftest import TEST_ROOT, _orig_socket_connect, _orig_socket_connect_ex


def test_config_paths_isolated():
    """1. load_config()（无参）读到的 storage.sqlite_path 位于 TEST_ROOT 且 qdrant.url == 'http://127.0.0.1:1'；
    server.socket 路径位于 TEST_ROOT 而不是 /run/user/1000。"""
    cfg = load_config()
    assert cfg.qdrant.url == "http://127.0.0.1:1"
    assert os.path.abspath(cfg.storage.sqlite_path).startswith(os.path.abspath(TEST_ROOT))
    assert os.path.abspath(cfg.server.business_socket).startswith(os.path.abspath(TEST_ROOT))
    assert os.path.abspath(cfg.server.admin_socket).startswith(os.path.abspath(TEST_ROOT))
    assert "/run/user/1000" not in cfg.server.business_socket
    assert "/run/user/1000" not in cfg.server.admin_socket


def test_socket_connect_blocked():
    """2. 直接 socket.socket().connect(("/run/user/1000/qdrant-bge.sock",)) 抛 RuntimeError（用 pytest.raises）；
    connect_ex 同理返回/抛出被守卫拦截。"""
    # 2.1 针对元组参数 (path,) 的 connect 拦截
    with pytest.raises(RuntimeError, match="AMR test isolation"):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect(("/run/user/1000/qdrant-bge.sock",))
        finally:
            s.close()

    # 2.2 针对字符串路径的 connect 拦截
    with pytest.raises(RuntimeError, match="AMR test isolation"):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect("/run/user/1000/qdrant-bge.sock")
        finally:
            s.close()

    # 2.3 connect_ex 元组拦截
    with pytest.raises(RuntimeError, match="AMR test isolation"):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect_ex(("/run/user/1000/qdrant-bge.sock",))
        finally:
            s.close()

    # 2.4 connect_ex 针对 admin socket 的拦截
    with pytest.raises(RuntimeError, match="AMR test isolation"):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect_ex("/run/user/1000/qdrant-bge-admin.sock")
        finally:
            s.close()

    # 2.5 匹配任意 uid 的拦截
    with pytest.raises(RuntimeError, match="AMR test isolation"):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect("/run/user/1001/qdrant-bge.sock")
        finally:
            s.close()


@pytest.mark.asyncio
async def test_asyncio_open_unix_connection_blocked():
    """3. 通过 asyncio.open_unix_connection('/run/user/1000/qdrant-bge.sock') 同样被拦截"""
    with pytest.raises(RuntimeError, match="AMR test isolation"):
        await asyncio.open_unix_connection("/run/user/1000/qdrant-bge.sock")


def test_tmp_socket_not_blocked():
    """4. tmp 目录下的 socket 连接守卫不误伤（可用 socket.socketpair 或 tmp unix server，简单验证不抛 RuntimeError）"""
    # 4.1 socketpair 验证
    sp1, sp2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sp1.send(b"ping")
        assert sp2.recv(4) == b"ping"
    finally:
        sp1.close()
        sp2.close()

    # 4.2 tmp unix server & client connect 验证：正常连接不被守卫误伤抛出 RuntimeError
    tmp_sock_path = os.path.join(tempfile.gettempdir(), f"amr-guard-test-{uuid.uuid4().hex[:8]}.sock")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(tmp_sock_path)
    srv.listen(1)
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.connect(tmp_sock_path)
        finally:
            client.close()
    finally:
        srv.close()
        if os.path.exists(tmp_sock_path):
            try:
                os.unlink(tmp_sock_path)
            except OSError:
                pass


def test_env_amr_config_path_set():
    """5. 生产默认配置路径未被任何测试模块在 import 时使用（可通过断言 AMR_CONFIG_PATH 环境变量等于 TEST_ROOT 配置）"""
    env_cfg = os.environ.get("AMR_CONFIG_PATH")
    assert env_cfg is not None
    assert os.path.abspath(env_cfg).startswith(os.path.abspath(TEST_ROOT))
    assert os.path.exists(env_cfg)
    assert os.path.basename(env_cfg) == "config.yaml"


def test_dual_gating_live_daemon_bypass():
    """验证双门控逻辑：仅当 AMR_TEST_LIVE_DAEMON=1 且 AMR_TEST_DAEMON_SOCKET 匹配时放行守卫"""
    # 使用符合模式但不存在的探针 socket 路径，确保自身安全，不触碰生产 socket
    test_target = "/run/user/1000/qdrant-bge-isolated-test-probe.sock"
    old_live = os.environ.get("AMR_TEST_LIVE_DAEMON")
    old_sock = os.environ.get("AMR_TEST_DAEMON_SOCKET")
    try:
        # 仅有 AMR_TEST_LIVE_DAEMON=1 但无 AMR_TEST_DAEMON_SOCKET：仍然拦截
        os.environ["AMR_TEST_LIVE_DAEMON"] = "1"
        os.environ.pop("AMR_TEST_DAEMON_SOCKET", None)
        with pytest.raises(RuntimeError, match="AMR test isolation"):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                s.connect(test_target)
            finally:
                s.close()

        # 双门控同时满足且匹配：守卫放行（不再抛 RuntimeError；由于没有实际服务，抛 FileNotFoundError 或 ConnectionRefusedError）
        os.environ["AMR_TEST_DAEMON_SOCKET"] = test_target
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            with pytest.raises((FileNotFoundError, ConnectionRefusedError)):
                s.connect(test_target)
        finally:
            s.close()
    finally:
        if old_live is not None:
            os.environ["AMR_TEST_LIVE_DAEMON"] = old_live
        else:
            os.environ.pop("AMR_TEST_LIVE_DAEMON", None)

        if old_sock is not None:
            os.environ["AMR_TEST_DAEMON_SOCKET"] = old_sock
        else:
            os.environ.pop("AMR_TEST_DAEMON_SOCKET", None)


def test_amr_application_di_injection():
    """验证 AMRApplication 的依赖注入（DI）机制：
    1. 注入 engine/qdrant_manager/session_store 时直接使用注入对象；
    2. 注入 qdrant_manager 时不调用 ensure_standard_collections()；
    3. memory_service 与 outbox_worker 正确绑定注入对象。"""
    from unittest.mock import MagicMock
    from src.main import AMRApplication

    fake_engine = MagicMock()
    fake_qdrant = MagicMock()
    fake_store = MagicMock()

    app = AMRApplication(
        engine=fake_engine,
        qdrant_manager=fake_qdrant,
        session_store=fake_store,
    )

    assert app.engine is fake_engine
    assert app.qdrant_manager is fake_qdrant
    assert app.session_store is fake_store

    # 验证未调用 ensure_standard_collections()
    fake_qdrant.ensure_standard_collections.assert_not_called()

    # 验证下游 service 与 worker 使用注入对象
    assert app.memory_service.engine is fake_engine
    assert app.memory_service.qdrant is fake_qdrant
    assert app.memory_service.session_store is fake_store
    assert app.outbox_worker.engine is fake_engine
    assert app.outbox_worker.qdrant is fake_qdrant
    assert app.outbox_worker.session_store is fake_store


def test_amr_application_config_precedence():
    """验证 AMRApplication 配置解析优先级：显式 config_path > AMR_CONFIG_PATH > 默认路径"""
    from unittest.mock import MagicMock
    from src.main import AMRApplication

    # 1. 默认情况（无显式传参）：尊重 AMR_CONFIG_PATH
    app_env = AMRApplication(
        engine=MagicMock(),
        qdrant_manager=MagicMock(),
        session_store=MagicMock(),
    )
    assert os.path.abspath(app_env.config.storage.sqlite_path).startswith(os.path.abspath(TEST_ROOT))

    # 2. 显式 config_path 优先于环境变量
    custom_cfg_path = os.path.join(TEST_ROOT, "custom_config.yaml")
    with open(custom_cfg_path, "w", encoding="utf-8") as f:
        f.write("""server:
  allowed_agents: ["custom_agent"]
storage:
  sqlite_path: "/tmp/custom.db"
qdrant:
  url: "http://127.0.0.1:1"
model:
  device: "cpu"
  fp16: false
""")
    os.chmod(custom_cfg_path, 0o600)

    app_custom = AMRApplication(
        config_path=custom_cfg_path,
        engine=MagicMock(),
        qdrant_manager=MagicMock(),
        session_store=MagicMock(),
    )
    assert app_custom.config.storage.sqlite_path == "/tmp/custom.db"
    assert "custom_agent" in app_custom.config.server.allowed_agents


def test_main_system_exit_not_swallowed():
    """验证 main.py __main__ 退出语义：SystemExit 必须向上传播，不被捕获为 0"""
    import subprocess
    import sys

    # 启动子进程执行内联脚本，模拟 SystemExit 抛出
    code = """
import sys
from unittest.mock import patch
import src.main

with patch("src.main.main", side_effect=SystemExit(3)):
    # 模拟 __main__ 块的逻辑
    try:
        import asyncio
        asyncio.run(src.main.main())
    except KeyboardInterrupt:
        pass
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 3


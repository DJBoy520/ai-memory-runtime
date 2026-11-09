"""
AMR Test Isolation Guard & Test Environment Bootstrap.
Must execute at top-level before any test module is imported.
"""

import atexit
import os
import re
import shutil
import socket
import tempfile
from pathlib import Path

# 1. 进程级临时目录与运行目录初始化
TEST_ROOT = tempfile.mkdtemp(prefix="amr-test-")
RUN_DIR = os.path.join(TEST_ROOT, "run")
os.makedirs(RUN_DIR, exist_ok=True)

# 注册退出时清理进程级临时目录
atexit.register(lambda: shutil.rmtree(TEST_ROOT, ignore_errors=True))

# 2. 生成 TEST_ROOT/config.yaml
BUSINESS_SOCK = os.path.join(RUN_DIR, "qdrant-bge.sock")
ADMIN_SOCK = os.path.join(RUN_DIR, "qdrant-bge-admin.sock")
SQLITE_PATH = os.path.join(TEST_ROOT, "sessions.db")
CONFIG_YAML_PATH = os.path.join(TEST_ROOT, "config.yaml")

CONFIG_YAML_CONTENT = f"""# AMR Isolated Test Configuration
server:
  business_socket: "{BUSINESS_SOCK}"
  admin_socket: "{ADMIN_SOCK}"
  socket_mode: 0o600
  max_request_bytes: 4194304
  allowed_agents:
    - "system"
    - "test_runner"
    - "hermes"
    - "qwenwork"
    - "openclaw"
    - "default_agent"
    - "claude_code"
    - "opencode"
    - "dsh"
    - "doubao"
    - "zcode"
    - "kle"

model:
  name_or_path: "BAAI/bge-m3"
  device: "cpu"
  fp16: false
  max_batch: 16
  max_queue_size: 64
  idle_timeout_seconds: 0
  max_token_length: 8192

storage:
  sqlite_path: "{SQLITE_PATH}"
  busy_timeout: 5000
  wal_enabled: true

qdrant:
  url: "http://127.0.0.1:1"
  api_key: ""
  timeout: 10.0
  collections:
    - "ai_memory"
    - "crypto_standards"
    - "project_docs"

search:
  project_fallback_ids:
    - "global"
    - "general"
"""

with open(CONFIG_YAML_PATH, "w", encoding="utf-8") as f:
    f.write(CONFIG_YAML_CONTENT)

# 确保满足 check_and_fix_file_permissions(0o600)
os.chmod(CONFIG_YAML_PATH, 0o600)

# 3. 设置环境变量，必须覆盖已有环境变量
os.environ["AMR_CONFIG_PATH"] = CONFIG_YAML_PATH
os.environ["XDG_RUNTIME_DIR"] = RUN_DIR

# 若 config.settings 已导入，刷新全局单例
try:
    import config.settings
    config.settings.settings = config.settings.load_config()
except Exception:
    pass

# 4. 安装 socket 守卫
_orig_socket_connect = socket.socket.connect
_orig_socket_connect_ex = socket.socket.connect_ex

# 保存原函数引用，暴露给模块和 socket 便于断言与测试
socket._orig_connect = _orig_socket_connect
socket._orig_connect_ex = _orig_socket_connect_ex

PROD_SOCKET_PATTERN = re.compile(r"^/run/user/[^/]+/qdrant-bge.*\.sock$")
PROD_DEFAULT_SOCKETS = {
    "/run/user/1000/qdrant-bge.sock",
    "/run/user/1000/qdrant-bge-admin.sock",
}


def _extract_target_path(address):
    """从 socket address 参数中提取 Unix socket 路径"""
    if isinstance(address, (str, bytes, os.PathLike)):
        return address
    if isinstance(address, (tuple, list)):
        if len(address) == 1 and isinstance(address[0], (str, bytes, os.PathLike)):
            return address[0]
        # TCP connection: (host, port)
        if len(address) == 2 and isinstance(address[1], int):
            return None
    return None


def _is_blocked_production_socket(address) -> bool:
    target = _extract_target_path(address)
    if target is None:
        return False

    if isinstance(target, bytes):
        try:
            target_str = target.decode("utf-8")
        except UnicodeDecodeError:
            target_str = target.decode("utf-8", errors="ignore")
    elif isinstance(target, os.PathLike):
        target_str = os.fspath(target)
    elif isinstance(target, str):
        target_str = target
    else:
        return False

    # 规范化绝对路径
    norm_path = os.path.abspath(os.path.normpath(target_str))

    # tmp 路径一律放行
    tmp_prefixes = (
        os.path.abspath(tempfile.gettempdir()),
        "/tmp",
        "/var/tmp",
        os.path.abspath(TEST_ROOT),
    )
    if any(norm_path.startswith(p.rstrip("/") + "/") or norm_path == p for p in tmp_prefixes):
        return False

    # 判定是否属于生产 daemon socket
    is_prod = bool(PROD_SOCKET_PATTERN.match(norm_path)) or (norm_path in PROD_DEFAULT_SOCKETS)
    if not is_prod:
        return False

    # 双门控例外：仅当 AMR_TEST_LIVE_DAEMON=="1" 且 AMR_TEST_DAEMON_SOCKET 显式存在且该地址恰好等于 AMR_TEST_DAEMON_SOCKET 时放行
    if os.environ.get("AMR_TEST_LIVE_DAEMON") == "1":
        allowed_socket = os.environ.get("AMR_TEST_DAEMON_SOCKET")
        if allowed_socket and os.path.abspath(os.path.normpath(allowed_socket)) == norm_path:
            return False

    return True


def _guarded_connect(self, address):
    if _is_blocked_production_socket(address):
        raise RuntimeError(
            f"AMR test isolation: attempt to connect to production daemon socket ({address}) is blocked. "
            "To run live daemon tests, set AMR_TEST_LIVE_DAEMON=1 and AMR_TEST_DAEMON_SOCKET=<path>."
        )
    return _orig_socket_connect(self, address)


def _guarded_connect_ex(self, address):
    if _is_blocked_production_socket(address):
        raise RuntimeError(
            f"AMR test isolation: attempt to connect to production daemon socket ({address}) is blocked. "
            "To run live daemon tests, set AMR_TEST_LIVE_DAEMON=1 and AMR_TEST_DAEMON_SOCKET=<path>."
        )
    return _orig_socket_connect_ex(self, address)


socket.socket.connect = _guarded_connect
socket.socket.connect_ex = _guarded_connect_ex

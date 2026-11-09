"""
Tests for OutboxWorker Single-Writer flock (RFC v5.1 §2, V5.2 §5)
验证：
1. 子进程 A 启动并持有 amr-outbox.lock 独占排他锁；
2. 子进程 B 尝试启动同一个 lock_path 时被拒绝，触发 SystemExit(3) 退出；
3. 子进程 A 退出并释放锁后，子进程可成功启动持锁；
4. 同一进程内 start() -> stop() 释放锁后，其他实例可正常获得锁。
"""

import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from src.service.outbox_worker import OutboxWorker


def test_outbox_lock_mutual_exclusion(tmp_path: Path):
    """验证子进程 A 持锁时，子进程 B 启动被拒绝并以 exit code 3 退出；A 退出后可启动"""
    lock_file = tmp_path / "amr-outbox-test.lock"

    # 子进程 A 代码：启动 worker 持锁，成功后输出 LOCKED 并挂起等待
    script_a = f"""
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock
from src.service.outbox_worker import OutboxWorker

store = MagicMock()
store.count_projection_backlog.return_value = {{}}
store.fetch_pending_projection_tasks.return_value = []
store.fetch_pending_sync_tasks.return_value = []

worker = OutboxWorker(
    session_store=store,
    qdrant_manager=MagicMock(),
    engine=MagicMock(),
    lock_path=r"{lock_file}",
)
worker.start()
print("LOCKED", flush=True)

# 保持存活直到被终止
try:
    while True:
        time.sleep(0.1)
except KeyboardInterrupt:
    worker.stop()
    sys.exit(0)
"""

    env = os.environ.copy()
    # 启动子进程 A
    proc_a = subprocess.Popen(
        [sys.executable, "-c", script_a],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    try:
        # 等待子进程 A 输出 LOCKED
        line = proc_a.stdout.readline().strip()
        assert line == "LOCKED", f"Process A failed to lock: {proc_a.stderr.read()}"

        # 子进程 B 代码：尝试抢占相同的锁文件
        script_b = f"""
from unittest.mock import MagicMock
from src.service.outbox_worker import OutboxWorker

worker = OutboxWorker(
    session_store=MagicMock(),
    qdrant_manager=MagicMock(),
    engine=MagicMock(),
    lock_path=r"{lock_file}",
)
worker.start()
"""
        env = os.environ.copy()
        proc_b = subprocess.Popen(
            [sys.executable, "-c", script_b],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        returncode_b = proc_b.wait(timeout=15)
        stdout_b = proc_b.stdout.read()
        stderr_b = proc_b.stderr.read()

        # 断言子进程 B 退出码严格为 3
        assert returncode_b == 3, f"Expected returncode 3, got {returncode_b}. Stderr: {stderr_b}"
        assert "another outbox worker holds amr-outbox.lock" in stderr_b

    finally:
        # 终止子进程 A
        proc_a.terminate()
        proc_a.wait(timeout=15)

    # A 退出后，启动子进程 C 应当能够成功持锁
    script_c = f"""
import sys
from unittest.mock import MagicMock
from src.service.outbox_worker import OutboxWorker

store = MagicMock()
store.count_projection_backlog.return_value = {{}}
store.fetch_pending_projection_tasks.return_value = []
store.fetch_pending_sync_tasks.return_value = []

worker = OutboxWorker(
    session_store=store,
    qdrant_manager=MagicMock(),
    engine=MagicMock(),
    lock_path=r"{lock_file}",
)
worker.start()
print("LOCKED_C", flush=True)
worker.stop()
sys.exit(0)
"""
    proc_c = subprocess.Popen(
        [sys.executable, "-c", script_c],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    stdout_c, stderr_c = proc_c.communicate(timeout=15)
    assert proc_c.returncode == 0, f"Process C failed. Stderr: {stderr_c}"
    assert "LOCKED_C" in stdout_c


def test_in_process_start_stop_releases_lock(tmp_path: Path):
    """验证同一进程内 start() -> stop() 能够释放锁，允许下一个 worker 启动"""
    lock_file = tmp_path / "in_process.lock"
    from unittest.mock import MagicMock

    store = MagicMock()
    store.count_projection_backlog.return_value = {}
    store.fetch_pending_projection_tasks.return_value = []
    store.fetch_pending_sync_tasks.return_value = []

    worker1 = OutboxWorker(
        session_store=store,
        qdrant_manager=MagicMock(),
        engine=MagicMock(),
        lock_path=lock_file,
    )
    worker1.start()
    assert worker1._running is True
    worker1.stop()
    assert worker1._running is False

    worker2 = OutboxWorker(
        session_store=store,
        qdrant_manager=MagicMock(),
        engine=MagicMock(),
        lock_path=lock_file,
    )
    worker2.start()
    assert worker2._running is True
    worker2.stop()
    assert worker2._running is False

"""
Tests for Safe Shutdown and Concurrency Robustness (ROBUST-01 & ROBUST-02)

ROBUST-01: BGEM3Engine.close() & aclose() synchronization in event loop threads.
- Prevents UAF / CUDA crash / NoneType errors when close() is called while inference is active.
- Verifies background teardown thread and graceful aclose() draining.

ROBUST-02: OutboxWorker.stop() lock retention semantics.
- Guarantees flock is NOT released if worker thread does not exit within timeout.
- Prevents double-writers / split-brain on Qdrant during rolling restarts or long network operations.
"""

import asyncio
from pathlib import Path
import threading
import time
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import BGEM3Engine, EngineError, ModelState
from src.service.outbox_worker import OutboxWorker
from src.main import AMRApplication


@pytest.mark.asyncio
async def test_application_does_not_close_dependencies_if_worker_times_out():
    app = object.__new__(AMRApplication)
    app.outbox_worker = MagicMock()
    app.outbox_worker.stop.side_effect = TimeoutError("still running")
    app.server = MagicMock()
    app.engine = MagicMock()
    app.session_store = MagicMock()
    app.qdrant_manager = MagicMock()

    with pytest.raises(TimeoutError):
        await app.stop()
    app.session_store.close.assert_not_called()
    app.qdrant_manager.close.assert_not_called()
    app.engine.close.assert_not_called()


class SlowMockModel:
    """Mock model that simulates inference execution time and monitors access after deletion."""

    def __init__(self, delay: float = 0.15, hidden_dim: int = 1024):
        self.delay = delay
        self.hidden_dim = hidden_dim
        self.device = "cpu"
        self.was_called = False
        self.call_finished = False

    def to(self, device: str):
        self.device = device
        return self

    def half(self):
        return self

    def eval(self):
        return self

    def __call__(self, **kwargs):
        self.was_called = True
        time.sleep(self.delay)
        input_ids = kwargs.get("input_ids")
        batch_size = input_ids.shape[0] if input_ids is not None else 1
        hidden = torch.ones((batch_size, 4, self.hidden_dim), dtype=torch.float32)
        mock_output = MagicMock()
        mock_output.last_hidden_state = hidden
        self.call_finished = True
        return mock_output


class DummyTokenizer:
    def __call__(self, texts, **kwargs):
        batch_size = len(texts)
        return {
            "input_ids": torch.zeros((batch_size, 4), dtype=torch.long),
            "attention_mask": torch.ones((batch_size, 4), dtype=torch.long),
        }


# =========================================================================
# FINDING-ROBUST-01: BGEM3Engine Close & Teardown Synchronization
# =========================================================================

@pytest.mark.asyncio
async def test_engine_close_in_event_loop_with_active_inference():
    """
    [ROBUST-01 验证]
    在异步事件循环线程内发起 embed 推理，并在推理正在执行期间直接同步调用 engine.close()。
    修复前行为：
      is_event_loop_thread=True 导致 wait=False，主线程立即执行 _do_unload_resources()，
      工作线程访问 self.model 时抛出 NoneType AttributeError 或底严重 CUDA / SIGSEGV 崩溃。
    修复后契约：
      1. close() 识别到活跃请求，将 shutdown(wait=True) 与资源释放委派给后台 Teardown 线程；
      2. 正在执行的推理正常完成，返回正确的 embedding，未发生 AttributeError；
      3. 主事件循环未发生死锁阻塞；
      4. 后台 Teardown 线程在请求排空后安全卸载模型并将状态置为 UNLOADED。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=16,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    mock_model = SlowMockModel(delay=0.1)
    engine.tokenizer = DummyTokenizer()
    engine.model = mock_model
    engine._set_state(ModelState.READY)

    # 启动异步 embed 请求
    embed_task = asyncio.create_task(engine.embed(["Active inference test sentence"]))

    # 等待请求进入执行状态（_active_requests == 1）
    for _ in range(50):
        with engine._queue_lock:
            if engine._active_requests > 0:
                break
        await asyncio.sleep(0.005)

    with engine._queue_lock:
        assert engine._active_requests == 1

    # 在同一事件循环线程内调用同步 close()
    engine.close()

    # 验证在新发请求时被拒绝
    assert engine._closed is True
    with pytest.raises(EngineError) as exc_info:
        await engine.embed(["New request after close"])
    assert "Engine has already been closed" in str(exc_info.value)

    # 验证在途请求顺利完成，未抛出 AttributeError
    embeddings = await embed_task
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024
    assert mock_model.call_finished is True

    # 等待后台 teardown 线程完成
    if engine._teardown_thread:
        for _ in range(100):
            if not engine._teardown_thread.is_alive():
                break
            await asyncio.sleep(0.02)
        assert not engine._teardown_thread.is_alive()

    assert engine.state == ModelState.UNLOADED
    assert engine.model is None
    assert engine.tokenizer is None


@pytest.mark.asyncio
async def test_engine_aclose_graceful_drain():
    """
    [ROBUST-01 异步优雅停机]
    验证 await engine.aclose() 能够自然等待在途请求完成，
    返回后引擎彻底处于 UNLOADED 状态，显存/内存已释放。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=16,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    mock_model = SlowMockModel(delay=0.08)
    engine.tokenizer = DummyTokenizer()
    engine.model = mock_model
    engine._set_state(ModelState.READY)

    # 发起后台任务
    t = asyncio.create_task(engine.embed(["Sentence to drain"]))
    await asyncio.sleep(0.01)

    # 异步停机
    await engine.aclose(timeout=3.0)

    # 验证任务成功完成且引擎已卸载
    res = await t
    assert len(res) == 1
    assert engine.state == ModelState.UNLOADED
    assert engine.model is None
    assert engine.tokenizer is None


def test_engine_close_synchronous_when_idle():
    """
    [ROBUST-01 闲置同步停机]
    当无活跃请求时，调用 engine.close() 立即同步完成停机，状态变为 UNLOADED，
    且重复调用 close() 幂等安全。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=16,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine.tokenizer = DummyTokenizer()
    engine.model = SlowMockModel(delay=0.0)
    engine._set_state(ModelState.READY)

    engine.close()
    assert engine.state == ModelState.UNLOADED
    assert engine.model is None
    assert engine._teardown_thread is None

    # 重复 close 幂等
    engine.close()
    assert engine.state == ModelState.UNLOADED


# =========================================================================
# FINDING-ROBUST-02: OutboxWorker.stop() Single-Writer Lock Retention
# =========================================================================

def test_outbox_worker_stop_timeout_retains_lock(tmp_path: Path):
    """
    [ROBUST-02 验证]
    模拟 OutboxWorker 后台线程正在处理耗时/挂起操作（如慢网络写 Qdrant 或推断）。
    当 stop(timeout=0.1) 超时返回而线程仍然存活时：
    修复前行为：
      无条件执行 fcntl.flock(LOCK_UN) 并关闭 lock_fd，
      导致新进程成功抢占同一 lock 文件，产生双写者与脑裂。
    修复后契约：
      1. 检测到 thread.is_alive()，禁止释放 flock 与关闭 lock_fd；
      2. 另一个 worker 实例尝试 start() 该 lock_path 时被严格拒绝（SystemExit: 3）；
      3. 待后台任务解除阻塞并退出后，再次调用 stop() 才能安全释放锁，随后其他 worker 方可启动。
    """
    lock_file = tmp_path / "robust_outbox_test.lock"

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

    entered_event = threading.Event()
    unblock_event = threading.Event()

    async def hanging_run_once(*args, **kwargs):
        # 模拟长时间阻塞的 I/O / 网络任务
        entered_event.set()
        unblock_event.wait()
        return 0

    worker1.run_once = hanging_run_once
    worker1.start()
    assert worker1._running is True
    assert entered_event.wait(timeout=2.0), "Worker did not enter hanging_run_once"
    assert worker1._thread.is_alive()

    try:
        # 调用 stop，设置极短超时 (0.1s)，必定超时
        with pytest.raises(TimeoutError):
            worker1.stop(timeout=0.1)

        # 契约断言 1：线程依然存活
        assert worker1._thread.is_alive()

        # 契约断言 2：lock_fd 绝未被置 None，仍然持有文件句柄与 flock
        assert worker1._lock_fd is not None

        # 契约断言 3：另一个 Worker 尝试抢占同一锁，必须被互斥拒绝 (SystemExit 3)
        worker2 = OutboxWorker(
            session_store=store,
            qdrant_manager=MagicMock(),
            engine=MagicMock(),
            lock_path=lock_file,
        )
        with pytest.raises(SystemExit) as exc_info:
            worker2.start()
        assert exc_info.value.code == 3

    finally:
        # 解除阻塞，允许后台线程正常退出
        unblock_event.set()
        worker1._thread.join(timeout=3.0)
        assert not worker1._thread.is_alive()

        # 再次调用 stop()，此时线程已退出，安全释放锁
        worker1.stop()
        assert worker1._lock_fd is None

    # 契约断言 4：锁释放后，新 Worker 能够成功启动
    worker3 = OutboxWorker(
        session_store=store,
        qdrant_manager=MagicMock(),
        engine=MagicMock(),
        lock_path=lock_file,
    )
    worker3.start()
    assert worker3._running is True
    worker3.stop()
    assert worker3._running is False


def test_outbox_worker_clean_stop_promptly_releases_lock(tmp_path: Path):
    """
    [ROBUST-02 正常路径验证]
    验证在空闲轮询状态下，stop() 通过 _stop_event 毫秒级唤醒并退出线程，
    及时释放 lock，后续 worker 实例可无缝启动。
    """
    lock_file = tmp_path / "prompt_stop.lock"

    store = MagicMock()
    store.count_projection_backlog.return_value = {}
    store.fetch_pending_projection_tasks.return_value = []
    store.fetch_pending_sync_tasks.return_value = []

    worker1 = OutboxWorker(
        session_store=store,
        qdrant_manager=MagicMock(),
        engine=MagicMock(),
        lock_path=lock_file,
        poll_interval=5.0,  # 故意设置较长的 poll_interval (5s)
    )

    t0 = time.monotonic()
    worker1.start()
    assert worker1._running is True

    # 即使 poll_interval=5.0s，stop() 应当在 0.5s 内迅速响应并退出
    worker1.stop(timeout=2.0)
    stop_duration = time.monotonic() - t0

    assert worker1._running is False
    assert not worker1._thread.is_alive()
    assert worker1._lock_fd is None
    assert stop_duration < 1.5, f"Stop took too long: {stop_duration:.2f}s"

    # 新 worker 立即顺利启动
    worker2 = OutboxWorker(
        session_store=store,
        qdrant_manager=MagicMock(),
        engine=MagicMock(),
        lock_path=lock_file,
    )
    worker2.start()
    assert worker2._running is True
    worker2.stop()


def test_outbox_worker_stop_before_start():
    """验证未 start() 的 worker 调用 stop() 安全幂等"""
    worker = OutboxWorker(
        session_store=MagicMock(),
        qdrant_manager=MagicMock(),
        engine=MagicMock(),
    )
    worker.stop()
    assert worker._running is False
    assert worker._lock_fd is None

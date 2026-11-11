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
from src.service.memory_service import MemoryService
from src.service.outbox_worker import OutboxWorker
from src.main import AMRApplication


@pytest.mark.asyncio
async def test_application_does_not_close_dependencies_if_server_drain_times_out():
    """
    [CRITICAL 验证]
    当 DualUDSServer.stop() 抛出 TimeoutError 时（在途 handler 超时未排空），
    AMRApplication.stop() 必须抛出 TimeoutError 并阻止关闭底层的 session_store、qdrant_manager 和 engine。
    """
    app = object.__new__(AMRApplication)
    app.outbox_worker = MagicMock()
    app.server = MagicMock()
    app.server.stop = MagicMock(side_effect=TimeoutError("Drain timeout reached with 1 active handlers still running"))
    app.memory_service = MagicMock()
    app.engine = MagicMock()
    app.session_store = MagicMock()
    app.qdrant_manager = MagicMock()

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(app.stop(), timeout=2.0)
    app.memory_service.aclose.assert_not_called()
    app.session_store.close.assert_not_called()
    app.qdrant_manager.close.assert_not_called()
    app.engine.close.assert_not_called()


@pytest.mark.asyncio
async def test_application_does_not_close_dependencies_if_worker_times_out():
    app = object.__new__(AMRApplication)
    app.outbox_worker = MagicMock()
    app.outbox_worker.stop.side_effect = TimeoutError("still running")
    app.server = MagicMock()
    app.memory_service = MagicMock()
    app.engine = MagicMock()
    app.session_store = MagicMock()
    app.qdrant_manager = MagicMock()

    with pytest.raises(TimeoutError):
        await app.stop()
    app.memory_service.aclose.assert_not_called()
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


# =========================================================================
# FINDING-ROBUST-07: UDSServer Safe Draining & Active Client Teardown
# =========================================================================

from src.interfaces.ipc.server import DualUDSServer, UDSServer
from src.interfaces.ipc.protocol import (
    ConnectionClosedError,
    make_jsonrpc_request,
    read_frame,
    write_frame,
)
import tempfile


@pytest.mark.asyncio
async def test_uds_server_idle_clients_closed_on_shutdown():
    """
    [ROBUST-07 验证 - 空闲客户端场景]
    客户端建立连接后处于空闲长连接状态（等待接收或未发送新请求）。
    当 server.stop() 调用时：
    1. 服务端应主动关闭存量空闲客户端 StreamWriter 并取消挂起任务；
    2. 客户端读取时应感知到 EOF / ConnectionClosedError；
    3. stop() 结束后，没有残留的活跃连接记录与悬挂任务。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "idle_test.sock")

        async def handler(method: str, params: dict):
            return {"echo": method}

        server = UDSServer(
            name="test_idle",
            socket_path=sock_path,
            handler=handler,
            drain_timeout=2.0,
        )
        await server.start()

        # 客户端 1、客户端 2 连接并保持空闲
        reader1, writer1 = await asyncio.wait_for(
            asyncio.open_unix_connection(path=sock_path), timeout=2.0
        )
        reader2, writer2 = await asyncio.wait_for(
            asyncio.open_unix_connection(path=sock_path), timeout=2.0
        )

        # 显式等待服务端接收两个连接（避免事件循环调度时差导致未接受）
        async def wait_server_accept(count: int, timeout: float = 2.0):
            deadline = time.monotonic() + timeout
            while len(server._active_connections) < count:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Server accepted only {len(server._active_connections)}/{count} connections")
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_server_accept(2), timeout=2.0)
        assert len(server._active_connections) == 2

        # 触发优雅关闭
        await asyncio.wait_for(server.stop(), timeout=3.0)

        assert len(server._active_connections) == 0
        assert len(server._active_handlers) == 0

        # 客户端 1 和 2 此时读取应立刻感知到 EOF / ConnectionClosedError
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(read_frame(reader1), timeout=2.0)
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(read_frame(reader2), timeout=2.0)

        writer1.close()
        await asyncio.wait_for(writer1.wait_closed(), timeout=2.0)
        writer2.close()
        await asyncio.wait_for(writer2.wait_closed(), timeout=2.0)


@pytest.mark.asyncio
async def test_uds_server_in_flight_handlers_drain_safely_before_dependencies_closed():
    """
    [ROBUST-07 验证 - 在途请求安全排空场景]
    请求已进入 handler 执行（模拟访问依赖如 Database / Engine）。
    在 handler 执行期间发起 server.stop()：
    1. server.stop() 必须等待在途请求正常完成返回（而不是中断底层资源导致报错）；
    2. 客户端成功收到正常响应；
    3. 模拟依赖在 server.stop() 返回后才被关闭，期间 dependency.is_closed 保持 False；
    4. 确保在 stop() 返回时，无任何 handler 仍在运行。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "inflight_test.sock")

        dependency_closed = False
        in_flight_handler_started = asyncio.Event()
        can_finish_handler = asyncio.Event()
        handler_completed = False

        async def handler(method: str, params: dict):
            nonlocal handler_completed
            # 标记 handler 已开始
            in_flight_handler_started.set()
            # 检查依赖是否仍存活
            assert not dependency_closed, "Dependency was accessed after being closed!"
            # 等待 stop() 启动并尝试排空
            await asyncio.wait_for(can_finish_handler.wait(), timeout=3.0)
            assert not dependency_closed, "Dependency closed while handler was running!"
            handler_completed = True
            return {"status": "success", "processed_during_shutdown": True}

        server = UDSServer(
            name="test_inflight",
            socket_path=sock_path,
            handler=handler,
            drain_timeout=3.0,
        )
        await server.start()

        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(path=sock_path), timeout=2.0
        )

        # 发送请求
        req = make_jsonrpc_request("memory.process", {"data": "test"}, req_id=999)
        await asyncio.wait_for(write_frame(writer, req), timeout=2.0)

        # 等待 handler 开始执行
        await asyncio.wait_for(in_flight_handler_started.wait(), timeout=2.0)

        # 此时在后台发起 stop()
        stop_task = asyncio.create_task(server.stop())

        # 给 stop_task 一小段时间进入 wait 阶段
        await asyncio.sleep(0.05)
        assert not stop_task.done(), "stop() should wait for in-flight handler to drain"

        # 允许 handler 完成
        can_finish_handler.set()

        # 等待 stop_task 完成
        await asyncio.wait_for(stop_task, timeout=4.0)

        # stop 成功返回后，模拟上层释放依赖
        dependency_closed = True

        assert handler_completed is True
        assert len(server._active_handlers) == 0
        assert len(server._active_connections) == 0

        # 客户端应成功收到响应
        resp = await asyncio.wait_for(read_frame(reader), timeout=2.0)
        assert resp["id"] == 999
        assert resp["result"]["status"] == "success"

        writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=2.0)


@pytest.mark.asyncio
async def test_uds_server_drain_timeout_raises_timeout_error_without_cancelling():
    """
    [ROBUST-07 验证 - 有界超时保护与线程安全]
    如果某个请求 handler 阻塞超时（如 awaiting asyncio.to_thread），
    server.stop(drain_timeout=0.2) 严禁 cancel handler 并声称成功关闭，
    而必须抛出 TimeoutError，从而阻止上层应用释放底层依赖。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "hung_test.sock")

        handler_started = asyncio.Event()
        handler_done_or_cancelled = False

        async def hung_handler(method: str, params: dict):
            nonlocal handler_done_or_cancelled
            handler_started.set()
            try:
                await asyncio.sleep(100.0)  # 模拟长时间任务（如底层线程池）
                handler_done_or_cancelled = True
            except asyncio.CancelledError:
                handler_done_or_cancelled = True
                raise
            return {"status": "never"}

        server = UDSServer(
            name="test_hung",
            socket_path=sock_path,
            handler=hung_handler,
            drain_timeout=0.2,
        )
        await server.start()

        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(path=sock_path), timeout=2.0
        )
        req = make_jsonrpc_request("hung.method", {}, req_id=500)
        await asyncio.wait_for(write_frame(writer, req), timeout=2.0)

        await asyncio.wait_for(handler_started.wait(), timeout=2.0)

        t0 = time.monotonic()
        with pytest.raises(TimeoutError, match="Drain timeout reached with 1 active handlers still running"):
            await asyncio.wait_for(server.stop(drain_timeout=0.2), timeout=2.0)
        elapsed = time.monotonic() - t0

        assert 0.18 <= elapsed <= 1.0, f"Shutdown duration unexpected: {elapsed:.2f}s"
        # 核心保证：handler 绝对没有被 cancel！
        assert handler_done_or_cancelled is False, "Active handler was cancelled unexpectedly!"
        assert len(server._active_handlers) == 1

        writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=2.0)

        # 善后测试中的挂起 handler 以免测试泄漏
        for h in list(server._active_handlers):
            h.cancel()
        await asyncio.gather(*server._active_handlers, return_exceptions=True)


@pytest.mark.asyncio
async def test_dual_uds_server_drain_on_shutdown():
    """
    [ROBUST-07 验证 - DualUDSServer 统一优雅排空]
    验证业务 Socket 和管理 Socket 都能由 DualUDSServer 协调执行优雅排空，
    并且在任一服务排空超时时正确向外传播 TimeoutError。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        biz_sock = str(Path(tmpdir) / "biz.sock")
        adm_sock = str(Path(tmpdir) / "adm.sock")

        biz_started = asyncio.Event()
        biz_can_finish = asyncio.Event()

        async def biz_handler(method: str, params: dict):
            biz_started.set()
            await asyncio.wait_for(biz_can_finish.wait(), timeout=3.0)
            return {"role": "business"}

        async def adm_handler(method: str, params: dict):
            return {"role": "admin"}

        from config.settings import AppConfig, ServerConfig
        cfg = AppConfig(
            server=ServerConfig(
                business_socket=biz_sock,
                admin_socket=adm_sock,
                socket_mode=0o600,
                drain_timeout_seconds=2.0,
            )
        )

        dual_server = DualUDSServer(
            config=cfg,
            business_handler=biz_handler,
            admin_handler=adm_handler,
        )
        await dual_server.start()

        # 分别连接业务端和管理端
        r_biz, w_biz = await asyncio.wait_for(
            asyncio.open_unix_connection(path=biz_sock), timeout=2.0
        )
        r_adm, w_adm = await asyncio.wait_for(
            asyncio.open_unix_connection(path=adm_sock), timeout=2.0
        )

        # 业务端发送在途请求
        await asyncio.wait_for(
            write_frame(w_biz, make_jsonrpc_request("test.biz", {}, req_id=1)),
            timeout=2.0,
        )
        await asyncio.wait_for(biz_started.wait(), timeout=2.0)

        async def wait_dual_server_accept(timeout: float = 2.0):
            deadline = time.monotonic() + timeout
            while len(dual_server.business_server._active_connections) < 1 or len(dual_server.admin_server._active_connections) < 1:
                if time.monotonic() > deadline:
                    raise TimeoutError("DualUDSServer failed to register active connections in time")
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_dual_server_accept(), timeout=2.0)

        # 管理端处于空闲状态
        assert len(dual_server.business_server._active_connections) == 1
        assert len(dual_server.admin_server._active_connections) == 1

        stop_task = asyncio.create_task(dual_server.stop())
        await asyncio.sleep(0.05)
        assert not stop_task.done()

        biz_can_finish.set()
        await asyncio.wait_for(stop_task, timeout=3.0)

        # 业务端收到结果
        resp_biz = await asyncio.wait_for(read_frame(r_biz), timeout=2.0)
        assert resp_biz["result"]["role"] == "business"

        # 管理端作为空闲长连接被关闭，读取到 EOF
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(read_frame(r_adm), timeout=2.0)

        w_biz.close()
        await asyncio.wait_for(w_biz.wait_closed(), timeout=2.0)
        w_adm.close()
        await asyncio.wait_for(w_adm.wait_closed(), timeout=2.0)


@pytest.mark.asyncio
async def test_dual_uds_server_stop_propagates_failure():
    """
    [ROBUST-07 验证 - DualUDSServer 失败传播]
    当 DualUDSServer 中某个 server stop() 抛出异常（如排空超时）时，
    DualUDSServer.stop() 必须向调用方传播该异常，不得静默吞掉。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        biz_sock = str(Path(tmpdir) / "biz_fail.sock")
        adm_sock = str(Path(tmpdir) / "adm_fail.sock")

        biz_started = asyncio.Event()

        async def hung_biz_handler(method: str, params: dict):
            biz_started.set()
            await asyncio.sleep(100.0)
            return {"role": "business"}

        async def adm_handler(method: str, params: dict):
            return {"role": "admin"}

        from config.settings import AppConfig, ServerConfig
        cfg = AppConfig(
            server=ServerConfig(
                business_socket=biz_sock,
                admin_socket=adm_sock,
                socket_mode=0o600,
                drain_timeout_seconds=0.2,
            )
        )

        dual_server = DualUDSServer(
            config=cfg,
            business_handler=hung_biz_handler,
            admin_handler=adm_handler,
        )
        await dual_server.start()

        r_biz, w_biz = await asyncio.wait_for(
            asyncio.open_unix_connection(path=biz_sock), timeout=2.0
        )
        await asyncio.wait_for(
            write_frame(w_biz, make_jsonrpc_request("test.biz", {}, req_id=1)),
            timeout=2.0,
        )
        await asyncio.wait_for(biz_started.wait(), timeout=2.0)

        # 停止 DualUDSServer，预期抛出 TimeoutError
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(dual_server.stop(drain_timeout=0.2), timeout=2.0)

        w_biz.close()
        await asyncio.wait_for(w_biz.wait_closed(), timeout=2.0)

        # 善后挂起任务
        for h in list(dual_server.business_server._active_handlers):
            h.cancel()
        await asyncio.gather(*dual_server.business_server._active_handlers, return_exceptions=True)


@pytest.mark.asyncio
async def test_uds_server_active_handlers_no_leak_on_notification_and_exception():
    """
    [ROBUST-07 验证 - 回归测试通知与异常场景下 _active_handlers 不泄漏]
    1. Notification 请求执行完成（无论是成功返回还是抛出异常）后，_active_handlers 必须被及时清空；
    2. 常规 Request handler 抛出业务异常返回 error response 后，_active_handlers 也必须被及时清空。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = str(Path(tmpdir) / "handler_cleanup_test.sock")

        async def handler(method: str, params: dict):
            if method == "notify.success":
                return "ok"
            elif method == "notify.error":
                raise ValueError("Notification error")
            elif method == "request.error":
                raise RuntimeError("Request failure")
            return {"echo": method}

        server = UDSServer(
            name="test_cleanup",
            socket_path=sock_path,
            handler=handler,
            drain_timeout=2.0,
        )
        await server.start()

        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(path=sock_path), timeout=2.0
        )

        # 1. 发送正常的 Notification 请求 (无 id)
        notify_req = {"jsonrpc": "2.0", "method": "notify.success", "params": {}}
        await asyncio.wait_for(write_frame(writer, notify_req), timeout=2.0)
        # 等待事件循环调度 handler 完成
        await asyncio.sleep(0.05)
        assert len(server._active_handlers) == 0, "Notification success leaked in _active_handlers!"

        # 2. 发送抛出异常的 Notification 请求
        notify_err_req = {"jsonrpc": "2.0", "method": "notify.error", "params": {}}
        await asyncio.wait_for(write_frame(writer, notify_err_req), timeout=2.0)
        await asyncio.sleep(0.05)
        assert len(server._active_handlers) == 0, "Notification error leaked in _active_handlers!"

        # 3. 发送抛出异常的常规 Request
        req_err = make_jsonrpc_request("request.error", {}, req_id=101)
        await asyncio.wait_for(write_frame(writer, req_err), timeout=2.0)
        resp = await asyncio.wait_for(read_frame(reader), timeout=2.0)
        assert resp["id"] == 101
        assert "error" in resp
        assert len(server._active_handlers) == 0, "Request exception leaked in _active_handlers!"

        # 4. 正常关闭 server，验证完全排空且无挂起 handler
        await asyncio.wait_for(server.stop(), timeout=2.0)
        assert len(server._active_handlers) == 0
        assert len(server._active_connections) == 0

        writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=2.0)


@pytest.mark.asyncio
async def test_dual_uds_server_stop_multiple_exceptions_python310_compatible():
    """
    [ROBUST-07 验证 - DualUDSServer 多异常无 ExceptionGroup 且兼容 Python 3.10]
    当 business_server 与 admin_server 同时抛出异常时，
    DualUDSServer.stop() 应安全向外传播异常（不使用 Python 3.11+ 的 ExceptionGroup），
    并且其余异常通过日志记录。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        biz_sock = str(Path(tmpdir) / "biz_multi_fail.sock")
        adm_sock = str(Path(tmpdir) / "adm_multi_fail.sock")

        from config.settings import AppConfig, ServerConfig
        cfg = AppConfig(
            server=ServerConfig(
                business_socket=biz_sock,
                admin_socket=adm_sock,
                socket_mode=0o600,
                drain_timeout_seconds=0.2,
            )
        )

        from unittest.mock import AsyncMock
        dual_server = DualUDSServer(
            config=cfg,
            business_handler=AsyncMock(),
            admin_handler=AsyncMock(),
        )

        # 模拟两个 server stop() 均抛出异常
        dual_server.business_server.stop = AsyncMock(side_effect=TimeoutError("Biz timeout"))
        dual_server.admin_server.stop = AsyncMock(side_effect=RuntimeError("Admin failure"))

        with pytest.raises(TimeoutError, match="Biz timeout"):
            await dual_server.stop()


# =========================================================================
# FINDING-ROBUST-03: MemoryService Enrichment Executor Lifecycle Management
# =========================================================================

@pytest.mark.asyncio
async def test_memory_service_close_shuts_down_executor():
    """
    [ROBUST-03 验证: 线程池回收与优雅关闭]
    验证 MemoryService.close() 正常关闭富化线程池，线程池标记已关闭且工作线程被终止。
    """
    store = MagicMock()
    qdrant = MagicMock()
    service = MemoryService(
        engine=MagicMock(),
        qdrant_manager=qdrant,
        session_store=store,
    )
    store.get_memory.return_value = {"memory_id": "mem_001", "content": "text"}
    qdrant.get_point_by_memory_id.return_value = {"payload": {"meta": {"foo": "bar"}}}

    # 先发起一次富化，触发线程池 worker 线程的真实启动
    res = await service.memory_get("mem_001")
    assert res is not None
    assert res["enrichment"] == "ok"
    assert not service._enrich_executor._shutdown

    # 执行关闭
    service.close(timeout=2.0)
    assert service._closed is True
    assert service._enrich_executor._shutdown is True

    # 幂等性：再次调用 close 不会抛出异常
    service.close()


@pytest.mark.asyncio
async def test_memory_service_in_flight_enrichment_drained_before_close():
    """
    [ROBUST-03 验证: 在途富化排空与资源保护]
    验证在途的富化操作不会在 closed 资源上崩溃：
    1. close() 等待在途任务完成；
    2. close() 后外部依赖（如 Qdrant / Store）关闭；
    3. 后续新发起的 memory_get 请求被拦截，直接降级为 degraded，绝对不向 closed executor 提交任务。
    """
    store = MagicMock()
    qdrant = MagicMock()
    store.get_memory.return_value = {"memory_id": "mem_slow", "content": "slow content"}

    in_call_event = threading.Event()
    finish_call_event = threading.Event()
    resource_closed = False

    def slow_point_lookup(mid):
        if resource_closed:
            raise RuntimeError("CRITICAL: Accessing closed resource!")
        in_call_event.set()
        # 等待外部指令再返回
        finish_call_event.wait(timeout=2.0)
        return {"payload": {"meta": {"tags": ["in_flight"]}}}

    qdrant.get_point_by_memory_id = slow_point_lookup

    service = MemoryService(
        engine=MagicMock(),
        qdrant_manager=qdrant,
        session_store=store,
    )

    # 异步启动 memory_get
    enrich_task = asyncio.create_task(service.memory_get("mem_slow"))

    # 等待确认在途任务已经进入 qdrant.get_point_by_memory_id
    await asyncio.to_thread(in_call_event.wait, 2.0)
    assert in_call_event.is_set()

    # 异步启动 service.aclose()
    async def drain_and_close():
        await service.aclose(timeout=2.0)
        nonlocal resource_closed
        resource_closed = True

    close_task = asyncio.create_task(drain_and_close())

    # 稍作等待，确保 close 正在等待在途任务
    await asyncio.sleep(0.05)
    assert not close_task.done(), "close_task should wait for in-flight enrichment"

    # 此时如果来了一个新的 memory_get 请求，必须直接 degraded，不能进入线程池
    new_req = await service.memory_get("mem_slow")
    assert new_req["enrichment"] == "degraded"

    # 释放在途任务
    finish_call_event.set()
    res = await enrich_task
    await close_task

    # 验证在途请求成功获取结果且没有访问关闭后的资源
    assert res["enrichment"] == "ok"
    assert res["meta"]["tags"] == ["in_flight"]
    assert resource_closed is True


@pytest.mark.asyncio
async def test_memory_service_closed_rejects_new_enrichments():
    """
    [ROBUST-03 验证: 关闭后新请求防线]
    已 closed 的 MemoryService 在调用 memory_get 时，安全退化为 degraded，
    绝不向已被 shutdown 的 executor 提交任务抛出 RuntimeError。
    """
    store = MagicMock()
    qdrant = MagicMock()
    store.get_memory.return_value = {"memory_id": "mem_002", "content": "some text"}
    service = MemoryService(
        engine=MagicMock(),
        qdrant_manager=qdrant,
        session_store=store,
    )
    service.close()
    assert service._closed is True

    # 再次发起 memory_get，应该安全返回 SQLite 数据，enrichment 为 degraded
    res = await service.memory_get("mem_002")
    assert res is not None
    assert res["memory_id"] == "mem_002"
    assert res["content"] == "some text"
    assert res["enrichment"] == "degraded"
    # Qdrant 绝不应被调用
    qdrant.get_point_by_memory_id.assert_not_called()


@pytest.mark.asyncio
async def test_memory_service_concurrent_closes_both_wait_for_in_flight_work():
    """
    [ROBUST-03 验证: 并发双 close 等待在途任务]
    验证存在两个并发 closer 时：
    1. 两个 close() 调用都会阻塞等待正在执行的在途富化任务完成；
    2. 第二个 close() 绝不提前返回（避免过早关闭 Qdrant 等外部资源）；
    3. 在途富化安全执行完毕且不被破坏。
    """
    store = MagicMock()
    qdrant = MagicMock()
    store.get_memory.return_value = {"memory_id": "mem_concurrent", "content": "test content"}

    in_work_event = threading.Event()
    release_work_event = threading.Event()
    work_finished = threading.Event()

    def slow_lookup(mid):
        in_work_event.set()
        release_work_event.wait(timeout=2.0)
        work_finished.set()
        return {"payload": {"meta": {"done": True}}}

    qdrant.get_point_by_memory_id = slow_lookup

    service = MemoryService(
        engine=MagicMock(),
        qdrant_manager=qdrant,
        session_store=store,
    )

    # 启动在途任务
    get_task = asyncio.create_task(service.memory_get("mem_concurrent"))
    await asyncio.to_thread(in_work_event.wait, 2.0)
    assert in_work_event.is_set()

    closer1_finished = False
    closer2_finished = False

    async def closer_1():
        nonlocal closer1_finished
        await service.aclose(timeout=2.0)
        closer1_finished = True

    async def closer_2():
        nonlocal closer2_finished
        await service.aclose(timeout=2.0)
        closer2_finished = True

    c1_task = asyncio.create_task(closer_1())
    c2_task = asyncio.create_task(closer_2())

    # 稍微等待，确保两个 closer 均已调用 aclose
    await asyncio.sleep(0.05)
    # 验证两个 closer 都在等待中，没有一个提前返回
    assert not closer1_finished, "Closer 1 should not have finished before work is done"
    assert not closer2_finished, "Closer 2 should not have finished before work is done"
    assert not work_finished.is_set()

    # 释放工作任务
    release_work_event.set()

    await asyncio.gather(c1_task, c2_task, get_task)

    assert closer1_finished is True
    assert closer2_finished is True
    assert work_finished.is_set()
    assert service._closed is True


@pytest.mark.asyncio
async def test_memory_service_close_waits_for_timed_out_asyncio_enrichment():
    """
    [ROBUST-03 验证: asyncio 层面超时的富化任务在 executor 中继续运行，close 必须等待其完成]
    验证当 memory_get 触发 asyncio.wait_for 超时（0.2s）之后：
    1. memory_get 降级返回 degraded；
    2. 底层线程池中的 Qdrant 任务依然在运行；
    3. 随后触发的 close() / aclose() 依然会等待此在途任务执行完成，
       绝不提前 shutdown 导致正在运行的 Qdrant 查询被意外切断或资源提前关闭。
    """
    store = MagicMock()
    qdrant = MagicMock()
    store.get_memory.return_value = {"memory_id": "mem_timeout", "content": "content"}

    in_lookup_event = threading.Event()
    allow_finish_event = threading.Event()
    qdrant_closed = False
    qdrant_accessed_after_close = False

    def hung_lookup(mid):
        nonlocal qdrant_accessed_after_close
        in_lookup_event.set()
        # 等待超过 asyncio.wait_for(0.2s)
        allow_finish_event.wait(timeout=2.0)
        if qdrant_closed:
            qdrant_accessed_after_close = True
        return {"payload": {"meta": {"status": "eventually_done"}}}

    qdrant.get_point_by_memory_id = hung_lookup

    service = MemoryService(
        engine=MagicMock(),
        qdrant_manager=qdrant,
        session_store=store,
    )

    # 发起请求，0.2s 后在 asyncio 层超时
    get_res = await service.memory_get("mem_timeout")
    # asyncio 层应该已经返回 degraded
    assert get_res["enrichment"] == "degraded"
    assert in_lookup_event.is_set()

    # 此时底层 hung_lookup 仍在线程池中运行
    close_done = False

    async def do_close():
        nonlocal close_done, qdrant_closed
        await service.aclose(timeout=2.0)
        qdrant_closed = True
        close_done = True

    close_task = asyncio.create_task(do_close())

    # 确认 close 正在等待底层任务
    await asyncio.sleep(0.05)
    assert not close_done, "Close must wait for timed-out enrichment executor work to finish"

    # 允许底层任务完成
    allow_finish_event.set()
    await close_task

    assert close_done is True
    assert qdrant_closed is True
    assert not qdrant_accessed_after_close
    assert service._closed is True


@pytest.mark.asyncio
async def test_memory_service_no_work_submitted_after_shutdown_begins():
    """
    [ROBUST-03 验证: shutdown 开始后不再允许提交任何新任务]
    验证当 close 已经启动（_closing 为 True 但仍在等待在途任务完成时），
    新到来的 memory_get 请求被原子拦截，直接降级，绝不向 executor 提交新任务。
    """
    store = MagicMock()
    qdrant = MagicMock()
    store.get_memory.return_value = {"memory_id": "mem_atomic", "content": "content"}

    in_work_event = threading.Event()
    release_work_event = threading.Event()

    call_count = 0

    def slow_lookup(mid):
        nonlocal call_count
        call_count += 1
        in_work_event.set()
        release_work_event.wait(timeout=2.0)
        return {"payload": {"meta": {"count": call_count}}}

    qdrant.get_point_by_memory_id = slow_lookup

    service = MemoryService(
        engine=MagicMock(),
        qdrant_manager=qdrant,
        session_store=store,
    )

    # 1. 提交第一个在途任务
    first_task = asyncio.create_task(service.memory_get("mem_atomic"))
    await asyncio.to_thread(in_work_event.wait, 2.0)
    assert in_work_event.is_set()

    # 2. 启动 close()（此时处于 closing 状态但尚未关闭）
    close_task = asyncio.create_task(service.aclose(timeout=2.0))
    await asyncio.sleep(0.05)
    assert service._closing is True
    assert service._closed is False

    # 3. 尝试在此刻提交新的 memory_get
    second_res = await service.memory_get("mem_atomic")
    # 必须直接降级，且 call_count 不增加（未提交给 executor）
    assert second_res["enrichment"] == "degraded"
    assert call_count == 1

    # 释放第一个任务
    release_work_event.set()
    first_res = await first_task
    await close_task

    assert first_res["enrichment"] == "ok"
    assert call_count == 1
    assert service._closed is True


@pytest.mark.asyncio
async def test_application_stop_calls_memory_service_aclose_in_sequence():
    """
    验证 AMRApplication.stop() 的停机序列：
    1. server.stop() 排空并关闭请求；
    2. memory_service.aclose() 关闭在途任务并关闭 executor；
    3. engine, session_store, qdrant_manager 关闭。
    若 memory_service.aclose 抛出异常（例如超时），后续依赖不能被关闭。
    """
    call_order = []

    app = object.__new__(AMRApplication)
    app.outbox_worker = MagicMock()
    app.outbox_worker.stop = MagicMock(side_effect=lambda: call_order.append("outbox_worker"))

    async def fake_server_stop():
        call_order.append("server")

    app.server = MagicMock()
    app.server.stop = MagicMock(side_effect=fake_server_stop)

    async def fake_mem_aclose():
        call_order.append("memory_service")

    app.memory_service = MagicMock()
    app.memory_service.aclose = MagicMock(side_effect=fake_mem_aclose)

    async def fake_engine_aclose():
        call_order.append("engine")

    app.engine = MagicMock()
    app.engine.aclose = MagicMock(side_effect=fake_engine_aclose)

    app.session_store = MagicMock()
    app.session_store.close = MagicMock(side_effect=lambda: call_order.append("session_store"))

    app.qdrant_manager = MagicMock()
    app.qdrant_manager.close = MagicMock(side_effect=lambda: call_order.append("qdrant"))

    await app.stop()

    assert call_order == [
        "outbox_worker",
        "server",
        "memory_service",
        "engine",
        "session_store",
        "qdrant",
    ]


@pytest.mark.asyncio
async def test_application_does_not_close_dependencies_if_memory_service_fails():
    """
    当 memory_service.aclose() 抛出异常（例如任务超时）时，
    AMRApplication.stop() 必须抛出异常并阻止关闭底层的 engine、session_store 和 qdrant_manager。
    """
    app = object.__new__(AMRApplication)
    app.outbox_worker = MagicMock()
    app.server = MagicMock()

    async def fake_server_stop():
        pass

    app.server.stop = MagicMock(side_effect=fake_server_stop)

    async def hung_mem_aclose():
        raise TimeoutError("MemoryService close timed out")

    app.memory_service = MagicMock()
    app.memory_service.aclose = MagicMock(side_effect=hung_mem_aclose)

    app.engine = MagicMock()
    app.session_store = MagicMock()
    app.qdrant_manager = MagicMock()

    with pytest.raises(TimeoutError, match="MemoryService close timed out"):
        await app.stop()

    app.engine.close.assert_not_called()
    app.session_store.close.assert_not_called()
    app.qdrant_manager.close.assert_not_called()


@pytest.mark.asyncio
async def test_memory_service_concurrent_close_timeout_when_first_closer_times_out():
    """
    当第一个 closer 因在途任务超时抛出 TimeoutError 时，
    后续并发的 closer 也应感知到超时并抛出 TimeoutError，而不是挂死或静默返回。
    """
    store = MagicMock()
    qdrant = MagicMock()
    store.get_memory.return_value = {"memory_id": "mem_concurrent_timeout", "content": "test"}

    in_work_event = threading.Event()
    release_work_event = threading.Event()

    def hung_lookup(mid):
        in_work_event.set()
        release_work_event.wait(timeout=2.0)
        return {"payload": {"meta": {"done": True}}}

    qdrant.get_point_by_memory_id = hung_lookup

    service = MemoryService(
        engine=MagicMock(),
        qdrant_manager=qdrant,
        session_store=store,
    )

    get_task = asyncio.create_task(service.memory_get("mem_concurrent_timeout"))
    await asyncio.to_thread(in_work_event.wait, 2.0)
    assert in_work_event.is_set()

    # 启动两个短超时的 aclose
    t1 = asyncio.create_task(service.aclose(timeout=0.1))
    await asyncio.sleep(0.02)
    t2 = asyncio.create_task(service.aclose(timeout=0.1))

    res1, res2 = await asyncio.gather(t1, t2, return_exceptions=True)
    assert isinstance(res1, TimeoutError)
    assert isinstance(res2, TimeoutError)

    assert service._closed is False

    release_work_event.set()
    await get_task
    await service.aclose(timeout=2.0)
    assert service._closed is True



@pytest.mark.asyncio
async def test_memory_service_waiter_recovers_after_first_close_times_out():
    store = MagicMock()
    store.get_memory.return_value = {"memory_id": "m", "content": "x"}
    entered, release = threading.Event(), threading.Event()
    qdrant = MagicMock()
    def lookup(_):
        entered.set()
        release.wait(3)
        return None
    qdrant.get_point_by_memory_id = lookup
    service = MemoryService(engine=MagicMock(), qdrant_manager=qdrant, session_store=store)
    request = asyncio.create_task(service.memory_get("m"))
    assert await asyncio.to_thread(entered.wait, 2)
    first = asyncio.create_task(service.aclose(timeout=0.2))
    await asyncio.sleep(0.05)
    second = asyncio.create_task(service.aclose())
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(first, 2)
    assert not second.done()
    release.set()
    await asyncio.wait_for(second, 2)
    await request
    assert service._closed

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
    app.engine = MagicMock()
    app.session_store = MagicMock()
    app.qdrant_manager = MagicMock()

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(app.stop(), timeout=2.0)
    app.session_store.close.assert_not_called()
    app.qdrant_manager.close.assert_not_called()
    app.engine.close.assert_not_called()


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

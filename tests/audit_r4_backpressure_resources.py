"""
R4 引擎红队审计：背压队列、资源句柄、线程泄漏与指标污染测试

覆盖项：
1. max_queue_size 边界值（恰好满与超限背压）→ 超限抛 QueueFullError(N/N)；完成后深度归零。
2. 并发 embed 请求活跃计数 (_active_requests) 与 total_inferences 统计严格准确。
3. get_model_status 诊断字段完备性（state/unhealthy/queue_depth/last_error 等）。
4. [缺陷复现] last_error 在故障恢复为 READY 后未被清理，诊断接口持续上报陈旧错误。
5. [缺陷复现] 未显式调用 close() 的引擎实例因 watchdog 绑定强引用发生永久内存泄漏与线程泄漏。
6. [缺陷复现] 重复/多个 engine 初始化时全局指标污染（__init__ 盲目重置 model_unhealthy 为 0.0）。
7. [缺陷复现] loader 线程在 close() 后未被 join，悬挂在后台持续占用 _load_exec_lock。
"""

import asyncio
import gc
from pathlib import Path
import threading
import time
from unittest.mock import MagicMock
import weakref

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import BGEM3Engine, EngineError, ModelState, QueueFullError
from src.service.metrics import METRICS


class DummyModel:
    def __init__(self, hidden_dim: int = 1024):
        self.hidden_dim = hidden_dim
        self.device = "cpu"

    def to(self, device: str):
        self.device = device
        return self

    def half(self):
        return self

    def eval(self):
        return self

    def __call__(self, **kwargs):
        input_ids = kwargs.get("input_ids")
        batch_size = input_ids.shape[0] if input_ids is not None else 1
        hidden = torch.ones((batch_size, 4, self.hidden_dim), dtype=torch.float32)
        mock_output = MagicMock()
        mock_output.last_hidden_state = hidden
        return mock_output


class DummyTokenizer:
    def __call__(self, texts, **kwargs):
        batch_size = len(texts)
        return {
            "input_ids": torch.zeros((batch_size, 4), dtype=torch.long),
            "attention_mask": torch.ones((batch_size, 4), dtype=torch.long),
        }


@pytest.mark.asyncio
async def test_max_queue_size_exact_boundary_and_overload():
    """验证 max_queue_size 边界背压：恰好满正常排队，超额立即抛出 QueueFullError，完成后深度精准归零"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=3,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine.tokenizer = DummyTokenizer()
    engine.model = DummyModel()
    engine._set_state(ModelState.READY)

    pause_event = threading.Event()
    orig_infer = engine._infer_batch_sync

    def blocking_infer(batch):
        pause_event.wait(timeout=3.0)
        return orig_infer(batch)

    engine._infer_batch_sync = blocking_infer

    try:
        # 并发启动 3 个请求（恰好达到 max_queue_size=3）
        t1 = asyncio.create_task(engine.embed(["req1"]))
        t2 = asyncio.create_task(engine.embed(["req2"]))
        t3 = asyncio.create_task(engine.embed(["req3"]))
        await asyncio.sleep(0.05)

        status = await engine.get_model_status()
        assert status["queue_depth"] == 3

        # 第 4 个请求立即被背压拒绝并抛出 QueueFullError
        with pytest.raises(QueueFullError) as exc_info:
            await engine.embed(["req4_rejected"])

        err_msg = str(exc_info.value)
        assert "Inference queue full (3/3)" in err_msg

        # 释放执行，3 个请求成功完成
        pause_event.set()
        r1, r2, r3 = await asyncio.gather(t1, t2, t3)
        assert len(r1) == 1 and len(r2) == 1 and len(r3) == 1

        # 深度精准归零
        status_after = await engine.get_model_status()
        assert status_after["queue_depth"] == 0

        # 新请求可再次正常执行
        r_new = await engine.embed(["req_post_drain"])
        assert len(r_new) == 1
    finally:
        pause_event.set()
        engine.close()


def test_concurrent_embed_active_requests_accounting_accuracy():
    """并发 10 个线程发起 embed，验证 _active_requests 计数精准性与最终归零"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=32,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine.tokenizer = DummyTokenizer()
    engine.model = DummyModel()
    engine._set_state(ModelState.READY)

    concurrency = 10
    barrier = threading.Barrier(concurrency)
    errors = []

    def worker(idx: int):
        try:
            barrier.wait(timeout=3.0)
            res = asyncio.run(engine.embed([f"Text {idx}"]))
            assert len(res) == 1
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    try:
        assert len(errors) == 0, f"Concurrent workers failed: {errors}"
        with engine._queue_lock:
            assert engine._active_requests == 0
        assert engine._total_inferences == concurrency
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_get_model_status_diagnostics_fields():
    """验证 get_model_status 返回全部关键诊断字段"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=16,
        idle_timeout_seconds=60,
    )
    engine = BGEM3Engine(config=cfg)
    try:
        status = await engine.get_model_status()
        expected_keys = {
            "state",
            "allocated_mb",
            "reserved_mb",
            "driver_used_mb",
            "queue_depth",
            "max_queue_size",
            "total_inferences",
            "device",
            "idle_timeout_seconds",
            "last_error",
            "unhealthy",
        }
        assert expected_keys.issubset(status.keys())
        assert status["state"] == "UNLOADED"
        assert status["max_queue_size"] == 16
        assert status["unhealthy"] is False
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_stale_last_error_persists_after_recovery():
    """
    [缺陷审计与复现 · 契约断言]
    引擎首次加载失败时记录 last_error（如 "Disk read failure"）。
    修复后重新加载并成功进入 READY 且 unhealthy=False。
    契约期望：成功就绪后应清除历史故障文本，status["last_error"] 应恢复为 None。
    实际观察：_set_state() 在传入 error_msg=None 时忽略赋值，导致 last_error 永远保留历史报错字符串。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
    )
    engine = BGEM3Engine(config=cfg)

    # 1. 触发加载错误
    engine._load_model_sync = MagicMock(side_effect=RuntimeError("Disk read failure"))
    with pytest.raises(EngineError):
        await engine.load_model()

    status_err = await engine.get_model_status()
    assert status_err["state"] == "ERROR"
    assert status_err["last_error"] == "Disk read failure"

    # 2. 修复 loader 并恢复
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())
    await engine.load_model()

    status_ready = await engine.get_model_status()
    assert status_ready["state"] == "READY"
    assert status_ready["unhealthy"] is False

    try:
        # 契约断言：既然已经就绪且健康，last_error 必须为 None
        assert status_ready["last_error"] is None, (
            f"Stale diagnostic defect: last_error was not cleared after recovery to READY; "
            f"still reports '{status_ready['last_error']}'"
        )
    finally:
        engine.close()


def test_unclosed_engine_watchdog_thread_and_memory_leak():
    """
    [缺陷审计与复现 · 契约断言]
    当 BGEM3Engine 实例未显式调用 close() 时被引用方丢弃。
    契约期望：实例应能在垃圾回收阶段被自动回收，守护线程应能在宿主释放后退出。
    实际观察：watchdog 线程内部循环持有对 self._watchdog_loop 的强绑定引用，
    形成 threading._active -> thread -> _watchdog_loop -> engine 的强引用环。
    导致 engine 永远无法被垃圾回收，watchdog 线程永久以 50ms 间隔空转，产生隐蔽的严重线程与内存泄漏。
    """
    cfg = ModelConfig(name_or_path="mock/bge-m3", device="cpu", fp16=False)
    engine = BGEM3Engine(config=cfg)
    ref = weakref.ref(engine)
    wd_thread = engine._watchdog_thread

    # 丢弃强引用并触发 GC
    del engine
    gc.collect()

    try:
        # 契约断言：未被外部持有时应该可被 GC 回收
        assert ref() is None, (
            "Resource leak defect: BGEM3Engine instance cannot be garbage-collected without explicit close() "
            "due to watchdog thread cyclic reference."
        )
        assert not wd_thread.is_alive()
    finally:
        # 测试自身清理，避免泄漏干扰后续用例
        if ref() is not None:
            ref().close()


def test_multiple_engine_metrics_pollution():
    """
    [缺陷审计与复现 · 契约断言]
    当存在活跃的 Engine 1 处于 unhealthy=True（如 watchdog stall）时，
    外部创建了新的 Engine 2。
    契约期望：Engine 2 初始化不应当擦除 Engine 1 的全局 unhealthy 指标报警。
    实际观察：BGEM3Engine.__init__ 中硬编码 METRICS.set_gauge('model_unhealthy', 0.0)，
    直接将全局指标重置为 0.0，使 Prometheus/admin metrics 瞬时丢失模型不健康的报警状态。
    """
    cfg = ModelConfig(name_or_path="mock/bge-m3", device="cpu", fp16=False)
    e1 = BGEM3Engine(config=cfg)
    e1._set_unhealthy(True)

    # 确认 e1 设置了 gauge=1.0
    assert METRICS.snapshot()["gauges"]["model_unhealthy"] == 1.0

    # 初始化 e2
    e2 = BGEM3Engine(config=cfg)
    try:
        # 契约断言：全局 gauge 依然应该反映 e1 的不健康状态 (1.0)
        current_gauge = METRICS.snapshot()["gauges"]["model_unhealthy"]
        assert current_gauge == 1.0, (
            f"Metric pollution defect: Engine 2 __init__ reset global model_unhealthy gauge to {current_gauge}, "
            f"masking Engine 1's unhealthy status."
        )
    finally:
        e1.close()
        e2.close()


def test_loader_thread_not_joined_on_close():
    """
    [缺陷审计与复现 · 契约断言]
    当模型加载线程正在运行（卡在外部 I/O 或权重反序列化）时外部调用 close()。
    契约期望：close() 应当对 loader 线程进行 join 或具备受管停止机制，确保返回时没有残留的 loader 线程。
    实际观察：close() 既不保留 load_thread 句柄，也不对其 join，导致 bge-loader-1 悬挂运行并在后台持续持有 _load_exec_lock。
    """
    cfg = ModelConfig(name_or_path="mock/bge-m3", device="cpu", fp16=False)
    engine = BGEM3Engine(config=cfg)

    started_event = threading.Event()
    finish_event = threading.Event()

    def blocked_loader():
        started_event.set()
        finish_event.wait(timeout=2.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = blocked_loader
    engine._trigger_load()

    assert started_event.wait(timeout=2.0)
    current_epoch = engine._load_epoch
    loader_name = f"bge-loader-{current_epoch}"

    # 确认 loader 线程活跃
    loader_thread = next((t for t in threading.enumerate() if t.name == loader_name), None)
    assert loader_thread is not None and loader_thread.is_alive()

    # 调用 close()
    engine.close()

    try:
        # 契约断言：close() 返回后，loader 线程应当已退出或被终止
        assert not loader_thread.is_alive(), (
            f"Thread leak defect: {loader_name} thread is still alive after engine.close() returned!"
        )
    finally:
        finish_event.set()
        if loader_thread and loader_thread.is_alive():
            loader_thread.join(timeout=1.0)

"""
单飞加载测试：跨线程/跨事件循环 single-flight 机制
1. 两个独立线程各自 asyncio.run(engine.embed(...)) 并发触发冷启动，实际加载函数只执行 1 次。
2. 请求预算超时抛 ModelLoadingPendingError，共享加载仍继续并在释放后成功就绪。
3. 加载期间代次变更（如 unload_model），迟到的加载结果被安全丢弃，不挂载且不破坏当前状态。
"""

import asyncio
import threading
import time
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import BGEM3Engine, ModelState
from src.core.errors import ModelLoadingPendingError


class DummyModel:
    def __init__(self):
        self.device = "cpu"

    def to(self, device):
        self.device = device
        return self

    def half(self):
        return self

    def eval(self):
        return self

    def __call__(self, **kwargs):
        batch_size = kwargs.get("input_ids", torch.zeros((1, 4))).shape[0]
        hidden = torch.ones((batch_size, 4, 1024), dtype=torch.float32)
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


def test_concurrent_threads_singleflight_load():
    """两个线程各自 asyncio.run 触发冷启动，验证 single-flight 保证加载函数仅被调用 1 次"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        request_load_wait_seconds=5.0,
        load_timeout_seconds=10.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    load_count = 0
    load_lock = threading.Lock()

    def fake_load_sync():
        nonlocal load_count
        with load_lock:
            load_count += 1
        time.sleep(0.15)  # 保证两个线程并发交叠
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = fake_load_sync

    results = [None, None]
    exceptions = [None, None]
    barrier = threading.Barrier(2)

    def worker(idx: int):
        try:
            barrier.wait(timeout=2.0)
            res = asyncio.run(engine.embed([f"Thread {idx} text"]))
            results[idx] = res
        except Exception as e:
            exceptions[idx] = e

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1,))

    t1.start()
    t2.start()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    try:
        assert exceptions[0] is None, f"Thread 0 raised: {exceptions[0]}"
        assert exceptions[1] is None, f"Thread 1 raised: {exceptions[1]}"
        assert len(results[0]) == 1 and len(results[0][0]) == 1024
        assert len(results[1]) == 1 and len(results[1][0]) == 1024
        assert load_count == 1, f"Expected 1 load execution, got {load_count}"
        assert engine.state == ModelState.READY
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_request_timeout_budget_does_not_abort_shared_load():
    """单个请求等待预算耗尽时抛 ModelLoadingPendingError，但共享加载不被终止且最终就绪"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        request_load_wait_seconds=0.1,  # 短请求预算
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    load_proceed_event = threading.Event()

    def slow_load_sync():
        load_proceed_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = slow_load_sync

    try:
        # 触发 embed，由于加载阻塞在 event，0.1s 预算耗尽后应抛出 ModelLoadingPendingError
        with pytest.raises(ModelLoadingPendingError) as exc_info:
            await engine.embed(["Budget test"])

        err = exc_info.value
        assert err.model_state == "LOADING"
        assert err.retry_after_ms == 1000

        # 此时共享加载仍在进行中
        assert engine.state == ModelState.LOADING
        assert engine._load_exec_lock.locked()

        # 释放加载事件，允许模型完成加载
        load_proceed_event.set()

        # 等待加载就绪
        await engine._wait_for_ready(timeout=2.0)
        assert engine.state == ModelState.READY
        assert engine.model is not None

        # 后续请求正常完成
        res = await engine.embed(["Retry test"])
        assert len(res) == 1
        assert len(res[0]) == 1024
    finally:
        load_proceed_event.set()
        engine.close()


@pytest.mark.asyncio
async def test_load_completion_with_stale_epoch_discards_model():
    """加载完成但 epoch 发生变更时，结果被安全丢弃，不覆盖当前状态与句柄"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    started_event = threading.Event()
    finish_event = threading.Event()

    def paused_load_sync():
        started_event.set()
        finish_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = paused_load_sync

    try:
        engine._trigger_load()
        assert started_event.wait(timeout=2.0), "Loader failed to start"
        assert engine.state == ModelState.LOADING
        initial_epoch = engine._load_epoch

        # 在加载在途中执行主动卸载，这会递增 _load_epoch 并转入 UNLOADED
        await engine.unload_model()
        assert engine.state == ModelState.UNLOADED
        assert engine._load_epoch > initial_epoch

        # 允许原本的加载线程返回
        finish_event.set()

        # 等待后台加载线程退出并完成 cleanup
        for _ in range(50):
            if not engine._load_exec_lock.locked():
                break
            await asyncio.sleep(0.02)

        # 校验过期对象被丢弃，未挂载到 engine
        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        finish_event.set()
        engine.close()

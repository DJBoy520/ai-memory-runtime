"""
R4 引擎红队审计：Single-Flight 加载机制攻击与验证

覆盖项：
1. 20 个独立线程各自 asyncio.run(engine.embed(...)) 并发冷启动：实际 loader 调用恰 1 次。
2. 两个事件循环并发执行 load_model/embed：一个等待预算超时（ModelLoadingPendingError），共享加载继续并最终 READY。
3. loader 迟到成功但 epoch 已变更（unload_model）→ 丢弃、不挂载、不泄漏。
4. loader 迟到成功但 epoch 已变更（close）→ 丢弃、不挂载、不泄漏。
5. loader 迟到成功但 epoch 已变更（watchdog timeout）→ 丢弃、不挂载、不泄漏。
"""

import asyncio
from pathlib import Path
import threading
import time
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import BGEM3Engine, ModelState
from src.core.errors import ModelLoadingPendingError


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


def test_twenty_threads_cold_start_singleflight():
    """20 个独立线程各自 asyncio.run(engine.embed(...)) 冷启动，验证 single-flight loader 调用恰好 1 次"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=64,
        request_load_wait_seconds=10.0,
        load_timeout_seconds=10.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    load_invocations = 0
    load_lock = threading.Lock()

    def instrumented_loader():
        nonlocal load_invocations
        with load_lock:
            load_invocations += 1
        time.sleep(0.2)  # 保证 20 线程完全交叠
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = instrumented_loader

    concurrency = 20
    results = [None] * concurrency
    errors = [None] * concurrency
    barrier = threading.Barrier(concurrency)

    def worker(idx: int):
        try:
            barrier.wait(timeout=5.0)
            res = asyncio.run(engine.embed([f"Prompt for thread {idx}"]))
            results[idx] = res
        except Exception as e:
            errors[idx] = e

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    try:
        # 断言所有 20 个线程均无异常且获取到预期维度的 embedding
        for i in range(concurrency):
            assert errors[i] is None, f"Thread {i} raised error: {errors[i]}"
            assert results[i] is not None, f"Thread {i} returned None"
            assert len(results[i]) == 1, f"Thread {i} batch size mismatch"
            assert len(results[i][0]) == 1024, f"Thread {i} vector dim mismatch"

        # 核心断言：loader 恰调用 1 次
        assert load_invocations == 1, f"Expected exactly 1 load call, got {load_invocations}"
        assert engine.state == ModelState.READY
    finally:
        engine.close()


def test_concurrent_event_loops_timeout_budget_shared_load_completes():
    """两个事件循环同时接入：一个请求等待预算超时抛 ModelLoadingPendingError，共享加载仍继续并最终 READY"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        request_load_wait_seconds=0.1,  # 短等待预算
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    proceed_event = threading.Event()

    def controlled_loader():
        proceed_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = controlled_loader

    embed_result = None
    embed_error = None
    load_error = None

    barrier = threading.Barrier(2)

    def loop_embed():
        nonlocal embed_result, embed_error
        barrier.wait(timeout=2.0)
        try:
            embed_result = asyncio.run(engine.embed(["Short timeout embed"]))
        except Exception as e:
            embed_error = e

    def loop_load():
        nonlocal load_error
        barrier.wait(timeout=2.0)
        try:
            asyncio.run(engine.load_model())
        except Exception as e:
            load_error = e

    t_embed = threading.Thread(target=loop_embed)
    t_load = threading.Thread(target=loop_load)

    t_embed.start()
    t_load.start()

    # t_embed 应该在 0.1s + 裕量后因为预算耗尽抛出 ModelLoadingPendingError
    t_embed.join(timeout=2.0)
    assert embed_error is not None, "Expected embed to raise ModelLoadingPendingError"
    assert isinstance(embed_error, ModelLoadingPendingError)
    assert embed_error.retry_after_ms == 1000

    # 此时共享加载仍在进行，状态仍为 LOADING
    assert engine.state == ModelState.LOADING
    assert engine._load_exec_lock.locked()

    # 释放 loader 完成加载
    proceed_event.set()
    t_load.join(timeout=3.0)

    try:
        assert load_error is None, f"load_model raised: {load_error}"
        assert engine.state == ModelState.READY
        assert engine.model is not None

        # 超时后重试能够立即使用已经 READY 的模型
        res_retry = asyncio.run(engine.embed(["Retry after ready"]))
        assert len(res_retry) == 1
        assert len(res_retry[0]) == 1024
    finally:
        proceed_event.set()
        engine.close()


@pytest.mark.asyncio
async def test_late_loader_discarded_after_unload():
    """loader 迟到完成但在中途被 unload_model() 推进了 epoch → 丢弃、不挂载、不泄漏"""
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

    def slow_loader():
        started_event.set()
        finish_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = slow_loader

    try:
        engine._trigger_load()
        assert started_event.wait(timeout=2.0), "Loader thread failed to start"
        assert engine.state == ModelState.LOADING
        epoch_before_unload = engine._load_epoch

        # 主动卸载，这会递增 _load_epoch 并转入 UNLOADED
        await engine.unload_model()
        assert engine.state == ModelState.UNLOADED
        assert engine._load_epoch > epoch_before_unload

        # 允许原本的 loader 线程返回
        finish_event.set()

        # 等待后台加载线程退出并释放执行锁
        for _ in range(50):
            if not engine._load_exec_lock.locked():
                break
            await asyncio.sleep(0.02)

        assert not engine._load_exec_lock.locked()
        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        finish_event.set()
        engine.close()


@pytest.mark.asyncio
async def test_late_loader_discarded_after_close():
    """loader 迟到完成但在中途 engine.close() 被调用 → 丢弃、不挂载、不覆盖 UNLOADED 状态"""
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

    def slow_loader():
        started_event.set()
        finish_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = slow_loader

    engine._trigger_load()
    assert started_event.wait(timeout=2.0), "Loader thread failed to start"
    assert engine.state == ModelState.LOADING
    epoch_before_close = engine._load_epoch

    # 调用 close()，递增 epoch 并置 UNLOADED
    engine.close()
    assert engine.state == ModelState.UNLOADED
    assert engine._load_epoch > epoch_before_close

    # 允许 loader 线程返回
    finish_event.set()

    # 等待后台加载线程退出
    for _ in range(50):
        if not engine._load_exec_lock.locked():
            break
        await asyncio.sleep(0.02)

    assert not engine._load_exec_lock.locked()
    assert engine.state == ModelState.UNLOADED
    assert engine.model is None
    assert engine.tokenizer is None


@pytest.mark.asyncio
async def test_late_loader_discarded_after_watchdog_timeout():
    """loader 迟到完成但在中途被 watchdog 超时推进了 epoch 并转为 ERROR → 迟到对象丢弃，不挂载"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=0.1,  # 0.1s 超时
        load_stall_exit_seconds=10.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    started_event = threading.Event()
    finish_event = threading.Event()

    def slow_loader():
        started_event.set()
        finish_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = slow_loader

    try:
        engine._trigger_load()
        assert started_event.wait(timeout=2.0), "Loader thread failed to start"
        assert engine.state == ModelState.LOADING
        epoch_before = engine._load_epoch

        # 等待 watchdog 触发超时判定转入 ERROR
        for _ in range(50):
            if engine.state == ModelState.ERROR:
                break
            await asyncio.sleep(0.02)

        assert engine.state == ModelState.ERROR
        assert engine._load_epoch > epoch_before
        assert engine.unhealthy is True

        # 此时允许迟到的 loader 线程返回
        finish_event.set()

        for _ in range(50):
            if not engine._load_exec_lock.locked():
                break
            await asyncio.sleep(0.02)

        assert not engine._load_exec_lock.locked()
        # 迟到对象必须被丢弃，不能挂载，状态依然是 ERROR
        assert engine.state == ModelState.ERROR
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        finish_event.set()
        engine.close()

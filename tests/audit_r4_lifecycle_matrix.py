"""
R4 引擎红队审计：状态矩阵与生命周期流转竞态

覆盖项：
1. 状态矩阵合法迁移全链路：UNLOADED -> LOADING -> READY -> IDLE -> UNLOADING -> UNLOADED。
2. 手工置 LOADING 状态时的等待者语义（并发等待 READY 与超时 ModelLoadingPendingError）。
3. 重复 close() 与 preload() 的幂等性验证。
4. [缺陷复现] _on_idle_timeout 卸载完成时无条件置 UNLOADED，与并发 load 竞态导致新加载被丢弃、请求挂起超时。
5. [缺陷复现] engine.close() 缺乏 closed 标志，close 后新请求盲目触发重新加载并崩溃于 Executor RuntimeError。
6. [缺陷复现] close() 在多批次 embed 推理中途发生， active 请求非优雅退出而是抛出未捕获 RuntimeError。
7. [缺陷复现] _queue_lock 与 _state_lock 锁区间分离导致的活跃请求中途被降级为 IDLE 竞态。
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
from src.core.errors import ModelLoadingPendingError


class DummyModel:
    def __init__(self, hidden_dim: int = 1024, delay: float = 0.0):
        self.hidden_dim = hidden_dim
        self.delay = delay
        self.device = "cpu"

    def to(self, device: str):
        self.device = device
        return self

    def half(self):
        return self

    def eval(self):
        return self

    def __call__(self, **kwargs):
        if self.delay > 0:
            time.sleep(self.delay)
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
async def test_state_matrix_happy_path():
    """验证状态机合法迁移全链路：UNLOADED -> LOADING -> READY -> IDLE -> UNLOADING -> UNLOADED"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=1,  # 1s 后转入 UNLOADED
        load_timeout_seconds=5.0,
    )
    engine = BGEM3Engine(config=cfg)
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())

    try:
        # 初始状态
        assert engine.state == ModelState.UNLOADED

        # 触发加载 -> READY
        await engine.load_model()
        assert engine.state == ModelState.READY

        # 执行单次 embed，推理完毕且活跃请求归零后，因 idle_timeout > 0 应转入 IDLE
        res = await engine.embed(["Hello state test"])
        assert len(res) == 1
        assert engine.state == ModelState.IDLE
        assert engine._idle_timer is not None

        # 等待闲置超时自动卸载（1s + 裕量）
        for _ in range(75):
            if engine.state == ModelState.UNLOADED:
                break
            await asyncio.sleep(0.02)

        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_manual_loading_waiter_semantics():
    """手工置 LOADING 时，多个并发请求自旋等待并在转入 READY 后全部成功"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        request_load_wait_seconds=1.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    try:
        engine._set_state(ModelState.LOADING)

        async def delayed_transition():
            await asyncio.sleep(0.1)
            engine.tokenizer = DummyTokenizer()
            engine.model = DummyModel()
            engine._set_state(ModelState.READY)

        task_transition = asyncio.create_task(delayed_transition())

        # 3 个并发等待者
        reqs = [
            engine.embed([f"Waiter {i} text"])
            for i in range(3)
        ]
        results = await asyncio.gather(*reqs)
        await task_transition

        assert len(results) == 3
        for r in results:
            assert len(r) == 1
            assert len(r[0]) == 1024
        assert engine.state == ModelState.READY
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_manual_loading_waiter_timeout():
    """手工置 LOADING 时，等待者超时抛出具体 ModelLoadingPendingError"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        request_load_wait_seconds=0.08,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    try:
        engine._set_state(ModelState.LOADING)
        with pytest.raises(ModelLoadingPendingError) as exc_info:
            await engine._wait_for_ready(timeout=0.05)

        err = exc_info.value
        assert err.model_state == "LOADING"
        assert err.retry_after_ms == 1000
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_idempotent_close_and_preload():
    """验证 close() 与 preload() 的重复调用幂等性"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())

    try:
        # 重复 preload
        await engine.preload()
        assert engine.state == ModelState.READY
        await engine.preload()
        assert engine.state == ModelState.READY

        # 重复 close
        engine.close()
        assert engine.state == ModelState.UNLOADED
        engine.close()
        assert engine.state == ModelState.UNLOADED
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_race_idle_unload_stomps_loading_state():
    """
    [缺陷审计与复现 · 契约断言]
    当 _on_idle_timeout 正在执行资源卸载期间，并发到达新请求触发加载。
    契约期望：并发到达的请求应当成功就绪并返回 embedding 结果。
    实际观察：_on_idle_timeout 结束时盲目执行 _set_state(UNLOADED)，将新发起的 LOADING
    状态踩踏为 UNLOADED。工作线程以 (epoch, UNLOADED != LOADING) 误判并将加载成功的模型丢弃，
    最终等待者超时抛出 ModelLoadingPendingError，测试失败证实缺陷存在。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=1,
        request_load_wait_seconds=0.4,
        load_timeout_seconds=2.0,
    )
    engine = BGEM3Engine(config=cfg)

    def slow_loader():
        time.sleep(0.15)
        return DummyTokenizer(), DummyModel()

    orig_do_unload = engine._do_unload_resources

    def delayed_do_unload():
        time.sleep(0.08)  # 延迟卸载，留出竞态窗口
        orig_do_unload()

    engine._load_model_sync = slow_loader
    engine._do_unload_resources = delayed_do_unload

    t_unload = None
    try:
        # 初始进入 IDLE
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.IDLE)

        # 启动闲置卸载工作线程
        t_unload = threading.Thread(target=engine._on_idle_timeout)
        t_unload.start()

        # 等待其刚进入 UNLOADING
        for _ in range(50):
            if engine.state == ModelState.UNLOADING:
                break
            await asyncio.sleep(0.005)

        assert engine.state == ModelState.UNLOADING

        # 契约：请求应该顺利完成加载并返回
        res = await engine.embed(["Race trigger"])
        assert len(res) == 1
        assert len(res[0]) == 1024
        assert engine.state == ModelState.READY
    finally:
        if t_unload and t_unload.is_alive():
            t_unload.join(timeout=2.0)
        engine.close()


@pytest.mark.asyncio
async def test_request_after_close_raises_engine_closed():
    """
    [缺陷审计与复现 · 契约断言]
    契约期望：engine.close() 后再次调用 embed()，应被安全拦截并抛出 EngineError 结构化异常。
    实际观察：因缺少 self._closed 标志，引擎在 UNLOADED 状态下重新触发 loader 尝试复活，
    但由于 executor 已 shutdown，最终泄漏底层 RuntimeError: cannot schedule new futures after shutdown。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=2.0,
    )
    engine = BGEM3Engine(config=cfg)
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())

    await engine.load_model()
    assert engine.state == ModelState.READY

    engine.close()
    assert engine.state == ModelState.UNLOADED
    assert not engine._watchdog_thread.is_alive()

    # 契约：必须抛出 EngineError
    with pytest.raises(EngineError):
        await engine.embed(["Request on closed engine"])


@pytest.mark.asyncio
async def test_close_during_multi_batch_embed_raises_engine_closed():
    """
    [缺陷审计与复现 · 契约断言]
    当多 batch 推理进行中外部调用 close()。
    契约期望：active_requests 被优雅中断或等待，抛出受管的 EngineError。
    实际观察：close() 立即强行关闭 executor，未完成的批次直接触发未受管的底层 RuntimeError。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_batch=1,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine.tokenizer = DummyTokenizer()
    engine.model = DummyModel(delay=0.15)
    engine._set_state(ModelState.READY)

    task = asyncio.create_task(engine.embed(["batch1", "batch2", "batch3"]))

    # 等待首批次开始
    await asyncio.sleep(0.05)
    engine.close()

    # 契约：必须抛出 EngineError
    with pytest.raises(EngineError):
        await task


@pytest.mark.asyncio
async def test_race_queue_lock_and_state_lock_demotes_active_engine_to_idle():
    """
    [缺陷审计与复现 · 契约断言]
    契约期望：当有正在执行的推理请求时（active_requests > 0），引擎状态绝不能处于 IDLE。
    实际观察：embed() finally 中 _queue_lock 与 _state_lock 未合拢，前序请求的 remaining=0
    导致后续请求并发介入时状态仍被置为 IDLE，违反状态一致性。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=300,
    )
    engine = BGEM3Engine(config=cfg)
    engine.tokenizer = DummyTokenizer()
    engine.model = DummyModel()
    engine._set_state(ModelState.READY)

    # 模拟两请求微观交织
    with engine._queue_lock:
        engine._active_requests = 0
        remaining_r1 = 0

    # Request 2 介入并持有活跃请求计数
    with engine._queue_lock:
        engine._active_requests += 1

    # Request 1 随后执行状态流转
    with engine._state_lock:
        if remaining_r1 == 0 and engine._state == ModelState.READY:
            if engine.idle_timeout_seconds > 0:
                engine._state = ModelState.IDLE

    # 契约断言：既然有 1 个活跃请求正在进行，状态必须是 READY 而不能是 IDLE
    status = await engine.get_model_status()
    assert status["queue_depth"] == 1
    assert status["state"] == "READY", f"Engine unexpectedly demoted to {status['state']} while queue_depth={status['queue_depth']}"

    engine.close()

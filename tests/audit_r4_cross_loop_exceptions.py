"""
R4 引擎红队审计：事件循环交叉、异常注入与部分构造红队测试

覆盖项：
1. 主 loop 与 Worker 风格 loop 交替独立调用；验证无 `Future attached to a different loop`。
2. 多线程独立并发事件循环交织调用 embed() 推理。
3. loader 同步抛错：状态转为 ERROR，抛出 EngineError，指标递增。
4. [缺陷复现] loader 部分构造（返回 (None, Model) 或 (Tokenizer, None)）被错误挂载为 READY，导致引擎死锁在故障就绪态。
5. unload_model 内部抛错时转入 ERROR，后续重试能够安全恢复。
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


def test_alternating_independent_event_loops():
    """主 loop 与 Worker 风格独立 loop 交替调用，验证不得出现 Future attached to a different loop"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())

    try:
        # 交替使用 5 组全新的独立 event loop
        for round_idx in range(5):
            # Loop 1: 执行 preload
            loop1 = asyncio.new_event_loop()
            try:
                loop1.run_until_complete(engine.preload())
                assert engine.state == ModelState.READY
            finally:
                loop1.close()

            # Loop 2: 执行 embed
            loop2 = asyncio.new_event_loop()
            try:
                res2 = loop2.run_until_complete(engine.embed([f"Round {round_idx} loop2 text"]))
                assert len(res2) == 1
                assert len(res2[0]) == 1024
            finally:
                loop2.close()

            # Loop 3: 执行 unload_model
            loop3 = asyncio.new_event_loop()
            try:
                loop3.run_until_complete(engine.unload_model())
                assert engine.state == ModelState.UNLOADED
            finally:
                loop3.close()
    finally:
        engine.close()


def test_concurrent_threads_independent_event_loops():
    """多线程并发拥有各自独立的事件循环调用 embed()，验证 cross-loop 并发安全性"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_queue_size=32,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())

    concurrency = 8
    barrier = threading.Barrier(concurrency)
    errors = []

    def thread_loop_worker(worker_id: int):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            barrier.wait(timeout=3.0)
            for i in range(5):
                res = loop.run_until_complete(
                    engine.embed([f"worker_{worker_id}_req_{i}"])
                )
                assert len(res) == 1
                assert len(res[0]) == 1024
        except Exception as e:
            errors.append((worker_id, e))
        finally:
            loop.close()

    threads = [
        threading.Thread(target=thread_loop_worker, args=(i,))
        for i in range(concurrency)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    try:
        assert len(errors) == 0, f"Cross-loop concurrency encountered errors: {errors}"
        assert engine.state == ModelState.READY
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_loader_sync_exception_transitions_to_error():
    """loader 同步抛错：状态转为 ERROR，对外抛出 EngineError 并递增指标"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    def failing_loader():
        raise RuntimeError("Simulated weight file corrupt")

    engine._load_model_sync = failing_loader

    try:
        with pytest.raises(EngineError) as exc_info:
            await engine.load_model()

        assert "Simulated weight file corrupt" in str(exc_info.value)
        assert engine.state == ModelState.ERROR
        assert engine.model is None
        assert engine.tokenizer is None

        metrics_snap = METRICS.snapshot()
        assert metrics_snap["counters"]["model_load_failures_total"] >= 1
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_partial_loader_construction_wedges_engine_in_ready():
    """
    [缺陷审计与复现 · 契约断言]
    当 loader 发生部分构造（例如 tokenizer 创建成功但 model 缺失，返回 (None, DummyModel)）。
    契约期望：_load_worker 必须检验 tokenizer 与 model 均有效，缺失任一组件应判定加载失败并置为 ERROR。
    实际观察：_load_worker 仅凭 success=True 无条件挂载 self.tokenizer=None 并将状态强推至 READY。
    导致引擎对外声称已就绪且健康，但所有后续 embed() 均因 Model or tokenizer is not initialized 崩溃；
    且因状态处于 READY，主动 load_model()/preload() 判定已就绪直接跳过，引擎永久死锁在破损状态。
    """
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    # 模拟缺陷 loader：tokenizer 为 None，model 存在
    engine._load_model_sync = lambda: (None, DummyModel())

    await engine.load_model()

    try:
        # 契约断言：部分构造绝不能使引擎进入 READY，必须处于 ERROR 状态
        assert engine.state == ModelState.ERROR, (
            f"Partial construction defect: engine entered {engine.state} despite tokenizer being None! "
            f"Engine is permanently wedged in broken READY state."
        )
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_unload_failure_transitions_to_error_and_recovers():
    """unload_model 内部执行抛错时转入 ERROR，后续能够再次加载并恢复 READY"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())

    await engine.load_model()
    assert engine.state == ModelState.READY

    # 注入卸载异常
    engine._do_unload_resources = MagicMock(side_effect=RuntimeError("Driver lock failure during unload"))

    with pytest.raises(RuntimeError) as exc_info:
        await engine.unload_model()

    assert "Driver lock failure during unload" in str(exc_info.value)
    assert engine.state == ModelState.ERROR

    # 恢复卸载函数并重新加载，验证可自愈至 READY
    engine._do_unload_resources = lambda: None
    await engine.load_model()
    assert engine.state == ModelState.READY

    res = await engine.embed(["Recovered text"])
    assert len(res) == 1
    engine.close()

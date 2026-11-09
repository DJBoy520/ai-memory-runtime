"""
生命周期与看门狗测试：
1. 模型加载失败转换至 ERROR 并更新 METRICS。
2. 卸载与加载竞态、close() 的幂等与安全性。
3. 手工置 LOADING 状态时的等待成功与超时抛错。
4. Watchdog 超时触发 ERROR 与 unhealthy 诊断状态。
5. Watchdog 卡死达到 stall 阈值触发受控退出 (exit_fn 注入，code==1，且仅 1 次)。
6. 启动时 crash 计数文件 10 分钟内 >=3 次告警 "repeated watchdog exits"。
7. preload 幂等性验证。
"""

import asyncio
import logging
from pathlib import Path
import threading
import time
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import BGEM3Engine, EngineError, ModelState
from src.core.errors import ModelLoadingPendingError
from src.service.metrics import METRICS


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


@pytest.mark.asyncio
async def test_load_failure_transitions_to_error():
    """模型加载内部抛错时，状态转换为 ERROR，抛出 EngineError 并记录指标"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    def failing_load():
        raise RuntimeError("Simulated weight file corrupt")

    engine._load_model_sync = failing_load

    try:
        with pytest.raises(EngineError) as exc_info:
            await engine.load_model()

        assert "Simulated weight file corrupt" in str(exc_info.value)
        assert engine.state == ModelState.ERROR
        metrics_snap = METRICS.snapshot()
        assert metrics_snap["counters"]["model_load_failures_total"] >= 1
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_unload_and_load_race():
    """加载期间调用 unload_model，不会出现异常且最终状态为 UNLOADED"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    stall_event = threading.Event()

    def slow_load():
        stall_event.wait(timeout=2.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = slow_load

    try:
        engine._trigger_load()
        await asyncio.sleep(0.05)
        assert engine.state == ModelState.LOADING

        # 竞态：在加载过程中直接卸载
        await engine.unload_model()
        assert engine.state == ModelState.UNLOADED

        stall_event.set()
        await asyncio.sleep(0.1)
        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
    finally:
        stall_event.set()
        engine.close()


@pytest.mark.asyncio
async def test_close_lifecycle_safety():
    """验证 close 方法多次调用安全幂等，watchdog 线程及时退出"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine.tokenizer = DummyTokenizer()
    engine.model = DummyModel()
    engine._set_state(ModelState.READY)

    assert engine._watchdog_thread.is_alive()

    # 首次关闭
    engine.close()
    assert engine.state == ModelState.UNLOADED
    assert engine.model is None
    assert engine._watchdog_stop_event.is_set()
    assert not engine._watchdog_thread.is_alive()

    # 幂等二次关闭
    engine.close()
    assert engine.state == ModelState.UNLOADED


@pytest.mark.asyncio
async def test_manual_loading_state_waiters():
    """手工置 LOADING 时，请求按等待者自旋排队；超时抛 ModelLoadingPendingError"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        request_load_wait_seconds=0.1,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    try:
        # 1. 成功等待场景
        engine._set_state(ModelState.LOADING)

        async def _set_ready_later():
            await asyncio.sleep(0.05)
            engine.tokenizer = DummyTokenizer()
            engine.model = DummyModel()
            engine._set_state(ModelState.READY)

        asyncio.create_task(_set_ready_later())
        res = await engine.embed(["Waiting test"])
        assert len(res) == 1
        assert len(res[0]) == 1024

        # 2. 超时场景
        engine._set_state(ModelState.LOADING)
        with pytest.raises(ModelLoadingPendingError):
            await engine._wait_for_ready(timeout=0.08)
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_watchdog_timeout_marks_error_and_unhealthy():
    """Watchdog 超时将状态标记为 ERROR 并设置 unhealthy=True，可经 get_model_status 观测"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=0.1,
        load_stall_exit_seconds=10.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    stall_event = threading.Event()

    def stalled_load():
        stall_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = stalled_load

    try:
        engine._trigger_load()
        assert engine.state == ModelState.LOADING

        # 轮询等待 watchdog 判定超时（0.1s + tick 延迟）
        for _ in range(50):
            if engine.state == ModelState.ERROR:
                break
            await asyncio.sleep(0.02)

        assert engine.state == ModelState.ERROR
        assert engine.unhealthy is True

        status = await engine.get_model_status()
        assert status["state"] == "ERROR"
        assert status["unhealthy"] is True
        assert "watchdog timeout" in status["last_error"].lower()

        metrics_snap = METRICS.snapshot()
        assert metrics_snap["gauges"]["model_unhealthy"] == 1.0
        assert metrics_snap["counters"]["model_load_failures_total"] >= 1
    finally:
        stall_event.set()
        engine.close()


@pytest.mark.asyncio
async def test_watchdog_exit_enabled_triggers_exit_fn(tmp_path: Path):
    """当 watchdog_exit_enabled 开启且卡死达到 load_stall_exit_seconds，受控调用 exit_fn(1) 仅 1 次"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=0.05,
        load_stall_exit_seconds=0.15,
        watchdog_exit_enabled=True,
        idle_timeout_seconds=0,
    )
    mock_exit = MagicMock()
    crash_file = tmp_path / "amr.crash.count"

    stall_event = threading.Event()

    def permanent_stall():
        stall_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine = BGEM3Engine(
        config=cfg, exit_fn=mock_exit, crash_count_path=crash_file
    )
    engine._load_model_sync = permanent_stall

    try:
        engine._trigger_load()

        # 等待 stall exit 触发（0.15s + 裕量）
        for _ in range(50):
            if mock_exit.called:
                break
            await asyncio.sleep(0.02)

        assert mock_exit.call_count == 1
        assert mock_exit.call_args[0][0] == 1

        # 再次等待，验证仅触发 1 次
        await asyncio.sleep(0.1)
        assert mock_exit.call_count == 1

        # 验证崩溃时间戳已写入 crash 文件
        assert crash_file.exists()
        content = crash_file.read_text(encoding="utf-8").strip()
        assert len(content.splitlines()) == 1
    finally:
        stall_event.set()
        engine.close()


def test_repeated_watchdog_exits_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    """启动时检测 10 分钟内 crash 记录 >= 3 条，产生 CRITICAL 告警"""
    crash_file = tmp_path / "amr.crash.count"
    now = time.time()
    # 写入 3 条 10 分钟内的崩溃时间戳
    crash_file.write_text(f"{now - 120:.3f}\n{now - 60:.3f}\n{now - 10:.3f}\n")

    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
    )

    with caplog.at_level(logging.CRITICAL):
        engine = BGEM3Engine(config=cfg, crash_count_path=crash_file)
        try:
            assert "repeated watchdog exits" in caplog.text
        finally:
            engine.close()


@pytest.mark.asyncio
async def test_preload_idempotent():
    """验证 preload() 的幂等性：从 UNLOADED 推进至 READY，多次调用直接返回"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=5.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    engine._load_model_sync = lambda: (DummyTokenizer(), DummyModel())

    try:
        assert engine.state == ModelState.UNLOADED
        await engine.preload()
        assert engine.state == ModelState.READY
        assert engine.model is not None

        # 再次 preload 直接返回，状态保持 READY
        await engine.preload()
        assert engine.state == ModelState.READY
    finally:
        engine.close()

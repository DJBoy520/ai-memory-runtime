"""
R4 引擎红队审计：Watchdog 守护、崩溃计数与闲置超时红队测试

覆盖项：
1. 卡死 loader（同步 sleep 超过 load_timeout）→ 判定 ERROR + unhealthy=True + 指标递增；迟到成功被安全丢弃。
2. watchdog_exit_enabled + 注入 exit_fn → 恰调用一次、code=1；测试进程不退出。
3. crash_count 文件：10 分钟外清理（pruning），窗口内保留；全部过期时写空。
4. crash_count 文件：10 分钟内 >= 3 条启动触发 CRITICAL 告警 "repeated watchdog exits"。
5. [缺陷复现] crash_count 并发写丢失更新（缺乏 fcntl.flock 导致多进程崩溃覆盖丢失）。
6. idle_timeout <= 0（0 或负数）彻底禁用自动卸载，embed finally 不把 READY 降为 IDLE。
7. idle_timeout > 0 时调度 Timer 并自动卸载转入 UNLOADED。
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor
import logging
from pathlib import Path
import threading
import time
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import BGEM3Engine, ModelState
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
async def test_loader_stall_watchdog_timeout_marks_error_and_unhealthy():
    """卡死 loader 超过 load_timeout_seconds → 判定 ERROR，置 unhealthy=True，指标增加，迟到完成被丢弃"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        load_timeout_seconds=0.1,  # 0.1s 超时
        load_stall_exit_seconds=10.0,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)

    stall_event = threading.Event()

    def stalling_loader():
        stall_event.wait(timeout=3.0)
        return DummyTokenizer(), DummyModel()

    engine._load_model_sync = stalling_loader

    try:
        engine._trigger_load()
        assert engine.state == ModelState.LOADING

        # 轮询等待 watchdog 触发超时判定
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

        # 释放 loader，迟到的模型应被安全丢弃，不改变 ERROR 状态
        stall_event.set()
        for _ in range(50):
            if not engine._load_exec_lock.locked():
                break
            await asyncio.sleep(0.02)

        assert engine.state == ModelState.ERROR
        assert engine.model is None
    finally:
        stall_event.set()
        engine.close()


@pytest.mark.asyncio
async def test_watchdog_exit_fn_called_exactly_once(tmp_path: Path):
    """当 watchdog_exit_enabled 开启且卡死达到 load_stall_exit_seconds，受控调用 exit_fn(1) 恰 1 次且进程不退出"""
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

        # 持续等待进一步验证不会重复触发 exit_fn
        await asyncio.sleep(0.1)
        assert mock_exit.call_count == 1

        # 验证已写入崩溃文件
        assert crash_file.exists()
        lines = crash_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
    finally:
        stall_event.set()
        engine.close()


def test_crash_count_window_and_pruning(tmp_path: Path):
    """验证 crash_count 文件在启动时仅保留 10 分钟 (600s) 之内的有效时间戳，超出部分清理"""
    crash_file = tmp_path / "amr.crash.count"
    now = time.time()

    # 构造：2 条过期时间戳（700s 前、1200s 前），2 条有效时间戳（100s 前、30s 前），1 条畸形文本
    lines = [
        f"{now - 1200.0:.3f}",
        "INVALID_TIMESTAMP_LINE",
        f"{now - 700.0:.3f}",
        f"{now - 100.0:.3f}",
        f"{now - 30.0:.3f}",
    ]
    crash_file.write_text("\n".join(lines) + "\n")

    cfg = ModelConfig(name_or_path="mock/bge-m3", device="cpu", fp16=False)
    engine = BGEM3Engine(config=cfg, crash_count_path=crash_file)
    try:
        content = crash_file.read_text().strip().splitlines()
        assert len(content) == 2, f"Expected 2 pruned valid timestamps, got {len(content)}"
        ts1 = float(content[0])
        ts2 = float(content[1])
        assert abs(ts1 - (now - 100.0)) < 2.0
        assert abs(ts2 - (now - 30.0)) < 2.0
    finally:
        engine.close()


def test_crash_count_all_expired_becomes_empty(tmp_path: Path):
    """全部时间戳过期时，文件清理为空文件"""
    crash_file = tmp_path / "amr.crash.count"
    now = time.time()
    crash_file.write_text(f"{now - 1000.0:.3f}\n{now - 800.0:.3f}\n")

    cfg = ModelConfig(name_or_path="mock/bge-m3", device="cpu", fp16=False)
    engine = BGEM3Engine(config=cfg, crash_count_path=crash_file)
    try:
        content = crash_file.read_text().strip()
        assert content == "", f"Expected empty file, got {content}"
    finally:
        engine.close()


def test_crash_count_repeated_exits_critical_alert(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    """10 分钟内 crash 记录 >= 3 条时，启动检测产生 CRITICAL 告警"""
    crash_file = tmp_path / "amr.crash.count"
    now = time.time()
    crash_file.write_text(f"{now - 300.0:.3f}\n{now - 120.0:.3f}\n{now - 10.0:.3f}\n")

    cfg = ModelConfig(name_or_path="mock/bge-m3", device="cpu", fp16=False)

    with caplog.at_level(logging.CRITICAL):
        engine = BGEM3Engine(config=cfg, crash_count_path=crash_file)
        try:
            assert "repeated watchdog exits" in caplog.text
        finally:
            engine.close()


def test_crash_count_concurrent_writes_lost_updates(tmp_path: Path):
    """
    [缺陷审计与复现 · 契约断言]
    当多个并发引擎实例因 watchdog stall 同时调用 _record_crash_and_exit()。
    契约期望：并发写入时不应丢失更新，所有崩溃时间戳（5 次）均应安全保留在 crash_count_path 中。
    实际观察：_record_crash_and_exit() 未使用文件锁 (fcntl.flock)，
    各自读-改-写发生严重覆盖，最终仅保留 1 条记录，导致启动告警阈值 (>=3) 无法被触发。
    同时验证文件本身未出现半截残损写入（replace 原子性成立）。
    """
    crash_file = tmp_path / "amr_concurrent.crash.count"
    num_threads = 5
    barrier = threading.Barrier(num_threads)

    def thread_worker(idx: int):
        cfg = ModelConfig(name_or_path="mock/bge-m3", device="cpu", fp16=False)
        engine = BGEM3Engine(
            config=cfg,
            exit_fn=lambda code: None,
            crash_count_path=crash_file,
        )
        barrier.wait(timeout=3.0)
        engine._record_crash_and_exit()
        engine.close()

    threads = [
        threading.Thread(target=thread_worker, args=(i,))
        for i in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    assert crash_file.exists()
    lines = crash_file.read_text().strip().splitlines()
    # 验证未发生格式损坏：每行都必须可解析为合法 float
    for line in lines:
        float(line)

    # 契约断言：必须完整保留 5 次崩溃时间戳
    assert len(lines) == num_threads, (
        f"Concurrency defect: Expected {num_threads} timestamps, but only {len(lines)} recorded. "
        f"Remaining were lost due to uncoordinated file overwrite without flock."
    )


@pytest.mark.asyncio
async def test_idle_timeout_zero_disabled():
    """idle_timeout_seconds=0 时彻底禁用闲置卸载，embed 推理后保持 READY 且无 Timer"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=0,
    )
    engine = BGEM3Engine(config=cfg)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)

        res = await engine.embed(["Stay warm"])
        assert len(res) == 1
        assert engine.state == ModelState.READY
        assert engine._idle_timer is None

        # 经过等待后依然为 READY
        await asyncio.sleep(0.1)
        assert engine.state == ModelState.READY
        assert engine.model is not None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_idle_timeout_negative_disabled():
    """idle_timeout_seconds < 0（如 -1）时同样视为禁用自动卸载"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=-1,
    )
    engine = BGEM3Engine(config=cfg)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)

        await engine.embed(["Negative timeout stay warm"])
        assert engine.state == ModelState.READY
        assert engine._idle_timer is None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_idle_timeout_positive_auto_unloads():
    """idle_timeout_seconds > 0 时正常调度 Timer 并自动卸载转入 UNLOADED"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=1,  # 1s 后自动卸载
    )
    engine = BGEM3Engine(config=cfg)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)

        await engine.embed(["Positive timeout auto unload"])
        assert engine.state == ModelState.IDLE
        assert engine._idle_timer is not None

        # 等待超时卸载
        for _ in range(75):
            if engine.state == ModelState.UNLOADED:
                break
            await asyncio.sleep(0.02)

        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        engine.close()

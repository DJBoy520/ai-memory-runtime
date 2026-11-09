"""
闲置超时卸载测试：
1. idle_timeout_seconds <= 0（0 或负数）时彻底禁用自动卸载，embed 推理后状态保持 READY，无 Timer。
2. idle_timeout_seconds > 0 时正常调度 Timer，超时后卸载模型并转换为 UNLOADED。
3. _on_idle_timeout 手动触发可正确卸载模型资源。
"""

import asyncio
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import BGEM3Engine, ModelState


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
async def test_idle_timeout_zero_disabled():
    """idle_timeout_seconds=0 时禁用自动卸载，推理后保持 READY 且无 Timer 创建"""
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

        res = await engine.embed(["Keep warm forever"])
        assert len(res) == 1
        assert len(res[0]) == 1024

        # 状态依然保持 READY，而不是降级为 IDLE
        assert engine.state == ModelState.READY
        assert engine._idle_timer is None

        # 即使等待，也不会触发任何卸载
        await asyncio.sleep(0.2)
        assert engine.state == ModelState.READY
        assert engine.model is not None
        assert engine.tokenizer is not None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_idle_timeout_negative_disabled():
    """idle_timeout_seconds < 0 时同样视为禁用自动卸载"""
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

        await engine.embed(["Negative timeout test"])
        assert engine.state == ModelState.READY
        assert engine._idle_timer is None

        await asyncio.sleep(0.2)
        assert engine.state == ModelState.READY
        assert engine.model is not None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_idle_timeout_positive_enabled():
    """idle_timeout_seconds > 0 时推理后置为 IDLE 并启动 Timer，超时自动卸载为 UNLOADED"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=60,
    )
    # 注入短闲置超时 0.15s
    engine = BGEM3Engine(config=cfg, idle_timeout_seconds=0.15)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)

        await engine.embed(["Unload after idle"])

        # 推理结束后应降级为 IDLE 且启动定时器
        assert engine.state == ModelState.IDLE
        assert engine._idle_timer is not None

        # 等待定时器触发
        await asyncio.sleep(0.3)
        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        engine.close()


def test_direct_on_idle_timeout_invocation():
    """验证直接调用 _on_idle_timeout 时的卸载行为"""
    cfg = ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        idle_timeout_seconds=60,
    )
    engine = BGEM3Engine(config=cfg)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.IDLE)

        engine._on_idle_timeout()

        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        engine.close()

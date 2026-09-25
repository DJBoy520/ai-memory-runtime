"""
单元测试：STEP 3 - BGE-M3 向量引擎与 6 态状态机
覆盖：
1. 6 态初始状态及状态转换（UNLOADED -> LOADING -> READY -> IDLE -> UNLOADING -> ERROR）
2. embed 推理向量维度（1024 维）、数值格式与归一化
3. 物理批处理批次拆分（如 25 条拆分成 16 + 9 批次并正确拼接）
4. LOADING 状态下并发请求自旋排队不丢失
5. Idle 自动卸载机制与显存/内存释放回调
6. 并发安全性与队列背压 (QueueFullError)
7. get_model_status 状态与显存指标输出
"""

import asyncio
import time
from typing import List
from unittest.mock import MagicMock

import pytest
import torch

from config.settings import ModelConfig
from src.core.engine import (
    BGEM3Engine,
    EngineError,
    ModelLoadingTimeoutError,
    ModelState,
    QueueFullError,
)


@pytest.fixture
def mock_model_config():
    return ModelConfig(
        name_or_path="mock/bge-m3",
        device="cpu",
        fp16=False,
        max_batch=16,
        max_queue_size=64,
        idle_timeout_seconds=1,
        max_token_length=512,
    )


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
async def test_initial_state_and_status(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        assert engine.state == ModelState.UNLOADED
        status = await engine.get_model_status()
        assert status["state"] == "UNLOADED"
        assert status["queue_depth"] == 0
        assert status["max_queue_size"] == 64
        assert "allocated_mb" in status
        assert "driver_used_mb" in status
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_load_and_unload_lifecycle(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)
        assert engine.state == ModelState.READY

        status = await engine.get_model_status()
        assert status["state"] == "READY"

        await engine.unload_model()
        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
        assert engine.tokenizer is None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_embed_dimension_and_format(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)

        texts = ["Test query 1", "Test query 2"]
        embeddings = await engine.embed(texts)

        assert len(embeddings) == 2
        assert len(embeddings[0]) == 1024
        assert len(embeddings[1]) == 1024
        assert isinstance(embeddings[0][0], float)

        norm_sq = sum(x * x for x in embeddings[0])
        assert pytest.approx(norm_sq, rel=1e-3) == 1.0
        assert engine.state == ModelState.IDLE
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_batch_splitting(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)

        texts = [f"Item {i}" for i in range(25)]
        embeddings = await engine.embed(texts)
        assert len(embeddings) == 25
        assert len(embeddings[0]) == 1024
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_loading_spin_queue_success(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        engine._set_state(ModelState.LOADING)

        async def _delayed_ready():
            await asyncio.sleep(0.1)
            engine.tokenizer = DummyTokenizer()
            engine.model = DummyModel()
            engine._set_state(ModelState.READY)

        asyncio.create_task(_delayed_ready())
        embeddings = await engine.embed(["Waiting item"])
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 1024
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_loading_timeout(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        engine._set_state(ModelState.LOADING)
        with pytest.raises(ModelLoadingTimeoutError):
            await engine._wait_for_ready(timeout=0.2)
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_queue_full_backpressure(mock_model_config):
    mock_model_config.max_queue_size = 2
    engine = BGEM3Engine(config=mock_model_config)
    try:
        with engine._queue_lock:
            engine._active_requests = 2

        with pytest.raises(QueueFullError):
            await engine.embed(["Blocked item"])
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_idle_auto_unload(mock_model_config):
    mock_model_config.idle_timeout_seconds = 0.2
    engine = BGEM3Engine(config=mock_model_config)
    try:
        engine.tokenizer = DummyTokenizer()
        engine.model = DummyModel()
        engine._set_state(ModelState.READY)

        await engine.embed(["Hello"])
        assert engine.state == ModelState.IDLE

        await asyncio.sleep(0.4)
        assert engine.state == ModelState.UNLOADED
        assert engine.model is None
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_error_state_handling(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        engine._set_state(ModelState.ERROR, error_msg="Mock CUDA OOM")
        with pytest.raises(EngineError) as exc_info:
            await engine._wait_for_ready(timeout=1.0)
        assert "Mock CUDA OOM" in str(exc_info.value)
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_empty_embed_input(mock_model_config):
    engine = BGEM3Engine(config=mock_model_config)
    try:
        res = await engine.embed([])
        assert res == []
    finally:
        engine.close()

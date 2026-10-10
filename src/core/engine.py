"""
BGE-M3 向量嵌入引擎与 6 态状态机实现
状态流转：UNLOADED -> LOADING -> READY -> IDLE -> UNLOADING -> ERROR
严格遵循 Tesla P4 显存防爆、并发线程隔离、批处理切分、排队自旋与 Idle 自动卸载机制
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import gc
import logging
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional

import torch
from transformers import AutoModel, AutoTokenizer

from config.settings import ModelConfig, settings

logger = logging.getLogger(__name__)


class ModelState(str, Enum):
    UNLOADED = "UNLOADED"
    LOADING = "LOADING"
    READY = "READY"
    IDLE = "IDLE"
    UNLOADING = "UNLOADING"
    ERROR = "ERROR"


class EngineError(Exception):
    """引擎通用异常基类"""
    pass


class QueueFullError(EngineError):
    """请求队列超限背压异常 (503)"""
    pass


class ModelLoadingTimeoutError(EngineError):
    """LOADING 排队自旋超时异常"""
    pass


class BGEM3Engine:
    """
    BGE-M3 向量推理引擎
    提供 6 态生命周期管理、FP16 推理、分批截断与显存回收
    """

    def __init__(
        self,
        config: Optional[ModelConfig] = None,
        model_path: Optional[str] = None,
        idle_timeout_seconds: Optional[int] = None,
    ):
        self.config = config or settings.model
        self.model_path = model_path or self._resolve_model_path(self.config.name_or_path)
        self.max_batch = self.config.max_batch
        self.max_queue_size = self.config.max_queue_size
        self.idle_timeout_seconds = (
            idle_timeout_seconds
            if idle_timeout_seconds is not None
            else self.config.idle_timeout_seconds
        )
        self.max_token_length = getattr(self.config, "max_token_length", 8192)

        # 状态机与并发锁
        self._state: ModelState = ModelState.UNLOADED
        self._state_lock = threading.RLock()

        # 推理线程池
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="bge_worker"
        )
        self._active_requests = 0
        self._queue_lock = threading.RLock()

        # 模型资源句柄
        self.tokenizer: Optional[Any] = None
        self.model: Optional[Any] = None

        # 硬件与精度决策
        self.device = self._resolve_device(self.config.device)
        self.use_fp16 = self.config.fp16 and (self.device == "cuda")

        # 卸载定时器
        self._idle_timer: Optional[threading.Timer] = None
        self._total_inferences = 0
        self._last_error: Optional[str] = None

    def _resolve_device(self, requested_device: str) -> str:
        """设备判定：优先 cuda，不可用时降级 cpu"""
        if requested_device.startswith("cuda"):
            if torch.cuda.is_available():
                return requested_device
            logger.warning(
                "CUDA requested but torch.cuda.is_available() is False. Falling back to cpu."
            )
            return "cpu"
        return requested_device

    def _resolve_model_path(self, path_str: str) -> str:
        """解析本地或默认模型路径"""
        root_dir = Path(__file__).resolve().parent.parent.parent
        local_candidate = root_dir / "models" / "bge-m3"
        if local_candidate.exists() and (local_candidate / "config.json").exists():
            return str(local_candidate)

        configured_path = Path(path_str)
        if configured_path.is_absolute() and configured_path.exists():
            return str(configured_path)
        if (root_dir / path_str).exists():
            return str(root_dir / path_str)

        return path_str

    @property
    def state(self) -> ModelState:
        with self._state_lock:
            return self._state

    def _set_state(self, new_state: ModelState, error_msg: Optional[str] = None):
        """线程安全的状态转移"""
        with self._state_lock:
            old_state = self._state
            self._state = new_state
            if error_msg:
                self._last_error = error_msg
            logger.info(f"Model state transition: {old_state.value} -> {new_state.value}")

    def _cancel_idle_timer(self):
        """取消闲置定时器"""
        with self._state_lock:
            if self._idle_timer is not None:
                self._idle_timer.cancel()
                self._idle_timer = None

    def _schedule_idle_unload(self):
        """重置并启动闲置卸载定时器"""
        with self._state_lock:
            if self._idle_timer is not None:
                self._idle_timer.cancel()
                self._idle_timer = None

            if self._active_requests == 0 and self._state == ModelState.IDLE:
                timer = threading.Timer(
                    self.idle_timeout_seconds, self._on_idle_timeout
                )
                timer.daemon = True
                self._idle_timer = timer
                timer.start()

    def _on_idle_timeout(self):
        """定时器触发卸载逻辑"""
        with self._state_lock:
            if self._state != ModelState.IDLE or self._active_requests > 0:
                return
            self._state = ModelState.UNLOADING

        try:
            self._do_unload_resources()
            self._set_state(ModelState.UNLOADED)
        except Exception as e:
            logger.error(f"Error during auto-unloading: {e}", exc_info=True)
            self._set_state(ModelState.ERROR, error_msg=str(e))

    def _do_unload_resources(self):
        """释放模型与深度显存回收逻辑"""
        self._cancel_idle_timer()

        if hasattr(self, "model") and self.model is not None:
            del self.model
            self.model = None

        if hasattr(self, "tokenizer") and self.tokenizer is not None:
            del self.tokenizer
            self.tokenizer = None

        gc.collect()

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            if hasattr(torch.cuda, "ipc_collect"):
                torch.cuda.ipc_collect()

    def _load_model_sync(self):
        """同步执行模型载入逻辑（由 ThreadPoolExecutor 运行）"""
        tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        model = AutoModel.from_pretrained(self.model_path)

        if self.device == "cuda":
            if self.use_fp16:
                model = model.half()
            model = model.to("cuda")
        else:
            model = model.to("cpu")

        model.eval()
        self.tokenizer = tokenizer
        self.model = model

    async def load_model(self) -> None:
        """公有异步方法：主动加载模型或推进至 READY 状态"""
        with self._state_lock:
            if self._state == ModelState.READY:
                return
            if self._state == ModelState.IDLE:
                self._cancel_idle_timer()
                self._state = ModelState.READY
                return
            if self._state in (
                ModelState.UNLOADED,
                ModelState.ERROR,
                ModelState.UNLOADING,
            ):
                self._cancel_idle_timer()
                self._state = ModelState.LOADING

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(self._executor, self._load_model_sync)
            self._set_state(ModelState.READY)
        except Exception as e:
            self._set_state(ModelState.ERROR, error_msg=str(e))
            raise EngineError(f"Failed to load model: {e}")

    async def unload_model(self) -> None:
        """公有异步方法：主动卸载模型释放显存"""
        self._cancel_idle_timer()
        with self._state_lock:
            if self._state == ModelState.UNLOADED:
                return
            self._state = ModelState.UNLOADING

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(self._executor, self._do_unload_resources)
            self._set_state(ModelState.UNLOADED)
        except Exception as e:
            self._set_state(ModelState.ERROR, error_msg=str(e))
            raise

    async def _wait_for_ready(self, timeout: float = 25.0) -> None:
        """LOADING 状态排队自旋等待机制"""
        start_time = time.monotonic()
        while True:
            current_state = self.state
            if current_state == ModelState.READY:
                return
            if current_state == ModelState.IDLE:
                with self._state_lock:
                    if self._state == ModelState.IDLE:
                        self._cancel_idle_timer()
                        self._state = ModelState.READY
                        return
            if current_state == ModelState.ERROR:
                raise EngineError(f"Model failed to load: {self._last_error}")

            if current_state not in (ModelState.LOADING, ModelState.UNLOADED, ModelState.UNLOADING):
                raise EngineError(f"Unexpected state: {current_state}")

            elapsed = time.monotonic() - start_time
            if elapsed >= timeout:
                raise ModelLoadingTimeoutError(f"Timed out waiting for READY ({timeout}s)")

            await asyncio.sleep(0.02)

    def _infer_batch_sync(self, batch_texts: List[str]) -> List[List[float]]:
        """在工作线程内执行单批次推理"""
        if not self.model or not self.tokenizer:
            raise EngineError("Model or tokenizer is not initialized")

        encoded = self.tokenizer(
            batch_texts,
            padding=True,
            truncation=True,
            max_length=self.max_token_length,
            return_tensors="pt",
        )

        device = self.device
        if device == "cuda":
            encoded = {k: v.to("cuda") for k, v in encoded.items()}

        with torch.no_grad():
            if device == "cuda" and self.use_fp16:
                with torch.amp.autocast("cuda"):
                    outputs = self.model(**encoded)
            else:
                outputs = self.model(**encoded)

            cls_token = outputs.last_hidden_state[:, 0]
            normalized = torch.nn.functional.normalize(cls_token, p=2, dim=1)
            embeddings = normalized.cpu().to(torch.float32).tolist()

        return embeddings

    async def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []

        with self._queue_lock:
            if self._active_requests >= self.max_queue_size:
                raise QueueFullError(
                    f"Inference queue full ({self._active_requests}/{self.max_queue_size})"
                )
            self._active_requests += 1

        try:
            current_state = self.state
            if current_state in (
                ModelState.UNLOADED,
                ModelState.IDLE,
                ModelState.ERROR,
            ):
                await self.load_model()
            elif current_state == ModelState.LOADING:
                await self._wait_for_ready(timeout=25.0)

            if self.state != ModelState.READY:
                raise EngineError(f"Engine is not in READY state (current: {self.state})")

            all_embeddings: List[List[float]] = []
            batches = [
                texts[i : i + self.max_batch]
                for i in range(0, len(texts), self.max_batch)
            ]

            loop = asyncio.get_running_loop()
            for batch in batches:
                batch_res = await loop.run_in_executor(
                    self._executor, self._infer_batch_sync, batch
                )
                all_embeddings.extend(batch_res)

            self._total_inferences += len(texts)
            return all_embeddings

        except Exception as e:
            logger.error(f"Inference error in embed: {e}", exc_info=True)
            if "CUDA" in str(e) or "cuda" in str(e).lower():
                self._set_state(ModelState.ERROR, error_msg=str(e))
            raise

        finally:
            with self._queue_lock:
                self._active_requests -= 1
                remaining = self._active_requests

            with self._state_lock:
                if remaining == 0 and self._state == ModelState.READY:
                    self._state = ModelState.IDLE
                    self._schedule_idle_unload()

    def _get_driver_used_mb(self) -> float:
        if not torch.cuda.is_available():
            return 0.0
        try:
            output = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
                timeout=1.0,
                text=True,
            )
            return float(output.strip().split("\n")[0])
        except Exception:
            return 0.0

    async def get_model_status(self) -> Dict[str, Any]:
        allocated_mb = 0.0
        reserved_mb = 0.0
        if torch.cuda.is_available() and self.device == "cuda":
            allocated_mb = round(torch.cuda.memory_allocated() / (1024 * 1024), 2)
            reserved_mb = round(torch.cuda.memory_reserved() / (1024 * 1024), 2)

        driver_used_mb = self._get_driver_used_mb()

        with self._queue_lock:
            queue_depth = self._active_requests

        return {
            "state": self.state.value,
            "allocated_mb": allocated_mb,
            "reserved_mb": reserved_mb,
            "driver_used_mb": driver_used_mb,
            "queue_depth": queue_depth,
            "max_queue_size": self.max_queue_size,
            "total_inferences": self._total_inferences,
            "device": self.device,
            "idle_timeout_seconds": self.idle_timeout_seconds,
            "last_error": self._last_error,
        }

    def close(self):
        self._cancel_idle_timer()
        self._executor.shutdown(wait=False)
        self._do_unload_resources()

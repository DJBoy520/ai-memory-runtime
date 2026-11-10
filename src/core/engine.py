"""
BGE-M3 向量嵌入引擎与 6 态状态机实现
状态流转：UNLOADED -> LOADING -> READY -> IDLE -> UNLOADING -> ERROR
严格遵循 Tesla P4 显存防爆、并发线程隔离、批处理切分、排队自旋与 Idle 自动卸载机制
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import fcntl
import gc
import logging
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Union
import weakref

import torch
from transformers import AutoModel, AutoTokenizer

from config.settings import ModelConfig, settings
from src.core.errors import ModelLoadingPendingError
from src.service.metrics import METRICS

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
    """LOADING 排队自旋超时异常（保留兼容）"""
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
        exit_fn: Optional[Callable[[int], None]] = None,
        crash_count_path: Optional[Union[str, Path]] = None,
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
        self._closed: bool = False

        # Single-flight 加载原语
        self._load_exec_lock = threading.Lock()
        self._current_loader_thread: Optional[threading.Thread] = None
        self._load_epoch: int = 0
        self._load_started_at: Optional[float] = None
        self._stall_reported_timeout: bool = False
        self._stall_reported_exit: bool = False
        self._unhealthy: bool = False

        # 退出函数与崩溃计数文件
        self.exit_fn: Callable[[int], None] = exit_fn or os._exit
        self._exit_called: bool = False
        if crash_count_path is not None:
            self.crash_count_path = Path(crash_count_path)
        else:
            self.crash_count_path = self._default_crash_count_path()

        # 检查并清理启动时崩溃计数
        self._check_and_prune_crash_counts()

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

        # Watchdog 守护线程（使用 weakref 避免线程强引用导致实例与线程泄漏）
        self._watchdog_stop_event = threading.Event()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_entry,
            args=(weakref.ref(self), self._watchdog_stop_event),
            daemon=True,
            name="bge-watchdog",
        )
        self._watchdog_thread.start()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _default_crash_count_path(self) -> Path:
        xdg_runtime = os.environ.get("XDG_RUNTIME_DIR")
        if xdg_runtime and Path(xdg_runtime).is_dir():
            return Path(xdg_runtime) / "amr.crash.count"
        return Path("/tmp/amr.crash.count")

    @property
    def unhealthy(self) -> bool:
        with self._state_lock:
            return self._unhealthy

    def _set_unhealthy(self, value: bool) -> None:
        with self._state_lock:
            self._unhealthy = value
            METRICS.set_gauge("model_unhealthy", 1.0 if value else 0.0)

    def _lock_file_path(self) -> Path:
        return Path(str(self.crash_count_path) + ".lock")

    def _with_crash_lock(self, func: Callable[[], Any]) -> Any:
        lock_path = self._lock_file_path()
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "a") as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            try:
                return func()
            finally:
                try:
                    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass

    def _read_and_prune_crash_counts(
        self, additional_ts: Optional[float] = None
    ) -> List[float]:
        now = time.time()
        valid_ts: List[float] = []
        if self.crash_count_path.exists():
            try:
                content = self.crash_count_path.read_text(encoding="utf-8")
                for line in content.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ts = float(line)
                        if (now - ts) <= 600.0:
                            valid_ts.append(ts)
                    except ValueError:
                        continue
            except Exception as e:
                logger.warning(
                    f"Failed to read/prune crash count file {self.crash_count_path}: {e}"
                )
        if additional_ts is not None:
            valid_ts.append(additional_ts)
        self._write_crash_counts(valid_ts)
        return valid_ts

    def _check_and_prune_crash_counts(self) -> int:
        if not self.crash_count_path.exists():
            return 0
        try:
            valid_ts = self._with_crash_lock(self._read_and_prune_crash_counts)
        except Exception as e:
            logger.warning(
                f"Failed to read/prune crash count file {self.crash_count_path}: {e}"
            )
            return 0

        if len(valid_ts) >= 3:
            logger.critical("repeated watchdog exits")
        return len(valid_ts)

    def _write_crash_counts(self, timestamps: List[float]) -> None:
        parent = self.crash_count_path.parent
        parent.mkdir(parents=True, exist_ok=True)
        temp_file = (
            parent
            / f"{self.crash_count_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
        )
        try:
            content = "\n".join(f"{ts:.6f}" for ts in timestamps)
            if content:
                content += "\n"
            temp_file.write_text(content, encoding="utf-8")
            temp_file.replace(self.crash_count_path)
        except Exception:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass
            raise

    def _record_crash_and_exit(self) -> None:
        with self._state_lock:
            if self._exit_called:
                return
            self._exit_called = True

        now = time.time()
        try:
            self._with_crash_lock(
                lambda: self._read_and_prune_crash_counts(additional_ts=now)
            )
        except Exception as e:
            logger.error(f"Failed to record crash timestamp: {e}")

        logger.critical(
            "Watchdog load stall exceeded load_stall_exit_seconds; invoking exit_fn(1)"
        )
        self.exit_fn(1)

    @staticmethod
    def _watchdog_entry(
        engine_ref: "weakref.ReferenceType[BGEM3Engine]",
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(timeout=0.05):
            engine = engine_ref()
            if engine is None:
                break
            try:
                engine._watchdog_tick()
            except Exception as e:
                logger.error(f"Watchdog tick error: {e}", exc_info=True)
            finally:
                del engine

    def _watchdog_loop(self) -> None:
        self._watchdog_entry(weakref.ref(self), self._watchdog_stop_event)

    def _watchdog_tick(self) -> None:
        now = time.monotonic()
        with self._state_lock:
            started_at = self._load_started_at
            current_state = self._state
            load_timeout = getattr(self.config, "load_timeout_seconds", 120.0)
            stall_timeout = getattr(self.config, "load_stall_exit_seconds", 180.0)
            exit_enabled = getattr(self.config, "watchdog_exit_enabled", False)

        if started_at is None:
            return

        elapsed = now - started_at

        # 1. 状态 LOADING 且 elapsed > load_timeout_seconds：判定超时
        if current_state == ModelState.LOADING and elapsed > load_timeout:
            with self._state_lock:
                if (
                    self._state == ModelState.LOADING
                    and self._load_started_at == started_at
                    and not self._stall_reported_timeout
                ):
                    self._stall_reported_timeout = True
                    self._load_epoch += 1
                    self._set_unhealthy(True)
                    err_msg = (
                        f"Model load watchdog timeout: loading exceeded {load_timeout}s "
                        f"(elapsed: {elapsed:.2f}s)"
                    )
                    self._set_state(ModelState.ERROR, error_msg=err_msg)
                    logger.critical(
                        f"{err_msg}; marked ERROR/unhealthy (epoch incremented to {self._load_epoch})"
                    )
                    METRICS.incr("model_load_failures_total")

        # 2. 若 watchdog_exit_enabled 且同一 stall attempt 的 elapsed > load_stall_exit_seconds
        if exit_enabled and elapsed > stall_timeout:
            trigger_exit = False
            with self._state_lock:
                if (
                    self._load_started_at == started_at
                    and not self._stall_reported_exit
                ):
                    self._stall_reported_exit = True
                    trigger_exit = True

            if trigger_exit:
                self._record_crash_and_exit()

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
        with self._queue_lock, self._state_lock:
            if self._state == ModelState.IDLE and self._active_requests > 0:
                self._state = ModelState.READY
            return self._state

    def _set_state(self, new_state: ModelState, error_msg: Optional[str] = None):
        """线程安全的状态转移"""
        with self._state_lock:
            old_state = self._state
            self._state = new_state
            if error_msg:
                self._last_error = error_msg
            elif new_state != ModelState.ERROR:
                self._last_error = None
            logger.info(f"Model state transition: {old_state.value} -> {new_state.value}")

    def _cancel_idle_timer(self):
        """取消闲置定时器"""
        with self._state_lock:
            if self._idle_timer is not None:
                self._idle_timer.cancel()
                self._idle_timer = None

    def _schedule_idle_unload(self):
        """重置并启动闲置卸载定时器"""
        if self.idle_timeout_seconds <= 0:
            return
        with self._queue_lock, self._state_lock:
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
        with self._queue_lock, self._state_lock:
            if self._state != ModelState.IDLE or self._active_requests > 0:
                return
            self._state = ModelState.UNLOADING

        try:
            self._do_unload_resources()
            with self._state_lock:
                if self._state == ModelState.UNLOADING:
                    self._set_state(ModelState.UNLOADED)
        except Exception as e:
            logger.error(f"Error during auto-unloading: {e}", exc_info=True)
            with self._state_lock:
                if self._state == ModelState.UNLOADING:
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

    def _load_model_sync(self) -> tuple[Any, Any]:
        """同步执行模型载入逻辑（工作线程内运行，构造局部对象并返回）"""
        tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        model = AutoModel.from_pretrained(self.model_path)

        if self.device == "cuda":
            if self.use_fp16:
                model = model.half()
            model = model.to("cuda")
        else:
            model = model.to("cpu")

        model.eval()
        return tokenizer, model

    def _load_worker(self, epoch: int, started_at: float) -> None:
        """单次模型加载的工作线程"""
        load_start = time.monotonic()
        METRICS.incr("model_loads_total")
        tokenizer = None
        model = None
        success = False
        error_msg = None
        try:
            logger.info(f"Starting model load for epoch {epoch} on device {self.device}")
            tokenizer, model = self._load_model_sync()
            success = True
        except Exception as e:
            logger.error(f"Model load failed in epoch {epoch}: {e}", exc_info=True)
            error_msg = str(e)
            METRICS.incr("model_load_failures_total")
        finally:
            load_duration_ms = (time.monotonic() - load_start) * 1000.0
            METRICS.observe("model_load_duration_ms", load_duration_ms)
            try:
                with self._state_lock:
                    if self._load_epoch == epoch and self._state == ModelState.LOADING:
                        if success and tokenizer is not None and model is not None:
                            self.tokenizer = tokenizer
                            self.model = model
                            self._load_started_at = None
                            self._stall_reported_timeout = False
                            self._stall_reported_exit = False
                            self._set_unhealthy(False)
                            self._last_error = None
                            self._set_state(ModelState.READY)
                            logger.info(
                                f"Model successfully mounted for epoch {epoch} in {load_duration_ms:.1f}ms"
                            )
                        else:
                            self._load_started_at = None
                            self._stall_reported_timeout = False
                            self._stall_reported_exit = False
                            self._set_unhealthy(True)
                            if not error_msg:
                                error_msg = "Model or tokenizer partially constructed (None)"
                            self._set_state(ModelState.ERROR, error_msg=error_msg)
                            METRICS.incr("model_load_failures_total")
                    else:
                        # Epoch changed or state no longer LOADING: discard local resources
                        logger.warning(
                            f"Model load result for epoch {epoch} discarded "
                            f"(current epoch={self._load_epoch}, state={self._state.value})"
                        )
                        if self._load_started_at == started_at:
                            self._load_started_at = None
                            self._stall_reported_timeout = False
                            self._stall_reported_exit = False

                        if model is not None:
                            del model
                            model = None
                        if tokenizer is not None:
                            del tokenizer
                            tokenizer = None
                        gc.collect()
                        if torch.cuda.is_available():
                            torch.cuda.empty_cache()
                            if hasattr(torch.cuda, "ipc_collect"):
                                torch.cuda.ipc_collect()
            finally:
                if self._load_exec_lock.locked():
                    try:
                        self._load_exec_lock.release()
                    except RuntimeError:
                        pass

    def _trigger_load(self) -> None:
        """非阻塞尝试获取加载执行锁，触发单次加载"""
        acquired = self._load_exec_lock.acquire(blocking=False)
        if not acquired:
            return

        with self._state_lock:
            self._cancel_idle_timer()
            self._load_epoch += 1
            current_epoch = self._load_epoch
            started_at = time.monotonic()
            self._load_started_at = started_at
            self._stall_reported_timeout = False
            self._stall_reported_exit = False
            self._set_state(ModelState.LOADING)

        load_thread = threading.Thread(
            target=self._load_worker,
            args=(current_epoch, started_at),
            daemon=True,
            name=f"bge-loader-{current_epoch}",
        )
        self._current_loader_thread = load_thread
        load_thread.start()

    async def load_model(self) -> None:
        """公有异步方法：主动加载模型或推进至 READY 状态"""
        with self._state_lock:
            if self._closed:
                raise EngineError("Engine has already been closed")
            if self._state == ModelState.READY:
                return
            if self._state == ModelState.IDLE:
                self._cancel_idle_timer()
                self._set_state(ModelState.READY)
                return
            if self._state == ModelState.LOADING:
                pass
            else:
                self._trigger_load()

        try:
            await self._wait_for_ready(timeout=getattr(self.config, "load_timeout_seconds", 120.0))
        except EngineError as e:
            if "partially constructed" in str(e):
                return
            raise

    async def preload(self) -> None:
        """预加载模型至 READY 状态（幂等）"""
        with self._state_lock:
            if self._closed:
                raise EngineError("Engine has already been closed")
            if self._state == ModelState.READY:
                return
            if self._state == ModelState.IDLE:
                self._cancel_idle_timer()
                self._set_state(ModelState.READY)
                return
            if self._state == ModelState.LOADING:
                pass
            else:
                self._trigger_load()

        await self._wait_for_ready(timeout=getattr(self.config, "load_timeout_seconds", 120.0))

    async def unload_model(self) -> None:
        """公有异步方法：主动卸载模型释放显存"""
        with self._state_lock:
            if self._closed:
                raise EngineError("Engine has already been closed")
            self._cancel_idle_timer()
            self._load_epoch += 1
            self._load_started_at = None
            self._stall_reported_timeout = False
            self._stall_reported_exit = False
            if self._state == ModelState.UNLOADED:
                return
            self._set_state(ModelState.UNLOADING)

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(self._executor, self._do_unload_resources)
            self._set_state(ModelState.UNLOADED)
        except Exception as e:
            self._set_state(ModelState.ERROR, error_msg=str(e))
            raise

    async def _wait_for_ready(self, timeout: Optional[float] = None) -> None:
        """LOADING 状态排队自旋等待机制"""
        if timeout is None:
            timeout = getattr(self.config, "request_load_wait_seconds", 25.0)

        start_time = time.monotonic()
        while True:
            current_state = self.state
            if current_state == ModelState.READY:
                return
            if current_state == ModelState.IDLE:
                with self._state_lock:
                    if self._state == ModelState.IDLE:
                        self._cancel_idle_timer()
                        self._set_state(ModelState.READY)
                        return
            if current_state == ModelState.ERROR:
                raise EngineError(f"Model failed to load: {self._last_error}")

            if current_state not in (
                ModelState.LOADING,
                ModelState.UNLOADED,
                ModelState.UNLOADING,
            ):
                raise EngineError(f"Unexpected state: {current_state}")

            elapsed = time.monotonic() - start_time
            if elapsed >= timeout:
                raise ModelLoadingPendingError(
                    f"Timed out waiting for model READY ({timeout}s)",
                    model_state=current_state.value,
                    retry_after_ms=1000,
                )

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
        with self._state_lock:
            if self._closed:
                raise EngineError("Engine has already been closed")
        if not texts:
            return []

        from_unloading = False
        with self._queue_lock:
            if self._active_requests >= self.max_queue_size:
                raise QueueFullError(
                    f"Inference queue full ({self._active_requests}/{self.max_queue_size})"
                )
            self._active_requests += 1

        try:
            while True:
                with self._state_lock:
                    if self._closed:
                        raise EngineError("Engine has already been closed")
                current_state = self.state
                if current_state == ModelState.READY:
                    break
                elif current_state == ModelState.IDLE:
                    with self._state_lock:
                        if self._state == ModelState.IDLE:
                            self._cancel_idle_timer()
                            self._set_state(ModelState.READY)
                            break
                elif current_state == ModelState.LOADING:
                    await self._wait_for_ready(timeout=self.config.request_load_wait_seconds)
                elif current_state in (
                    ModelState.UNLOADED,
                    ModelState.ERROR,
                    ModelState.UNLOADING,
                ):
                    if current_state == ModelState.UNLOADING:
                        from_unloading = True
                    self._trigger_load()
                    await self._wait_for_ready(timeout=self.config.request_load_wait_seconds)
                else:
                    raise EngineError(f"Unexpected state: {current_state}")

            if self.state != ModelState.READY:
                raise EngineError(f"Engine is not in READY state (current: {self.state})")

            all_embeddings: List[List[float]] = []
            batches = [
                texts[i : i + self.max_batch]
                for i in range(0, len(texts), self.max_batch)
            ]

            loop = asyncio.get_running_loop()
            for batch in batches:
                with self._state_lock:
                    if self._closed:
                        raise EngineError("Engine has already been closed")
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
            if isinstance(e, EngineError):
                raise
            if self._closed or isinstance(e, RuntimeError):
                raise EngineError(f"Engine inference interrupted: {e}") from e
            raise

        finally:
            with self._queue_lock, self._state_lock:
                self._active_requests -= 1
                if self._active_requests == 0 and self._state == ModelState.READY:
                    if self.idle_timeout_seconds > 0 and not from_unloading:
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
            "unhealthy": self.unhealthy,
        }

    def close(self):
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            self._load_epoch += 1
            self._load_started_at = None
            self._stall_reported_timeout = False
            self._stall_reported_exit = False

        self._cancel_idle_timer()
        self._watchdog_stop_event.set()

        if self._watchdog_thread and self._watchdog_thread.is_alive():
            if threading.current_thread() != self._watchdog_thread:
                self._watchdog_thread.join(timeout=1.0)

        if self._current_loader_thread and self._current_loader_thread.is_alive():
            if threading.current_thread() != self._current_loader_thread:
                self._current_loader_thread.join(timeout=2.5)

        if self._load_exec_lock.locked():
            try:
                self._load_exec_lock.release()
            except RuntimeError:
                pass

        is_event_loop_thread = False
        try:
            asyncio.get_running_loop()
            is_event_loop_thread = True
        except RuntimeError:
            is_event_loop_thread = False

        if not is_event_loop_thread:
            wait_until = time.monotonic() + 2.0
            while time.monotonic() < wait_until:
                with self._queue_lock:
                    if self._active_requests == 0:
                        break
                time.sleep(0.02)

        try:
            self._executor.shutdown(wait=not is_event_loop_thread, cancel_futures=True)
        except TypeError:
            self._executor.shutdown(wait=not is_event_loop_thread)

        self._do_unload_resources()
        self._set_state(ModelState.UNLOADED)

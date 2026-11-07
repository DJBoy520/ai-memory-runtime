"""跨层共享的领域异常：携带结构化上下文，由 IPC 层映射为稳定错误码。

设计约定（docs/rfcs/RFC-AMR-RELIABILITY-FIXES.md D3）：
- 核心层只抛领域异常，不感知 JSON-RPC / 传输细节；
- 每个异常携带机器可读字段（current_version 等），上层无需解析错误消息字符串。
"""
from typing import Iterable, Optional


class VersionConflictError(ValueError):
    """乐观锁冲突：content 修改时 expected_version 与当前版本不一致。

    继承 ValueError 以兼容既有契约测试（tests/test_v3_contracts.py 的
    pytest.raises(ValueError, match="VERSION_CONFLICT")）与消息前缀约定。
    """

    def __init__(
        self,
        message: str,
        *,
        memory_id: Optional[str] = None,
        current_version: Optional[int] = None,
        expected_version: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.memory_id = memory_id
        self.current_version = current_version
        self.expected_version = expected_version


class ModelLoadingPendingError(Exception):
    """模型正在加载且超出单个请求的等待预算：可重试（IPC 层映射 MODEL_LOADING）。

    与 engine.ModelLoadingTimeoutError（加载本身超时/失败）严格区分：
    本异常不代表加载失败，共享加载过程仍在进行，调用方应稍后重试。
    """

    def __init__(
        self,
        message: str,
        *,
        model_state: str = "LOADING",
        retry_after_ms: int = 1000,
    ) -> None:
        super().__init__(message)
        self.model_state = model_state
        self.retry_after_ms = retry_after_ms


class InvalidStatusError(ValueError):
    """状态值不在统一枚举内（含 legacy 值未通过 normalize_status 的场景）。"""

    def __init__(
        self,
        message: str,
        *,
        value: Optional[str] = None,
        allowed: Optional[Iterable[str]] = None,
    ) -> None:
        super().__init__(message)
        self.value = value
        self.allowed = list(allowed or [])

"""
AI Memory Runtime - Model Worker Subsystem for Cognitive Distillation
遵循 RFC-004、RFC-005 及系统安全规范：
1. 提供显式配置的 OpenAI 兼容端点模型工作器（OpenAICompatibleWorker）；
2. 默认安全防线：默认不发送真实会话、不自动启用外部模型（DryRunWorker / MockModelWorker）；
3. 严格超时与异常隔离：严格控制 HTTP 连接与读取超时，失败原因清晰可查；
4. 绝对隐私安全铁律：严禁在任何日志中打印凭据（API Keys/Tokens）或会话原始内容；
5. 自动去除 Markdown 代码块包裹，强校验返回的 JSON 结构。
"""

import asyncio
from abc import ABC, abstractmethod
import json
import logging
import os
import re
import socket
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger("amr_model_worker")


def sanitize_text(text: Optional[str]) -> str:
    """
    清洗文本中的敏感信息（API Key、Token、Bearer 鉴权头、密码等），
    严禁泄露凭据。
    """
    if not text:
        return ""
    # 替换 Bearer token
    s = re.sub(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]+", "Bearer ***", text)
    # 替换 OpenAI/通用 sk- 密钥
    s = re.sub(r"sk-[a-zA-Z0-9_\-]{8,}", "sk-***", s)
    # 替换 key=..., secret=..., token=... 赋值形式
    s = re.sub(
        r'''(?i)(api[_-]?key|secret|token|password|auth)["']?\s*[:=]\s*["']?[a-zA-Z0-9_\-\.]+''',
        r"\1=***",
        s,
    )
    # 替换 URL Query 中的 token/key 参数
    s = re.sub(r"([?&](?:api_key|key|token|secret|access_token)=)[^&\s]+", r"\1***", s)
    return s


def mask_secret(val: Optional[str]) -> str:
    """返回掩码后的敏感字符串（如 sk-12***ab）"""
    if not val:
        return ""
    if len(val) <= 6:
        return "***"
    return f"{val[:3]}***{val[-2:]}"


class WorkerError(Exception):
    """模型工作器通用基础异常"""

    def __init__(self, message: str):
        super().__init__(sanitize_text(message))


class ExternalModelDisabledError(WorkerError):
    """当未显式授权调用外部模型时抛出"""
    pass


class ModelTimeoutError(WorkerError):
    """当模型端点调用超时（严格超时）时抛出"""
    pass


class ModelAPIError(WorkerError):
    """模型端点 HTTP 异常或网络通信异常"""

    def __init__(self, status_code: int, message: str, body: Optional[str] = None):
        self.status_code = status_code
        self.response_format_unsupported = bool(body and "response_format" in body.lower())
        self.raw_message = message
        clean_msg = sanitize_text(message)
        super().__init__(f"ModelAPIError (HTTP {status_code}): {clean_msg}")


class ModelResponseParseError(WorkerError):
    """模型输出非合法 JSON 或不符合 Schema 契约"""
    pass


class BaseModelWorker(ABC):
    """模型工作器基类接口"""

    model_name: str = "base_worker"
    is_external: bool = False

    @abstractmethod
    async def __call__(self, system_prompt: str, user_content: str) -> Dict[str, Any]:
        """
        调用模型执行提炼：
        入参：system_prompt, user_content
        返回：解析后的 JSON 字典对象
        """
        raise NotImplementedError


class OpenAICompatibleWorker(BaseModelWorker):
    """
    OpenAI 兼容端点模型工作器：
    - 支持 base_url、model、api_key 显式配置；
    - 严格遵循 allow_external 门禁铁律：未显式开启时绝对拒绝向外部发送网络请求；
    - 严格超时控制 (timeout)；
    - 零凭据、零会话内容泄漏日志保护；
    - 兼容 ```json 代码块去除与 JSON Schema 校验。
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000/v1",
        model: str = "gpt-4o",
        api_key: Optional[str] = None,
        timeout: float = 30.0,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        allow_external: bool = False,
        custom_headers: Optional[Dict[str, str]] = None,
    ):
        raw_url = (base_url or "http://localhost:8000/v1").strip().rstrip("/")
        self.base_url = raw_url
        self.model_name = (model or "gpt-4o").strip()
        self.api_key = (api_key or "").strip()
        self.timeout = float(timeout) if timeout is not None else 30.0
        if self.timeout <= 0:
            raise ValueError(f"Timeout must be positive, got {timeout}")
        self.temperature = float(temperature)
        self.max_tokens = max_tokens
        self.allow_external = bool(allow_external)
        self.custom_headers = custom_headers or {}
        self.is_external = True

    def _resolve_completions_url(self) -> str:
        """根据 base_url 智能规整出完整的 /chat/completions 路由"""
        url = self.base_url
        if url.endswith("/chat/completions"):
            return url
        if url.endswith("/v1"):
            return f"{url}/chat/completions"
        return f"{url}/v1/chat/completions"

    def _build_request_payload(self, system_prompt: str, user_content: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
        }
        if self.max_tokens:
            payload["max_tokens"] = self.max_tokens
        return payload

    def _sync_http_post(
        self,
        url: str,
        headers: Dict[str, str],
        data_bytes: bytes,
        timeout: float,
    ) -> Tuple[int, str]:
        """
        同步底层 HTTP POST 发送函数，由 asyncio.to_thread 驱动。
        使用标准库 urllib.request，严格控制超时并保护凭据。
        """
        req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status_code = resp.status
                body_bytes = resp.read(4 * 1024 * 1024 + 1)
                if len(body_bytes) > 4 * 1024 * 1024:
                    raise ModelResponseParseError("Model response exceeds size limit")
                body = body_bytes.decode("utf-8")
                return status_code, body
        except urllib.error.HTTPError as ex:
            status = ex.code
            # Upstream errors can echo the prompt, including arbitrary private text.
            body = ex.read(16384).decode("utf-8", errors="replace")
            raise ModelAPIError(status_code=status, message=f"HTTP {status}", body=body) from ex
        except (socket.timeout, TimeoutError) as ex:
            raise ModelTimeoutError(f"HTTP request timed out after {timeout:.1f}s") from ex
        except urllib.error.URLError as ex:
            if isinstance(ex.reason, (socket.timeout, TimeoutError)) or "timed out" in str(ex.reason).lower():
                raise ModelTimeoutError(f"HTTP request timed out after {timeout:.1f}s") from ex
            clean_reason = sanitize_text(str(ex.reason))
            raise ModelAPIError(status_code=0, message=f"Connection failed: {clean_reason}") from ex
        except Exception as ex:
            clean_ex = sanitize_text(str(ex))
            raise ModelAPIError(status_code=0, message=f"Unexpected network error: {clean_ex}") from ex

    async def __call__(self, system_prompt: str, user_content: str) -> Dict[str, Any]:
        """
        异步入口：执行模型调用并返回结构化字典
        """
        # 1. 核心铁律保护：未显式开启外部模型时，绝不发送真实网络请求
        if not self.allow_external:
            raise ExternalModelDisabledError(
                "External model execution is disabled by default. "
                "Explicitly enable via --enable-external-model, allow_external=True, "
                "or AMR_DISTILL_ENABLE_EXTERNAL=true."
            )

        url = self._resolve_completions_url()

        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if self.custom_headers:
            headers.update(self.custom_headers)

        payload = self._build_request_payload(system_prompt, user_content)
        payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        # 2. 严格超时请求（注意：日志中绝对不打印 payload、user_content 或 Authorization 凭据）
        start_time = time.time()
        logger.info(
            "Calling OpenAI-compatible model worker (model: %s, timeout: %.1fs)",
            self.model_name,
            self.timeout,
        )

        try:
            status_code, resp_text = await asyncio.to_thread(
                self._sync_http_post,
                url,
                headers,
                payload_bytes,
                self.timeout,
            )
        except ModelAPIError as api_err:
            # 兼容某些不支持 response_format="json_object" 的第三方后端，自动降级重试一次
            if api_err.status_code == 400 and api_err.response_format_unsupported:
                logger.info("Endpoint does not support response_format, retrying without it...")
                payload.pop("response_format", None)
                payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                status_code, resp_text = await asyncio.to_thread(
                    self._sync_http_post,
                    url,
                    headers,
                    payload_bytes,
                    self.timeout,
                )
            else:
                raise

        duration = time.time() - start_time
        logger.info(
            "Model worker responded in %.2fs (HTTP status: %d)",
            duration,
            status_code,
        )

        # 3. 解析与清洗响应内容
        try:
            resp_data = json.loads(resp_text)
        except Exception as ex:
            raise ModelResponseParseError("Model returned non-JSON HTTP response body") from ex

        if not isinstance(resp_data, dict):
            raise ModelResponseParseError(
                f"Expected JSON object in model response, got {type(resp_data).__name__}"
            )

        choices = resp_data.get("choices")
        if not choices or not isinstance(choices, list):
            raise ModelResponseParseError("Model response missing 'choices' list")

        msg = choices[0].get("message") or {}
        raw_content = msg.get("content", "")
        if not raw_content:
            raise ModelResponseParseError("Model response choices[0].message.content is empty")

        # 4. 去除可能存在的 Markdown 代码块包裹 ```json ... ```
        cleaned_content = raw_content.strip()
        if cleaned_content.startswith("```"):
            lines = cleaned_content.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned_content = "\n".join(lines).strip()

        try:
            parsed_result = json.loads(cleaned_content)
        except Exception as ex:
            raise ModelResponseParseError(
                "Failed to parse JSON dictionary from model message content"
            ) from ex

        if not isinstance(parsed_result, dict):
            raise ModelResponseParseError(
                f"Distillation output must be a JSON object, got {type(parsed_result).__name__}"
            )

        # 补充默认 prompt_version 字段以满足契约
        if "prompt_version" not in parsed_result:
            parsed_result["prompt_version"] = "v3.0.0-mos"

        return parsed_result


class MockModelWorker(BaseModelWorker):
    """
    轻量模拟模型工作器：
    用于单元测试、离线测试与仿真，无需发起外部网络连接。
    """

    def __init__(
        self,
        model_name: str = "mock-llm-worker",
        static_response: Optional[Dict[str, Any]] = None,
        handler: Optional[Callable[[str, str], Any]] = None,
    ):
        self.model_name = model_name
        self.static_response = static_response
        self.handler = handler
        self.is_external = False

    async def __call__(self, system_prompt: str, user_content: str) -> Dict[str, Any]:
        if self.handler is not None:
            res = self.handler(system_prompt, user_content)
            if asyncio.iscoroutine(res):
                res = await res
            if isinstance(res, str):
                return json.loads(res)
            return res
        if self.static_response is not None:
            return self.static_response

        # 默认返回安全空提纯结构
        messages = []
        try:
            messages = json.loads(user_content)
        except Exception:
            pass

        return {
            "prompt_version": "v3.0.0-mos",
            "candidates": [],
            "message_disposition": [
                {"message_id": m.get("message_id", "m_unknown"), "disposition": "DISCARDED", "reason": "mock_no_extraction"}
                for m in messages
            ],
        }


class DryRunWorker(BaseModelWorker):
    """
    安全默认 Dry-Run 工作器：
    当用户未显式开启外部模型时默认装载。
    不发起任何外联，对所有消息自动输出保全性 DISCARDED（明确注明 DRY_RUN），
    保证认知流水线 CoverageValidator 守恒通过，同时零编造、零外泄。
    """

    def __init__(self, model_name: str = "dry-run-safe-mode"):
        self.model_name = model_name
        self.is_external = False

    async def __call__(self, system_prompt: str, user_content: str) -> Dict[str, Any]:
        messages = []
        try:
            messages = json.loads(user_content)
        except Exception:
            pass

        return {
            "prompt_version": "v3.0.0-mos",
            "candidates": [],
            "message_disposition": [
                {
                    "message_id": m.get("message_id", "m_unknown"),
                    "disposition": "DISCARDED",
                    "reason": "[DRY_RUN] Safe default: external model is disabled.",
                }
                for m in messages
            ],
        }

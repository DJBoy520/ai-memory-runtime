"""
tests/test_distill_runner.py
专属集成与单元测试：AMR 会话认知提纯调度入口与模型工作器
覆盖要求：
1. 隔离假 HTTP 模型服务（FakeOpenAIHandler + ThreadingHTTPServer），真实验证网络交互、鉴权头与超时；
2. 临时 SQLite 数据库隔离，完全不访问生产数据；
3. 验证增量扫描（通用会话兼顾 messages 与 raw_messages）；
4. 验证提炼闭环与 14 天 Dry-Run 铁律（仅入 curation_candidates PROPOSED，绝不写 memories SSOT）；
5. 验证默认安全底线：默认不外联、不自动启用外部模型；
6. 验证严格超时、模型调用失败可见性与保留待重试、二次重试成功；
7. 验证绝对隐私安全：严禁日志打印凭据或会话内容。
"""

import asyncio
import json
import logging
import os
import re
import socket
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
import pytest

from src.core.session_store import SessionStore
from src.cognitive.model_worker import (
    DryRunWorker,
    ExternalModelDisabledError,
    MockModelWorker,
    ModelAPIError,
    ModelResponseParseError,
    ModelTimeoutError,
    OpenAICompatibleWorker,
    mask_secret,
    sanitize_text,
)
from src.cognitive.distill_runner import (
    DistillationRunner,
    resolve_runner_config,
)


class FakeOpenAIHandler(BaseHTTPRequestHandler):
    """隔离假 HTTP OpenAI 兼容端点处理器"""

    def log_message(self, format: str, *args: Any) -> None:
        # 静默标准日志，防止污染测试输出
        pass

    def do_POST(self) -> None:
        server: "FakeOpenAIServer" = self.server  # type: ignore

        # 读取请求头与内容
        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length)
        body_str = body_bytes.decode("utf-8")

        # 记录请求信息供断言
        req_record = {
            "path": self.path,
            "headers": dict(self.headers),
            "body_raw": body_str,
        }
        try:
            req_record["body_json"] = json.loads(body_str)
        except Exception:
            pass

        with server.lock:
            server.received_requests.append(req_record)
            server.request_count += 1
            current_count = server.request_count
            mode = server.mode
            fail_times = server.fail_times
            sleep_sec = server.sleep_sec

        # 1. 模拟超时模式
        if mode == "timeout":
            time.sleep(sleep_sec)
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"choices": [{"message": {"content": "{}"}}]}')
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            return

        # 2. 模拟前 N 次失败后恢复成功
        if mode == "fail_n_times_then_succeed":
            if current_count <= fail_times:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error": {"message": "Simulated upstream temporary outage", "type": "server_error"}}')
                return

        # 3. 模拟恒常 500 错误
        if mode == "always_500":
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error": {"message": "Upstream internal error 500", "code": 500}}')
            return

        if mode == "echo_private_error":
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": {"message": "private-conversation-sentinel"}}).encode())
            return

        # 4. 模拟 429 速率限制
        if mode == "rate_limit":
            self.send_response(429)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error": {"message": "Rate limit exceeded, please retry later"}}')
            return

        # 5. 模拟损坏的响应
        if mode == "invalid_json":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'INVALID_JSON_CORRUPTED_STREAM')
            return

        # 6. 成功返回合法提取产物 (符合 JSON Schema，证据与 spans 精确匹配会话)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        mock_distill_result = {
            "prompt_version": "v3.0.0-mos",
            "session_id": "mock_sess",
            "candidates": [
                {
                    "proposal_id": "prop_fake_01",
                    "operation": "EXTRACT",
                    "subject": "StorageConfig",
                    "predicate": "uses_wal_mode",
                    "object": "true",
                    "content": "AMR 会话数据库启用 SQLite WAL 模式，保障写入并发安全性。",
                    "evidence_source_ids": ["msg_001"],
                    "extracted_spans": [
                        {"message_id": "msg_001", "span": "AMR 会话数据库启用 SQLite WAL 模式，保障写入并发安全性。"}
                    ],
                    "rationale": "持久化系统核心配置",
                    "self_assessed_confidence": 0.95,
                }
            ],
            "message_disposition": [
                {"message_id": "msg_001", "disposition": "EXTRACTED"},
                {"message_id": "msg_002", "disposition": "DISCARDED", "reason": "闲聊消息"},
            ],
        }

        # 包装成 OpenAI chat.completions 响应格式，测试带 Markdown 代码块解析
        resp_payload = {
            "id": "chatcmpl-test-fake-01",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "```json\n" + json.dumps(mock_distill_result, ensure_ascii=False) + "\n```",
                    },
                    "finish_reason": "stop",
                }
            ],
        }
        self.wfile.write(json.dumps(resp_payload, ensure_ascii=False).encode("utf-8"))


class FakeOpenAIServer(ThreadingHTTPServer):
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        super().__init__((host, port), FakeOpenAIHandler)
        self.lock = threading.Lock()
        self.received_requests: List[Dict[str, Any]] = []
        self.request_count = 0
        self.mode = "success"
        self.fail_times = 0
        self.sleep_sec = 0.5


@pytest.fixture
def fake_http_model():
    """启动本地隔离假 HTTP OpenAI 模型服务，使用随机可用端口"""
    server = FakeOpenAIServer()
    port = server.server_port
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    server.base_url = f"http://127.0.0.1:{port}/v1"
    yield server

    server.shutdown()
    server.server_close()


@pytest.fixture
def temp_db():
    """临时隔离 SQLite 数据库存储"""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        store = SessionStore(db_path=tmp.name)
        yield store


def seed_test_session_data(store: SessionStore, session_id: str = "test_sess_001"):
    """向临时存储灌入标准会话与消息数据"""
    now = int(time.time())
    # 模拟 MCP ingest 录入会话
    messages = [
        {
            "message_id": "msg_001",
            "role": "user",
            "content": "AMR 会话数据库启用 SQLite WAL 模式，保障写入并发安全性。",
            "sequence": 1,
            "timestamp": now - 60,
        },
        {
            "message_id": "msg_002",
            "role": "assistant",
            "content": "收到，这是闲聊回复，天气不错。",
            "sequence": 2,
            "timestamp": now - 30,
        },
    ]
    store.ingest_messages(
        session_id=session_id,
        agent_id="test_agent",
        project_id="test_project",
        messages=messages,
    )


# =========================================================================
# 1. 默认安全门禁测试
# =========================================================================

def test_default_safe_mode_refuses_to_call_external(temp_db, fake_http_model):
    """
    铁律验证：默认安全模式下，绝不向外部模型发送真实请求。
    """
    seed_test_session_data(temp_db, "sess_safe_001")

    # 未传入 enable_external_model / allow_external
    runner = DistillationRunner(
        session_store=temp_db,
        allow_external=False,
        dry_run=True,
    )

    summary = asyncio.run(runner.run_batch(lookback_seconds=86400))

    # 验证没有发起任何 HTTP 请求
    assert fake_http_model.request_count == 0
    assert summary["dry_run"] is True
    assert summary["external_model_enabled"] is False
    assert summary["sessions_succeeded"] == 1
    assert summary["total_written_candidates"] == 0  # Dry-Run 自动保全，零提取


def test_openai_worker_direct_gate_rejection():
    """
    工作器级别安全铁律：直接调用 OpenAICompatibleWorker 未授权时必定抛出 ExternalModelDisabledError
    """
    worker = OpenAICompatibleWorker(
        base_url="http://127.0.0.1:9999/v1",
        model="gpt-4o",
        allow_external=False,  # 默认关闭
    )

    with pytest.raises(ExternalModelDisabledError) as exc_info:
        asyncio.run(worker("system_prompt", "[]"))
    assert "disabled by default" in str(exc_info.value)


@pytest.mark.asyncio
async def test_dry_run_does_not_mark_session_processed(temp_db, fake_http_model):
    seed_test_session_data(temp_db, "dry-then-real")
    worker = OpenAICompatibleWorker(base_url=fake_http_model.base_url, model="test", allow_external=True)
    dry = DistillationRunner(session_store=temp_db, model_worker=worker, allow_external=True, dry_run=True)
    await dry.run_batch(lookback_seconds=3600)
    assert fake_http_model.request_count == 0
    real = DistillationRunner(session_store=temp_db, model_worker=worker, allow_external=True)
    assert any(s["session_id"] == "dry-then-real" for s in real.scan_sessions(lookback_seconds=3600))


@pytest.mark.asyncio
async def test_successful_session_is_rescanned_after_append(temp_db, fake_http_model):
    sid = "growing-session"
    seed_test_session_data(temp_db, sid)
    worker = OpenAICompatibleWorker(base_url=fake_http_model.base_url, model="test", allow_external=True)
    runner = DistillationRunner(session_store=temp_db, model_worker=worker, allow_external=True)
    result = await runner.run_batch(lookback_seconds=3600)
    assert result["sessions_succeeded"] == 1
    assert not any(s["session_id"] == sid for s in runner.scan_sessions(lookback_seconds=3600))
    temp_db.ingest_messages(sid, "test_agent", "test_project", [{"message_id": "new-message", "role": "user", "content": "A later message", "timestamp": int(time.time())}])
    assert any(s["session_id"] == sid for s in runner.scan_sessions(lookback_seconds=3600))
    rerun = await runner.run_batch(lookback_seconds=3600)
    assert rerun["sessions_succeeded"] == 1
    assert temp_db.get_connection().execute("SELECT count(*) FROM curation_candidates").fetchone()[0] == 1


def test_processed_sessions_do_not_starve_unprocessed_with_small_limit(temp_db):
    for sid in ("one", "two", "three"):
        seed_test_session_data(temp_db, sid)
    runner = DistillationRunner(session_store=temp_db, allow_external=False)
    ordered = runner.scheduler.scan_incremental_sessions(lookback_seconds=3600, limit=100)
    conn = temp_db.get_connection()
    for session in ordered[:2]:
        sid = session["session_id"]
        conn.execute("INSERT INTO curation_distillation_state (session_id, batch_id, status, created_at, updated_at, source_digest) VALUES (?, 'b', 'SUCCESS', 1, 1, ?)", (sid, runner._source_digest(sid)))
    conn.commit()
    scanned = runner.scan_sessions(lookback_seconds=3600, limit=1)
    assert [s["session_id"] for s in scanned] == [ordered[2]["session_id"]]


@pytest.mark.asyncio
async def test_model_error_cannot_echo_private_content_into_logs(temp_db, fake_http_model, caplog):
    seed_test_session_data(temp_db, "private-test")
    fake_http_model.mode = "echo_private_error"
    worker = OpenAICompatibleWorker(base_url=fake_http_model.base_url, model="test", allow_external=True)
    runner = DistillationRunner(session_store=temp_db, model_worker=worker, allow_external=True, default_max_retries=1)
    with caplog.at_level(logging.DEBUG):
        summary = await runner.run_batch(lookback_seconds=3600)
    assert summary["sessions_failed"] == 1
    assert "private-conversation-sentinel" not in " ".join(r.getMessage() for r in caplog.records)
    assert "private-conversation-sentinel" not in json.dumps(summary)


# =========================================================================
# 2. 端到端扫描、假 HTTP 模型提炼与 14 天 Dry-Run 验证
# =========================================================================

@pytest.mark.asyncio
async def test_fake_http_model_scanning_and_distillation(temp_db, fake_http_model):
    """
    测试端到端闭环：
    1. 扫描增量会话；
    2. 假 HTTP 模型解析与提炼；
    3. EvidenceValidator & CoverageValidator 门禁通过；
    4. 14 天 Dry-Run 铁律：写入 curation_candidates (PROPOSED)，memories 零写入！
    """
    session_id = "sess_e2e_001"
    seed_test_session_data(temp_db, session_id)

    worker = OpenAICompatibleWorker(
        base_url=fake_http_model.base_url,
        model="gpt-4o-test",
        api_key="sk-test-secret-key-12345",
        timeout=10.0,
        allow_external=True,  # 显式授权
    )

    runner = DistillationRunner(
        session_store=temp_db,
        model_worker=worker,
        allow_external=True,
        dry_run=False,
    )

    # 1. 扫描会话
    scanned = runner.scan_sessions(lookback_seconds=3600)
    assert len(scanned) >= 1
    assert any(s["session_id"] == session_id for s in scanned)

    # 2. 批量执行
    summary = await runner.run_batch(lookback_seconds=3600)

    assert summary["sessions_succeeded"] == 1
    assert summary["sessions_failed"] == 0
    assert summary["total_written_candidates"] == 1

    # 3. 验证假 HTTP 模型收到了带有 Bearer 的鉴权请求
    assert fake_http_model.request_count == 1
    req = fake_http_model.received_requests[0]
    auth_header = req["headers"].get("Authorization") or req["headers"].get("authorization")
    assert auth_header == "Bearer sk-test-secret-key-12345"

    # 4. 验证 14 天 Dry-Run 铁律：
    # curation_candidates 隔离表中存在 PROPOSED 记录
    conn = temp_db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM curation_candidates WHERE state = 'PROPOSED'")
    cand = cur.fetchone()
    assert cand is not None
    assert cand["state"] == "PROPOSED"
    assert cand["operation"] == "EXTRACT"
    assert cand["model_name"] == "gpt-4o-test"
    assert "SQLite WAL" in cand["content"]

    # memories SSOT 主表中绝对没有任何写入！
    cur.execute("SELECT count(*) as cnt FROM memories")
    assert cur.fetchone()["cnt"] == 0

    # 5. 验证 curation_distillation_state 状态表记录为 SUCCESS
    cur.execute("SELECT status, attempts, candidates_count FROM curation_distillation_state WHERE session_id = ?", (session_id,))
    state = cur.fetchone()
    assert state is not None
    assert state["status"] == "SUCCESS"
    assert state["attempts"] == 1
    assert state["candidates_count"] == 1


# =========================================================================
# 3. 模型失败、错误可见性与保留待重试验证
# =========================================================================

@pytest.mark.asyncio
async def test_fake_http_model_transient_failure_and_retry_success(temp_db, fake_http_model):
    """
    测试偶发故障：假模型前 2 次返回 500 错误，第 3 次成功返回。
    验证调度器自动执行重试并在最终成功入库。
    """
    session_id = "sess_transient_002"
    seed_test_session_data(temp_db, session_id)

    fake_http_model.mode = "fail_n_times_then_succeed"
    fake_http_model.fail_times = 2

    worker = OpenAICompatibleWorker(
        base_url=fake_http_model.base_url,
        model="gpt-4o-retry",
        api_key="sk-retry-key",
        timeout=10.0,
        allow_external=True,
    )

    runner = DistillationRunner(
        session_store=temp_db,
        model_worker=worker,
        allow_external=True,
        default_max_retries=3,
        default_retry_delay=0.01,
    )

    summary = await runner.run_batch(lookback_seconds=3600)
    assert summary["sessions_succeeded"] == 1
    assert summary["sessions_failed"] == 0
    assert fake_http_model.request_count == 3  # 失败 2 次，重试第 3 次成功


@pytest.mark.asyncio
async def test_fake_http_model_permanent_failure_retained_for_retry(temp_db, fake_http_model):
    """
    测试持续故障：上游服务始终报 500。
    验证：
    1. 失败可见，详细原因记录入 curation_distillation_state 并标记为 FAILED；
    2. 上游恢复后，通过 retry_failed_only 重新拉取并重试成功。
    """
    session_id = "sess_perm_fail_003"
    seed_test_session_data(temp_db, session_id)

    fake_http_model.mode = "always_500"

    worker = OpenAICompatibleWorker(
        base_url=fake_http_model.base_url,
        model="gpt-4o",
        api_key="sk-test",
        timeout=10.0,
        allow_external=True,
    )

    runner = DistillationRunner(
        session_store=temp_db,
        model_worker=worker,
        allow_external=True,
        default_max_retries=2,
        default_retry_delay=0.01,
    )

    # 1. 执行批处理，预期失败
    summary = await runner.run_batch(lookback_seconds=3600)
    assert summary["sessions_failed"] == 1
    assert summary["sessions_succeeded"] == 0
    assert len(summary["failed_sessions"]) == 1
    failed_item = summary["failed_sessions"][0]
    assert failed_item["session_id"] == session_id
    assert "500" in failed_item["error"]

    # 2. 检查失败是否已保留在状态库中
    failed_list = runner.get_failed_sessions()
    assert len(failed_list) == 1
    assert failed_list[0]["session_id"] == session_id
    assert failed_list[0]["attempts"] == 2

    # 3. 模拟上游模型服务恢复正常
    fake_http_model.mode = "success"

    # 4. 针对失败会话执行定向重试 (retry_failed_only=True)
    retry_summary = await runner.run_batch(
        retry_failed_only=True,
        lookback_seconds=3600,
    )
    assert retry_summary["sessions_succeeded"] == 1
    assert retry_summary["sessions_failed"] == 0
    assert retry_summary["total_written_candidates"] == 1

    # 5. 验证状态已更新为 SUCCESS，失败列表已清空
    assert len(runner.get_failed_sessions()) == 0


# =========================================================================
# 4. 严格超时控制验证
# =========================================================================

@pytest.mark.asyncio
async def test_strict_timeout_enforcement(temp_db, fake_http_model):
    """
    测试严格超时控制：端点延迟 0.5s，客户端配置超时 0.05s。
    必须立即触发 ModelTimeoutError 并中止，失败保留待重试。
    """
    session_id = "sess_timeout_004"
    seed_test_session_data(temp_db, session_id)

    fake_http_model.mode = "timeout"
    fake_http_model.sleep_sec = 0.5

    worker = OpenAICompatibleWorker(
        base_url=fake_http_model.base_url,
        model="gpt-4o",
        api_key="sk-test",
        timeout=0.05,  # 严格 50ms 超时
        allow_external=True,
    )

    runner = DistillationRunner(
        session_store=temp_db,
        model_worker=worker,
        allow_external=True,
        default_max_retries=1,
        default_retry_delay=0.0,
    )

    start_t = time.time()
    summary = await runner.run_batch(lookback_seconds=3600)
    elapsed = time.time() - start_t

    # 必须在很短时间内超时退出，严禁死锁阻塞
    assert elapsed < 0.4
    assert summary["sessions_failed"] == 1
    failed_info = summary["failed_sessions"][0]
    assert "timed out" in failed_info["error"].lower()


# =========================================================================
# 5. 凭据与会话内容零泄漏隐私验证
# =========================================================================

@pytest.mark.asyncio
async def test_credential_and_content_privacy_in_logs(temp_db, fake_http_model, caplog):
    """
    绝对隐私安全铁律验证：
    无论是成功还是网络异常，日志中绝对不得出现：
    1. 真实 API Key 凭据；
    2. 会话中的敏感私密内容。
    """
    secret_key = "sk-super-secret-production-token-998877"
    sensitive_session_text = "CONFIDENTIAL_PROJECT_PASSWORD_XY_778899"

    now = int(time.time())
    session_id = "sess_confidential_005"
    temp_db.ingest_messages(
        session_id=session_id,
        agent_id="agent_x",
        project_id="proj_x",
        messages=[
            {"message_id": "m1", "role": "user", "content": f"核心机密: {sensitive_session_text}", "sequence": 1, "timestamp": now}
        ],
    )

    fake_http_model.mode = "always_500"  # 故意让其报错

    worker = OpenAICompatibleWorker(
        base_url=fake_http_model.base_url,
        model="gpt-4o",
        api_key=secret_key,
        timeout=1.0,
        allow_external=True,
    )

    runner = DistillationRunner(
        session_store=temp_db,
        model_worker=worker,
        allow_external=True,
        default_max_retries=1,
    )

    with caplog.at_level(logging.DEBUG):
        await runner.run_batch(lookback_seconds=3600)

    all_logs = " ".join([record.getMessage() for record in caplog.records])

    # 严禁出现完整 API Key
    assert secret_key not in all_logs
    # 严禁出现会话私密文本
    assert sensitive_session_text not in all_logs


# =========================================================================
# 6. 配置解析与工具函数单元测试
# =========================================================================

def test_sanitize_text_utility():
    raw = "Failed Authorization: Bearer sk-1234567890abcdef and api_key=sk-secret-key-456"
    sanitized = sanitize_text(raw)
    assert "sk-1234567890abcdef" not in sanitized
    assert "sk-secret-key-456" not in sanitized
    assert "Bearer ***" in sanitized
    assert "api_key=***" in sanitized


def test_mask_secret_utility():
    assert mask_secret("") == ""
    assert mask_secret("short") == "***"
    assert mask_secret("sk-1234567890abcdef") == "sk-***ef"


def test_resolve_runner_config_defaults_and_env(monkeypatch):
    # 默认值验证
    cfg = resolve_runner_config()
    assert cfg["enable_external_model"] is False
    assert cfg["timeout"] == 30.0

    # 环境变量覆盖验证
    monkeypatch.setenv("AMR_DISTILL_ENABLE_EXTERNAL", "true")
    monkeypatch.setenv("AMR_DISTILL_BASE_URL", "http://remote:8000/v1")
    monkeypatch.setenv("AMR_DISTILL_MODEL", "qwen2.5-72b")
    monkeypatch.setenv("AMR_DISTILL_API_KEY", "env-key-999")
    monkeypatch.setenv("AMR_DISTILL_TIMEOUT", "45.0")

    cfg_env = resolve_runner_config()
    assert cfg_env["enable_external_model"] is True
    assert cfg_env["base_url"] == "http://remote:8000/v1"
    assert cfg_env["model"] == "qwen2.5-72b"
    assert cfg_env["api_key"] == "env-key-999"
    assert cfg_env["timeout"] == 45.0


def test_discovered_backlog_survives_window_expiry_and_restart(temp_db):
    now = int(time.time())
    for sid in ('backlog-a', 'backlog-b'):
        temp_db.record_raw_session(session_id=sid, agent_id='agent', project_id='project', started_at=now)
        temp_db.record_raw_message(message_id=f'{sid}-msg', session_id=sid, role='user', content='Important durable fact', sequence=1, created_at=now)
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    first = runner.scan_sessions(lookback_seconds=3600, limit=1)
    assert len(first) == 1
    first_id = first[0]['session_id']
    conn = temp_db.get_connection()
    conn.execute("UPDATE curation_distillation_state SET status='SUCCESS', source_digest=? WHERE session_id=?", (runner._source_digest(first_id), first_id))
    conn.execute('UPDATE raw_sessions SET started_at=?', (now - 10000,))
    conn.execute('UPDATE raw_messages SET created_at=?', (now - 10000,))
    conn.execute('UPDATE ingest_log SET processed_at=?', (now - 10000,))
    conn.commit()
    restarted = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    remaining = restarted.scan_sessions(lookback_seconds=3600, limit=1)
    assert len(remaining) == 1
    assert remaining[0]['session_id'] != first_id


def test_scan_recovers_sessions_created_during_long_outage(temp_db, monkeypatch):
    from types import SimpleNamespace
    import src.cognitive.distill_runner as runner_module
    import src.cognitive.scheduler as scheduler_module
    clock = {'now': 1800000000}
    fake_time = SimpleNamespace(time=lambda: clock['now'], strftime=time.strftime)
    monkeypatch.setattr(runner_module, 'time', fake_time)
    monkeypatch.setattr(scheduler_module, 'time', fake_time)
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    assert runner.scan_sessions(lookback_seconds=86400) == []
    clock['now'] += 3600
    temp_db.record_raw_session(session_id='outage', agent_id='agent', project_id='project', started_at=clock['now'])
    temp_db.record_raw_message(message_id='outage-msg', session_id='outage', role='user', content='Important outage fact', sequence=1, created_at=clock['now'])
    clock['now'] += 3 * 86400
    assert [item['session_id'] for item in runner.scan_sessions(lookback_seconds=86400)] == ['outage']


def test_newly_ingested_backdated_messages_are_discovered(temp_db):
    now = int(time.time())
    temp_db.ingest_messages('backdated', 'agent', 'project', [{'message_id':'late', 'role':'user', 'content':'A late arriving fact', 'timestamp':now - 30 * 86400}])
    temp_db.get_connection().execute('UPDATE sessions SET started_at=? WHERE session_id=?', (now - 30 * 86400, 'backdated'))
    temp_db.get_connection().commit()
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    assert any(item['session_id'] == 'backdated' for item in runner.scan_sessions(lookback_seconds=86400))


@pytest.mark.asyncio
async def test_empty_session_is_not_repeated_until_source_changes(temp_db):
    now = int(time.time())
    temp_db.record_raw_session(session_id='empty-session', agent_id='agent', project_id='project', started_at=now)
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    result = await runner.run_batch(lookback_seconds=3600, limit=1)
    assert result['sessions_skipped'] == 1
    assert runner.scan_sessions(lookback_seconds=3600) == []
    temp_db.record_raw_message(message_id='later', session_id='empty-session', role='user', content='New useful fact', sequence=1, created_at=now)
    assert [item['session_id'] for item in runner.scan_sessions(lookback_seconds=3600)] == ['empty-session']


def test_dry_run_previews_existing_backlog_without_advancing_watermark(temp_db):
    now = int(time.time())
    temp_db.record_raw_session(session_id='queued', agent_id='agent', project_id='project', started_at=now)
    temp_db.record_raw_message(message_id='queued-msg', session_id='queued', role='user', content='Useful backlog fact', sequence=1, created_at=now)
    real = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    assert real.scan_sessions(lookback_seconds=3600)[0]['session_id'] == 'queued'
    conn = temp_db.get_connection()
    before = conn.execute('SELECT scanned_until FROM curation_discovery_watermark WHERE id=1').fetchone()[0]
    conn.execute('UPDATE raw_sessions SET started_at=?', (now - 10000,))
    conn.execute('UPDATE raw_messages SET created_at=?', (now - 10000,))
    conn.execute('UPDATE ingest_log SET processed_at=?', (now - 10000,))
    conn.commit()
    dry = DistillationRunner(session_store=temp_db, dry_run=True)
    assert [item['session_id'] for item in dry.scan_sessions(lookback_seconds=3600)] == ['queued']
    assert conn.execute('SELECT scanned_until FROM curation_discovery_watermark WHERE id=1').fetchone()[0] == before
    assert conn.execute("SELECT status FROM curation_distillation_state WHERE session_id='queued'").fetchone()[0] == 'PENDING'


def test_new_raw_message_with_old_source_timestamp_is_discovered(temp_db):
    now = int(time.time())
    temp_db.record_raw_session(session_id='raw-old', agent_id='agent', project_id='project', started_at=now - 30 * 86400)
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    assert runner.scan_sessions(lookback_seconds=86400) == []
    temp_db.record_raw_message(message_id='late-raw', session_id='raw-old', role='user', content='Late arriving raw fact', sequence=1, created_at=now - 30 * 86400)
    assert [item['session_id'] for item in runner.scan_sessions(lookback_seconds=86400)] == ['raw-old']


@pytest.mark.asyncio
async def test_two_runners_do_not_distill_same_session_concurrently(temp_db):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []
    async def handler(_system, _messages):
        calls.append(1)
        entered.set()
        await release.wait()
        return {'candidates': [], 'message_disposition': []}
    temp_db.record_raw_session(session_id='concurrent-distill', agent_id='agent', project_id='project')
    temp_db.record_raw_message(message_id='concurrent-msg', session_id='concurrent-distill', role='user', content='Important source message', sequence=1)
    first = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(handler=handler), allow_external=True)
    second_store = SessionStore(db_path=str(temp_db.db_path))
    second = DistillationRunner(session_store=second_store, model_worker=MockModelWorker(handler=handler), allow_external=True)
    task = asyncio.create_task(first.run_pipeline_for_session('concurrent-distill'))
    await asyncio.wait_for(entered.wait(), 5)
    try:
        result = await asyncio.wait_for(second.run_pipeline_for_session('concurrent-distill'), 2)
        assert result['status'] == 'ALREADY_RUNNING'
        assert len(calls) == 1
    finally:
        release.set()
        await task
        second_store.close()


def test_old_interrupted_session_returns_to_backlog(temp_db):
    now = int(time.time())
    temp_db.record_raw_session(session_id='interrupted', agent_id='agent', project_id='project', started_at=now - 100000)
    temp_db.record_raw_message(message_id='interrupted-msg', session_id='interrupted', role='user', content='Important source message', sequence=1, created_at=now - 100000)
    conn = temp_db.get_connection()
    conn.execute('UPDATE ingest_log SET processed_at=?', (now - 100000,))
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    conn.execute("INSERT INTO curation_distillation_state (session_id, batch_id, status, created_at, updated_at) VALUES ('interrupted', 'old', 'RUNNING', ?, ?)", (now - 7200, now - 7200))
    conn.commit()
    assert [item['session_id'] for item in runner.scan_sessions(lookback_seconds=3600)] == ['interrupted']


@pytest.mark.asyncio
async def test_unexpected_pipeline_exception_is_recorded_as_retryable_without_content(temp_db):
    temp_db.record_raw_session(session_id='pipeline-exception', agent_id='agent', project_id='project')
    temp_db.record_raw_message(message_id='source', session_id='pipeline-exception', role='user', content='Private source', sequence=1)
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(), allow_external=True)
    async def fail(**kwargs):
        raise RuntimeError('Private source should not appear')
    runner.scheduler.run_pipeline_for_session = fail
    result = await runner.run_pipeline_for_session('pipeline-exception')
    assert result['status'] == 'PIPELINE_ERROR'
    assert result['error'] == 'RuntimeError'
    state = temp_db.get_connection().execute("SELECT status, last_error FROM curation_distillation_state WHERE session_id='pipeline-exception'").fetchone()
    assert tuple(state) == ('FAILED', 'RuntimeError')


@pytest.mark.asyncio
async def test_lost_lease_does_not_write_proposals_or_overwrite_new_owner(temp_db):
    entered = asyncio.Event()
    release = asyncio.Event()
    async def handler(_system, _messages):
        entered.set()
        await release.wait()
        return {'candidates': [{'operation':'EXTRACT', 'subject':'db', 'predicate':'uses', 'object':'SQLite', 'content':'The database uses SQLite.', 'evidence_source_ids':['lost-m'], 'extracted_spans':[{'message_id':'lost-m','span':'The database uses SQLite.'}]}], 'message_disposition':[{'message_id':'lost-m','disposition':'EXTRACTED'}]}
    temp_db.record_raw_session(session_id='lost-lease', agent_id='agent', project_id='project')
    temp_db.record_raw_message(message_id='lost-m', session_id='lost-lease', role='user', content='The database uses SQLite.', sequence=1)
    runner = DistillationRunner(session_store=temp_db, model_worker=MockModelWorker(handler=handler), allow_external=True)
    task = asyncio.create_task(runner.run_pipeline_for_session('lost-lease'))
    await asyncio.wait_for(entered.wait(), 5)
    conn = temp_db.get_connection()
    conn.execute("UPDATE curation_distillation_state SET lease_token='new-owner' WHERE session_id='lost-lease'")
    conn.commit()
    release.set()
    result = await task
    assert result['status'] == 'LEASE_LOST'
    assert conn.execute('SELECT count(*) FROM curation_candidates').fetchone()[0] == 0
    assert conn.execute("SELECT lease_token FROM curation_distillation_state WHERE session_id='lost-lease'").fetchone()[0] == 'new-owner'

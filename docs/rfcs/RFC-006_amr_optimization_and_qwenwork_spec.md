# AI Memory Runtime (AMR) 修复与优化设计方案 (RFC-006)

> **文档状态**：待 Hermes 终审  
> **编制者**：Antigravity (Coding Agent)  
> **审计方**：Hermes  
> **执行方**：Antigravity (待 Hermes 终审通过后实施)  
> **生效范围**：AMR Core Daemon (`src/main.py`, `src/service/memory_service.py`), Configuration (`config/`), Agent 插件 (`plugins/`)

---

## 1. 背景与现状分析

近期在排查 `amr.service` 生产环境运行状况及代码全量审计时，发现了以下核心缺陷与改进诉求：
1. **日志真实报错（P0）**：生产日志中频繁爆出 `Handler error executing method 'system.health': Unknown business method: system.health`，导致外部探针判定失败。
2. **硬编码信任 Agent 列表，配置不灵活（P0）**：`src/main.py` 内硬编码 `allowed_agents = {"hermes", "openclaw", "opencode", "dsh", "system", ...}`。现需将 Windows 端 AI 助手 `qwenwork` 纳入白名单，且原则上所有被允许的 agent 必须在配置文件中进行管理，禁止写死在 Python 业务逻辑中。
3. **记忆状态检索漏召回隐患（P0）**：`src/service/memory_service.py` 的 `memory_search` 逻辑中，`status` 判定受大小写影响（如 `active` vs `ACTIVE`），导致历史或知识层点位可能因 Qdrant 精确字符串匹配而漏召回。
4. **长文本切片 I/O 重复开销（P1）**：`chunk_text_by_tokens` 未单例缓存分词器，缺失时每次切片均触发 `AutoTokenizer.from_pretrained` 磁盘解析。
5. **OpenCode 插件自动化测试缺失脚本（P1）**：`plugins/opencode-amr/package.json` 缺少 `"test"` 脚本定义，导致 CI 规范检查失败。
6. **Hermes 插件测试环境隔离（P2）**：在缺少 `hermes-agent` 宿主环境的机器上执行单测时无法平滑 fallback mock。

---

## 2. 详细改造方案

### 改造一：配置化管理 `allowed_agents` 并正式接入 `qwenwork`
#### 2.1 配置 Schema 扩展 (`config/settings.py` 与 `config/config.yaml`)
在 `ServerConfig` 中增加 `allowed_agents` 配置项，赋予默认安全集合，并在配置文件中显式列出：

```yaml
# config/config.yaml & config/config.example.yaml
server:
  business_socket: "/run/user/1000/qdrant-bge.sock"
  admin_socket: "/run/user/1000/qdrant-bge-admin.sock"
  socket_mode: 0o600
  max_request_bytes: 4194304  # 4MB
  allowed_agents:
    - "hermes"
    - "openclaw"
    - "opencode"
    - "dsh"
    - "qwenwork"      # 新增 Windows 端 AI 助手
    - "system"
    - "default_agent"
    - "test_runner"
```

在 `config/settings.py` 中：
```python
class ServerConfig(BaseModel):
    business_socket: str = "/run/user/1000/qdrant-bge.sock"
    admin_socket: str = "/run/user/1000/qdrant-bge-admin.sock"
    socket_mode: int = 0o600
    max_request_bytes: int = 4 * 1024 * 1024  # 4MB
    allowed_agents: List[str] = Field(
        default_factory=lambda: [
            "hermes", "openclaw", "opencode", "dsh", "qwenwork",
            "system", "default_agent", "test_runner"
        ]
    )
```

#### 2.2 守护进程身份鉴权解除硬编码 (`src/main.py`)
替换写死集合，改为动态从 `self.config.server.allowed_agents` 读取（进行大小写归一化处理）：
```python
allowed_agents = {a.lower() for a in self.config.server.allowed_agents}
if agent_id not in allowed_agents:
    raise ValueError(f"AGENT_UNAUTHORIZED: Unknown or untrusted agent_id '{agent_id}'")
```

---

### 改造二：补充 `system.health` 与 `system.status` RPC 分发路由 (`src/main.py`)
在 `dispatch_business_rpc` 补充探针响应，允许 Agent 和运维探针快速获取健康状态，无需穿透到 Admin Socket：
```python
elif method in ("system.health", "system_health", "system.ping", "ping"):
    return {
        "status": "healthy",
        "service": "ai-memory-runtime",
        "model_state": self.engine.state.value if hasattr(self.engine, "state") else "ready",
        "qdrant_ok": self.qdrant_manager.is_healthy() if hasattr(self.qdrant_manager, "is_healthy") else True,
        "sqlite_ok": True,
    }
elif method in ("system.status", "system_status"):
    return {
        "status": "running",
        "allowed_agents": list(self.config.server.allowed_agents),
        "engine": await self.engine.get_model_status(),
    }
```

---

### 改造三：`memory.search` 的 `status` 过滤条件规范化与大小写自适应 (`src/service/memory_service.py`)
避免调用方因传入 `"active"`、`"ACTIVE"` 或大写状态导致的 Qdrant 精确匹配漏召回：
```python
# 确定状态过滤列表（自适应大小写兼容）
if status:
    raw_statuses = [status] if isinstance(status, str) else list(status)
    expanded = set()
    for s in raw_statuses:
        expanded.add(s.lower())
        expanded.add(s.upper())
    allowed_statuses = list(expanded)
elif include_history:
    allowed_statuses = ["ACTIVE", "active", "HISTORICAL", "historical", "SUPERSEDED", "superseded"]
else:
    allowed_statuses = ["ACTIVE", "active"]
```

---

### 改造四：Token 切片器 `AutoTokenizer` 单例缓存 (`src/service/memory_service.py`)
在 `MemoryService` 内部维护 `self._cached_tokenizer = None`：
```python
if tokenizer is None:
    if not hasattr(self, "_cached_tokenizer") or self._cached_tokenizer is None:
        try:
            from transformers import AutoTokenizer
            self._cached_tokenizer = AutoTokenizer.from_pretrained(self.engine.model_path)
        except Exception as e:
            logger.warning(f"Could not load tokenizer for chunking: {e}, falling back to approx character chunking")
            self._cached_tokenizer = None
    tokenizer = self._cached_tokenizer
```

---

### 改造五：`opencode-amr` 补充 `npm test` 脚本 (`plugins/opencode-amr/package.json`)
```json
{
  "name": "opencode-amr",
  "version": "1.0.0",
  "description": "Native AI Memory Runtime (AMR) zero-friction plugin for OpenCode",
  "main": "index.js",
  "type": "module",
  "scripts": {
    "test": "node --test tests/test_*.js"
  },
  "dependencies": {}
}
```

---

### 改造六：Hermes 插件测试环境优雅隔离 (`plugins/hermes-amr/__init__.py`)
对 `agent.memory_provider` 引入做优雅回退处理，当非 Hermes 宿主运行单元测试时自动加载 Mock 类型，确保任何环境与 CI `pytest` 100% 成功。

---

## 3. 验收标准与测试保障
1. **现有测试全量通过**：`python3 -m pytest tests/` 208 个用例持续 100% PASS。
2. **新增用例保障**：
   - 测试 `qwenwork` 发送 RPC 请求正常响应，非白名单 agent 抛出 `AGENT_UNAUTHORIZED`。
   - 测试 `system.health` RPC 方法返回 `{"status": "healthy", ...}`。
   - 测试 `status="active"` 与 `status="ACTIVE"` 均能准确召回目标记忆。
3. **插件测试验证**：
   - `cd plugins/openclaw-amr && npm test` (119 tests PASS)
   - `cd plugins/dsh-amr && npm test` (246 tests PASS)
   - `cd plugins/opencode-amr && npm test` (5 tests PASS)
   - Hermes 插件单元测试 PASS。
4. **进程无缝升级**：更新代码后重启 `amr.service`，验证日志中不再产生 `Unknown business method: system.health` 报错。

---

请 Hermes 审计官审查该方案。终审通过后即可启动编码实施。

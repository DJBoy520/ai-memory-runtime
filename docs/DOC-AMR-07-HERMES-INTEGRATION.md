# AI Memory Runtime (AMR) - Hermes 统一记忆插件集成方案 (DOC-AMR-07)

**文档标识**：`DOC-AMR-07-HERMES-INTEGRATION`  
**版本号**：`v1.0.0`  
**编写方**：Hermes（方案设计）  
**审计方**：OpenClaw（架构与代码审计）  
**执行方**：opencode（代码交付）  
**定案日期**：2026-09-26  

---

## 1. 背景与核心目标

### 1.1 现状痛点
1. **MCP 模式的被动缺陷**：纯粹作为 MCP 工具挂载时，智能体只能“被动按需调用”；如果模型在交互中未主动调用 `memory_search` 或 `memory_record`，长效记忆与重要决策容易产生断层。
2. **Mem0 库的冗余与异构**：第三方 Mem0 库体系庞大、依赖繁琐，要求独立的 LLM/Embedder/VectorStore 三元驱动，曾导致本地需要维护粗暴常驻 GPU 的 HTTP 8100 端口（`bge-m3.service`）。
3. **架构大一统诉求**：本地已有成熟自启、具备 3600 秒空闲回收显存与 SQLite WAL 审计的 AI Memory Runtime（AMR，`amr.service`）。必须将 Hermes 的记忆机制直接纳管至 AMR 架构中，实现全自动无感感知。

### 1.2 目标定位
在 Hermes 插件体系内实现原生 **`amr` 记忆插件**（`plugins/memory/amr/`），继承 Hermes 的 `MemoryProvider` 抽象基类：
- **无感预取 (Prefetch)**：老板输入后、生成回答前，后台毫秒级查询 AMR UDS（`memory_search`），将关联历史决策无感注入 Prompt；
- **无感归档 (Sync Turn)**：每轮对话结束后，在非阻塞后台线程将对话流水自动持久化（`session.ingest`），并按需提炼沉淀事实（`memory_record`）；
- **零额外显存常驻**：完全复用已自启的 `amr.service`，共享 3600 秒自动释放；
- **全 Agent 数据贯通**：Hermes 自动提取的记忆与 OpenClaw、OpenCode 写入的记忆同库、同集合（`ai_memory`）、同 Schema。

---

## 2. 系统详细设计

### 2.1 模块目录结构
```text
~/.hermes/hermes-agent/plugins/memory/amr/
├── __init__.py           # 插件注册与生命周期管理 (AmrMemoryProvider)
├── plugin.yaml           # Hermes 插件元数据描述
├── _client.py            # 高性能轻量 UDS 客户端 (纯标准库，<5ms，零第三方依赖)
└── _extractor.py         # 对话轻量事实提炼引擎 (可选本地/网关 LLM 提炼)
```

### 2.2 核心生命周期流转 (AmrMemoryProvider)
```text
[用户输入] 
   │
   ▼
1. queue_prefetch(query) ──► 线程池异步通过 UDS 调用 memory.search(query, limit=3)
   │
   ▼
2. prefetch() ─────────────► 提取检索结果，格式化为 Markdown 事实上下文注入 Prompt
   │
   ▼
[模型生成回复]
   │
   ▼
3. sync_turn() ────────────► 触发后台 spawn_context_thread:
                                ├─► 1. session.ingest (原始消息无损落盘 sessions.db)
                                └─► 2. 事实提炼 -> memory.record (自动入库 Qdrant)
```

### 2.3 零依赖轻量 UDS 客户端 (`_client.py`)
- **通信目标**：`/run/user/1000/qdrant-bge.sock`
- **协议兼容**：AMR 标准 4 字节 Big-Endian `uint32` 长度前缀 + JSON-RPC 2.0；
- **连接复用与容错**：短连接/长连接心跳容错，服务未就绪时 Fail-open（静默降级，不阻断正常聊天）。

### 2.4 安全与配置规范
- **配置声明**：在 `~/.hermes/config.yaml` 中配置：
  ```yaml
  memory:
    provider: 'amr'
    memory_enabled: true
    user_profile_enabled: true
  ```
- **配置项落盘**：`~/.hermes/amr.json`，配置项包括：
  ```json
  {
    "socket_path": "/run/user/1000/qdrant-bge.sock",
    "project_id": "crypto-infrastructure",
    "scope": "global",
    "auto_extract": true,
    "top_k": 3
  }
  ```

---

## 3. 验收标准与测试矩阵
1. **启动与探测验证**：`is_available()` 严格判定 UDS 存在且守护进程健康，探测耗时 ≤ 10ms；
2. **预取断言**：模拟提问“国密标准”，断言 Prompt 成功自动附带检索出的历史事实片段；
3. **提炼与持久化断言**：完成一轮交互后，断言 `sessions.db` 成功插入该条消息记录，Qdrant 集合点数保持正常；
4. **资源与内存断言**：Hermes 进程内存无任何额外 GPU/PyTorch 膨胀，AMR 状态机正常维系 3600 秒超时逻辑。

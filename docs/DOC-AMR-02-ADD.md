# AI Memory Runtime - 系统架构与开发设计方案 (ADD)

**文档标识**：`DOC-AMR-02-ADD`  
**版本号**：`v1.0.0`  
**定案日期**：2026-09-26  
**编写方**：OpenClaw（架构团队）  
**审计方**：Hermes（架构专家）  
**执行方**：opencode（代码交付）

---

## 1. 总体架构拓扑 (Topological Architecture)

系统全面收敛为纯本机 IPC（UDS）基础设施，彻底移除任何 TCP/HTTP 网络端口：

```text
OpenClaw / Hermes / opencode / Cursor (各 AI Agent)
        │
        │ stdio (标准 JSON-RPC 2.0，极轻量 ~20MB，零 PyTorch/零显存)
        ▼
mcp-bridge (统一 MCP 桥接客户端)
        │
        │ 业务 UDS: /run/user/1000/qdrant-bge.sock (权限 0600)
        ▼
qdrant-bge daemon (系统常驻守护进程，Systemd 用户服务托管)
  ├── 业务核心层 (Memory Manager)
  │     ├── memory_search / memory_record / memory_get / memory_update_status
  │     └── memory_ingest_session (仅落盘原始流水，无脆弱规则 NLP)
  ├── 会话存储层 (Session Store - SQLite WAL)
  │     └── 表结构：sessions / messages / ingest_log (content_hash 识别 revision)
  ├── 模型引擎层 (BGE-M3 Engine - Tesla P4)
  │     ├── 6 态生命周期状态机 (300 秒 Idle 自动卸载，按需加载)
  │     ├── 独立线程池推理队列 (MAX_BATCH=16, 线程隔离防卡死主循环)
  │     └── 自动分块引擎 (超 8192 Token 自动切分 parent/chunk)
  └── Qdrant 适配层 (Qdrant Manager - 官方客户端长连接复用)
        └── collections: ai_memory, crypto_standards, project_docs
```

管理控制面（完全解耦与独立）：
```text
admin-cli (管理员运维 CLI)
        │
        │ 管理 UDS: /run/user/1000/qdrant-bge-admin.sock (权限 0600)
        ▼
qdrant-bge daemon
  ├── 内部健康指标监控 (Qdrant连通、显存 allocated/reserved、P50耗时、队列深度)
  ├── 模型强制调度 (load / unload)
  └── 集合状态与快照备份
```

---

## 2. 模块划分与核心设计

### 2.1 模型引擎层 (`engine.py`)
- **6 态状态机模型**：
  ```text
  [UNLOADED] ──(新请求到达)──> [LOADING] ──(加载完毕)──> [READY]
      ▲                            │                     │
      │                     (加载失败)                   (队列为空)
      │                            ▼                     ▼
  [UNLOADING] <──(超时300s)─── [IDLE] <──────────────────┘
      │                            │
      └───(发生异常)──> [ERROR] <───┘
  ```
- **排队与防爆机制**：
  - 单推理 Worker（`asyncio.Queue` + 独立 `ThreadPoolExecutor`）。
  - `MAX_BATCH = 16`：物理切批推断，超长批次分批计算拼接。
  - `MAX_QUEUE_SIZE = 64`：超限直接返回 503 背压拒绝。

### 2.2 存储层 (`session_store.py` & `qdrant.py`)
- **SQLite WAL 原始流水库**：
  - 路径可配置：`data/sessions.db`。
  - 启动强制执行：`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`。
  - 三表联合：`sessions`, `messages`, `ingest_log`。
- **Qdrant 长连接复用**：
  - 依赖官方 SDK 底层连接池，单例复用 `QdrantClient`。
  - 检索强规则：检索 Filter 底层强行注入 `status == "active"`。

---

## 3. 目录工程组织规范

工程统一落地于 `/home/dj/WorkSpaces/openclaw/qdrant-bge-memory/`：

```text
qdrant-bge-memory/
├── README.md
├── requirements.txt
├── config/
│   ├── config.example.yaml
│   └── config.yaml           # 权限 0600，目录 0700，不进 git
├── src/
│   ├── core/
│   │   ├── engine.py         # 状态机、BGE-M3推理、线程池排队
│   │   ├── qdrant.py         # Qdrant连接复用、集合操作
│   │   └── session_store.py  # SQLite三表落盘与幂等
│   ├── service/
│   │   ├── memory_service.py # 记忆检索、记录、状态流转、Chunking
│   │   └── metrics.py        # 内部监控指标收集
│   ├── interfaces/
│   │   ├── ipc/              # UDS 通信服务层
│   │   │   ├── server.py     # 双 UDS (业务+管理) 事件监听
│   │   │   └── protocol.py   # UDS JSON-RPC 编解码与4MB帧限制
│   │   └── mcp/              # Agent Stdio MCP 桥接器
│   │       ├── bridge.py     # 零 torch 极轻量桥接
│   │       └── tools.py      # memory_* 5个工具契约
│   └── admin/
│       └── cli.py            # 管理员 admin-cli 入口
├── systemd/
│   └── qdrant-bge.service
└── tests/
```

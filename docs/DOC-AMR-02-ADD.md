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
┌────────────────────────────────────────────────────────────────────────┐
│                      智能体生态层 (AI Fleet Layer)                      │
│                                                                        │
│   ┌─────────────────────┐   ┌───────────────────┐   ┌──────────────┐   │
│   │    Hermes Agent     │   │   OpenClaw Host   │   │   OpenCode   │   │
│   │ plugins/memory/amr  │   │ plugins/openclaw- │   │  MCP Client  │   │
│   │ (AmrMemoryProvider) │   │        amr        │   │ (Code Refact)│   │
│   └──────────┬──────────┘   └─────────┬─────────┘   └───────┬──────┘   │
└──────────────┼────────────────────────┼─────────────────────┼──────────┘
               │ 预取/入库              │ 预取/入库           │ 按需 MCP
               │                        │                     │
               ▼                        ▼                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   统一客户端适配层 (Ultra-Thin Adapters)                 │
│  - 纯原生 Node.js / Python 标准库，零 PyTorch/零 ONNX，零显存额外开销    │
│  - 预取检索：Hard Deadline ≤ 80ms~100ms，严格 Fail-Open，绝不拖慢会话   │
│  - 会话入库：Fire-and-forget 异步无感投递，主线程等待 0ms                │
│  - 协议规范：4 字节 Big-Endian uint32 前缀 + UTF-8 JSON-RPC 2.0 (单帧≤4MB)│
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ 业务 UDS: /run/user/1000/qdrant-bge.sock (0600)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│             qdrant-bge daemon (AI Memory Runtime 守护进程)             │
│                                                                        │
│  ├── 业务核心层 (Memory Manager)                                       │
│  │     ├── memory.search / memory.record / memory.get                  │
│  │     ├── memory.update_status (4态流转: active/superseded/archived/deleted) │
│  │     └── session.ingest (原始流水落盘，时序 sequence 校验，无模型开销) │
│  ├── 会话存储层 (Session Store - SQLite WAL)                           │
│  │     └── 表结构：sessions / messages / ingest_log (content_hash 防重) │
│  ├── 模型引擎层 (BGE-M3 Engine - Tesla P4 GPU Daemon)                  │
│  │     ├── 6 态生命周期状态机 (300 秒 Idle 自动卸载回收显存，按需自愈唤醒) │
│  │     ├── 独立线程池推理队列 (MAX_BATCH=16, 线程隔离防卡死主循环)     │
│  │     └── 自动分块引擎 (超 8192 Token 自动切分 parent/chunk)          │
│  └── Qdrant 适配层 (Qdrant Manager - 官方客户端长连接复用，API Key 鉴权)│
│        └── collections: ai_memory, crypto_standards, project_docs      │
└───────────────────────────────────▲────────────────────────────────────┘
                                    │
                                    │ 管理 UDS: /run/user/1000/qdrant-bge-admin.sock (0600)
                                    │
┌───────────────────────────────────┴────────────────────────────────────┐
│                    管理控制面 (Admin Control Plane)                     │
│  - admin-cli (独立运维工具，不注册进普通 Agent MCP)                     │
│  - 内部健康指标监控 (Qdrant 连通性、显存 allocated/reserved、P50 耗时)  │
│  - 模型生命周期强制调度 (load / unload)                                │
└────────────────────────────────────────────────────────────────────────┘
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

工程统一落地于 `/home/dj/WorkSpaces/ai-memory-runtime/`：

```text
ai-memory-runtime/
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
│   └── amr.service
└── tests/
```

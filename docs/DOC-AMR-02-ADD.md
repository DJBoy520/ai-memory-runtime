# AI Memory Runtime - 系统架构设计方案 (ADD)

**文档标识**：`DOC-AMR-02-ADD`  
**版本号**：`v2.2.0`  
**核心结构**：本地 IPC (UDS) + 本地 SQLite 存储 + 异步向量同步 (Outbox)

---

## 1. 系统架构示意

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        智能体客户端 (Agent Layer)                      │
│      Hermes Agent        OpenClaw           OpenCode           DSH     │
└───────────┬──────────────────┬──────────────────┬───────────────┬──────┘
            │ (UDS 直连)       │ (Stdio MCP 桥接) │ (Stdio MCP)   │
            ▼                  ▼                  ▼               ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        AI Memory Runtime 服务进程                      │
│                                                                        │
│   ├── IPC / MCP 协议层 (UDS Socket: $XDG_RUNTIME_DIR/qdrant-bge.sock)   │
│   │   • 4 字节 Big-Endian 长度前缀 + JSON-RPC 2.0 (单包 ≤ 4MB)         │
│   ├── 业务逻辑层                                                       │
│   │   • memory_service / cognitive_engine / retrieval_core             │
│   ├── 本地持久化层 (SQLite WAL)                                        │
│   │   • sessions / messages / memories / qdrant_sync_queue             │
│   └── 异步同步层 (Outbox Worker)                                       │
│       • 消费 qdrant_sync_queue 队列，后台同步至 Qdrant 实例            │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 关键设计说明

### 2.1 本地存储与发件箱
- **本地数据库**：默认路径 `data/sessions.db`，开启 WAL 模式。
- **发件箱机制 (Outbox)**：记忆卡片写入本地 SQLite 时，同步向 `qdrant_sync_queue` 插入待同步任务；后台 Worker 异步计算向量并投递至 Qdrant，降低外部网络或向量库短暂不可用对客户端调用的影响。

### 2.2 接口通信规范
- **通信方式**：Unix Domain Socket（业务 Socket 默认权限 `0600`）。
- **客户端接入**：支持直接通过 UDS 二进制帧通信，或通过 `src/interfaces/mcp/bridge.py` 转化为标准 Model Context Protocol (MCP) 工具。

---

## 3. 代码目录结构

```text
ai-memory-runtime/
├── README.md                 # 项目说明
├── requirements.txt          # Python 依赖清单
├── config/
│   ├── config.example.yaml   # 开源配置模板
│   └── config.yaml           # 本地私有配置 (git ignore)
├── src/
│   ├── core/                 # 核心底层：BGE-M3 推理、Qdrant 客户端、SQLite 存储
│   ├── service/              # 业务服务：记忆管理、提纯、检索、Outbox Worker
│   ├── interfaces/           # 接口层：UDS Socket 服务端与 MCP Bridge
│   └── admin/                # 运维工具：CLI 工具
├── scripts/                  # 辅助脚本：对账与测试工具
├── systemd/                  # 守护进程与对账服务的 systemd 模板
└── docs/                     # 文档中心
```

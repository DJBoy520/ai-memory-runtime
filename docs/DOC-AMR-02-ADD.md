# AI Memory Runtime - 系统架构设计方案 (ADD)

**文档标识**：`DOC-AMR-02-ADD`  
**基线版本**：`v2.2.0`（含 `v5.0.0` 变更摘要）  
**核心结构**：本地 IPC (UDS) + 本地 SQLite SSOT + 事务发件箱异步投影 (Outbox)

---

## 0. v5 变更摘要与架构演进 (v5 Architecture Evolution)

在 AMR v5 中，底层数据流与并发控制完成了重要工业级升级（注：下文 §1~§2 的历史图示与 `qdrant_sync_queue` 命名保留作为早期背景参考）：

1. **统一事务发件箱 (`memory_projection_outbox`)**：
   - 彻底废弃旧版 `qdrant_sync_queue`，升级为生产级 `memory_projection_outbox` 表；
   - 写入链路采用 **Commit-then-Project** 机制：SQLite SSOT 提交完成立即返回 `projection_status="pending"`，写链路零外部网络 I/O；
   - 引入死信队列（Dead Letter Queue）：超过最大重试次数自动标记为 `dead_letter`，不阻塞后续任务，支持通过 Admin CLI 重放。
2. **单执行者独占锁 (`fcntl.flock`)**：
   - `OutboxWorker` 在进程启动时以非阻塞模式抢占 `$XDG_RUNTIME_DIR/amr-outbox.lock`；
   - 同一时刻同机仅允许唯一 Worker 消费发件箱投递 Qdrant，从根本上杜绝并发乱序与版本覆盖；
   - 冲突进程以退出码 `3` 受控退出，配合 systemd `RestartSec=3s` 实现退避恢复。
3. **读写双重一致性模型**：
   - **强一致 (Strong Consistency)**：`memory_get` 直查 SQLite SSOT，事务提交即立即可见最新状态与版本；
   - **最终一致 (Eventual Consistency)**：`memory_search` 检索 Qdrant 向量索引，依赖 Outbox 异步投影，通常毫秒级就绪。
4. **生命周期全大写 6 态**：
   - 统一收敛为 `ACTIVE`, `PENDING_VERIFY`, `CONFLICT`, `HISTORICAL`, `TEMPORARY`, `DELETED`，自动兼容旧版小写状态。

---

## 1. 系统架构示意（注：旧版 v2.2 背景示意图）

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
│   │   • sessions / messages / memories / memory_projection_outbox      │
│   │     (注：历史早期版本曾命名为 qdrant_sync_queue)                   │
│   └── 异步同步层 (Outbox Worker - 单写者 flock 独占)                   │
│       • 消费 memory_projection_outbox 队列，后台同步至 Qdrant 实例     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 关键设计说明

### 2.1 本地存储与发件箱
- **本地数据库**：默认路径 `data/sessions.db`，开启 WAL 模式。
- **发件箱机制 (Outbox)**：记忆卡片写入本地 SQLite 时，在同一数据库事务中向 `memory_projection_outbox` 插入待同步任务；后台单执行者 Worker 异步计算向量并投递至 Qdrant，降低外部网络或向量库短暂不可用对客户端调用的影响。

### 2.2 接口通信规范
- **通信方式**：Unix Domain Socket（业务 Socket 默认权限 `0600`）。
- **客户端接入**：支持直接通过 UDS 二进制帧通信，或通过 `src/interfaces/mcp/bridge.py` 转化为标准 Model Context Protocol (MCP) 工具。

---

## 3. 代码目录结构

```text
ai-memory-runtime/
├── README.md                 # 项目说明与快速上手
├── requirements.txt          # Python 核心依赖清单
├── config/
│   ├── config.example.yaml   # 开源配置模板（与 settings 默认值严格对齐）
│   └── config.yaml           # 本地私有配置 (git ignore)
├── src/
│   ├── core/                 # 核心底层：BGE-M3 推理引擎、Qdrant 客户端、SQLite 会话存储
│   ├── service/              # 业务服务：记忆生命周期、检索路由、Outbox Worker 单写者
│   ├── interfaces/           # 接口层：UDS 二进制帧 Socket 服务端与 MCP Bridge
│   └── admin/                # 运维工具：Admin CLI
├── scripts/                  # 辅助工具：amr_doctor 系统体检、对账与测试工具
└── tests/                    # 完整单元与集成测试套件
```

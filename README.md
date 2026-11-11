# AI Memory Runtime (AMR)

面向多智能体（Claude Code、OpenClaw、OpenCode、Hermes Agent、DSH 等）的统一本地语义记忆与会话存储运行时。

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Storage](https://img.shields.io/badge/storage-SQLite%20WAL%20(SSOT)-lightgrey)](docs/DOC-AMR-02-ADD.md)
[![Vector](https://img.shields.io/badge/vector-Qdrant-red)](https://qdrant.tech/)
[![Protocol](https://img.shields.io/badge/protocol-UDS%20%7C%20MCP-orange)](docs/DOC-AMR-04-API.md)

---

## 核心特性与架构

```text
  [ Claude Code ]      [ OpenClaw ]      [ OpenCode / DSH ]      [ Other Agents ]
         │                  │                    │                      │
         └─────────┬────────┴────────────┬───────┘                      │
                   │ (Stdio MCP 桥接)    │ (本地 UDS 二进制帧直连)        │
                   ▼                     ▼                              ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           AI Memory Runtime (AMR v5)                          │
│                                                                               │
│  • 接口层: 业务 UDS ($XDG_RUNTIME_DIR/qdrant-bge.sock) [0600]                  │
│           管理 UDS ($XDG_RUNTIME_DIR/qdrant-bge-admin.sock) [0600]            │
│  • SSOT 存储: SQLite WAL (memories + memory_projection_outbox + revisions)   │
│  • 异步单写者: OutboxWorker (OS flock 独占) ──(BGE-M3 向量化)──► Qdrant 索引  │
└───────────────────────────────────────────────────────────────────────────────┘
```

- **Commit-then-Project 零延迟写**：写入操作仅原子落盘 SQLite SSOT 并在同一事务生成发件箱任务，立即返回 `projection_status="pending"`，写链路耗时 `<5ms`，零外部 I/O 阻塞。
- **读-写-搜双重一致性**：
  - `memory_get`：直读本地 SQLite SSOT，**强一致 (Read-your-writes)**，事务提交立即可见。
  - `memory_search`：检索 Qdrant 向量索引，OutboxWorker 异步单写者投递，**最终一致 (Eventual Consistency)**。
- **单执行者独占锁 (flock)**：同机唯一 Worker 抢占 `$XDG_RUNTIME_DIR/amr-outbox.lock`，彻底避免并发写 Qdrant 导致版本乱序；冲突进程以退出码 `3` 受控退出。
- **乐观锁 CAS 防并发覆盖**：正文更新强制要求 `expected_version` 与 `change_reason`，版本冲突返回结构化错误 `-32002 VERSION_CONFLICT`。
- **7 天幂等重放保护**：支持客户端传入 `idempotency_key`；相同载荷安全重放返回 `idempotent_replay: true`，冲突载荷返回 `-32006 IDEMPOTENCY_CONFLICT`。
- **全大写 6 态生命周期**：标准 6 态（`ACTIVE`, `PENDING_VERIFY`, `CONFLICT`, `HISTORICAL`, `TEMPORARY`, `DELETED`），自动兼容旧版小写。

---

## 快速开始 (Quick Start)

### 1. 准备虚拟环境与依赖

强烈建议在 Python 虚拟环境（`.venv`）中安装与运行：

```bash
git clone https://github.com/DJBoy520/ai-memory-runtime.git
cd ai-memory-runtime

# 创建并激活虚拟环境
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

### 2. 初始化配置文件

复制配置示例并设置严格的 0600 权限（AMR 启动时会校验权限）：

```bash
cp config/config.example.yaml config/config.yaml
chmod 600 config/config.yaml
```

核心配置项说明（`config/config.yaml`）：
- `server.business_socket`：业务 Unix Socket 路径（默认 `/run/user/<uid>/qdrant-bge.sock`）；
- `server.admin_socket`：管理 Unix Socket 路径（默认 `/run/user/<uid>/qdrant-bge-admin.sock`）；
- `server.allowed_agents`：允许连接的客户端 Agent 标识白名单（大小写不敏感）；
- `model.preload_on_start`：生产建议设为 `true`（消除首个请求冷启动延迟）；
- `model.idle_timeout_seconds`：生产建议设为 `0`（禁用空闲卸载，模型常驻显存）；
- `model.watchdog_exit_enabled`：生产建议设为 `true`（仅在 systemd 托管环境下开启，卡死受控退出自动重启）；
- `storage.sqlite_path`：本地数据库路径（默认 `data/sessions.db`）。

### 3. 启动服务

提供两种启动方式：

#### 方式 A：前台启动（开发调试）

```bash
# 使用虚拟环境 Python 启动（带 PYTHONPATH 兼容）
PYTHONPATH=. .venv/bin/python src/main.py
```

#### 方式 B：作为 Systemd 用户服务运行（生产部署推荐）

```bash
mkdir -p ~/.config/systemd/user/
cp systemd/amr.service ~/.config/systemd/user/

# 若克隆路径不是默认的 ~/WorkSpaces/ai-memory-runtime，请编辑对应路径：
# nano ~/.config/systemd/user/amr.service

systemctl --user daemon-reload
systemctl --user enable --now amr.service
systemctl --user status amr.service
```

### 4. 服务健康体检 (AMR Doctor)

启动后运行官方体检脚本，一键校验 Business IPC、Admin IPC、SQLite quick_check、Outbox 死信及 Qdrant：

```bash
# 控制台可读输出 (Exit Code: 0=HEALTHY, 1=DEGRADED, 2=UNHEALTHY)
.venv/bin/python scripts/amr_doctor.py

# 输出纯 JSON 诊断结构 (可集成至监控或 CI)
.venv/bin/python scripts/amr_doctor.py --json
```

---

## 客户端接入方式

### 方式一：MCP Stdio 桥接接入（推荐）

AMR 提供开箱即用的跨环境桥接脚本 `scripts/mcp-bridge.sh`（自动推导仓库根目录与虚拟环境）。

在你的 Agent 客户端（如 Claude Desktop、Claude Code、OpenClaw）配置文件中配置：

```json
{
  "mcpServers": {
    "ai-memory": {
      "command": "/bin/bash",
      "args": ["/绝对路径/ai-memory-runtime/scripts/mcp-bridge.sh"],
      "env": {
        "AMR_SOURCE_AGENT": "openclaw"
      }
    }
  }
}
```

若直接使用 Python：
```json
{
  "mcpServers": {
    "ai-memory": {
      "command": "/绝对路径/ai-memory-runtime/.venv/bin/python",
      "args": ["/绝对路径/ai-memory-runtime/src/interfaces/mcp/bridge.py"],
      "env": {
        "PYTHONPATH": "/绝对路径/ai-memory-runtime",
        "AMR_SOURCE_AGENT": "openclaw"
      }
    }
  }
}
```
*安全注意：客户端标识由环境变量 `AMR_SOURCE_AGENT` 决定，且必须在 `config.yaml` 的 `allowed_agents` 白名单中；不可在请求载荷中伪造未授权身份。*

#### 主要 MCP 工具：
1. `memory_search`：语义检索（弱一致，默认仅召回 `ACTIVE` 现役记忆，支持命名空间与历史查询）；
2. `memory_create`：创建记忆（强一致落盘返回 `pending`，支持 `idempotency_key` 与溯源审计）；
3. `memory_get`：按 ID 获取详情（强一致，直查 SQLite SSOT）；
4. `memory_update`：CAS 乐观锁更新（修改内容必须传 `expected_version` 与 `change_reason`）；
5. `memory_history`：查询修订历史时间线；
6. `memory_delete`：软删除（SQLite 标记 `DELETED`，Qdrant 异步物理删点）；
7. `memory_ingest_session`：批量归档会话原始消息流水；
8. `memory_record`：旧版兼容写入接口；
9. `memory_update_status`：旧版兼容状态变更接口。

会话接续与治理工具：`session_checkpoint` 写入项目检查点，`session_get_handoff` 按项目读取交接；`memory_candidates` 列出有证据的待审核候选，`memory_approve_candidate` 审核通过后创建正式记忆并异步索引。`memory_spool_status` / `memory_spool_retry` 用于查看和重试本机待发送队列。检查点和候选按项目过滤；项目标识是数据作用域，不构成不同本机进程之间的安全隔离。

使用 `scripts/mcp-bridge.sh` 接入时，`memory_ingest_session` 与 `session_checkpoint` 会先写本机持久队列。AMR 不可用时返回 `status="queued"`，表示本机已排队、尚未确认服务端落盘；服务恢复后，桥运行期间定期重试，重新启动桥时也会重试。队列默认位于 `$XDG_DATA_HOME/amr/mcp-spool`（未设置时为 `~/.local/share/amr/mcp-spool`），包含会话内容，文件权限为 0600。直接运行 Python 桥时需设置 `AMR_SPOOL_DIR` 才启用队列。

`scripts/amr_distill_runner.py` 可扫描新增或变更的会话并产生待审核候选。默认不调用外部模型；配置 OpenAI 兼容端点后可使用 `systemd/amr-distillation.timer` 定时运行。候选不会自动变为正式记忆；需要调用 `memory_candidates` 审阅来源，再调用 `memory_approve_candidate`。审批成功只代表 SQLite 已提交和索引任务已排队，语义搜索在后台投影完成后可见。

`memory_ingest_session` 和 `session_checkpoint` 需要客户端主动调用；仅配置 MCP 不会自动触发会话开始或结束事件。每条消息应提供稳定的 `message_id`，重试时保持相同 ID；一个 `session_id` 绑定最初的 Agent 和项目，其他 Agent 或项目须使用不同的会话 ID。归档会话与生成长期记忆是独立步骤；会话入库不代表已自动提炼为可搜索记忆。

---

## 运维管理 (Admin CLI)

通过专用管理 Socket (`qdrant-bge-admin.sock`) 执行运维操作（零外部网络监听）：

```bash
# 查看服务 6 态状态与 GPU/CPU 显存占用
.venv/bin/python src/admin/cli.py status

# 查看性能计数器（请求总数、延迟分布、发件箱任务数）
.venv/bin/python src/admin/cli.py metrics

# 查看当前向量集合与点位数量
.venv/bin/python src/admin/cli.py collections

# 查看发件箱死信任务 (Dead Letters)
.venv/bin/python src/admin/cli.py dead-letters

# 重放所有死信任务（或指定 --task-ids 1,2,3）
.venv/bin/python src/admin/cli.py replay

# 显式手动加载/卸载模型显存
.venv/bin/python src/admin/cli.py load
.venv/bin/python src/admin/cli.py unload
```

---

## 常见错误排查 (Troubleshooting)

| 症状 / 错误信息 | 根因分析 | 解决步骤 |
| :--- | :--- | :--- |
| `ModuleNotFoundError: No module named 'config'` | 运行环境缺少项目根目录 `PYTHONPATH`，或使用了系统全局 Python 而非 `.venv` | 使用 `.venv/bin/python` 执行，或在命令前增加 `PYTHONPATH=.`；MCP 配置推荐使用 `scripts/mcp-bridge.sh`。 |
| `ConnectionRefusedError` 或 `Socket not found` | AMR 守护进程未启动，或客户端与服务端的 `$XDG_RUNTIME_DIR` 不在同一用户会话 | 执行 `systemctl --user status amr.service` 或 `python3 scripts/amr_doctor.py` 检查服务是否存活。 |
| 启动立即退出，退出码 `3` (`SystemExit: 3`) | `OutboxWorker` 单执行者锁冲突：同机已有另一个 AMR 进程正在运行并占有 `$XDG_RUNTIME_DIR/amr-outbox.lock` | 检查是否有残留守护进程：`ps aux \| grep "src/main.py"`；单机仅允许运行单个 AMR Worker 实例，严禁多实例共享同一把锁。 |
| `[-32003] MODEL_UNAVAILABLE: Model is loading` | BGE-M3 嵌入模型正在从磁盘加载到 GPU/CPU，尚未就绪 | 等待数秒即可就绪；或在 `config.yaml` 中将 `model.preload_on_start` 设为 `true`，服务启动时自动预加载。 |
| `[-32002] VERSION_CONFLICT` | 并发更新冲突：传入的 `expected_version` 与数据库当前版本不一致 | 读取最新版本（`memory_get`），基于返回的 `current_version` 进行版本合并后重试更新。 |
| `[-32005] AGENT_UNAUTHORIZED` | 传入的 `AMR_SOURCE_AGENT` 未加入服务端白名单 | 在 `config/config.yaml` 的 `server.allowed_agents` 列表中追加该 Agent 名称，然后重启服务。 |
| `[-32006] IDEMPOTENCY_CONFLICT` | 同一 `idempotency_key` 被重复使用，但提交的正文或参数发生改变 | 幂等键应保证全局唯一（如 UUID v4）；若为网络超时重试，请保证请求载荷与首次调用严格一致。 |
| 刚调用 `memory_create` 成功，立即调用 `memory_search` 搜不到 | 最终一致性延迟：写路径只写 SQLite SSOT，Qdrant 向量索引由后台 OutboxWorker 异步投递 | 等待数十至数百毫秒后即可检索到；若需立即确认写入内容，请使用强一致的 `memory_get(memory_id)`。 |
| MCP 客户端报 Schema 枚举校验错误 | 传入了旧版小写状态词汇（如 `active`） | MCP 工具枚举严格遵循全大写规范：`["ACTIVE", "PENDING_VERIFY", "CONFLICT", "HISTORICAL", "TEMPORARY", "DELETED"]`。 |

---

## 架构边界与多实例限制

1. **单机单 Worker 独占**：
   AMR v5 的 Outbox 投影保证单写者有序性。同一台宿主机上，多个实例**绝对不能共享同一个 `amr-outbox.lock`**。若需在同一机器启动测试/多环境实例，必须隔离 `$XDG_RUNTIME_DIR`。
2. **读写分离与零网络依赖**：
   写入只依赖本地 SQLite，外部 Qdrant 发生网络闪断或重启时不影响客户端 `memory_create` 写入。待 Qdrant 恢复后，OutboxWorker 自动重试投递。

---

## 文档参考

- [接口契约规范 (DOC-AMR-04-API)](docs/DOC-AMR-04-API.md)
- [多智能体客户端接入指南 (DOC-AMR-06)](docs/DOC-AMR-06-MULTI-AGENT-INTEGRATION.md)
- [系统架构设计 (DOC-AMR-02-ADD)](docs/DOC-AMR-02-ADD.md)
- [详细设计与数据模型规范 (DOC-AMR-03-DDD)](docs/DOC-AMR-03-DDD.md)
- [生产部署与回退手册 (ROLLOUT-AMR-RELIABILITY)](docs/ROLLOUT-AMR-RELIABILITY.md)

---

## 许可证

[Apache License 2.0](LICENSE)

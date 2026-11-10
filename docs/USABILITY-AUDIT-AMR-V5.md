# AI Memory Runtime (AMR) v5.0.0 易用性 / 开发者体验 / 文档一致性 独立审计报告

**报告版本**：v1.0.0 (红队审计)  
**审计基线**：`main @ 1e30119` (AMR v5.0.0 正式发布版)  
**审计分支**：`audit/r5-usability` (隔离 worktree，未修改 `src/`)  
**审计环境**：Linux 7.0.0-43-generic x86_64 / Python 3.12 (纯隔离运行：FakeEngine + 内存 Qdrant + 临时 UDS + 独立 SQLite，绝不触碰生产)

---

## 1. 总体评价与 5 句话结论

### 1.1 总体评分表（各维度 1-5 分）

| 维度 | 得分 | 评价概述 |
| :--- | :---: | :--- |
| **安装配置 (Installation & Config)** | **2.5 / 5** | 依赖声明与实际不符（多装无用依赖、漏装分词依赖）；配置默认值与 YAML 示例不一致；单执行者锁导致本地多实例启动互斥崩溃。 |
| **首次接入 (First-time Onboarding)** | **2.0 / 5** | 阻断性体验问题显著：缺少 `PYTHONPATH` 导致 README 中 `python3 src/main.py` 和 MCP 配置直接报 `ModuleNotFoundError` 崩溃；官方脚本与 systemd 硬编码个人路径。 |
| **API 易用性 (API Usability & Schema)** | **3.8 / 5** | v5 的 6 态流转、乐观锁（`expected_version`）、幂等键（`idempotency_key`）底层机制健全且数据自解释，但接口出入参及概念术语较复杂，缺乏概念模型指引。 |
| **错误可诊断性 (Error Diagnosability)** | **4.2 / 5** | 异常码系统（`-32001` ~ `-32006`）及机器可读 `data` 字段设计优异；错误自解释度高；但部分工具层缺少参数前置校验，失败提示缺少下一步修复行动指南。 |
| **运维可观测性 (Operations & Observability)** | **3.0 / 5** | `src/admin/cli.py` 提供了 status/metrics/dead-letters/replay 等实用命令；但上线/运维文档反复宣称的 `amr doctor` 根本未实现；`--dry-run` 脚本仍会触发写库与备份。 |
| **文档准确性 (Documentation Consistency)** | **1.8 / 5** | 核心架构文档严重滞后（停留在 v2.2.0），大篇幅描述已被废弃的 `qdrant_sync_queue`、过时的入参字段和旧状态词汇；MCP 工具数量少报近一半。 |
| **综合加权得分** | **2.9 / 5** | **底层内核可靠性高，但开发者门面与文档体验存在多处严重断层（“能跑通全靠读源码猜”）。** |

### 1.2 5 句话“好用程度”结论
1. **谁适合用**：适合熟悉 Python 源码、深刻理解分布式 CQRS/Outbox 模式、愿意自行调试本地 IPC 协议的内部资深 Agent 核心开发者与平台工程团队。
2. **谁不适合用**：目前**极不适合**希望“按 README 复制粘贴 5 分钟快速接入”的第三方外部 Agent 开发者或初学者。
3. **上手成本**：高。新开发者如果只看 `README.md` 和 `DOC-AMR-04-API.md`，从环境配置到 MCP 调通至少会踩 4 个阻断性环境坑（ModuleNotFoundError、路径硬编码、锁互斥、接口参数被无声忽略）。
4. **最大摩擦点**：**文档与真实代码的割裂**——文档停留在 v2.2.0 的旧表与旧参数，而代码已进化为 v5.0.0 的严格状态机与统一发件箱；同时运维急需的健康体检工具 `amr doctor` 查无此物。
5. **核心价值与潜力**：一旦越过接入阻断点，AMR v5 的写路径零 I/O、版本冲突保护（`-32002`）以及幂等重放机制（`-32006`）表现出极高的工业级健壮性与响应速度。

---

## 2. 新手接入路径核对与阻断点分析

完全按照官方 `README.md` 与 `docs/` 提供的指导步骤进行新开发者模拟接入，逐条核对：

### 2.1 依赖安装：`requirements.txt` vs `README.md` 冲突
- **文档步骤**（`README.md:49`）：
  ```bash
  pip install torch transformers sentence-transformers qdrant-client pyyaml pydantic
  ```
- **实际文件**（`requirements.txt`）：
  ```text
  fastapi>=0.110.0
  uvicorn>=0.28.0
  pydantic>=2.6.0
  pyyaml>=6.0.1
  qdrant-client>=1.8.0
  pytest>=8.0.0
  pytest-asyncio>=0.23.0
  torch
  transformers
  ```
- **不一致与摩擦**：
  1. `requirements.txt` 中残留了 `fastapi` 与 `uvicorn`。AMR 在 v3/v5 已彻底收敛至 Unix Domain Socket（二进制自定义帧协议），移除了所有 HTTP 端口监听。安装 FastAPI/Uvicorn 浪费带宽与磁盘空间。
  2. `README.md` 声明安装 `sentence-transformers`，但 `requirements.txt` 中漏填；如果用户使用 `pip install -r requirements.txt`，某些测试或辅助脚本可能会缺少模块。

### 2.2 启动服务：`ModuleNotFoundError` 阻断
- **文档步骤**（`README.md:85`）：
  ```bash
  python3 src/main.py
  ```
- **实际行为**：
  ```text
  Traceback (most recent call last):
    File "src/main.py", line 17, in <module>
      from config.settings import check_and_fix_file_permissions, load_config
  ModuleNotFoundError: No module named 'config'
  ```
- **根本原因**：`src/main.py` 没有像 `src/admin/cli.py:3-5` 那样将项目根目录加入 `sys.path`。在终端中直接运行 `python3 src/main.py` 时，Python 默认将 `src/` 放入 `sys.path[0]`，找不到 `config` 包。
- **初学者体验**：第一步按照文档敲回车直接报错崩溃，体验极差。

### 2.3 Systemd 用户服务：路径与 Python 解释器硬编码
- **文档步骤**（`README.md:89-91`）：
  ```bash
  cp systemd/amr.service ~/.config/systemd/user/
  systemctl --user daemon-reload
  systemctl --user enable --now amr.service
  ```
- **实际文件**（`systemd/amr.service`）：
  ```ini
  WorkingDirectory=%h/WorkSpaces/ai-memory-runtime
  Environment="PYTHONPATH=%h/WorkSpaces/ai-memory-runtime"
  Environment="CUDA_VISIBLE_DEVICES=0"
  ExecStart=/usr/bin/python3 src/main.py
  ReadWritePaths=/run/user/%U %h/WorkSpaces/ai-memory-runtime/data
  ```
- **不一致与阻断**：
  1. 硬编码工作目录 `%h/WorkSpaces/ai-memory-runtime`：若开发者克隆到其他路径（如 `~/amr` 或 worktree），服务完全无法启动。
  2. `ExecStart` 强制指定 `/usr/bin/python3`：由于 PyTorch、Transformers、Qdrant 等重量级依赖通常安装在虚拟环境（如 `.venv`），系统 Python 缺少这些库，启动即崩溃退出。
  3. 缺少服务前置条件提示（如确保 `$XDG_RUNTIME_DIR` 目录存在）。

### 2.4 MCP 客户端接入：缺少 PYTHONPATH 与硬编码脚本
- **文档步骤**（`README.md:105-115`）：
  ```json
  {
    "mcpServers": {
      "ai-memory": {
        "command": "python3",
        "args": ["src/interfaces/mcp/bridge.py"],
        "env": { "AMR_SOURCE_AGENT": "openclaw" }
      }
    }
  }
  ```
- **实际行为**：
  Claude Desktop 或 OpenClaw 启动该进程时，由于工作目录和环境中无 `PYTHONPATH`，`src/interfaces/mcp/bridge.py:18` 同样会因为 `from config.settings import load_config` 抛出 `ModuleNotFoundError`，导致 MCP 客户端连接直接挂掉且宿主报错“Server process exited with code 1”。
  （注：`docs/DOC-AMR-06-MULTI-AGENT-INTEGRATION.md:53` 虽然补写了 `"PYTHONPATH": "<仓库路径>"`，但 `README.md` 中漏写了，初学者大多依据 README 接入）。
- **官方辅助脚本 `scripts/mcp-bridge.sh` 的缺陷**：
  ```bash
  export PYTHONPATH="/home/dj/WorkSpaces/ai-memory-runtime:${PYTHONPATH:-}"
  export AMR_SOURCE_AGENT="${AMR_SOURCE_AGENT:-zcode}"
  exec /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python /home/dj/WorkSpaces/ai-memory-runtime/src/interfaces/mcp/bridge.py "$@"
  ```
  路径全部绝对硬编码在特定开发者的个人目录与 `.venv`，离开该特定机器或切换目录后脚本完全失效。

---

## 3. 隔离环境下的实际最小可用流程 (E2E Verification)

在纯隔离环境中运行（FakeEngine + 内存 Qdrant + 临时 SQLite + 临时 UDS），并配置 `AMR_SOCKET_PATH`，通过官方 `scripts/mcp-bridge.sh` 完整走完标准链路。每一步的参数猜测度、响应字段与一致性表现如下：

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        Minimal Usable Flow Test (E2E)                                  │
│                                                                                        │
│  [Step 1] initialize     ──► protocolVersion: 2024-11-05, serverInfo: ok               │
│  [Step 2] tools/list     ──► 实际返回 9 个工具 (文档只记载 5 个)                       │
│  [Step 3] memory_create  ──► 返回 pending, version=1, 自动补全 provenance-missing      │
│  [Step 4] memory_get     ──► 强一致！SQLite SSOT 立即返回 ACTIVE 事实                  │
│  [Step 5] Outbox Worker  ──► 异步单执行者线程消费 outbox 投递 Qdrant                    │
│  [Step 6] memory_search  ──► 弱一致！投影后检索命中，但 created_at 为 null (Defect)   │
│  [Step 7] memory_update  ──► expected_version 乐观锁防护，冲突返回 -32002              │
│  [Step 8] memory_delete  ──► 软删除返回 DELETED, pending, Qdrant 异步物理删点           │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 详细执行步骤与发现记录

#### Step 1: `initialize`
- **请求**：`{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}`
- **响应**：
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
      "protocolVersion": "2024-11-05",
      "capabilities": {"tools": {}},
      "serverInfo": {"name": "ai-memory-runtime-mcp", "version": "1.0.0"}
    }
  }
  ```
- **评估**：符合 MCP 2024-11-05 协议标准，开箱即用。

#### Step 2: `tools/list`
- **请求**：`{"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}`
- **响应**：返回 9 个工具定义：
  `memory_search`, `memory_create`, `memory_update`, `memory_history`, `memory_delete`, `memory_get`, `memory_record`, `memory_update_status`, `memory_ingest_session`。
- **评估与摩擦**：
  文档 `DOC-AMR-06` 宣称只有 5 个工具（遗漏了 `memory_history`、`memory_delete` 等）。但工具自身的 JSON Schema 描述详尽，包含了 `description` 与字段校验，自解释度好。

#### Step 3: `memory_create`
- **请求参数**：
  ```json
  {
    "content": "AEP architecture consensus: single writer projection with CAS",
    "project_id": "aep-arch",
    "type": "decision/arch"
  }
  ```
- **实际返回**：
  ```json
  {
    "memory_id": "mem_20261109_775697",
    "version": 1,
    "status": "ACTIVE",
    "projection_status": "pending",
    "projection_revision": 1,
    "source_refs": [
      "provenance-missing:openclaw:20261109_233147"
    ],
    "revisions": [
      {
        "version": 1,
        "content": "AEP architecture consensus: single writer projection with CAS",
        "change_reason": "Initial creation"
      }
    ]
  }
  ```
- **关键发现**：
  1. **参数猜测**：若按 `DOC-AMR-04-API:42-60` 传入 `source_message_ids` 或 `session_id`，会被无声忽略。
  2. **溯源审计打标**：因为未传 `source_refs`，AMR 自动打上了 `"provenance-missing:openclaw:<timestamp>"`。机制设计极佳，但文档未说明，新用户会困惑“为什么多了一个没传的字段”。
  3. **异步状态**：明确返回 `"projection_status": "pending"`。

#### Step 4: `memory_get`
- **请求参数**：`{"memory_id": "mem_20261109_775697"}`
- **实际返回**：立即从本地 SQLite SSOT 返回最新记录（`status: "ACTIVE"`, `version: 1`, `enrichment: "ok"`）。
- **评估**：强一致读取体验极好，无需等待向量异步刷新。

#### Step 5: `memory_search` (弱一致性验证)
- **请求参数**：`{"query": "AEP architecture consensus", "project_id": "aep-arch"}`
- **时序与结果**：
  - 在创建后的前 ~100ms 内，Qdrant 尚未同步，`memory_search` 弱一致返回空。
  - Outbox Worker 处理任务后，Qdrant 向量就绪，`memory_search` 正确返回该点位。
- **严重发现 (Bug)**：
  返回的搜索条目中，`"created_at": null`！
  **根因**：`src/service/outbox_worker.py:118-129` 在构造写入 Qdrant 的 11 字段 payload 时漏掉了 `created_at`，而 `src/service/memory_service.py:326` 取 `payload.get("created_at")`，因此检索结果中 `created_at` 恒为 `null`。与文档 `DOC-AMR-04-API` 明确声明有 `created_at` 的契约违背。

#### Step 6: `memory_update`
- **参数校验测试**：
  - 修改 `content` 但未传 `expected_version` 时：返回 `ValueError: Parameter 'expected_version' is required when updating content.`。错误清晰自解释。
  - 传入错误的 `expected_version: 99`（当前为 1）时：正确返回标准 JSON-RPC 错误：
    ```text
    [-32002] VERSION_CONFLICT: Expected version 99, but current version is 1.
    {"memory_id": "mem_20261109_775697", "current_version": 1, "expected_version": 99}
    ```
    结构化携带了 `current_version`，客户端无需解析字符串即可自愈重试。
  - 传入正确 `expected_version: 1`：成功升级为 `version: 2`，`projection_revision: 2`，`projection_status: "pending"`。

#### Step 7: `memory_delete`
- **请求参数**：`{"memory_id": "mem_20261109_775697", "reason": "lifecycle cleanup"}`
- **实际返回**：
  ```json
  {
    "memory_id": "mem_20261109_775697",
    "previous_status": "ACTIVE",
    "current_status": "DELETED",
    "projection_status": "pending",
    "projection_revision": 3
  }
  ```
- **评估**：软删除生效；SQLite 状态变为 `DELETED`；Outbox Worker 后台从 Qdrant 中物理删点。随后的默认 `memory_search` 不再召回该条记录。

---

## 4. 运维体验评估 (Admin & Operations)

### 4.1 `src/admin/cli.py` 可发现性与交互体验
- **可用命令**：`status`, `load`, `unload`, `collections`, `snapshot`, `metrics`, `dead-letters`, `replay`。
- **优点**：
  1. `python3 src/admin/cli.py --help` 文档与参数描述非常清晰。
  2. 输出均为格式规范的标准 JSON，便于运维脚本（如 `jq`）解析。
  3. 支持 `-s / --socket` 参数指定 admin socket，支持隔离环境诊断。
- **不足与体验摩擦**：
  1. **缺少统一 CLI 命令**：必须以 `python3 src/admin/cli.py` 这种脚本形式运行；未打包为 `amr` 控制台命令。
  2. **默认 Socket 易错连**：未带 `-s` 时，默认直连 `/run/user/<uid>/qdrant-bge-admin.sock`。如果在测试或自定义环境中运行，会误连或报错 `Admin socket not found`，且没有任何排查提示（如未提示检查进程或服务状态）。
  3. **指标瞬态性**：`status.metrics` 取自内存单例，守护进程重启后指标全部清零，运维无法查看跨重启的历史延迟分布。
  4. **Replay 缺少预览**：`replay` 命令直接将死信重新入队，不支持 `--dry-run` 预览将要重放的任务。

### 4.2 缺失的关键运维工具：`amr doctor` 案情调查
- **严重问题**：
  在上线与回退手册中，多处将 `amr doctor` 列为核心运维手段：
  - `docs/ROLLOUT-AMR-RELIABILITY.md:114`: 显式要求执行 `amr doctor`
  - `docs/ROLLOUT-AMR-RELIABILITY.md:97`: 将 `amr doctor 报告 CRITICAL` 列为回退红线
  - `docs/DEPLOYMENT-RECORD-20261110.md:78`: 将 `amr doctor CRITICAL` 列为回退触发条件
  - `docs/AUDIT-FOR-GPT-RELIABILITY-FIXES-V5.md:235`: 明确声明存在 `scripts/amr_doctor.py`
  - `src/service/metrics.py:3`: 源码注释提及 `snapshot() 足够 amr doctor 使用`
- **代码实况**：
  全局搜索发现，代码库中**根本不存在 `scripts/amr_doctor.py`**，系统中也没有 `amr doctor`。
- **后果**：
  当生产环境发生故障或新版本上线时，运维工程师按照上线手册操作，终端报错 `command not found: amr`，或者找不到脚本。直接丧失关键故障体检能力，引发严重运维恐慌。

### 4.3 `scripts/daily_memory_reconciliation.py --dry-run` 的副作用陷阱
- **问题**：`README.md:126` 推荐运行数据对账脚本检查模式：
  `python3 scripts/daily_memory_reconciliation.py --dry-run`
- **代码实现审查**：
  - `src/reconciliation/engine.py:108-109`：**Phase 0 (Preflight)** 在 `--dry-run` 模式下，依然执行了 `cursor.execute("INSERT INTO memory_reconciliation_checkpoints ...")` 并执行 `conn.commit()`！
  - `src/reconciliation/engine.py:116-128`：**Phase 1 (Cold Backup)** 在 `--dry-run` 模式下，依然调用 `conn.backup()`，并在磁盘上创建 `data/backups/sqlite_sessions_<date>.db` 物理文件！
- **运维风险**：
  运维本意是“只读探测，不改动任何状态与文件”，但该脚本会在磁盘中生成数百兆备份并向数据库写入检查点记录。这违反了 UNIX 工具对 `--dry-run` 的只读心理预期。

---

## 5. 契约易用性深度评估

### 5.1 异步写 (Pending) 与双重一致性困惑
- **架构事实**：
  - 写入链路（`memory_create` / `update` / `delete`）：纯写 SQLite SSOT，生成 outbox 任务，立即返回 `projection_status: "pending"`。耗时极低（< 5ms）。
  - 读取链路（`memory_get`）：读取 SQLite SSOT，强一致，立即拿到最新内容与版本。
  - 检索链路（`memory_search`）：检索 Qdrant 向量索引，依赖 Outbox Worker 后台计算向量并写入，弱一致（Eventual Consistency）。
- **用户心智模型断层**：
  新手开发者通常习惯传统的同步写数据库模式。调用 `memory_create` 成功后立即调用 `memory_search`，发现搜不到，第一反应是“系统出 Bug 了”或“创建失败了”。
- **文档缺失**：现有所有文档均未提及这一架构特性，未解释 `projection_status: "pending"` 的含义，未提示检索存在异步延迟。

### 5.2 状态双轨制（标准 6 态 vs Legacy 4 态）的踩坑面
- **两套状态体系**：
  - 标准 6 态：`ACTIVE`, `PENDING_VERIFY`, `CONFLICT`, `HISTORICAL`, `TEMPORARY`, `DELETED`（全大写）。
  - Legacy 4 态：`active`, `superseded`, `archived`, `deleted`（全小写）。
- **主要踩坑点**：
  1. `DOC-AMR-04-API` 示例全部使用小写 `active`, `archived`。然而 MCP Tool Schema（`TOOL_DEFINITIONS`）中，`status` 的 enum 约束全是大写 `["ACTIVE", "PENDING_VERIFY", ...]`。如果客户端依赖严格 JSON Schema 校验，按文档传小写 `active` 会直接在客户端拦截报错！
  2. **严重缺陷：`SUPERSEDED` 链条追踪失效 Bug**：
     在 `src/service/memory_service.py:239-244` 中，Exact-ID 检索设计了沿 `superseded_by` 自动跳转 3 跳寻找最新版本的功能：
     ```python
     status_val = (payload.get("status") or "").upper()
     if status_val == "SUPERSEDED" and payload.get("superseded_by"):
         point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, payload["superseded_by"])
         continue
     ```
     然而在 v5 中，所有 v3 投影点由 `normalize_status` 处理，`superseded` 被归一化为了 `HISTORICAL`！
     Qdrant 中点的 payload status 只能是 `HISTORICAL`，永远不可能等于 `SUPERSEDED`。因此这个 3 跳重定向功能对所有 v3 记忆**永久失效**。

### 5.3 幂等键 (`idempotency_key`) 易用性
- **实现水准**：
  - 成功重放：返回 `idempotent_replay: true`。
  - 冲突拒绝：返回错误码 `-32006 IDEMPOTENCY_CONFLICT`，附带已有 `existing_memory_id`。
  - 保留周期：`StorageConfig.idempotency_retention_days = 7` 天。
- **易用性缺失**：
  - 文档中对 `idempotency_key` 仅有一句说明，未说明保留期（7 天）、未说明摘要比对范围（仅对比核心字段，排除 memory_id）、未说明错误码 `-32006`。
  - MCP 工具的 `inputSchema` 中只标注了可选字符串，缺少何时应使用的实践建议（如网络超时重试时必须带上）。

### 5.4 领域术语的认知复杂度
- **术语混杂**：
  - `memory_type` vs `type`：文档中写 `memory_type`，代码和 MCP 中主推 `type`（且支持 `decision/arch` 命名空间通配）。
  - `scope` vs `project_id`：`project_id` 用于跨项目隔离与继承（默认继承 `global`, `general`），而 `scope` 同样存在且默认 `global`。两者在概念上有部分重叠，文档未给出概念边界。
  - `collection_name`：代码支持 `ai_memory`, `crypto_standards`, `project_docs`，但 MCP `memory_create` 的输入参数中根本没有暴露 `collection_name`！导致 MCP Agent 无法指定集合写入。

---

## 6. 文档与代码/配置不一致性清单 (Evidence File:Line)

以下清单按严重程度排序，每一条均经代码核对与测试验证：

| 序号 | 文档位置 | 代码/实际位置 | 严重度 | 缺陷描述与证据 |
|:---:|:---|:---|:---:|:---|
| **1** | `ROLLOUT-AMR-RELIABILITY.md:114`<br>`DEPLOYMENT-RECORD-20261110.md:78` | *(代码库全局不存在)* | **Critical** | **虚构命令 `amr doctor`**：上线和回退手册将 `amr doctor` 列为核心验收与回退门禁，但代码中没有任何 `doctor` 脚本或 CLI 命令。 |
| **2** | `README.md:85`<br>`README.md:109-111` | `src/main.py:17`<br>`src/interfaces/mcp/bridge.py:18` | **High** | **缺少 PYTHONPATH 导致快速启动崩溃**：`main.py` 和 `bridge.py` 顶部缺少 sys.path 根目录注入，用户按文档执行直接报 `ModuleNotFoundError: No module named 'config'`。 |
| **3** | `scripts/mcp-bridge.sh:3-5` | `scripts/mcp-bridge.sh:3-5` | **High** | **官方脚本绝对路径硬编码**：脚本中硬编码 `/home/dj/WorkSpaces/ai-memory-runtime` 和特定 `.venv`，其他环境/目录执行直接失效。 |
| **4** | `DOC-AMR-04-API.md:110-111` | `src/main.py:235, 250` | **High** | **管理接口 RPC 方法名错误**：文档写 `admin.get_status` 和 `admin.collection_list`，代码实际只认 `admin.status` 和 `admin.collections`。按文档调用必抛 `Unknown admin method`。 |
| **5** | `systemd/amr.service:7,10` | `systemd/amr.service:7,10` | **High** | **Systemd 服务路径与 Python 解释器硬编码**：硬编码工作目录 `%h/WorkSpaces/...` 且使用系统 `/usr/bin/python3`（无 venv 依赖），导致开箱部署失败。 |
| **6** | `DOC-AMR-04-API.md:42-60` | `src/interfaces/mcp/tools.py:68-107`<br>`src/main.py:141-152` | **High** | **`memory_create` 入参与出参完全过时**：文档写 `source_message_ids`/`session_id`，代码实际为 `source_refs`（传错被静默忽略）；出参文档写小写 `active` 且无 `projection_status`，代码实际为大写 `ACTIVE` 且含 `projection_status="pending"`。 |
| **7** | `DOC-AMR-04-API.md:33`<br>`src/service/memory_service.py:326` | `src/service/outbox_worker.py:118-129` | **Medium** | **检索结果 `created_at` 恒为 null**：文档规定检索返回 `created_at`，但 Outbox Worker 写入 Qdrant 时漏填该字段，导致检索出参此字段恒为 `null`。 |
| **8** | `src/service/memory_service.py:240` | `src/service/memory_status.py:18`<br>`src/service/outbox_worker.py:101` | **Medium** | **`SUPERSEDED` 跳转断言与规范化矛盾 (Bug)**：`memory_search` 判定 `status_val == "SUPERSEDED"`，但 v5 状态已将 `superseded` 统一归一化为 `HISTORICAL`，导致链式重定向永久失效。 |
| **9** | `DOC-AMR-06-MULTI-AGENT-INTEGRATION.md:61-67, 139` | `src/interfaces/mcp/tools.py:19-259` | **Medium** | **MCP 工具清单少报**：文档宣称有 5 个工具，代码实际暴露 9 个工具（漏记 `memory_history`, `memory_delete`, `memory_record`, `memory_update_status`）。 |
| **10** | `README.md:126`<br>`scripts/daily_memory_reconciliation.py` | `src/reconciliation/engine.py:108, 122` | **Medium** | **对账脚本 `--dry-run` 违反只读原则**：`--dry-run` 模式下仍然向数据库写 checkpoint 记录，且向磁盘创建备份文件。 |
| **11** | `config/settings.py:49, 94`<br>`config/config.example.yaml:27, 50` | `config/settings.py`<br>`config/config.example.yaml` | **Low** | **配置默认值与 YAML 示例不一致**：Pydantic 默认 `idle_timeout_seconds=300`、`default_score_threshold=None`，YAML 示例却写 `3600` 和 `0.52`。 |
| **12** | `README.md:49`<br>`requirements.txt:1-2` | `requirements.txt` | **Low** | **依赖冗余与遗漏**：`requirements.txt` 中残留无用的 `fastapi`, `uvicorn`，且未声明 `sentence-transformers`。 |
| **13** | `DOC-AMR-02-ADD.md:20`<br>`DOC-AMR-03-DDD.md:55` | `src/core/session_store.py:2259`<br>`src/service/outbox_worker.py:78` | **Low** | **架构设计表名停留在废弃版本**：设计文档大篇幅描述 `qdrant_sync_queue`，而 v5 已全面升级为 `memory_projection_outbox`。 |

---

## 7. 自动化测试固化结果

为确保上述审计发现具有可复现的代码级证据，本轨道在 `tests/audit_r5_usability.py` 中编写了 4 个大型用例，固化了接入流程、一致性断言与缺陷点：

- **测试用例 1**：`test_fresh_agent_minimal_flow_and_doc_inconsistencies`
  - 启动隔离守护进程与官方 `scripts/mcp-bridge.sh`。
  - 完整走通 7 步最小可用流程。
  - 验证 9 个工具存在、`projection_status="pending"` 异步契约、强一致 SSOT 读取、弱一致搜索延迟、检索结果 `created_at is None` 缺陷、以及 `expected_version` 乐观锁报错。
- **测试用例 2**：`test_idempotency_key_replay_and_conflict`
  - 验证同一幂等键重放返回 `idempotent_replay: true`。
  - 验证同一幂等键冲突内容返回 `-32006 IDEMPOTENCY_CONFLICT` 错误及结构化数据。
- **测试用例 3**：`test_admin_cli_operability`
  - 验证 `src/admin/cli.py` 在隔离 Socket 下的 status/metrics/dead-letters/replay/collections。
- **测试用例 4**：`test_documentation_discrepancies`
  - 固化 `scripts/amr_doctor.py` 不存在断言。
  - 固化 `src/main.py` 和 `bridge.py` 脱离 `PYTHONPATH` 报 `ModuleNotFoundError` 缺陷。
  - 固化 `normalize_status('superseded') == 'HISTORICAL'` 与重定向判定条件冲突。

### 运行结果
```bash
$ PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/audit_r5_usability.py -q
....                                                                     [100%]
4 passed in 9.56s

$ PYTHONPATH=$PWD /home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python -m pytest tests/ -q
...................................................................ssss. [ 84%]
..........s...............s...........................                   [100%]
336 passed, 6 skipped in 47.51s
```
**全量 336 个既有测试用例全部通过，新增 4 个审计用例全部通过，零破损。**

---

## 8. Top 10 改进建议（按投入产出比排序）

建议按照“极低成本高收益的文档与门面修复”先行、“低风险接口契约补全”次之的原则推进（给出具体修复 patch 建议）：

### 建议 1: 在 `src/main.py` 与 `src/interfaces/mcp/bridge.py` 头部注入 sys.path
- **目标**：彻底解决新用户直接运行 `python3 src/main.py` 报错 `ModuleNotFoundError`。
- **修复位置**：`src/main.py:1` 与 `src/interfaces/mcp/bridge.py:1`
- **Patch 建议**：
  ```python
  import sys
  from pathlib import Path
  root_dir = str(Path(__file__).resolve().parent.parent if "mcp" in __file__ else Path(__file__).resolve().parent)
  if root_dir not in sys.path:
      sys.path.insert(0, root_dir)
  ```

### 建议 2: 将 `scripts/mcp-bridge.sh` 改为动态解析自身路径
- **目标**：移除所有个人主目录 `/home/dj/...` 硬编码，使其在任何目录与机器上均可直接执行。
- **修复位置**：`scripts/mcp-bridge.sh`
- **Patch 建议**：
  ```bash
  #!/usr/bin/env bash
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
  export AMR_SOURCE_AGENT="${AMR_SOURCE_AGENT:-openclaw}"
  PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python"
  [ ! -f "$PYTHON_BIN" ] && PYTHON_BIN="$(command -v python3)"
  exec "$PYTHON_BIN" "${SCRIPT_DIR}/src/interfaces/mcp/bridge.py" "$@"
  ```

### 建议 3: 实现官方 `scripts/amr_doctor.py` 并提供 CLI 入口
- **目标**：补齐文档宣称但实际缺失的 `amr doctor`，支持输出健康状态、Qdrant/SQLite 连通性、锁状态与脱敏诊断。
- **建议**：新建轻量检查脚本 `scripts/amr_doctor.py`，调用 `admin.status` 与 `status.metrics`，并在 `admin/cli.py` 中增加 `doctor` 子命令。

### 建议 4: 修复 `OutboxWorker` 写入 Qdrant 遗漏 `created_at` 的缺陷
- **目标**：使 `memory_search` 返回的字段与文档及 `memory_get` 一致，不再为 `null`。
- **修复位置**：`src/service/outbox_worker.py:126`
- **Patch 建议**：
  ```python
  payload = {
      "memory_id": snapshot["memory_id"],
      "version": snapshot.get("version", 1),
      "content": content,
      "created_at": snapshot.get("created_at"),  # 补齐此行
      "updated_at": snapshot.get("updated_at", int(time.time())),
      ...
  }
  ```

### 建议 5: 修复 `memory_search` 中 `SUPERSEDED` 跳转逻辑
- **目标**：让 v3 记忆（状态为 `HISTORICAL` 且有 `superseded_by`）能正常触发 3 跳重定向找到最新现役版本。
- **修复位置**：`src/service/memory_service.py:240`
- **Patch 建议**：
  ```python
  status_val = (payload.get("status") or "").upper()
  if status_val in ("SUPERSEDED", "HISTORICAL") and payload.get("superseded_by"):
      redirect_from = redirect_from or matched_id
      point_info = await asyncio.to_thread(self.qdrant.get_point_by_memory_id, payload["superseded_by"])
      continue
  ```

### 建议 6: 全面更新 `DOC-AMR-04-API.md` 至 v5.0.0
- **目标**：消灭过时入参误导。
- **改动点**：
  1. 更新 `memory.create` 入参：移除 `source_message_ids`/`session_id`，增加 `source_refs`, `conflicts_with`, `idempotency_key`。
  2. 显式记录出参 `projection_status: "pending"`，并在一级章节说明“读-写-搜双重一致性模型”（Read-by-ID 强一致，Search 弱一致）。
  3. 修正管理接口方法名为 `admin.status` 和 `admin.collections`。
  4. 增加标准错误码表（`-32001` ~ `-32006`）及对应的 `data` 结构。

### 建议 7: 更新 `README.md` 与 `DOC-AMR-06` 中的 MCP 配置示例
- **目标**：防止新开发者复制 JSON 到配置后启动失败。
- **改动点**：在 `README.md` 的 MCP 配置示例中显式加入 `"PYTHONPATH": "<path-to-amr>"`，或者推荐使用打包后的可执行脚本。更新 `DOC-AMR-06` 中的工具清单为全量 9 个工具。

### 建议 8: 规范 `scripts/daily_memory_reconciliation.py --dry-run` 行为
- **目标**：遵循 UNIX 规范，只读模式下严禁执行任何写操作。
- **改动点**：
  - 在 Phase 0 中增加 `if not self.dry_run:` 守卫，跳过向 `memory_reconciliation_checkpoints` 插入记录。
  - 在 Phase 1 中增加 `if not self.dry_run:` 守卫，跳过创建 `data/backups/sqlite_sessions_*.db`。

### 建议 9: 清理 `requirements.txt` 与配置默认值对齐
- **目标**：减少环境冗余，消除隐性配置分歧。
- **改动点**：
  1. 从 `requirements.txt` 中移除无用的 `fastapi` 与 `uvicorn`，补上 `sentence-transformers`。
  2. 对齐 `config/settings.py` 与 `config/config.example.yaml` 的默认值（如 `idle_timeout_seconds` 统一为 3600 秒，`default_score_threshold` 统一推荐 0.52）。

### 建议 10: 解耦 `OutboxWorker` 单执行者锁路径
- **目标**：允许通过配置文件或环境变量显式指定 `outbox_lock_path`。
- **改动点**：在 `ServerConfig` 中添加 `outbox_lock_path: Optional[str] = None`，`main.py` 在初始化 `OutboxWorker` 时传入该路径，避免本地多实例或测试环境由于共享 `/run/user/<uid>/amr-outbox.lock` 发生互斥崩溃。

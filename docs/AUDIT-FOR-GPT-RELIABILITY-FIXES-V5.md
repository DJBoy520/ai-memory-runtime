# AI Memory Runtime (AMR) 可靠性重构与故障排查审计报告（v5 阶段）

- **审计目的**: 针对多智能体高并发调用 AMR 时出现的**超时雪崩、假 OCC 冲突、旧数据覆盖与状态分裂**，经用户纠偏、GPT 外部首轮审计、以及 Gemini + DeepSeek 连续三轮严苛双向评审拦截与推演后，对最新的 **RFC v5 重构修复全案** 进行独立合规性与边界安全性审查。
- **项目路径**: `/home/dj/WorkSpaces/ai-memory-runtime`
- **当前 Git 分支**: `fix/amr-reliability-v3`
- **最新 Commit**: `b690bee` (`docs(rfc): v5 per review round 3...`)
- **生成时间**: 2026-11-08

---

## 目录
1. [背景脉络与真实生产事实](#1-背景脉络与真实生产事实)
2. [故障日志与第一手核验证据](#2-故障日志与第一手核验证据)
3. [多模型协同排查与审查演进全景（三次评审 62 项 Blocker 深度博弈）](#3-多模型协同排查与审查演进全景三次评审-62-项-blocker-深度博弈)
4. [核心根因与机制缺陷剖析](#4-核心根因与机制缺陷剖析)
5. [RFC v5 最终架构方案深度规格（核心重点）](#5-rfc-v5-最终架构方案深度规格核心重点)
6. [测试矩阵、隔离纪律与既有测试适配白名单](#6-测试矩阵隔离纪律与既有测试适配白名单)
7. [已落盘的代码、契约与迁移脚本基座](#7-已落盘的代码契约与迁移脚本基座)
8. [专供外部 GPT 深度审查的技术争议与风险清单](#8-专供外部-gpt-深度审查的技术争议与风险清单)

---

## 1. 背景脉络与真实生产事实

AMR（AI Memory Runtime）是部署于本机环境下的多智能体统一语义记忆系统，核心拓扑如下：
- **宿主环境**: Linux 7.0 (Ubuntu), Python 3.12, systemd 用户级服务 (`amr.service`, PID 3448659)。
- **硬件基座**: NVIDIA Tesla P4 (8GB VRAM)，承载 `BAAI/bge-m3` 模型（FP16，常驻显存约 1.3GB）。
- **双通道 UDS (0600)**:
  - 业务端口: `/run/user/1000/qdrant-bge.sock`（JSON-RPC 2.0，4 字节 big-endian 长度前缀，单帧上限 4MB）。
  - 管理端口: `/run/user/1000/qdrant-bge-admin.sock`。
- **存储拓扑**:
  - **唯一事实真相源 (SSOT)**: 本地 SQLite 3 WAL 模式 (`data/sessions.db`)。
  - **向量检索引擎**: 独立 Qdrant 节点 (`http://192.168.30.161:6333`)。
- **生产数据库审计快照 (2026-11-07 实测)**:
  - `memories` 主表: `ACTIVE: 605`, `HISTORICAL: 522`, `DELETED: 28`（无遗留脏状态）。
  - `qdrant_sync_queue`: 历史任务 3587 条，全部为 `synced`。
  - `memory_projection_outbox`: 0 行（此前建表但实际无有效消费，仅对账引擎向其写入）。

---

## 2. 故障日志与第一手核验证据

### 2.1 模型冷启动自旋超时（近 7 天 journald：`ModelLoadingTimeoutError` 7 次、`Client disconnected before response could be sent` 40 次；次数为直接计数，未推算占比）
```text
2026-11-03 19:35:42,097 [ERROR] [src.core.engine] Inference error in embed: Timed out waiting for READY (25.0s)
  File "/home/dj/WorkSpaces/ai-memory-runtime/src/core/engine.py", line 325, in embed
    await self._wait_for_ready(timeout=25.0)
src.core.engine.ModelLoadingTimeoutError: Timed out waiting for READY (25.0s)
2026-11-07 18:30:37,725 [INFO] [ipc.server] [business] Client disconnected before response could be sent
```

### 2.2 版本冲突现场（该日志证明冲突存在；"假失败"的判定依据是源码链路分析——`memory_service.py:728-769` 先提交 SQLite 后同步 embed/Qdrant，日志本身不构成独立证明）
```text
2026-11-07 18:30:30,858 [ERROR] [ipc.server] [business] Handler error executing method 'memory.update': VERSION_CONFLICT: Expected version 2, but current version is 3.
  File "/home/dj/WorkSpaces/ai-memory-runtime/src/core/session_store.py", line 1545, in update_memory_v3
    raise ValueError(f"VERSION_CONFLICT: Expected version {expected_version}, but current version is {curr_version}.")
```

### 2.3 客户端命名空间未授权
```text
2026-11-07 12:52:20,300 [ERROR] [ipc.server] [business] Handler error executing method 'memory.search': AGENT_UNAUTHORIZED: Unknown or untrusted agent_id 'codex'
2026-10-10 21:59:44,265 [ERROR] [ipc.server] [business] Handler error executing method 'memory.search': AGENT_UNAUTHORIZED: Unknown or untrusted agent_id 'zcode-win'
```

### 2.4 状态枚举断裂
```text
2026-10-25 11:54:04,445 [ERROR] [ipc.server] [business] Handler error executing method 'memory.update': Invalid status: 'SUPERSEDED'. Must be one of {'TEMPORARY', 'ACTIVE', 'CONFLICT', 'DELETED', 'HISTORICAL', 'PENDING_VERIFY'}
```

---

## 3. 多模型协同排查与审查演进全景（三次评审 62 项 Blocker 深度博弈）

本项目的方案演进是一场严谨的工程博弈，经历了**四轮大重构、三次双模型拦截、共计 62 项 Blocker 逐项歼灭**：

```
初查直觉方案 (Gemini) 
   │ 纠偏：Timer(0)立即卸载、事务假失败、80ms fail-open 是设计预算
   ▼
用户深度复核
   │ 形成 RFC v1，启动 Gemini + DeepSeek 自动化双向评审门
   ▼
【第一轮流水线评审】── 拦截！22 项 Blocker (15 High / 7 Medium)
   │ 致命漏洞：认错生产队列表、asyncio.Future 跨 Loop 死锁、缺少 RpcError 直通
   ▼
RFC v2 修订
   │ 外部 GPT 专家介入审计，纠正快照重放与版本控制逻辑
   ▼
外部 GPT 审计规范输入 (GPT-AUDIT-RELIABILITY.md)
   │ 确立：阶段 A/B/C 串行、实时读 SQLite 态、独立 projection_revision、独立 Watchdog
   ▼
【第二轮流水线评审】── 拦截！20 项 Blocker (12 High / 8 Medium)
   │ 致命漏洞：既有表 UNIQUE 约束无法 ALTER、reconciliation 脏任务穿透、无 processing 认领未闭环
   ▼
RFC v4 修订
   │ 引入表重建迁移、无 processing 认领、memories.collection_name
   ▼
【第三轮流水线评审】── 拦截！20 项 Blocker (12 High / 8 Medium)
   │ 致命漏洞：新表删快照却要 legacy 重放、legacy 路由冲突、合并 DO UPDATE 丢失更新、测试触碰生产库
   ▼
RFC v5 最终架构方案 (当前版本)
   │ 核心决策：新队列 v3-only + legacy 队列冻结、CAS 认领、真实 schema 锚点、测试隔离白名单
   ▼
当前执行流 (dwfrun-9dfc0517 正在复审中)
```

---

## 4. 核心根因与机制缺陷剖析

1. **冷启动单飞机制缺失**: `src/core/engine.py` 内 `load_model()` 没有并发排队锁，25s 等待超时硬编码导致并发冷启动瞬间打崩。且 `idle_timeout_seconds: 0` 会被当成 `Timer(0)` 瞬间卸载。
2. **写路径同步双写制造假 OCC 冲突**: `memory_service.py:728-769` 在同一个方法内先通过 SQLite commit 递增版本，紧接着同步计算 Embedding 并写入 Qdrant。一旦 GPU 加载排队超时，客户端捕获到 500 异常发起重试，但此时数据库已是新版本，进而报出虚假的 `VERSION_CONFLICT`。
3. **队列表割裂与快照覆盖**:
   - 原先存在 `qdrant_sync_queue` 与 `memory_projection_outbox` 双表。
   - 依赖队列中存储的静态 `payload_snapshot` 进行异步回放，在并发重试、乱序、网络延迟下，旧任务会强行覆盖已提交的更新数据，甚至复活已删除卡片。
4. **错误码与链路追踪断裂**: 所有未捕获异常被简单包装为 `-32603 Internal error`，ContextVar 无法跨越持久队列和进程重启。

---

## 5. RFC v5 最终架构方案深度规格（核心重点）

RFC v5（完整文档位于 `docs/rfcs/RFC-AMR-RELIABILITY-FIXES.md`）严格按**阶段 A（写入一致性）→ 阶段 B（模型冷启动）→ 阶段 C（接口与观测）**顺序推进：

### 阶段 A：写入与投影一致性（最高优先级）

#### A1. 写入成功定义与零外部同步依赖
- `memory_create`、`memory_update`、`memory_delete` 必须且仅在同一 SQLite 事务内完成：
  1. `memories` 主表变更并递增 `projection_revision`；
  2. `memory_revisions` 版本记录（仅正文变更时递增 `version`）；
  3. 审计记录；
  4. 规范队列 `memory_projection_outbox` 任务合并插入；
  5. `idempotency_keys` 幂等快照落库。
- 事务提交后立即返回 `{"memory_id": "...", "version": 3, "projection_status": "pending"}`。
- **请求主路径彻底剔除** `engine.embed` 和 `qdrant.*` 调用（包括 `_create_v3_memory` 中的同步向量计算）。
- **`memory_get` SSOT + Best-effort 富化**: 只要 SQLite 有行，直接以 SQLite 为绝对权威返回；同时以短超时异步拉取 Qdrant 仅富化补充历史字段（`meta.tags`, `raw_messages`, `chunk_index` 等）。Qdrant 失败或超时字段置 null，`memory_get` 永远成功。

#### A2. 核心结构决策：新队列 v3-Only 与 Legacy 队列彻底冻结
针对第三轮评审揭露的“legacy 分片快照缺失与随机 Point ID 无法重算”矛盾，RFC v5 作出重大架构切分：
1. **规范队列 `memory_projection_outbox`（v3 专用）**:
   - 表结构全新重建，**不再包含 `payload_snapshot` 与 `op_type`**：
     ```sql
     CREATE TABLE memory_projection_outbox (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         target_collection TEXT NOT NULL DEFAULT 'ai_memory',
         memory_id TEXT NOT NULL,
         projection_revision INTEGER NOT NULL DEFAULT 0,
         request_id TEXT,
         projected_revision INTEGER,
         status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','dead_letter')),
         retry_count INTEGER NOT NULL DEFAULT 0,
         next_retry_at INTEGER,
         last_error TEXT,
         dead_letter_at INTEGER,
         created_at INTEGER NOT NULL,
         updated_at INTEGER NOT NULL
     );
     CREATE UNIQUE INDEX ux_outbox_pending_key ON memory_projection_outbox(target_collection, memory_id) WHERE status='pending';
     CREATE INDEX ix_outbox_status ON memory_projection_outbox(status, next_retry_at);
     ```
   - `memories` 表补充字段: `projection_revision INTEGER DEFAULT 0`, `collection_name TEXT DEFAULT 'ai_memory'`。
2. **合并入队 Helper (`ON CONFLICT DO UPDATE`)**:
   ```sql
   INSERT INTO memory_projection_outbox (target_collection, memory_id, projection_revision, request_id, status, retry_count, next_retry_at, created_at, updated_at)
   VALUES (?, ?, ?, ?, 'pending', 0, NULL, ?, ?)
   ON CONFLICT(target_collection, memory_id) WHERE status='pending' DO UPDATE
     SET projection_revision=excluded.projection_revision, request_id=excluded.request_id,
         retry_count=0, next_retry_at=NULL, updated_at=excluded.updated_at;
   ```
3. **Worker 投影算法与 CAS 认领协议（无 processing 假死状态）**:
   - **路由决策**: 任务来自新队列 → **只读 SQLite 最新完整状态**；任务来自老队列 → 维持旧版重放快照逻辑。
   - **v3 投影**:
     - SQLite 行存在且非 DELETED → 组装 **11 字段 Payload**（含 `superseded_by` 与 `scope`）→ 算 Embedding → Upsert Qdrant；
     - SQLite 行存在且 DELETED → 按 `qdrant_point_id` 删除 Qdrant Point；
     - SQLite 行已物理丢失 → 标 done 并记录 `memory row missing; skip`。
   - **CAS 状态推进**:
     ```sql
     UPDATE memory_projection_outbox 
     SET status='done', projected_revision=:claim_revision, updated_at=:now 
     WHERE id=:id AND status='pending' AND projection_revision=:claim_revision
     ```
     如果更新影响行数为 0，说明在 Worker 处理期间有新写请求合并更新了该任务，该任务自动保留在 `pending` 状态留待下轮处理，**绝不丢失最新写**！
4. **统一收编隐藏写者**:
   - `memory_service.memory_update_status` 改走 SQLite 写库 + 统一入队；
   - `session_store.update_memory_status` 状态大写规范化；
   - 对账引擎 `reconciliation/engine.py:218-242` 改为同事务递增 `projection_revision` 并调用统一入队 Helper。

#### A3. 幂等性控制（“写入即仲裁”模型）
针对过期键占死主键导致重复插入崩溃的问题，采用：
```sql
INSERT INTO idempotency_keys (agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at)
VALUES (?,?,?,?,?,?,?)
ON CONFLICT(agent_id_raw, op, idem_key) DO UPDATE
  SET request_digest=excluded.request_digest, memory_id=excluded.memory_id,
      response_json=excluded.response_json, created_at=excluded.created_at
  WHERE created_at < :cutoff; -- 仅过期行允许原子覆写
```
- `request_digest`: 按操作严格提取字段生成规范 JSON SHA256。**针对 create 操作，明确排除服务端自增生成的 memory_id**。
- 未过期冲突: 摘要一致重放快照；摘要不一致抛领域异常映射为 `-32006 IDEMPOTENCY_CONFLICT`。

---

### 阶段 B：模型冷启动（线程原语 + 独立 Watchdog）

1. **Loop-Agnostic Single-Flight 加载器**:
   - 禁止在 `engine.py` 内部使用 `asyncio.Future`。
   - 基于 `threading.Lock()`（`_load_exec_lock`）非阻塞 `acquire(blocking=False)` 竞争唯一加载权。
   - 加载线程在局部变量完成 `(tokenizer, model)` 构建，持锁校验 `_load_epoch` 有效性。
2. **独立后台 Watchdog**:
   - 独立守护线程基于 `_load_started_at` 计算超时，不依赖前台是否有请求在等待。
   - 超过 `load_timeout_seconds` (120s) → epoch+1，状态标记为 `ERROR` / `unhealthy`。
   - 超过 `load_stall_exit_seconds` (180s) → 记录 CRITICAL 致命日志，触发受控 `os._exit(1)`，由 systemd `Restart=always` 重新拉起干净进程（明确声明：epoch 不能暴力终止 Python 堵死线程）。
3. **禁用自动卸载语义确立**:
   - `idle_timeout_seconds <= 0` 明确为永不卸载，短路卸载定时器，并在 `embed()` 结束的 `finally` 中严格保持 `READY` 状态，不回退为 `IDLE`。

---

### 阶段 C：接口、状态与可观测性

1. **结构化 JSON-RPC 业务错误码**:
   - `-32001 AGENT_UNAUTHORIZED`
   - `-32002 VERSION_CONFLICT`（必须返回 `data: {memory_id, current_version, expected_version}`）
   - `-32003 MODEL_LOADING`（返回 `data: {model_state, retry_after_ms}`）
   - `-32004 QUEUE_FULL`
   - `-32005 NOT_FOUND`
   - `-32006 IDEMPOTENCY_CONFLICT`
   - `-32602 INVALID_ARGUMENT`
   - `-32603 INTERNAL`（兜底）
   - IPC 映射第一优先级为 `isinstance(exc, RpcError)` 直通，防止被底层宽泛的 `except ValueError` 吞掉。
2. **状态统一与重定向解耦**:
   - 6 态统一标准: `ACTIVE`, `HISTORICAL`, `DELETED`, `PENDING_VERIFY`, `CONFLICT`, `TEMPORARY`。
   - `memory_search` 重定向逻辑与 `status == 'SUPERSEDED'` 解耦，只要卡片具有 `superseded_by` 且目标有效即执行重定向追踪（上限 5 级防死循环）。
3. **可观测性与死信治理**:
   - request_id 服务端生成并持久化写入 SQLite 任务行，跨进程与崩溃可追踪。
   - 开放 Admin UDS RPC: `status.metrics`, `outbox.list_dead_letters`, `outbox.replay`（重放任务经入队 Helper 合并，原死信标记 absorbed，严防撞部分唯一索引）。
   - `scripts/amr_doctor.py` 诊断脚本，支持 `--json` 输出脱敏诊断。

---

## 6. 测试矩阵、隔离纪律与既有测试适配白名单

针对评审指出“全量测试门触碰生产环境与破坏既有断言”的严厉批评，RFC v5 制定了严格的测试隔离与适配白名单纪律：

### 6.1 绝对测试隔离铁律
- **禁止连接生产 Socket**: 绝不连接 `/run/user/1000/qdrant-bge.sock`。
- **禁止打开生产数据库**: 绝不打开 `data/sessions.db`。
- **测试环境隔离改造**:
  - `tests/test_v3_contracts.py:214` 的 `AMRApplication` 构造注入临时环境变量 `AMR_CONFIG_PATH` 指向 `/tmp` 临时 SQLite 与 mock Qdrant。
  - `tests/test_v2_end_to_end.py` 增加 `@pytest.mark.skipif(not os.environ.get("AMR_TEST_LIVE_DAEMON"))`，默认离线跳过。

### 6.2 既有测试适配白名单（严格限制）
仅允许在以下范围内适配期望字符串或异常类型，**严禁弱化断言逻辑或删除用例**：
1. `tests/test_step3_engine.py::test_loading_timeout`: 断言异常从 `ModelLoadingTimeoutError` 更新为 `ModelLoadingPendingError`。
2. `tests/test_reconciliation.py:153,157`: 状态期望大写化，断言适配新统一入队语义。
3. `tests/test_v2_engine.py:413,420`: 状态期望大写化。
4. `tests/test_step4_memory_service.py` 及 `test_step5_*`: 状态断言按 6 态大写归一适配，`memory_get` 字段按富化形态适配。

---

## 7. 已落盘的代码、契约与迁移脚本基座

分支 `fix/amr-reliability-v3` 当前已就绪的组件：
- **`docs/rfcs/GPT-AUDIT-RELIABILITY.md`**: GPT 外部审计原文。
- **`docs/rfcs/RFC-AMR-RELIABILITY-FIXES.md`**: RFC v5 完备设计规格（Commit `b690bee`）。
- **`src/core/errors.py`**: 跨层领域异常（继承 `ValueError` 保障兼容）。
- **`src/interfaces/ipc/errors.py`**: 包含 `-32006 IDEMPOTENCY_CONFLICT` 在内的标准 RPC 异常体系。
- **`src/core/request_context.py`**: 升级支持 `request_id` (服务端生成) 与 `client_ref` (客户端关联) 双字段上下文。
- **`src/service/metrics.py`**: 单进程无锁指标收集器。

---

## 8. 专供外部 GPT 深度审查的技术争议与风险清单

请 GPT 重点就以下关键架构抉择进行推演与严苛审查：

1. **新队列 v3-Only 与老队列冻结策略**:
   - 彻底切断 legacy `memory_record` 进入新队列，老队列仅保留单线程串行排水。这种做法在彻底规避字段丢失和随机 Point ID 无法重算的同时，是否会在老数据完全排干前遗留一致性隐患？
2. **CAS 认领协议 (`WHERE projection_revision=:claim_revision`)**:
   - 在高并发写入场景下，如果在 Worker 执行 Embedding + Qdrant 写入的 20ms 时间窗内，SQLite 发生该 memory 的新更新，Worker 的 CAS UPDATE 将影响 0 行，任务保留 pending。这种方式是否会引发极端情况下的活锁（Livelock），还是能平稳实现最终一致性？
3. **幂等性“写入即仲裁” (`ON CONFLICT DO UPDATE WHERE created_at < cutoff`)**:
   - 在 SQLite 单写锁串行机制下，利用此 SQL 一步完成“未过期重放读取”与“已过期覆写插入”，是否存在并发重试时的竞态盲区？
4. **独立 Watchdog 与受控进程退出**:
   - 针对 Python 线程在 C 扩展或 CUDA 调用中完全卡死的问题，方案放弃了无用的“线程强杀”，选择标记 unhealthy 并在超阈值后执行 `os._exit(1)` 由 systemd 恢复。该机制对于系统整体高可用性是否是最稳妥的下策方案？
5. **Memory Get 的 Best-effort 富化一致性**:
   - `memory_get` 以 SQLite SSOT 为主，异步降级读取 Qdrant 补全历史非结构化字段，当 Qdrant 短暂不可用时这些字段为 null。这种读体验是否满足实际多智能体协作的语义要求？

---
*(报告完毕，请 GPT 针对上述 RFC v5 决策开展独立审计)*

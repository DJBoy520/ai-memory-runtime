# RFC: AMR 可靠性修复 v3（外部审计后的实施规格）

- 状态: v3（2026-11-08，按外部审计意见全面改写；v1 曾被双模型评审拦截 22 条，v2 吸收后又被外部审计纠正关键设计）
- 规范性输入: `docs/rfcs/GPT-AUDIT-RELIABILITY.md`（审计原文，逐条对照本文件核查覆盖）
- 实施顺序（用户指定，硬性）: **阶段 A 写入一致性 → 阶段 B 模型加载 → 阶段 C 接口与观测**
- 影响面: 守护进程 `amr.service`；含增量 DDL 与可重复迁移脚本；**不触碰生产 `data/sessions.db` 与运行中服务**，迁移在数据库副本上预演，上线步骤单独成文
- 分支: `fix/amr-reliability-v3`

## 0. 与 v2 的关键差异（审计纠正项）

1. **队列任务语义反转**：队列行表达"某条记忆需要重新投影"，Worker 处理时**读取 SQLite 最新完整状态**执行完整 upsert/delete——**不复放 payload_snapshot**（v2 的快照重放存在旧覆盖新风险，审计 A2 否决）。
2. **projection_revision 独立于 version**：`version` 仅正文修改递增，metadata-only 更新不递增，不能充当投影版本。新增 `memories.projection_revision`，任何影响投影的变更（内容/状态/类型/冲突/集合）都在同事务递增。
3. **幂等增加请求摘要**：相同 key 不同参数 → `IDEMPOTENCY_CONFLICT`（新错误码 -32006），不再是简单重放；并发首请求靠唯一约束 + 败方回读校验。
4. **看门狗独立于等待者**：加载超时判定由后台 watchdog 独立完成，不依赖"仍有请求在等"；卡死线程**不能**被 epoch 终止——标记 unhealthy 后走受控进程退出，由 systemd `Restart=always` 恢复。
5. **request_id 持久化到投影任务行**：ContextVar 无法跨越持久队列与进程重启，队列行增加 `request_id` 关联列。
6. **双队列迁移独立成步**：先盘点生产者清单，迁移可重复、可恢复、不删历史行，数据库副本演练并输出前后对账。
7. **错误码往返验证**：必须通过真实 IPC（进程内 UDS Server）+ MCP 桥接往返测试，不只测异常继承。
8. **状态迁移先盘点后迁移**：迁移脚本先只读统计（`--inventory`），再可重复迁移（`--apply`），副本演练。

## 1. 阶段 A：写入与投影一致性（最高优先级）

### A1 写入成功语义

`memory_create / memory_update / memory_delete`（`src/service/memory_service.py` + `src/core/session_store.py`）同步路径在同一 SQLite 事务内完成：主表 + `memory_revisions` + 审计 + 投影任务 + 幂等记录及响应快照，**提交后立即返回**。请求路径零 `engine.embed`、零 `qdrant.*` 调用（`_create_v3_memory` 约 628-630 的同步向量计算一并删除）。响应含 `projection_status: "pending"`。`memory_get` 改为 SQLite SSOT 优先（`get_memory_v3` 命中即返回）；`memory_search` 保持查 Qdrant，弱一致性写入 `tools.py` 的工具描述与 `docs/DOC-AMR-04-API.md`。

### A2 规范队列与单写者

**第 0 步（强制，写入 deviations）**：盘点所有 Qdrant 写入口（v3 写路径、legacy `memory_record`、reconciliation、脚本、后台任务），形成生产者清单。未纳入统一顺序控制的入口必须在清单中标注。

**队列 DDL（在既有 `memory_projection_outbox` 上幂等升级；该表当前 0 行、无消费者，由 reconciliation 写入）**：

```sql
ALTER TABLE memory_projection_outbox ADD COLUMN target_collection TEXT;      -- 既有行回填 'ai_memory'
ALTER TABLE memory_projection_outbox ADD COLUMN projection_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE memory_projection_outbox ADD COLUMN request_id TEXT;             -- C3 跨重启关联
ALTER TABLE memory_projection_outbox ADD COLUMN projected_revision INTEGER;  -- done 时记录实际投影到的 revision
ALTER TABLE memory_projection_outbox ADD COLUMN task_class TEXT NOT NULL DEFAULT 'legacy'; -- 既有行=legacy
-- 既有列 version/projection_type/op_type/payload_snapshot 保留但标记 deprecated（reconciliation 兼容）
CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_pending_key
    ON memory_projection_outbox(target_collection, memory_id) WHERE status = 'pending';
ALTER TABLE memories ADD COLUMN projection_revision INTEGER NOT NULL DEFAULT 0;
```

**入队（合并语义）**：每次影响投影的写事务内，`INSERT ... ON CONFLICT(target_collection, memory_id) WHERE status='pending' DO UPDATE SET projection_revision=excluded.projection_revision, request_id=excluded.request_id, updated_at=...`——同键至多一条 pending，新变更只刷新 revision，不堆积任务。

**Worker 算法（`src/service/outbox_worker.py` 重写消费端）**：
1. **单实例防护**：启动时 `fcntl.flock` 独占 `data/outbox_worker.lock`，第二个 Worker 拒绝启动（第一版单 Worker；多 Worker 需租约+fencing，明确不在本次范围）。
2. 认领 pending 任务 → **以 (target_collection, memory_id) 为投影键，实时读取 SQLite 最新完整状态**：
   - `memories` 行存在且 `status != 'DELETED'` → 按当前内容/状态/元数据构造 payload，完整 upsert（向量现算，经 engine.embed）；
   - `memories` 行存在且 `status == 'DELETED'` → 按 `qdrant_point_id` 删除 point；
   - 行不存在 → 删除 point（防御性清理）。
   - **路由规则统一**：legacy 队列（`qdrant_sync_queue`）任务若其 memory 在 SQLite 有行，同样走上述"读最新态"路径；无行（legacy 孤儿卡）才沿用原 payload 重放。两队列由同一 Worker 线程串行消费，满足审计 A3.3"同一串行投影器"。
3. 成功后：任务标 `done` 并记录 `projected_revision`；随后**复查** `memories.projection_revision`——若大于已投影值（处理期间发生了新写入），靠合并索引保证的 pending 任务（或本轮重新入队）接管，不丢更新。
4. **至少一次投递 + 最终状态幂等**：Qdrant 写成功但标记 done 前崩溃 → 重试重做完整 upsert（幂等，point id 稳定）；绝不承诺网络恰好一次。
5. 重试退避 `min(2^retry_count, 60)s`（`next_retry_at`）；`retry_count >= 8` → `dead_letter_at` 置位、状态 `dead_letter`，可查询、可经 admin RPC `outbox.replay` 人工重放。绝不静默丢弃。
6. METRICS：`outbox_lag_seconds`（最老 pending 年龄）、积压/失败/死信计数、`embedding_duration_ms`、`qdrant_duration_ms`。

**审计特别检查矩阵（必须各有测试断言）**：upsert→metadata 更新（同 version 连续变更）、upsert→delete、同正文版本连续状态更新、不同 collection 同 memory、处理期间出现新写入。

### A3 双队列迁移（独立迁移步骤）

`scripts/migrate_projection_queue.py`（新建，A 轨交付）：
1. `--inventory`（只读）：统计 `qdrant_sync_queue` 各状态行数、全部生产者清单、`memory_projection_outbox` 现状。
2. `--apply`（可重复执行）：将旧队列 `pending/failed` 行映射为新队列 pending 任务（键 (回填 collection, memory_id)，revision 从 memories 现值取），旧行标 `migrated`——**不删除旧队列与历史行**；已 `synced` 行不动。
3. `--rehearse`（演练）：对**数据库副本**执行（`sqlite3 .backup` 到 tmp 或测试夹具构造），输出任务数量与前后一致性对账报告（JSON + 人读摘要）。
4. 失败恢复步骤写入脚本 `--help` 与上线手册；切换屏障 = 本次部署（v3 写路径切换入队目标即为屏障，legacy `memory_record` 生产者保留在旧队列由 Worker 继续排水）。
5. 测试旧任务延迟重试不覆盖新状态、不复活已删除记录。

### A4 幂等

`idempotency_keys` 表（业务写同事务写入）：

```sql
CREATE TABLE IF NOT EXISTS idempotency_keys (
    agent_id_raw   TEXT NOT NULL,     -- 客户端自声明命名空间，非认证凭据
    op             TEXT NOT NULL,     -- create | update | delete
    idem_key       TEXT NOT NULL,
    request_digest TEXT NOT NULL,     -- sha256(canonical_json(op, memory_id, content, type, status, expected_version, change_reason, collection_name, project_id, ...))
    memory_id      TEXT NOT NULL,
    response_json  TEXT NOT NULL,     -- 完整首次响应快照（含 projection_status、服务端生成的 memory_id）
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (agent_id_raw, op, idem_key)
);
```

规则：同 key 同摘要 → 事务内重放（附 `idempotent_replay: true`，pending 含义=首次提交时状态，当前投影状态用独立查询）；同 key 不同摘要 → `IDEMPOTENCY_CONFLICT`(-32006)；并发首请求 → 唯一约束仲裁，败方回读并校验摘要一致后重放；create 的服务端 ID 在事务内生成并进快照；保留 7 天（`idempotency_retention_days`），过期视为不存在重新执行（文档说明）。`tools.py` 补 `idempotency_key` 参数与契约说明。

**阶段 A 交付文件**：
- A写轨（deepseek）：`src/service/memory_service.py`、`src/core/session_store.py`、`src/interfaces/mcp/tools.py`、`tests/test_outbox_single_writer.py`、`tests/test_idempotency.py`、`tests/test_memory_get_ssot.py`
- A投影轨（gemini）：`src/service/outbox_worker.py`、`scripts/migrate_projection_queue.py`、`tests/test_projection_ordering.py`、`tests/test_dual_queue_migration.py`

## 2. 阶段 B：模型冷启动（线程原语 + 独立看门狗）

`src/core/engine.py`（B 轨，gemini）：

1. **跨线程/跨 Loop 单次加载**：`_load_exec_lock = threading.Lock()` 非阻塞 acquire 定唯一加载者（替代 v1 的 asyncio.Future——engine 被 main loop 与 Worker 独立 loop 共享）；加载线程在**局部变量**构造 (tokenizer, model)，完成后持 `_state_lock` 校验 `_load_epoch` 仍有效才挂载，过期则 `del` + `torch.cuda.empty_cache()` 丢弃；`_loader_active`/`_load_started_at` 受锁保护。等待方任意 loop 以 50ms 轮询状态。
2. **状态转换矩阵（逐一测试）**：加载中、等待、失败、卸载、关闭（daemon stop 与加载并发）、**加载完成但结果已过期**（epoch 已推进→丢弃且不动状态）。
3. **请求预算与加载生命周期分离**：请求等满 `request_load_wait_seconds` → `ModelLoadingPendingError`（映射 MODEL_LOADING 可重试），绝不取消共享加载。
4. **独立 watchdog**：守护线程按 `_load_started_at` 独立判定，不依赖等待请求存在：
   - 超过 `load_timeout_seconds` → 判定本次尝试失败：epoch+1、状态 ERROR（此后完成线程丢弃产物）；若加载线程仍卡住，状态标 **unhealthy**（admin 状态与 RPC 错误可见）；
   - 超过 `load_stall_exit_seconds`（新增配置，默认 `load_timeout_seconds + 60`）→ journald CRITICAL 后 `os._exit(1)` 受控退出，由 systemd `Restart=always`（`systemd/amr.service:11-12` 已配）拉起恢复。**明确：epoch 不能终止卡住线程，文档不得如此描述。**
5. `idle_timeout_seconds <= 0` = 禁用自动卸载：`_schedule_idle_unload` 入口短路 + `embed()` finally 不降 READY→IDLE。
6. 新配置：`preload_on_start`（false）、`load_timeout_seconds`（120）、`request_load_wait_seconds`（30）、`load_stall_exit_seconds`；本机 `config/config.yaml` 设 `preload_on_start: true`、`idle_timeout_seconds: 0`；记录实际加载耗时到 METRICS（`model_load_duration_ms`/`model_loads_total`）。
7. 插件 80–300ms fail-open 契约零改动（B3）：服务端在模型未就绪时返回 MODEL_LOADING 并继续后台加载。
8. 既有 `tests/test_step3_engine.py`（手工置 LOADING 的用例）必须保持通过：LOADING 但无活动加载者时，新请求可成为加载者。

**交付文件（B 轨）**：`src/core/engine.py`、`config/settings.py`、`config/config.yaml`、`config/config.example.yaml`、`tests/test_engine_singleflight.py`、`tests/test_engine_lifecycle.py`、`tests/test_idle_timeout_disabled.py`

## 3. 阶段 C：接口、状态与可观测性

### C1 稳定业务错误（C 接口轨，gemini）

映射表（`src/interfaces/ipc/errors.py` 已有基座，增补 `RpcIdempotencyConflict` code=-32006）：AGENT_UNAUTHORIZED(-32001)、VERSION_CONFLICT(-32002，data 带 current_version)、MODEL_LOADING(-32003)、QUEUE_FULL(-32004)、NOT_FOUND(-32005)、INVALID_ARGUMENT(-32602)、**IDEMPOTENCY_CONFLICT(-32006)**、INTERNAL(-32603 兜底)。

- `server.py` 映射首位 `isinstance(exc, RpcError)` 直通；随后领域异常→码；其余 ValueError 消息前缀解析兜底。
- **审计所有 `except ValueError`**（grep src/）：领域异常现继承 ValueError，宽泛捕获会提前吞掉——逐处核对处理顺序与日志，发现吞掉处修复并在 deviations 报告清单。
- **真实往返测试**（`tests/test_rpc_roundtrip.py`）：进程内 `DualUDSServer` 绑 tmp socket（禁触生产 daemon/生产 socket）+ `bridge.py` 以 `AMR_SOCKET_PATH` env 指向 tmp socket 跑子进程，断言 AGENT_UNAUTHORIZED/VERSION_CONFLICT(含 current_version)/IDEMPOTENCY_CONFLICT/MODEL_LOADING 端到端可区分。
- `main.py` dispatch 改用 `auth.authorize_agent`（RPC 规则见 RFC v2 D6：精确命中→(raw,None)；`<type>-<instance>` 且 type 命中→(type,instance)；否则未授权）。

### C2 状态统一（C 状态轨，deepseek）

- `src/core/memory_status.py`：6 态 + `normalize_status`（legacy SUPERSEDED/ARCHIVED→HISTORICAL，保留 `superseded_by` 指针）。
- **先盘点后迁移**：`scripts/migrate_status_enum.py --inventory`（只读统计 SQLite/Qdrant payload 样本/legacy 接口/reconciliation 现有语义，输出报告）→ `--apply`（可重复，先 SQLite 后 Qdrant payload，默认 dry-run）。
- 重定向修正（`memory_service.py:231-234,205-216`）：条件从 `status=="SUPERSEDED"` 改为 **`superseded_by` 非空即重定向**，且重定向前校验：目标存在、非 DELETED、可见性与作用域；链环深度上限 5 + 环检测；目标不存在→返回原记录并标注 `redirect_broken`；跨项目/跨 collection 行为写入文档（同 collection 内重定向，跨集合仅提示）。
- 写库真相源 `session_store.update_memory_status`（现落小写 superseded）接 normalize；`memory_service.memory_update_status`（只写 Qdrant）补规范队列入队 + normalize，消除分叉。
- reconciliation / cognitive_engine 等越界文件只盘点不改（deviations 报告）。

### C3 可观测性（C 状态轨）

- `request_id`：server 每请求生成（服务端生成为主，客户端 ID 仅关联字段）；**持久化到队列行 `request_id` 列**（A 轨已建列，此处确保写入与 doctor 呈现）——ContextVar 只覆盖进程内同请求，跨持久队列/重启靠落库。
- `main.py`：logging.Filter 注入 request_id、preload 接线（`preload_on_start`）、admin `status.metrics`（METRICS + 引擎状态 + 队列积压/最老任务年龄/失败/死信）。
- `scripts/amr_doctor.py`：服务/模型/SQLite/Qdrant/队列/近 24h journald 分类错误；**输出脱敏：不含记忆正文与凭据**；`--json`；对未升级 daemon 优雅降级。

**交付文件（C 轨）**：
- C接口轨（gemini）：`src/interfaces/ipc/server.py`、`src/interfaces/ipc/auth.py`、`src/interfaces/mcp/bridge.py`、`tests/test_rpc_errors.py`、`tests/test_auth_normalization.py`、`tests/test_rpc_roundtrip.py`
- C状态轨（deepseek）：`src/core/memory_status.py`、`src/service/memory_service.py`、`src/core/session_store.py`、`src/main.py`、`scripts/amr_doctor.py`、`scripts/migrate_status_enum.py`、`tests/test_status_unification.py`、`tests/test_admin_metrics.py`

## 4. 文件归属与阶段纪律

- 阶段串行：A 全部完成并通过定向测试门后，B 才开工；C 在 B 之后。同阶段内两轨文件互斥（上表）。
- 越界禁改：共享契约（`src/core/errors.py`、`src/core/request_context.py`、`src/service/metrics.py`、`src/interfaces/ipc/errors.py`）编码轨只读。
- 测试隔离硬约束：一切测试用 tmp_path 内存 SQLite / 临时 socket / fake engine+qdrant；**禁止打开生产 `data/sessions.db`、连接生产 Qdrant、触碰运行中 daemon**。
- 插件目录（`plugins/`）与 `memory_record` legacy 路径零改动。

## 5. 交付验收 ↔ 测试映射（审计"五、交付验收"10 条）

| # | 审计验收 | 测试/产物 |
| --- | --- | --- |
| 1 | 并发冷启动只加载一次；请求超时不取消共享加载 | `tests/test_engine_singleflight.py` |
| 2 | 卡死→unhealthy+受控退出、迟到完成、卸载/关闭竞态确定行为 | `tests/test_engine_lifecycle.py` |
| 3 | SQLite 提交后 Qdrant 不可用，写请求仍返回已提交+pending | `tests/test_outbox_single_writer.py` |
| 4 | 同幂等键并发重试单次提交；不同参数同 key → 冲突 | `tests/test_idempotency.py` |
| 5 | 投影乱序/元数据连续更新/删除后旧任务重试/Qdrant 成功后崩溃恢复 | `tests/test_projection_ordering.py` |
| 6 | pending 期 memory_get 读到最新事实；search 弱一致性写入文档 | `tests/test_memory_get_ssot.py` + `tools.py`/`DOC-AMR-04` 说明 |
| 7 | 新旧队列并存最终一致 | `tests/test_dual_queue_migration.py` |
| 8 | IPC/MCP 往返可区分错误码；插件预算不变 | `tests/test_rpc_roundtrip.py` + 既有插件测试 |
| 9 | 副本迁移演练 + 前后对账 | `scripts/migrate_projection_queue.py --rehearse` 输出报告 |
| 10 | 上线顺序/重启步骤/回退条件 | `docs/ROLLOUT-AMR-RELIABILITY.md`（预演轨产出） |

全量门：`python3 -m pytest tests/ -q`（deselect 基线既有失败 `test_v2_end_to_end` E2E 延迟用例），不新增失败。

## 6. 已知风险

- Worker 改为"读最新态"后，单次任务成本略增（实时读 + 现算向量），但消除快照陈旧类缺陷；合并索引防任务堆积。
- `os._exit(1)` 受控退出会瞬断在途请求（MODEL_LOADING/连接重置），属卡死场景的兜底，常态不触发。
- `bridge.py` 错误文本格式变化（首行 `[error_code]` 前缀）对旧调用方可感知。
- 幂等键空间按 raw agent_id：实例随机化后缀的 Agent 键不复用（安全方向的失败）。

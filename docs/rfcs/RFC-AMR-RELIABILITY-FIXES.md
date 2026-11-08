# RFC: AMR 可靠性修复 v4（第二轮评审修订稿，外部审计规范不变）

- 状态: v4（2026-11-08，吸收第二轮双模型评审 20 条 blocker；其中队列 DDL/约束类断言经临时 SQLite 实测复现确认）
- 规范性输入: `docs/rfcs/GPT-AUDIT-RELIABILITY.md`（审计原文，要求不变）
- 实施顺序（用户指定，硬性）: **阶段 A 写入一致性 → 阶段 B 模型加载 → 阶段 C 接口与观测**
- 影响面: 守护进程 `amr.service`；含**表重建迁移**与增量 DDL；不触碰生产库与运行中服务
- 分支: `fix/amr-reliability-v3`

## 0. v4 变更摘要（第二轮评审 20 条 → 处置）

| # | 评审发现（证据） | v4 处置 |
| --- | --- | --- |
| 1,5 | `memory_projection_outbox` 既有 `UNIQUE(memory_id, version, projection_type, op_type)`（session_store.py:186,421）无法 ALTER 移除；op_type NOT NULL 而合并 INSERT 未提供——同 version 二次入队必 IntegrityError（评审用 SQLite 3.45.1 实测复现） | §A2 改为**表重建迁移**：新建目标 schema 表→映射拷贝→DROP→RENAME；合并 INSERT 列清单完整定义 |
| 2,8 | reconciliation Phase 3（engine.py:218-242）以旧格式直插队列、不递增 revision、状态小写，是隐藏生产者；"只盘点不改"会放进脏任务 | §A2 reconciliation 写路径**纳入阶段 A 修复**（改用统一入队 helper + 同事务递增 revision + 大写状态）；"只盘点不改"收缩到 cognitive_engine |
| 3 | 幂等表无过期过滤/清理，7 天保留期失效 | §A4 查询带 `created_at >= now-retention`；Worker 每小时清理过期行 |
| 4,13 | `memory_update_status` 直写 Qdrant、`session_store.update_memory_status` 落小写状态——阶段 A 部署后 Worker 会复活已删除记录（违反审计 A3.5） | 两处写库修复**前移到阶段 A**（§A2.7）；C2 只保留检索重定向与迁移脚本 |
| 6 | target_collection 可空列 + NULL 不被部分唯一索引去重（实测两条 NULL pending 并存） | 重建 schema 中 `target_collection TEXT NOT NULL DEFAULT 'ai_memory'`；迁移前现存 pending 去重 |
| 7 | Worker 认领/崩溃恢复未定义；合并写入会改写 pending 行 revision，Worker 若记录改写后值则复查失效 | §A2.4 定义**无 processing 状态的认领协议**：claim_revision 取自 memories 读取，done 条件更新，事后复查 revision 并补队 |
| 9,12 | legacy memory_record 字段只存在于 payload（SQLite 未存全），读最新态重建会丢字段；行不存在时"删除 vs 重放"自相矛盾 | §A2.3 路由优先级定死：**task_class=v3 → 读最新态（行缺失/DELETED→删 point）；task_class=legacy → 永远重放 payload_snapshot**（保住 legacy 孤儿卡） |
| 10 | memories 表无 collection 归属列，任务 done 后关联丢失，非默认集合无法更新/删除 | memories 新增 `collection_name`（默认 'ai_memory'），写路径落库，投影键以库内值为准 |
| 11 | create 的 memory_id 服务端随机生成，进 digest 会把重试误判为冲突；canonical_json 未定义 | §A4 按操作定义 digest 字段清单（create 排除服务端 ID）+ canonical_json 规则 |
| 14 | memory_get 改 SSOT 会丢 legacy 字段（meta.tags 等），既有 test_step4 断言失败 | §A1 memory_get = SQLite SSOT + **payload 非权威富化**（legacy 字段从 Qdrant payload 合并）；无行卡片走 legacy 路径；测试适配清单见 §7 |
| 15 | qdrant_sync_queue CHECK 只允许 pending/synced/failed（实测），无 'migrated'；无退避/死信列 | §A3 迁移改标 **'synced' + last_error 注记**；legacy 队列不建死信（已知限制，新队列承担） |
| 16 | 手工置 LOADING 的既有用例与"无加载者时成为加载者"冲突 | §B2 取消该分支：手工置 LOADING = **等待者语义**（自旋至 READY/ERROR/预算超时）；test_loading_timeout 断言更新为 ModelLoadingPendingError（§7 清单） |
| 17 | 死信"可人工重放"无实现归属与测试 | §C3 C 状态轨交付 admin RPC `outbox.replay` / `outbox.list_dead_letters` + 测试 |
| 18 | 状态归一会破坏断言小写四态的既有测试（step4/step5 三文件） | §7 测试适配清单：仅状态字符串期望值按映射更新，语义断言不得弱化 |
| 19 | legacy 队列无 request_id 列；request_context 现为 JSON-RPC id 优先，与 C3 冲突 | §C3 服务端生成为主 + client_ref 关联（request_context.py 已由编排方修订）；legacy 队列行 request_id=NULL（记录为已知限制） |
| 20 | VERSION_CONFLICT 结构化 data 无交付归属 | §A2.8 A 写轨交付：`update_memory_v3` 改抛 `VersionConflictError(memory_id, current_version, expected_version)`（ValueError 子类，既有断言不变） |

## 1. 阶段 A：写入与投影一致性（最高优先级）

### A1 写入成功语义

同 v3：create/update/delete 同步路径单事务完成主表+revisions+审计+投影任务+幂等快照，提交即返回 `projection_status:"pending"`；请求路径零 embed、零 qdrant 调用。`memory_get`（`memory_service.py:479-533` 改造）：
- SQLite `memories` 有行 → **SQLite SSOT 为准**（memory_id/version/content/status/type/project_id 等以库内值为权威），response 保留 legacy 别名字段（`memory_type`=type 等），并以 Qdrant payload 做**非权威富化**（仅补 SQLite 不存的 legacy 字段：`meta.tags`、`parent_memory_id`、`chunk_index`、`total_chunks`、`session_id`、`source_message_ids`，payload 缺失则置 null）；
- SQLite 无行（legacy 孤儿卡）→ 维持现 Qdrant 读取路径不变。

### A2 规范队列与单写者（v4 核心修订）

**第 0 步不变**：盘点全部 Qdrant 写入口形成生产者清单进 deviations。

**1) 目标 schema（表重建，单一事实来源；A写轨的 `_ensure_schema` 对新库直接建此结构，A投影轨的迁移脚本对存量库重建）**：

```sql
CREATE TABLE memory_projection_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_collection TEXT NOT NULL DEFAULT 'ai_memory',
    memory_id TEXT NOT NULL,
    projection_revision INTEGER NOT NULL DEFAULT 0,
    request_id TEXT,                    -- 关联原始写请求（跨重启持久）
    projected_revision INTEGER,         -- done 时记录实际投影到的 revision
    task_class TEXT NOT NULL DEFAULT 'legacy',  -- v3 | legacy
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
-- 旧列 version/projection_type/op_type/payload_snapshot 废弃不迁移（reconciliation 同步改造后无生产者）

ALTER TABLE memories ADD COLUMN projection_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE memories ADD COLUMN collection_name TEXT NOT NULL DEFAULT 'ai_memory';  -- 存量默认 ai_memory
```

**2) 统一入队 helper（session_store，所有生产者必须走它）**：事务内 `INSERT INTO memory_projection_outbox (target_collection, memory_id, projection_revision, request_id, task_class, status, retry_count, next_retry_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'v3', 'pending', 0, NULL, ?, ?) ON CONFLICT(target_collection, memory_id) WHERE status='pending' DO UPDATE SET projection_revision=excluded.projection_revision, request_id=excluded.request_id, updated_at=excluded.updated_at`。`target_collection` 一律取 **`memories.collection_name`**（create 时把入参 collection_name 落库；update/delete 不再信任调用方参数）。

**3) 路由优先级（定死，消除 v3 矛盾）**：
- `task_class='v3'`（新队列行）：memories 行存在且 `status.upper()!='DELETED'` → 读最新完整状态构造 payload（含 9 字段）→ embed → upsert；行存在但 DELETED → 按 `qdrant_point_id` 删 point；行不存在 → 删 point（SSOT 防御清理）。
- `task_class='legacy'`（自 qdrant_sync_queue 迁移而来）：**永远重放 payload_snapshot**，不论 SQLite 行是否存在（保住 legacy 孤儿卡与其独有字段 tags/session_id/source_message_ids/parent_memory_id/chunk_index/total_chunks）。

**4) 认领协议（无 processing 状态，崩溃安全）**：
```
处理任务时：
  在同一读事务中取得 memories 当前状态与当前 projection_revision → 记为 claim_revision（不信队列行上的值，
  因为合并 upsert 会在处理期间改写它）
  按 §A2.3 投影（幂等：point id 稳定，重复 upsert 结果一致 → 至少一次投递 + 最终状态幂等）
  UPDATE outbox SET status='done', projected_revision=:claim_revision, updated_at=:now
    WHERE id=:id AND status='pending'
  复查：SELECT projection_revision FROM memories WHERE memory_id=?
    若 > claim_revision → 重新合并入队（helper 幂等，承接处理期间的新写入）
```
进程在标记 done 前崩溃 → 行仍 pending → 下轮重做（安全）；标记 done 后崩溃 → 已投影完成，无丢失。

**5) 重试与死信（仅新队列）**：失败 `next_retry_at = now + min(2^retry_count, 60)s`；`retry_count>=8` → status='dead_letter' + dead_letter_at；可查询（`outbox.list_dead_letters`）可人工重放（`outbox.replay`，见 §C3）。绝不静默丢弃。legacy 队列维持现状（retry_count>=5 停拉为已知限制，文档记录）。

**6) 单实例防护**：`fcntl.flock` 独占 lock 文件（测试注入 tmp 路径）。

**7) 阶段 A 必须一并修复的两个隐藏写者（否则单写者门禁不成立、且会复活已删记录）**：
- `memory_service.memory_update_status`（535-581，现直写 Qdrant payload）：改为 SQLite 写库（走 update_memory_v3 元数据路径或专用 store 函数）+ 统一入队 helper（update_payload 语义由 v3 读最新态重投影天然覆盖）；
- `session_store.update_memory_status`（1004-1057，现落小写 superseded/deleted）：状态经 normalize 为大写规范值；
- `src/reconciliation/engine.py:218-242`：状态更新改经 normalize + 同事务递增 `memories.projection_revision`，队列入队改走统一 helper（废除旧格式 INSERT OR IGNORE）。

**8) `update_memory_v3` VERSION_CONFLICT 改抛 `core/errors.VersionConflictError(memory_id, current_version, expected_version)`**（消息保留 `VERSION_CONFLICT: ...` 前缀；ValueError 子类，`tests/test_v3_contracts.py` 既有断言不变）。

**9) Worker 状态比较一律 `status.upper()`**，双保险防大小写复活已删记录。

### A3 双队列迁移（v4 修订）

`scripts/migrate_projection_queue.py`：
1. `--inventory`（只读）：旧队列各状态行数、生产者清单、新队列现状、reconciliation 兼容性核对。
2. `--apply`（可重复）：
   - 若新 schema 未就绪 → **表重建**（建 `_new` 表 → 按映射拷贝现存行（现存 0 行；如有 pending 先按 (collection, memory_id) 去重）→ DROP 旧 → RENAME）+ memories 两列 ALTER（幂等判断列存在性）；
   - 旧队列 `pending/failed` 行 → 生成新队列任务：SQLite 有行 → task_class='v3'（revision 取 memories 现值）；无行 → task_class='legacy'（payload 从旧行 snapshot 拷贝语义由 Worker 重放路径承接）；
   - 旧队列已处理行**标 'synced' 并在 last_error 注记 "migrated: outbox#id"**（CHECK 约束只允许 pending/synced/failed，不得使用 'migrated'）；绝不删除旧行；
   - 幂等：重复执行跳过已完成步骤（以 schema/数据状态判断）。
3. `--rehearse`：对数据库副本（sqlite3 backup API 到 tmp）执行全流程，输出前后对账 JSON+摘要。
4. 失败恢复步骤写入 `--help` 与上线手册；切换屏障 = 本次部署（生产者代码与迁移同批生效）。
5. 测试：旧任务延迟重试不覆盖新状态、不复活已删除记录；新 queue CHECK/唯一索引行为断言。

### A4 幂等（v4 修订）

表结构同 v3（PK `(agent_id_raw, op, idem_key)`），新增：
- **请求摘要按操作定义**（canonical_json = `json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',',':'))`）：
  - create：`{op, content, type, status, project_id, collection_name, source_refs(排序), conflicts_with(排序)}`——**排除服务端生成的 memory_id**；
  - update：`{op, memory_id, content, type, status, expected_version, change_reason, conflicts_with}`（None 字段显式置 null 参与摘要）；
  - delete：`{op, memory_id, reason}`。
- 同 key 同摘要 → 事务内重放（附 `idempotent_replay:true`）；同 key 异摘要 → 领域异常携带 `{op, idem_key, existing_memory_id}`（IPC 映射 -32006 IDEMPOTENCY_CONFLICT）；并发首请求 → PK 仲裁，败方回读并校验摘要一致后重放，摘要不一致按冲突处理。
- **保留期过滤**：查询条件带 `created_at >= now - retention*86400`（默认 7 天，`idempotency_retention_days`）；Worker 每小时 `DELETE` 过期行；过期后同 key 重新执行（文档说明）。
- 快照含完整首次响应（含服务端生成 memory_id 与 projection_status）；`tools.py` 契约说明同 v3。

**阶段 A 交付文件（v4）**：
- A写轨（deepseek）：`src/service/memory_service.py`、`src/core/session_store.py`、`src/reconciliation/engine.py`、`src/interfaces/mcp/tools.py`、`tests/test_outbox_single_writer.py`、`tests/test_idempotency.py`、`tests/test_memory_get_ssot.py`
- A投影轨（gemini）：`src/service/outbox_worker.py`、`scripts/migrate_projection_queue.py`、`tests/test_projection_ordering.py`、`tests/test_dual_queue_migration.py`

## 2. 阶段 B：模型冷启动（v4 修订）

同 v3 线程原语方案（执行锁 + epoch + 局部构造 + 过期丢弃），以下为 v4 修正：
1. **取消"LOADING 无加载者时成为加载者"分支**：状态 LOADING 时请求一律**等待者语义**（50ms 自旋至 READY/ERROR/预算超时）——与既有手工置 LOADING 用例（test_loading_spin_queue_success：另一任务置 READY 后自旋成功）天然兼容。
2. 请求预算超时抛 `ModelLoadingPendingError`（映射 MODEL_LOADING）；**`tests/test_step3_engine.py::test_loading_timeout` 的断言允许更新为该异常类型**（§7 清单，语义不变：预算到点报可重试错误）。
3. watchdog/受控退出/idle<=0/配置/度量同 v3（watchdog 独立于等待者；`load_stall_exit_seconds` 后 CRITICAL + `os._exit(1)` 交 systemd 重启；epoch 不声称能杀线程）。

**交付文件（B 轨）**：同 v3（engine.py、settings.py、config 两份 yaml、三个新测试文件）。

## 3. 阶段 C：接口、状态与可观测性（v4 修订）

### C1 稳定业务错误（C 接口轨，gemini）
- 映射首位 RpcError 直通；VERSION_CONFLICT 的 data 现由 A 轨的结构化异常提供（memory_id/expected/current），映射代码取异常字段；IDEMPOTENCY_CONFLICT(-32006) 同理；其余同 v3。
- `except ValueError` 全面审计（清单进 deviations）。
- 真实往返测试（进程内 UDS + bridge 子进程，tmp socket）覆盖 AGENT_UNAUTHORIZED / VERSION_CONFLICT(含 current_version) / IDEMPOTENCY_CONFLICT / MODEL_LOADING。

### C2 状态统一（C 状态轨，deepseek）—— 写库部分已前移至 A
- `memory_search` 重定向：条件改 `superseded_by` 非空（与 status 解耦），重定向前校验目标存在/非 DELETED/可见性作用域；链深上限 5 + 环检测；目标缺失返回原记录标 `redirect_broken`；跨 collection 仅提示不跨跳。
- `scripts/migrate_status_enum.py`：先 `--inventory` 只读盘点（SQLite/Qdrant payload 样本/legacy/reconciliation/cognitive_engine 语义），再 `--apply`（默认 dry-run，可重复）；cognitive_engine 只盘点不改。

### C3 可观测性（C 状态轨）
- request_id **服务端生成为主**（uuid），客户端标识作为 `client_ref` 关联字段（request_context.py 已由编排方修订为双字段）；server 每请求 set/finally reset。
- 队列行 `request_id` 持久化（A 轨建列、入队写入）；legacy 队列无该列，行值 NULL（已知限制，文档记录）。
- `main.py`：logging.Filter（request_id + client_ref）、preload 接线、dispatch 改 auth.authorize_agent、admin `status.metrics`。
- **admin RPC 补齐（审计 A2 落地）**：`outbox.list_dead_letters`（查询死信：id/memory_id/collection/revision/retry_count/last_error/age）、`outbox.replay`（死信→pending、retry_count 清零，返回重放数）；测试覆盖。
- `scripts/amr_doctor.py`：服务/模型/SQLite/Qdrant/队列（积压/最老任务年龄/失败/死信）/近 24h journald 分类错误；`--json`；输出脱敏（不含正文与凭据）；对未升级 daemon 优雅降级。

**阶段 C 交付文件（v4）**：
- C接口轨（gemini）：`src/interfaces/ipc/server.py`、`src/interfaces/ipc/auth.py`、`src/interfaces/mcp/bridge.py`、`tests/test_rpc_errors.py`、`tests/test_auth_normalization.py`、`tests/test_rpc_roundtrip.py`
- C状态轨（deepseek）：`src/core/memory_status.py`、`src/service/memory_service.py`（仅 search 重定向）、`src/main.py`、`scripts/amr_doctor.py`、`scripts/migrate_status_enum.py`、`tests/test_status_unification.py`、`tests/test_admin_metrics.py`

## 4. 文件归属与阶段纪律

同 v3，外加：A 写轨新增 `src/reconciliation/engine.py`；C2 不再改 `session_store.py`（A 已完成大小写规范化）。阶段串行 A→B→C；同阶段内两轨互斥；共享契约四文件编码轨只读；测试隔离硬约束不变（tmp 内存库/临时 socket/fake 依赖；禁生产库、生产 Qdrant、运行中 daemon）；插件目录与 legacy `memory_record` 路径零改动。

## 5. 交付验收 ↔ 测试映射

同 v3 十条映射，补充：
- #4 幂等：并发首请求唯一约束仲裁 + 过期 key 重新执行用例（test_idempotency.py）。
- #5 投影：增加"claim 后崩溃恢复（done 前杀 Worker 模拟）"与"处理期间新写入不丢失（claim_revision 复查）"用例（test_projection_ordering.py）。
- #7 迁移：增加"同 version 重复入队不炸唯一约束（重建后）"、"NULL collection 去重"、"旧队列行标 synced"断言（test_dual_queue_migration.py）。
- #8 往返：死信查询/重放经 admin RPC 用例（test_admin_metrics.py）。

## 6. 已知风险

同 v3，另加：
- 表重建迁移在部署窗口内短暂锁表（现有表 0 行，实际开销可忽略；预演报告验证）。
- legacy 队列无退避/死信/request_id（记录为已知限制，随 memory_record 废弃一并退场）。

## 7. 测试适配清单（允许的既有测试修改，逐条登记 deviations）

| 文件 | 允许的修改 | 禁止 |
| --- | --- | --- |
| `tests/test_step3_engine.py::test_loading_timeout` | 期望异常 `ModelLoadingTimeoutError` → `ModelLoadingPendingError`（B 轨） | 改其他用例语义 |
| `tests/test_step4_memory_service.py`、`tests/test_step5_extra_contracts.py`、`tests/test_step5_comprehensive.py` | 状态字符串期望按 normalize 映射更新（小写→大写规范值）；memory_get 响应字段按 §A1 合并形态适配（A/C 轨按归属） | 弱化语义断言、删除用例、放宽超时/并发断言 |

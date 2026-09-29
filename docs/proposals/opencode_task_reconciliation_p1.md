# 任务书：AMR 多 Agent 记忆一致性对账（第一阶段编码实施）

- **执行方**: OpenCode (编码执行专家)
- **验收方**: Hermes (方案架构与验收)
- **技术依据**: `/home/dj/WorkSpaces/ai-memory-runtime/docs/proposals/RFC-003_daily_memory_reconciliation_spec.md` (v1.0.1 终审冻结版)
- **工作目录**: `/home/dj/WorkSpaces/ai-memory-runtime`

---

## 一、 任务目标与实施边界

本任务为 RFC-003 的第一阶段：**基础 Schema 迁移、Projection Outbox 机制、反向补登脚本与 Reconciliation 核心驱动框架**。

### 1. 核心铁律（必须严格遵守）
- **Invariant-01 (SSOT 唯一性)**: SQLite 是唯一事实来源。严禁 Qdrant 失败反向回滚已提交的 SQLite 事务。
- **状态字段归一**: 严禁新增 `curation_status` 或 `is_tombstone`。系统统一复用 `memories.status`（`active` / `stale` / `archived` / `deleted`）。
- **Chunky Commit**: 批量对账提交严格控制每 200 条提交一次并 `sleep(0.02)`，严禁长时间大事务锁表。
- **配置驱动**: 凭据从 AMR 配置文件中的 `api_key_file` 读取，严禁硬编码，零环境变量。

---

## 二、 具体执行子任务清单

### 子任务 1：SQLite Schema 增量迁移升级
在 `src/core/session_store.py` 中增加自动迁移逻辑：
1. 对 `memories` 表无损追加字段（若不存在）：
   - `content_hash TEXT`
   - `curation_batch_id TEXT`
   - `last_reconciled_at INTEGER`
   - 补充索引：`idx_memories_status`、`idx_memories_content_hash`、`idx_memories_reconciled`。
2. 创建 `memory_projection_outbox` 表：
   - 字段：`id, memory_id, version, projection_type, op_type, payload_snapshot, status, retry_count, next_retry_at, last_error, dead_letter_at, created_at, updated_at`。
   - 约束：`UNIQUE(memory_id, version, projection_type, op_type)`。
   - 索引：`idx_outbox_queue(status, next_retry_at)`。
3. 创建 `curation_candidates` 隔离表（字段符合 RFC-003 第 4 节）。
4. 创建 `reconciliation_checkpoints` 表（字段符合 RFC-003 第 4 节，含 `embedding_model`、`embedding_dimension`、`distance_metric` 等 Provenance）。

### 子任务 2：反向补登脚本（纳入 71 条 Qdrant 孤儿点位）
编写独立数据修复迁移脚本 `scripts/adopt_orphan_points.py`：
1. 通过 Qdrant scroll 检索出所有在 Qdrant 中存在但在 SQLite `memories` 表中不存在的点位（当前已知 71 条，`source_agent = 'openclaw'`）；
2. 保持原有 `memory_id` 与 `qdrant_point_id` 不变；
3. 将其合法写入 SQLite `memories` 表（`status = 'active'`, `created_at` 复用原有时间戳，`source_agent = 'openclaw'`，计算 SHA256 存入 `content_hash`）；
4. 保证幂等执行（重复执行不报错、不重复插入）。

### 子任务 3：Reconciliation 核心引擎骨架开发
在 `src/reconciliation/` 目录下构建基础模块：
1. `src/reconciliation/__init__.py`
2. `src/reconciliation/policy_gate.py`：
   - 实现状态机：`DETECTED -> PROPOSED -> POLICY_CHECK -> APPROVED -> APPLIED`；
   - 实现熔断限制：`max(50, min(0.05 * total, 5000))`；
   - 实现白名单保护：`[LESSON_LEARNED]` 标记拒绝删除；
3. `src/reconciliation/rules.py`：
   - 实现确定性规则判定（纯心跳、超长循环工具报错、临时协议注入标记）；
4. `src/reconciliation/engine.py`：
   - 实现 Phase 0 ~ Phase 6 流水线框架，原生支持 `--dry-run` 标志（只写 `curation_candidates`，不修改 `memories.status`，不下架 Qdrant）；
5. 入口包装器：`scripts/daily_memory_reconciliation.py`（提供 CLI 参数 `--dry-run`、`--batch-id`）。

---

## 三、 验收标准（Hermes 验收断言）
1. 运行迁移脚本后，`data/sessions.db` 正确建立新表与新字段；
2. 运行 `adopt_orphan_points.py` 后，SQLite `memories` 表总行数从 356 平滑增至 427，与 Qdrant 点位严格 1:1 吻合；
3. 运行 `python3 scripts/daily_memory_reconciliation.py --dry-run` 能够跑通 Phase 0~Phase 6 完整流程，正常输出 Dry-Run 候选审计报表，退出码 0，无任何报错；
4. 现有单元测试及 AMR 守护服务正常运行不受破坏。

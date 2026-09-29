# RFC-003: 多 Agent 记忆中枢一致性对账与生命周期治理规范 (daily_memory_reconciliation)

- **提案状态**: 架构终审冻结版 (Frozen Baseline for Implementation)
- **版本**: v1.0.1
- **所属系统**: AI Memory Runtime (AMR)
- **定位**: SQLite 单向真相源 (SSOT) 到 Qdrant 只读检索投影 (Retrieval Projection) 的确定性校准中枢

---

## 1. 顶层架构哲学与七条一致性不变量 (Core Invariants)

本规范确立“SQLite 决定记忆事实，Qdrant 决定检索效率”的单向从属模型。以下 7 条不变量是全系统不可逾越的“宪法”，在任何组件重构、数据库迁移或向量库替换时必须永久成立：

- **Invariant-01 (SSOT 唯一性)**: SQLite 是全系统唯一不可变事实来源（Single Source of Truth）。一切状态机流转、版本迭代、事实更迭与删除标记必须在 SQLite 事务中完成并固化。
- **Invariant-02 (投影从属性与可见性)**: Qdrant 作为只读检索投影，不得产生任何在 SQLite 中不存在的点位。**仅状态为 `active` 与 `stale` 的记忆允许存在于 Qdrant 主检索投影**；状态为 `archived` 与 `deleted` 的记忆绝对不得存在于 Qdrant 主检索投影。
- **Invariant-03 (删除不可逆检索 - 强拒识保证)**: SQLite 中状态为 `deleted` 的记忆，绝对不得出现在 Qdrant 的任何主动检索结果中。
- **Invariant-04 (归档非毁灭性)**: `archived` 记忆根据存储与性能策略从 Qdrant 主检索索引中下架，但绝对不得从 SQLite 中消失，且其血缘关系与唤醒路径必须保持畅通。
- **Invariant-05 (标识符生命周期唯一性)**: `memory_id` 在全系统全局生命周期内绝对不可复用。任何逻辑更新均为版本递增 (`version + 1`) 或取代 (`superseded_by`)。
- **Invariant-06 (治理全链路可审计可回滚)**: 任何自动化治理、规则命中、降级与清理操作，必须具备批次 ID（`curation_batch_id`）、全量审计日志，且具备一键还原回滚能力。
- **Invariant-07 (无损灾备自愈性与显式过滤)**: 当 Qdrant 发生灾难性故障、数据损坏或清空时，系统必须能够仅凭 SQLite 库无损重建。**重建时必须显式过滤跳过 `status IN ('deleted', 'archived')`**，且重建计算必须基于 SQLite 固化的 Embedding Provenance（模型版本、维度与配置哈希）。

---

## 2. 真实系统 Schema 与 Qdrant 数据现状映射 (P0 实测定案)

经穿透核查 AMR 生产 SQLite (`data/sessions.db`) 与 Qdrant (`192.168.30.161:6333`)，现状与映射关系定案如下：

### 2.1 SQLite `memories` 现状与状态机统一
- **实测分布**: 当前 `memories` 表共 356 条记录，`status` 字段 100% 均为 `'active'`。
- **状态字段归一**: 确认现有字段正是生命周期状态。**彻底废弃 `curation_status`**，系统状态统一收敛至单值字段 `memories.status`：
  - `active`: 正常活跃，参与检索；
  - `stale`: 观察期，参与检索但权重降低；
  - `archived`: 冷存，移出 Qdrant 主索引，保留 SQLite SSOT；
  - `deleted`: 逻辑删除（Tombstone 状态），彻底移出 Qdrant，禁止召回。
- **删除语义裁定**: 统一使用 `status = 'deleted'` 表达逻辑删除，`deleted_at` 记录时间，`deletion_reason` 记录原因，废弃冗余的 `is_tombstone` 字段，杜绝 Schema 裂脑。

### 2.2 356 memories 与 427 Qdrant Points 的差额成因与对账契约
- **实测事实**: SQLite 中有 356 条（来源均为历史认知提纯）；Qdrant 中共有 427 个点位。
  - 其中 356 个点位与 SQLite 的 `memory_id` 严格 1:1 吻合；
  - 差额的 **71 个点位**（`source_agent = 'openclaw'`，时间戳在 2026-09-27~09-29）是此前 OpenClaw 通过老版 MCP 插件直连 Qdrant 写入的历史孤儿点位（Orphan Points），尚未在 SQLite SSOT 中登记。
- **对账处置契约**:
  - 第一阶段（14 天 Dry-Run）：识别这 71 个孤儿点位并记录在 `curation_candidates` 报表中，**严禁物理删除**；
  - 启动对账补登程序：将这 71 条高价值技术记忆（如 TLCP 密管、AEP Releases 架构等）反向补登入 SQLite `memories` SSOT，赋予正式合法身份后，使系统收敛至严格 1:1 投影映射。

---

## 3. 架构设计：Policy Gate 与非阻塞 Outbox 链路

### 3.1 治理流水线与策略闸门
规则引擎（Rule Engine）只负责发现，不负责决定：

```
Rule Engine ──(发现)──> DETECTED ──> PROPOSED ──> Policy Gate ──(审核)──> APPROVED
                                                          │
                                     (Dry-Run/超阈值/白名单拦截)
                                                          ▼
                                                       REJECTED
                                                          │
                                          (审核通过进入 SSOT 事务)
                                                          ▼
                                                 SQLite Transaction
                                            (Chunky Commit, 200/批)
                                                          │
                                                          ▼
                                                   APPLIED (SSOT)
                                                          │
                                                          ▼
                                             memory_projection_outbox
                                                          │ (异步重试收敛)
                                                          ▼
                                                  Qdrant Projection
```

### 3.2 故障隔离核心法则：Qdrant 失败不得反向回滚 SQLite
- **单向事务**: SQLite SSOT 事务提交即视为事实生效。
- **投影滞后（Projection Lag）**: 若 Qdrant 在同步或下架点位时网络超时或故障，**绝对严禁回滚已 COMMIT 的 SQLite 事务**！
- **最终一致性**: 将操作事件保留在 Outbox 队列中，通过指数退避（Exponential Backoff）重试，直至 Qdrant 最终收敛。

---

## 4. SQLite Schema 增量迁移设计 (DDL)

针对 AMR `data/sessions.db` 执行轻量、非破坏性 DDL：

```sql
-- 1. memories 表增量补齐治理与对账字段
ALTER TABLE memories ADD COLUMN content_hash TEXT;
ALTER TABLE memories ADD COLUMN curation_batch_id TEXT;
ALTER TABLE memories ADD COLUMN last_reconciled_at INTEGER;

CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);
CREATE INDEX IF NOT EXISTS idx_memories_content_hash ON memories(content_hash);
CREATE INDEX IF NOT EXISTS idx_memories_reconciled ON memories(last_reconciled_at);

-- 2. 抽象并升级 Projection Outbox（解耦 Qdrant，支持多投影扩展，带幂等约束）
CREATE TABLE IF NOT EXISTS memory_projection_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    projection_type TEXT NOT NULL DEFAULT 'qdrant_main', -- qdrant_main, shadow_drill, search_index
    op_type TEXT NOT NULL, -- upsert, delete, update_payload
    payload_snapshot TEXT,
    status TEXT NOT NULL DEFAULT 'pending', -- pending, processing, completed, dead_letter
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER DEFAULT 0,
    last_error TEXT,
    dead_letter_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(memory_id, version, projection_type, op_type)
);

CREATE INDEX IF NOT EXISTS idx_outbox_queue ON memory_projection_outbox(status, next_retry_at);

-- 3. 治理候选表 (Dry-Run 与 Policy Gate 隔离表)
CREATE TABLE IF NOT EXISTS curation_candidates (
    candidate_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    current_status TEXT NOT NULL,
    proposed_status TEXT NOT NULL,
    matched_rule_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_snapshot TEXT,
    state TEXT NOT NULL DEFAULT 'PROPOSED', -- PROPOSED, APPROVED, REJECTED, APPLIED
    created_at INTEGER NOT NULL,
    processed_at INTEGER,
    FOREIGN KEY(memory_id) REFERENCES memories(memory_id)
);

-- 4. 对账检查点与元数据表 (固化 Embedding Provenance)
CREATE TABLE IF NOT EXISTS reconciliation_checkpoints (
    batch_id TEXT PRIMARY KEY,
    start_watermark_ts INTEGER NOT NULL,
    end_watermark_ts INTEGER,
    sqlite_memory_count INTEGER NOT NULL,
    qdrant_active_count INTEGER NOT NULL,
    proposed_count INTEGER DEFAULT 0,
    applied_count INTEGER DEFAULT 0,
    embedding_model TEXT NOT NULL DEFAULT 'BGE-M3',
    embedding_dimension INTEGER NOT NULL DEFAULT 1024,
    distance_metric TEXT NOT NULL DEFAULT 'Cosine',
    status TEXT NOT NULL, -- RUNNING, COMPLETED, FAILED
    error_message TEXT,
    created_at INTEGER NOT NULL,
    completed_at INTEGER
);
```

---

## 5. 规则、权重与动态评分精化 (P1 闭环)

### 5.1 批次与扫描时间解耦
- `curation_batch_id`: 仅当记忆的 `status`、`content` 或核心元数据**真正发生变更**时才更新该批次号；
- `last_reconciled_at`: 每次凌晨对账只要扫描检查过该点位，即刷新为当天时间戳（避免无变更点位被迫全量刷新 Qdrant Payload）。

### 5.2 保护白名单与门槛模型
- **绝对保护白名单**: 包含 `[LESSON_LEARNED]` 标记、显式人工确认、或存在活跃依赖的记忆，Policy Gate 绝对拒绝流转为 `deleted`；
- **提高删除门槛**: 高置信度/高重要度记忆（`confidence >= 0.9` 且 `importance >= 0.8`）不予永久豁免，但其废弃必须进入人工二次确认队列，禁止算法自动销毁。

### 5.3 熔断保护绝对上下限
单批次允许流转变更的最大数量限制算子：
$$\text{MaxAllowedChanges} = \max(50, \min(0.05 \times \text{TotalPoints}, 5000))$$
- 保证小规模时至少允许 50 条变动，大规模时最高不超过 5000 条，杜绝规则失效导致的大面积误删。

### 5.4 动态检索加权评分修正 (消除纯乘法否决)
检索排序得分采用保底加权模型：
$$\text{FinalScore} = \text{CosineSimilarity} \times \text{ImportanceFactor} \times \text{ConfidenceFactor} \times \max(\text{RecencyFactor}, 0.2)$$
- 设定 RecencyFactor 下限保底为 0.2，确保“重要但长期未提及”的核心架构与密码学知识绝不会因时间推移被强制归零。

---

## 6. 每日 02:30 执行流水线 (Phase 0 ~ Phase 6)

### Phase 0: Preflight
- 排他锁 `flock -n /run/user/1000/amr_reconciliation.lock`；
- `nice -n 19 ionice -c 3` 低优先级运行；
- 检查 SQLite 完整性 (`PRAGMA quick_check;`) 与 Qdrant `/healthz`；
- 配置文件驱动：从 `~/.config/amr/config.yaml` 的 `api_key_file` 读取凭据，零环境变量。

### Phase 1: Cold Backup (解耦分离)
- **SQLite 备份**: 调用 SQLite Online Backup API 输出至 `data/backups/sqlite_sessions_YYYYMMDD.db`（重型 VACUUM 维护移至独立排期）；
- **Qdrant 快照**: 调用原生 Snapshot API 并导出 Manifest；
- **轮转**: 14 天日备 + 4 周周备 + 3 月月备。

### Phase 2: Deterministic Curation & Policy Gate
- 依据 `curation_rules.yaml` 匹配噪音（心跳、工具循环报错、临时协议片段）；
- 写入 `curation_candidates` 表（`state = 'PROPOSED'`）；
- Policy Gate 执行熔断检查与白名单过滤（Dry-run 模式下直接退出）。

### Phase 3: Lifecycle Transition
- 采用 **Chunky Commit**：每 200 条记录提交一次事务，批次间 `sleep(0.02)` 出让锁；
- 更新 `memories.status`；
- 向 `memory_projection_outbox` 写入同步事件。

### Phase 4: Projection Reconciliation
- 取水位 `batch_start_watermark = CURRENT_TIMESTAMP`；
- 双向核验：补齐缺失的 `active/stale` 点位，下架无效点位；
- Qdrant 失败仅保留 Outbox 重试，严禁回滚 SQLite。

### Phase 5: Dual Correctness Gate & Telemetry
- **Correctness Gate (硬门禁)**:
  - Canary-1 (正向核心召回 Recall@1 $\ge 1.0$)；
  - Canary-2 (已删除记忆绝对拒识 - 召回结果集完全排除)；
  - Canary-3 (过时状态抑制)；
- **Telemetry (软指标)**: 记录 P95 延迟（$< 50\text{ ms}$）与 Qdrant Optimizer 状态。

### Phase 6: Shadow Restore Drill & Audit
- **影子恢复演练**: 每月在临时影子集合 `ai_memory_drill_<timestamp>` 中演练从 SQLite 全量重建并验证，演练完成立即销毁影子集合，**主服务零停机**；
- 记录审计报告，发送 Telegram 异步只读通报。

---

## 7. 渐进式落地执行路线 (Roadmap)

1. **第一阶段：基线固化与 DDL 迁移（3 天）**
   - 补充 DDL，打通 `memory_projection_outbox`，补齐 Provenance 元数据；
   - 梳理反向补登脚本，将 71 条孤儿点位纳管至 SQLite SSOT。
2. **第二阶段：14 天只读 Dry-Run（14 天）**
   - 部署 `systemd.timer` 每日 02:30 触发 `--dry-run`；
   - 仅写 `curation_candidates` 观察误判率。
3. **第三阶段：正式开启生命周期对账与影子演练**
   - 激活 Policy Gate 自动提交与 Outbox 同步；
   - 验证 Correctness Gate 与影子演练。

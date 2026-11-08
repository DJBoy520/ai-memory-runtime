# RFC: AMR 可靠性修复 v5（第三轮评审修订稿，外部审计规范不变）

- 状态: v5（2026-11-08，吸收第三轮双模型评审 20 条 blocker；队列/约束/幂等类断言再次经临时 SQLite 实测复现，编排方抽查 4 条均属实）
- 规范性输入: `docs/rfcs/GPT-AUDIT-RELIABILITY.md`（审计原文，要求不变）
- 实施顺序（用户指定，硬性）: **阶段 A 写入一致性 → 阶段 B 模型加载 → 阶段 C 接口与观测**
- 影响面: 守护进程 `amr.service`；schema 重建随 daemon 启动幂等执行；不触碰生产库与运行中服务
- 分支: `fix/amr-reliability-v3`

## 0. v5 核心决策（相对 v4 的结构性修正）

| # | 第三轮发现（证据） | v5 处置 |
| --- | --- | --- |
| D1 | 新表删了 payload_snapshot/op_type 却要求 legacy 重放快照；memory_record 分片 point 是随机 uuid4（memory_service.py:399）不可由 memory_id 重算；"行不存在→删 point"无 point 可用（均 high） | **结构性决策：新队列只承载 v3 任务；legacy 队列整体冻结**（qdrant_sync_queue 的 schema、payload_snapshot、既有排水行为原样保留）。新表无需快照/point 列；v3 任务行缺失→标 done+last_error 注记 "memory row missing; skip"（不假装能删） |
| D2 | "SQLite 有行→v3 / 迁自旧队列即 legacy" 自相矛盾；分片在 SQLite 有行会被误当 v3 丢字段（high） | 路由依据=**队列来源**：新队列→v3 语义；旧队列→既有重放语义。legacy 行永不迁移进新队列，矛盾消除 |
| D3 | legacy 延迟重放会复活 DELETED（违反审计 A3.5，high） | 随 D1 消除：v3 删除走新队列读最新态删 point；legacy 卡无删除语义，重放安全（论证写入规格） |
| D4 | 实测：合并 DO UPDATE 不改 task_class/revision 错位 → 新写入永久丢失；done 无 CAS；revision 递增点未逐路径列明（high） | §A2.4 认领协议 v5：done 加 **CAS**（`AND projection_revision=:claim`，0 行更新即任务已被合并改写、留待重试）；§A2.5 **逐写路径列明递增点** |
| D5 | reconciliation 隐藏生产者、旧格式直插（high，上轮已确认） | 不变：阶段 A 改造（统一 helper + 同事务递增 + 大写状态） |
| D6 | 切换屏障无停写步骤；memory_record 直写 Qdrant 不在单写者口径（high） | §A3.4 部署顺序固化：**停 daemon → 迁移/重建 → 起新代码**；单写者口径显式限定 **v3 memories**，memory_record 直写列为已知 legacy 例外（无 v3 生命周期，producer 清单标注） |
| D7 | legacy 非默认集合卡会被 collection_name 默认值误归（high） | 随 D1 消除：legacy 卡不产生新队列任务；v3 非默认集合由落库 collection_name 保证；存量默认回填仅影响 v3 行（可接受，文档记录） |
| D8 | §7 适配清单不全：test_reconciliation.py:153,157、test_v2_engine.py:413,420、test_v2_storage.py:134,136,219,237、test_retrieval_contract_fixes.py:128、test_step4:231,238,280（high） | §7 v5 补齐精确 file:line 与允许的修改类型（编排方已抽查 4 处属实） |
| D9 | 全量门触生产：test_v3_contracts.py:214 裸 `AMRApplication()` 开生产库连生产 Qdrant；test_v2_end_to_end 连运行中 daemon socket（high，编排方抽查确认） | §7 授权隔离改造：前者改 tmp config（AMR_CONFIG_PATH→tmp yaml + tmp sqlite + fake/断网 qdrant）；后者加 `AMR_TEST_LIVE_DAEMON=1` 门控默认 skip。**合规优先于覆盖** |
| D10 | outbox.replay 死信直改 pending 撞部分唯一索引（medium，实测 IntegrityError） | 重放=走统一入队 helper（合并语义）+ 死信行标 done + last_error "absorbed by pending#id" |
| D11 | 合并入队未重置退避（medium） | DO UPDATE 同时 `retry_count=0, next_retry_at=NULL` |
| D12 | 幂等过期键占 PK 至清理（实测重复 INSERT IntegrityError；medium） | 写入即仲裁：`INSERT ... ON CONFLICT(agent_id_raw,op,idem_key) DO UPDATE SET ... WHERE created_at < :cutoff`（过期原子覆写）；查询视过期为不存在 |
| D13 | `_ensure_schema` 不存在；真实锚点是 CREATE_TABLES_SQL（:171-187）与 _migrate_schema（:407-424）（medium） | 重建逻辑落在 **CREATE_TABLES_SQL（新库）+ _migrate_schema（存量库，启动幂等执行）**；迁移脚本=副本上的 inventory/rehearse 包装器，不再自带 DDL |
| D14 | update_memory_v3 无 superseded_by 参数；v3 payload 9 字段缺 superseded_by/scope → C2 重定向失效（high） | store 级 update_status 持久化 superseded_by/superseded_at（列已存在）；**v3 payload 扩至 11 字段**（+superseded_by+scope），search 重定向与 scope 过滤继续可用 |
| D15 | admin RPC 无 outbox.replay/list_dead_letters 交付与测试（medium） | 不变（C 状态轨），语义按 D10 修正 |
| D16 | C2 迁移无 --rehearse（medium） | migrate_status_enum 补 --rehearse + 顺序说明（先 SQLite 后 Qdrant payload，daemon 停机窗口） |
| D17 | memory_get 富化缺 raw_messages；无容错会连坐 Qdrant 故障（medium） | 富化清单补 raw_messages；**best-effort**（try/except+超时，Qdrant 故障时字段置 null，memory_get 必须成功） |
| D18 | main.py 未透传 idempotency_key；retention 配置无归属（medium） | C 状态轨交付：main.py dispatch 透传 idempotency_key + `settings.storage.idempotency_retention_days` |
| D19 | ROLLOUT/预演报告无交付归属（medium） | §6 交付清单明确两个文档（预演轨） |
| D20 | 测试适配纪律 vs 大量既有断言（high，汇总） | §7 v5 完整清单 + 适配类型白名单（仅期望值/夹具注入/隔离门控三类） |

## 1. 阶段 A：写入与投影一致性

### A1 写入成功语义

同 v4：create/update/delete 单事务完成主表+revisions+审计+投影任务+幂等快照，提交即返回 `projection_status:"pending"`；请求路径零 embed、零 qdrant。`memory_get` = SQLite SSOT + **best-effort payload 富化**（legacy 字段含 `raw_messages`：`meta.tags`、`parent_memory_id`、`chunk_index`、`total_chunks`、`session_id`、`source_message_ids`、`raw_messages`；try/except + 短超时，Qdrant 任何故障→字段 null，memory_get 必须成功）；SQLite 无行的 legacy 卡走现 Qdrant 路径。

### A2 新队列（v3 专用）与单写者

**1) 目标 schema（v5：新队列只承载 v3 任务，无快照/point 列）**：

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

ALTER TABLE memories ADD COLUMN projection_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE memories ADD COLUMN collection_name TEXT NOT NULL DEFAULT 'ai_memory';
```
**落地锚点（评审 D13）**：新库结构进 `CREATE_TABLES_SQL`（session_store.py:171-187）；存量库重建/加列逻辑进 `_migrate_schema`（:407-424，daemon 启动幂等执行，重建步骤：建 `_new` 表→拷贝映射（现存行按键去重，当前 0 行）→DROP→RENAME→ALTER memories）。`scripts/migrate_projection_queue.py` 不自带 DDL，作为副本上的 `--inventory/--apply(调用同一 store 初始化)/--rehearse` 包装器。旧列废弃不迁移。

**2) 统一入队 helper（唯一入队通道，v5 增加退避重置）**：
```sql
INSERT INTO memory_projection_outbox (target_collection, memory_id, projection_revision, request_id, status, retry_count, next_retry_at, created_at, updated_at)
VALUES (?, ?, ?, ?, 'pending', 0, NULL, ?, ?)
ON CONFLICT(target_collection, memory_id) WHERE status='pending' DO UPDATE
  SET projection_revision=excluded.projection_revision, request_id=excluded.request_id,
      retry_count=0, next_retry_at=NULL, updated_at=excluded.updated_at;
```
`target_collection` 一律取 `memories.collection_name`（create 落库入参；update/delete 用库内值）。

**3) 投影语义（按队列来源，无歧义）**：新队列任务 → 实时读 SQLite 最新完整状态：行存在且 `status.upper()!='DELETED'` → 构造 **11 字段 payload**（9 字段 + `superseded_by` + `scope`，均取自 memories 行）→ embed → upsert（point id = `memories.qdrant_point_id`）；DELETED → 按 point id 删 point；行缺失 → 标 done + "memory row missing; skip"。旧队列 → **既有重放行为原样保留**（schema 冻结；legacy 卡无删除语义，重放不构成复活）。

**4) 认领协议（v5：CAS）**：同一读事务取得 memories 状态与 `claim_revision`（不信队列行值）；投影成功后 `UPDATE outbox SET status='done', projected_revision=:claim WHERE id=:id AND status='pending' AND projection_revision=:claim`——**0 行更新说明任务在处理期间被合并改写（有新写入），保持 pending 留待下轮**（不丢更新）；随后复查 `memories.projection_revision > :claim` 则再走 helper 兜底。崩溃任意点：未 done 即重做（至少一次+最终幂等）。

**5) projection_revision 递增点（逐写路径，同事务）**：create（置 1）；update 内容（+1）；update 元数据/status（+1）；update_status / 软删除（+1）；reconciliation 状态变更（+1）。

**6) 重试与死信**：`next_retry_at=now+min(2^n,60)s`；`retry_count>=8` → dead_letter；重放经 helper（D10 语义）。legacy 队列无退避/死信（冻结，已知限制）。

**7) 隐藏写者改造（不变）**：`memory_service.memory_update_status` 改 SQLite 写库（经扩展后的 store 级 update_status，**持久化 superseded_by/superseded_at**）+ 统一入队；`session_store.update_memory_status` 大写规范化；`reconciliation/engine.py:218-242` 同事务递增 + helper 入队。

**8) `update_memory_v3` VERSION_CONFLICT → `VersionConflictError(memory_id, current_version, expected_version)`（不变）。

**9) 单写者口径（v5 显式）**：单写者声明仅覆盖 **v3 memories 的 Qdrant 投影**。producer 清单中 `memory_record` 直写 Qdrant（memory_service.py:456-461）登记为 **legacy 例外**（无 v3 生命周期，point 归 legacy 队列管理），不得据此宣称全库单写者。

### A3 迁移与切换屏障（v5）

1. `--inventory`：旧/新队列行数、生产者清单（含 legacy 例外标注）、schema 状态。
2. `--apply`：对副本或停机库执行 store 初始化（触发 `_migrate_schema` 重建），输出前后对账。
3. `--rehearse`：副本上全流程演练 + 对账 JSON/摘要报告。
4. **切换屏障（固化到上线手册）**：停 daemon → （备份）→ 迁移/预演复核 → 启动新代码（启动即完成 schema 幂等迁移）→ doctor 体检。运行中旧 daemon 与迁移并存被流程禁止。
5. 测试：重建后同 version 重复入队不炸唯一约束；旧队列行原样保留；新队列 CHECK/唯一索引行为。

### A4 幂等（v5）

- 表结构同 v4；**写入即仲裁**（D12）：
```sql
INSERT INTO idempotency_keys (agent_id_raw, op, idem_key, request_digest, memory_id, response_json, created_at)
VALUES (?,?,?,?,?,?,?)
ON CONFLICT(agent_id_raw, op, idem_key) DO UPDATE
  SET request_digest=excluded.request_digest, memory_id=excluded.memory_id,
      response_json=excluded.response_json, created_at=excluded.created_at
  WHERE created_at < :cutoff;   -- 仅过期行可被覆写
```
未过期冲突 → DO NOTHING → 回读：摘要一致→重放（`idempotent_replay:true`）；不一致→`IDEMPOTENCY_CONFLICT` 领域异常；并发首请求同理由 PK+回读仲裁。Worker 每小时 `DELETE` 过期行（保鲜），但正确性不依赖清理及时性。
- digest 按操作字段清单与 canonical_json 规则同 v4（create 排除服务端 memory_id）。
- main.py 透传与 retention 配置归 C 状态轨（D18）；A 轨交付 service/store 层完整能力 + tools.py 契约说明。

**阶段 A 交付**：A写轨（deepseek）：`memory_service.py`、`session_store.py`（含 CREATE_TABLES_SQL/_migrate_schema 重建）、`reconciliation/engine.py`、`tools.py`、tests（outbox_single_writer / idempotency / memory_get_ssot）；A投影轨（gemini）：`outbox_worker.py`（新队列消费+CAS 认领+legacy 排水保留）、`scripts/migrate_projection_queue.py`、tests（projection_ordering / dual_queue_migration）。

## 2. 阶段 B：模型冷启动

同 v4（线程原语 single-flight、epoch 局部构造+过期丢弃、独立 watchdog、受控退出、idle<=0 禁卸载、手工置 LOADING=等待者语义、preload 入口、四项新配置、METRICS、插件预算零改动、test_loading_timeout 按 §7 适配）。B 轨交付 settings.py 时**不得动 storage 段**（C 轨后续加 retention）。

## 3. 阶段 C：接口、状态与可观测

- C1（C 接口轨，gemini）：同 v4（RpcError 直通首位、11 字段 payload 使 VERSION_CONFLICT/重定向数据可用、except ValueError 审计、真实 UDS+bridge 往返测试）。
- C2（C 状态轨，deepseek）：search 重定向按 superseded_by（payload 已由 A 轨扩至 11 字段）+ 目标校验/链环/跨集合规则；`migrate_status_enum.py` **补 --rehearse**（D16，先 SQLite 后 Qdrant payload、停机窗口说明）；cognitive_engine 只盘点不改。
- C3（C 状态轨）：request_id 服务端生成 + client_ref；main.py：logging.Filter、preload 接线、auth.authorize_agent、**idempotency_key 透传（D18）**、admin `status.metrics` + `outbox.list_dead_letters` + `outbox.replay`（D10 语义）；`settings.storage.idempotency_retention_days`（D18）；amr_doctor（脱敏、--json、降级）。

## 4. 交付清单（含文档归属）

代码：§1-§3 各轨文件。文档：`docs/ROLLOUT-AMR-RELIABILITY.md`（上线顺序=停 daemon→迁移→起新码→doctor→观察指标、重启步骤、回退条件与操作）（预演轨）；`docs/MIGRATION-REHEARSAL-REPORT.md`（前后对账数字）（预演轨）；`tools.py`/`DOC-AMR-04-API.md` 补 search 弱一致性与幂等契约说明（A 写轨）。共享契约四文件编码轨只读。

## 5. 交付验收 ↔ 测试映射

v4 十条映射 + v5 增补：#4 并发首请求+过期 key 重新执行；#5 认领后崩溃恢复（done 前杀 Worker）、处理期间新写入经 CAS 不丢失；#7 重建后唯一约束行为、旧队列行原样；#8 死信查询/重放 + **测试生产隔离**（D9：tmp config / AMR_TEST_LIVE_DAEMON 门控）。

## 6. 已知风险

同 v4，另加：legacy 队列冻结意味着 memory_record 卡的投影质量维持现状（随其废弃退场）；测试隔离改造可能减少默认全量门的 E2E 覆盖（AMR_TEST_LIVE_DAEMON=1 可恢复）。

## 7. 测试适配清单（白名单三类：期望值更新 / 夹具注入 / 隔离门控；禁止弱化语义断言、删用例）

| 文件:位置 | 允许的修改 | 归属 |
| --- | --- | --- |
| tests/test_step3_engine.py::test_loading_timeout | 期望异常 → ModelLoadingPendingError | B |
| tests/test_reconciliation.py:153,157 | 状态期望 'deleted'→'DELETED'；outbox 断言改经新 helper 语义 | A |
| tests/test_v2_engine.py:413,420 | 状态期望大写化；旧队列断言维持（legacy 冻结）或改经 memory_get | A |
| tests/test_v2_storage.py:134,136,219,237 | 旧队列 snapshot/op_type 断言维持（legacy 冻结后应原样通过；如夹具建表需换新 schema 则适配） | A |
| tests/test_retrieval_contract_fixes.py:128 | 快照保留断言维持（legacy 冻结后原样通过） | A |
| tests/test_step4_memory_service.py:159-164,186-193,231,238,280,319-332 | memory_get 合并形态字段适配；update_status 后改经 memory_get 或夹具内联投影（search 弱一致） | A/C |
| tests/test_step5_extra_contracts.py:165-180、tests/test_step5_comprehensive.py:222-237 | 状态字符串期望按 normalize 映射更新 | C |
| tests/test_v3_contracts.py:214 起的 AMRApplication 用例 | **隔离改造**：AMR_CONFIG_PATH→tmp yaml（tmp sqlite + fake/断网 qdrant） | A |
| tests/test_v2_end_to_end.py | **隔离门控**：`@pytest.mark.skipif(not os.environ.get("AMR_TEST_LIVE_DAEMON"), ...)`（连真实 daemon 的用例默认跳过；E2E 延迟用例维持 deselect） | A |

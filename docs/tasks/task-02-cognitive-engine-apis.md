# Task 02 任务书：五大核心 API 与确定性治理引擎（Engine & Outbox Worker）

## 一、 执行者与工作区
- **执行者**：OpenCode
- **工作区**：`/home/dj/WorkSpaces/ai-memory-runtime`
- **当前 Git 分支**：`feat/amr-engine-upgrade-v2`
- **规范基准**：`docs/proposals/AMR认知记忆引擎升级与全量离线提纯实施规格_v2.2终版.md` (第三节与第四节)

---

## 二、 目标与交付清单

基于 Task 01 的 SQLite SSOT 存储层，重构/实现 AMR 认知记忆引擎核心逻辑与 Outbox 后台 Worker：

1. **实体归一化服务（`src/service/entity_normalizer.py`）**：
   - 建立实体别名映射字典（支持中英文同义词、大小写、连字符归一化，例如：“美式”/“Americano” -> `americano_coffee`，“AEP-TSA”/“aep_tsa” -> `aep_tsa`）；
   - 提取三元组后先归一化，保障判定精准。

2. **认知治理引擎扩展（`src/service/cognitive_engine.py` 或扩充 `memory_service.py`）**：
   - **`extract`**：实体归一化，生成 `CandidateMemory`（默认状态 `candidate`），判定晋升门槛（`confidence >= 0.90 AND mention_count >= 2` 或显式强制）；
   - **`merge`（多级混合防语义漂移与证据累积）**：
     - Level 1：三元组 `(subject, predicate)` 严格匹配，`object` 一致才进候选，互斥则走冲突策略；
     - Level 2：否定词与反义对抗拦截（`不/未/严禁/禁用/开启/停止`），逻辑相反者强制转为冲突分支；
     - Level 3：跨 Session 贝叶斯证据累加：
       `confidence_new = 1 - (1 - confidence_old) * (1 - evidence_strength)`，更新 `mention_count += 1`；
   - **`update`**：正交解耦 `type` 与 `conflict_policy`（`overwrite`, `coexist`, `state_machine`, `immutable`），状态机流转依据合法 DAG 拓扑；
   - **`retrieve`**：
     - 服务端硬过滤：强制注入 `status == 'active'` 与 `project_id IN ('general', current)`；
     - 写后读一致性：并发查询 SQLite 最近 60s 内未同步到 Qdrant 的 `pending` 记忆并合并；
     - 召回点回表校验当前最新状态（防止索引延迟造成脏读）；
     - 复合重排评分公式（向量相似度 + 重要性 + 置信度 + 半衰期新鲜度 + 类型权重），响应体同时返回 `final_score` 与 `vector_score`；
   - **`forget`**：事务中标记 `status='deleted'`，记录 `deleted_at/by/reason`，投递 Outbox `op_type='delete'` 触发 Qdrant 物理删除并写审计日志。

3. **Transactional Outbox Worker（`src/service/outbox_worker.py`）**：
   - 独立后台守护协程/线程，按 `memory_id` 顺序消费 `qdrant_sync_queue`；
   - 支持 `upsert`（调用 BGE-M3 生成 1024 维向量并推送到 Qdrant `qdrant_point_id` UUIDv5）、`update_payload`、`delete`（物理删除 Point）；
   - 支持失败重试（上限 5 次）、异常捕获与状态更新，网络故障自愈。

4. **单测保障（`tests/test_v2_engine.py`）**：
   - 验证五大 API 契约；
   - 验证三级混合合并防语义漂移（反义词不合并）；
   - 验证贝叶斯证据累加数学结果；
   - 验证写后读一致性与复合重排打分；
   - 验证 Outbox Worker 异步将 SQLite 变更同步至 Qdrant（可结合本地 Mock/内存 Qdrant）。

---

## 三、 质量与审计门禁
1. **零现有测试破坏**：运行 `pytest tests/` 全部 175 项现有用例必须保持全绿；
2. **新增测试完备**：`pytest tests/test_v2_engine.py` 必须 100% 通过；
3. **完成标准**：代码结构优雅，模块解耦，零硬编码。

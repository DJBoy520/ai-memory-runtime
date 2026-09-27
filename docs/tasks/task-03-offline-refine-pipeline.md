# Task 03 任务书：离线批处理提纯流水线（100 条样本闭环与回滚脚本）

## 一、 执行者与工作区
- **执行者**：OpenCode
- **工作区**：`/home/dj/WorkSpaces/ai-memory-runtime`
- **当前 Git 分支**：`feat/amr-engine-upgrade-v2`
- **规范基准**：`docs/proposals/AMR认知记忆引擎升级与全量离线提纯实施规格_v2.2终版.md` (第五节)

---

## 二、 目标与交付清单

基于 Task 01 与 Task 02 的底层能力，开发离线提纯合并流水线脚本及回滚预案：

1. **离线提纯合并脚本（`scripts/migrate_refine_v2.py`）**：
   - 数据源：读取冷备数据文件 `/home/dj/WorkSpaces/ai-memory-runtime/data/backups/ai_memory_backup_20260927.json`；
   - 支持 `--sample 100` 参数（先小步 100 条端到端闭环验证）与 `--full` 参数（全量）；
   - 提取逻辑：
     - 将原始记录注入 SQLite `raw_sessions` 与 `raw_messages`，显式标记 `source_type = 'legacy_memory'`, `is_synthetic = 1`；
     - 结构化提取：解析提炼三元组 `(subject, predicate, object)` 及原子陈述句 `content`，自动判定 `type` (`fact`, `preference`, `decision`, `task`, `episode`)；
     - 调用 `EntityNormalizer` 进行实体规一化；
     - 调用 `CognitiveEngine.merge_memory` 执行三级防语义漂移仲裁与贝叶斯证据累加；
     - Episode 记忆保持不可变，不盲目合并，完整保全关键事件；
     - 将精炼出的结构化卡片与证据关系原子写入 SQLite SSOT；
     - 统计并输出处理报告：原始点数、去重合并数、生成精炼卡片数、平均置信度、耗时。

2. **数据回滚与安全审计脚本（`scripts/rollback_refine_v2.py`）**：
   - 提供一键回滚操作：基于备份快照校验哈希；
   - 若测试未达标，可一键清理 `raw_messages` 中 `is_synthetic = 1` 及关联 memories，安全复原状态。

3. **流水线测试保障（`tests/test_v2_migration.py`）**：
   - 编写单元测试验证流水线逻辑：
     - 测试 10~20 条典型历史碎片（含重复偏好、反义词、不同状态任务）输入；
     - 断言经过流水线后反义词未被合并、相同偏好合并且 mention_count 增加、置信度符合贝叶斯增长；
     - 断言回滚脚本能安全幂等清理并复原。

---

## 三、 质量与审计门禁
1. **零现有测试破坏**：运行 `pytest tests/` 全部 184 项用例 100% 保持全绿；
2. **新增测试完备**：`pytest tests/test_v2_migration.py` 全部 PASS；
3. **完成标准**：脚本具备友好 CLI 选项、进度条/日志输出，执行安全可控。

# 任务书：AMR 记忆操作系统 (MOS v3.0) 核心认知整理模块编码实施

- **执行方**: OpenCode (编码执行专家)
- **验收方**: Hermes (方案架构与验收)
- **技术依据**: `/home/dj/WorkSpaces/ai-memory-runtime/docs/proposals/RFC-005_memory_operating_system_spec.md` (v3.0 终审冻结版)
- **工作目录**: `/home/dj/WorkSpaces/ai-memory-runtime`

---

## 一、 任务目标与实施边界

本任务在 AMR 项目内部实现第二阶段核心认知提纯与治理能力：
1. **完善 Schema 演化支持 (DDL)**：在 `src/core/session_store.py` 中为 `memories` 补充版本化与保护字段（`version`, `previous_version_id`, `root_memory_id`, `superseded_by`, `superseded_at`, `protection_level`）。
2. **构建 `src/cognitive/` 核心认知提纯子系统**：
   - `evidence_validator.py`：三层匹配（NFKC归一化、子串包含、Jaccard>=0.95）验证 `extracted_spans`；
   - `coverage_validator.py`：检查 `message_disposition`（EXTRACTED/DISCARDED/UNPROCESSED 守恒）；
   - `context_builder.py`：确定性组装 `Memory Context Pack`（Top-20 向量 + project/entity/近7天变更，截断 30 条）；
   - `prompt_manager.py`：固化《记忆治理宪法五原则》与结构化 JSON Schema；
   - `scheduler.py`：驱动认知提纯流水线（增量窗口扫描 -> 组装 Pack -> 调用 LLM 提纯 -> 双验证器门禁 -> 写入 `curation_candidates` 隔离表）。
3. **编写专属单元测试**：在 `tests/test_cognitive_distill.py` 覆盖双验证器（正例通过、编造引文拦截、未分类消息覆盖拦截）、Context Pack 组装逻辑与版本演化。

---

## 二、 核心铁律（必须严格遵守）
- **AMR 拥有唯一主控调度权**：LLM 仅作为认知计算工人，只有提案权，零写权限；
- **14 天 Dry-Run 铁律**：所有提纯结果仅作为 `PROPOSED` 状态写入 `curation_candidates` 隔离表，绝对不得直接修改 `memories` SSOT；
- **测试通过标准**：编写的 `tests/test_cognitive_distill.py` 与已有测试全绿通过。

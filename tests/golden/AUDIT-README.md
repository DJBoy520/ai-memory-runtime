# AMR 检索黄金评估集质检与审核规范 (AUDIT-README)

> **状态更新（2026-10-07 终态）**：黄金集经退役清洗 530 → 470 条，并完成 DeepSeek 4 路 AI 审计
> （keep 447 / fix 23 / retire 0），状态由 `provisional` → **`ai-audited`**；人工抽审按用户指示取消。
> 当前冻结语料为 `retrieval_golden.jsonl`（470 条），本节以下 530 条口径与人工抽审方案为建集期原始规范，保留作过程记录。
> 终态指标与门禁见 `AMR-RETRIEVAL-IMPLEMENTATION-PLAN.md`。

## 1. 评估集概述与当前状态

当前 `tests/golden/retrieval_golden.jsonl` 作为 AI Memory Runtime (AMR) 检索质量评测的基准语料集，当前状态为 **`provisional`（临时/预备版本）**。
该评估集汇聚了多批次自动生成的候选查询集与专项查询集，经过格式规范与业务约束自动化校验，全量共计 530 条。

### 数据构成与分层统计

- **总查询数 (Total Queries)**: 530 条
- **分布明细 (byCategory)**:
  - `semantic` (语义检索问答): 300 条
  - `exact_id` (精确 ID 直查): 150 条
  - `no_result` (无结果负向查询): 40 条
  - `identifier` (符号/标识符/路径/配置项查询): 40 条

---

## 2. 人工分层抽审方案 (≥30%)

为了确保评测基准的严谨性与有效性，避免大模型幻觉与不合理的弱相关/假负例污染评测结果，本黄金集进入正式 release 状态前，必须由评审人员按类别进行分层随机抽样人工复核。

### 抽样指标与要求

- **最低抽样比例**：各 category 抽审比例 **≥ 30%**。
- **推荐各类别最低抽审样本量**：
  - `semantic`: 300 条中至少抽审 **90 条**（建议 100 条）；
  - `exact_id`: 150 条中至少抽审 **45 条**（建议 50 条）；
  - `no_result`: 40 条中至少抽审 **12 条**（建议 15 条）；
  - `identifier`: 40 条中至少抽审 **12 条**（建议 15 条）；
  - **总计至少抽审**：**159 条**（占全量 30.0%）。

### 审核重点

1. **`semantic` 类**：
   - 确认 `query` 的语义描述是否合理，是否覆盖自然提问、技术提炼、同义改写等形式；
   - 确认 `relevant_ids` 中标注的记忆卡片是否能够真实且准确地回答 query；
   - 确认 `hard_negative_ids`（2~5条）是否具备强干扰性（如相同 project、相似词汇但语义不符），严禁误将实际相关的真正例标注为负例。
2. **`exact_id` 类**：
   - 确认 query 内包含的 ID 与 `relevant_ids` 完全一致，且只有该 ID 1 条真正例。
3. **`no_result` 类**：
   - 确认查询的主题在语料库全集（`data/sessions.db`）中确实不存在，`relevant_ids` 必须为空，防止误伤语料中真实存在的模糊相关知识。
4. **`identifier` 类**：
   - 确认路径、文件名、端口号、配置项键名、类名等标识符确实存在于对应记忆正文中（1~3条真实命中）。
5. **标记更新**：
   - 经过人工复核确认的条目，将 `reviewed` 字段置为 `true`，并在 `reviewed_by` 字段填写评审人标识（如 `"human_auditor"` 或对应人员 ID）。

---

## 3. 版本链标注与相关性判定策略

在多 Agent 协同记忆系统（AMR）中，记忆具备生命周期演进（包括修订、替代、追加等），容易形成针对同一事实的版本链（例如 `expected_version` 迭代、旧版本被标记为历史或被新版本替代）。

### 判定策略与规则

1. **同一事实新旧版本判定准则**：
   - **同一事实新旧版本任一命中即算相关**。
   - 当某条 query 对应的事实在 AMR 中存在修订历史（版本 1 与版本 2、或者原始条目与 superseded 条目）时，检索器若返回了当时有效的历史版本或最新版本，在 Recall@K / HitRate 评估中均视为命中了相关事实。
2. **黄金集标注约定**：
   - 在构建与维护 `relevant_ids` 时，优先收录当前有效的最新 `ACTIVE` 记忆 ID；
   - 若测试环境包含历史版本或跨版本切片，评测脚本或标注池支持版本链等价映射表（Version Chain Equivalence Map），确保检索到版本链上的任一有效节点均计为有效召回；
   - 审核员在复核时，若发现某条相关记忆后续被新记忆替代，可将新旧 ID 均追加至 `relevant_ids`，或记录在 `notes` 中予以说明。

---

## 4. 自动化校验流程

所有对本文件的更新与修订，必须通过内置门禁脚本校验：

```bash
python3 scripts/build_golden_set.py --validate tests/golden/retrieval_golden.jsonl
```

门禁要求：
- 每行必须为合法 JSON 且包含 9 个必需字段；
- `query_id` 必须全局唯一且按序排列；
- `category` 属于 `{"semantic", "no_result", "exact_id", "identifier"}`；
- `semantic` 类别 `relevant_ids` 非空且 `hard_negative_ids` 包含 2~5 条（特殊情况需 notes 明确说明）；
- `no_result` 类别 `relevant_ids` 必须为空；
- 所有 `relevant_ids` 与 `hard_negative_ids` 中的 ID 必须符合 `mem_YYYYMMDD_xxxxxx(_chunk_N)?` 正则格式。

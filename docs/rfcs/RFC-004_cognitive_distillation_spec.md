# RFC-004: AMR 认知记忆整理宪法与 LLM 蒸馏提纯引擎规范 (Cognitive Distillation Engine)

- **提案状态**: 架构终审冻结版 (Frozen Specification)
- **版本**: v1.0.0
- **所属系统**: AI Memory Runtime (AMR)
- **核心定位**: 从“垃圾清理器”升级为“三层认知提纯中枢”，确立 LLM 认知整理边界与证据链检验防线

---

## 一、 系统三层定位与架构分工

```text
第一层：Raw Memory (原始证据)
  │  载体: raw_sessions / raw_messages
  │  铁律: 只进不改，只增不删，为永久原始证据底座。
  ▼
第二层：Curated Memory (认知知识 SSOT)
  │  载体: memories / memory_evidence
  │  铁律: 系统真正值得长期记住的高密度结构化知识，唯一事实源。
  ▼
第三层：Retrieval Projection (只读检索投影)
  │  载体: Qdrant (ai_memory)
  │  铁律: 仅作为第二层知识的高速近邻检索索引，随时可全量重建。
```

---

## 二、 认知提纯闭环流水线 (LLM + 证据门禁)

严禁 LLM 直接改写或越权决定 SSOT。全链路遵循四权分立与强制证据检验：

```text
Raw Conversation (今日新对话 + 历史相关上下文)
       │
       ▼
┌─────────────────────────┐
│ LLM Cognitive Distiller │  “这里有什么值得记住的技术方案与客观事实？”
│ (Extractive, not Rewrite)│
└────────────┬────────────┘
             │ 产出结构化 Candidate (带 extracted_spans + source_ids)
             ▼
┌─────────────────────────┐
│ Evidence Validator      │  “你凭什么这么说？原文字符串比对能对上吗？”
│ (确定性字符/语义锚定器)   │  (原文字句缺失 ──> 直接判定幻觉并丢弃)
└────────────┬────────────┘
             │ 验证通过，写入 curation_candidates
             ▼
┌─────────────────────────┐
│ Policy Gate             │  “是否允许进入长期记忆？是否存在冲突与覆盖？”
│ (策略裁决闸门)          │  (单批熔断 + 白名单保护 + 状态机仲裁)
└────────────┬────────────┘
             │ 裁决 APPROVED
             ▼
┌─────────────────────────┐
│ SQLite SSOT Transaction │  更新 memories (EXTRACT/MERGE/SUPERSEDE/ARCHIVE)
└────────────┬────────────┘
             │ 写入 Outbox
             ▼
┌─────────────────────────┐
│ Qdrant Projection       │  更新向量点位与 Payload (主动检索可见性)
└─────────────────────────┘
```

---

## 三、 《AMR Memory Curation Constitution》（记忆治理宪法五原则）

在启动任何 LLM 提纯任务前，模型必须严格遵守以下五条不可逾越的治理宪法：

1. **宁可少整理，不可错误整理**：在信息价值不明确时，保持现状，严禁过度提炼。
2. **宁可保留冗余，不可因追求简洁而丢失技术细节**：绝不能为了格式美观而删减参数与上下文。
3. **任何新结论必须能追溯到原始证据**：一条记忆若无法映射到原始消息片段，视为无效。
4. **不得把推测写成事实**：助手或用户的猜测、临时测试设想，不得沉淀为既成事实。
5. **不得因语言优化而改变原始事实的语义强度**：
   - *反例*：原文“目前尚未完成某项密评合规检测”，禁止提炼为“系统不合规/不具备资质”（语义被主观放大）。

---

## 四、 绝不能被“整理掉”的技术资产清单（保护红线）

无论模型如何合并与提纯，以下实体与内容**严禁被丢弃或模糊化**：
- **精确技术数值**：数字、时间戳、版本号、IP 地址、端口号、文件绝对路径、终端命令、错误码。
- **配置与接口契约**：配置项名称、API 路由、协议名称、密码学算法名称（如 SM2/SM3/SM4-GCM/TLCP）、标准号（如 GM/T 0015-2023）、依赖包版本。
- **关键决策与经验**：明确决策、否定结论、**失败经验与已废弃方案的教训**（“为什么方案 A 失败而选方案 B”）、用户明确偏好。

---

## 五、 允许的 6 种标准认知操作（LLM 严禁自由发挥）

LLM 输出的操作动词必须严格限制在以下 6 种内：

1. `EXTRACT`: 从多轮会话中提取全新客观事实/技术方案。
2. `MERGE`: 将同一主题的碎片对话融合为一条高信息密度的完整记忆。
3. `UPDATE`: 补充、完善已有记忆的技术细节或上下文。
4. `SUPERSEDE`: 新的明确决策推翻/取代旧方案（旧记忆自动置为 `superseded`）。
5. `LINK`: 建立两条记忆之间的依赖、关联或因果关系。
6. `ARCHIVE`: 标记某项已完结、废弃项目的记忆移入冷存，降低检索权重。

**铁律：`DELETE` 操作默认严禁 LLM 使用。物理或逻辑删除只能由确定性规则或人工完成。**

---

## 六、 跨会话全局关联机制（非仅总结当天）

提纯引擎不得割裂历史。采用“新会话触发全局回溯”策略：
```text
新会话事实提取 ──> 检索历史相关记忆 (UDS memory.search, Top-5)
               ──> 联合比对新旧事实
               ──> 判定演进关系:
                   * UNCHANGED (已有记忆完全覆盖，无需动作)
                   * UPDATE (新信息补充已有记忆)
                   * SUPERSEDE (新决策取代旧方案)
                   * CONFLICT (存在矛盾，上报人工裁决候选)
                   * MERGE (多碎片合并)
```

---

## 七、 LLM 结构化输入/输出 JSON Schema 契约

LLM 认知整理输出必须严格匹配以下 JSON 契约：

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "CognitiveDistillationResult",
  "type": "object",
  "required": ["prompt_version", "session_id", "candidates", "unprocessed_message_ids"],
  "properties": {
    "prompt_version": { "type": "string", "enum": ["v1.0.0-cognitive"] },
    "session_id": { "type": "string" },
    "candidates": {
      "type": "array",
      "items": {
        "type": "object",
        "required": [
          "operation",
          "subject",
          "predicate",
          "object",
          "content",
          "evidence_source_ids",
          "extracted_spans",
          "importance",
          "confidence",
          "target_memory_id"
        ],
        "properties": {
          "operation": {
            "type": "string",
            "enum": ["EXTRACT", "MERGE", "UPDATE", "SUPERSEDE", "LINK", "ARCHIVE"]
          },
          "target_memory_id": { "type": ["string", "null"] },
          "subject": { "type": "string" },
          "predicate": { "type": "string" },
          "object": { "type": ["string", "null"] },
          "content": { "type": "string" },
          "evidence_source_ids": {
            "type": "array",
            "items": { "type": "string" },
            "minItems": 1
          },
          "extracted_spans": {
            "type": "array",
            "items": { "type": "string" },
            "minItems": 1
          },
          "importance": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
          "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
          "rationale": { "type": "string" }
        }
      }
    },
    "unprocessed_message_ids": {
      "type": "array",
      "items": { "type": "string" }
    }
  }
}
```

---

## 八、 Evidence Validator（确定性证据验证器算法）

在进入 Policy Gate 之前，系统使用纯代码执行严格的字符串与语义锚定校验：
1. **Span 原文存在性断言**：
   - 遍历每个 `extracted_spans` 中的字符串，必须在对应的 `evidence_source_ids` 原始消息内容中精确存在（或满足忽略标点空格后的子串包含）。
   - **若有任何一个 span 无法在原始会话中找到，整条 Candidate 直接标记为 `INVALID_EVIDENCE` 并硬性丢弃**。
2. **防推断放大断言**：
   - 若 content 包含极端否定词或结论性大词（如“完全不支持”、“毫无资质”），比对原文，若原文仅有温和描述，直接打回。
3. **Coverage 保护**：
   - `unprocessed_message_ids` 记录未被提炼的会话 ID，保留在原始消息表中，绝不删除。

---

## 九、 落地执行顺序

1. **先冻结规范**：固化本文档 `RFC-004_cognitive_distillation_spec.md`；
2. **实现 Evidence Validator** 与 JSON 校验器；
3. **编写专属提炼 Prompt**（版本化注入 `config/prompts/cognitive_distillation_v1.yaml`）；
4. **挂接 02:30 定时任务**：与现有的 RuleEngine 并列运行，产出 Candidates；
5. **开启 14 天只读 Dry-Run**：LLM 提炼结果只存入 `curation_candidates` 报表，由老板与专家人工校验质量后，方可开启自动 SSOT 提交。

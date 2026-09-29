"""
AI Memory Runtime - Cognitive Distillation Prompt Manager
遵循 RFC-004 第三/四/五/七节与 RFC-005 第五节：
固化《记忆治理宪法五原则》与结构化 JSON Schema 契约。
"""

import json
from typing import Any, Dict, List, Optional

CONSTITUTION_FIVE_PRINCIPLES = """
《AMR 记忆治理宪法五原则》（必须绝对遵守）：
1. 宁可少整理，不可错误整理：在信息价值不明确时，保持现状，严禁过度提炼。
2. 宁可保留冗余，不可因追求简洁而丢失技术细节：绝不能为了格式美观而删减参数与上下文。
3. 任何新结论必须能追溯到原始证据：一条记忆若无法映射到原始消息片段，视为无效。
4. 不得把推测写成事实：助手或用户的猜测、临时测试设想，不得沉淀为既成事实。
5. 不得因语言优化而改变原始事实的语义强度：严禁将原文中性或未定论描述主观放大为极端或确凿结论。

【红线保护资产】：精确技术数值、版本号、IP/端口、路径、命令、配置项、标准号、失败教训与废弃原因严禁被整理掉。
【允许的标准操作 (operation)】：EXTRACT, MERGE, UPDATE, SUPERSEDE, LINK, ARCHIVE（严禁使用 DELETE）。
"""

DISTILLATION_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "CognitiveDistillationResult",
    "type": "object",
    "required": ["prompt_version", "session_id", "candidates", "message_disposition"],
    "properties": {
        "prompt_version": { "type": "string", "enum": ["v1.0.0-cognitive", "v3.0.0-mos"] },
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
                    "extracted_spans"
                ],
                "properties": {
                    "proposal_id": { "type": "string" },
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
                        "items": {
                            "oneOf": [
                                { "type": "string" },
                                {
                                    "type": "object",
                                    "required": ["span"],
                                    "properties": {
                                        "message_id": { "type": "string" },
                                        "span": { "type": "string" }
                                    }
                                }
                            ]
                        },
                        "minItems": 1
                    },
                    "rationale": { "type": "string" },
                    "self_assessed_confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                    "proposed_relation": {
                        "type": "object",
                        "properties": {
                            "type": { "type": "string" },
                            "related_memory_id": { "type": ["string", "null"] }
                        }
                    }
                }
            }
        },
        "message_disposition": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["message_id", "disposition"],
                "properties": {
                    "message_id": { "type": "string" },
                    "disposition": { "type": "string", "enum": ["EXTRACTED", "DISCARDED", "UNPROCESSED"] },
                    "reason": { "type": "string" }
                }
            }
        }
    }
}


class PromptManager:
    """
    认知整理提示词与 Schema 约束管理器。
    """

    PROMPT_VERSION = "v3.0.0-mos"

    @classmethod
    def get_system_prompt(cls, context_pack: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        生成注入了《记忆治理宪法五原则》与只读 Memory Context Pack 的 System Prompt。
        """
        pack_text = ""
        if context_pack:
            pack_lines = []
            for idx, mem in enumerate(context_pack, 1):
                pack_lines.append(
                    f"[{idx}] ID: {mem.get('memory_id')} | Status: {mem.get('status')} | "
                    f"Subject: {mem.get('subject')} | Predicate: {mem.get('predicate')} | "
                    f"Content: {mem.get('content')}"
                )
            pack_text = "\n".join(pack_lines)
        else:
            pack_text = "(无历史相关记忆)"

        prompt = f"""你是由 AMR 记忆操作系统 (MOS) 调度的无状态认知计算工人 (Cognitive Worker)。
你只有【提纯建议权】，零直接写权限。你的所有输出都将被 Evidence Validator 和 Coverage Validator 门禁严格校验。

{CONSTITUTION_FIVE_PRINCIPLES}

---
【Memory Context Pack (只读背景，去重截断在 30 条内)】
{pack_text}
---

请分析输入窗口中的增量会话 raw_messages，提取真正值得长期记住的事实，输出严格的 JSON 格式（符合 JSON Schema）。
必须做到：
1. 每一条 candidate 中的 extracted_spans 必须在 raw_messages 原文中真实存在（一字不改），否则整条将被判定为幻觉抛弃！
2. 每一个输入的原始 message_id 必须在 message_disposition 中被声明为 EXTRACTED、DISCARDED 或 UNPROCESSED 之一（守恒不漏）。
"""
        return prompt.strip()

    @classmethod
    def get_json_schema(cls) -> Dict[str, Any]:
        return DISTILLATION_JSON_SCHEMA

"""
MCP Tools Definition for AI Memory Runtime (v3.0 Multi-Agent Memory Hub).
Defines standard semantic memory tools exposed to AI Agents:
- memory_search
- memory_create
- memory_update
- memory_history
- memory_delete
- memory_get
- memory_record (compat)
- memory_update_status (compat)
- memory_ingest_session

STRICT SAFETY: Zero torch/CUDA imports.
"""

from typing import Any, Dict, List, Optional

TOOL_DEFINITIONS = [
    {
        "name": "memory_search",
        "description": "Semantic search across long-term memories. By default returns current valid facts (status=ACTIVE). Supports history lookup (include_history=true) and domain filtering.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The natural language query or technical problem statement to search for."
                },
                "project_id": {
                    "type": "string",
                    "description": "Optional project identifier to narrow down context (e.g. 'aep-chain', 'global'). Defaults to searching active scope."
                },
                "type": {
                    "type": "string",
                    "description": "Optional category filter. Supports namespace wildcard (e.g. 'decision/*', 'design/arch') or exact type."
                },
                "include_history": {
                    "type": "boolean",
                    "default": False,
                    "description": "When true, includes historical valid facts (ACTIVE + HISTORICAL) to understand design evolution. Defaults to false (current facts only)."
                },
                "status": {
                    "type": "string",
                    "enum": ["ACTIVE", "PENDING_VERIFY", "CONFLICT", "HISTORICAL", "TEMPORARY", "DELETED"],
                    "description": "Optional explicit status filter for governance/audit queries."
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 20,
                    "default": 5,
                    "description": "Maximum number of memory items to return."
                },
                "score_threshold": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "description": "Optional custom cosine similarity score threshold."
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "memory_create",
        "description": "Create a new durable shared memory. AMR automatically tracks version=1, timestamps, and caller agent_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "Substantive knowledge text, architectural decision, rule, or learned fact (min 5 chars)."
                },
                "type": {
                    "type": "string",
                    "default": "general",
                    "description": "Knowledge domain namespace/name (e.g. 'decision/arch', 'lesson/git', 'general')."
                },
                "status": {
                    "type": "string",
                    "enum": ["ACTIVE", "PENDING_VERIFY", "CONFLICT", "TEMPORARY"],
                    "default": "ACTIVE",
                    "description": "Initial lifecycle state. Default ACTIVE. Use PENDING_VERIFY for unverified candidate knowledge."
                },
                "project_id": {
                    "type": "string",
                    "default": "global",
                    "description": "Project identifier. Defaults to 'global'."
                },
                "source_refs": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of source session IDs or document references."
                },
                "conflicts_with": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "If in conflict with existing ACTIVE memories, list target memory_ids. AMR will atomically mark both as CONFLICT."
                }
            },
            "required": ["content"]
        }
    },
    {
        "name": "memory_update",
        "description": "Update an existing memory. Content changes require expected_version and change_reason (triggers version increment). Pure metadata updates (type/status) update in-place without versioning.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "memory_id": {
                    "type": "string",
                    "description": "The unique memory ID to update."
                },
                "content": {
                    "type": "string",
                    "description": "New revised memory content text. Required if updating substantive fact."
                },
                "expected_version": {
                    "type": "integer",
                    "description": "Required when content is provided: the version expected by caller to prevent concurrent overwrite."
                },
                "change_reason": {
                    "type": "string",
                    "description": "Required when content is provided: explanation of why this memory was updated/refined."
                },
                "type": {
                    "type": "string",
                    "description": "Optional updated type namespace."
                },
                "status": {
                    "type": "string",
                    "enum": ["ACTIVE", "PENDING_VERIFY", "CONFLICT", "HISTORICAL", "TEMPORARY", "DELETED"],
                    "description": "Optional updated lifecycle status."
                },
                "conflicts_with": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of memory IDs this record conflicts with."
                }
            },
            "required": ["memory_id"]
        }
    },
    {
        "name": "memory_history",
        "description": "Retrieve the complete revision history timeline for a memory by memory_id, showing content evolution and change reasons.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "memory_id": {
                    "type": "string",
                    "description": "The unique memory ID."
                }
            },
            "required": ["memory_id"]
        }
    },
    {
        "name": "memory_delete",
        "description": "Soft delete a memory (sets status to DELETED). Hidden from standard and historical search, preserved in SQLite for audit and recovery.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "memory_id": {
                    "type": "string",
                    "description": "The memory ID to soft delete."
                },
                "reason": {
                    "type": "string",
                    "description": "Optional explanation for why this memory was deleted."
                }
            },
            "required": ["memory_id"]
        }
    },
    {
        "name": "memory_get",
        "description": "Retrieve precise memory details and current state by memory_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "memory_id": {
                    "type": "string",
                    "description": "The unique memory ID."
                }
            },
            "required": ["memory_id"]
        }
    },
    {
        "name": "memory_record",
        "description": "Legacy compatibility tool for storing durable memory.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "The substantive fact to preserve."},
                "memory_type": {"type": "string", "default": "fact"},
                "scope": {"type": "string", "default": "global"},
                "project_id": {"type": "string"},
                "session_id": {"type": "string"},
                "source_message_ids": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["content"]
        }
    },
    {
        "name": "memory_update_status",
        "description": "Legacy compatibility tool for updating memory status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "memory_id": {"type": "string"},
                "new_status": {"type": "string"},
                "superseded_by": {"type": "string"}
            },
            "required": ["memory_id", "new_status"]
        }
    },
    {
        "name": "memory_ingest_session",
        "description": "Ingest complete raw conversation dialogue messages into local WAL SQLite storage for lossless audit and provenance tracking.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string"},
                "project_id": {"type": "string"},
                "messages": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "message_id": {"type": "string"},
                            "role": {"type": "string", "enum": ["user", "assistant", "system"]},
                            "content": {"type": "string"},
                            "sequence": {"type": "integer"},
                            "timestamp": {"type": "integer"}
                        },
                        "required": ["message_id", "role", "content"]
                    }
                }
            },
            "required": ["session_id", "messages"]
        }
    }
]


def validate_tool_args(name: str, args: Dict[str, Any]) -> None:
    """Validate parameters according to tool definitions."""
    if not isinstance(args, dict):
        raise ValueError("Tool arguments must be a dictionary")

    if name == "memory_search":
        if "query" not in args or not isinstance(args["query"], str) or not args["query"].strip():
            raise ValueError("Parameter 'query' is required and must be a non-empty string.")
        if "limit" in args and args["limit"] is not None:
            if not isinstance(args["limit"], int) or isinstance(args["limit"], bool) or args["limit"] <= 0:
                raise ValueError("Parameter 'limit' must be a positive integer.")
    elif name == "memory_create":
        if "content" not in args or not isinstance(args["content"], str) or not args["content"].strip():
            raise ValueError("Parameter 'content' is required and must be a non-empty string.")
    elif name == "memory_update":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required.")
        if "content" in args and args["content"] is not None:
            if "expected_version" not in args or args["expected_version"] is None:
                raise ValueError("Parameter 'expected_version' is required when updating content.")
            if "change_reason" not in args or not args.get("change_reason"):
                raise ValueError("Parameter 'change_reason' is required when updating content.")
    elif name == "memory_history" or name == "memory_get" or name == "memory_delete":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required.")
    elif name == "memory_record":
        if "content" not in args or not isinstance(args["content"], str) or not args["content"].strip():
            raise ValueError("Parameter 'content' is required and must be a non-empty string.")
    elif name == "memory_update_status":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required.")
        valid_statuses = {"active", "superseded", "archived", "deleted", "ACTIVE", "PENDING_VERIFY", "CONFLICT", "HISTORICAL", "TEMPORARY", "DELETED"}
        new_stat = args.get("new_status")
        if new_stat not in valid_statuses:
            raise ValueError(f"Invalid status '{new_stat}'")
        if new_stat.lower() == "superseded" and not args.get("superseded_by"):
            raise ValueError("Parameter 'superseded_by' is required when new_status is 'superseded'")
    elif name == "memory_ingest_session":
        if "session_id" not in args or not isinstance(args["session_id"], str) or not args["session_id"].strip():
            raise ValueError("Parameter 'session_id' is required.")
        if "messages" not in args or not isinstance(args["messages"], list):
            raise ValueError("Parameter 'messages' must be a list.")
    else:
        raise ValueError(f"Unknown tool name: {name}")

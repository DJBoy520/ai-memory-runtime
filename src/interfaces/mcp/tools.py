"""
MCP Tools Definition for AI Memory Runtime.
Defines the 5 standard semantic memory tools exposed to AI Agents:
- memory_search
- memory_record
- memory_get
- memory_update_status
- memory_ingest_session

STRICT SAFETY: Zero torch/CUDA imports.
"""

from typing import Any, Dict, List, Optional


TOOL_DEFINITIONS = [
    {
        "name": "memory_search",
        "description": "Semantic search across long-term memories, decisions, and knowledge collections. Automatically filters out inactive/superseded/deleted records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The natural language query or technical problem statement to search for."
                },
                "collections": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Collections to search across. Defaults to ['ai_memory']. Options: ['ai_memory', 'crypto_standards', 'project_docs', 'all']."
                },
                "project_id": {
                    "type": "string",
                    "description": "Optional project identifier to narrow down project-specific context (e.g. 'Reduction-Go')."
                },
                "memory_type": {
                    "type": "string",
                    "enum": ["fact", "decision", "rule", "context"],
                    "description": "Optional memory category filter."
                },
                "scope": {
                    "type": "string",
                    "enum": ["global", "project", "agent", "session"],
                    "description": "Visibility scope. Defaults to 'global'."
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
        "name": "memory_record",
        "description": "Explicitly store a durable long-term memory, architectural decision, code standard, or verified fact. Automatically chunks long inputs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The substantive fact, decision, rule, or learning to preserve."
                },
                "memory_type": {
                    "type": "string",
                    "enum": ["fact", "decision", "rule", "context"],
                    "default": "fact",
                    "description": "Classification of the memory."
                },
                "scope": {
                    "type": "string",
                    "enum": ["global", "project", "agent", "session"],
                    "default": "global",
                    "description": "Visibility scope."
                },
                "project_id": {
                    "type": "string",
                    "description": "Associated project identifier."
                },
                "session_id": {
                    "type": "string",
                    "description": "Source conversation/session ID for traceability."
                },
                "source_message_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of specific message IDs that originated this memory."
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Searchable topic keywords or tags."
                }
            },
            "required": ["content"]
        }
    },
    {
        "name": "memory_get",
        "description": "Retrieve precise memory details by memory_id, including raw dialogue message provenance if available.",
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
        "name": "memory_update_status",
        "description": "Update memory lifecycle state across 4 states: active, superseded, archived, deleted. Preserves historical decision timelines.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "memory_id": {
                    "type": "string",
                    "description": "The unique memory ID to transition."
                },
                "new_status": {
                    "type": "string",
                    "enum": ["active", "superseded", "archived", "deleted"],
                    "description": "Target lifecycle state."
                },
                "superseded_by": {
                    "type": "string",
                    "description": "Required when new_status is 'superseded': the new memory_id that replaces this one."
                }
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
                "session_id": {
                    "type": "string",
                    "description": "Unique session identifier."
                },
                "project_id": {
                    "type": "string",
                    "description": "Optional project identifier."
                },
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
                    },
                    "description": "List of dialogue messages to ingest idempotently."
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
        if "limit" in args and (not isinstance(args["limit"], int) or args["limit"] < 1):
            raise ValueError("Parameter 'limit' must be a positive integer.")
    elif name == "memory_record":
        if "content" not in args or not isinstance(args["content"], str) or not args["content"].strip():
            raise ValueError("Parameter 'content' is required and must be a non-empty string.")
    elif name == "memory_get":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required and must be a non-empty string.")
    elif name == "memory_update_status":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required.")
        valid_statuses = ["active", "superseded", "archived", "deleted"]
        if args.get("new_status") not in valid_statuses:
            raise ValueError(f"Parameter 'new_status' must be one of {valid_statuses}.")
        if args["new_status"] == "superseded" and not args.get("superseded_by"):
            raise ValueError("Parameter 'superseded_by' is required when new_status is 'superseded'.")
    elif name == "memory_ingest_session":
        if "session_id" not in args or not isinstance(args["session_id"], str) or not args["session_id"].strip():
            raise ValueError("Parameter 'session_id' is required.")
        if "messages" not in args or not isinstance(args["messages"], list):
            raise ValueError("Parameter 'messages' must be a list.")
    else:
        raise ValueError(f"Unknown tool name: {name}")

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
        "description": "Semantic search across long-term memories (weakly consistent with recent writes while asynchronous projection is pending). By default returns current valid facts (status=ACTIVE). Supports history lookup (include_history=true) and domain filtering.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The natural language query or technical problem statement to search for."
                },
                "project_id": {
                    "type": "string",
                    "description": "Optional project identifier to narrow down context (e.g. 'aep-chain'). When set, scope = [this value] + the server's project_fallback_ids (default 'global','general'); omit to search the whole library."
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
        "description": "Create a new durable shared memory. SQLite committed immediately with projection_status='pending' (asynchronous vector projection; memory_search is weakly consistent). Idempotency conflicts return error code -32006. AMR automatically tracks version=1, timestamps, and caller agent_id.",
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
                    "description": "Optional list of source session IDs or document references. If omitted, the card is stamped 'provenance-missing:<agent>:<utc-ts>' so untraced cards stay auditable — always pass e.g. ['zcode:<session_id>:<message_id>']."
                },
                "conflicts_with": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "If in conflict with existing ACTIVE memories, list target memory_ids. AMR will atomically mark both as CONFLICT."
                },
                "idempotency_key": {
                    "type": "string",
                    "description": "Optional client-generated idempotency key (e.g. UUID) to safely retry memory creation without duplicates. Returns error code -32006 on conflict."
                }
            },
            "required": ["content"]
        }
    },
    {
        "name": "memory_update",
        "description": "Update an existing memory. Content changes require expected_version and change_reason (triggers version increment). Pure metadata updates (type/status) update in-place without versioning. SQLite committed immediately with projection_status='pending' (asynchronous vector projection; memory_search is weakly consistent). Idempotency conflicts return error code -32006.",
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
                },
                "idempotency_key": {
                    "type": "string",
                    "description": "Optional client-generated idempotency key for safe retries."
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
        "description": "Soft delete a memory (sets status to DELETED). SQLite committed immediately with projection_status='pending' (asynchronous vector projection). Hidden from standard and historical search, preserved in SQLite for audit and recovery. Idempotency conflicts return error code -32006.",
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
                },
                "idempotency_key": {
                    "type": "string",
                    "description": "Optional client-generated idempotency key for safe retries."
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
        "description": "Legacy memory tool (kept for compatibility). Now backed by the SSOT ledger: every chunk writes a matching SQLite memories row plus an Outbox task, so records survive daily reconciliation instead of being retired as orphan projections. Prefer memory_create for new integrations.",
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
        "description": "Ingest conversation messages into SQLite. Supply stable message_id values for retry; a session_id is bound to its originating agent and project. Reusing it with a different agent or project is rejected. This tool must be invoked by the client; MCP does not automatically capture conversations.",
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
    },
    {
        "name": "memory_candidates",
        "description": "List evidence-backed proposed memories for review in a project.",
        "inputSchema": {"type": "object", "properties": {"project_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}}, "required": ["project_id"]}
    },
    {
        "name": "memory_approve_candidate",
        "description": "Approve a reviewed project memory candidate and queue it for search indexing. Evidence and project are checked again.",
        "inputSchema": {"type": "object", "properties": {"project_id": {"type": "string"}, "candidate_id": {"type": "string"}}, "required": ["project_id", "candidate_id"]}
    },
    {
        "name": "session_checkpoint",
        "description": "Create a persistent structured session checkpoint and cross-agent handoff containing completed work, in-progress items, and next steps for the project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Session identifier"},
                "project_id": {"type": "string", "description": "Project identifier for project isolation"},
                "completed": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Completed tasks or achievements",
                },
                "in_progress": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "In-progress, pending, or uncompleted items",
                },
                "next_steps": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Recommended next actions for subsequent agents",
                },
                "context": {
                    "type": "object",
                    "description": "Optional environment or task metadata (e.g. branch, modified files)",
                },
                "messages": {
                    "type": "array",
                    "description": "Optional dialogue messages to ingest alongside checkpoint",
                },
                "checkpoint_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["session_id", "project_id", "completed", "next_steps"],
        },
    },
    {
        "name": "session_get_handoff",
        "description": "Retrieve the latest structured cross-agent handoff (completed, in-progress, next steps) for a project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string", "description": "Project identifier to query handoff for"},
                "session_id": {"type": "string", "description": "Optional session identifier to filter"},
                "limit": {"type": "integer", "description": "Maximum number of handoffs to return (default 1)"},
            },
            "required": ["project_id"],
        },
    },
    {
        "name": "memory_session_checkpoint",
        "description": "Alias for session_checkpoint: Create a structured session checkpoint and cross-agent handoff.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string"},
                "project_id": {"type": "string"},
                "completed": {"type": "array", "items": {"type": "string"}},
                "in_progress": {"type": "array", "items": {"type": "string"}},
                "next_steps": {"type": "array", "items": {"type": "string"}},
                "context": {"type": "object"},
                "messages": {"type": "array"},
                "checkpoint_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["session_id", "project_id", "completed", "next_steps"],
        },
    },
    {
        "name": "memory_get_handoff",
        "description": "Alias for session_get_handoff: Retrieve the latest cross-agent handoff for a project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string"},
                "session_id": {"type": "string"},
                "limit": {"type": "integer"},
            },
            "required": ["project_id"],
        },
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
        if len(args["content"].strip()) < 5:
            raise ValueError("Parameter 'content' must be at least 5 characters (min 5 chars).")
        if "idempotency_key" in args and args["idempotency_key"] is not None and not isinstance(args["idempotency_key"], str):
            raise ValueError("Parameter 'idempotency_key' must be a string.")
        if "status" in args and args["status"] is not None:
            raw_status = args["status"]
            valid_create_statuses = {"active", "pending_verify", "conflict", "temporary"}
            if not isinstance(raw_status, str) or raw_status.strip().lower() not in valid_create_statuses:
                raise ValueError(
                    f"Invalid status '{raw_status}'. Parameter 'status' for memory_create must be one of: "
                    "ACTIVE, PENDING_VERIFY, CONFLICT, TEMPORARY."
                )
    elif name == "memory_update":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required.")
        if "idempotency_key" in args and args["idempotency_key"] is not None and not isinstance(args["idempotency_key"], str):
            raise ValueError("Parameter 'idempotency_key' must be a string.")
        if "content" in args and args["content"] is not None:
            if not isinstance(args["content"], str) or not args["content"].strip():
                raise ValueError("Parameter 'content' must be a non-empty string when updating content.")
            if len(args["content"].strip()) < 5:
                raise ValueError("Parameter 'content' must be at least 5 characters (min 5 chars).")
            if "expected_version" not in args or args["expected_version"] is None:
                raise ValueError("Parameter 'expected_version' is required when updating content.")
            if "change_reason" not in args or not args.get("change_reason"):
                raise ValueError("Parameter 'change_reason' is required when updating content.")
        if "status" in args and args["status"] is not None:
            raw_status = args["status"]
            valid_update_statuses = {"active", "pending_verify", "conflict", "historical", "temporary", "deleted"}
            if not isinstance(raw_status, str) or raw_status.strip().lower() not in valid_update_statuses:
                raise ValueError(
                    f"Invalid status '{raw_status}'. Parameter 'status' for memory_update must be one of: "
                    "ACTIVE, PENDING_VERIFY, CONFLICT, HISTORICAL, TEMPORARY, DELETED."
                )
    elif name == "memory_history" or name == "memory_get" or name == "memory_delete":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required.")
        if name == "memory_delete" and "idempotency_key" in args and args["idempotency_key"] is not None and not isinstance(args["idempotency_key"], str):
            raise ValueError("Parameter 'idempotency_key' must be a string.")
    elif name == "memory_record":
        if "content" not in args or not isinstance(args["content"], str) or not args["content"].strip():
            raise ValueError("Parameter 'content' is required and must be a non-empty string.")
    elif name == "memory_update_status":
        if "memory_id" not in args or not isinstance(args["memory_id"], str) or not args["memory_id"].strip():
            raise ValueError("Parameter 'memory_id' is required.")
        valid_statuses = {
            "active",
            "superseded",
            "archived",
            "deleted",
            "pending_verify",
            "conflict",
            "historical",
            "temporary",
        }
        new_stat = args.get("new_status")
        if not isinstance(new_stat, str) or new_stat.strip().lower() not in valid_statuses:
            raise ValueError(f"Invalid status '{new_stat}'")
        if new_stat.strip().lower() == "superseded" and not args.get("superseded_by"):
            raise ValueError("Parameter 'superseded_by' is required when new_status is 'superseded'")
    elif name == "memory_ingest_session":
        if "session_id" not in args or not isinstance(args["session_id"], str) or not args["session_id"].strip():
            raise ValueError("Parameter 'session_id' is required.")
        if "messages" not in args or not isinstance(args["messages"], list):
            raise ValueError("Parameter 'messages' must be a list.")
        for message in args["messages"]:
            if not isinstance(message, dict):
                raise ValueError("Each message must be an object.")
            if not isinstance(message.get("message_id"), str) or not message["message_id"].strip():
                raise ValueError("Each message requires a stable message_id for safe retries.")
            if message.get("role") not in ("user", "assistant", "system"):
                raise ValueError("Each message requires a valid role.")
            if not isinstance(message.get("content"), str):
                raise ValueError("Each message requires string content.")
    elif name in ("memory_candidates", "memory_approve_candidate"):
        for field in (("project_id", "candidate_id") if name == "memory_approve_candidate" else ("project_id",)):
            if not isinstance(args.get(field), str) or not args[field].strip():
                raise ValueError(f"Parameter '{field}' is required.")
        if "limit" in args and (isinstance(args["limit"], bool) or not isinstance(args["limit"], int) or not 1 <= args["limit"] <= 100):
            raise ValueError("Parameter 'limit' must be an integer from 1 to 100.")
    elif name in ("session_checkpoint", "memory_session_checkpoint", "session_get_handoff", "memory_get_handoff"):
        for field in (("session_id", "project_id") if name in ("session_checkpoint", "memory_session_checkpoint") else ("project_id",)):
            if not isinstance(args.get(field), str) or not args[field].strip():
                raise ValueError(f"Parameter '{field}' is required.")
        if name in ("session_checkpoint", "memory_session_checkpoint"):
            for field in ("completed", "next_steps", "in_progress"):
                if field in ("completed", "next_steps") or field in args:
                    if not isinstance(args.get(field), list) or any(not isinstance(item, str) for item in args[field]):
                        raise ValueError(f"Parameter '{field}' must be a list of strings.")
            if "messages" in args:
                validate_tool_args("memory_ingest_session", {"session_id": args["session_id"], "messages": args["messages"]})
        if "limit" in args and (isinstance(args["limit"], bool) or not isinstance(args["limit"], int) or not 1 <= args["limit"] <= 100):
            raise ValueError("Parameter 'limit' must be an integer from 1 to 100.")
    else:
        raise ValueError(f"Unknown tool name: {name}")

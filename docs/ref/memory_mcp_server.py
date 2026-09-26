#!/usr/bin/env python3
"""
Lightweight JSON-RPC 2.0 Stdio MCP Server for AI Memory
Supports tools:
  - memory_search(query, project_id, assistant_id, limit)
  - memory_record(content, project_id, memory_type, importance)
"""
import sys
import json
import hashlib
import time
from pathlib import Path
import torch
from transformers import AutoTokenizer, AutoModel
from qdrant_client import QdrantClient
from qdrant_client.http import models

QDRANT_URL = "http://192.168.30.161:6333"
COLLECTION_NAME = "ai_memory"
MODEL_PATH = "/home/dj/WorkSpaces/openclaw/knowledge-base/models/bge-m3"

_model = None
_tokenizer = None
_device = None
_qdrant = None

def get_engine():
    global _model, _tokenizer, _device, _qdrant
    if _model is None:
        _device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
        _model = AutoModel.from_pretrained(
            MODEL_PATH, 
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32
        ).to(_device)
        _model.eval()
        _qdrant = QdrantClient(url=QDRANT_URL)
    return _tokenizer, _model, _device, _qdrant

def encode_text(text: str):
    tokenizer, model, device, _ = get_engine()
    inputs = tokenizer([text], padding=True, truncation=True, max_length=512, return_tensors="pt").to(device)
    with torch.no_grad():
        out = model(**inputs)
        cls_rep = out.last_hidden_state[:, 0]
        norm_rep = torch.nn.functional.normalize(cls_rep, p=2, dim=1)
        return norm_rep.cpu().to(torch.float32).tolist()[0]

def do_search(query: str, project_id: str = None, assistant_id: str = None, limit: int = 5):
    _, _, _, qdrant = get_engine()
    query_vec = encode_text(query)

    must_conditions = [{"key": "status", "match": {"value": "active"}}]
    if project_id:
        must_conditions.append({"key": "project_id", "match": {"value": project_id}})
    if assistant_id:
        must_conditions.append({"key": "assistant_id", "match": {"value": assistant_id}})

    filter_obj = models.Filter(must=must_conditions) if must_conditions else None

    try:
        hits = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vec,
            query_filter=filter_obj,
            limit=limit,
            with_payload=True
        ).points
    except Exception:
        hits = qdrant.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_vec,
            query_filter=filter_obj,
            limit=limit,
            with_payload=True
        )

    results = []
    for h in hits:
        p = h.payload or {}
        results.append({
            "score": round(float(h.score), 4),
            "assistant_id": p.get("assistant_id"),
            "occurred_at": p.get("occurred_at"),
            "project_id": p.get("project_id"),
            "memory_type": p.get("memory_type"),
            "session_title": p.get("session_title"),
            "session_id": p.get("session_id"),
            "content": p.get("content")
        })
    return results

def do_record(content: str, project_id: str = "general", memory_type: str = "knowledge", importance: int = 3):
    _, _, _, qdrant = get_engine()
    vec = encode_text(content)
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    mem_hash = hashlib.sha256(f"openclaw_mcp_{time.time()}_{content[:20]}".encode()).hexdigest()[:16]
    memory_id = f"mem_openclaw_{mem_hash}"
    point_id = hashlib.md5(memory_id.encode()).hexdigest()

    payload = {
        "memory_id": memory_id,
        "content": content,
        "memory_type": memory_type,
        "scope": "global",
        "project_id": project_id,
        "assistant_id": "openclaw",
        "assistant_model": "openclaw-agent",
        "session_id": "openclaw_mcp_session",
        "session_title": "OpenClaw MCP Recorded Memory",
        "source_message_ids": [],
        "importance": importance,
        "confidence": 0.95,
        "status": "active",
        "occurred_at": now_str,
        "ingested_at": now_str
    }

    qdrant.upsert(
        collection_name=COLLECTION_NAME,
        points=[models.PointStruct(id=point_id, vector=vec, payload=payload)]
    )
    return {
        "status": "success",
        "memory_id": memory_id,
        "occurred_at": now_str,
        "project_id": project_id
    }

def handle_rpc():
    """Stdio JSON-RPC 2.0 消息处理循环"""
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except Exception:
            continue

        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if method == "initialize":
            res = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "ai-memory-service",
                        "version": "1.0.0"
                    }
                }
            }
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()

        elif method == "tools/list":
            res = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
                        {
                            "name": "memory_search",
                            "description": "跨 AI 助手统一检索 Qdrant 中的历史记忆、架构方案、配置与老板偏好 (基于本地 BGE-M3 语义向量)",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "query": {"type": "string", "description": "要搜索的自然语言问题或主题"},
                                    "project_id": {"type": "string", "description": "可选项目域过滤 (如 crypto-infrastructure, network-infra, eth-key-fitting)"},
                                    "assistant_id": {"type": "string", "description": "可选助手过滤 (如 hermes, openclaw, dsh)"},
                                    "limit": {"type": "integer", "description": "返回最多几条结果，默认 3"}
                                },
                                "required": ["query"]
                            }
                        },
                        {
                            "name": "memory_record",
                            "description": "将新的重要定案、架构决策、事实经验记录进统一 Qdrant 知识库",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "content": {"type": "string", "description": "要记录的完整事实、决策或经验内容"},
                                    "project_id": {"type": "string", "description": "所属项目域 (默认 general)"},
                                    "memory_type": {"type": "string", "enum": ["decision", "knowledge", "preference", "fact"], "description": "记忆类型"},
                                    "importance": {"type": "integer", "description": "重要性评级 1-5，默认 3"}
                                },
                                "required": ["content"]
                            }
                        }
                    ]
                }
            }
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()

        elif method == "tools/call":
            tool_name = params.get("name")
            args = params.get("arguments", {})

            try:
                if tool_name == "memory_search":
                    query = args.get("query", "")
                    proj = args.get("project_id")
                    asst = args.get("assistant_id")
                    limit = args.get("limit", 3)
                    hits = do_search(query, proj, asst, limit)
                    output_text = json.dumps(hits, ensure_ascii=False, indent=2)
                elif tool_name == "memory_record":
                    content = args.get("content", "")
                    proj = args.get("project_id", "general")
                    mtype = args.get("memory_type", "knowledge")
                    imp = args.get("importance", 3)
                    rec = do_record(content, proj, mtype, imp)
                    output_text = json.dumps(rec, ensure_ascii=False, indent=2)
                else:
                    output_text = f"未知工具: {tool_name}"

                res = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": output_text}
                        ]
                    }
                }
            except Exception as e:
                res = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32603,
                        "message": str(e)
                    }
                }
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()
        else:
            # notifications 或未识别方法
            pass

if __name__ == "__main__":
    handle_rpc()

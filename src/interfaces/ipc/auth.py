"""IPC Agent 授权与身份规范化模块（RFC v4 §C3 / RFC v5.2）。"""
from typing import Any, Dict, Optional, Tuple
from src.interfaces.ipc.errors import RpcAgentUnauthorized


def authorize_agent(config: Any, params: Optional[Dict[str, Any]] = None) -> Tuple[str, Optional[str]]:
    """校验 Agent 身份并规范化 agent_id 与 instance_id。

    规则：
    - agent_id = (params.get("agent_id") or params.get("source_agent") or "system")，
      支持前后空格去除与大小写归一化（小写）；
    - 若不在 config.server.allowed_agents（大小写不敏感比较）中：
      抛出 RpcAgentUnauthorized(message 包含 AGENT_UNAUTHORIZED，data={"agent_id": ...})；
    - instance_id = params.get("instance_id") or params.get("client_ref") or None（去空格）。

    返回：
        (agent_id, instance_id)
    """
    params = params or {}

    raw_agent = params.get("agent_id")
    if raw_agent is None or (isinstance(raw_agent, str) and not raw_agent.strip()):
        raw_agent = params.get("source_agent")
    if raw_agent is None or (isinstance(raw_agent, str) and not raw_agent.strip()):
        raw_agent = "system"

    agent_id = str(raw_agent).strip().lower()

    allowed_list = getattr(getattr(config, "server", None), "allowed_agents", [])
    allowed_agents = {str(a).strip().lower() for a in allowed_list}

    if agent_id not in allowed_agents:
        raise RpcAgentUnauthorized(
            f"AGENT_UNAUTHORIZED: Unknown or untrusted agent_id '{agent_id}'",
            data={"agent_id": agent_id},
        )

    raw_instance = params.get("instance_id")
    if raw_instance is None or (isinstance(raw_instance, str) and not str(raw_instance).strip()):
        raw_instance = params.get("client_ref")

    if raw_instance is not None:
        cleaned_instance = str(raw_instance).strip()
        instance_id = cleaned_instance if cleaned_instance else None
    else:
        instance_id = None

    return agent_id, instance_id

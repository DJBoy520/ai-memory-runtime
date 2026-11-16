"""IPC Agent 授权与身份规范化模块（RFC v4 §C3 / RFC v5.2）。"""
import json
import os
import stat
from typing import Any, Dict, Optional, Tuple
from src.interfaces.ipc.errors import RpcAgentUnauthorized, RpcRegistryUnavailable


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

    credentials_file = os.environ.get('AMR_AGENT_CREDENTIALS_FILE')
    if credentials_file:
        try:
            fd = os.open(credentials_file, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
            with os.fdopen(fd, 'r', encoding='utf-8') as f:
                info = os.fstat(f.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) not in (0o400, 0o600) or info.st_size > 1024 * 1024:
                    raise ValueError('invalid credentials')
                credentials = json.load(f)
            if not isinstance(credentials, dict) or not credentials:
                raise ValueError('invalid credentials')
            names = [entry['agent_id'] for entry in credentials.values()]
            if any(not isinstance(n, str) or not n or n != n.strip().lower() for n in names):
                raise ValueError('invalid identities')
            allowed_agents = set(names) | ({'system'} if 'system' in allowed_agents else set())
        except (OSError, ValueError, TypeError, KeyError, UnicodeError):
            raise RpcRegistryUnavailable('active agent registry unavailable') from None

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

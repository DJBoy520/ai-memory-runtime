# ./secrets/ 目录内容（部署时手工准备，严禁提交 git / 烘焙进镜像）

| 文件 | 用途 | 来源 |
| --- | --- | --- |
| `internal.token` | gateway → daemon 业务 socket 内部 token | Ubuntu：`~/.config/amr/internal.token` |
| `admin.token` | daemon 管理 socket token | Ubuntu：`~/.config/amr/admin.token` |
| `credentials.json` | {token: {agent_id, tools}} 全量身份映射（gateway 校验 + daemon 读取启用身份） | Ubuntu：`~/.config/amr/agents/current/credentials.json` |
| `tls/server-chain.crt` | 网关 TLS 证书链（amr-unraid.djper.cn） | `scripts/make-unraid-cert.sh` 生成 |
| `tls/server.key` | 网关 TLS 私钥 | 同上，0600 |
| `tls/root.crt` | 新私有根 CA（分发给所有连 Unraid 网关的客户端做 `ca_file`） | 同上 |

权限要求：目录 0700、文件 0600、属主 1000:1000（容器内 uid 1000 需可读）。

规则（与仓库记忆治理一致）：
- token/api_key/私钥只允许以文件形式存在于部署目录，禁止写入 git、镜像、聊天记录；
- 只允许在文档中记录「密钥存放于哪个文件」这类位置描述；
- 轮换用 `python -m src.agent_access.cli rotate <agent>`（在任一持有 agents 注册表的机器上执行后同步 credentials.json）。

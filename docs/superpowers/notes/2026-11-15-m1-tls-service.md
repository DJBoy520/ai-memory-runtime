# M1：可启动的认证 HTTPS MCP 服务（2026-11-15）

状态：已完成并通过隔离测试。**未部署、未修改生产服务/配置/数据、未提交 git。**

## 交付内容

| 文件 | 作用 |
| --- | --- |
| `src/interfaces/mcp/remote_service.py` | 离线配置校验：显式 TLS/凭证/套接字，新增 `bind_host`、`bind_port`、`allowed_origins` |
| `src/interfaces/mcp/remote_server.py` | 服务端 TLS 材料校验 + 进程内 uvicorn 生命周期（`start`/`stop`/`serve`） |
| `src/interfaces/mcp/remote_runtime.py` | 装配时复用同一个 `TokenAuthenticator`，不构造第二套认证 |
| `tests/test_remote_server.py` | 真实 TLS 端到端、认证、明文拒绝、证书链/主机名、生命周期边界 |
| `tests/test_remote_service.py` | 配置字段、权限、路径与可选网络参数的 fail-closed 校验 |
| `requirements-remote.txt` | 远程 MCP 独立依赖；`mcp==1.30.0` 精确锁定 |

## 配置契约

必填：`business_socket`、`admin_socket`、`credentials_file`、`cert_file`、
`key_file`、`allowed_hosts`。

可选：

- `bind_host`：默认 `127.0.0.1`；必须是裸主机名/IP，不含空白、URL 或路径语法。
- `bind_port`：默认 `8443`；整数 0–65535（`0` 仅测试用，实际端口通过
  `bound_port` 读取）。
- `allowed_origins`：默认 `None`（沿用 `build_app` 的回环来源默认值）；出现时
  必须是非空 `list[str]`，严格 `scheme://host[:port]` 语法由 `build_app`
  在监听前校验。

凭证文件：JSON 对象 `{token: {"agent_id": str, "tools": [str, ...]}}`，权限必须
为 `0600` 或 `0400`。令牌不会出现在配置对象的 `repr`/`str`、日志或错误信息中。

示例（路径与令牌均为占位，不得直接用于生产）：

```json
{
  "business_socket": "/run/user/1000/qdrant-bge.sock",
  "admin_socket": "/run/user/1000/qdrant-bge-admin.sock",
  "credentials_file": "/etc/amr/remote-credentials.json",
  "cert_file": "/etc/amr/tls/server.crt",
  "key_file": "/etc/amr/tls/server.key",
  "allowed_hosts": ["amr.internal", "amr.internal:8443"],
  "allowed_origins": ["https://amr.internal"],
  "bind_host": "127.0.0.1",
  "bind_port": 8443
}
```

## 启动方式（当前仅编程入口）

```python
from src.interfaces.mcp.remote_server import RemoteMcpService

service = RemoteMcpService.from_config_file("/etc/amr/remote-service.json")
await service.start()
try:
    ...  # service.bound_port 可读取实际端口
finally:
    await service.stop()
```

尚未提供 `python -m ...` CLI、systemd unit 或容器入口；这些属于部署阶段，
应和 Unraid/Docker、密钥挂载、健康检查一起做，不在 M1 范围内。

## 失败关闭规则

- 证书/私钥缺失、不可读、损坏或不匹配 → 构造阶段 `TLSMaterialError`。
- 证书尚未生效或已过期 → 构造阶段 `TLSMaterialError`。
- 凭证文件权限不是 `0600/0400` → `load_config` 阶段 `ValueError`。
- `business_socket` 与 `admin_socket` 相同（含 realpath 软链别名）→ 拒绝。
- 仅 TLS 监听，不提供明文开关；TLS 最低版本 1.2。
- 无效 `allowed_origins` 在 `build_app` 阶段失败，早于绑定监听。
- 端口被占用或 lifespan 启动失败：uvicorn 会 `sys.exit()`；服务内部捕获并
  转换为 `RemoteServerError`，**不会**把 `SystemExit` 抛给宿主进程。

## 测试证据（2026-11-15）

```bash
# 隔离 venv，全部远程测试，无 skip
AMR_REQUIRE_REMOTE=1 /tmp/amr-remote-venv/bin/python -m pytest \
  tests/test_remote_*.py tests/test_http_auth.py -q
# → 376 passed

# 生产 venv，全量回归（远程测试按设计 skip）
.venv/bin/python -m pytest -q
# → 730 passed, 11 skipped
```

隔离 venv 的创建方式见
[远程 MCP 测试环境](2026-11-15-remote-test-env.md)。

## 已知边界与后续

1. **未实现** Ubuntu 本地 UDS → 远程 MCP 适配器（现有插件的 `memory.search`、
   `session.ingest` 等调用尚未改走远程服务）。
2. **未实现** 离线队列与远程服务的衔接；现有客户端 spool 语义保持不变。
3. **未提供** CLI/systemd/Compose/Unraid 部署、健康检查与迁移回滚演练。
4. 私钥文件权限未在启动期强制（容器 secret 常以 `0444` 挂载）；建议部署层
   用文件系统权限或 secret 管理保证，部署文档中需写明。
5. `ssl._ssl._test_decode_cert` 是 CPython 私有 API，仅承诺 CPython 3.12；
   升级 Python 时需回归证书有效期检查。
6. `mcp` 必须保持 `<2`；2.0 移除了 `streamablehttp_client` 并修改了
   `StreamableHTTPSessionManager` 签名。

## 下一步建议顺序

1. 本地 UDS 适配器：固定身份、方法白名单、复用 Gateway（不做第二套队列）。
2. 离线归档与幂等重试：区分 `queued` 与已提交，覆盖响应丢失场景。
3. 部署物：CLI 入口、systemd/Compose、TLS 与凭证样例、健康检查、备份恢复。
4. 在目标 Unraid 上独立验证 TLS、P4、持久化、切换与回滚后再迁移。

# AMR 2.x 会话交接文档

- **交接日期**：2026-11-16
- **交接对象**：Codex（接手后续开发）
- **当前分支**：`release/2.x`，HEAD `33abbae`
- **1.x 基线**：`main` / `release/1.x`，HEAD `e4d8677`（`v5.1.0-complete-consolidation`）
- **工作区**：干净（`git status` 无改动）
- **生产状态**：`amr.service` active，仍是 1.x；生产配置/数据未改
- **推送状态**：2.x 所有提交只在本地，**未 push**

---

## 1. 一句话结论

2.x 的核心开发已完成并通过交叉审计与测试：认证 HTTPS MCP 服务、UDS 适配器、
stdio shim、四个仓库内插件 2.0、标准客户端接入模板都已落地。
**未完成的是目标机部署、真实客户端联调、迁移/回滚演练和正式 `v2.0.0` 发布。**

---

## 2. 先读哪些文档

按顺序读：

1. [DOC-AMR-08 版本线](DOC-AMR-08-VERSIONING.md)：为什么有 v5 标签又有 1.x/2.x。
2. [DOC-AMR-09 2.x 客户端契约](DOC-AMR-09-CLIENT-2X.md)：两种接入方式、错误码、spool 语义。
3. [DOC-AMR-10 2.x 部署草案](DOC-AMR-10-DEPLOY-2X.md)：systemd、客户端、迁移/回滚。
4. [2.x 架构草案](superpowers/specs/2026-11-15-amr2-architecture.md)：拓扑与安全边界。
5. [交叉审计记录](superpowers/notes/2026-11-15-cross-audit-log.md)：Gemini/DeepSeek 发现、判定与修复。
6. [M1 TLS 服务说明](superpowers/notes/2026-11-15-m1-tls-service.md)、
   [测试环境](superpowers/notes/2026-11-15-remote-test-env.md)、
   [适配器待决问题](superpowers/notes/2026-11-15-uds-adapter-open-questions.md)。

---

## 3. 已完成模块与入口

| 模块 | 关键文件 | 说明 |
| --- | --- | --- |
| HTTPS MCP 服务 | [remote_server.py](../src/interfaces/mcp/remote_server.py)、[remote_service.py](../src/interfaces/mcp/remote_service.py)、[remote_runtime.py](../src/interfaces/mcp/remote_runtime.py)、[remote_main.py](../src/interfaces/mcp/remote_main.py) | TLS 1.2+、证书/私钥匹配与有效期、`start/stop`、`/mcp` + `/healthz`、CLI `--check` |
| 鉴权/错误 | [remote_auth.py](../src/interfaces/mcp/remote_auth.py)、[remote_mcp.py](../src/interfaces/mcp/remote_mcp.py)、[http_auth.py](../src/interfaces/mcp/http_auth.py) | 单一 `TokenAuthenticator`、SHA-256 预哈希、32 字符下限、结构化 `amr_error` |
| UDS 适配器 | [src/interfaces/adapter/](../src/interfaces/adapter/) | mapping/config/client/server/main；每身份 0600 socket、固定身份、错误码还原、1.x notify 兼容、无自动重试 |
| stdio shim | [bridge2.py](../src/interfaces/mcp/bridge2.py) | ZCode/DSH 等仅 stdio 客户端；不读生产配置、不注入身份、不暴露 spool/admin |
| DSH 插件 2.0 | [plugins/dsh-amr/](../plugins/dsh-amr/) | spool、replay、turn 去重、`amr.ping` |
| OpenCode 插件 2.0 | [plugins/opencode-amr/](../plugins/opencode-amr/) | adapter socket、persist-first spool |
| OpenClaw 插件 2.0 | [plugins/openclaw-amr/](../plugins/openclaw-amr/) | 现有 spool + 2.x 错误分类、容量上限、in-flight replay 防重 |
| Hermes 插件 2.0 | [plugins/hermes-amr/](../plugins/hermes-amr/) | `_client.py` 错误类型、`_spool.py` JSONL、persist-first |
| 部署模板 | [deploy/](../deploy/) | systemd、客户端配置、SSH streamlocal |
| 配置样例 | [config/examples/](../config/examples/) | 远程服务、适配器、凭证（占位令牌） |

---

## 4. 关键设计决策（不要回退）

1. **1.x/2.x 是接入架构版本线，不是存储 schema 版本**。SQLite schema / Qdrant
   集合 / `memory.*` 语义保持 v5；历史 `v5.x` 标签保留。2.x 数据可直接打开 1.x 备份，
   但任何时刻只允许一个写者。
2. **TLS 由 MCP 服务直接终止**（uvicorn + `ssl_context_factory`），不提供明文开关；
   默认 `127.0.0.1:8443`，跨机需显式 `bind_host` + 私网/防火墙。
3. **只有一个 `TokenAuthenticator`**。配置层与 HTTP 中间件/Gateway 复用同一实例；
   不要在配置层再写第二套认证器。
4. **令牌比较先 SHA-256**，避免长度时序泄露；远程服务配置要求令牌 >= 32 字符。
5. **daemon 错误走结构化信封**：`structuredContent.amr_error` 只含
   `code/message_class/retryable/data` 白名单字段；原始异常文本不进入响应。
6. **适配器无状态、不自动重试**。写操作超时返回“状态未知”，由客户端用
   `(session_id, message_id)` / 幂等键安全重试；1.x 无 id 通知仍执行但不回复。
7. **每个固定身份一个 0600 UDS**，socket 父目录必须属主且不可 group/other 写；
   启动前探测已存在监听并拒绝重复实例。
8. **插件 persist-first**：先落 spool 再发 `session.ingest`（带 id），确认后才删除；
   瞬时错误保留、永久错误标记一次并保留；replay 跳过在途 key。
9. **mcp SDK 必须锁 `<2`**：2.0 移除了 `streamablehttp_client` 并改了
   `StreamableHTTPSessionManager` 签名；当前固定 `mcp==1.30.0`。
10. **生产 venv 不装 mcp**；远程测试必须用隔离 venv + `AMR_REQUIRE_REMOTE=1`
    门禁，禁止把 skip 当通过。

---

## 5. 如何跑测试（复现命令）

### 5.1 隔离 venv（当前 `/tmp/amr-remote-venv`，重启后需重建）

```bash
cd /home/dj/WorkSpaces/ai-memory-runtime
~/.local/bin/uv venv --python 3.12 /tmp/amr-remote-venv
~/.local/bin/uv pip install \
  --python /tmp/amr-remote-venv/bin/python \
  -r requirements-remote.txt
```

### 5.2 远程/适配器/shim

```bash
AMR_REQUIRE_REMOTE=1 /tmp/amr-remote-venv/bin/python -m pytest \
  tests/test_remote_*.py tests/test_http_auth.py \
  tests/test_adapter_*.py tests/test_bridge2.py -q
```

交接时结果：**495 passed**。

### 5.3 生产 venv 全量回归

```bash
.venv/bin/python -m pytest -q
```

交接时结果：**837 passed, 15 skipped**（远程模块按设计 skip；`test_bridge2`、
adapter config/mapping 等不依赖 mcp 的测试会运行）。

### 5.4 四个插件

```bash
cd plugins/dsh-amr
PATH="/home/dj/.nvm/versions/node/v24.22.1/bin:$PATH" node --test tests/*.test.js

cd ../opencode-amr
PATH="/home/dj/.nvm/versions/node/v24.22.1/bin:$PATH" node --test tests/*.js

cd ../openclaw-amr
PATH="/home/dj/.nvm/versions/node/v24.22.1/bin:$PATH" node --test tests/test_*.js

cd /home/dj/WorkSpaces/ai-memory-runtime
.venv/bin/python -m pytest plugins/hermes-amr/test_amr_plugin.py -q
```

交接时结果：DSH **262**、OpenClaw **164**、OpenCode **15**、Hermes **23**，全绿。

### 5.5 服务/生产检查

```bash
systemctl --user is-active amr.service
systemctl --user status amr.service --no-pager
```

当前为 **active**；1.x 生产未受影响。

---

## 6. 已完成的验证证据

- **真实 TLS 端到端**：临时 CA/证书、`RemoteMcpService`、临时 daemon、真实
  MCP 客户端；覆盖 initialize/list/call、401、明文拒绝、错误 CA/主机名、stop 释放端口。
- **适配器端到端**：临时 UDS 客户端 → 适配器 → HTTPS MCP → 临时 daemon；
  覆盖身份绑定、`VERSION_CONFLICT` 保真、-32010/32012、通知、并发、重复启动、
  大响应、恶意 socket 占位。
- **bridge2 全链路**：`bridge2 → 适配器 → HTTPS MCP → daemon`，身份由令牌绑定，
  错误码穿过整条链。
- **标准客户端隔离实测**（临时证书、隔离 HOME/XDG，未改生产配置）：
  - Claude Code 2.1.322：`claude mcp list` → `✔ Connected`
  - Codex 0.167.0：`codex mcp add --url ... --bearer-token-env-var` 成功，列表 enabled
  - OpenCode 1.19.5：`opencode mcp list` → `✓ amr connected`
- **交叉审计**：Gemini 分块 + DeepSeek 子 agent；逐条判定 accepted/rejected，
  记录在 [交叉审计记录](superpowers/notes/2026-11-15-cross-audit-log.md)。

---

## 7. 未完成 / 已知边界（按优先级）

### P1：部署与目标机验证

1. **Unraid/Docker 未完成**：仓库只有 systemd 模板和部署草案，没有 Dockerfile/Compose，
   也没有在 Unraid 上验证 TLS、Tesla P4/CUDA、持久卷、Qdrant、调度任务。
2. **迁移/回滚未演练**：单写者冻结、SQLite/WAL 一致备份、Qdrant 快照、切换与回滚
   只写在文档里；未在目标机执行。
3. **正式 `v2.0.0` 未打标签**：只有 `v2.0.0-dev.1`；发布条件见 DOC-AMR-08。

### P1：真实客户端联调

4. **ZCode/DSH 真实进程未连真实适配器**：`bridge2` 全链路测试已通过，但还没有
   部署适配器后让真实 ZCode/DSH 进程跑一轮工具调用。
5. **Doubao/Windows 未实测**：只有 SSH streamlocal 模板；未验证跨机 UDS 转发。
6. **标准客户端只验证了“连接/工具发现”**：Claude/Codex/OpenCode 的 MCP 连接已通，
   未在真实模型会话中验证记忆注入/写入行为。

### P2：代码边界

7. **私钥权限未在启动期强制**：容器 secret 常以 `0444` 挂载；当前只校验可读/匹配/
   有效期，部署层需用 secret 管理或文件系统权限保证。
8. **OpenClaw 多实例单例挂载**：`plugin.persistence` 是导出单例上的属性，重复
   `register` 会覆盖为最后一个实例；OpenClaw 通常单进程单注册，未作为阻塞项。
9. **OpenCode 缺省 timestamp**：无 `createdAt` 时用 `Date.now()`；幂等依赖
   `message_id`，不影响去重，但同一快照重放的时间戳不稳定。
10. **Hermes 旧 `strict_200`**：已标记为 1.x-only 并在 2.x pytest 跳过；1.x 证据
    请在 `release/1.x` 上跑。

### P2：仓库/发布

11. **未 push、未开 PR**；`main` 保持 1.x，2.x 只在 `release/2.x`。
12. **测试污染历史**：OpenClaw 测试曾写用户 spool；24 条记录已移动到
    `/tmp/amr-test-contamination-openclaw-20261116/`（未删除），用户 spool 已清空；
    测试已改为每测试独立临时目录。请复核后再决定是否删除隔离目录。

---

## 8. 重要事故与清理记录

- **测试污染**：`~/.local/share/openclaw-amr/sessions` 曾出现 24 条测试记录，
  已整体移动到 `/tmp/amr-test-contamination-openclaw-20261116/`；目录当前为空。
- **临时环境**：`/tmp/amr-remote-venv`、`/tmp/amr2-*` 均为临时；重启后按 §5.1 重建。
- **未触碰**：生产 `config/config.yaml`、`amr.service`、AMR SQLite/Qdrant 数据、
  各 agent 的真实配置（探测使用隔离 HOME/XDG）。
- **未 push**：2.x 提交只在本地 `release/2.x`。

---

## 9. 建议的后续顺序

1. **Review & 回归**：读审计记录与 DOC-AMR-08/09/10；跑 §5 全部命令，确认 495 /
   837 / 插件计数一致；检查 `git status` 干净。
2. **决定发布路径**：是否 push `release/2.x`、是否开 PR；`main` 暂不合入 2.x。
3. **部署 Unraid/Ubuntu**：补 Dockerfile/Compose（AMR daemon + amr2-mcp + Qdrant），
   systemd 或容器二选一；加 `/healthz` 健康检查；验证 TLS、P4/CUDA、持久卷。
4. **真实客户端联调**：先启动适配器，再让 ZCode/DSH/Doubao/OpenClaw 各跑一轮
   检索 + 归档；保留 1.x 回退 socket/配置。
5. **迁移演练**：停 1.x 写者 → 备份 → 起 2.x → 验证工具/会话/错误码 → 回滚演练；
   记录写入丢失边界。
6. **发布**：满足 DOC-AMR-08 条件后打 `v2.0.0`；更新 README/版本矩阵。

### 每步验收建议

- 部署：`--check` 通过、`/healthz` 200、`/mcp` 无 token 401、真实 token 可 list/call。
- 客户端：每个 agent 至少一次 `memory_search` 成功、一次 `session.ingest` 确认后
  spool 清空、一次 -32010 降级不崩溃。
- 迁移：备份校验和、单写者证据、回滚后 1.x 检索正常。

---

## 10. 交接检查清单

```bash
cd /home/dj/WorkSpaces/ai-memory-runtime

# 1. 分支与工作区
git status --short
git branch -vv
git log --oneline e4d8677..HEAD

# 2. 版本
cat VERSION
git tag -l | sort -V | tail -5

# 3. 生产仍是 1.x
systemctl --user is-active amr.service

# 4. 测试（见 §5）
# 5. 检查没有把 2.x 合入 main
git log --oneline -1 main        # 应为 e4d8677
git log --oneline -1 release/1.x # 应为 e4d8677
```

---

## 11. 提交地图（`e4d8677..33abbae`）

```text
33abbae test(amr2): verify full bridge2→adapter→MCP chain; mark 1.x audit
2675ce9 docs(amr2): record isolated Claude Code/Codex/OpenCode client validation
7a33ae0 feat(amr2): OpenClaw spool capacity guard and client templates
c692063 fix(amr2): isolate OpenClaw tests and skip in-flight replay
6e57aed feat(amr2): service CLIs, health endpoint, and deployment templates
14fe639 feat(amr2): stdio MCP shim and 2.x client docs
613756f feat(amr2): Hermes plugin 2.0 with persist-first spool
8c4658e feat(amr2): OpenClaw plugin 2.0 with adapter client hardening
0555aaf feat(amr2): OpenCode plugin 2.0 with adapter UDS and spool
8c14380 docs(amr2): record adapter re-audit fixes and implementation status
e1fc87c feat(amr2): DSH plugin 2.0 with persist-first ingest and spool
a2a4029 feat(amr2): local UDS adapter with MCP error fidelity
e06a711 feat(amr2): structured daemon errors and token hardening
a3a90d8 feat(amr2): authenticated HTTPS MCP service (M1)
```

---

## 12. 需要你/制作人拍板的问题

1. 2.x 是否 push 到公开远端 / 开 PR？`main` 何时合并？
2. 目标部署是 **Unraid 容器** 还是先 Ubuntu 同机过渡？
3. 证书由 AEP-PKI 还是其他 CA 签发？域名/SAN 是否已定？
4. 各 agent 的令牌轮换/吊销流程由谁负责？
5. 切换窗口与可接受的回滚数据丢失边界。
6. `/tmp/amr-test-contamination-openclaw-20261116/` 复核后是否删除。

---

**交接人备注**：本次所有“已通过”结论都有对应测试命令与审计记录；未在目标机、
真实客户端会话或生产切换中验证的内容已在 §7 明确标出，请不要把模板/草案当成
已部署事实。

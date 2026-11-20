# DOC-AMR-10 — Docker 打包与 Unraid 部署、生产切换 Runbook

状态：2.1.0 起生效（首个容器化发布）。上游架构见 DOC-AMR-09（客户端接入矩阵）、docs/AGENT-ACCESS-OPERATIONS.md（身份/激活）。

## 1. 镜像设计约束

- **不内置 Qdrant**：daemon 通过 `config.yaml` 的 `qdrant.url` 指向外部 Qdrant 服务器（生产 = Unraid 192.168.30.161:6333）。
- **不内置模型与数据**：BGE-M3 以只读卷挂载 `/models/bge-m3`；SQLite SSOT 挂载 `/data/amr`；密钥挂载 `/etc/amr/secrets`。
- **依赖与生产 venv 同版本**：transformers 5.22.2 / qdrant-client 1.20.0 / pydantic 2.14.3 等；torch 2.6.0 仅把 cu124 换成 `+cpu` 轮子（Unraid 宿主无 GPU）。
- **两进程拓扑**：`amr-daemon`（src/main.py）+ `amr-gateway`（src/interfaces/mcp/remote_main），经共享卷 `/run/amr` 传递业务/管理 UDS；gateway 对外暴露 8443。
- **认证链不变**：agent token（credentials.json）→ gateway → internal/admin token → daemon（`AMR_INTERNAL_AUTH_REQUIRED=1`）。

构建：

```bash
docker build -f deploy/docker/Dockerfile \
  --build-arg AMR_REVISION=$(git rev-parse --short HEAD) \
  -t djboy520/amr:2.1.0 -t djboy520/amr:2.1 -t djboy520/amr:latest .
```

## 2. 部署目录布局（Unraid /mnt/user/appdata/amr/）

```
docker-compose.yml      ← deploy/docker/docker-compose.yml 原样复制
config/config.yaml      ← deploy/docker/examples/config.container.yaml.example 改制（0600, 1000:1000）
config/remote.json      ← deploy/docker/examples/remote.container.json.example 改制（0600）
secrets/                ← 见 deploy/docker/examples/secrets.README.md（internal/admin token、credentials.json、tls/）
data/                   ← SQLite SSOT（sessions.db）
models/bge-m3/          ← 从 Ubuntu rsync（至少 config.json、tokenizer*、sentencepiece、pytorch_model.bin）
```

```bash
docker compose pull && docker compose up -d
docker compose ps          # 两个容器 healthy
curl -k https://127.0.0.1:8443/readyz
```

## 3. 生产切换（两批，账本不分叉）

SQLite 是 SSOT 账本：若第一批就直连生产数据，两台 daemon 各写一份账本会分叉。因此：

### 第一批（实验）：openclaw / hermes / pi / dsh → Unraid **实验实例**
- Unraid daemon 用**全新空 sqlite** + 独立集合（`ai_memory_exp` 等）+ 独立证书（`amr-unraid.djper.cn`，`scripts/make-unraid-cert.sh` 生成新私有 CA）。
- Ubuntu 侧新增 `amr2-adapter-exp.service`（remote_url=`https://amr-unraid.djper.cn:8443/mcp`，ca_file=新 root.crt），bindings=这 4 个 agent 的 socket；主 adapter 移除这 4 个 binding。
- `/etc/hosts` 加 `192.168.30.161 amr-unraid.djper.cn`（仅本机生效，不影响公网解析）。
- 验证通过（读写检索、created_by_agent 归属、性能）后进入第二批；回滚 = 删 exp adapter、恢复 binding、重启。

### 第二批（真实切换）：数据迁移 + 其余 agents 一把切
1. 停写窗口（分钟级）：Ubuntu daemon `sqlite3 data/sessions.db ".backup '/tmp/sessions-cut.db'"` → 完整性校验（`PRAGMA integrity_check`）→ rsync 到 Unraid `./data/sessions.db`。
2. Unraid daemon 配置切生产集合（`ai_memory` / `crypto_standards` / `project_docs`）+ 生产 sqlite，`docker compose up -d`。
3. 主 adapter（`~/.config/amr/agents/current/adapter.json`）`remote_url` 切到 `https://amr-unraid.djper.cn:8443/mcp`、`ca_file` 切新 root.crt，重启 `amr2-adapter.service`；逐 agent 实测 `memory_search`/`memory_create`。
4. **Ubuntu 旧网关保留热备**（amr2-mcp.service 不停），观察期后 stop+disable。
5. Ubuntu `amr.service` 改造为 dev/test 实例：独立配置（`data/dev.db` + `ai_memory_dev` 集合 + 独立 socket 路径）；生产 sqlite 快照归档 `data/backups/`。
6. 实验实例下线：exp 集合与 exp adapter 删除。

### 回滚
- 任意时刻把 adapter `remote_url`/`ca_file` 改回旧值（Ubuntu 网关在线期间）→ `systemctl --user restart amr2-adapter` 即回到 Ubuntu 生产。
- 第二批完成后 Ubuntu daemon 已降级 dev，回滚需再次走数据快照（旧网关 + 归档快照），窗口更长——第二批前务必确认第一批实验全绿。

## 4. 已知边界

- reconciliation / distillation 定时任务（systemd timer）容器版暂缺，观察期由 Ubuntu dev 侧继续跑；后续以 cron 侧车补齐。
- `prefer_grpc` 走宿主 6334 端口，Unraid Qdrant 容器必须发布 6334（当前已发布 0.0.0.0）。
- 容器内 daemon/gateway 同 UID（1000），安全边界与 2.0 鉴权文档一致：工具白名单不是项目级数据隔离。
- 镜像 public：内容与公开 GitHub 仓库同源；构建上下文经 .dockerignore 排除全部密钥/数据/模型，push 前按 Phase 验证清单复核。

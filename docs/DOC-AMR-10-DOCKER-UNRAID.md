# DOC-AMR-10 — Docker 打包与 Unraid 部署、生产切换 Runbook

状态：2.1.0 起生效（首个容器化发布）。上游架构见 DOC-AMR-09（客户端接入矩阵）、docs/AGENT-ACCESS-OPERATIONS.md（身份/激活）。
本文档为公开仓库文件，统一使用占位符：Qdrant/宿主 = `192.0.2.50`、Unraid 网关域名 = `amr-unraid.example.net`；部署时以本地配置/环境变量注入真值。

## 1. 镜像设计约束

- **不内置 Qdrant**：daemon 通过 `config.yaml` 的 `qdrant.url` 指向外部 Qdrant 服务器。
- **不内置模型与数据**：BGE-M3 以只读卷挂载 `/models/bge-m3`；SQLite SSOT 挂载 `/data/amr`；密钥挂载 `/etc/amr/secrets`（只读）。
- **依赖与生产 venv 同版本**：transformers 5.22.2 / qdrant-client 1.20.0 / pydantic 2.14.3 / mcp 1.30.0 / uvicorn 0.54.0；torch 2.6.0 仅把 cu124 换成 `+cpu` 轮子。
- **两进程拓扑**：`amr-daemon`（CMD `python src/main.py`，启动前清理残留 socket）+ `amr-gateway`（`src.interfaces.mcp.remote_main`），经共享卷 `/run/amr` 传递业务/管理 UDS；gateway 对外暴露 8443。
- **集合名配置驱动**（2.1.0 修复）：写入路径主集合 = `qdrant.collections[0]`（memory_service / OutboxWorker 统一解析）。实验实例配 `collections: [ai_memory_exp]` 即整体隔离，不会把向量投进生产集合。
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
config/config.yaml      ← deploy/docker/examples/config.container.yaml.example 改制（0600, owner 1000:1000，先于启动设好权限）
config/remote.json      ← deploy/docker/examples/remote.container.json.example 改制（0600, 1000:1000）
secrets/                ← 见 deploy/docker/examples/secrets.README.md
data/                   ← SQLite SSOT（sessions.db）
models/bge-m3/          ← 从构建机 rsync（至少 config.json、tokenizer*、sentencepiece、pytorch_model.bin）
```

```bash
chown -R 1000:1000 config secrets data && chmod 700 config secrets
docker compose pull && docker compose up -d
docker compose ps        # 等两个容器 healthy（BGE-M3 CPU 冷加载约 2 分钟，healthcheck 有 start_period 240s）
curl -k https://127.0.0.1:8443/readyz   # 轮询至全身份 200
# 证书链/SAN/主机名验证（healthcheck 的 -k 只证活，不证 TLS 配置正确）：
curl --cacert secrets/tls/root.crt https://amr-unraid.example.net:8443/healthz
```

## 3. 生产切换（两批，账本不分叉）

SQLite 是 SSOT 账本：若第一批就直连生产数据，两台 daemon 各写一份账本会分叉。因此：

### 第一批（实验）：openclaw / hermes / pi / dsh → Unraid **实验实例**
- Unraid daemon 用**全新空 sqlite** + 集合 `ai_memory_exp`（写入路径随 `collections[0]` 自动隔离）。
- 独立证书：`AMR_UNRAID_CN=<真域名> AMR_UNRAID_IP=<宿主IP> bash scripts/make-unraid-cert.sh`（新私有 CA 自包含；**root.key 留在签发机，不进部署目录/仓库/镜像**）。
- Ubuntu 侧新增 `amr2-adapter-exp.service`（独立配置文件：remote_url=`https://amr-unraid.example.net:8443/mcp`、ca_file=新 root.crt），bindings=这 4 个 agent 的 socket。
- **切换顺序（避免 socket 抢占）**：`systemctl --user stop amr2-adapter` → 确认 9 个 socket 已消失（`ls /run/user/1000/amr2-*.sock`）→ 编辑主 adapter 配置移除这 4 个 binding → 启动主 adapter（剩余 5 binding）→ 启动 exp adapter（4 binding）→ 逐 agent 验证。
- `/etc/hosts` 加 `<宿主IP> <真域名>`（仅本机生效，不影响公网解析）。
- 已知边界：同批 agent token 在两个实例同时有效（凭据爆炸半径扩大），实验网关只暴露内网；实验结束评估是否轮换这 4 个 token（`python -m src.agent_access.cli rotate <agent>`）。candidate 提升（candidate_review）仍写默认集合 `ai_memory`（store 列默认），实验实例**不要**做候选提升操作。
- 回滚 = 反向执行切换顺序，把 binding 还回主 adapter。

### 第二批（真实切换）：数据迁移 + 其余 agents 一把切

前置：第一批实验全绿 + 用户确认。切换前对 Qdrant 生产集合做快照（三个生产集合各一次，可回滚）：
`curl -X POST "http://192.0.2.50:6333/collections/ai_memory/snapshots" -H "api-key: ***"`。

停写迁移协议（保证同一时刻只有一个生产 SQLite 写入者）：
1. `systemctl --user stop amr2-adapter`（切断全部 agent 写入入口）；
2. 观察 Ubuntu daemon 日志至 Outbox 队列排空（无 pending 投影任务）；
3. `systemctl --user stop amr2-mcp amr.service`；reconciliation / distillation timer 临时 stop；
4. `sqlite3 data/sessions.db ".backup '/tmp/sessions-cut.db'"` → `PRAGMA integrity_check` 通过；
5. 停 Unraid 实验实例（`docker compose stop amr-daemon`）；**清掉目标目录残留 `sessions.db-wal/-shm`**；
6. rsync 到 `data/sessions.db.tmp` 后 `mv` 原子替换；目标上再跑一次 `PRAGMA integrity_check`；`chown 1000:1000`；
7. Unraid daemon 配置切生产集合（`ai_memory`/`crypto_standards`/`project_docs`）+ 该 sqlite，`docker compose up -d`，等 healthy + `/readyz` 全 200；
8. 主 adapter（`~/.config/amr/agents/current/adapter.json`）`remote_url`→Unraid 网关、`ca_file`→新 root.crt，`systemctl --user daemon-reload && restart amr2-adapter`；逐 agent 实测 `memory_search`/`memory_create`；
9. reconciliation 先 dry-run（只读核对 SQLite 记录数 vs Qdrant points、outbox 待处理数=0），确认无孤儿误判后再恢复清理类任务。

### 回滚
- adapter `remote_url`/`ca_file` 改回旧值 → 重启 adapter 即回 Ubuntu 生产（Ubuntu 网关热备在线期间）。
- 数据级回滚：恢复 Qdrant 快照 + 归档 sqlite 快照，窗口较长——第二批前务必确认第一批全绿。

## 4. Ubuntu 降级为 dev/test（第二批之后）
1. `amr2-mcp.service`（旧网关）保留热备观察数天 → 稳定后 `stop + disable`。
2. `amr.service` 改独立 dev 配置：`AMR_CONFIG_PATH` 指向 dev 配置（`data/dev.db` + `collections: [ai_memory_dev]` + 独立 socket 路径 `/run/user/1000/amr-dev-*.sock`）；生产 sqlite 快照归档 `data/backups/`。
3. **reconciliation / distillation timer 必须显式带 dev 配置**（`systemctl cat` 逐项核对 `AMR_CONFIG_PATH`/集合名/sqlite 路径），防止残留生产配置造成双向脏写。
4. 实验实例下线：删 exp adapter、exp 集合与 exp sqlite；评估轮换实验 token。

## 5. 已知边界
- reconciliation / distillation 定时任务容器版暂缺，观察期由 Ubuntu dev 侧跑（带 dev 配置）；后续以 cron 侧车补齐。
- 容器访问宿主 Qdrant：compose bridge 网络下直连 `192.0.2.50:6333/6334`（Qdrant 发布在 0.0.0.0）；若宿主 hairpin NAT 受限，改 `network_mode: host` 或 `extra_hosts: ["host.docker.internal:host-gateway"]`。
- `prefer_grpc` 走宿主 6334 端口，Qdrant 容器必须发布 6334。
- 容器内 daemon/gateway 同 UID（1000），安全边界与 2.0 鉴权文档一致：工具白名单不是项目级数据隔离。
- 镜像 public：内容与公开 GitHub 仓库同源；.dockerignore/.gitignore 已覆盖密钥/证书/数据/模型模式，push 前仍按第 6 节复核。
- 文档与示例中的内网 IP/域名一律用占位符；真实值只存在于部署机本地配置（不入 git）。

## 6. push 前复核清单
- [ ] `git grep -nE '192\.168\.|djper|/home/dj' -- deploy docs scripts` 零命中
- [ ] 镜像内 `find /app` 无 token/key/credentials 文件；`docker history` 无敏感 build-arg
- [ ] `docker run --rm djboy520/amr:2.1.0 python -c "import src.main"` 可导入
- [ ] compose 起停冒烟通过（写+搜同 memory_id）
- [ ] 全量 pytest 绿

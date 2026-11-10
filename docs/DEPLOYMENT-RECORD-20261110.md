# AMR v5 生产部署记录（2026-11-10）

## 1. 版本与范围

- **发布提交**: `67f64d1`（main）
- **分支**: `feat/amr-reliability-v5` → `main`（--no-ff）
- **范围**:
  - 阶段 A：SQLite 提交即成功、v3 统一投影队列、单写者 Worker、幂等键、legacy 受控路径、治理链路分派；
  - 阶段 B：模型 single-flight 加载、watchdog/受控退出、preload、idle 禁用、加载指标；
  - 阶段 C：稳定 JSON-RPC 错误码、MCP error.data 透传、auth、request_id/client_ref、admin metrics / dead-letters / replay；
  - 测试隔离防线（生产 socket/库默认拒绝）与迁移/上线文档。
- **迁移预演**: `docs/MIGRATION-REHEARSAL-REPORT.md`（生产快照 1176 行 → 1173 v3 / 3 legacy，quarantine=0）。

## 2. 部署时间线（CST）

| 时间 | 动作 | 结果 |
| --- | --- | --- |
| 00:49:29 | 冷备份生产 DB + config | `data/backups/pre_v5_20261110_004929/`（sessions.db 39M、wal、shm、config.yaml，含 sha256） |
| 00:49:48 | 停止 amr.service | 优雅停止，socket 清理 |
| 00:50:33 | 启动新版本 | `SessionStore` 自动完成 v5 幂等迁移 |
| 00:50:38 | UDS + Outbox Worker 就绪 | business/admin socket、flock 单写者 |
| 00:52:36 | 模型 preload READY | BGE-M3 冷启动 117.9s（GPU） |

## 3. 迁移对账（生产）

- `_amr_migrations`: `v5_projection_class_backfill`（v3=1173，legacy=3）、`v5_outbox_rebuild`（converted_0_tasks）。
- `memories`: 1176 → 1176（新增 1 条部署冒烟软删除行后 1177），分类 v3=1173 / legacy=3。
- `memory_projection_outbox`: 新建 v5 schema，当前 done=2（部署冒烟 create/delete 投影），pending=0，dead_letter=0。
- `qdrant_sync_queue`: 3610 行全部 synced，**原样冻结**，未删除。
- `PRAGMA integrity_check`: ok（预演副本）；生产启动无 `MigrationBlocked`。

## 4. 生产验证结果（官方 MCP Bridge / Admin CLI）

| 检查 | 结果 |
| --- | --- |
| `system.health` | healthy，qdrant_ok=true，模型 READY |
| `memory.create`（v3 + idempotency_key） | 返回 `projection_status=pending`、rev=1、projection_class=v3 |
| `memory.get` 立即读取 | SQLite SSOT 命中，`enrichment=ok`，内容一致 |
| 同 key 同参数重放 | `idempotent_replay=true`，同一 memory_id，无重复写入 |
| 投影 + `memory.search` | 创建后约 1s 可检索（真实 BGE-M3 + 生产 Qdrant） |
| `memory.delete` | `current_status=DELETED`；`memory_get` 显示 DELETED；投影完成后搜索不再返回 |
| 错误码透传 | 不存在 ID `-32005 NOT_FOUND`；异摘要同 key `-32006 IDEMPOTENCY_CONFLICT`（data 含 op/key/existing_memory_id） |
| `status.metrics` | `outbox_lag_seconds=0`、`outbox_backlog=0`、`outbox_dead_letter=0`、`embedding_duration_ms≈82ms` |
| `outbox.list_dead_letters` | total=0 |
| 测试 | main 工作区全量：**337 passed, 5 skipped**；集成分支：336 passed, 6 skipped |
| 独立端到端冒烟 | fake-engine + 内存 Qdrant + tmp UDS：`SMOKE_OK`（含 bridge、去重、版本冲突、删除、legacy、admin） |

## 5. 生产配置变更（`config/config.yaml`，git 忽略，已备份）

```yaml
model:
  idle_timeout_seconds: 0          # 禁用自动卸载，避免冷启动抖动
  preload_on_start: true
  load_timeout_seconds: 300.0      # 实测冷启动约 118s
  request_load_wait_seconds: 25.0
  load_stall_exit_seconds: 420.0   # > load_timeout
  watchdog_exit_enabled: true      # systemd Restart=always 托管
storage:
  idempotency_retention_days: 7
```

## 6. 已知行为变化与边界

1. **写请求提交即成功**：`memory_create/update/delete` 不再在请求路径 embed/Qdrant；返回 `projection_status=pending`。
   - `memory_get` 强一致读 SQLite；`memory_search` 弱一致（通常 ≤1s，取决于 Worker/模型/Qdrant）。
2. **状态规范化**：写入 SQLite 的标准状态为大写 6 态；legacy 行经 `memory_get` 仍映射为小写兼容值。
   `memory_update_status` 现在持久化 `superseded_by` 并校验目标存在（不存在返回 INVALID_ARGUMENT）。
3. **单写者边界**：仅覆盖单机、单 Worker、v3 记忆的 Qdrant 投影；多机/多实例需分布式租约+fencing，不在本次范围。
4. **legacy 队列冻结**：`memory_record` 仍是 legacy 直写例外；旧队列由同一 Worker 排水。
5. **Watchdog**：仅保证卡死可见与受控恢复，不保证强杀 Python/GPU 卡死线程；依赖 systemd 重启。
6. 生产保留 1 条部署冒烟软删除行（`project_id=smoke_v5`，状态 DELETED）及 2 条 done 投影任务；不影响检索。

## 7. 回退

- 代码：`git reset --hard 065b24a`（发布前 main）；或切回旧分支。
- 数据：停止服务后，用 `data/backups/pre_v5_20261110_004929/sessions.db` 覆盖 `data/sessions.db`
  （同时恢复 `config.yaml`），再启动旧代码。
- 触发条件：`MigrationBlocked`、启动失败、Worker 锁冲突循环、dead_letter 持续上升、`python3 scripts/amr_doctor.py` 报告 UNHEALTHY / CRITICAL。
- 详见 `docs/ROLLOUT-AMR-RELIABILITY.md`。

## 8. v5.0.1 独立审计加固部署（2026-11-10 08:25 CST）

- **发布提交**: `3d66710`（main），tag `v5.0.1`；审计详情见 `docs/AUDIT-ROUND-20261110.md`。
- **备份**: `data/backups/pre_v501_20261110_082350/`（DB + WAL/SHM + config）。
- **停机/启动**: 08:24 停止；08:25:30 启动；08:27:51 模型 READY（136.3s < 300s watchdog 阈值）。
- **迁移**: `_amr_migrations` 两条标记不变；v3=1174 / legacy=3（含 1 条部署冒烟行）；outbox 正常排水。
- **验证**:
  - `python3 scripts/amr_doctor.py --json` → overall **healthy**，exit 0；
  - MCP Bridge 生产冒烟：身份伪造被拦截（配置 `test_runner`，客户端传 `hermes` 仍落 `test_runner`）；
    同 key 仅 status 大小写不同 → `idempotent_replay=true`；search 结果 `created_at` 非空；delete 投影后 backlog/lag/dead=0；
  - main 全量测试：**352 passed, 5 skipped**；审计专项：**181 passed**。
- **回退**：代码 `git reset --hard 1e30119`（v5.0.0）；数据用上述 pre_v501 备份恢复。

# AMR 可靠性重构：阶段 A 投影器与迁移上线手册

本文档规定 AMR 阶段 A（写入与投影一致性、单执行者 Outbox Worker、双队列迁移）的生产上线顺序、回退机制、一致性边界与可观测性指标。

---

## 一、上线顺序（严格按序执行）

生产部署必须遵守“**先停机、冷备份、副本预演、再起新服务**”的硬性屏障流程，严禁在线不停机执行模式迁移。

```
[1. 停止 Daemon] ──> [2. 数据库冷备份] ──> [3. 副本盘点与预演] ──> [4. 启动新版本] ──> [5. Doctor 巡检] ──> [6. 指标监控]
```

### 步骤 1：停止守护进程
确保主服务与所有写者停止，释放文件锁与 SQLite 数据库写连接：
```bash
systemctl --user stop amr.service
# 确认进程已完全退出
systemctl --user status amr.service
```

### 步骤 2：生产数据库冷备份
完整备份 SQLite 主文件及 WAL/SHM 临时文件：
```bash
BACKUP_DIR="/home/dj/WorkSpaces/ai-memory-runtime/data/backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
# 使用 sqlite3 backup API 生成一致性快照（服务已停止）
python3 - "$BACKUP_DIR" <<'PY'
import sqlite3, os, sys
src = "/home/dj/WorkSpaces/ai-memory-runtime/data/sessions.db"
dst = os.path.join(sys.argv[1], "sessions.db")
con = sqlite3.connect(src); out = sqlite3.connect(dst); con.backup(out); out.close(); con.close()
PY
cp /home/dj/WorkSpaces/ai-memory-runtime/data/sessions.db-wal "$BACKUP_DIR/" 2>/dev/null || true
cp /home/dj/WorkSpaces/ai-memory-runtime/data/sessions.db-shm "$BACKUP_DIR/" 2>/dev/null || true
sha256sum "$BACKUP_DIR"/* > "$BACKUP_DIR/checksums.txt"
```

### 步骤 3：只读资产盘点与副本演练
使用迁移脚本进行只读盘点与无害演练，检查是否有不可转换的孤儿任务：
```bash
# 1. 资产盘点
PYTHONPATH=. python scripts/migrate_projection_queue.py --db /home/dj/WorkSpaces/ai-memory-runtime/data/sessions.db --inventory

# 2. 副本迁移演练（sqlite3 backup API 复制到临时副本，绝不改动生产库）
PYTHONPATH=. python scripts/migrate_projection_queue.py --db /home/dj/WorkSpaces/ai-memory-runtime/data/sessions.db --rehearse --report docs/MIGRATION-REHEARSAL-REPORT.md
```
> **硬性门禁**：若输出包含 `必须人工确认后才可上线` 或 `quarantine_count > 0`，**严禁启动新服务**！必须由研发介入排查隔离表任务原因。

### 步骤 4：部署新代码并启动守护进程
```bash
# 切换至已验证的集成分支/主分支（本机工作区：/home/dj/WorkSpaces/ai-memory-runtime）
git checkout main
git log --oneline -1   # 确认是本次 release commit

# 启动新版服务（SessionStore 启动时自动并在单事务内完成 _migrate_schema 增量迁移）
systemctl --user start amr.service
```

### 步骤 5：服务健康体检
```bash
# 模型/引擎状态、Qdrant 集合、SQLite 路径
python3 src/admin/cli.py status

# JSON-RPC 健康与错误码冒烟（business socket）
python3 - <<'PY'
import asyncio
from src.interfaces.ipc.protocol import make_jsonrpc_request, read_frame, write_frame
async def main():
    r, w = await asyncio.open_unix_connection("/run/user/1000/qdrant-bge.sock")
    await write_frame(w, make_jsonrpc_request("system.health", {"agent_id": "system"}, 1))
    print(await read_frame(r)); w.close()
asyncio.run(main())
PY
```
确认：`model_state` 为 READY/IDLE、`qdrant_ok` 为 true、无迁移异常日志（`journalctl --user -u amr.service -n 100`）。

### 步骤 6：观测核心指标
```bash
python3 src/admin/cli.py metrics
python3 src/admin/cli.py dead-letters --limit 20
```
观察 `outbox_lag_seconds` 是否逐步归零、`outbox_backlog` 是否下降、`outbox_dead_letter` 是否为 0；
旧队列 `qdrant_sync_queue` 的 pending 任务会被同一 Worker 排水（`migrated to outbox#...` / `synced` 日志）。

---

## 二、回退条件与操作手册

### 1. 触发回退的条件
满足下列任一条件时，立即中止上线并执行回退：
1. 步骤 3 演练报告中 `quarantine_count > 0` 且未经研发人工核准；
2. 服务启动失败，日志中出现 `MigrationBlocked` 阻断异常；
3. `OutboxWorker` 因锁冲突或异常连续崩溃触发 systemd 快速重启限制；
4. 指标中 `outbox_dead_letter` 持续上升且存在不可恢复的底层数据错误；
5. `amr doctor` 报告存储状态处于严重不可用 (CRITICAL)。

### 2. 回退操作步骤
```bash
# 1. 立即停止新服务
systemctl --user stop amr.service

# 2. 从备份目录恢复 SQLite 数据库
cp /data/amr/backups/<backup_timestamp>/amr.db* /data/amr/storage/

# 3. 回滚代码到上一稳定版本
git checkout <previous-stable-tag>

# 4. 重新启动稳定版服务
systemctl --user start amr.service

# 5. 验证服务状态
amr doctor
```

---

## 三、单执行者锁 (flock) 与 systemd RestartSec 边界说明

1. **OS 级文件锁 (`fcntl.flock`)**：
   - `OutboxWorker.start()` 时以非阻塞独占模式 (`LOCK_EX | LOCK_NB`) 抢占 `$XDG_RUNTIME_DIR/amr-outbox.lock`。
   - 若锁已被占用，记录 `logger.critical` 后直接抛出 `SystemExit(3)` 终止进程。
   - `stop()` 时主动调用 `fcntl.flock(LOCK_UN)` 并关闭文件句柄；进程异常被杀时由 Linux 内核负责释放该文件描述符上的锁。
2. **systemd 服务配置**：
   - 守护进程 `systemd` unit 文件配置必须包含 `Restart=always` 与 `RestartSec=3s`。
   - `RestartSec=3s` 确保旧进程在退出/崩溃后内核完全回收锁资源，避免新进程启动发生伪冲突；
   - 若因锁冲突抛出 exit code 3，systemd 会进行退避重启。
3. **架构边界声明**：
   - 当前单执行者设计严格限定于**单机单进程部署**（当前 AMR 生产形态）；
   - **多机/多实例集群不在本次范围**：若未来扩展到分布式多实例部署，必须引入具备租约机制与 Fencing Token 的分布式协调者（如 etcd / Redis Redlock），本地 flock 无法跨机器防御脑裂。

---

## 四、数据一致性边界声明

1. **写入成功的语义**：
   - `memory_create`、`memory_update`、`memory_delete` 成功提交 SQLite 事务后即返回响应（携带 `"projection_status": "pending"`）；
   - 此时数据事实已在 SQLite SSOT 中永久持久化；搜索索引的 Qdrant 写入在后台异步完成。
2. **最终一致性保证的三要素**：
   - 只有同时满足以下三个条件，AMR 系统才承诺搜索投影与 SQLite 的最终一致：
     1. **写入最终停止**（系统不处于无休止的高频持续写压）；
     2. **Outbox Worker 持续存活**；
     3. **下游依赖恢复可用**（模型推理引擎 BGEM3Engine 与 Qdrant 向量库均可正常通信）。
   - **持续高频写入场景**：系统允许投影在短时间内存在落后（表现为 `outbox_lag_seconds` 出现正值），这是异步解耦的预期行为；
   - **读写一致性契约**：`memory_get` 实时强一致读取 SQLite 核心表；`memory_search` 遵循弱一致性（允许秒级落后）。

---

## 五、可观测性指标清单

指标经由内部 `METRICS` 单例收集，并通过 Admin UDS `status.metrics` 统一暴露：

| 指标名称 | 类型 | 说明 | 告警基线 |
| --- | --- | --- | --- |
| `outbox_lag_seconds` | Gauge | 当前新队列中最老待处理任务的已存活时间（秒） | `> 60s` 提示积压，`> 300s` 告警 |
| `outbox_backlog` | Gauge | `memory_projection_outbox` 中待处理 (pending) 任务总数 | `> 100` 提示积压 |
| `outbox_dead_letter` | Gauge / Counter | 重试超过 8 次失败进入死信表 (`dead_letter`) 的任务总数 | `> 0` 立即告警，需人工介入 |
| `outbox_pending_processed`| Counter | 成功完成投影的任务累计计数 | 稳定单调递增 |
| `outbox_failures` | Counter | 投影执行过程中抛出异常的累计次数 | 短期激增需排查 Qdrant / 模型健康度 |
| `embedding_duration_ms` | Observation | BGE-M3 向量模型推理生成耗时 (ms) | P95 `> 500ms` 告警 |
| `qdrant_duration_ms` | Observation | Qdrant 点位写入 / 物理删除耗时 (ms) | P95 `> 200ms` 告警 |

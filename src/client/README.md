# AMR 通用客户端会话可靠补传库 (Universal Client Session Spooling Library)

> 独立于任何特定 Agent 框架的高可靠对话会话补传客户端库。为各类 Agent（包括 OpenClaw、Hermes、Claude Code、DSH、OpenCode 及自研 Agent）提供统一的写前落盘（Spool-before-Send）、进程重启补传、故障隔离与幂等保障。

---

## 核心设计与保障机制 (Core Design & Guarantees)

### 1. 作用域与保障能力 (Scope & Guarantees)
- **写前落盘 (Spool-before-send)**：每批消息在发起任何网络或 IPC/UDS 传输前，必须先在本地磁盘完成序列化、临时文件写入并执行操作系统级 `fsync`，成功后原子替换为正式批次文件。
- **稳定消息 ID (Stable Message IDs)**：每条消息在持久化时生成或保留确定性 `message_id`。重试与进程重启补传时严格复用相同的 `message_id`，利用 AMR 服务端 `(session_id, message_id)` 的内容哈希幂等机制，确保重试零重复插入、零状态污染。
- **确认隔离 (Batch-level Isolation)**：服务端成功确认（Ack）后，**仅且仅删除本批次文件**。同会话的其他在途批次或未确认批次严格保留，绝不误删。
- **失败保留与重启补传 (Retain & Replay)**：遭遇连接超时、网络中断、套接字重置等瞬态故障时，批次文件完好保留在磁盘中。客户端进程重启时自动扫描并按 FIFO 时序重放。
- **文件原子性与权限 (Atomic Operations, fsync & Permissions)**：
  - 目录权限严格强制为 `0o700` (`rwx------`)；
  - 批次文件权限严格强制为 `0o600` (`rw-------`)；
  - 写入使用同目录隐藏临时文件写入 + `os.fsync(fd)` + `os.replace` 原子重命名 + 父目录 `os.fsync`。
- **并发与时序安全 (Concurrency Safety & FIFO Ordering)**：
  - 基于 `fcntl.flock` 进程级文件锁与内部 `threading.RLock`；
  - 单个会话在重传与写入时互斥，保证同会话批次间的因果时序。若第 N 批遭遇瞬态故障，立即暂停该会话后续批次重传，防止会话消息乱序到达。
- **死信隔离 (Poison-Pill Quarantine)**：遭遇服务端永久性语义拒绝（如：跨 Agent 会话 ID 冲突、未授权身份、无效参数）时，自动隔离至 `dead_letter/` 目录，避免毒丸阻塞队列。

---

## 不可保障的边界 (Non-Guarantees & Boundaries)

为了工程落地的严谨性，明确本库无法通过纯客户端软件手段解决的客观系统边界：
1. **掉电前硬件写缓存丢失 (Hard Power Loss / Hardware Cache Volatility)**：若主机突然物理断电且硬盘控制器无备用电池（BBU/Supercapacitor），位于磁盘物理写缓冲区尚未刷入闪存颗粒的数据可能丢失。
2. **本地磁盘完全耗尽 (ENOSPC)**：当客户端所在分区可用空间为 0 时，本地持久化将直接抛出 `SpoolStorageError`，客户端无法凭空存储无限积压的消息。
3. **服务端永久身份冲突 (Server Identity Collision)**：若 `session_id` 已在 AMR 服务端绑定至其他 `agent_id` 或 `project_id`，客户端无限重试无法强行篡改服务端归属；该批次将被归入 `dead_letter/` 待人工介入。
4. **下游认知提炼流水线延迟 (Downstream Cognitive Distillation Latency)**：本库保障原始对话可靠摄入至 AMR 的 SQLite WAL 会话库；服务端何时提炼出长期记忆点并写入向量库属于异步管线，不构成本客户端的同步完成承诺。
5. **未落盘前的不可抗力杀进程 (SIGKILL prior to fsync completion)**：若宿主在操作系统完成 `fsync` 之前收到 `SIGKILL`，内存中尚未落盘的数据将丢失。
6. **分布式多机时钟漂移 (Cross-Host Clock Skew)**：消息时间戳依赖客户端本地时钟，多机并发且时钟未同步时，跨机时钟单调性不由本库仲裁。

---

## 磁盘目录结构 (Storage Structure)

```text
{spool_dir}/
  ├── .spool.lock                             # 全局互斥锁
  ├── sessions/                               # 活跃待传输批次
  │   ├── session_1__a1b2c3d4/                # 安全转义的会话子目录 (0700)
  │   │   ├── .session.lock                   # 会话级互斥锁 (0600)
  │   │   ├── batch_0000000001_xxx.json       # 待传批次 1 (0600)
  │   │   └── batch_0000000002_yyy.json       # 待传批次 2 (0600)
  │   └── session_2__e5f6g7h8/
  └── dead_letter/                            # 永久失败隔离区 (0700)
      └── session_bad__1234abcd/
          └── quarantine_0000000001_zzz.json  # 附带失败原因与堆栈 (0600)
```

---

## 快速使用 (Quick Start)

### 1. 同步客户端 (Sync Client)

适合脚本、后台线程、标准 Python Agent 插件（如 Hermes AMR）：

```python
from pathlib import Path
from src.client import AmrSessionClient

client = AmrSessionClient(
    spool_dir=Path("/tmp/amr_spool"),
    agent_id="my_agent",
    project_id="my_project",
    socket_path="/run/user/1000/qdrant-bge.sock",
    auto_replay=True,  # 启动时自动补传此前积压批次
)

# 写入一条消息并发送（写前自动落盘，成功后自动清除该批）
result = client.record_messages(
    session_id="session_abc_123",
    messages=[
        {"role": "user", "content": "Hello AMR, please remember this"},
        {"role": "assistant", "content": "I have remembered your request."},
    ],
)

if result.ok:
    print(f"Ingested successfully, batch_id: {result.batch_id}")
else:
    print(f"Transmission failed, safely spooled locally: {result.error}")

# 手动触发补传重试
replayed = client.replay_pending()
print(f"Replayed {len(replayed)} batches")
```

### 2. 异步客户端 (Async Client)

适合 FastAPI、asyncio 事件循环或 MCP Server：

```python
import asyncio
from pathlib import Path
from src.client import AsyncAmrSessionClient

async def main():
    client = AsyncAmrSessionClient(
        spool_dir=Path("/tmp/amr_async_spool"),
        agent_id="openclaw",
    )

    result = await client.record_messages(
        session_id="session_async_001",
        messages=[
            {"role": "user", "content": "Async message"},
        ],
    )
    print(f"Result: ok={result.ok}")

asyncio.run(main())
```

---

## 稳定消息 ID 与幂等去重契约

客户端生成的批次消息格式完全符合 AMR 服务端 `session.ingest` 规范：
```json
{
  "message_id": "msg_a1b2c3d4_0001_8f9e0a...",
  "role": "user",
  "content": "Message content",
  "sequence": 1,
  "timestamp": 1731300000
}
```
若 Agent 业务未指定 `message_id`，客户端根据 `session_id + sequence + role + content` 计算 SHA256 确定性生成。一旦落盘，该批次后续无论重传多少次，均携带完全相同的 `message_id`。

AMR 服务端判定逻辑：
1. 首次到达：插入新消息，记录 `status='ingested'`；
2. 超时已提交后重试到达：查询已有相同 `message_id` 且 `content_hash` 一致，返回 `status='success', ignored=N`；
3. 客户端收到 `success` 后安全删除对应本地批次文件。

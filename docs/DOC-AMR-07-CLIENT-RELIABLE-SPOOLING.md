# DOC-AMR-07: 通用 AMR 客户端会话可靠补传规范与架构设计 (Client Reliable Spooling Specification)

## 1. 背景与目标 (Background & Objectives)

AMR (AI Memory Runtime) 承担全生命周期 Agent 记忆沉淀与跨系统检索。以往客户端（如 OpenClaw、Hermes、DSH 插件）在向 AMR 传输原始会话流水（`session.ingest`）时主要依赖即时请求或内存缓冲区；一旦遇到 AMR 服务端重启、高负载超时（`MODEL_LOADING`、`QUEUE_FULL`）或网络瞬断，未持久化的消息极易丢失或造成乱序。

为了从根本上消除 Agent 会话流水丢失的隐患，构建 **通用 AMR 客户端会话可靠补传库 (`src/client`)**。
本规范独立于任何特定 Agent 实现，确立以下核心标准：
1. **写前持久化 (Spool-before-send)**；
2. **稳定消息 ID (Stable Message IDs)** 与服务端幂等对齐；
3. **成功确认仅删本批 (Batch Isolation Ack)**；
4. **失败持久保留与进程重启重放 (Failure Retention & Crash Recovery)**；
5. **文件原子写入、fsync 刷盘与权限约束 (Atomic File Ops, Fsync & 0o600/0o700)**；
6. **并发互斥安全 (Thread & Process Concurrency Safety)**；
7. **明确可保障的作用域与不可保障的系统边界 (Explicit Guarantees & Boundaries)**。

---

## 2. 架构设计与状态机 (Architecture & State Machine)

### 2.1 批次生命周期 (Batch Lifecycle)

```
[Agent Message Request]
          │
          ▼
[1. Normalize & Assign Stable Message IDs]
          │
          ▼
[2. Spool-before-send: Atomic Write + fsync + 0o600] ──> Batch stored on Disk
          │
          ▼
[3. Transport Send (UDS JSON-RPC / Network)]
          │
   ───────┴───────────────────────────────┐
   │ Success Ack                          │ Failure
   ▼                                      ▼
[4. Delete ONLY This Batch]      [5. Error Classification]
                                          │
                   ┌──────────────────────┴──────────────────────┐
                   │ Transient (Timeout, Refused, Queue Full)    │ Permanent (Session Collision, Unauthorized)
                   ▼                                             ▼
          [Keep Batch on Disk]                          [Quarantine to dead_letter/]
                   │
                   ▼
         [Replay on Restart / Drain]
```

### 2.2 目录与权限模型 (Directory & Permissions Model)

- **基准目录 (`spool_dir`)**：权限必须为 `0o700` (`drwx------`)。
- **会话子目录 (`sessions/{safe_session_id}`)**：权限必须为 `0o700` (`drwx------`)。会话目录名由原始 `session_id` 经字符净化与 SHA256 散列后缀生成，防范跨目录逃逸（Path Traversal）。
- **批次文件 (`batch_{timestamp_ns}_{batch_id}.json`)**：权限必须为 `0o600` (`-rw-------`)。
- **临时文件 (`.tmp.{pid}.{timestamp_ns}.{uuid}`)**：在同目录下生成，赋予 `0o600`，写入完成并调用 `os.fsync(fd)` 之后，通过 `os.replace` 原子覆写为正式批次文件。
- **父目录持久化**：原子重命名后，对父目录文件描述符调用 `os.fsync`，确保目录项元数据在 POSIX 文件系统上持久化。

---

## 3. 稳定消息 ID 与服务端幂等契约 (Stable Message ID & Idempotency)

### 3.1 稳定 ID 生成算法
当上游未提供 `message_id` 时，客户端为每条消息生成随机 ID 并在发送前写入批次文件。重放同一批次复用该 ID；独立提交的相同文本不会因批内序号相同而被错误合并。若上游重新提交同一事件为新批次，随机 ID 无法识别重复；需要上游提供稳定的源消息 ID。

### 3.2 超时已提交与幂等去重 (Timeout When Already Committed)
在网络或 IPC 通信中，可能出现“服务端已成功插入 SQLite WAL 并提交事务，但响应在回传或客户端读取阶段遭遇超时（Read Timeout）”。
- **传统客户端风险**：若重发时动态生成新 ID，服务端会将同一条消息判定为全新消息重复插入，破坏会话统计与时序。
- **本客户端保障**：
  1. 批次从磁盘重新读取，完全继承原有的稳定 `message_id`；
  2. AMR 服务端 `SessionStore.ingest_messages` 收到相同 `session_id` 与 `message_id` 时，比对 `content_hash`；
  3. 服务端返回 `{"status": "success", "ignored": N, "inserted": 0}`；
  4. 客户端收到 `success` 确认，判定该批次已圆满提交，安全删除本地批次文件。

---

## 4. 确认隔离性：成功确认仅删本批 (Batch Isolation Ack)

客户端严格维护批次级别的一对一映射：
- 单个会话在运行过程中可能先后产生 `Batch-1`、`Batch-2`、`Batch-3`；
- 当且仅当 `Batch-1` 的响应通过协议校验（无 RPC Error 且 status="success"）后，执行 `storage.delete_batch(session_id, "Batch-1")`；
- `Batch-2` 和 `Batch-3` 处于在途或未确认状态时，其磁盘文件绝不被触及；
- 只有当某会话目录下最后一个批次被确认删除后，客户端才会清理空的会话目录及锁文件。

---

## 5. 并发安全与有序性保障 (Concurrency & Order Safety)

1. **进程间互斥 (`fcntl.flock`)**：
   - 每个会话目录下设置独立 `.session.lock` 文件；
   - 任何批次的写入（Save）、删除（Delete）或重发（Replay）均需先持有会话级独占锁。
2. **线程间互斥 (`threading.RLock`)**：
   - 进程内由全局线程锁字典按文件路径哈希分配可重入锁，阻断同一进程内不同线程的竞态。
3. **有序重放 (In-Order Replay)**：
   - 批次文件名前缀包含 20 位纳秒时间戳（`created_at_ns`），文件名自然排序即为真实因果创建顺序；
   - 重放时按时序自前向后逐批发送。若批次 $K$ 遭遇瞬态错误，立即中止该会话的后续重放，防止批次 $K+1$ 超前到达服务端造成消息颠倒。

---

## 6. 作用域与不可保障的边界 (Guarantees & Non-Guarantees)

| 维度 | 本客户端明确保障 (Guaranteed) | 客观不可保障边界 (Non-Guaranteed) |
| :--- | :--- | :--- |
| **持久化时序** | 每次发送前必须本地先 `fsync` 刷盘（Spool-before-send） | 硬件掉电且磁盘控制器无备用电池（BBU）时的写缓存易失 |
| **传输可靠性** | 进程崩溃或重启后自动重新加载未确认批次进行补传 | 宿主机磁盘空间完全耗尽（`ENOSPC`）导致无法写入磁盘 |
| **幂等去重** | 稳定 `message_id` 保证网络重试零重复插入 | 会话被其他 Agent 抢先在服务端占用（跨 Agent 身份冲突） |
| **数据安全性** | 强制 `0o700` 目录与 `0o600` 文件权限隔离 | 同一 UID 或 root 特权用户的恶意文件篡改 |
| **记忆提炼** | 保证原始会话流水可靠送达 AMR SQLite WAL | AMR 后台异步记忆提炼流水线（Cognitive Distillation）延迟或失败 |
| **并发安全** | 基于 `fcntl.flock` 的跨进程并发与重发互斥 | 跨物理主机挂载网络文件系统（NFS）且未启用文件锁租约 |

---

## 7. 异常分类与毒丸隔离策略 (Error Taxonomy & Dead-Letter Isolation)

- **瞬态错误 (Transient Errors)**：
  - 套接字超时 (`socket.timeout`, `TimeoutError`)；
  - 连接被拒绝或对端关闭 (`ConnectionRefusedError`, `ConnectionResetError`, `BrokenPipeError`)；
  - 服务端瞬态负载码：`-32003` (`MODEL_LOADING`), `-32004` (`QUEUE_FULL`), `-32000` (`SERVER_BUSY`)。
  - **处理策略**：递增重试计数器，记录最新错误时间与信息，批次完好保留在待传队列。
- **永久拒绝 (Permanent Rejections)**：
  - `-32001` (`AGENT_UNAUTHORIZED` 未授权 Agent)；
  - `-32602` (`INVALID_ARGUMENT` 参数错误，或如 `Session ID already belongs to a different agent`)；
  - `-32600` (`INVALID_REQUEST` 协议畸形)；
  - `-32601` (`METHOD_NOT_FOUND` 未知方法)。
  - **处理策略**：继续无限重试毫无意义且会无限阻塞该会话队列。客户端将该批次移动至 `dead_letter/{safe_session_id}/`，附带错误详情供排查，并放行后续会话。

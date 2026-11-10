# AI Memory Runtime (AMR) 敏感信息与凭证泄露独立安全审计报告

> **审计执行实体**：独立安全审计委员会（AppSec / DevSecOps 红队）  
> **审计基线基准**：`review/sec`（基于 `main @ 0105978`，含历史版本 `v5.0.0` 及 `v5.0.1` 完整提交链）  
> **审计时间**：2026-11-10  
> **审计结论评级**：**PASS WITH CONDITIONS (带条件通过 / 需整改加固)**  
> **核心结论**：**全量 Git 历史（44 次 Commit、552 个对象、26 个悬空对象）中未检出高危明文生产密钥（OpenAI/Anthropic/AWS/Qdrant Production API Keys/私钥证书）；无真实数据库快照（`.db`、`.aep`）被提交上库。** 当前存在主要风险聚焦于**内网真实拓扑 IP 泄漏、开发机个人主目录绝对路径硬编码、SQLite 与 UDS Socket 文件权限防护缺陷（TOCTOU 竞争与权限过宽）、缺乏环境变量凭据注入通道以及部分调试日志敏感度控制不足**。

---

## 1. 审计基线与检测方法体系

本审计依据国际权威应用安全规范构建四维立体检测体系，杜绝漏报与假阴性：

```
                ┌──────────────────────────────────────────────┐
                │          AMR DevSecOps 凭证泄露审计体系       │
                └──────────────────────┬───────────────────────┘
                                       │
     ┌──────────────────┬──────────────┴─────┬──────────────────┐
     ▼                  ▼                    ▼                  ▼
┌──────────────┐ ┌──────────────┐     ┌──────────────┐   ┌──────────────┐
│  Git 历史全量 │ │ 工作区静态代码│     │  运行时与日志 │   │ 文件/系统权限│
│  深度挖掘与溯源│ │ 与配置敏感扫描│     │  异常信息暴露 │   │ 与 Socket 防护│
└──────────────┘ └──────────────┘     └──────────────┘   └──────────────┘
```

### 1.1 依据的国际标准与权威规范
1. **OWASP Secrets Management Cheat Sheet**:
   - 密钥全生命周期安全管控、严禁代码与配置硬编码、优先采用环境变量/机密管理器注入、生产与研发环境凭据强物理隔离、运行日志与异常追踪脱敏输出。
2. **CWE-798 (Use of Hard-coded Credentials)**:
   - 审查源码、自动化脚本、示例配置、测试用例、评测语料及设计文档中是否存在硬编码凭证（API 密钥、对称秘钥、私钥、身份 Token、数据库口令）。
3. **CWE-312 (Cleartext Storage of Sensitive Information)**:
   - 审查敏感数据在本地持久化介质、IPC 通道、临时文件、错误诊断输出中的明文暴露情况。
4. **GitGuardian & TruffleHog 启发式检测规范**:
   - 包含全量 Commit 历史、Tag、分支、合并提交（Merge Commits）、已删除历史文件 Diff、Git 悬空对象（Dangling Commits/Blobs）；
   - 香农熵（Shannon Entropy）数学模型分析（$H(X) = -\sum P(x) \log_2 P(x)$，阈值设定为 $\ge 4.5$）；
   - 全特征正规表达式规则库（匹配 AWS/GCP/Anthropic/OpenAI/HuggingFace/GitHub/Slack/JWT/私钥证书头）。

### 1.2 审计检测工具链与执行命令
本次审计执行了以下可复现的自动化与脚本化检测：

* **全量 Git 历史特征扫描（正则矩阵）**：
  ```bash
  # 遍历全部提交历史各版本 Diff，匹配高危特征正则
  git log -p --all --full-history -U0 | python3 -c '
  import re, sys
  PATTERNS = {
      "AWS": r"(?:A3T[A-Z0-9]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}",
      "Anthropic": r"sk-ant-[a-zA-Z0-9_\-]{20,}",
      "OpenAI": r"sk-[a-zA-Z0-9]{20,}|sk-proj-[a-zA-Z0-9_\-]{20,}",
      "GitHub": r"gh[pousr]_[A-Za-z0-9_]{36,255}",
      "PrivateKey": r"-----BEGIN (?:[A-Z0-9_-]+ )?PRIVATE KEY-----",
      "GenericCred": r"(?i)(?:api_key|secret|password|passwd|auth_token)\s*[:=]\s*[\"'\'][^\"'\']{8,}[\"'\']"
  }
  # ... 执行模式匹配
  '
  ```
* **Git 底层全对象空间扫描（包含 Loose & Packed Objects）**：
  ```bash
  git rev-list --objects --all | while read sha path; do
      type=$(git cat-file -t $sha)
      [ "$type" = "blob" ] && git cat-file -p $sha
  done
  ```
* **悬空对象与暂存区完整性校验（Dangling Objects Check）**：
  ```bash
  git fsck --lost-found
  # 逐一提取悬空 blob 与 commit 进行熵值与特征分析
  ```
* **网络与拓扑敏感标识检索（RFC 1918 私网 IP、内部域名、绝对用户路径）**：
  ```bash
  git grep -E '\b(10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})\b'
  git grep -E '/home/[a-zA-Z0-9_\-]+'
  ```

---

## 2. 审计发现清单（Findings Inventory）

根据风险严重等级，本次审计共识别出 **8 项安全缺陷与隐患**，具体分布如下：
- **High (高风险)**: 0 项（未发现生产真实核心密钥泄露）
- **Medium (中风险)**: 4 项
- **Low (低风险)**: 3 项
- **Informational / Safe (安全确认)**: 1 项

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             审计发现风险矩阵汇总                             │
├────┬──────────┬─────────────────────────────┬─────────────────┬─────────────┤
│ 编号│ 风险等级 │ 漏洞类型 / 描述             │ 国际标准依据    │ 处置优先级  │
├────┼──────────┼─────────────────────────────┼─────────────────┼─────────────┤
│ M1 │  Medium  │ 生产 Qdrant 节点内网 IP 泄露 │ CWE-200 / OWASP │ P1 (发布前) │
│ M2 │  Medium  │ SQLite 数据库文件权限过宽   │ CWE-312/CWE-732 │ P1 (发版前) │
│ M3 │  Medium  │ UDS Socket 创建时 TOCTOU    │ CWE-377/CWE-276 │ P2 (近期迭代)│
│ M4 │  Medium  │ 缺乏环境变量注入凭据支持    │ OWASP Secrets   │ P2 (近期迭代)│
├────┼──────────┼─────────────────────────────┼─────────────────┼─────────────┤
│ L1 │   Low    │ 测试用例与文档硬编码个人路径 │ CWE-200         │ P2 (维护整洁)│
│ L2 │   Low    │ 评测黄金集含真实会话与脱敏口令│ CWE-200/CWE-312 │ P3 (合规建议)│
│ L3 │   Low    │ IPC 错误响应未脱敏底层异常   │ CWE-209         │ P3 (合规建议)│
├────┼──────────┼─────────────────────────────┼─────────────────┼─────────────┤
│ S1 │   Safe   │ 历史 Commit 与悬空对象无私钥 │ CWE-798 验证通过│ N/A (保持)  │
└────┴──────────┴─────────────────────────────┴─────────────────┴─────────────┘
```

---

### [Medium-01] 生产环境真实 Qdrant 节点私网 IP 地址在文档中泄露

* **标准归属**：CWE-200 (Exposure of Sensitive Information to an Unauthorized Actor), OWASP Top 10 A01:2021-Broken Access Control
* **风险定级**：**Medium**
* **精准定位**：
  1. `docs/AUDIT-FOR-GPT-RELIABILITY-FIXES-V5.md:33`
     ```markdown
     - **向量检索引擎**: 独立 Qdrant 节点 (`http://192.168.30.161:6333`)。
     ```
  2. `docs/AUDIT-FOR-GPT-RELIABILITY-FIXES.md:34`
     ```markdown
     - **向量检索库**: 外部独立 Qdrant 节点 (`http://192.168.30.161:6333`)，向量维度 1024。
     ```
  3. `docs/AUDIT-R3-MIGRATION.md:188`
     ```markdown
     3. **真实生产 Qdrant 节点直连**: 审计全程严格遵守纪律，使用 FakeQdrant / In-memory Qdrant，未向生产 192.168.30.161 发送网络流量。
     ```
* **威胁场景与危害**：
  文档直接披露了内网生产环境的具体私有 IP（`192.168.30.161`）及服务端口（`6333`）。对于开源或对外公开审查的代码库，暴露内网网段划分、真实服务器私网寻址，将直接协助潜在攻击者进行内网横向移动（Lateral Movement）探测，且默认 Qdrant 在内网若未配置双向 mTLS 或 API Key，极易遭受未授权向量窃取或篡改。
* **处置建议**：
  替换文档中的所有真实生产 IP，改用 RFC 5737 规定的文档测试保留地址（如 `198.51.100.161`）或内部抽象主机名 `qdrant.internal.local`。

---

### [Medium-02] SQLite 存储库与数据文件未做严格的文件系统权限控制

* **标准归属**：CWE-312 (Cleartext Storage of Sensitive Information), CWE-732 (Incorrect Permission Assignment for Critical Resource)
* **风险定级**：**Medium**
* **精准定位**：
  - `src/core/session_store.py:332-352`
  ```python
  # 确保父级目录存在
  if not self.db_path.parent.exists():
      self.db_path.parent.mkdir(parents=True, exist_ok=True)
  ...
  self._conn = sqlite3.connect(
      str(self.db_path),
      timeout=self.busy_timeout / 1000.0,
      check_same_thread=False,
  )
  ```
  - `src/reconciliation/engine.py:122-128`
  ```python
  backup_dir = Path("data/backups")
  backup_dir.mkdir(parents=True, exist_ok=True)
  sqlite_backup_file = backup_dir / f"sqlite_sessions_{date_str}.db"
  backup_conn = sqlite3.connect(str(sqlite_backup_file))
  ```
* **威胁场景与危害**：
  1. `data/sessions.db` 保存着全量对话记忆卡片正文、会话上下文（`payload_snapshot`）、记忆分级与项目分类，属于系统的核心知识库资产。
  2. 代码在创建 `data/` 目录以及通过 `sqlite3.connect` 创建 `sessions.db`、`sessions.db-wal`、`sessions.db-shm` 时，未设置文件模式，完全依赖操作系统当前进程的默认 `umask`（常见为 `0022` 或 `0002`）。
  3. 这将导致生成的数据库文件权限为 `0644` 或 `0664`（同宿主机其他同组/普通用户具备读权限）。多租户或共享 Linux 主机上的恶意非特权本地进程可直接读取 `sessions.db` 窃取所有记忆明文。
  4. 此外，`config/settings.py` 中的 `check_and_fix_file_permissions` 仅针对 `config.yaml` 实施 `0o600`，未对 `data/sessions.db` 执行同样的安全基线保障。
* **处置建议**：
  在 `SessionStore.__init__` 初始化与建库之后，显式执行权限校准：
  1. `data/` 目录强制为 `0o700` (`drwx------`)；
  2. `sessions.db` 及关联的 WAL/SHM 文件在连接建立后强制执行 `os.chmod(self.db_path, 0o600)`。

---

### [Medium-03] UDS Socket 创建存在 TOCTOU 竞争且缺乏父目录权限防篡改校验

* **标准归属**：CWE-377 (Insecure Temporary File), CWE-276 (Incorrect Default Permissions), CWE-362 (Race Condition / TOCTOU)
* **风险定级**：**Medium**
* **精准定位**：
  - `src/interfaces/ipc/server.py:266-285`
  ```python
  async def start(self) -> None:
      # 确保父级目录存在
      self.socket_path.parent.mkdir(parents=True, exist_ok=True)
      if self.socket_path.exists() or self.socket_path.is_socket():
          try:
              self.socket_path.unlink()
          except OSError:
              pass

      self._running = True
      self.server = await asyncio.start_unix_server(
          self._handle_client,
          path=str(self.socket_path),
      )

      # 严格应用 0600 文件权限
      os.chmod(self.socket_path, self.socket_mode)
  ```
  - `config/settings.py:114-120`
  ```python
  def check_and_fix_file_permissions(config_path: Path) -> None:
      """保证 config.yaml 的权限严格为 0600，且其父目录不向 public 暴露"""
      if config_path.exists():
          current_mode = stat.S_IMODE(os.stat(config_path).st_mode)
          if current_mode != 0o600:
              os.chmod(config_path, 0o600)
  ```
* **威胁场景与危害**：
  1. **TOCTOU 窗口**：`asyncio.start_unix_server` 依据进程 umask 创建 socket 文件。在 `start_unix_server` 完成与紧随其后的 `os.chmod(..., 0o600)` 之间，存在微秒级的时间窗口。在此窗口期内，socket 文件的系统权限可能是 `0755` 或 `0664`，本地非授权进程可在此瞬间建立 TCP/Unix 连接。
  2. **父目录逃逸风险**：`check_and_fix_file_permissions` 的文档注释明确声称“保证 config.yaml 的权限严格为 0600，且其父目录不向 public 暴露”，但实际代码完全未检查 `config_path.parent` 的权限！若 socket 或 config 位于 `/tmp` 等世界可写目录（World-writable Directory），攻击者可利用符号链接攻击（Symlink Attack）或目录替换进行阻断。
* **处置建议**：
  - 在调用 `start_unix_server` 前后利用临时 umask 保护创建原子性：
    ```python
    old_umask = os.umask(0o077)
    try:
        self.server = await asyncio.start_unix_server(...)
    finally:
        os.umask(old_umask)
    ```
  - 在 `check_and_fix_file_permissions` 中真实校验父目录权限，防止父目录包含 `S_IWOTH`。

---

### [Medium-04] 配置体系缺失环境变量注入凭证机制（强制明文落盘 YAML）

* **标准归属**：OWASP Secrets Management Cheat Sheet §3 (Storage & Access Control), CWE-312
* **风险定级**：**Medium**
* **精准定位**：
  - `config/settings.py:78-86`
  ```python
  class QdrantConfig(BaseModel):
      url: str = "http://localhost:6333"
      api_key: Optional[str] = ""
      timeout: float = 10.0
      prefer_grpc: bool = True
      collections: List[str] = Field(
          default_factory=lambda: ["ai_memory", "crypto_standards", "project_docs"]
      )
  ```
* **威胁场景与危害**：
  1. 当前配置加载机制（`load_config()`）仅支持从 `config.yaml` 读取 `qdrant.api_key`。
  2. 系统**完全不支持**通过标准环境变量（如 `AMR_QDRANT_API_KEY` 或 `QDRANT_API_KEY`）覆写或注入该密钥。
  3. 这迫使生产部署、容器化环境或 CI 流程必须在磁盘物理文件 `config/config.yaml` 中以明文写入 Qdrant 凭据。根据 OWASP 密钥管理规范，应优先通过环境注入（Process Environment / Secrets Vault），降低密钥在持久化介质遗留或被不慎打包备份的风险。
* **处置建议**：
  为 `QdrantConfig` 增加环境变量自动读取与优先级覆盖机制：
  ```python
  api_key: Optional[str] = Field(
      default_factory=lambda: os.getenv("AMR_QDRANT_API_KEY") or os.getenv("QDRANT_API_KEY") or ""
  )
  ```

---

### [Low-01] 测试用例与多处运维文档硬编码开发机个人绝对路径

* **标准归属**：CWE-200 (Information Disclosure), 环境移植性缺陷
* **风险定级**：**Low**
* **精准定位**：
  1. `tests/audit_r3_scripts_and_rehearsal.py:26`
     ```python
     PYTHON_EXE = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"
     ```
  2. `tests/audit_r3_malicious_and_corruption.py:22`
     ```python
     PYTHON_EXE = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"
     ```
  3. `tests/audit_r5_usability.py:453`
     ```python
     py_bin = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"
     ```
  4. `tests/audit_r5_usability.py:497`
     ```python
     py_bin = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"
     ```
  5. 运维与审计记录文档硬编码（共计 52 处引用 `/home/dj/...`，见 `docs/ROLLOUT-AMR-RELIABILITY.md`、`docs/AUDIT-FOR-GPT-RELIABILITY-FIXES-V5.md` 等）。
* **威胁场景与危害**：
  - 泄露内部开发主机的系统用户名（`dj`）、工作区命名结构与目录拓扑；
  - 破坏测试套件的无状态可移植性。当代码在 CI 容器或其他审计员机器上运行时，若 `/home/dj/...` 路径不存在，直接导致 4 处测试用例失败。
* **处置建议**：
  在测试用例中统一采用 `sys.executable` 动态获取当前虚拟环境 Python 解释器；在文档中统一使用 `$HOME` 或 `%h` 代替。

---

### [Low-02] 评测黄金集与抽样语料包含真实工程会话、项目代号及脱敏密码痕迹

* **标准归属**：CWE-200 / CWE-312 (Data Sanitization & Hygiene)
* **风险定级**：**Low**
* **精准定位**：
  - `tests/golden/work/memories_sample.jsonl:18`
    ```json
    "content": "IAM运维管理后台 web-admin 运行在 8086 端口（仅限内网访问，不对外暴露），前端源码位于 web-admin/，默认初始管理员凭据为账号 admin、口令 REDACTED。管理端通过反向代理将 /api 与 /iam 请求代理至 8088 核心后端服务。"
    ```
  - `tests/golden/work/memories_sample.jsonl:26`
    ```json
    "content": "AEP 记忆治理盘点与教训（2026-10-05 千问办公）：...教训二：至少 1 条 ACTIVE 对话记忆（mem_20260927_38a726）正文含明文的 GitHub 细粒度访问口令，属凭据入共享库红线；AI 侧不代改他人手写条目，需制作人执行两项处置：轮换该口令 + 对条目脱敏或归档。"
    ```
  - `tests/golden/work/memories_sample.jsonl:51`
    ```json
    "content": "User: 请用 AEP 完成一次完整存证... aep init --non-interactive --password 'REDACTED' ... project create hermes-agent-t --password 'REDACTED' ..."
    ```
  - `tests/golden/audit/split_4.jsonl:104` 及多处评测集：
    ```json
    "DID: did:key:zJaNKA6NfJJKu64hETChaf1Lu1xRJddnvj9M6y5V8VBUV9QtJ\nkeyId: sm2-mu92ww9m-0475c822"
    ```
* **威胁场景与危害**：
  - **正面表现**：经核验，评测集中所有的口令均已在生成前被替换为 `'REDACTED'` 占位符，DID 公钥标识符属于标准公开身份，未检出真实生产密码明文。
  - **合规隐患**：`memories_sample.jsonl` 保存了 150 条来源于内部实际开发业务的原始对话问答体。内容包含具体内部微服务端口（8085/8086/8088/8237）、未公开项目代号（`SignalKeepers`、`aep-iam`）、内部网关代理架构（Sub2API）、以及外部上游凭据泄露治理记录。若本项目代码公开，容易造成商业与业务信息泄露。
* **处置建议**：
  若 AMR 计划完全开源，建议对 `tests/golden/work/memories_sample.jsonl` 及 `tests/golden/retrieval_golden.jsonl` 进行全合规匿名化脱敏，采用完全虚构的项目名称（如 `demo-project-alpha`）和通用系统描述。

---

### [Low-03] JSON-RPC 异常捕获向客户端直吐未脱敏底层异常文本

* **标准归属**：CWE-209 (Generation of Error Message Containing Sensitive Information)
* **风险定级**：**Low**
* **精准定位**：
  - `src/interfaces/ipc/server.py:262-264`
  ```python
  # 9. 其余 Exception -> -32603 INTERNAL
  logger.exception("[%s] Internal error executing method '%s' (request_id=%s): %s", self.name, method, req_ctx_id, ex)
  return make_jsonrpc_error(-32603, f"Internal error: {str(ex)}", req_id=req_id)
  ```
  - `plugins/openclaw-amr/index.js:92`
  ```javascript
  logger.debug(`Triggering prefetch for query: "${userQuery.slice(0, 50)}..."`);
  ```
* **威胁场景与危害**：
  1. 当服务端发生意料之外的未处理异常（如 SQLite 故障、网络连接故障等）时，`str(ex)` 被直接格式化为 `-32603` 错误信息回传给调用方。若异常字符串中包含局部文件绝对路径、SQL 片段或含认证串的 URL，可能造成信息回显泄露。
  2. OpenClaw 插件在 `debug` 级别会输出用户 Prompt 的前 50 个字符。若用户在提问中粘贴了敏感密钥或隐私，可能泄漏至本地 debug 日志。
* **处置建议**：
  - 对 `-32603` 内部异常返回通用提示（如 `"Internal server error, check server logs with request_id"`），内部堆栈仅记录于受信日志。
  - OpenClaw 插件中对调试日志的 `userQuery` 仅输出哈希值或长度，或在输出前做敏感脱敏。

---

### [Safe-01] 历史 Commit 全量溯源与悬空对象验证（安全确认）

* **标准归属**：OWASP Secrets Management, CWE-798, TruffleHog / GitGuardian 规范
* **评价结论**：**VERIFIED SAFE (确认安全)**
* **核验事实与证据**：
  1. **提交历史全量分析**：全量 44 个 Commit，自首个公开 Commit `534a144` 以来，没有任何 Commit 曾提交过真实的 `.pem`、`.key`、`.env`、生产 `config.yaml` 或 `.aep` 文件。
  2. **`migration_report.json` 溯源**：该文件于 Commit `d125c30` 意外引入，在 `d1423d5` 中被删除并写入 `.gitignore`。经比对该提交的 Diff，其内容仅为 `{ "v3_count": 0, "legacy_count": 0, "ambiguous_count": 0, "quarantine": [], "status": "in_progress", "updated_at": ... }`，为纯整型统计占位，无任何隐私信息。
  3. **Git 悬空对象（26 个 fsck 悬空节点）**：分析全部 4 个悬空 commit 与 22 个悬空 blob，均为历史草稿 Markdown（DOC-AMR-01~06 演进）、bge-m3 模型的 `config.json`（公开模型开源结构配置）与历史图片，未发现任何被 rebase/amend 隐藏的私有凭证。
  4. **`scripts/amr_doctor.py` 诊断工具脱敏**：
     该脚本严格实现了 `sanitize_dead_letters()`，在向用户或监控输出 Dead Letter 队列明细时，**绝对过滤**了 `content`、`source_refs`、`api_key` 与敏感载荷，仅保留了诊断所需的 `id`、`target_collection`、`retry_count` 与截断的错误代码（见 `tests/test_amr_doctor.py:290-314`）。

---

## 3. 历史泄露与当前暴露评估

### 3.1 历史提交物（Historical Commits）安全评估
| 检查项 | 状态 | 详细说明 |
| :--- | :---: | :--- |
| **真实生产 API Key** | **SAFE** | 历史提交中无 OpenAI (`sk-*`)、Anthropic (`sk-ant-*`)、Qdrant API Key 泄漏 |
| **真实用户数据库** | **SAFE** | 无 `sessions.db`、`*.sqlite`、`*.db-wal` 曾经入库 |
| **AEP 归档证据包** | **SAFE** | 全库无任何 `*.aep` 二进制包或签名密钥被提交 |
| **私钥与证书** | **SAFE** | 无 `BEGIN PRIVATE KEY`、RSA 私钥或敏感证书文件 |
| **已删除文件审计** | **SAFE** | 仅删除过 1 个中间报告 `migration_report.json`，内容安全 |

### 3.2 当前工作区暴露面（Current Attack Surface）安全评估
| 检查项 | 状态 | 详细说明 |
| :--- | :---: | :--- |
| **`.gitignore` 覆盖度** | **GOOD** | 涵盖 `config/config.yaml`、`.env*`、`*.aep`、`data/`、`*.sock` 等核心敏感路径 |
| **`config.example.yaml`** | **SAFE** | `api_key: ""` 纯空串，无误留的测试秘钥 |
| **UDS Socket 权限隔离** | **MEDIUM** | 虽配置 `0o600`，但创建过程存在短期 TOCTOU，且创建目录未防 public 暴露 |
| **SQLite 存储文件权限** | **MEDIUM** | `data/` 目录与 SQLite 库文件依赖 umask 创建，权限过宽 (`0644`) |
| **文档信息合规** | **WARN** | 存在 3 处内网 Qdrant 节点 IP 与 52 处个人用户主目录绝对路径 |

---

## 4. 修复与应急处置建议

### 4.1 代码与配置加固修复方案

#### 建议 1：修复 SQLite 数据库与存储目录权限（解决 Medium-02）
修改 `src/core/session_store.py`，在建库逻辑中确保父目录权限为 `0700`，且数据库文件本身限制为 `0600`：
```python
# src/core/session_store.py
import stat

if not self.db_path.parent.exists():
    self.db_path.parent.mkdir(parents=True, exist_ok=True)
    # 强制保护数据目录，禁止非所有者访问
    try:
        os.chmod(self.db_path.parent, 0o700)
    except OSError:
        pass

# 在 _init_db 中紧接着连接建立后限制 db 文件及 wal/shm 权限
def _fix_db_permissions(self) -> None:
    for p in [self.db_path, Path(f"{self.db_path}-wal"), Path(f"{self.db_path}-shm")]:
        if p.exists():
            try:
                cur_mode = stat.S_IMODE(os.stat(p).st_mode)
                if cur_mode != 0o600:
                    os.chmod(p, 0o600)
            except OSError:
                pass
```

#### 建议 2：修复 UDS Socket 创建的 TOCTOU 竞争（解决 Medium-03）
修改 `src/interfaces/ipc/server.py`，在 `asyncio.start_unix_server` 执行期间通过 `umask` 保证原子性：
```python
# src/interfaces/ipc/server.py
async def start(self) -> None:
    self.socket_path.parent.mkdir(parents=True, exist_ok=True)
    if self.socket_path.exists() or self.socket_path.is_socket():
        try:
            self.socket_path.unlink()
        except OSError:
            pass

    self._running = True
    
    # 临时应用 0077 umask，确保 socket 诞生即为 0700/0600，杜绝 TOCTOU 窗口
    old_umask = os.umask(0o077)
    try:
        self.server = await asyncio.start_unix_server(
            self._handle_client,
            path=str(self.socket_path),
        )
    finally:
        os.umask(old_umask)

    # 再次显式确认 0600
    try:
        os.chmod(self.socket_path, self.socket_mode)
    except OSError:
        pass
```

#### 建议 3：支持环境变量注入 Qdrant API Key 与父目录检查（解决 Medium-04 & Medium-03）
修改 `config/settings.py`：
```python
# config/settings.py
class QdrantConfig(BaseModel):
    url: str = "http://localhost:6333"
    api_key: Optional[str] = Field(
        default_factory=lambda: os.getenv("AMR_QDRANT_API_KEY") or os.getenv("QDRANT_API_KEY") or ""
    )
    timeout: float = 10.0
    prefer_grpc: bool = True
    collections: List[str] = Field(
        default_factory=lambda: ["ai_memory", "crypto_standards", "project_docs"]
    )

def check_and_fix_file_permissions(config_path: Path) -> None:
    """保证 config.yaml 的权限严格为 0600，且其父目录不向 public 暴露"""
    if config_path.exists():
        # 1. 检查并修正文件自身为 0600
        current_mode = stat.S_IMODE(os.stat(config_path).st_mode)
        if current_mode != 0o600:
            try:
                os.chmod(config_path, 0o600)
            except OSError:
                pass
        
        # 2. 真实检查父目录安全权限：不可被其他用户（others）写入
        parent_mode = stat.S_IMODE(os.stat(config_path.parent).st_mode)
        if parent_mode & stat.S_IWOTH:
            import warnings
            warnings.warn(
                f"Security Warning: Config parent directory '{config_path.parent}' is world-writable ({oct(parent_mode)})!",
                RuntimeWarning,
            )
```

#### 建议 4：清理测试用例中的开发机个人路径（解决 Low-01）
将 `tests/audit_r3_scripts_and_rehearsal.py`、`tests/audit_r3_malicious_and_corruption.py`、`tests/audit_r5_usability.py` 中的：
```python
PYTHON_EXE = "/home/dj/WorkSpaces/ai-memory-runtime/.venv/bin/python"
# 替换为：
PYTHON_EXE = sys.executable
```

#### 建议 5：清理文档中的内网真实 IP（解决 Medium-01）
将 `docs/AUDIT-FOR-GPT-RELIABILITY-FIXES-V5.md`、`docs/AUDIT-FOR-GPT-RELIABILITY-FIXES.md`、`docs/AUDIT-R3-MIGRATION.md` 中的 `192.168.30.161` 批量脱敏为 `198.51.100.161`（RFC 5737 规范）或 `qdrant.internal.local`。

---

### 4.2 历史凭证撤销与 Git 历史清洗研判

#### 研判结论：**当前不需要执行强行破坏性 Git 历史清洗（`git-filter-repo`）**

* **研判依据**：
  1. 本项目历史提交中**从未提交过生产使用的任何有效密码或对称/非对称私钥**。
  2. 评测集（`tests/golden/`）中的密码全部为 `'REDACTED'` 占位，DID 密钥为算法验证公共测试标识；
  3. `migration_report.json` 仅为本地无害统计结构，已被删除且被 `.gitignore` 规则收敛；
  4. 存在暴露的仅为内网私有 IP（`192.168.30.161`）及个人绝对路径。在纯内网或私有仓库下，这类信息不涉及外部凭证越权风险；
* **若后续计划向公网公开仓库（Open Source Release）的预备操作清单**：
  若后续 AMR 计划推向公共 GitHub/开源社区，建议在最终公开前执行一次清洗：
  ```bash
  # 1. 批量将文档中真实 IP 192.168.30.161 替换为 198.51.100.161
  # 2. 将个人绝对路径 /home/dj/... 替换为通用占位符
  # 3. 使用 git-filter-repo 进行历史清洗（仅限开源发布前）：
  git filter-repo --replace-text <(echo "192.168.30.161==>198.51.100.161")
  ```

---

## 5. 审计结论签名

本报告客观记录了 AI Memory Runtime (AMR) 当前状态下的敏感信息与凭证防护水平。经严格验证，项目无 CWE-798 级别的硬编码有效生产密钥，整体敏感数据管控具有良好基础，在完成上述推荐的中低风险整改项（权限修补、环境变量注入支持与拓扑文档脱敏）后，可达到生产级 DevSecOps 凭据安全基线要求。

**独立安全审计专家（AppSec / DevSecOps 红队）**  
**日期**：2026-11-10  
**报告归档路径**：`docs/SECURITY-AUDIT-REPORT.md`

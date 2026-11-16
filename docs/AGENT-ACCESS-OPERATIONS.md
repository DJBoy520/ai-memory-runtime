# AMR agent access operations

Status: deployed on 2026-11-16 to the local single-host installation. The
remote gateway and adapter now require per-agent credentials; the daemon's
business and admin sockets require distinct internal service credentials. See
`DEPLOYMENT-RECORD-20261116-AGENT-ACCESS.md` for the exact generation, backups
and verification evidence.

An agent has an independent bearer credential, an explicit list of allowed remote tools and, optionally, an adapter socket. The remote MCP gateway and adapter both use the generated credentials snapshot. The adapter also requires the caller to present the token for every request, including ping. `agent_id` and `source_agent` submitted by clients do not set the principal.

## Security boundaries

The daemon's business and admin sockets can be configured to require distinct internal tokens via `AMR_INTERNAL_TOKEN_FILE`, `AMR_ADMIN_TOKEN_FILE` and `AMR_INTERNAL_AUTH_REQUIRED=1`. The daemon also needs `AMR_AGENT_CREDENTIALS_FILE` pointing to `agents/current/credentials.json`; it reads the enabled identities from this active snapshot for each request. The HTTPS gateway needs the business token; diagnostics need the appropriate business or admin token. Without that configuration, the legacy daemon socket remains usable. This mode must be enabled as part of deployment, not assumed from the existence of the registry.

All current processes share the same Unix UID. Files with mode 0600 are readable by that UID, and a compromised same-UID process can steal another agent's token or the internal daemon token. A stronger isolation boundary requires distinct OS identities or equivalent sandboxing. The tool allowlist controls operations; it does not enforce per-project data isolation.

Local clients that still speak the daemon protocol can be moved to the adapter: set `AMR_SOCKET_PATH` to the agent's adapter socket and `AMR2_TOKEN_FILE` to its private token file. `src/interfaces/mcp/bridge.py` sends that token on every RPC and uses it for its session spool. `src/client/transport.py`, `AmrSessionClient` and `src/admin/cli.py` also read token files via `auth_token_file` / `AMR_ADMIN_TOKEN_FILE`. Once daemon internal authentication is enabled, direct unauthenticated daemon calls fail; an unmigrated client must be retired or updated, not silently left on the legacy socket.

The legacy business socket remains a same-UID boundary: holding the internal token grants broad daemon access rather than one agent's tool grants. Do not describe it as per-agent isolation. Retire direct clients after the gateway, adapter and maintenance tools have all been moved to their own token files.

## Client integrations

Checked-in templates and the CC Switch installer live in `deploy/clients/`.
Each client has its own identity and token file; the Pi/Gemini/Codex/Claude/DSH
stdio examples use `bridge.py` or `bridge2` over the adapter socket, while
OpenClaw/Hermes/OpenCode use the native plugins in `plugins/`.

## Inventory commands

Run from the repository with its Python environment. The default private registry is `~/.config/amr/agents` (directory 0700, inventory 0600). Commands never print bearer tokens.

```sh
python -m src.agent_access.cli list
python -m src.agent_access.cli status
python -m src.agent_access.cli add example --tools memory_search,memory_ingest_session --socket /run/user/1000/amr2-example.sock
python -m src.agent_access.cli grant example --tools memory_search
python -m src.agent_access.cli set-socket example --socket /run/user/1000/amr2-example.sock
python -m src.agent_access.cli rotate example
python -m src.agent_access.cli disable example
python -m src.agent_access.cli enable example
```

Disabling also rotates the stored credential; enabling the agent later never revives its old token. Changing the registry **does not** change running services: the operator must render, validate, and activate a generation. `status` compares files, not the running process state. Tool names are canonical remote MCP names from `TOOL_RPC_METHODS`. A socket is optional for direct HTTPS clients.

To import the existing deployment into an empty private registry:

```sh
python -m src.agent_access.cli import --credentials ~/.config/amr/remote-credentials.json --adapter ~/.config/amr/amr2-adapter.json
```

After making changes, create a new generation directory, then render it from the current service templates and activate it. The parent `generations` directory must exist with mode 0700; the named generation directory must not already exist. Both service unit `ExecStart` commands must reference `~/.config/amr/agents/current/remote.json` and `~/.config/amr/agents/current/adapter.json` respectively.

```sh
mkdir -p -m 700 ~/.config/amr/agents/generations
python -m src.agent_access.cli render --output ~/.config/amr/agents/generations/GENERATION --adapter-base /path/to/adapter-base.json --remote-base /path/to/remote-base.json
python -m src.agent_access.cli activate ~/.config/amr/agents/generations/GENERATION
```

Activation rejects a snapshot that differs from the current registry, checks the config and TLS files, invalidates the active pointer, stops both services, switches to the new snapshot and starts both services. It then probes the authenticated HTTPS-to-daemon path for every enabled identity, checks Qdrant and model status and performs a SQLite read probe, and authenticates to each configured adapter socket. The SQLite probe establishes readability at that moment; it does not prove that subsequent writes will succeed. If startup fails it attempts to stop both; if stopping fails the command warns that access may still be active. A blocked pointer ensures old configuration is not automatically reloaded after a crash. Activation does not prove agent hook behavior; test that separately.

Stop or quiesce the affected agents before activation, then restart them after readiness succeeds and verify their pending spool records are replayed. Transient failures remain on disk, but the clients do not guarantee immediate automatic replay solely when a service becomes available. Configure OpenClaw with an explicit `persistenceDir` and startup replay enabled. Confirm agent shutdown has flushed queued writes before switching; do not assume a restart automatically proves the spool is empty.

When all agents are disabled, `activate-deny-all` invalidates the current pointer and stops both services. A future service restart then fails to load the old generation. It does not protect the daemon's sockets unless internal authentication is also enabled.

The snapshot contains sensitive bearer credentials. Its directory must remain private. Each enabled agent's token is available in `GENERATION/tokens/<agent>.token` (0600); local clients should use `~/.config/amr/agents/current/tokens/<agent>.token`. Do not copy these contents to logs, shell command arguments or repositories. Old generations retain old secrets: keep them private, manage retention, and ensure no old gateway process remains running after revocation.

For daemon diagnostics in internal-auth mode, provide the two **paths**, not the secrets themselves:

```sh
AMR_INTERNAL_TOKEN_FILE="$HOME/.config/amr/internal.token" \
AMR_ADMIN_TOKEN_FILE="$HOME/.config/amr/admin.token" \
  python scripts/amr_doctor.py
```

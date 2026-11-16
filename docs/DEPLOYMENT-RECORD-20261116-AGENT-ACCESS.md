# Agent access production switch — 2026-11-16

This record covers the per-agent credential rollout on the local single-host
installation. It does **not** claim per-project data isolation, and it does
**not** claim that processes sharing the same Unix UID are isolated from one
another.

## Source and artifacts

- Source commit: `04d423f` on `release/2.x`; the feature commit is `2da295f`.
- Active generation: `~/.config/amr/agents/generations/g20261116T230136` (earlier cutover generations `g20261116T221944` and `g20261116T223504` were superseded as identities were added; existing tokens are unchanged).
- Active pointer: `~/.config/amr/agents/current` (symlink).
- Backup root (mode 0700): `~/.local/share/amr/backups/agent-access-20261116T221927`.
  It contains the pre-switch gateway/adapter configs, old credential file,
  tokens directory, old systemd units, OpenClaw/Claude/Hermes client configs,
  the old Hermes plugin copy and a SQLite backup of `data/sessions.db`.
- Private internal credentials: `~/.config/amr/internal.token` (business) and
  `~/.config/amr/admin.token` (admin), both mode 0600 and distinct.
- Installed units: `amr2-mcp.service`, `amr2-adapter.service` and the
  `amr.service.d/amr-internal-auth.conf` drop-in. Their `ExecStart` and
  `ExecStartPre` paths read `agents/current/{remote,adapter}.json`.
- Enabled identities in the generation: `claude_code`, `codex`, `dsh`,
  `gemini_cli`, `hermes`, `openclaw`, `opencode`, `pi`, `zcode`. All retain the
  pre-switch tool grants (`memory_search`, `memory_create`, `memory_ingest_session`).

## Cutover sequence

1. Backed up the configs, units, client configs, plugin copy and SQLite
   database listed above.
2. Imported the existing seven tokens into the private registry and added the
   `claude_code` adapter binding used by the local Claude/DSH bridge.
3. Rendered the generation, validated TLS/config/permissions offline, and ran
   `amr2-mcp --check` plus `adapter --check` against the snapshot.
4. Stopped OpenClaw, Hermes, the adapter and the gateway, then switched the
   `current` pointer atomically and restarted `amr.service` with the internal
   auth drop-in.
5. Activated the generation with the registry command. Activation invalidated
   the pointer first, switched it after stopping the entry services, and then
   started the gateway and adapter and ran the authenticated readiness probes.
6. Restarted OpenClaw and Hermes with adapter/token configuration; updated the
   Hermes provider and MCP bridge configs, installed the missing `_spool.py`
   module, and restarted Hermes again.
7. Migrated the OpenCode plugin and its two local MCP bridge entries to the
   `opencode` adapter binding and token file. The plugin also defaults to the
   per-agent token path when no explicit `tokenFile` is supplied.
8. Added a dedicated `codex` adapter socket, rendered and activated the new
   generation, and migrated the Codex MCP bridge to the `codex` token.
9. Updated the DSH profile patches to the adapter/token bridge and verified
   that the restarted child process carries `AMR_SOCKET_PATH` and
   `AMR2_TOKEN_FILE` for `claude_code`.
10. Added `pi` and `gemini_cli` identities with dedicated adapter sockets,
    rendered and activated `g20261116T230136`, and migrated Pi and Gemini
    CLI MCP configs to the adapter plus their own token files.
11. Updated CC Switch through `deploy/clients/ccswitch-amr.py`, creating
    per-application rows (`amr`, `amr-codex`, `amr-gemini`, `amr-pi`,
    `ai-memory`) so each app uses a distinct identity. CC Switch must be
    reopened/refreshed to apply the database rows to its managed apps.

## Verification evidence

- `systemctl --user is-active` reports `amr.service`, `amr2-mcp.service`,
  `amr2-adapter.service`, `openclaw-gateway.service` and
  `hermes-gateway.service` active.
- `/healthz` returns 200; `/readyz` returns 401 without a token, 401 with a
  wrong token, and 200 for each of `openclaw`, `hermes`, `claude_code` and
  `codex`.
- `/mcp` returns 401 without a bearer token. An authenticated `initialize` and
  `tools/list` succeed; `openclaw` sees only its three granted tools. The
  `codex`, `pi` and `gemini_cli` adapter sockets return `amr.ping` with their
  own tokens and reject other tokens.
- The daemon business socket returns `AUTH_FAILED` without the internal token
  and rejects an agent token. The admin socket rejects the business token.
- The adapter rejects missing, wrong, cross-agent and internal-service tokens
  before dispatch. A correct agent token returns `amr.ping`, the granted tool
  list, and a real `memory.search` result.
- An HTTPS `memory_create` probe through the gateway succeeded; the matching
  `memory.delete` cleanup through the trusted internal channel returned
  `DELETED`.
- Activating the superseded first generation was refused with
  `snapshot differs from current registry` and all three services stayed
  active, confirming stale-grant activation is blocked before stopping.
- `scripts/amr_doctor.py` reports healthy business/admin IPC, metrics, outbox,
  Qdrant and SQLite checks when given the two token paths.
- OpenClaw logged `Initialized openclaw-amr plugin (AMR 2.x)` with the adapter
  socket and persistence directory. The Hermes provider reports
  `available=True` with the adapter socket and Hermes token file. The OpenCode
  client default-token path returned `amr.ping` and its plugin suite passed 16
  tests.
- Python suite: `894 passed, 16 skipped` in the feature worktree before the
  final adapter cleanup; after the cleanup and client integration tests, the
  production workspace reported `898 passed, 15 skipped`. OpenClaw plugin suite: `166 passed`. Hermes plugin
  suite: `27 passed`.

## Known boundaries

- All services and clients run as the same Unix UID. A malicious same-UID
  process can read another agent's token or the internal daemon token. The
  credential check blocks unauthenticated normal calls; it does not isolate
  same-UID processes.
- The daemon internal token grants broad daemon access, not one agent's tool
  grants. It is intended for the gateway, diagnostics and maintenance.
- A permitted search can read the full memory corpus; tool grants are not
  project-level isolation.
- The DSH web process itself was not restarted; its AMR MCP child was
  restarted with the new adapter/token environment and verified. The profile
  patches are the durable source for future DSH starts.
- CC Switch may cache its MCP rows in the running desktop process. The
  database is updated and the installer is idempotent, but the app must be
  reopened/refreshed before its managed app configs are rewritten.
- OpenClaw/Hermes spool directories were empty at cutover. A transient failure
  remains on disk, but clients do not guarantee immediate replay merely because
  the service becomes available; verify the spool after any future switch.
- SQLite readiness is a read probe; it does not prove later writes will succeed.

## Rollback

The old credentials and client/unit configs are in the backup root. To roll
back, stop the two entry services and `amr.service`, restore the old units and
drop-in removal, restore `amr2-remote.json`, `amr2-adapter.json`,
`remote-credentials.json` and the old client configs, then start `amr.service`,
`amr2-mcp.service` and `amr2-adapter.service`. Restore `sessions.db` only if the
database itself is damaged; the switch did not change its schema. Rollback
reopens the legacy daemon socket, so treat it as an emergency path and record
the reason.

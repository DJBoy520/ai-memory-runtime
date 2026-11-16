# Agent access production switch — 2026-11-16

This record covers the per-agent credential rollout on the local single-host
installation. It does **not** claim per-project data isolation, and it does
**not** claim that processes sharing the same Unix UID are isolated from one
another.

## Source and artifacts

- Source commit: `9b66b02` (`fix(amr2): suppress peer reset during adapter
  connection cleanup`) on `release/2.x`; the feature commit is `2da295f`.
- Active generation: `~/.config/amr/agents/generations/g20261116T221944`.
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
  `hermes`, `openclaw`, `opencode`, `zcode`. All retain the pre-switch tool
  grants (`memory_search`, `memory_create`, `memory_ingest_session`).

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
7. Updated the DSH profile patches to the adapter/token bridge. The currently
   running DSH process was started before that change and must be restarted
   before its AMR MCP child can use the new configuration.

## Verification evidence

- `systemctl --user is-active` reports `amr.service`, `amr2-mcp.service`,
  `amr2-adapter.service`, `openclaw-gateway.service` and
  `hermes-gateway.service` active.
- `/healthz` returns 200; `/readyz` returns 401 without a token, 401 with a
  wrong token, and 200 for each of `openclaw`, `hermes`, `claude_code` and
  `codex`.
- `/mcp` returns 401 without a bearer token. An authenticated `initialize` and
  `tools/list` succeed; `openclaw` sees only its three granted tools.
- The daemon business socket returns `AUTH_FAILED` without the internal token
  and rejects an agent token. The admin socket rejects the business token.
- The adapter rejects missing, wrong, cross-agent and internal-service tokens
  before dispatch. A correct agent token returns `amr.ping`, the granted tool
  list, and a real `memory.search` result.
- An HTTPS `memory_create` probe through the gateway succeeded; the matching
  `memory.delete` cleanup through the trusted internal channel returned
  `DELETED`.
- `scripts/amr_doctor.py` reports healthy business/admin IPC, metrics, outbox,
  Qdrant and SQLite checks when given the two token paths.
- OpenClaw logged `Initialized openclaw-amr plugin (AMR 2.x)` with the adapter
  socket and persistence directory. The Hermes provider reports
  `available=True` with the adapter socket and Hermes token file.
- Python suite: `894 passed, 16 skipped` in the feature worktree before the
  final adapter cleanup; after the cleanup, the production workspace reported
  `895 passed, 15 skipped`. OpenClaw plugin suite: `166 passed`. Hermes plugin
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
- The currently running DSH web process predates the profile patch update. Its
  old AMR MCP child has no token and will fail against the protected daemon
  until DSH is restarted. The profile patches are already updated for the next
  start.
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

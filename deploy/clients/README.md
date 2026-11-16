# AMR 2.0 client integrations

These files are **templates and small installers**, not credentials. Replace
the `/home/dj/...` paths if the checkout or home directory differs. Never put a
token value in a config file, command line, shell history or git.

Every client has its own agent identity and private token file:

```text
~/.config/amr/agents/current/tokens/<agent>.token   # mode 0600
~/.config/amr/agents/current/{adapter,remote}.json  # active generation
```

The local adapter socket is `/run/user/1000/amr2-<agent>.sock`; it accepts only
that agent's token. The HTTPS gateway remains available for remote clients.

| Client | Identity | Integration | Repository file |
| --- | --- | --- | --- |
| Claude Code | `claude_code` | stdio bridge over adapter UDS | `claude.mcp.json` |
| Codex | `codex` | stdio bridge over adapter UDS | `codex.toml` |
| Pi | `pi` | stdio MCP over adapter UDS | `pi.mcp.json` |
| Gemini CLI | `gemini_cli` | stdio MCP over adapter UDS | `gemini.settings.json` |
| OpenCode | `opencode` | native plugin + optional MCP bridge | `opencode.jsonc` |
| OpenClaw | `openclaw` | native plugin | `openclaw-plugin.json` |
| Hermes | `hermes` | native memory provider | `hermes-config.yaml` |
| ZCode | `zcode` | stdio shim (`bridge2`) | `zcode.mcp.json` |
| DSH | `dsh` | stdio shim (`bridge2`) | `dsh-cordis.patch.yml` |
| CC Switch | per app | manages MCP entries for Claude/Codex/Gemini/Pi/OpenCode | `ccswitch-amr.py` |
| Windows/remote | any bound agent | SSH local-socket forward | `ssh-streamlocal.sh` |

## Native plugins

The plugin source lives in this repository:

- `plugins/openclaw-amr/` — OpenClaw memory plugin
- `plugins/hermes-amr/` — Hermes memory provider (including `_spool.py`)
- `plugins/opencode-amr/` — OpenCode memory plugin
- `plugins/dsh-amr/` — DSH recall/spool plugin source

OpenClaw and OpenCode can load the checkout directly. Hermes installations keep
a copy under its plugin directory; install `__init__.py`, `_client.py`,
`_spool.py`, `plugin.yaml` and the test file together.

## Applying the stdio examples

Pi (`~/.pi/agent/mcp.json`):

```sh
python3 -m json.tool deploy/clients/pi.mcp.json
# merge the mcpServers.amr object into the existing file; do not replace
# unrelated servers
```

Gemini CLI (`~/.gemini/settings.json`): merge the `mcpServers.amr` object from
`gemini.settings.json`.

Claude Code: merge `claude.mcp.json`, or use `claude-code.sh` for the remote
HTTPS variant.

Codex: replace the `[mcp_servers.amr]` section in `~/.codex/config.toml` with
`codex.toml` (keep unrelated providers).

ZCode and DSH: use the `bridge2` examples; `--token-file` is required.

## CC Switch

CC Switch stores one MCP server row shared by several applications, which
cannot preserve per-agent identity. `ccswitch-amr.py` writes separate rows:

```text
amr          -> claude_code
ai-memory    -> opencode
amr-codex    -> codex
amr-gemini   -> gemini_cli
amr-pi       -> pi
```

Run it with a backup and while CC Switch is closed or immediately before
reopening it:

```sh
cp ~/.cc-switch/cc-switch.db ~/.cc-switch/cc-switch.db.bak-$(date +%Y%m%dT%H%M%S)
python3 deploy/clients/ccswitch-amr.py --dry-run
python3 deploy/clients/ccswitch-amr.py
```

The script writes only paths, never token contents, and is idempotent.

CC Switch may write the new entries under the names `amr-codex`,
`amr-gemini` and `amr-pi`. If an app still has an older manual `amr` MCP
entry, remove the duplicate so the tool namespace is not registered twice.

## Tokens and CA

Tokens are generated and rotated with the identity CLI:

```sh
python -m src.agent_access.cli rotate <agent>
python -m src.agent_access.cli render ...   # see AGENT-ACCESS-OPERATIONS.md
python -m src.agent_access.cli activate ...
```

For direct HTTPS clients, trust the private CA:

- Node clients: `NODE_EXTRA_CA_CERTS=/path/root.crt`
- Rust/Codex: `SSL_CERT_FILE=/path/root.crt` or system trust
- Never disable certificate or hostname verification.

Local adapter clients do not need the CA; the adapter verifies the gateway
using `ca_file` in its own config.

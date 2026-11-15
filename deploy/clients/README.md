# AMR 2.x client examples

These are **templates**, not applied to the running 1.x production. Replace
`amr.internal`, paths and token environment variables. Keep tokens out of the
repository and shell history.

| Client | File | Mode |
| --- | --- | --- |
| Claude Code | `claude-code.sh` | HTTPS MCP + header |
| Codex | `codex.toml` | Streamable HTTP + bearer env |
| OpenCode | `opencode.jsonc` | Remote MCP + header |
| ZCode | `zcode.mcp.json` | stdio shim (`bridge2`) |
| DSH MCP | `dsh-cordis.patch.yml` | stdio shim (`bridge2`) |
| Doubao/Windows | `ssh-streamlocal.sh` | SSH → adapter UDS |

Private CA trust:

- Node-based clients (Claude Code/OpenCode): `NODE_EXTRA_CA_CERTS=/path/ca.crt`
- Codex (Rust): `SSL_CERT_FILE=/path/ca.crt` or system trust store
- Never disable certificate or hostname verification.

# AMR 2.x agent access management design

Status: implementation in isolated worktree; production migration pending. The user explicitly permits retiring all legacy client access.

## Goal and scope

Give every client its own strong credential, explicit per-tool access, and an operator-managed inventory with safe issuance, revocation, rotation and inspection. Deny unrecognized credentials and unlisted functions. Migrate OpenClaw and Hermes and validate real retrieval and session ingestion. Do not claim project-level isolation: a permitted search can access the full memory corpus.

## Security boundary

HTTPS authenticates a client-supplied bearer token at the server and applies the token's tool grants. A local adapter must also authenticate every request with a credential actually supplied by the calling client, bind that credential to the socket's agent identity, strip it and client-claimed identities before forwarding, and enforce the same grants. Anonymous ping can indicate only adapter liveness, not authorization. Tokens must never be logged or committed. Each credential has at least 256 bits from the OS random generator.

Credential files and 0600 sockets do not isolate processes running under the same Unix UID; they can read each other's files and impersonate each other. Strong isolation requires separate OS principals or another externally enforced trust boundary. The legacy business socket accepts self-declared agent IDs; it must be restricted or retired before saying credential enforcement covers all AMR access. It cannot simply be removed while 1.x clients or the 2.x gateway depend on it.

## Approaches

1. Maintain the current three configurations by hand. Smallest change but prone to inconsistent grants and unsafe revocation.
2. Maintain one root-owned/private registry and generate validated deployment snapshots for gateway and adapter. This reuses the existing server authorization and is the intended approach; activation must be explicit, atomic per file, validated, with backup, rollback and coordinated service restart. A failed activation must not be reported as revoked.
3. Introduce a live authorization service and database. Supports immediate revocation but creates a new critical runtime dependency and is unnecessary for the current single-host deployment.

## Registry and operations

Inventory records contain stable agent ID, enabled state, explicit canonical tool allowlist, transport/binding and credential reference. Secret material is separate, mode 0600, under a private directory and never printed by list/check/status. Reject duplicate IDs/tokens/sockets, unknown tools, empty grants for enabled identities, malformed IDs and unsafe paths; default deny. Commands must cover list, inspect, create, disable/revoke, rotate, set-grants, check and activate; they must identify which changes need restarts. Creating or rotating securely writes a separate per-agent credential file; existing tokens must be imported without exposing them or accidentally revoking existing consumers. Rotation needs a documented cutover/rollback and must not leak tokens to shell history. Reconciliation must detect daemon allowlist and adapter/gateway drift. Changes should not automatically alter unrelated entries.

## Migration and tests

Require credentials for all adapter requests, and protect the daemon sockets with an internal service credential. Legacy access is intentionally retired; communicate which clients are not migrated. Verify wrong/missing/other-agent tokens and non-granted tools are denied before forwarding, identity spoofing is ineffective, secrets do not reach logs, and revoked tokens fail after activation. Test crashes, partial activation, malformed registry and rollback in isolation. Run real-client tests using isolated instances/data before production cutover; verify actual hook invocation, search injection, ingestion, source identity, spool acknowledgement and replay. Inspect pending spool records first. Roll out OpenClaw and Hermes with backups and restart only after adapter and HTTPS checks, then verify negative cases and service health. Record that any legacy socket bypass remains until its clients are moved and access is constrained.

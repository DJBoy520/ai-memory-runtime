# Agent Access Implementation Plan

**Goal:** Require independent client-held credentials for AMR 2.x access, centrally manage agents and granular tool grants, and migrate OpenClaw/Hermes.

**Architecture:** An operator-only inventory and per-agent secrets generate checked gateway and adapter configuration. Each authenticated local adapter binding validates the presented bearer credential per request and binds it to the socket identity before forwarding. The HTTPS gateway remains the authoritative remote tool-permission enforcement point. Legacy daemon access and same-UID access remain separately identified security boundaries until isolated.

**Tech stack:** Python 3, existing JSON configuration, existing UDS JSON-RPC, Node.js, systemd.

**Spec:** `docs/superpowers/specs/2026-11-16-agent-access-design.md`.

## Global constraints

Never commit or print credentials; preserve existing grants and pending spool data; default deny for newly migrated bindings; never claim process isolation for agents sharing a UID; validate and test with isolated services before production changes.

## Tasks

1. Write adversarial tests for credential absence, invalid/cross-agent credentials, identity forgery, tool denial and secret leakage. Add authenticated local adapter bindings with constant-time credential verification, credential stripping, strict config validation, and explicit migration mode.
2. Extend OpenClaw and Hermes clients to read their own protected credential files and send credentials on every request, with fail-closed handling for missing credentials; preserve fail-open host behavior and spool writes. Test real host behavior and adapter integration with isolated data.
3. Implement a private inventory and CLI: import existing identities, list/inspect/check, add, set grants, disable, rotate and activate. Generate validated snapshots without logging secrets; reject duplicates, invalid grants, unsafe permissions or inconsistent daemon identities. Test interruption, rollback, idempotence and restart requirements.
4. Identify and migrate every local 2.x consumer before requiring credentials globally; explicitly identify the remaining legacy business socket bypass. Cover bridge2 and non-target clients or retain per-binding migration status with a visible warning.
5. Test negative and positive HTTPS and local requests, permission updates, revocation after activation, client hook retrieval and ingest, spool acknowledgement, and safe rollback. Independently review changes.
6. Inspect pending spool and current services, back up production configuration, deploy only validated snapshots, restart dependencies and OpenClaw/Hermes, verify real data flow with uniquely identifiable test data and clean it up safely. Record actual status and known boundaries.

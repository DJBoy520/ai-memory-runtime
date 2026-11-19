---
name: amr-memory
description: Use when working with AMR shared memory, cross-session project context, durable facts, session handoffs, or memory candidate review through MCP.
---

# AMR shared memory

Use the AMR MCP tools available to the current agent. Names may have a client-specific prefix; inspect the live tool descriptions and schema. The server and credentials enforce permissions. If a required tool is unavailable, report the limitation; never bypass it with another agent's identity or direct database access.

## Recall and verify

For work that depends on prior project decisions, user preferences, or a handoff, search with the relevant `project_id` and a specific query. Use `session_get_handoff` when a handoff is needed. Inspect result project, status, source, and date as applicable; memories are historical context, not automatic authority over the current user request. Confirm critical or time-sensitive facts from current evidence.

Project searches may include configured global/general fallback projects; omitting the project may search all projects. A project filter is not an access-control boundary. Use `include_history` only when historical context is useful.

## Record or revise

Keep durable, useful facts with their scope, qualifiers, and traceable `source_refs`. Distinguish a confirmed fact from a proposal or uncertainty. Avoid storing secrets, irrelevant conversation, or unverified guesses as established facts. Respect the user's instructions about what to remember.

Before creating a fact, check relevant existing records when practical. If an existing record describes the same fact, use `memory_get` and, where permitted, `memory_update` with its current `version` as `expected_version` and a meaningful `change_reason`. Retain valid details and historical context. On version conflict, re-read and reconsider; never blindly overwrite. `memory_update` does not add `source_refs`; do not claim it does. If update permission is absent, explain rather than creating a duplicate as a workaround.

A successful create/update commits to SQLite but search indexing is asynchronous. A missing search result does not prove absence. After a write, use the returned ID with `memory_get` if available; otherwise report the pending indexing state. Do not create again merely because the search misses the new record. Reuse the same idempotency key for a retry of the same write and payload.

## Sessions and review

A session archive, a handoff checkpoint, and searchable long-term memory are different. `memory_ingest_session` requires stable message IDs and an agent/project-bound session ID. Only ingest real messages within the user's authorized scope; account for any native plugin that already ingests the session. A queued response is not a server commit. Use `session_checkpoint` for an authorized cross-session handoff; it does not itself make all session content searchable as memory.

Use `memory_candidates` only with permission to view its evidence messages, which may contain conversation text. Review the original messages, role, qualifiers, current target, and evidence before approving. If a candidate reports `content_truncated` or `omitted_evidence_messages`, its approval is unavailable; report the incomplete view. `results_truncated` means additional candidates were not listed; do not approve an unseen candidate. Pass the returned `evidence_digest` as `expected_evidence_digest`; if evidence changes, re-list and review again. If the live tools do not supply or accept the digest, report the version limitation and do not approve. An UPDATE candidate also requires the reviewed target version and a change reason. Evidence-span matching alone does not establish semantic truth. Do not assume MERGE or SUPERSEDE candidates can be applied by the approval tool.

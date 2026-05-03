# Source Sync Production-Readiness Plan

> - **Status:** Active next-step tracker
> - **Use this when:** deciding how to harden Baluffo source sync into production-grade remote registry behavior
> - **Canonical for:** active-source snapshot risk assessment, sync governance changes, conflict hardening sequence, and operational readiness criteria for BaluffoSync
> - **Not canonical for:** bridge payload contracts, runtime fetcher implementation, or source sync code internals (use `DATA_CONTRACT.md`, `admin-bridge-api.md`, `architecture-ai-map.md`, and `source-policy-runbook.md`)
> - **Then inspect:** [`source-policy-runbook.md`](../source-policy-runbook.md), [`architecture-ai-map.md`](../architecture-ai-map.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and [`admin-bridge-api.md`](../admin-bridge-api.md)
> - **Last updated:** 2026-05-03

Baluffo’s source sync is architecturally sound for an internal/local-first workflow but is not yet production-ready as an unattended source-of-truth registry. It correctly synchronizes source-registry state (`active` + `pending`) rather than the full job feed, which is the right architecture direction.

## Verdict

The sync engine is solid in `src/source_sync.py` and the current defaults point to:

```text
repo:   deathuman/BaluffoSync
branch: main
path:   baluffo/source-sync.json
```

Config and auth hooks, rejected/tombstoned filtering, transition metadata, conflict states, and admin visibility are already in place. The remaining work is operational hardening.

## Current active-source flow

```text
read remote snapshot
→ load local registry
→ merge remote active/pending into local registry
→ write local registry if changed
→ push current local active/pending snapshot back to BaluffoSync
```

## What is already good

1. `source-sync.json` is intentionally narrow (`active`, `pending`, and schema version + metadata), excluding rejected/tombstoned rows.
2. Transition metadata and conflict state exist; local decisions are not blindly overwritten by remote writes.
3. Auth/config foundations are real, including repo/branch/path normalization, allowlist validation, token or GitHub App auth modes, and rate-limit handling.
4. Admin reports sync enabled/configure/auth state, making failures visible without reading logs.

## Production gaps and fixes

| Area | Current state | Gap | Fix |
|---|---|---|---|
| Reviewability | compact JSON snapshot | one-line diffs block human review and rollback | pretty-print with stable key order and stable sort by source identity |
| Idempotency | push may rewrite with timestamp-only changes | bot churn and noisy history | hash active/pending content excluding `generatedAt` and skip no-op pushes |
| Conflict handling | state exists | no deterministic retry path on concurrent writes | handle 409 by re-pull + re-merge + recompute + one retry |
| Source health | registry tracks active/pending only | active does not guarantee fetch usefulness | add health fields (`lastSuccessfulFetchAt`, `failureCount`, etc.) |
| Repo governance | snapshot-only repo conventions | no explicit contract or rollback rules | add README, schema, contract, rollback, environments docs |
| Environment separation | shared default path | staging and prod may collide | introduce path/branch per environment |
| Observability | admin summary exists | no operational metrics of drift/churn/failures | emit structured sync summary + admin counters |
| Contract validation | tests cover behavior extensively | no live artifact schema check in CI | add JSON Schema validation for real snapshot payload |

## Highest-risk issue: snapshot churn

Repeated bot commits with timestamp-only replacements cause: noisy history, poor rollback visibility, reduced auditability, and higher conflict risk.

Immediate fix sequence:

1. canonicalize active/pending rows
2. remove volatile fields from comparison
3. hash meaningful content
4. skip push when no meaningful change
5. only update `generatedAt` when content changes

## Source quality gap (registry-health distinction)

Approved = exportable is not enough for production. Active sources should include operational context.

Recommended synced row fields:

```json
{
  "url": "...",
  "status": "active",
  "sourceType": "greenhouse|lever|ashby|workday|static|social|other",
  "identity": { "...": "..." },
  "lastSuccessfulFetchAt": "...",
  "lastSeenInFetchAt": "...",
  "lastJobsKept": 12,
  "failureCount": 0,
  "zeroJobStreak": 0,
  "health": "healthy|warning|broken|unknown",
  "healthReason": "..."
}
```

This keeps BaluffoSync a source-registry snapshot, not a job-feed-of-truth, while preventing silent active-source regressions.

## Conflict hardening invariants

1. canonical identity appears in `active` or `pending`, not both
2. local rejected/tombstoned suppress remote resurrection unless explicitly restored
3. local active beats remote pending
4. remote active cannot overwrite newer local transition metadata
5. same canonical URL + different identity is review-worthy
6. same identity + different canonical URL is review-worthy

Conflict queue output should include:

- `source`
- `local status`
- `remote status`
- `local updatedAt/transitionAt`
- `remote updatedAt/transitionAt`
- `chosen winner`
- `reason`
- `required action`

## Repo hardening artifacts

Add minimum files under BaluffoSync:

- `README.md`
- `schemas/source-sync.schema.json`
- `docs/sync-contract.md`
- `docs/rollback.md`
- `docs/environments.md`
- `.github/workflows/validate-source-sync.yml`

`README.md` should explicitly state:

- authoritative file and scope
- allowed writers (`GitHub App` / bot)
- rejected/tombstoned exclusion rule
- schema validation expectation
- rollback steps

## Testing gaps to close

1. no-op push with unchanged active/pending content
2. generatedAt-only change must not push
3. concurrent writer retry flow (`409` -> re-pull -> merge -> retry)
4. duplicate identity in active and pending fails validation
5. duplicate canonical URL dedupe/validation conflict path
6. real snapshot contract validation against schema
7. large snapshot merge/push stability
8. malformed remote payload isolation with quarantine path
9. unhealthy active source policy enforcement
10. branch/path allowlist enforcement proofs

## Operational metrics to emit

Structured payload example:

```json
{
  "ok": true,
  "remoteRepo": "deathuman/BaluffoSync",
  "branch": "main",
  "path": "baluffo/source-sync.json",
  "baseSha": "...",
  "newSha": "...",
  "noOp": false,
  "activeBefore": 240,
  "activeAfter": 242,
  "pendingBefore": 31,
  "pendingAfter": 29,
  "added": 4,
  "updated": 2,
  "removedFromRemote": 1,
  "conflicts": 0,
  "tombstonesSuppressed": 3,
  "rejectedSuppressed": 8,
  "durationMs": 1840
}
```

Admin should expose at least:

- last successful source sync
- last failed source sync
- last remote commit SHA
- active pushed
- pending pushed
- conflicts
- no-op skips
- sources suppressed by tombstone/rejected

## Roadmap

### P0 — Make snapshot safe and reviewable

- pretty-print
- stable sorting by identity
- content hash excluding volatile fields
- skip no-op pushes
- add README/schema
- add CI schema validation

### P1 — Harden write behavior

- 409 conflict handling with re-pull/re-merge/retry
- sync lock or GitHub Actions concurrency group
- enforce GitHub App path for production writes
- keep allowlist + branch/path hardening
- protect main branch in GitHub

### P2 — Add active source health

- `lastSuccessfulFetchAt`
- `lastJobsKept`
- `failureCount`
- `zeroJobStreak`
- `health` / `healthReason`
- avoid republishing broken sources as healthy active

### P3 — Make conflicts actionable

- admin conflict queue
- local vs remote diff visibility
- winner rationale
- restore/reject/promote/demote workflow
- preserve transition reason in review output

### P4 — Add contract/integration tests

- real artifact contract check in CI
- no-op push
- concurrent push simulation
- malformed payload handling
- duplicate identity tests
- large snapshot performance test

## Validation criteria while executing this plan

Implementation is complete when:

1. active/pending snapshot changes only produce commits when meaningful content changes
2. one source can be mapped to one canonical row with clear deterministic winner rules
3. source-health metadata is present and reviewed before promotion
4. push/retry path is deterministic under concurrent remote updates
5. CI rejects malformed/duplicate/non-conformant snapshot artifacts
6. admin exposes source-sync outcomes with enough signal for on-call triage

Suggested verification commands:

```powershell
python -m pytest tests/test_source_sync.py tests/test_source_sync_runtime.py tests/test_source_policy_soak_report.py
npm run lint:precommit
```

Adjust commands as implementation slices shrink; treat these as baseline guardrails, not exhaustive.

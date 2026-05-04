# Source Sync Production-Readiness Plan

> - **Status:** Active next-step tracker
> - **Use this when:** deciding how to harden Baluffo source sync into production-grade remote registry behavior
> - **Canonical for:** active-source snapshot risk assessment, sync governance changes, conflict hardening sequence, and operational readiness criteria for BaluffoSync
> - **Not canonical for:** bridge payload contracts, runtime fetcher implementation, or source sync code internals (use `DATA_CONTRACT.md`, `admin-bridge-api.md`, `architecture-ai-map.md`, and `source-policy-runbook.md`)
> - **Then inspect:** [`source-policy-runbook.md`](../source-policy-runbook.md), [`architecture-ai-map.md`](../architecture-ai-map.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and [`admin-bridge-api.md`](../admin-bridge-api.md)
> - **Last updated:** 2026-05-04

Baluffo's source sync is architecturally sound for an internal/local-first workflow but is not yet production-ready as an unattended source-of-truth registry. It correctly synchronizes source-registry state (`active` + `pending`) rather than the full job feed, which is the right architecture direction.

The wider project goal chain is: **source quality → fetch reliability → dedup correctness → lifecycle accuracy → sync confidence → UI polish**. Sync hardening slots into the confidence layer. Getting `baluffo/source-sync.json` to be a stable, validated, low-noise, auditable snapshot is the next high-leverage engineering investment before further frontend or discovery work.

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

Note: `push_sources_snapshot` always reads remote first, merges, builds a new snapshot, then writes. It never pushes raw local state directly — the diagram above is a logical flow, not a strict call sequence.

## What is already good

1. `source-sync.json` is intentionally narrow (`active`, `pending`, and schema version + metadata), excluding rejected/tombstoned rows. Rejected rows are consumed internally during merge for local-over-remote suppression, then stripped from output.
2. Transition metadata and conflict state exist; local decisions are not blindly overwritten by remote writes.
3. Auth/config foundations are real, including repo/branch/path normalization, allowlist validation, token or GitHub App auth modes, and rate-limit handling.
4. Admin reports sync enabled/configure/auth state, making failures visible without reading logs.
5. A registry sync summary layer (`src/bridge/registry_sync_summary.py`) already counts active, pending, synced, conflicted, rejected, tombstoned, and recently-changed sources, providing a foundation for operational metrics.
6. Test suite already covers: config validation, GitHub App auth, active/pending snapshot parsing, merge behavior, rejected/tombstone filtering, conflict states, bad JSON handling, push serialization, rate-limit behavior, and happy-path sync.

## Production gaps and fixes

| Area | Current state | Gap | Fix |
|---|---|---|---|
| Reviewability | compact JSON snapshot | one-line diffs block human review and rollback | pretty-print with stable key order and stable sort by source identity |
| Idempotency | push may rewrite with timestamp-only changes | bot churn and noisy history | hash active/pending content excluding `generatedAt` and skip no-op pushes |
| Conflict handling | state exists | no deterministic retry path on concurrent writes | handle 409 by re-pull + re-merge + recompute + one retry |
| Source health | registry tracks active/pending only | active does not guarantee fetch usefulness | implemented: active source rows now carry health aliases |
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

## GitHub-side hardening

The Baluffo sync depends on GitHub as its remote backend, but several GitHub-side configuration items are not yet addressed in either the codebase or the plan. These are distinct from the snapshot code changes above — they are repository and organization settings that must be in place for the sync to be production-grade.

### Current state

- GitHub App auth works (app ID `3047247`, installed on `deathuman/BaluffoSync`)
- Contents API read/write is functional
- Allowlist validation exists in code but there is no enforcement at the GitHub level
- The validate-source-sync CI workflow is planned but not yet written, let alone required

### Remaining GitHub-side gaps

| Gap | Risk | Fix |
|---|---|---|
| No commit signing | Anyone with push access can impersonate the bot | Configure GitHub App to sign commits; enable "Require signed commits" on branch protection |
| No required status checks | Bad snapshots can land even when CI fails | Register validate-source-sync as a required status check on branch protection |
| Classic branch protection only | No linear history enforcement, force-push protection, or bypass restrictions | Use GitHub repository rulesets instead of (or on top of) branch protection |
| No deployment environments | Staging/prod path separation has no approval gate or audit trail | Define GitHub Environments with required reviewers for production path writes |
| No rollback checkpoints | Reverting requires hunting through git history for the last-good SHA | Tag every validated write (`last-known-good`, date-stamped) for one-command revert |
| Rate-limit headers discarded | `x-ratelimit-remaining` is received but never logged or alerted on | Log remaining < 10% threshold; surface in admin runtime state |
| No repository visibility decision | Private vs public is implicit, not intentional | Document that BaluffoSync is intentionally private as a sync transport repo |
| No CI failure notification | Failed validation can sit undiscovered | GitHub Actions notifications are the baseline alert path for `validate-source-sync.yml`; optional Slack/webhook mirrors can be added separately |
| No API version deprecation plan | `X-GitHub-Api-Version: 2022-11-28` is hardcoded | Make version a config constant; add calendar reminder to check GitHub changelog annually |

**Side note on auth model:** The plan exclusively uses GitHub App auth, which is correct for production. For simpler single-writer deployments, deploy keys with write access are a viable alternative (no GitHub App registration, no JWT, no installation token exchange). That fits the private transport-repo model documented in `docs/environments.md`, even though GitHub App remains the recommended production path.

**Slice 1 landed in-repo:** the first governance slice should land the schema contract, the `validate-source-sync` workflow, and the `docs/environments.md` release-path note in this repository. GitHub-side branch protection, repository rulesets, signed-commit enforcement, and required-check activation remain optional hardening for the private transport-repo model because those settings live outside the repo and may not be available on the current plan.

### Implementation outline

**P0 — Commit signing + required status checks**

These are the highest-leverage GitHub-side changes because they make the audit trail cryptographically verifiable and prevent bad snapshots from reaching `main`:

1. Configure the GitHub App to sign commits. GitHub Apps can sign via their private key — the code already has the key loaded in `GitHubAppAuth`. The PUT to the Contents API should include the commit signature (GitHub supports `PUT /repos/{owner}/{repo}/contents/{path}` with a signed commit when the app's `sign-commits` permission is enabled).
2. Enable the branch protection rule: **Require signed commits**.
3. Write `validate-source-sync.yml` and register it as a **required status check** on `main`. The workflow:
   - Checks out the snapshot file
   - Validates against `schemas/source-sync.schema.json` (JSON Schema)
   - Validates no duplicate sourceIds across active/pending
   - Validates no unknown top-level keys
   - Exits non-zero on failure

**P1 — Repository rulesets + GitHub Environments**

Replace or augment classic branch protection with repository rulesets:

| Rule | Purpose |
|---|---|
| Require signed commits | Audit integrity |
| Require status checks | CI gate |
| Require linear history | Clean `main` history, easier rollback |
| Block force pushes | Prevent history rewrite |
| Restrict deletions | Prevent accidental file loss |
| Restrict bypass | Only repo admin can override |

If using multiple paths for environment separation (`baluffo/prod/...`, `baluffo/staging/...`), define GitHub Environments:

- **staging**: deployment branch = `staging`, no required reviewers
- **production**: deployment branch = `main`, requires 1 reviewer, no self-review

**P2 — Tagged rollback checkpoints + rate-limit monitoring**

After every successful validated write to `main`, the sync workflow tags the commit:

```
git tag -f last-known-good
git tag rollback-2026-05-04  (date-stamped for retention)
git push origin --tags
```

Rollback is then a one-command operation: force-push the tag to `main` (requires force-push bypass on the ruleset, restricted to admin).

Implemented here: `rate_limit_note_response` now logs a warning when `x-ratelimit-remaining` drops below 10% of the initial quota, and the admin runtime state now exposes `lastRateLimitRemaining` and `lastRateLimitResetAt` for operator visibility.

**P3 — Notification routing**

- Configure GitHub notification settings for workflow failures on `validate-source-sync.yml`
- API version management already landed in-repo via the `GITHUB_API_VERSION` constant and the note in `docs/sync-contract.md`

---

## Local data storage sustainability

The sync snapshot (`source-sync.json`) is intentionally lean, but the local data directory (`data/`) hosts several large files that are rewritten atomically on every pipeline run. At current scale and projected growth, these files need compression and retention governance.

### Current footprint

| File | Rows | Format | Raw size | Gzip'd | Ratio |
|---|---|---|---|---|---|
| `jobs-unified.json` | 36,289 | compact (1 line) | 40.0 MB | 4.1 MB | 10.2% |
| `jobs-unified-light.json` | 36,289 | compact (1 line) | 20.0 MB | 1.9 MB | 9.7% |
| `jobs-lifecycle-state.json` | 75,637 | pretty-print (458 lines) | 42.8 MB | 5.6 MB | 14.1% |
| `jobs-source-state.json` | — | pretty-print | 8.2 MB | — | — |
| `source-registry-active.json` | 2,301 | pretty-print (177K lines) | 19.6 MB | — | — |
| `source-registry-pending.json` | 516 | pretty-print (48K lines) | 5.2 MB | — | — |
| **Pipeline data subtotal** | | | **~136 MB** | **~14 MB** | **~10%** |

### Why compression works so well

All these files share three properties that make gzip extremely effective (10-14% ratio):

- **Repeated field names** — every row carries 30-90 key names. 36K jobs × 30 fields = 1M+ field name strings
- **Low-cardinality categorical values** — 3,383 companies, 1,062 sources, 11 adapter types shared across thousands of rows
- **Short string data** — average description is 38 chars, most fields are timestamps or short tokens

### Key sustainability risks

1. **Lifecycle state grows unbounded** — 75,637 entries today vs 36,289 active jobs. The ~39K historical archived/removed rows accumulate every fetch run with no eviction policy.

2. **Full rewrites on every pipeline run** — A single pipeline write atomically rewrites ~136 MB across all files via `save_json_atomic` (temp-file + rename). Only a fraction of rows actually change.

3. **Pretty-print overhead on storage** — `source-registry-active.json` (19.6 MB with `indent=2`) would be ~7 MB compact. `jobs-lifecycle-state.json` (42.8 MB) is the only large file still pretty-printed; the unified files are already compact.

4. **Registry rows carry 50-90 fields each** — Many are sparse (e.g. `gamedevmapRecovery` in 11% of rows, `migrationSourceIdentity` in 0.04%) but serialized as empty strings on every row.

### Storage roadmap

#### P0 — Compact file format

- Implemented in-repo for registry storage via `save_json_atomic(..., separators=(",", ":"))`
- Keep pretty-print for display-only paths (admin summary, debug output)
- Effect: registry snapshots shrink immediately without changing read behavior; the jobs-pipeline gzip migration now uses the same transparent storage pattern

#### P1 — Transparent gzip compression

- Implemented in-repo for the core registry state files and the high-volume jobs pipeline artifacts via `save_json_atomic`/`load_json_array`/`load_json_object` and `.json.gz` fallbacks
- Apply to: `source-registry-active.json`, `source-registry-pending.json`, `source-registry-rejected.json`, `source-registry-tombstones.json`, `jobs-unified.json`, `jobs-unified-light.json`, `jobs-lifecycle-state.json`, and `jobs-source-state.json`
- Migration strategy: read old `.json` if `.json.gz` does not exist; write new as `.json.gz`; old files can be deleted after one cycle once consumers are switched
- Effect: registry state and the jobs pipeline snapshot/state files shrink first; lifecycle retention keeps the daily hot path lean while the cold archive stays on-demand

#### P2 — Lifecycle state retention policy

- Implemented in-repo for hot lifecycle state plus yearly gzip-backed cold archives for aged archived rows
- Apply to: `jobs-lifecycle-state.json` plus `jobs-lifecycle-archive-{year}.json.gz`
- Retention strategy: keep active and recently removed rows in the hot file; move archived rows past the threshold into the yearly cold archive and load them on demand only
- Effect: stops unbounded lifecycle growth; daily lifecycle reads stay at current-job scale while archive reads remain opt-in

#### P3 — Lean registry storage

- Implemented in-repo for `source-registry-active.json` and `source-registry-pending.json` with lean core rows plus a gzip-backed `source-registry-metadata.json.gz` sidecar keyed by sourceId
- Core rows keep the always-present identity and transition fields inline; sparse compatibility and adapter-specific fields live in the sidecar and are merged back on read
- Legacy monolithic registry files still load through the same logical entrypoints, so existing admin / bridge / source-policy consumers keep the same row shape
- Effect: current registry storage drops the remaining sparse-field bloat while preserving the full reconstructed registry rows for callers

#### P4 — Incremental writes and journal replay

- Implemented here as a shared full-image JSONL journal for lean registry and other JSON payloads: each meaningful write appends a schema-versioned image record beside the snapshot
- Snapshot loads remain backward compatible, but readers now replay the latest valid journal image first so truncated tails do not lose the most recent state
- Bounded compaction keeps the journal short while no-op gating still skips journal and snapshot rewrites for unchanged payloads

## Snapshot Hardening & Operational Completeness

### P0 — Make snapshot safe and reviewable

**Existing from original:**
- pretty-print
- stable sorting by identity
- content hash excluding volatile fields
- skip no-op pushes
- add README/schema
- add CI schema validation

**New: no-op push with content digest**
- Add `_snapshot_digest(state) → str` that SHA-256 hashes `json.dumps({"active": rows, "pending": rows}, sort_keys=True)` — excludes `generatedAt`, `source`, `schemaVersion`
- `push_sources_snapshot` computes digest before write; skips PUT if it matches previous push's stored digest
- Store last-push digest in local runtime state (`data/source-sync-runtime.json`)
- Return `{"pushed": false, "noOp": true, ...}` in result
- This connects the `changed` flag that `pull_and_merge_sources` already computes (but discards) to the push decision

**New: identity collision validation at push**
- Before writing snapshot, validate: no `sourceId` appears in both `active` and `pending`
- Validate: no duplicate `sourceId` within `active` or within `pending`
- Raise `SyncOperationError("validation_error", ...)` on violation

### P1 — Harden write behavior

**Existing from original:**
- 409 conflict handling with re-pull/re-merge/retry
- sync lock or GitHub Actions concurrency group
- enforce GitHub App path for production writes
- keep allowlist + branch/path hardening
- protect main branch in GitHub

**New: idempotent PUT retry with SHA-based verification**
GitHub's Content API does not support idempotency keys natively, so we use SHA-based conditional writes:

```
PUT fails (transient network error, not HTTP 409/422/401)
→ re-read remote (GET + sha)
→ if sha matches the sha used in the original PUT
  → same content did not land → retry PUT once with same sha → safe
→ if sha differs
  → another writer modified the file → treat as concurrent write
  → follow 409 conflict retry path (re-pull → re-merge → retry once)
```

- On second failure, propagate the error
- Do NOT retry HTTP 409 (conflict — goes through conflict path), HTTP 422 (validation), or HTTP 401 (token — handled by existing retry)

**New: GET transient network retry**
- Retry `URLError`/`ssl.SSLError` on GET up to 2 attempts with exponential backoff (base 1s, max 5s)
- Do NOT retry `HTTPError` of any kind (401/409/429 already have dedicated handling)

**Implemented: structural snapshot validation on read**
- In `normalize_snapshot`, warn on unexpected top-level keys via log
- Reject snapshots with structural violations: missing required keys (`schemaVersion` int ≥ 1, `generatedAt` string, `active`/`pending` arrays), non-conformant rows
- Validate every active/pending row has a `sourceId` after normalization
- Malformed payloads: log the error with details, raise an exception — no quarantine file written to disk

**New: concurrent push guard**
- The bridge layer already has `sync_task_running()` (`src/bridge/sync_service.py`) and run-tracking (`src/bridge/sync_state.py`)
- Enhance `push_sources_snapshot` to acquire and release the run-tracking slot
- No separate file lock needed — avoid the complexity and I/O cost

**New: dry-run mode**
- Add `dry_run=True` parameter to `push_sources_snapshot`
- Runs full pipeline: read remote → merge → build snapshot
- Skips the PUT; returns `{"pushed": false, "dryRun": true, "wouldChange": <bool>, "activeBefore": N, "activeAfter": N, ...}`
- Exposed via `POST /sync/push?dry_run=1` in the admin bridge

### P2 — Add active source health (implemented)

**Implemented:**
- Active source rows now carry `lastSuccessfulFetchAt`, `lastSeenInFetchAt`, `lastJobsKept`, `failureCount`, `zeroJobStreak`, `health`, and `healthReason`.
- Existing `healthScore`, `lastSuccessAt`, and `consecutiveFailures` aliases remain preserved.
- Broken and repeated zero-job sources are no longer republished as healthy active.

**New: snapshot size governance**
- Configurable `max_snapshot_size_bytes` on `SyncConfig` (default 5MB; safe for 5000+ sources at ~200 bytes/row)
- Warn via log when raw JSON exceeds 3MB
- Reject push with `SyncOperationError("snapshot_too_large")` when limit exceeded

**New: admin daily-reset counters**
- Extend `SyncState` / `data/source-sync-runtime.json` with a counters block:
  ```json
  {
    "counters": {
      "date": "2026-05-04",
      "totalPushes": 0,
      "totalPulls": 0,
      "noOpSkips": 0,
      "conflictsDetected": 0,
      "conflictsResolved": 0,
      "tombstonesSuppressed": 0,
      "sourcesAdded": 0,
      "sourcesRemoved": 0
    }
  }
  ```
- Counters reset when `date < today_utc.date()` (calendar-day UTC boundary)
- Expose via `GET /sync/status` alongside existing config status
- Builds on the existing `registry_sync_summary.py` layer

### P3 — Make conflicts actionable

**(Unchanged from original plan)**

- admin conflict queue
- local vs remote diff visibility
- winner rationale
- restore/reject/promote/demote workflow
- preserve transition reason in review output

### P4 — Add contract/integration tests

This is a test-only checkpoint. The cases below stay as executable specifications for later runtime hardening slices, but this pass does not change source-sync runtime behavior, bridge routes, or payload contracts.

**Existing from original (keep):**
- real artifact contract check in CI
- no-op push
- concurrent push simulation
- malformed payload handling
- duplicate identity tests
- large snapshot performance test

**New test cases from hardening work:**
- `test_no_op_push_skips_write_when_content_unchanged` — same active/pending content must not create a new commit
- `test_content_hash_stable_excluding_volatile_fields` — `generatedAt`/`source` changes alone must not alter digest
- `test_idempotent_put_retry_re_reads_sha` — transient PUT failure triggers re-read, then retry
- `test_put_retry_detects_concurrent_write_as_conflict` — SHA mismatch on re-read routes to 409 path
- `test_transient_get_error_retries` — URLError/ssl.SSLError on GET retries up to 2 times
- `test_dry_run_returns_diff_without_side_effects` — dry-run mode must not write or change state
- `test_identity_collision_across_buckets_rejected` — same sourceId in active and pending must fail
- `test_daily_counters_reset_on_date_boundary` — counters clear when calendar date changes
- `test_snapshot_size_warning_and_rejection` — size governance threshold enforcement

**Already covered by existing tests (do not duplicate):**
- bad JSON / malformed remote payload on read
- duplicate canonical URL dedupe/validation conflict path
- branch/path allowlist enforcement proofs
- concurrent writer (409 → re-pull → merge → retry)

### P4 runtime follow-up slices

Keep the runtime work split out after this test-only checkpoint:

- implemented here: snapshot content fingerprinting, no-op write gating, idempotent PUT retry, conflict re-read handling, transient GET retry/backoff, dry-run support, structural snapshot validation on read, daily counters, rate-limit telemetry, and snapshot-size governance
- next: GitHub-side branch protection, required checks, commit signing, environments, and release-policy hardening
- validated in dry-run smoke against the live `deathuman/BaluffoSync` remote: the compare path resolves, reports `wouldChange=false`, and does not write

### Snapshot-size scalability path

The current cap is intentionally conservative. Treat it as a guardrail, not the long-term architecture. The scalability decision is:

1. Keep the canonical snapshot minimal.
   - `source-sync.json` should stay the apply/input contract only: canonical active/pending registry state.
   - Move noncanonical evidence, counters, timelines, and review context into runtime/report/Admin surfaces.
   - This is the default architecture for this repo because it keeps retry, conflict, and idempotence logic simple.
2. Shard only if a separate concern grows independently.
   - Split only when two payload groups have different update cadences or ownership.
   - Avoid sharding just to make the cap disappear; that usually creates more merge and partial-update risk than it removes.
3. Add adaptive warning bands and growth tracking as an operational guardrail.
   - Keep the hard limit, but warn earlier when size trends up.
   - Track `snapshotSizeBytes` and the per-run delta so operators can see whether the payload is stable, growing, or close to the cap.
   - Use the trend data to tune `maxSnapshotSizeBytes` by environment, not to silently normalize larger and larger blobs.

Recommended path for this codebase:

- architecture: option 1
- guardrail: option 3
- later escape hatch: option 2 only if a single concern starts growing faster than the rest

Concrete ownership:

- `src/source_sync_snapshot.py`: fingerprinting, no-op gating, and write-time size enforcement
- `src/source_sync_config.py`: per-environment `maxSnapshotSizeBytes`
- `src/source_sync_runtime.py` and `src/bridge/sync_service.py`: counters and status payloads that can surface rate-limit telemetry and be extended with growth telemetry later
- Admin/report surfaces: show the snapshot-size trend when it becomes useful, but keep the snapshot itself canonical and small

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
- daily counters (see P2 above)

## Roadmap

### P0 — Make snapshot safe and reviewable + compact storage + commit signing

- pretty-print
- stable sorting by identity
- content hash excluding volatile fields
- skip no-op pushes
- identity collision validation at push
- add README/schema
- add CI schema validation (`validate-source-sync.yml` workflow)
- register `validate-source-sync` as a **required status check** on `main` branch
- configure GitHub App to sign commits
- enable branch protection rule: **Require signed commits**
- switch `save_json_atomic` to compact `separators=(',',':')` for storage files
- migrate core registry state files to `.json.gz` with transparent read fallback

### P1 — Harden write behavior + repository rulesets + environments

- 409 conflict handling with re-pull/re-merge/retry
- idempotent PUT retry with SHA-based verification
- GET transient network retry
- implemented here: structural snapshot validation on read
- concurrent push guard (extend existing `sync_task_running()`)
- dry-run mode
- replace classic branch protection with **repository rulesets**:
  - require signed commits, require status checks, require linear history
  - block force pushes, restrict deletions, restrict bypass to repo admin
- define GitHub Environments for staging/prod path separation:
  - production: requires 1 reviewer, deployment branch = `main`
  - staging: no reviewer, deployment branch = `staging`
- migrate jobs pipeline files (`jobs-unified*`, `jobs-lifecycle-state`, `jobs-source-state`) to `.json.gz`

### P2 — Remaining follow-up: storage governance + checkpoint tags

- snapshot size governance (configurable 5MB limit, warn at 3MB)
- admin daily-reset counters (calendar-day UTC)
- lifecycle state retention policy: archive jobs removed > 90 days to cold storage
- exclude archived lifecycle entries from daily merge/read paths
- tag every validated write with `last-known-good` + date-stamped tag
- log warning when `x-ratelimit-remaining` < 10% of quota

### P3 — Make conflicts actionable + notification routing + API version

- admin conflict queue
- local vs remote diff visibility
- winner rationale
- restore/reject/promote/demote workflow
- preserve transition reason in review output
- implemented in this slice:
  - extract `GITHUB_API_VERSION` from hardcoded header into module constant
  - add API version deprecation monitoring note to `docs/sync-contract.md`
  - document the `validate-source-sync.yml` failure notification policy in `docs/environments.md`
  - subscribe the workflow repo to GitHub Actions notifications for workflow failures

### P4 — Add contract/integration tests

- real artifact contract check in CI
- no-op push
- content hash stability
- idempotent PUT retry
- concurrent push simulation (409 path)
- transient GET retry
- dry-run mode
- identity collision
- daily counter reset
- size governance
- malformed payload handling (existing coverage is good; add structural validation tests)
- large snapshot performance test
- `.json.gz` transparent read/write round-trip
- lifecycle cold archive load and retention toggle

## Validation criteria while executing this plan

Implementation is complete when:

1. active/pending snapshot changes only produce commits when meaningful content changes
2. one source can be mapped to one canonical row with clear deterministic winner rules
3. source-health metadata is present and reviewed before promotion
4. push/retry path is deterministic under concurrent remote updates
5. CI rejects malformed/duplicate/non-conformant snapshot artifacts
6. admin exposes source-sync outcomes with enough signal for on-call triage
7. daily counters reset cleanly and give operators push/pull/conflict volume per day
8. snapshot size governance prevents accidental oversized payloads
9. dry-run mode allows safe preview of what a push would change
10. `save_json_atomic` uses compact separators for storage; pretty-print is display-only
11. core registry state files are transparently read and written with fallback to legacy `.json`
12. lifecycle archive moves entries older than the threshold out of the daily read path
13. per-run registry storage I/O is measurably reduced by the compact+gzip changes already landed
14. all bot commits to `main` are signed (verified via `git verify-commit`)
15. `validate-source-sync.yml` workflow must pass before commits land on `main`
16. repository rulesets enforce linear history, signed commits, and block force pushes
17. `last-known-good` tag exists on `main` and points to the last validated write
18. admin runtime state exposes rate-limit remaining percentage

Suggested verification commands:

```powershell
python -m pytest tests/test_source_sync.py tests/test_source_sync_runtime.py tests/test_source_policy_soak_report.py
npm run lint:precommit
```

Adjust commands as implementation slices shrink; treat these as baseline guardrails, not exhaustive.

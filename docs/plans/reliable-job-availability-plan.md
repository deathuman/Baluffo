# Reliable Job Availability and Saved-Job Alerts

> - **Status:** Active rollout
> - **Use this when:** operating, reviewing, or extending job availability lifecycle, direct validation, or Saved attention
> - **Canonical for:** rollout gates and remaining promotion/reconciliation operations
> - **Not canonical for:** row fields or endpoint shapes; use the contract docs
> - **Then inspect:** [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../storage-contract.md`](../storage-contract.md), [`../testing.md`](../testing.md)
> - **Last updated:** 2026-07-16

## Implemented baseline

- Seed rows are no longer current observations; source-scoped success can retire them while failed/skipped sources age conservatively.
- Canonical `available`, `verification_overdue`, and `unavailable` state, exact stable aliases, active-only publication, 30-day lazy history, and availability health/conflict/shadow summaries are implemented.
- Direct checking is bounded, public-only, provider-detail-aware, posting-action/structured-evidence based, rate-limited, and shadow-first. Redirected evidence must retain the checked provider posting identity and tenant. Arbitrary successful pages, global apply phrases, unrelated application actions, unrelated visible closure copy, and raw script/template phrases are not definitive evidence. Successful and warning pipeline publication projects transitions through the shared service before consuming the complete safely bounded saved-first/oldest-first sweep plan through a four-worker background drain. A private compact latest-check checkpoint advances the rotation across runs; unavailable saved rows and privately scoped custom Saved URLs stay in rotation, and desktop startup catches up overdue rows without an OS service.
- Saved projection is transition-idempotent, keeps application tracking independent, supports terminal timeline-only updates, profile-local reports, acknowledgement, cross-page attention, and backup schema v4 with v1-v3 import compatibility.
- Desktop and container share bridge routes. Direct lifecycle rewrites, custom-ledger pruning, pipeline finalization, and bootstrap promotion/rollback share a re-entrant cross-process data-directory lock and wait through contention. Writers reload lifecycle authority after acquiring it, direct transitions recheck evidence freshness before mutation, and pipeline finalization merges exact definitive direct-live rows published after its scan snapshot. The transaction covers feed, lifecycle, history, and private canonical-tombstone commit, restoring every prior plain/gzip projection on failure. Tombstones permit complete exact-identity reopening after source absence; missing or invalid canonical recovery data fails closed. Saved reads use an exact-identity overlay that includes private custom monitoring without exposing its ledger. Static browser mode renders published status/history but hides check/report actions and cannot mutate reports/attention.
- `jobs-unified-light.json` is the sole supported public feed and startup JSON is its bounded cache. CSV is removed. Full JSON is deprecated/private until pipeline handoff, direct CLI output, and rollback are SQLite-native.
- Deduplicated rows pass an exact identity preflight before lifecycle projection. Conflicting source aliases split into deterministic URL identities, while ambiguous legacy lifecycle evidence is removed from active matching and retained only in a private 2,000-row/30-day quarantine. Monitorable carried rows receive unknown `carried_seed` lifecycle coverage without becoming observations. Feed publication fails if any monitorable row lacks bounded availability state or one identity spans different canonical URL fingerprints.
- Non-custom Saved rows affected by a repair migrate only through one exact stored-URL fingerprint match; ambiguous rows become unmonitored and their current availability/report state is cleared without changing application tracking or historical activity. Fetch-report full/summary projections retain bounded availability health, identity audit counts, conflicts, sweep coverage, and shadow-classifier counts.
- Finalization exposes explicit indeterminate deduplication, identity, lifecycle, quality-audit, and output phases with periodic heartbeats and completed elapsed timing. Direct classification remains in shadow mode for this repair line.

## Promotion gate

Keep direct enforcement disabled during one healthy seven-day sweep. Promotion requires zero confirmed
false-unavailable classifications among saved jobs, a reviewed stratified sample of at least 100
ordinary jobs, and no unresolved high-risk classifier family. Record review evidence outside raw page
content, then enable `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1` for the runtime.

After promotion, preserve a reversible pre-reconciliation snapshot and run source-health-aware
reconciliation. Automatic rollback is limited to schema, identity, write, or feed-integrity failures.
Saved users receive per-job records presented as one digest. Clear the global freshness warning only
after a completed scheduled run, daily trustworthy saved/active-application evidence, and at least 95%
of active rows verified within seven days.

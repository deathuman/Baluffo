# Reliable Job Availability and Saved-Job Alerts

> - **Status:** Live and enforced on the container — `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1` verified in the running 0.2.140 container via `docker exec printenv` on 2026-08-28 (evidence: [`../snapshots/availability-direct-promotion-2026-08-27.md`](../snapshots/availability-direct-promotion-2026-08-27.md)); two post-update scheduled runs clean, no false mass-unavailable wave. Desktop promotion follow-up: the packaged desktop launcher now also sets `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1` (2026-09-03), so manual availability checks apply evidence on desktop too — the earlier "desktop stays shadow" restriction is superseded by operator approval. Remaining: bounded monitoring window through ~2026-08-31 — canary rechecks of the operator-confirmed false-unavailable rows and a clean saved-page digest — then archive this plan.
> - **Use this when:** operating, reviewing, or extending job availability lifecycle, direct validation, or Saved attention
> - **Canonical for:** rollout gates and remaining promotion/reconciliation operations
> - **Not canonical for:** row fields or endpoint shapes; use the contract docs
> - **Then inspect:** [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../storage-contract.md`](../storage-contract.md), [`../testing.md`](../testing.md)
> - **Last updated:** 2026-08-28 (enforcement flag verified live in the running container; monitoring window opened through ~2026-08-31 before plan archival)

## Implemented baseline

- Seed rows are no longer current observations; source-scoped success can retire them while failed/skipped sources age conservatively.
- Canonical `available`, `verification_overdue`, and `unavailable` state, exact stable aliases, active-only publication, 30-day lazy history, and availability health/conflict/shadow summaries are implemented.
- Direct checking is bounded, public-only, provider-detail-aware, posting-action/structured-evidence based, rate-limited, and shadow-first. Redirected evidence must retain the checked provider posting identity and tenant. Arbitrary successful pages, global apply phrases, unrelated application actions, unrelated visible closure copy, and raw script/template phrases are not definitive evidence. Successful and warning pipeline publication projects transitions through the shared service before consuming the complete safely bounded saved-first/oldest-first sweep plan through a four-worker background drain. A private compact latest-check checkpoint advances the rotation across runs; unavailable saved rows and privately scoped custom Saved URLs stay in rotation, and desktop startup catches up overdue rows without an OS service.
- Saved projection is transition-idempotent, keeps application tracking independent, supports terminal timeline-only updates, profile-local reports, acknowledgement, cross-page attention, and backup schema v4 with v1-v3 import compatibility.
- Desktop and container share bridge routes. Direct lifecycle rewrites, custom-ledger pruning, pipeline finalization, and bootstrap promotion/rollback share a re-entrant cross-process data-directory lock and wait through contention. Writers reload lifecycle authority after acquiring it, direct transitions recheck evidence freshness before mutation, and pipeline finalization merges exact definitive direct-live rows published after its scan snapshot. The transaction covers feed, lifecycle, history, and private canonical-tombstone commit, restoring every prior plain/gzip projection on failure. Tombstones permit complete exact-identity reopening after source absence; missing or invalid canonical recovery data fails closed. Saved reads use an exact-identity overlay that includes private custom monitoring without exposing its ledger. Static browser mode renders published status/history but hides check/report actions and cannot mutate reports/attention.
- `jobs-unified-light.json` is the sole supported public feed and startup JSON is its bounded cache. CSV is removed. Full JSON is deprecated/private until pipeline handoff, direct CLI output, and rollback are SQLite-native.
- Deduplicated rows pass an exact identity preflight before lifecycle projection. Conflicting source aliases split into deterministic URL identities, while ambiguous legacy lifecycle evidence is removed from active matching and retained only in a private schema-v2 2,000-row/30-day quarantine. Candidates that remain unidentifiable are removed from both publication and observations, privately quarantined by hashed exact-alias evidence, and reported as degraded coverage. The accepted feed still fails closed if any monitorable row lacks bounded availability state or one identity spans different canonical URL fingerprints. Monitorable carried rows receive unknown `carried_seed` lifecycle coverage without becoming observations.
- A second exact pass catches collisions introduced by deterministic assignment itself. It re-keys URL-bearing rows by canonical URL and quarantines/excludes URL-less members, so production-only generated-ID collisions degrade coverage instead of aborting otherwise valid publication.
- Non-custom Saved rows affected by a repair migrate only through one exact stored-URL fingerprint match; ambiguous rows become unmonitored and their current availability/report state is cleared without changing application tracking or historical activity. Fetch-report full/summary projections retain bounded availability health, identity audit counts, conflicts, sweep coverage, and shadow-classifier counts.
- Finalization exposes explicit indeterminate deduplication, identity, lifecycle, quality-audit, and output phases with periodic heartbeats and elapsed timing. Exceptions write a bounded terminal failed report and task state before process exit so the pipeline parent receives the real error code; orphan repair remains a last-resort fallback. Direct classification remains in shadow mode for this repair line.

## Promotion gate

**Promoted 2026-08-27 for the private Umbrel raw-LAN container (ships in the pending 0.2.140 release)** —
evidence snapshot:
[`../snapshots/availability-direct-promotion-2026-08-27.md`](../snapshots/availability-direct-promotion-2026-08-27.md).
`BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1` is set in `deathuman-baluffo/docker-compose.yml` (container
runtime only; desktop stays shadow until separately promoted); it goes live when 0.2.140 is bumped,
published, and installed via the standard Umbrel app-store update. The gate that was satisfied:

- One healthy seven-day sweep: 17 consecutive successful pipelines 2026-08-20 → 2026-08-27.
- Zero confirmed false-unavailable among saved jobs: operator-confirmed clean Saved page 2026-08-27.
- Reviewed stratified sample of ≥100 ordinary jobs: 100-row sample reviewed with automated HEAD +
  title-presence verification; 3 confirmed false-unavailable rows were all `source_absent` residuals
  (no verdict-classifier errors), recorded in the snapshot.
- No unresolved high-risk classifier family: shadow counts benign; identity clean.

Post-promotion: preserve a reversible pre-reconciliation snapshot and run source-health-aware
reconciliation. Automatic rollback is limited to schema, identity, write, or feed-integrity failures.
Saved users receive per-job records presented as one digest. Clear the global freshness warning only
after a completed scheduled run, daily trustworthy saved/active-application evidence, and at least 95%
of active rows verified within seven days.

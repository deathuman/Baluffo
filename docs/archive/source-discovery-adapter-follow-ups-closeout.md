# Source Discovery Adapter Follow-Ups Closeout

> - **Status:** Closed — deletion-first cleanup complete, remaining items not actioned
> - **Use this when:** checking why the source-discovery evidence tracker is no longer active, or verifying what was cleaned up vs deferred
> - **Canonical for:** the completed deletion-first adapter cleanup, the Sheet-directory static fallback behavior change, and the explicit closeout decision for unactioned follow-up items
> - **Not canonical for:** saved-job/local-user data contracts, bridge endpoint contracts, persisted payload schemas, or fetcher adapter inventory
> - **Then inspect:** [`../snapshots/source-discovery-directory-web-evidence-2026-04-29.md`](../snapshots/source-discovery-directory-web-evidence-2026-04-29.md), [`../snapshots/source-discovery-fresh-audit-evidence-2026-04-29.md`](../snapshots/source-discovery-fresh-audit-evidence-2026-04-29.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and the archive's [`jobs-fetcher-aggressive-simplification-closeout.md`](jobs-fetcher-aggressive-simplification-closeout.md)
> - **Archive date:** 2026-05-04

The deletion-first adapter cleanup is complete. The project priority chain moved past source-discovery evidence work to dedup correctness, lifecycle accuracy, sync confidence, and UI polish. Source-discovery behavior changes are lower risk now because the quality pressure is understood (Sheet-directory/static zero-job dominance) and remaining gains are incremental without a production-quality sync foundation in place.

## Completion Baseline

- Gameprog, Gamesmap, Sheet-directory, Web-derived discovery, and GameDevMap public runtime paths use audit-artifact rows.
- `activeAuditEnabled`, adapter-owned `cachePath`, legacy `cacheTtlMinutes`, web direct scanner exports, and the unused generic direct-scan helper are removed from source-discovery runtime/tests.
- `src/source_discovery` C901 offenders are cleared in [`scripts/complexity_baseline.json`](../../scripts/complexity_baseline.json).
- The first evidence-backed P2 behavior change landed: unrecovered Sheet-directory static homepage fallbacks are no longer carried forward after HTTP recovery fails to find a usable provider or jobs page. Tested by:
  - `test_sheet_directory_audit_opt_in_recovery_miss_drops_static_fallback` (asserts `staticCandidates == []`)
  - `test_sheet_directory_audit_opt_in_recovery_replaces_static_fallback` (asserts recovered careers URL replaces homepage)
- The local after-change rerun under `_out/source-discovery-directory-web-evidence-20260429-sheet-static-after-nobom` showed improved zero-job pressure: validated candidates `166 -> 89`, zero-job candidates `143 -> 67`, Sheet zero-job `125 -> 49`, static zero-job `111 -> 19`.

## Items Not Actioned (intentionally closed)

These original items were not executed because the project shifted focus after April 29. They do not represent unfinished work — no source-discovery behavior changes are planned in the current priority chain.

| Original # | Item | Why Closed |
|---|---|---|
| 1 | Capture tracked Sheet/static after-change evidence as checked-in snapshot | Behavior shipped and tested; before/after counts already summarized in this closeout. Formalizing local `_out` data into a snapshot adds near-zero value. |
| 2 | Run bounded GameDevMap evidence | No GameDevMap behavior changes planned. Timeout root cause already documented in the fresh-audit evidence snapshot. |
| 3 | Choose the next P2 behavior change from evidence | Project priority moved past source-discovery behavior work to dedup/lifecycle/sync/UI. The strongest candidate from existing evidence was static fallback quality tuning. |

## Protected Surfaces (preserved)

- Saved jobs and local user data.
- Current frontend/local storage behavior for saved/local user sections.
- Current UI/runtime invocation paths that start discovery and fetch flows.
- Bridge/API contracts needed by the current UI/runtime.
- Queue, pending review, tombstone, static suppression, and admin auto-approval behavior when candidates enter the current product flow.

## Hard Gates (historical — for reference if source-discovery work resumes)

- No new helper unless the same slice deletes or substantially thins adapter-owned code.
- Each source-discovery refactor should be net LOC-negative unless it adds new source coverage.
- Do not restore legacy direct discovery or legacy config compatibility paths.
- No adapter should own fetch, recovery, probe, dedupe, report, or audit lifecycle after migration.
- No new source-discovery C901 offenders.
- Behavior changes are allowed inside discovery/fetch internals only when protected surfaces remain tested.
- Browser-recovery expansion must wait for evidence showing meaningful recovered yield.

## Decision Rules (historical)

- If a path does not improve current active-source/job discovery and is not a protected surface, prefer deletion or deferral.
- If preserving old behavior blocks simplification, preserve only current product behavior and test that boundary.
- If evidence points to operational noise rather than product yield, fix the noise narrowly and return to evidence-backed behavior work.
- If a behavior change cannot show before/after impact, capture evidence first instead of changing runtime policy.

## Archive Record

| Action | Detail |
|---|---|
| Plan closed and moved to archive | This file replaces `docs/plans/source-discovery-adapter-follow-ups.md` |
| Evidence snapshots remain in place | `docs/snapshots/source-discovery-*.md` (still valid as reference) |
| Completion is revert-safe | All removed code is accessible via git history |
| No active follow-up tracker | Source-discovery behavior work is not tracked elsewhere; resume conditions would need a fresh evidence run |

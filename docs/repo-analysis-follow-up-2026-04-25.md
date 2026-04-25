# Repo Analysis Follow-up Tracker - 2026-04-25

> - **Status:** Active follow-up tracker
> - **Use this when:** turning the 2026-04-25 repo-analysis findings into scoped follow-up work
> - **Canonical for:** triage priority, validation status, and pickup order for this analysis pass
> - **Not canonical for:** long-term data contracts, source-registry policy, adapter implementation details, or release requirements
> - **Then inspect:** [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md), [`scraping-pipeline.md`](scraping-pipeline.md), [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md), and the owning source files
> - **Last updated:** 2026-04-26

This tracker validates the repo-analysis results against active docs and targeted code evidence. It is a pickup plan, not a replacement for the time-bound failure snapshot or the subsystem contracts.

## Tracker Maintenance

When work from this tracker is started or completed, update this file in the same change:

- Move the item status in the progress table.
- Add implementation notes only for decisions that future work needs to know.
- Add verification evidence when an item is completed.
- Keep the original 2026-04-25 snapshot counts intact; record fresh run counts separately when a later run is used.

## Progress Overview

| Priority | Theme | Status | Current note |
|----------|-------|--------|--------------|
| P0 | Restore diagnostic trust and live fetch health | Complete | Implemented 2026-04-26; targeted verification passed. |
| P1 | Reduce operational noise | Not started | Rebaseline active/pending registry and latest fetch report before edits. |
| P2 | Update architecture inventory before more refactor work | Not started | Provider/social plugin facts need inventory refresh before extraction work. |
| P3 | Guardrails and cleanup decisions | Not started | Size policy and snapshot archival depend on later fresh-run evidence. |

## Completed Work Log

### 2026-04-26 - P0 data-pipeline follow-up

Completed:

- `social_x` loader compatibility: `run_social_x_source` accepts `heartbeat_callback` and forwards it through API, scraper fallback, and RSS fallback fetch paths.
- Static redirect handling: static HTML fetches can follow one safe same-host HTTP(S) redirect and reject missing-location, looping, credentialed, non-HTTP(S), downgrade, or cross-host redirects.
- `needsReviewBreakdown` diagnostics: added additive `rawMarkerCount` and `includedCount` counters while preserving the existing shaped breakdown semantics.
- Fetch report contract docs: updated [`DATA_CONTRACT.md`](DATA_CONTRACT.md) and [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md).

Baseline note:

- Current local report artifacts no longer reproduce the 2026-04-25 failure surface: `_out/latest/build/portable/ship/data/jobs-fetch-report.json` has `0` sources, and `data/jobs-fetch-report.json` is a small `fetch_live_1` report with `10` sources and `1` failed source. The implementation used the original snapshot as the historical failing baseline and targeted tests for verification.

Verification:

- `python -m pytest tests/test_jobs_fetcher_parsing.py tests/test_jobs_fetcher_pipeline.py::test_social_x_skips_fresh_query_without_fetching tests/test_pipeline_stage_source_execution.py::test_stage_passes_heartbeat_to_any_loader_that_accepts_it_without_breaking_plain_loaders tests/test_social_x_heartbeat.py -q` -> `25 passed`
- `python -m pytest tests/jobs_static/ -q` -> `123 passed`
- `python -m pytest tests/jobs_static/test_browser_and_regression_queues.py -q` -> `22 passed`
- `npm run lint:repo-guardrails` -> passed

## Validation Summary

| Finding | Status | Evidence / adjustment |
|---------|--------|-----------------------|
| Overall repo health is strong, with data-pipeline failures as the biggest open risk | Confirmed from docs | Active docs, guardrail routing, and the completed closeout record support the high structural score; the active failure snapshot identifies the live pipeline debt. |
| `social_x` heartbeat callback mismatch | Completed 2026-04-26 | The active X social loader now accepts `heartbeat_callback` and forwards it through fetch paths. |
| Static redirect failures are a high-yield fetch issue | Partially completed 2026-04-26 | Static HTML fetch now handles one safe same-host redirect. A future fresh run should confirm how many of the original 42 redirect failures remain. |
| `needs_review` source rows and summary breakdown differ | Completed 2026-04-26 | `needsReviewBreakdown` now reports `rawMarkerCount` and `includedCount` so raw markers and shaped zero-kept diagnostics can be reconciled without changing bucket semantics. |
| Duplicate registry entries for Scopely, Nintendo, and Paradox | Confirmed from snapshot; rebaseline before edits | The snapshot records stale duplicate coverage. Current registry data may have changed, so use the latest active/pending registry before changing suppression policy. |
| Greenhouse stale slugs such as `guerrillagames` and `larian-studios` | Confirmed | The snapshot and current registry references both show those slugs. Audit against provider behavior before deleting or replacing sources. |
| Provider API plugin extraction is still entirely pending | Partially stale | `src/jobs/adapters/plugins/provider_api/` now exists and registers provider plugins. Future work should update the inventory and continue extraction only where behavior work justifies it. |
| Social plugin extraction is still entirely pending | Partially stale | `src/jobs/adapters/plugins/social/register.py` exists, but the stable social surface still contains provider logic. Treat this as an inventory/update task before any new extraction wave. |
| Five deferred closeout modules remain acceptable large owners | Confirmed as historical context | `docs/archive/history/final-leaf-closeout-program.md` marks them intentionally deferred, not default refactor lanes. Reopen only for real behavior work. |

## Priority Work

### P0 - Restore Diagnostic Trust and Live Fetch Health - Complete

1. **Fix `social_x` loader signature compatibility.**
   - Status: Complete 2026-04-26.
   - Result: Active X loader accepts `heartbeat_callback`; RSS and scraper fallback coverage lives in `tests/test_social_x_heartbeat.py`.

2. **Rebaseline and normalize safe static redirects.**
   - Status: Implemented 2026-04-26; fresh-run impact still pending.
   - Result: Static HTML fetch follows one safe same-host HTTP(S) redirect; tests cover safe redirect, cross-host rejection, missing location, and redirect loop.
   - Follow-up: A later fresh fetch run should compare remaining redirect failures against the original 42-source snapshot bucket.

3. **Clarify `needs_review` reporting semantics.**
   - Status: Complete 2026-04-26.
   - Result: Existing shaped breakdown remains intact; `rawMarkerCount` and `includedCount` make the diagnostic reconciliation explicit.

### P1 - Reduce Operational Noise - Not started

4. **Resolve duplicate active/pending coverage for Scopely, Nintendo, and Paradox.**
   - Status: Not started.
   - Rebaseline current active, pending, and rejected registry rows.
   - Prefer annotation, tombstoning, or `existing_family_match` suppression over deleting useful provider coverage.

5. **Audit stale Greenhouse board slugs.**
   - Status: Not started.
   - Check failing slugs such as `guerrillagames` and `larian-studios` against current provider URLs.
   - If a slug has moved, update the registry and add a regression case; if it is gone, demote or tombstone it with a documented reason.

6. **Set policy for zero-job pending rows.**
   - Status: Not started.
   - The snapshot records 173 pending rows with zero positive discovery jobs.
   - Choose one policy before code changes: keep but annotate, hide from default UI, tombstone after N runs, or reject after human review.

7. **Separate clean `ok` from `ok` with warnings.**
   - Status: Not started.
   - The snapshot records successful sources carrying warning/error text.
   - Add a distinct reporting/UI classification only if it improves operator decisions without changing source success semantics.

### P2 - Update Architecture Inventory Before More Refactor Work - Not started

8. **Refresh `adapter-plugin-inventory.md`.**
   - Status: Not started.
   - Record the current provider plugin files and social plugin registration state.
   - Replace stale "not rolled out" wording with current boundaries and remaining gaps.

9. **Continue provider/social plugin extraction only when tied to behavior work.**
   - Status: Not started.
   - Provider plugins already cover more than the analysis implied.
   - Social extraction should start with a compatibility audit of `social.py`, `plugins/social/register.py`, and loader registration.

10. **Leave `social_parsers.py` alone unless parser behavior changes.**
    - Status: Standing guidance.
    - It remains an intentionally large specialized owner from the closeout stop list.

### P3 - Guardrails and Cleanup Decisions - Not started

11. **Decide the `jobs-unified.json` size policy.**
    - Status: Not started.
    - The snapshot records a 79 MB unified JSON output and a size-guardrail breach.
    - Decide whether the correct fix is a raised guardrail, smaller payload, compression, or package-time behavior.

12. **Close out the failure snapshot when P0/P1 are resolved.**
    - Status: Blocked on P1 and fresh-run evidence.
    - Archive the snapshot only after a fresh run proves the tracked failures are gone or intentionally reclassified.
    - If failures persist, keep the snapshot linked and replace this tracker with a living operational runbook.

13. **Do not reopen deferred large modules as cleanup-only work.**
    - Status: Standing guidance.
    - The closeout program intentionally stopped with five specialized owners.
    - Any line-budget guard should preserve the current stop-list rationale instead of creating a generic refactor mandate.

## Pickup Order

1. P1 rebaseline: inspect current fetch report plus active/pending/rejected registries and capture duplicate-family, zero-job pending, stale Greenhouse slug, and `ok`-with-warning counts.
2. P1 implementation: clean up duplicate coverage, stale Greenhouse slugs, zero-job pending policy, and `ok`-with-warning reporting.
3. P2 documentation: refresh `adapter-plugin-inventory.md` after P1 confirms current source-family behavior.
4. P3 decisions: decide output size policy and snapshot archival/promotion after fresh-run evidence exists.

## Rebaseline Checklist

Before closing any item above, capture:

- Fresh `jobs-fetch-report.json` path, run id, and timestamp.
- Fresh source count, failed source count, and `ok`-with-warning count.
- Fresh `needs_review` raw marker count and `summary.needsReviewBreakdown` total.
- Fresh active/pending registry counts and duplicate-family examples.
- Fresh `jobs-unified.json`, light JSON, and CSV byte sizes.

Keep those numbers in the implementation PR or in an updated operational doc; do not overwrite the original snapshot's observed counts.

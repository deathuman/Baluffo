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
| P1 | Reduce operational noise | Complete | Implemented 2026-04-26; duplicate active variants were demoted, repeated zero-job pending rows are hidden by policy, and ok-with-warning diagnostics are additive. |
| P2 | Update architecture inventory before more refactor work | Not started | Provider/social plugin facts need inventory refresh before extraction work. |
| P3 | Guardrails and cleanup decisions | Not started | Size policy and snapshot archival depend on later fresh-run evidence. |

## Completed Work Log

### 2026-04-26 - P1 operational-noise follow-up

Current local rebaseline before edits:

- `data/jobs-fetch-report.json`: `runId=fetch_live_1`, `sources=0`, `failedSources=0`; this artifact is not useful for current warning/failure counts.
- `data/source-registry-active.json`: `613` rows; all rows currently have `registryState=active` and `candidateState=live`.
- `data/source-registry-pending.json`: absent in this workspace snapshot.
- `data/source-registry-rejected.json`: absent in this workspace snapshot.
- Confirmed duplicate active families: Scopely `2`, Nintendo `3`, Paradox Interactive `3`.
- Confirmed stale/no-URL active placeholders for cleanup: Guerrilla Games and Larian Studios.

P1 decisions:

- Repeated zero-job pending rows remain recoverable but are hidden from default views after `deferCount >= 3`.
- Same-studio duplicate active coverage is resolved by demoting weaker variants, not deleting rows.
- Clean `ok` vs `ok` with warnings is exposed through additive report counters and admin diagnostics without changing source success semantics.

Completed:

- Added registry helpers for deterministic duplicate-family demotion and hidden repeated zero-job pending rows.
- Added default hidden-pending filtering to `/registry/pending`, with `includeHidden=1` for explicit review views.
- Added `summary.okCleanSources` and `summary.okWithWarningSources` while preserving source report `status="ok"`.
- Updated admin diagnostics to show ok-with-warning counts and hidden pending reasons.
- Updated registry data: active rows `613 -> 605`, pending rows `0 -> 8`; all 8 demoted rows are hidden pending rows with `pendingReason=duplicate_family_weaker_variant`.

Duplicate demotions:

- Guerrilla Games -> Guerrilla Games (Greenhouse)
- Larian Studios -> Larian Studios (Lever)
- Larian Studios (Manual Website) -> Larian Studios (Lever)
- Paradox Careers -> Paradox Interactive (Teamtailor)
- Paradox Interactive (Sheet) -> Paradox Interactive (Teamtailor)
- Nintendo (Sheet) -> Nintendo (Manual Website)
- Nintendo (Manual Website) -> Nintendo (Manual Website)
- Scopely (Sheet) -> Scopely (Greenhouse)

Verification:

- `python -m pytest tests/test_source_registry.py tests/test_source_registry_p1_operational_noise.py tests/source_discovery/ tests/test_admin_bridge_fetcher_metrics.py -q` -> `141 passed`
- `python -m pytest @test_jobs_fetcher_*.py tests/jobs_static/ -q` -> `376 passed`
- `npm run test:frontend:unit` -> passed
- `npm run test:frontend` -> `10 passed`
- `npm run lint:repo-guardrails` -> passed
- `npm run lint:precommit` -> passed
- Final focused rerun after adapter/router cleanup: `python -m pytest tests/test_social_x_heartbeat.py tests/test_source_registry_p1_operational_noise.py tests/source_discovery/test_p1_operational_noise.py tests/jobs_static/test_needs_review_breakdown_counters.py -q` -> `8 passed`; `npm run test:frontend:unit` -> passed

Note: `npm run test:frontend -- --runInBand` was attempted and failed before running tests because the repo's frontend suite is Playwright-based and Playwright does not support the Jest `--runInBand` option.

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
| Duplicate registry entries for Scopely, Nintendo, and Paradox | Completed 2026-04-26 | Weaker active variants were demoted to hidden pending rows with `duplicate_family_weaker_variant`; no rows were deleted. |
| Greenhouse stale slugs such as `guerrillagames` and `larian-studios` | Completed 2026-04-26 for local stale placeholders | No-URL/stale local placeholders were demoted through the duplicate-family policy. Future provider URL replacement still requires current provider evidence. |
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

### P1 - Reduce Operational Noise - Complete

4. **Resolve duplicate active/pending coverage for Scopely, Nintendo, and Paradox.**
   - Status: Complete 2026-04-26.
   - Result: Weaker same-studio active variants were demoted to hidden pending rows with `duplicateOfSourceId`; rows were not deleted.

5. **Audit stale Greenhouse board slugs.**
   - Status: Complete 2026-04-26 for local stale/no-URL placeholders.
   - Result: Guerrilla/Larian stale placeholders were demoted through duplicate-family cleanup.
   - Follow-up: Only update provider slugs after a later current provider check proves replacement URLs.

6. **Set policy for zero-job pending rows.**
   - Status: Complete 2026-04-26.
   - Result: Pending rows with `jobsFound == 0` and `deferCount >= 3` are marked `candidateState=hidden`, `hiddenFromDefault=true`, and `pendingReason=repeated_zero_jobs`.

7. **Separate clean `ok` from `ok` with warnings.**
   - Status: Complete 2026-04-26.
   - Result: Fetch summaries include additive `okCleanSources` and `okWithWarningSources`; admin diagnostics display ok-with-warning counts without changing success status semantics.

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

1. P2 documentation: refresh `adapter-plugin-inventory.md` after P1 confirms current source-family behavior.
2. P3 decisions: decide output size policy and snapshot archival/promotion after fresh-run evidence exists.
3. Future fresh-run validation: compare active/pending counts, hidden pending rows, ok-with-warning counts, and remaining provider failures against the original 2026-04-25 snapshot.

## Rebaseline Checklist

Before closing any item above, capture:

- Fresh `jobs-fetch-report.json` path, run id, and timestamp.
- Fresh source count, failed source count, and `ok`-with-warning count.
- Fresh `needs_review` raw marker count and `summary.needsReviewBreakdown` total.
- Fresh active/pending registry counts and duplicate-family examples.
- Fresh `jobs-unified.json`, light JSON, and CSV byte sizes.

Keep those numbers in the implementation PR or in an updated operational doc; do not overwrite the original snapshot's observed counts.

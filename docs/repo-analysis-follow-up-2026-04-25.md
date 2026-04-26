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
| P2 | Update architecture inventory before more refactor work | Complete | Implemented 2026-04-26; adapter plugin inventory now reflects current provider/social/static plugin boundaries. |
| P3 | Guardrails and cleanup decisions | In progress | Item 11 output-size policy is implemented; snapshot archival still depends on later fresh-run evidence. |

## Completed Work Log

### 2026-04-26 - P3 item 11 output-size policy

Current local rebaseline before edits:

- `data/jobs-unified.json`: `41,800,270` bytes.
- Compact full JSON estimate from the current local payload: `32,500,126` bytes.
- `data/jobs-unified-light.json`: `20,349,544` bytes.
- `data/jobs-unified.csv`: `23,475,837` bytes.

Historical snapshot baseline preserved from 2026-04-25:

- `jobs-unified.json`: `79,136,607` bytes.
- Light JSON: `54,626,176` bytes.
- CSV: `46,495,593` bytes.

Chosen policy:

- Keep all current output files and fields.
- Compact unified JSON outputs.
- Replace the hard-coded 50 MB JSON/CSV rule with documented per-artifact thresholds.
- Leave package-time behavior for a separate future decision.

Completed:

- Compact serialized `jobs-unified.json` and `jobs-unified-light.json`; report/debug JSON remains pretty-printed.
- Added named output-size thresholds: full JSON `80_000_000`, light JSON `60_000_000`, CSV `50_000_000`.
- Preserved `summary.sizeGuardrailExceeded` and added `summary.sizeGuardrails` with per-artifact bytes, limits, and exceeded flags.
- Documented the additive summary payload and clarified that packaging still ships full JSON, light JSON, CSV, and startup preview.

Verification:

- `python -m pytest tests/test_jobs_fetcher_pipeline.py tests/test_latest_run_report.py -q` -> `44 passed`
- `python -m pytest tests/jobs_static/test_needs_review_breakdown_counters.py -q` -> `3 passed`
- `python -m pytest tests/test_jobs_fetcher_pipeline.py tests/test_latest_run_report.py tests/test_pipeline_io.py tests/jobs_static/test_needs_review_breakdown_counters.py -q` -> `47 passed`
- `npm run lint:repo-guardrails` -> passed
- `npm run lint:precommit` -> passed

### 2026-04-26 - P2 adapter-plugin inventory refresh

Current local rebaseline before edits:

- `src/jobs/adapters/provider_api.py`: approximately 282 LOC; now a stable dispatch surface over provider plugins.
- `src/jobs/adapters/plugins/provider_api/register.py`: registers Greenhouse, Teamtailor, JSON-feed providers, Personio, BambooHR, Workday, and HTML-board providers.
- `src/jobs/adapters/social.py`: approximately 620 LOC; remains the stable social loader compatibility surface and still owns orchestration for X/Mastodon plus Reddit wrapping.
- `src/jobs/adapters/plugins/social/register.py`: registers Reddit, X, and Mastodon social plugins.
- `src/jobs/adapters/social_parsers.py`: approximately 811 LOC; remains a specialized parser owner and is not part of this P2 implementation wave.

Completed:

- Refreshed [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) as an active doc with current ownership metadata.
- Replaced stale first-wave extraction wording with current provider dispatch, social plugin registration, and behavior-tied future extraction guidance.
- Updated the static plugin inventory with currently registered modules including Amanotes, ATS wrappers, Frontier, Nintendo CSOD, and rendered-card support.
- Left runtime code, source registry data, and parser behavior unchanged.

Verification:

- `npm run lint:repo-guardrails` -> passed
- `npm run lint:precommit` -> passed

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
| Provider API plugin extraction status | Corrected 2026-04-26 | `adapter-plugin-inventory.md` now records provider plugin registration and dispatch boundaries; future provider work should start in the owning plugin module unless a compatibility surface must change. |
| Social plugin extraction status | Corrected 2026-04-26 | `adapter-plugin-inventory.md` now records social plugin registration and the remaining stable `social.py` compatibility surface; future extraction requires behavior work or a refactor charter. |
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

### P2 - Update Architecture Inventory Before More Refactor Work - Complete

8. **Refresh `adapter-plugin-inventory.md`.**
   - Status: Complete 2026-04-26.
   - Result: The inventory now records current provider plugin files, social plugin registration state, and static plugin modules.
   - Result: Stale "first-wave extraction" wording was replaced with current boundaries and remaining gaps.

9. **Continue provider/social plugin extraction only when tied to behavior work.**
   - Status: Standing guidance after P2 refresh.
   - Provider plugins already cover most provider lanes.
   - Social extraction should start with a compatibility audit of `social.py`, `plugins/social/register.py`, and loader registration.

10. **Leave `social_parsers.py` alone unless parser behavior changes.**
    - Status: Standing guidance.
    - It remains an intentionally large specialized owner from the closeout stop list.

### P3 - Guardrails and Cleanup Decisions - In progress

11. **Decide the `jobs-unified.json` size policy.**
    - Status: Complete 2026-04-26.
    - Result: Unified JSON outputs are compact serialized with no row-field pruning.
    - Result: `summary.sizeGuardrailExceeded` now derives from named per-artifact thresholds exposed in additive `summary.sizeGuardrails`.
    - Result: Package-time output selection remains unchanged and is a separate future decision.

12. **Close out the failure snapshot when P0/P1 are resolved.**
    - Status: Blocked on P1 and fresh-run evidence.
    - Archive the snapshot only after a fresh run proves the tracked failures are gone or intentionally reclassified.
    - If failures persist, keep the snapshot linked and replace this tracker with a living operational runbook.

13. **Do not reopen deferred large modules as cleanup-only work.**
    - Status: Standing guidance.
    - The closeout program intentionally stopped with five specialized owners.
    - Any line-budget guard should preserve the current stop-list rationale instead of creating a generic refactor mandate.

## Pickup Order

1. P3 decisions: decide snapshot archival/promotion after fresh-run evidence exists.
2. Future behavior-tied adapter work: use the refreshed plugin inventory before changing provider or social loader boundaries.
3. Future fresh-run validation: compare active/pending counts, hidden pending rows, ok-with-warning counts, and remaining provider failures against the original 2026-04-25 snapshot.

## Rebaseline Checklist

Before closing any item above, capture:

- Fresh `jobs-fetch-report.json` path, run id, and timestamp.
- Fresh source count, failed source count, and `ok`-with-warning count.
- Fresh `needs_review` raw marker count and `summary.needsReviewBreakdown` total.
- Fresh active/pending registry counts and duplicate-family examples.
- Fresh `jobs-unified.json`, light JSON, and CSV byte sizes.

Keep those numbers in the implementation PR or in an updated operational doc; do not overwrite the original snapshot's observed counts.

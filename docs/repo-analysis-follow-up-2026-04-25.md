# Repo Analysis Follow-up Tracker - 2026-04-25

> - **Status:** Active follow-up tracker
> - **Use this when:** picking the next work after the completed 2026-04-25 repo-analysis P0-P3 closeout
> - **Canonical for:** remaining follow-up order, rebaseline expectations, and boundaries for future work from this analysis pass
> - **Not canonical for:** completed implementation history, long-term data contracts, source-registry policy, adapter implementation details, or release requirements
> - **Then inspect:** [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md), [`scraping-pipeline.md`](scraping-pipeline.md), [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md), and [`archive/history/repo-analysis-follow-up-completed-2026-04-26.md`](archive/history/repo-analysis-follow-up-completed-2026-04-26.md)
> - **Last updated:** 2026-04-26

This tracker now only owns future pickup order. The completed P0-P3 implementation record was archived to [`archive/history/repo-analysis-follow-up-completed-2026-04-26.md`](archive/history/repo-analysis-follow-up-completed-2026-04-26.md).

## Current State

| Area | Status | Current note |
|------|--------|--------------|
| P0-P3 numbered items | Complete | `social_x`, safe static redirects, needs-review counters, operational-noise policy, adapter inventory refresh, output-size policy, failure-snapshot validation, and deferred-module budgets are implemented. |
| Residual static-failure triage | Complete | High-yield cleanup reduced fresh isolated failures from `73` to `46`; remaining failures are narrower static/provider/browser triage, not the original broad residual bucket. |
| Narrow static/provider/browser follow-up | Complete | Focused cleanup reduced fresh isolated failures from `46` to `32`; stale/dead HTTP 404/500/522 rows are removed from default active fetches. |
| Site-changed static failure follow-up | Complete | Focused cleanup reduced fresh isolated failures from `32` to `8`; no residual `site_changed` rows remain in the classifier split. |
| Browser-required static follow-up | Complete | Focused cleanup reduced fresh isolated failures from `8` to `5`; no residual `browser_required` rows remain in the classifier split. |
| Failure snapshot | Active external-access runbook | Fresh validation kept [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md) active only for the remaining `5` anti-bot/rate-limit rows. |
| Deferred large modules | Guarded | Exact source line ceilings are enforced through `tools/repo_health/deferred_source_line_budget.json` and `npm run lint:repo-guardrails`. |

## Recently Completed

### 2026-04-26 - Browser-required static follow-up

Starting baseline from `_out/static-site-changed-validation/jobs-fetch-report.json`:

- `488` sources attempted.
- `8` failed/error sources.
- Residual classifier split: `5` `anti_bot_or_rate_limited` and `3` `browser_required`.
- Scoped `browser_required` rows:
  - `static:listing_url:https://nca.ncsoft.com/en-us/careers`
  - `static:listing_url:https://krafton.com/en/careers/jobs/`
  - `static:listing_url:https://www.rollicgames.com/jobs`
- Out of scope for this pass: the `5` anti-bot/rate-limit rows.

Completed changes:

- Added active Greenhouse provider rows for verified KRAFTON boards: `krafton`, `studiokraftonboard`, `kraftonamericas`, and `kraftonindia`.
- Hid the KRAFTON static row and a nondeterministically resurfaced Santa Monica Studio static alias as provider-covered browser-required rows.
- Added a focused NCSoft static plugin and extended the rendered-card static plugin so Rollic can use Playwright after a blocked first fetch.
- Hid NCSoft and Rollic static rows as `browser_required_static_source` because clean validation did not prove valid active jobs after the plugin attempts.

Fresh validation from clean root `_out/static-browser-required-validation-clean/jobs-fetch-report.json`:

| Counter | Value |
|---------|------:|
| Sources attempted | 484 |
| Successful sources | 479 |
| Failed/error sources | 5 |
| Clean `ok` sources | 450 |
| `ok` sources with warnings | 29 |
| Final output jobs | 29,834 |
| `needsReviewBreakdown.rawMarkerCount` | 105 |
| `needsReviewBreakdown.includedCount` | 99 |
| Error rows containing `HTTP 301` | 0 |
| Error rows containing `HTTP 302` | 0 |
| Error rows containing `HTTP 308` | 0 |
| Error rows containing `HTTP 404` | 0 |
| Error rows containing `HTTP 429` | 4 |
| Active registry rows | 555 |
| Pending registry rows | 62 |
| Hidden pending rows | 62 |
| Size guardrail exceeded | no |
| `jobs-unified.json` bytes | 37,606,383 |
| Light JSON bytes | 20,792,655 |
| CSV bytes | 30,541,971 |

Residual classifier output after validation:

| Triage class | Count |
|--------------|------:|
| `anti_bot_or_rate_limited` | 5 |

Verification:

- `python -m pytest tests/test_static_residual_failures_measurement.py tests/test_source_registry.py tests/test_source_registry_p1_operational_noise.py -q`
- `python -m pytest tests/jobs_static/ -q`
- `python -m pytest tests/test_jobs_fetcher_*.py -q` equivalent via explicit PowerShell file expansion
- `npm run lint:repo-guardrails`
- `npm run lint:precommit`

Closeout decision:

- This browser-required follow-up is complete because no `browser_required` rows remain in the fresh residual classifier split.
- Keep the April 25 failure snapshot active as an external-access runbook for the remaining anti-bot/rate-limit rows only.

### 2026-04-26 - Site-changed static failure follow-up

Starting baseline from `_out/static-narrow-validation/jobs-fetch-report.json`:

- `510` sources attempted.
- `32` failed/error sources.
- Residual classifier split: `22` `site_changed`, `6` `anti_bot_or_rate_limited`, and `4` `browser_required`.
- `442` clean `ok` sources and `36` `ok` sources with warnings.
- `105` raw `needs_review` markers and `99` shaped included rows.
- Error rows: `21` with `HTTP 301`, `0` with `HTTP 302`, `1` with `HTTP 308`, `0` with `HTTP 404`, and `5` with `HTTP 429`.

Chosen scope:

- Act only on rows classified by `tools/measurements/pipeline/static_residual_failures.py` as `site_changed`.
- Preserve anti-bot/rate-limit and browser-required rows as baseline-only residuals for a separate follow-up.
- Preserve source rows during registry cleanup by hiding, demoting, or canonicalizing; do not delete rows or infer new provider families.

Completed changes:

- Hid `22` active static rows with `pendingReason="site_changed_static_source"` and `residualFailureClass="site_changed"` after validation showed redirect status evidence plus zero extraction.
- Preserved the source rows in `data/source-registry-pending.json`; no site-changed rows were deleted and no new provider families were inferred.
- Updated taxonomy so static zero-extract rows with redirect/status evidence are diagnosed as `site_changed`; generic static/manual no-jobs rows without redirect evidence remain `js_required`.

Fresh validation from `_out/static-site-changed-validation/jobs-fetch-report.json`:

| Counter | Value |
|---------|------:|
| Sources attempted | 488 |
| Successful sources | 480 |
| Failed/error sources | 8 |
| Clean `ok` sources | 446 |
| `ok` sources with warnings | 34 |
| Final output jobs | 29,757 |
| `needsReviewBreakdown.rawMarkerCount` | 106 |
| `needsReviewBreakdown.includedCount` | 100 |
| Error rows containing `HTTP 301` | 0 |
| Error rows containing `HTTP 302` | 0 |
| Error rows containing `HTTP 308` | 0 |
| Error rows containing `HTTP 404` | 0 |
| Error rows containing `HTTP 429` | 4 |
| Active registry rows | 555 |
| Pending registry rows | 58 |
| Hidden pending rows | 58 |
| Size guardrail exceeded | no |
| `jobs-unified.json` bytes | 37,499,551 |
| Light JSON bytes | 20,749,667 |
| CSV bytes | 30,456,676 |

Residual classifier output after validation:

| Triage class | Count |
|--------------|------:|
| `anti_bot_or_rate_limited` | 5 |
| `browser_required` | 3 |

Verification:

- `python -m pytest tests/test_static_residual_failures_measurement.py tests/test_source_registry.py tests/test_source_registry_p1_operational_noise.py tests/jobs_static/test_static_site_changed_taxonomy.py -q`
- `python -m pytest tests/jobs_static/ -q`
- `python -m pytest tests/test_jobs_fetcher_*.py -q` equivalent via explicit PowerShell file expansion
- `npm run lint:repo-guardrails`
- `npm run lint:precommit`

Closeout decision:

- This site-changed follow-up is complete because no `site_changed` rows remain in the fresh residual classifier split.
- Keep the April 25 failure snapshot active as a narrow residual runbook for the remaining anti-bot/rate-limit and browser-required rows only.

### 2026-04-26 - Residual static-failure triage

Starting baseline from `_out/p3-item-12-validation/jobs-fetch-report.json`:

- `73` failed/error static sources.
- `36` error rows contain `HTTP 301`; `5` contain `HTTP 302`.
- `2` rows contain invalid/template detail URL failures.
- `7` rows contain `HTTP 404`; `3` contain `HTTP 429`; `3` LinkedIn rows contain `HTTP 999`.
- `13` rows are plain no-jobs extraction failures without HTTP/template markers.

Chosen scope:

- Tackle high-yield residual buckets only: redirect aliases, static plugin fetch paths that bypass shared redirect handling, malformed/template detail links, unsupported LinkedIn/third-party static entries, and clear provider/static registry cleanup.
- Preserve rows during registry cleanup by hiding or demoting unsupported/static-no-value entries rather than deleting them.
- Keep the failure snapshot active unless fresh validation materially reduces or explicitly reclassifies the remaining failure surface.

Completed changes:

- Static redirect safety now allows one `www.`/bare-host alias redirect and HTTP-to-HTTPS same-site upgrade while preserving rejection of unrelated cross-host redirects, redirect chains, credentialed targets, non-HTTP(S) targets, and HTTPS downgrades.
- Static plugin fast paths receive the shared `fetch_html_cached` helper; `sheet_studios` listing/detail fetches now inherit redirect handling and cache behavior instead of calling direct `fetch_text`.
- Malformed/template detail links are skipped before fetch as `dead_listing_page` diagnostics.
- Registry rows were preserved while default-active noise was reduced:
  - Converted Lucky VR from static listing to the existing Breezy provider path.
  - Hid `13` unsupported static rows as pending with `pendingReason="unsupported_static_source"`: LinkedIn/profile/search/post rows, Y Combinator, opaque legacy/third-party pages, Homerun/Zoho hosted static rows, and other unsupported provider-hosted static entries.

Fresh validation from `_out/static-residual-validation/jobs-fetch-report.json`:

| Counter | Value |
|---------|------:|
| Sources attempted | 521 |
| Successful sources | 475 |
| Failed/error sources | 46 |
| Clean `ok` sources | 442 |
| `ok` sources with warnings | 33 |
| `needsReviewBreakdown.rawMarkerCount` | 103 |
| `needsReviewBreakdown.includedCount` | 98 |
| Error rows containing `HTTP 301` | 26 |
| Error rows containing `HTTP 302` | 2 |
| Error rows containing `HTTP 308` | 1 |
| Error rows containing `HTTP 404` | 5 |
| Error rows containing `HTTP 429` | 4 |
| LinkedIn `HTTP 999` rows | 0 |
| Invalid/template detail URL rows | 0 |
| Plain no-jobs extraction failures without HTTP/template markers | 0 |
| Active registry rows | 592 |
| Pending registry rows | 21 |
| Hidden pending rows | 21 |
| Size guardrail exceeded | no |
| `jobs-unified.json` bytes | 37,522,320 |
| Light JSON bytes | 20,756,135 |
| CSV bytes | 30,481,113 |

Verification:

- `python -m pytest tests/jobs_static/test_static_redirect_fetch.py tests/jobs_static/test_detail_fallback.py tests/jobs_static/test_detail_link_filtering.py tests/jobs_static/test_rendered_cards_and_plugins.py tests/jobs_static/test_static_plugin_fetch_helper.py tests/jobs_static/test_browser_and_regression_queues.py -q`
- `python -m pytest tests/test_source_registry.py tests/test_source_registry_p1_operational_noise.py -q`
- `npm run lint:repo-guardrails`
- `npm run lint:precommit`

Closeout decision:

- This broad residual triage item is complete because the high-yield buckets were reduced or reclassified with fresh evidence.
- Keep the April 25 failure snapshot active; the remaining `46` failures are still material and should be handled as narrower source/provider/browser triage instead of reopening this broad pass.

### 2026-04-26 - Narrow static/provider/browser follow-up

Starting baseline from `_out/static-residual-validation/jobs-fetch-report.json`:

- `521` sources attempted.
- `46` failed/error sources.
- `442` clean `ok` sources and `33` `ok` sources with warnings.
- `103` raw `needs_review` markers and `98` shaped included rows.
- Error rows: `26` with `HTTP 301`, `2` with `HTTP 302`, `1` with `HTTP 308`, `5` with `HTTP 404`, and `4` with `HTTP 429`.

Completed changes:

- Added `tools/measurements/pipeline/static_residual_failures.py` to classify failed rows into narrow follow-up classes without changing the public fetch-report schema.
- Hid `7` stale/dead static rows with `pendingReason="stale_or_dead_static_source"` after validation showed HTTP 404/500/522 or stale detail URL failures.
- Hid `8` redundant static aliases with `pendingReason="redundant_static_stronger_coverage"` and `duplicateOfSourceId` pointing at the retained stronger source.
- Fixed the Windows command-length failure in `scripts/precommit_gate.py` by chunking filtered full-repo `--files` pre-commit runs.

Fresh validation from `_out/static-narrow-validation/jobs-fetch-report.json`:

| Counter | Value |
|---------|------:|
| Sources attempted | 510 |
| Successful sources | 478 |
| Failed/error sources | 32 |
| Clean `ok` sources | 442 |
| `ok` sources with warnings | 36 |
| Final output jobs | 29,743 |
| `needsReviewBreakdown.rawMarkerCount` | 105 |
| `needsReviewBreakdown.includedCount` | 99 |
| Error rows containing `HTTP 301` | 21 |
| Error rows containing `HTTP 302` | 0 |
| Error rows containing `HTTP 308` | 1 |
| Error rows containing `HTTP 404` | 0 |
| Error rows containing `HTTP 429` | 5 |
| Error rows containing `HTTP 500` | 0 |
| Error rows containing `HTTP 522` | 0 |
| Active registry rows | 577 |
| Pending registry rows | 36 |
| Hidden pending rows | 36 |
| Size guardrail exceeded | no |
| `jobs-unified.json` bytes | 37,469,006 |
| Light JSON bytes | 20,742,497 |
| CSV bytes | 30,428,339 |

Residual classifier output after validation:

| Triage class | Count |
|--------------|------:|
| `site_changed` | 22 |
| `anti_bot_or_rate_limited` | 6 |
| `browser_required` | 4 |

Verification:

- `python -m pytest tests/test_static_residual_failures_measurement.py tests/test_source_registry.py tests/test_source_registry_p1_operational_noise.py -q`
- `python -m pytest tests/jobs_static/ -q`
- `python -m pytest tests/test_jobs_fetcher_*.py -q` equivalent via explicit PowerShell file expansion
- `npm run lint:repo-guardrails`
- `npm run lint:precommit`

Closeout decision:

- This narrow follow-up is complete because stale/dead and redundant coverage rows were removed from default active fetches and the remaining failures now fit a smaller runbook.
- Keep the April 25 failure snapshot active; the remaining `32` failures are still material and are mostly site-change redirects, anti-bot/rate-limit cases, or browser-required extraction gaps.

## Pickup Order

1. **Decide anti-bot/rate-limit residual policy.**
   - Start from `_out/static-browser-required-validation-clean/`, not the original April 25 counts.
   - Baseline to preserve: `484` sources attempted, `5` failed/error sources, `450` clean `ok`, `29` `ok` with warnings, `105` raw `needs_review` markers, `99` shaped included rows, `0` error rows containing `HTTP 301`, `0` containing `HTTP 302`, `0` containing `HTTP 308`, `0` containing `HTTP 404`, and `4` containing `HTTP 429`.
   - Goal: decide whether the remaining `5` `anti_bot_or_rate_limited` rows should be hidden as expected external-access limits, retried through a separate browser/Scrapy policy, or kept as an explicit runbook.
   - Success condition: a fresh isolated run leaves no unexpected static/provider/browser residuals, allowing the April 25 snapshot to be archived or converted into a stable external-access note.

2. **Behavior-tied adapter work only.**
   - Use [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) before changing provider, social, or static loader boundaries.
   - Provider plugin extraction is no longer a default task; most provider lanes already dispatch through registered plugins.
   - Social or parser extraction should only happen with behavior work or a separate explicit refactor charter.

3. **Deferred large-module budget maintenance.**
   - If real behavior work changes one of the five deferred modules, update `tools/repo_health/deferred_source_line_budget.json` in the same change with the new exact line count and rationale.
   - Do not grow those modules for cleanup-only work.

## Rebaseline Checklist

Before closing any future item from this tracker, capture:

- Fresh artifact root and command line.
- Fresh `jobs-fetch-report.json` path, run id if present, and timestamp.
- Fresh source count, failed/error source count, clean `ok` count, and `ok`-with-warning count.
- Fresh `needs_review` raw marker count and `summary.needsReviewBreakdown.includedCount`.
- Fresh active/pending registry counts and hidden pending count.
- Fresh full JSON, light JSON, and CSV byte sizes.

Keep new numbers separate from the original 2026-04-25 snapshot counts.

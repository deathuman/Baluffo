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
| Anti-bot/rate-limit follow-up | Complete with residual runbook | Scoped browser retry coverage reduced fresh isolated failures from `5` to `1`; the scoped static `429` rows no longer fail, while Lucky VR remains retry-exhausted in the browser queue and one new unscoped `429` row appeared. |
| Failure snapshot | Active external-access runbook | Fresh validation kept [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md) active only for the remaining external-access residuals. |
| Deferred large modules | Guarded | Exact source line ceilings are enforced through `tools/repo_health/deferred_source_line_budget.json` and `npm run lint:repo-guardrails`. |

## In Progress

No active implementation item is in progress. Start with the pickup order below.

## Recently Completed

### 2026-04-26 - Anti-bot/rate-limit browser retry follow-up

Starting baseline from `_out/static-browser-required-validation-clean/jobs-fetch-report.json`:

- `484` sources attempted.
- `5` failed/error sources.
- `450` clean `ok` sources and `29` `ok` sources with warnings.
- `105` raw `needs_review` markers and `99` shaped included rows.
- Error rows: `0` with `HTTP 301`, `0` with `HTTP 302`, `0` with `HTTP 308`, `0` with `HTTP 404`, and `4` with `HTTP 429`.
- Residual classifier split: `5` `anti_bot_or_rate_limited`.
- Scoped retry rows:
  - `static:listing_url:https://corp.worldwinner.com/careers/`
  - `static:listing_url:https://stairwaygames.com/careers`
  - `static:listing_url:https://www.creative-assembly.com/careers`
  - `breezy:board_url:https://lucky-vr.breezy.hr/`
  - `static:listing_url:https://hadean.com/careers/`

Chosen scope:

- Keep the five rows active and add scoped browser/Scrapy retry coverage with `antiBotBrowserRetry=true`.
- Preserve default behavior for non-flagged anti-bot/rate-limit sources.
- Keep the April 25 failure snapshot active unless fresh validation removes unexpected static/provider/browser residuals.

Completed changes:

- Added `antiBotBrowserRetry=true` to the five scoped active source rows without hiding, demoting, deleting, or migrating them.
- Static sources with this flag may use Playwright listing fallback for `HTTP 429` / "Too Many Requests"; non-flagged static `429` behavior remains unchanged.
- HTML-board provider rows for Breezy/JazzHR/Ashby can receive the guarded Playwright helper and retry flagged `403`, `429`, and timeout failures before emitting queue-compatible anti-bot diagnostics.
- Browser fallback queue classification now includes `anti_bot_or_challenge` and `rate_limited`, preserving the queue schema.

Fresh validation from `_out/static-antibot-validation/jobs-fetch-report.json` used two fetch passes against the same isolated root so the second pass could consume the first-pass browser fallback queue:

| Counter | Value |
|---------|------:|
| Sources attempted | 484 |
| Successful sources | 483 |
| Failed/error sources | 1 |
| Clean `ok` sources | 453 |
| `ok` sources with warnings | 30 |
| Final output jobs | 29,747 |
| `needsReviewBreakdown.rawMarkerCount` | 107 |
| `needsReviewBreakdown.includedCount` | 101 |
| Error rows containing `HTTP 301` | 0 |
| Error rows containing `HTTP 302` | 0 |
| Error rows containing `HTTP 308` | 0 |
| Error rows containing `HTTP 404` | 8 |
| Error rows containing `HTTP 429` | 2 |
| Browser fallback queue rows | 11 |
| Scoped anti-bot rows still in queue | 1 |
| Active registry rows | 555 |
| Pending registry rows | 62 |
| Hidden pending rows | 62 |
| Size guardrail exceeded | no |
| `jobs-unified.json` bytes | 37,570,743 |
| Light JSON bytes | 20,764,453 |
| CSV bytes | 30,539,396 |

Residual classifier output after the second validation pass:

| Triage class | Count |
|--------------|------:|
| `anti_bot_or_rate_limited` | 1 |

Residual details:

- The only source-level failure is a new unscoped live `429` row: `static:listing_url:https://www.kumkuatgames.com/career`.
- The scoped Lucky VR Breezy row remains in `jobs-browser-fallback-queue.json` after provider browser retry and the second-pass `scrapy_static_sources` attempt: `breezy:board_url:https://lucky-vr.breezy.hr/`.
- The scoped static rows for WorldWinner, Stairway Games, Creative Assembly, and Hadean no longer remain as failed source-level residuals in the fresh validation.

Verification:

- `python -m pytest tests/jobs_static/test_antibot_browser_retry.py tests/jobs_static/test_browser_and_regression_queues.py tests/jobs_static/test_static_source_execution.py tests/jobs_static/test_scrapy_static_runtime.py -q`
- `python -m pytest tests/test_provider_api_plugins.py tests/test_jobs_fetcher_providers.py tests/test_static_residual_failures_measurement.py tests/test_source_registry.py tests/test_source_registry_p1_operational_noise.py -q`
- `python -m pytest tests/test_jobs_fetcher_*.py -q` equivalent via explicit PowerShell file expansion
- `npm run lint:repo-guardrails`
- `npm run lint:precommit`

Closeout decision:

- This anti-bot/rate-limit follow-up is complete because the scoped browser retry policy is implemented and the fresh source-level failure count fell from `5` to `1`.
- Keep the April 25 failure snapshot active as an external-access runbook because Lucky VR remains retry-exhausted in the browser queue and KumKuat surfaced as a new unscoped live `429` residual.

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

1. **External-access residual closeout.**
   - Start from `_out/static-antibot-validation/`, not the original April 25 counts.
   - Baseline to preserve: `484` sources attempted, `1` failed/error source, `453` clean `ok`, `30` `ok` with warnings, `107` raw `needs_review` markers, `101` shaped included rows, classifier split `1` `anti_bot_or_rate_limited`, and `11` browser fallback queue rows.
   - Inspect first: retry-exhausted Lucky VR queue evidence (`breezy:board_url:https://lucky-vr.breezy.hr/`) and the new unscoped KumKuat `429` source-level failure (`static:listing_url:https://www.kumkuatgames.com/career`).
   - Goal: decide whether these are accepted external-access limits, need one more scoped retry flag, or should be hidden/demoted as non-viable static/provider rows.
   - Success condition: a fresh isolated run leaves no unexpected source-level static/provider/browser failures, allowing the April 25 snapshot to be archived or converted into a short stable external-access note.

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

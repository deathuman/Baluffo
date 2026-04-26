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
| Failure snapshot | Active | Fresh validation kept [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md) active because `46` static/provider failures remain material. |
| Deferred large modules | Guarded | Exact source line ceilings are enforced through `tools/repo_health/deferred_source_line_budget.json` and `npm run lint:repo-guardrails`. |

## Recently Completed

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

## Pickup Order

1. **Narrow remaining static/provider/browser failures.**
   - Start from the fresh validation counters in [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md), not the original April 25 counts.
   - Baseline to preserve: `521` sources attempted, `46` failed/error sources, `442` clean `ok`, `33` `ok` with warnings, `103` raw `needs_review` markers, `98` shaped included rows, `26` error rows containing `HTTP 301`, `2` containing `HTTP 302`, `1` containing `HTTP 308`, `5` containing `HTTP 404`, and `4` containing `HTTP 429`.
   - Goal: classify the remaining failures into source registry cleanup, existing-provider migration, static plugin behavior work, browser/Scrapy handling, anti-bot/rate-limit policy, or no-op documentation.
   - Success condition: a fresh isolated run proves the remaining `46` failures are materially reduced or converted into a narrow runbook suitable for archiving the April 25 snapshot.

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

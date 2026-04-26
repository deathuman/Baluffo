# Discovery and Fetch Failure Snapshot - 2026-04-25

> - **Status:** Active external-access runbook
> - **Use this when:** picking up the remaining source-access residuals from the 2026-04-25 repo-analysis follow-up
> - **Canonical for:** current external-access residuals and the latest isolated validation evidence
> - **Not canonical for:** discovery contracts, fetch contracts, bridge APIs, long-term source-registry policy, or historical implementation notes
> - **Then inspect:** [`repo-analysis-follow-up-2026-04-25.md`](repo-analysis-follow-up-2026-04-25.md), [`scraping-pipeline.md`](scraping-pipeline.md), and [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md)
> - **Last updated:** 2026-04-26

This page is no longer a broad historical snapshot. It now records only the live external-access residuals that still block archival of the April 25 failure handoff.

## Current Validation

Latest useful validation root: `_out/static-antibot-validation/`.

Commands used for the latest validation:

- First pass: `BALUFFO_DATA_DIR=_out/static-antibot-validation python src/jobs_fetcher.py --output-dir _out/static-antibot-validation --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`
- Second pass against the same root: `BALUFFO_DATA_DIR=_out/static-antibot-validation python src/jobs_fetcher.py --output-dir _out/static-antibot-validation --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`

Fresh fetch counters after the second pass:

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
| Browser fallback queue rows | 11 |
| Active registry rows | 555 |
| Pending registry rows | 62 |
| Hidden pending rows | 62 |
| Size guardrail exceeded | no |
| `jobs-unified.json` bytes | 37,570,743 |
| Light JSON bytes | 20,764,453 |
| CSV bytes | 30,539,396 |

Residual classifier output:

| Triage class | Count |
|--------------|------:|
| `anti_bot_or_rate_limited` | 1 |

## Current Residuals

- `breezy:board_url:https://lucky-vr.breezy.hr/`
  - Scope: browser fallback queue residual.
  - Evidence: remains retry-exhausted in `jobs-browser-fallback-queue.json` after provider browser retry and the second-pass `scrapy_static_sources` attempt.
  - Current decision needed: accept as external-access limit, add another scoped retry path, or hide/demote if the board is non-viable.

- `static:listing_url:https://www.kumkuatgames.com/career`
  - Scope: source-level residual.
  - Evidence: surfaced as a new unscoped live `429` failure in the latest isolated validation.
  - Current decision needed: add `antiBotBrowserRetry=true` if the source is still valuable, or hide/demote if it is not viable static coverage.

## Closeout Rule

Archive this page, or replace it with a short stable external-access note, only after a fresh isolated validation leaves no unexpected source-level static/provider/browser failures.

Do not reopen the old P0-P3 or broad static-failure history from this page. Use the completed-history archive only when implementation provenance is needed.

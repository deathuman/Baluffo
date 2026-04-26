# Repo Analysis Follow-up Tracker - 2026-04-25

> - **Status:** Active follow-up tracker
> - **Use this when:** picking the next work after the 2026-04-25 repo-analysis follow-up
> - **Canonical for:** current pickup order, rebaseline expectations, and boundaries for future work from this analysis pass
> - **Not canonical for:** completed implementation history, long-term data contracts, source-registry policy, adapter implementation details, or release requirements
> - **Then inspect:** [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md), [`scraping-pipeline.md`](scraping-pipeline.md), [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md), and [`archive/history/repo-analysis-follow-up-completed-2026-04-26.md`](archive/history/repo-analysis-follow-up-completed-2026-04-26.md)
> - **Last updated:** 2026-04-26

This tracker owns only current and future pickup work. Completed implementation history lives in [`archive/history/repo-analysis-follow-up-completed-2026-04-26.md`](archive/history/repo-analysis-follow-up-completed-2026-04-26.md), while current runtime behavior belongs in the contract docs linked above.

## Current State

| Area | Status | Current note |
|------|--------|--------------|
| P0-P3 numbered items | Complete | `social_x`, safe static redirects, needs-review counters, operational-noise policy, adapter inventory refresh, output-size policy, failure-snapshot validation, and deferred-module budgets are implemented. |
| Static/provider/browser triage | Complete | Broad static failures, stale/dead rows, site-changed rows, and browser-required rows have been reduced or reclassified through fresh isolated validation. |
| Anti-bot/rate-limit follow-up | Complete with residual runbook | Scoped browser retry coverage reduced fresh isolated failures from `5` to `1`; scoped static `429` rows no longer fail. |
| Failure snapshot | Active external-access runbook | [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md) now tracks only current external-access residuals. |
| Deferred large modules | Closed | The five former deferred roots are now thin compatibility facades under 500 LOC; `tools/repo_health/deferred_source_line_budget.json` has no active entries. |

## Active Residuals

Latest useful validation root: `_out/static-antibot-validation/`.

Latest validation state:

| Counter | Value |
|---------|------:|
| Sources attempted | 484 |
| Failed/error sources | 1 |
| Clean `ok` sources | 453 |
| `ok` sources with warnings | 30 |
| `needsReviewBreakdown.rawMarkerCount` | 107 |
| `needsReviewBreakdown.includedCount` | 101 |
| Residual classifier split | `1` `anti_bot_or_rate_limited` |
| Browser fallback queue rows | 11 |

Known residuals:

- `breezy:board_url:https://lucky-vr.breezy.hr/` remains retry-exhausted in `jobs-browser-fallback-queue.json` after provider browser retry and a second-pass `scrapy_static_sources` attempt.
- `static:listing_url:https://www.kumkuatgames.com/career` surfaced as a new unscoped live `429` source-level failure.

## Pickup Order

1. **External-access residual closeout.**
   - Start from `_out/static-antibot-validation/`, not the original April 25 counts.
   - Inspect first: retry-exhausted Lucky VR queue evidence and the unscoped KumKuat `429` source-level failure.
   - Decide whether these are accepted external-access limits, need one more scoped retry flag, or should be hidden/demoted as non-viable static/provider rows.
   - Success condition: a fresh isolated run leaves no unexpected source-level static/provider/browser failures, allowing the April 25 snapshot to be archived or converted into a short stable external-access note.

2. **Behavior-tied adapter work only.**
   - Use [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) before changing provider, social, or static loader boundaries.
   - Provider plugin extraction is no longer a default task; most provider lanes already dispatch through registered plugins.
   - Social or parser extraction should only happen with behavior work or a separate explicit refactor charter.

3. **Thin-facade maintenance.**
   - Keep `src/source_registry.py`, `src/ship/update_manager.py`, `src/jobs/adapters/social_parsers.py`, `src/source_discovery/core.py`, and `src/ship/packaged_smoke/runtime.py` as compatibility surfaces.
   - New behavior should land in the focused leaf owner unless the public facade contract itself is changing.

## Rebaseline Checklist

Before closing any future item from this tracker, capture:

- Fresh artifact root and command line.
- Fresh `jobs-fetch-report.json` path, run id if present, and timestamp.
- Fresh source count, failed/error source count, clean `ok` count, and `ok`-with-warning count.
- Fresh `needs_review` raw marker count and `summary.needsReviewBreakdown.includedCount`.
- Fresh active/pending registry counts and hidden pending count.
- Fresh full JSON, light JSON, and CSV byte sizes.

Keep new numbers separate from the original 2026-04-25 snapshot counts when a comparison is still useful.

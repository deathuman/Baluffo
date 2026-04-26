# Repo Analysis Follow-up Completed Work - 2026-04-26

Historical record for the completed P0-P3 work that came from [`../../repo-analysis-follow-up-2026-04-25.md`](../../repo-analysis-follow-up-2026-04-25.md). This page is not the active pickup tracker; use the active tracker and [`../../discovery-fetch-failure-snapshot-2026-04-25.md`](../../discovery-fetch-failure-snapshot-2026-04-25.md) for current follow-up.

## Completed Scope

| Priority | Result |
|----------|--------|
| P0 | Fixed `social_x` heartbeat callback compatibility, added safe same-host static redirect handling, and added raw/included `needsReviewBreakdown` counters without changing shaped breakdown semantics. |
| P1 | Reduced operational noise with duplicate-family demotion, repeated zero-job hidden pending policy, and additive clean `ok` vs `ok` with warnings counters. |
| P2 | Refreshed [`../../adapter-plugin-inventory.md`](../../adapter-plugin-inventory.md) to match current provider, social, and static plugin ownership. |
| P3 item 11 | Compact serialized unified JSON outputs and replaced the previous hard-coded size rule with named per-artifact guardrails. |
| P3 item 12 | Ran isolated fresh discovery/fetch validation and kept the April 25 failure snapshot active because residual static failures remain material. |
| P3 item 13 | Added exact source line ceilings for the five intentionally deferred large modules through repo guardrails. |

## Important Outcomes

- The `social_x` runtime signature mismatch is gone.
- The explicit `HTTP redirect not followed/accepted` failure bucket is gone after safe static redirect handling, but the fresh validation still had `73` static source-level errors, including `36` rows containing `HTTP 301` and `5` containing `HTTP 302` text inside broader extraction failures.
- `summary.needsReviewBreakdown` now exposes `rawMarkerCount` and `includedCount`.
- Fetch summaries now expose additive `okCleanSources` and `okWithWarningSources`.
- Output-size reporting now exposes additive `summary.sizeGuardrails`.
- Deferred large module budgets are enforced by `npm run lint:repo-guardrails`.

## Fresh Validation From P3 Item 12

Validation artifacts were generated under ignored `_out/p3-item-12-validation/`.

Commands:

- `BALUFFO_DATA_DIR=_out/p3-item-12-validation python src/source_discovery.py --preset default --top 0`
- `BALUFFO_DATA_DIR=_out/p3-item-12-validation python src/jobs_fetcher.py --output-dir _out/p3-item-12-validation --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`

Key counters:

| Counter | Value |
|---------|------:|
| Discovery endpoints generated | 887 |
| Discovery candidates probed | 323 |
| Discovery validated candidates | 181 |
| Discovery queued candidates | 108 |
| Discovery deferred by caps | 73 |
| Fetch sources attempted | 557 |
| Fetch successful sources | 484 |
| Fetch failed/error sources | 73 |
| Clean `ok` sources | 449 |
| `ok` sources with warnings | 35 |
| Final output jobs | 29,825 |
| `needsReviewBreakdown.rawMarkerCount` | 104 |
| `needsReviewBreakdown.includedCount` | 99 |
| Full JSON bytes | 37,601,189 |
| Light JSON bytes | 20,788,387 |
| CSV bytes | 30,538,766 |

## Verification History

- P0 targeted tests and static tests passed before commit.
- P1 source registry, discovery, fetcher, static, frontend, and guardrail checks passed before commit.
- P2 documentation guardrails passed before commit.
- P3 item 11 pipeline/reporting tests and guardrails passed before commit.
- P3 item 12 documentation guardrails passed before commit.
- P3 item 13 `tests/test_repo_guardrails.py`, `npm run lint:repo-guardrails`, and `npm run lint:precommit` passed before commit.

Use git history for exact diffs and commit-level command output.

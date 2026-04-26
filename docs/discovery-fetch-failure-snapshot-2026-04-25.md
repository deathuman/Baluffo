# Discovery and Fetch Failure Snapshot - 2026-04-25

> - **Status:** Active investigation snapshot
> - **Use this when:** picking up the unresolved discovery and fetch issues from the fresh Baluffo build generated on 2026-04-25
> - **Canonical for:** the observed failure/counter state of the 2026-04-25 discovery and fetch runs only
> - **Not canonical for:** discovery contracts, fetch contracts, bridge APIs, or long-term source-registry policy
> - **Then inspect:** [`scraping-pipeline.md`](scraping-pipeline.md), [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md), and the referenced `_out/latest/build/portable/ship/data/` reports
> - **Last updated:** 2026-04-26

This is a time-bound handoff note for later triage. It records what the current fresh build produced, not what the system should guarantee after fixes.

Follow-up prioritization and validation status live in [`repo-analysis-follow-up-2026-04-25.md`](repo-analysis-follow-up-2026-04-25.md). Use that tracker before picking implementation order, then return here for the original observed counts.

## Fresh Validation After P0-P3 Item 11 - 2026-04-26

P3 item 12 used a fresh isolated validation run instead of the placeholder-sized local reports under `data/` and `_out/latest/`.

Validation artifacts were written under ignored `_out/p3-item-12-validation/` and are not intended for commit.

Commands:

- `BALUFFO_DATA_DIR=_out/p3-item-12-validation python src/source_discovery.py --preset default --top 0`
- `BALUFFO_DATA_DIR=_out/p3-item-12-validation python src/jobs_fetcher.py --output-dir _out/p3-item-12-validation --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`

Fresh discovery counters:

| Counter | Value |
|---------|------:|
| Endpoints generated | 887 |
| Candidates probed | 323 |
| Validated candidates | 181 |
| Review queue candidates | 108 |
| Deferred by caps | 73 |
| Probe failures/misses | 139 |
| Auto-approved/live candidates | 27 |
| Active registry after run | 632 |
| Pending registry after run | 99 |
| Hidden pending rows after run | 1 |

Fresh fetch counters:

| Counter | Value |
|---------|------:|
| Sources attempted | 557 |
| Successful sources | 484 |
| Failed/error sources | 73 |
| Clean `ok` sources | 449 |
| `ok` sources with warnings | 35 |
| Final output jobs | 29,825 |
| `needsReviewBreakdown.rawMarkerCount` | 104 |
| `needsReviewBreakdown.includedCount` | 99 |
| `ambiguous_review` shaped rows | 95 |
| `transport_network` shaped rows | 4 |
| Size guardrail exceeded | no |
| `jobs-unified.json` bytes | 37,601,189 |
| Light JSON bytes | 20,788,387 |
| CSV bytes | 30,538,766 |

Closeout decision:

- Keep this snapshot active rather than archiving it.
- Resolved or reclassified: the `social_x` heartbeat signature mismatch is gone, explicit `HTTP redirect not followed/accepted` buckets are gone, raw/included `needsReviewBreakdown` counters exist, and output size guardrails are no longer exceeded.
- Still material: `73` static source-level errors remain. The fresh report still has `36` error rows containing `HTTP 301` text and `5` containing `HTTP 302` text inside broader static extraction failures.
- Next action: open a separate static-failure triage lane for the remaining extraction failures before this snapshot can be archived.

## Source Reports

| Run | File | Run ID | Fresh build timestamp |
|-----|------|--------|-----------------------|
| Discovery | `_out/latest/build/portable/ship/data/source-discovery-report.json` | `discovery_51096fc0c1` | 2026-04-25 10:46 local |
| Discovery candidates | `_out/latest/build/portable/ship/data/source-discovery-candidates.json` | same discovery run | 2026-04-25 10:46 local |
| Pending registry | `_out/latest/build/portable/ship/data/source-registry-pending.json` | registry output | 2026-04-25 10:46 local |
| Active registry | `_out/latest/build/portable/ship/data/source-registry-active.json` | registry output | 2026-04-25 10:46 local |
| Fetch | `_out/latest/build/portable/ship/data/jobs-fetch-report.json` | `fetch_5073c6668e` | 2026-04-25 10:52 local |

## Current Discovery State

| Counter | Value |
|---------|------:|
| Endpoints generated | 887 |
| Candidates probed | 309 |
| Validated candidates | 153 |
| Review queue candidates | 85 |
| Deferred by caps | 68 |
| Failed probes | 35 |
| Probe misses | 119 |
| Skipped duplicates | 564 |
| Skipped invalid | 2 |
| Auto-approved this run | 14 |
| Live candidates from this run | 14 |
| Active registry after run | 641 |
| Pending registry after run | 173 |

Loss accounting records `154` probe failures/misses in total: `35` hard failed probes plus `119` probe misses.

## Discovery Failures To Tackle

- `68` candidates were deferred by caps: `33` by `adapter_cap` and `35` by `domain_cap`.
- `18` candidates had positive job evidence, and all `18` were cap-deferred static candidates.
- `14` of those positive cap-deferred candidates were auto-approved in this fresh build.
- `2` positive candidates were intentionally blocked by `existing_family_match`: `Forge Reply (Gameprog)` and `SEGA (Manual Website)`.
- `2` positive candidates were blocked as weak signals: `redBit Games (Gameprog)` and `Twin Wolves (Gameprog)`.
- `85` queued/review candidates had zero discovered jobs; these are review queue items, not expected active-source increases.
- The pending registry still has `173` rows with `0` positive discovery jobs. That is currently expected for hidden-zero filtering, but it remains noisy operational debt.
- The pending registry still contains stale/duplicate entries for studios that are already active, including `Scopely`, `Nintendo`, and `Paradox Interactive`.

Top discovery probe failure buckets from the run:

| Bucket | Count |
|--------|------:|
| `greenhouse:boards-api.greenhouse.io` | 13 |
| `gameprog` | 9 |
| `seed_careers_page` | 2 |
| `gamesmap` | 1 |
| `static:aesir-interactive.com` | 1 |

High-priority discovery follow-ups:

- Decide whether stale pending entries for already-active studios should be removed, suppressed, or annotated as duplicates during registry hydration.
- Investigate why many zero-job review candidates remain pending after discovery when they are not actionable without enabling zero-job rows.
- Audit the Greenhouse board-slug failures, especially stale slugs such as `guerrillagames` and `larian-studios`.
- Re-check Gameprog-derived static URLs with positive jobs after the cap-deferred approval path has run in a restarted bridge/site session.
- Keep `existing_family_match` as a block unless duplicate active-source policy changes.

## Current Fetch State

| Counter | Value |
|---------|------:|
| Sources attempted | 565 |
| Successful sources | 490 |
| Failed sources | 75 |
| Raw fetched jobs | 40,603 |
| Canonical input count | 70,347 |
| Final output jobs | 31,678 |
| Dedup merged count | 38,669 |
| Active source count seen by fetch | 640 |
| Pending source count seen by fetch | 173 |
| Newly approved since last run | 0 |
| Size guardrail exceeded | yes |
| `jobs-unified.json` bytes | 79,136,607 |
| Light JSON bytes | 54,626,176 |
| CSV bytes | 46,495,593 |

The fetch run succeeded overall but has a large source-level failure surface. The official report counts `75` failed sources. There are also `45` sources with status `ok` but a non-empty error/warning field, which makes the health counters harder to interpret.

## Fetch Failures To Tackle

Hard failed sources:

| Adapter | Failed sources |
|---------|---------------:|
| `static` | 74 |
| `social` | 1 |

Error/warning buckets across hard failures plus `ok` sources with errors:

| Bucket | Count |
|--------|------:|
| HTTP redirect not followed/accepted | 42 |
| No jobs extracted | 24 |
| HTTP 401/403 | 16 |
| HTTP 404 | 14 |
| Other source error/warning | 14 |
| Network error | 8 |
| Disabled adapter: `scrapy_static` | 1 |
| Signature mismatch: `social_x` heartbeat callback | 1 |

Needs-review signal:

- `jobs-fetch-report.json` marks many static sources for review rather than hard failure.
- The summary frequency breakdown reports `95` `ambiguous_review` rows and `4` `transport_network` rows.
- A direct source-report scan found `105` rows marked `needs_review`, so the source-report marker count and summary breakdown should be reconciled.

Clear code/config failures:

- `social_x` fails with `run_social_x_source() got an unexpected keyword argument 'heartbeat_callback'`.
- `scrapy_static_sources` has status `ok` but reports `No enabled scrapy_static sources`; decide whether that should be hidden, reported as skipped, or treated as configuration debt.
- `greenhouse_boards` has status `ok` and keeps `767` jobs, but the same source report contains board-level HTTP 404 warnings for stale slugs.
- The fetch output exceeds the size guardrail even though record guardrails pass; output-size reduction or packaging behavior needs a separate decision.

Representative static source failures:

| Source | Observed issue |
|--------|----------------|
| `https://careers.nintendo.com/jobs` | hard error: HTTP 301 plus no jobs extracted |
| `https://careers.nintendo.com/` | status `ok`, kept `12`, but job-detail crawl hit maximum redirects |
| `https://www.scopely.com/en/careers` | status `ok`, kept `0`; Scopely also has active Greenhouse coverage |
| `https://career.paradoxplaza.com/#jobs` | status `ok`, kept `6`; Paradox also has active Teamtailor/static registry entries |
| `http://www.reply.com/careers` | HTTP 301 then no jobs extracted |
| `https://bkomstudios.zohorecruit.com/jobs/careers` | invalid URL handling plus no jobs extracted |
| `https://careers.bungie.com/` | no jobs extracted |
| `https://corp.worldwinner.com/careers/` | HTTP 429 then no jobs extracted |
| `https://careers.amd.com/careers-home/jobs?keywords=game` | status `ok` with zero kept and network-error review |
| `https://boombit.com/careers/` | status `ok` with zero kept and network-error review |
| `https://bandainamcostudios.sg/careers/` | status `ok` with zero kept and network-error review |
| `https://careers.roblox.com/jobs` | status `ok` with zero kept and network-error review |

High-priority fetch follow-ups:

- Fix the `social_x` runtime signature mismatch before treating social fetch health as meaningful.
- Normalize static redirect handling so simple HTTP 301/302 careers-page redirects do not become source failures when the destination is safe.
- Separate `ok with warnings` from clean `ok` in source health UI/reporting.
- Decide whether zero-kept `ok` static rows should remain successful, become `needs_review`, or be hidden when they are known duplicate coverage.
- Reconcile `needs_review` counts between source-report rows and `summary.needsReviewBreakdown`.
- Review stale/duplicate active and pending coverage for Scopely, Nintendo, and Paradox so fetch and discovery do not present duplicate source health as separate approval problems.

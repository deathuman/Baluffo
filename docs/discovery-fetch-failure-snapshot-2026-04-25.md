# Discovery and Fetch Failure Snapshot - 2026-04-25

> - **Status:** Active external-access runbook
> - **Use this when:** picking up the unresolved discovery and fetch issues from the fresh Baluffo build generated on 2026-04-25
> - **Canonical for:** the observed failure/counter state of the 2026-04-25 discovery and fetch runs only
> - **Not canonical for:** discovery contracts, fetch contracts, bridge APIs, or long-term source-registry policy
> - **Then inspect:** [`scraping-pipeline.md`](scraping-pipeline.md), [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md), and the referenced `_out/latest/build/portable/ship/data/` reports
> - **Last updated:** 2026-04-26

This is a time-bound handoff note for later triage. It records what the current fresh build produced, not what the system should guarantee after fixes. The original broad April 25 failure surface has been reduced to external-access residuals.

Follow-up prioritization and validation status live in [`repo-analysis-follow-up-2026-04-25.md`](repo-analysis-follow-up-2026-04-25.md). Use that tracker before picking implementation order, then return here for the original observed counts.

## Browser-Required Static Follow-up Validation - 2026-04-26

The browser-required pass migrated provider-covered static aliases, added scoped static parsing support for NCSoft/Rollic investigation, and hid rows that did not prove valid active jobs in a clean validation run.

Validation artifacts were written under ignored `_out/static-browser-required-validation-clean/` and are not intended for commit.

Command:

- `BALUFFO_DATA_DIR=_out/static-browser-required-validation-clean python src/jobs_fetcher.py --output-dir _out/static-browser-required-validation-clean --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`

Fresh fetch counters after the browser-required static pass:

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

Closeout decision:

- Keep this snapshot active as an external-access runbook rather than a broad failure snapshot.
- Resolved or reclassified in this pass: the scoped NCSoft, KRAFTON, and Rollic browser-required rows no longer appear in default active failures. Santa Monica Studio was also hidden as a provider-covered browser-required alias after it resurfaced in validation.
- Remaining scope: `5` anti-bot/rate-limit rows. No `site_changed` or `browser_required` rows remain in the clean residual classifier split.

## Site-Changed Static Follow-up Validation - 2026-04-26

The site-changed pass hid high-confidence static rows where the validation report showed redirect status evidence plus zero extraction. It preserved source rows as hidden pending records and did not add new provider families.

Validation artifacts were written under ignored `_out/static-site-changed-validation/` and are not intended for commit.

Command:

- `BALUFFO_DATA_DIR=_out/static-site-changed-validation python src/jobs_fetcher.py --output-dir _out/static-site-changed-validation --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`

Fresh fetch counters after the site-changed static pass:

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

Closeout decision:

- Keep this snapshot active as a narrow residual runbook rather than a broad failure snapshot.
- Resolved or reclassified in this pass: all `site_changed` rows from the `_out/static-narrow-validation/` classifier split were removed from default active fetches.
- Remaining scope: `5` anti-bot/rate-limit rows and `3` browser-required rows. These are intentionally separate from the site-changed cleanup and should be handled by a focused external-access/browser follow-up or documented as expected limits before archival.

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

## Residual Static-Failure Triage Validation - 2026-04-26

The first residual static-failure pass targeted high-yield buckets only. It did not attempt to repair every static source.

Validation artifacts were written under ignored `_out/static-residual-validation/` and are not intended for commit.

Command:

- `BALUFFO_DATA_DIR=_out/static-residual-validation python src/jobs_fetcher.py --output-dir _out/static-residual-validation --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`

Fresh fetch counters after the residual static pass:

| Counter | Value |
|---------|------:|
| Sources attempted | 521 |
| Successful sources | 475 |
| Failed/error sources | 46 |
| Clean `ok` sources | 442 |
| `ok` sources with warnings | 33 |
| Final output jobs | 29,759 |
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

Closeout decision:

- Keep this snapshot active rather than archiving it.
- Resolved or reclassified in this pass: unsupported LinkedIn static rows, unsupported third-party/provider-hosted static rows, invalid/template detail URL failures, plugin fast-path redirect bypass, and safe `www.`/bare-host plus HTTP-to-HTTPS redirect aliases.
- Still material: `46` source-level failures remain, dominated by static `js_required` / broken extraction rows with residual HTTP status text.
- Next action: continue from [`repo-analysis-follow-up-2026-04-25.md`](repo-analysis-follow-up-2026-04-25.md) pickup order with a narrower static/provider/browser triage lane.

## Narrow Static/Provider/Browser Validation - 2026-04-26

The second residual pass added a local measurement classifier, hid high-confidence stale/dead rows, and suppressed redundant static aliases when a stronger active source remained.

Validation artifacts were written under ignored `_out/static-narrow-validation/` and are not intended for commit.

Command:

- `BALUFFO_DATA_DIR=_out/static-narrow-validation python src/jobs_fetcher.py --output-dir _out/static-narrow-validation --force-refresh-all --ignore-circuit-breaker --social-enabled --quiet`

Fresh fetch counters after the narrow static/provider/browser pass:

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

Closeout decision:

- Keep this snapshot active rather than archiving it.
- Resolved or reclassified in this pass: stale/dead HTTP 404/500/522 rows and redundant active static aliases.
- Still material: `32` source-level failures remain, now narrowed to site-change redirects, anti-bot/rate-limit cases, and browser-required extraction gaps.
- Next action: continue from [`repo-analysis-follow-up-2026-04-25.md`](repo-analysis-follow-up-2026-04-25.md) pickup order with a smaller runbook focused on those three triage classes.

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

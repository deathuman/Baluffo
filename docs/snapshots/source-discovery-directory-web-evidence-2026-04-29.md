# Source Discovery Directory/Web Evidence - 2026-04-29

> - **Status:** Active evidence snapshot
> - **Use this when:** choosing Sheet-directory, Gameprog, Gamesmap, or Web-derived discovery behavior changes
> - **Canonical for:** the latest isolated directory/web source-discovery run without GameDevMap
> - **Not canonical for:** GameDevMap active-audit tuning, saved-job/local-user data contracts, bridge contracts, queue contracts, or adapter runtime behavior
> - **Then inspect:** [`source-discovery-fresh-audit-evidence-2026-04-29.md`](source-discovery-fresh-audit-evidence-2026-04-29.md), [`source-discovery-zero-job-evidence-2026-04-29.md`](source-discovery-zero-job-evidence-2026-04-29.md), and [`source-discovery-adapter-follow-ups-closeout.md`](../archive/source-discovery-adapter-follow-ups-closeout.md)
> - **Last updated:** 2026-04-29

This snapshot records an isolated evidence run only. It does not change source-discovery behavior, adapters, saved jobs, local user data, bridge routes, frontend storage, queue policy, registry contracts, or artifact schemas.

## Summary

The split directory/web evidence run completed cleanly after excluding GameDevMap and redirecting audit artifacts into `_out`. Pass 1 produced fresh directory/web artifacts in about 184 seconds. Pass 2 reused those artifacts and completed in about 20 seconds.

The strongest current behavior signal is still Sheet-directory/static quality. The fresh pass produced 166 validated candidates; 143 had `jobsFound == 0`. Zero-job rows are concentrated in Sheet-directory and static candidates.

Two operational follow-ups also surfaced:

- `gameprog_no_current_openings` is repeatedly dropped as an unknown evidence type during merge.
- A non-fatal Playwright permission exception can be printed after report write; both passes still exited `0`.

## Commands

The clean run used `_out/source-discovery-directory-web-evidence-20260429-clean` with a no-BOM isolated config. GameDevMap was disabled through `stageToggles` and `gamedevmap.enabled=false`.

```powershell
$env:BALUFFO_DATA_DIR = (Resolve-Path '_out\source-discovery-directory-web-evidence-20260429-clean').Path
python src\source_discovery.py --preset uncapped --top 0 --timeout 12 --gameprog-enabled
```

The first attempt against `_out/source-discovery-directory-web-evidence-20260429` was discarded because its config was not accepted by the loader and GameDevMap still ran. The clean root above is the source of truth for this snapshot.

## Pass Comparison

| Metric | Pass 1 | Pass 2 |
| --- | ---: | ---: |
| Command result | completed | completed |
| Approx runtime | 184s | 20s |
| GameDevMap | skipped | skipped |
| Audit cache | fresh artifacts | all cache hits |
| Generated candidates | 888 | 888 |
| Survived dedupe | 492 | 361 |
| Probed candidates | 470 | 346 |
| Healthy/validated | 298 | 166 |
| Queued | 239 | 131 |
| Deferred by cap | 59 | 35 |
| Probe misses | 128 | 17 |

Pass 2 loaded the registry after pass 1's isolated auto-approval and pending movement, so its lower dedupe/probe/queue counts are expected.

## Directory Audit Evidence

| Adapter | Cache hit on pass 2 | Provider | Static | Failures | Recovery attempts | Pages fetched | Recovered provider | Recovered static | Browser candidates |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Sheet-directory | yes | 117 | 566 | 0 | 2,140 | 744 | 103 | 368 | 0 |
| Web-derived | yes | 14 | 14 | 2 | 7 | 2 | 0 | 0 | 2 |
| Gamesmap | yes | 0 | 4 | 1 | 96 | 14 | 0 | 0 | 1 |
| Gameprog | yes | 0 | 139 | 9 | 591 | 79 | 0 | 11 | 5 |

Cache reuse reduced scan-stage work sharply. In pass 2 the report's slowest stage was probing, not source scanning.

## Candidate And Zero-Job Pressure

Pass 2 wrote 166 validated candidates:

| Dimension | Count |
| --- | ---: |
| `jobsFound == 0` | 143 |
| `jobsFound > 0` | 23 |
| Static candidates | 133 |
| Provider candidates | 33 |
| Sheet-directory candidates | 139 |
| Gameprog candidates | 22 |
| Seed-careers candidates | 2 |
| Pattern candidates | 2 |
| Gamesmap candidates | 1 |

Zero-job pressure remains concentrated:

| Bucket | Count |
| --- | ---: |
| Zero-job Sheet-directory | 125 |
| Zero-job static | 111 |
| Zero-job Gameprog | 15 |
| Zero-job provider adapters | 32 |

Registry movement in the isolated root after both passes:

| Registry | Count | Notes |
| --- | ---: | --- |
| Active | 2,179 | Sheet-directory active rows rose to 637; static rows rose to 2,070. |
| Pending | 141 | 138 pending rows are Sheet-directory; 125 pending rows have `jobsFound == 0`. |

## Decision

This run supports a targeted Sheet-directory/static quality slice before browser-recovery expansion:

1. Preserve HTTP recovery for Sheet-directory; it continues to have high candidate yield.
2. Investigate why many Sheet-directory rows with `sheet_roles_open_yes` still validate to zero jobs.
3. Treat static fallback quality and static queue pressure as the first P2 behavior target.
4. Keep GameDevMap as a separate evidence lane with bounded batches or a longer uninterrupted run.

Do not widen browser recovery from this evidence alone. Browser-recovery candidate volume stayed small in the directory/web lane.

## Completed follow-up: Playwright probe fallback containment

- The `gameprog_no_current_openings` evidence vocabulary gap was fixed and shipped before this cleanup.
- Source-discovery static probe fallback now contains Playwright launcher/environment exceptions, including Windows permission failures, so failed browser fallback remains a normal probe miss instead of escaping async probe tasks.
- This does not change Sheet-directory/static scoring, queue policy, browser-recovery eligibility, saved jobs, frontend storage, bridge routes, registry contracts, or artifact schemas.

## Completed follow-up: Sheet static fallback quality

- Sheet-directory HTTP recovery remains enabled and recovered provider/static rows remain eligible.
- Unrecovered Sheet static homepage fallbacks are no longer carried forward after recovery has already failed to find a provider or usable jobs page.
- This targets the observed Sheet/static zero-job pressure without changing provider inference, queue policy, browser-recovery eligibility, saved jobs, frontend storage, bridge routes, registry contracts, or artifact schemas.

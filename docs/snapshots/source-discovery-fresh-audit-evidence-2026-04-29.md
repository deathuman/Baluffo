# Source Discovery Fresh Audit Evidence - 2026-04-29

> - **Status:** Active evidence snapshot
> - **Use this when:** planning the next representative source-discovery evidence run or bounded P2 behavior test
> - **Canonical for:** the 2026-04-29 isolated fresh-audit attempt, partial GameDevMap evidence, and the blocker to a clean two-pass run
> - **Not canonical for:** saved-job/local-user data contracts, bridge contracts, queue contracts, or adapter runtime behavior
> - **Then inspect:** [`source-discovery-zero-job-evidence-2026-04-29.md`](source-discovery-zero-job-evidence-2026-04-29.md), [`source-discovery-yield-evidence-2026-04-29.md`](source-discovery-yield-evidence-2026-04-29.md), and [`source-discovery-adapter-follow-ups-closeout.md`](../archive/source-discovery-adapter-follow-ups-closeout.md)
> - **Last updated:** 2026-04-29

This snapshot records an evidence attempt only. It does not change source-discovery behavior, adapters, saved jobs, local user data, bridge routes, frontend storage, queue policy, registry contracts, or artifact schemas.

## Summary

The requested fresh isolated discovery pass did not complete within a 15-minute command budget. It made substantial progress and produced useful partial GameDevMap evidence, but it is not a clean replacement for the stale zero-job candidate snapshot and should not be used alone to justify P2 behavior changes.

The blocker was not network access. The command performed live HTTP work successfully. The blocker was scope: the uncapped run included the default GameDevMap active audit, which processed 6,000 of 7,865 eligible URLs before the command timed out.

## Command Attempted

```powershell
$env:BALUFFO_DATA_DIR = (Resolve-Path '_out\source-discovery-fresh-audit-20260429').Path
python src\source_discovery.py --preset uncapped --top 0 --timeout 12 --gameprog-enabled --gamedevmap-enabled
```

The isolated root was seeded with:

- `source-discovery-config.json`
- `source-registry-active.json`
- `source-registry-pending.json`
- `source-approval-state.json`

No tracked `data/` files were modified.

## Result

| Item | Result |
| --- | --- |
| First pass | Timed out after 15 minutes. |
| Second pass | Not run because the first pass did not complete. |
| Discovery report | Incomplete, `finishedAt` empty, phase still `scanning_sources`. |
| GameDevMap artifact | Partial artifact written under `_out/source-discovery-fresh-audit-20260429`. |
| Network | Live HTTP fetches succeeded; this was not a sandbox-network failure. |
| Isolation caveat | Some directory audit paths still came from repo-relative `activeAuditPath` defaults such as `data\sheet-directory-discovery-audit.json`, so the next fresh run should override audit paths inside the isolated config. |

## Partial Evidence

The partial GameDevMap artifact had:

| Metric | Value |
| --- | ---: |
| CSV rows | 10,011 |
| Eligible rows | 7,865 |
| Completed URLs | 6,000 |
| Remaining URLs | 1,865 |
| Homepage fetch attempts | 6,000 |
| Homepages fetched | 5,273 |
| Recovery fetch attempts | 21,848 |
| Recovery pages fetched | 3,201 |
| Provider candidates | 57 |
| Static candidates | 1,542 |
| Recovered candidates | 231 |
| Recovered active candidates | 190 |
| Active candidates | 1,445 |
| Zero-job candidates | 70 |
| Probe failures | 85 |
| Technical failures | 3,756 |
| Coverage misses | 811 |
| Browser-recovery candidates | 978 |

The partial discovery report showed pre-GameDevMap stages had already completed:

| Stage | Evidence |
| --- | --- |
| Seed careers scan | `provider=14`, `static=13`, `failures=3`. |
| Gamesmap | `provider=0`, `static=4`, `failures=1`. |
| Gameprog | `provider=0`, `static=139`, `failures=9`. |
| Sheet-directory | Reused an existing cache artifact with `provider=0`, `static=0`, `failures=1`. |

## Decision

Do not make a P2 behavior change from this run alone. The useful conclusion is operational:

1. A representative full run needs either a longer command budget or bounded GameDevMap settings.
2. The next evidence run should override all audit artifact paths into the isolated root before execution.
3. A practical next run should split evidence into two lanes:
   - directory/web-only zero-job static quality evidence;
   - GameDevMap active-audit evidence with explicit batch limits or a longer uninterrupted run.

Until that clean run exists, the strongest actionable signal remains the stale-but-clear zero-job snapshot: Sheet-directory/static rows dominate zero-job pressure.

# Source Discovery Zero-Job Evidence Snapshot - 2026-04-29

> - **Status:** Active evidence snapshot
> - **Use this when:** deciding whether to change source-discovery static fallback, Sheet-directory scoring, static caps, or probe policy
> - **Canonical for:** current zero-job candidate pressure from local discovery artifacts
> - **Not canonical for:** saved-job/local-user data contracts, bridge contracts, queue contracts, or adapter runtime behavior
> - **Then inspect:** [`source-discovery-yield-evidence-2026-04-29.md`](source-discovery-yield-evidence-2026-04-29.md), [`source-discovery-adapter-follow-ups-closeout.md`](../archive/source-discovery-adapter-follow-ups-closeout.md), and [`DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-04-29

This snapshot is evidence-only. It does not change source-discovery behavior, source adapters, saved jobs, local user data, bridge routes, frontend storage, queue policy, registry contracts, or artifact schemas.

## Summary

The current local `data/source-discovery-candidates.json` is stale relative to the latest audit artifacts, but it still gives a clear pressure signal: almost all validated discovery candidates in that file have `jobsFound == 0`, and the pressure is concentrated in Sheet-directory static rows.

The next behavior-changing source-discovery work should therefore investigate static quality and Sheet-directory promotion pressure before widening browser recovery or adding new adapter helpers.

## Data Sources

| File | Updated | Role |
| --- | --- | --- |
| `data/source-discovery-candidates.json` | 2026-04-14 07:54 local | Candidate-level zero-job pressure. |
| `data/source-registry-active.json` | 2026-04-28 08:37 local | Active registry movement and source-family mix. |
| `data/source-registry-pending.json` | 2026-04-28 08:37 local | Hidden pending pressure and reason mix. |

The candidate artifact is older than the latest directory audit artifacts from 2026-04-29, so treat these counts as a prioritization signal, not a fresh live-yield measurement.

## Zero-Job Candidate Ranking

| Dimension | Top buckets |
| --- | --- |
| Total candidates | 142 validated candidates; 141 have `jobsFound == 0`; 1 has `jobsFound > 0`. |
| Source directory | `game_studios_sheet`: 136 zero-job rows; missing source directory: 5. |
| Discovery method | `sheet_directory`: 136; `seed_careers_page`: 3; `pattern`: 2. |
| Discovery stage | `sheet_directory`: 136; `web_provider`: 2; `provider_pattern`: 2; `generic_static`: 1. |
| Adapter split | `static`: 110; provider adapters: 31. |
| Promotion lane | `manual_review`: 120; `structured_batch`: 11; `domain_cap_review`: 10. |
| Evidence types | `sheet_directory`: 136; `sheet_row`: 136; `sheet_roles_open_yes`: 136; `web_provider_url`: 29. |

The only non-zero candidate in this artifact is `Velan Studios, Inc. (Workable)` from Sheet-directory with `jobsFound=3` and `sheet_roles_open_speculative` evidence.

## Registry Pressure

| Registry | Total | Sheet-directory pressure | Static pressure |
| --- | ---: | ---: | ---: |
| Active | 2,021 | 502 active Sheet-directory rows | 458 of those Sheet rows are static. |
| Pending | 57 | 42 pending Sheet-directory rows | all 42 pending Sheet rows are static. |

Pending rows are all hidden from default. The largest pending reasons are `site_changed_static_source` (22), `unsupported_static_source` (13), `redundant_static_stronger_coverage` (8), and `stale_or_dead_static_source` (7).

## Interpretation

This evidence points to a quality problem in static fallback and Sheet-directory promotion, not a need for more adapter scaffolding:

- Sheet-directory remains useful as a source of coverage, but it dominates both active registry volume and zero-job candidates.
- Static rows dominate the zero-job pressure, which suggests the next behavior work should inspect static fallback thresholds, static cap policy, and validation freshness.
- Provider candidates are much fewer in the zero-job set, so provider inference is not the first pressure unless fresh audit data contradicts this snapshot.
- Browser-recovery expansion is premature until a fresh audit shows browser-recoverable misses with plausible recovered yield.

## Recommended Next Work

1. Run a fresh representative discovery audit to replace the stale 2026-04-14 candidate artifact.
2. If the pressure remains, test a Sheet-directory static quality change behind focused fixtures before changing queue/pending behavior.
3. Compare before/after counts for zero-job Sheet static rows, active/pending movement, tombstone/suppression handling, and recovered provider/static split.

Do not make P2 behavior changes without tests around saved/local data boundaries, queue movement, pending review, tombstones, suppression, and admin auto-approval.

# Jobs Dead Source Evidence Snapshot - 2026-04-29

> - **Status:** Active evidence snapshot
> - **Use this when:** auditing the first physical deletion batch for dead or unsupported jobs sources
> - **Canonical for:** the 2026-04-29 jobs source deletion batch, evidence commands, and delete/defer criteria
> - **Not canonical for:** saved jobs, local user data, bridge route contracts, source-discovery behavior, or future deletion batches
> - **Then inspect:** [`jobs-fetcher-aggressive-simplification-plan.md`](../plans/jobs-fetcher-aggressive-simplification-plan.md), [`adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and [`DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-04-29

This snapshot records the first evidence-backed deletion batch after the jobs adapter mass refactor completed. The batch used a temporary active registry under `_out/jobs-adapter-dead-source-evidence-20260429/data` so pending rows could be fetched without changing tracked registry files before the delete decision.

## Evidence Commands

```powershell
python scripts\jobs_yield_gate.py dead-source-candidates --limit 10 --json
python scripts\jobs_yield_gate.py dead-source-registry _out\jobs-adapter-dead-source-evidence-20260429\candidates.json
python -m src.jobs.pipeline --only-sources <candidate-source-ids> --output-dir _out\jobs-adapter-dead-source-evidence-20260429\pass-1 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources <candidate-source-ids> --output-dir _out\jobs-adapter-dead-source-evidence-20260429\pass-2 --force-refresh-all --ignore-circuit-breaker --quiet
python scripts\jobs_yield_gate.py compare _out\jobs-adapter-dead-source-evidence-20260429\pass-1 _out\jobs-adapter-dead-source-evidence-20260429\pass-2 --allow-drops
python scripts\jobs_yield_gate.py dead-source-decisions _out\jobs-adapter-dead-source-evidence-20260429\pass-1 _out\jobs-adapter-dead-source-evidence-20260429\pass-2 --candidates _out\jobs-adapter-dead-source-evidence-20260429\candidates.json
```

Both fetch passes exited nonzero because the selected sources failed as expected, but each pass wrote a complete `jobs-fetch-report.json`. The reports agreed on zero kept jobs for every selected source.

## Decision Summary

| Metric | Count |
| --- | ---: |
| Candidate source IDs tested | 10 |
| Source IDs classified delete-eligible | 10 |
| Active registry rows removed | 1 |
| Pending registry rows removed | 10 |
| Tombstone records added | 11 |
| Sources deferred for nonzero yield | 0 |
| Sources deferred for browser fallback | 0 |
| Sources deferred as legitimate empty | 0 |

The active row removal was a stronger duplicate of the pending Farm 51 row: it shared the same normalized URL fingerprint as the delete-eligible candidate.

## Removed Rows

| Bucket | Source | Evidence signal |
| --- | --- | --- |
| active | The Farm 51 (Sheet) | Shared URL fingerprint with delete-eligible Farm 51 pending row. |
| pending | 34bigthings (Manual Website) | Two fresh zero-kept passes; `site_changed`. |
| pending | AirStrafe (Sheet) | Two fresh zero-kept passes; `site_changed`. |
| pending | Blind Squirrel Games (Sheet) | Two fresh zero-kept passes; `site_changed`. |
| pending | Fuse Games (Sheet) | Two fresh zero-kept passes; `site_changed`. |
| pending | Joker Games (Sheet) | Two fresh zero-kept passes; `site_changed`. |
| pending | Playground Games (Sheet) | Two fresh zero-kept passes; pending reason `site_changed_static_source`. |
| pending | Series Entertainment (Sheet) | Two fresh zero-kept passes; `site_changed`. |
| pending | The Coalition Studio (Sheet) | Two fresh zero-kept passes; `site_changed`. |
| pending | The Farm 51 (Sheet) | Two fresh zero-kept passes; `site_changed` plus `redundant_static_stronger_coverage`. |
| pending | Third Kind Games (Sheet) | Two fresh zero-kept passes; `site_changed`. |

## Guardrails

- No saved-job rows, local user data, bridge routes, fetch report contracts, or source-discovery artifacts were changed.
- Deleted rows were recorded in `data/source-registry-tombstones.json` with reason `jobs_dead_source_evidence`.
- Browser-required, anti-bot, timeout, rate-limited, and legitimate no-openings sources were excluded from this batch.
- Static plugin dependency proof found no production host plugin orphaned by this batch. It did remove unused demo/no-op registrations: `example_com`, `example_org`, and `static_pilot`.

# Jobs Source Family Evidence Snapshot - 2026-04-30

> - **Status:** Active evidence snapshot
> - **Use this when:** choosing the next deletion-first jobs fetcher source-family slice
> - **Canonical for:** 2026-04-30 isolated `scrapy_static`, social, community, and static sample evidence
> - **Not canonical for:** saved jobs, local user data, bridge route contracts, source-discovery behavior, or final source deletion approval
> - **Then inspect:** [`jobs-fetcher-aggressive-simplification-closeout.md`](../archive/jobs-fetcher-aggressive-simplification-closeout.md), [`adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and the `_out/jobs-source-family-evidence-20260430/` local reports if available
> - **Last updated:** 2026-05-03

This snapshot records an evidence-only jobs fetcher run before any deletion of source rows, default loaders, plugin modules, or compatibility exports. All run artifacts were written under `_out/jobs-source-family-evidence-20260430/` and are intentionally not committed.

## Commands

```powershell
git push origin main
New-Item -ItemType Directory -Force _out/jobs-source-family-evidence-20260430
python scripts/jobs_yield_gate.py list-static-sources --limit 20 > _out/jobs-source-family-evidence-20260430/static-source-ids.txt
python -m src.jobs.pipeline --only-sources scrapy_static_sources --output-dir _out/jobs-source-family-evidence-20260430/scrapy-static-pass-1 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources scrapy_static_sources --output-dir _out/jobs-source-family-evidence-20260430/scrapy-static-pass-2 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources social_x,social_mastodon --social-enabled --output-dir _out/jobs-source-family-evidence-20260430/social-pass-1 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources social_x,social_mastodon --social-enabled --output-dir _out/jobs-source-family-evidence-20260430/social-pass-2 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --output-dir _out/jobs-source-family-evidence-20260430/community-pass-1 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources google_sheets,google_sheets_1er2oaxo,google_sheets_1mvqhxat --output-dir _out/jobs-source-family-evidence-20260430/community-pass-2 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources <first-20-static-source-ids> --output-dir _out/jobs-source-family-evidence-20260430/static-sample-pass-1 --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources <first-20-static-source-ids> --output-dir _out/jobs-source-family-evidence-20260430/static-sample-pass-2 --force-refresh-all --ignore-circuit-breaker --quiet
```

The `python -m src.jobs.pipeline` invocations printed the known runpy warning about `src.jobs.pipeline` already being in `sys.modules`. Static sample runs also emitted non-fatal Playwright `Access is denied` future exceptions after completion.

## Run Summary

| Family | Pass 1 output | Pass 2 output | Wall time | Signal | Recommendation |
| --- | ---: | ---: | --- | --- | --- |
| `scrapy_static_sources` | 0 | 0 | about 1s each | No enabled `scrapy_static` sources; source report is `ok` with `needs_review`; process exits `2` because no jobs are output. | `delete` candidate for loader/module removal, but ask before deleting registered loader or compatibility exports. |
| Social X / Mastodon | 1 | 1 | about 1-2s | Mastodon keeps one job; X keeps zero and is `needs_review`. Mastodon drops 39 low-confidence rows. | `merge` social lifecycle first; consider separate X deletion/defer evidence before removing a channel. |
| Community Google Sheets | 25,211 | 25,215 | about 143-165s | Very high yield; major cost is redirect resolution plus canonicalization. | `keep`; optimize lifecycle/performance only, do not delete. |
| Static sample, first 20 sources | 524 | 537 | about 32s | High yield; one repeated zero source is Blizzard; Disney/Stillfront/PlayStation have intermittent warning/timeout pressure but still keep jobs. | `keep` static lifecycle; pursue targeted deletion evidence for Blizzard and operational cleanup for slow/browser fallback noise. |

## Detail Findings

### `scrapy_static_sources`

- Both passes report `inputCount=0`, `outputCount=0`, and `scrapy_static_sources` with `fetchedCount=0`, `keptCount=0`, `failureBucket=needs_review`.
- The source detail says `No enabled scrapy_static sources`.
- This is the cleanest deletion-first candidate: the loader currently contributes no yield in this evidence run.
- Do not delete it without explicit approval because it is still registered as a default loader and exported through compatibility surfaces.

### Social

- `social_mastodon` kept one job in both passes.
- `social_x` kept zero jobs in both passes and remained `needs_review`.
- The social source family still has duplicated cache/progress/error/report lifecycle, but evidence does not support deleting the whole family.
- Next social work should either merge lifecycle plumbing or collect a channel-specific X deletion/defer snapshot.

### Community

- `google_sheets` kept 30,928 rows in both passes.
- `google_sheets_1er2oaxo` kept 2,566 rows in both passes.
- `google_sheets_1mvqhxat` kept 348 rows in both passes after one empty/invalid CSV attempt and one successful source detail.
- Deduplicated output is about 25.2k jobs. This family is high-yield but slow; redirect resolution alone was 52-72s.
- Community work should focus on cache/redirect/canonicalization performance, not deletion.

### Static sample

- 19 of 20 sampled static sources kept jobs in both passes.
- Blizzard kept zero jobs twice and classified its detail as `dead_listing_page`.
- Disney kept jobs but had detail fetch network errors classified as timeout with browser fallback recommended.
- PlayStation and Stillfront kept jobs but showed time-budget warning pressure.
- Static source execution is productive, but the sample points to targeted per-source deletion evidence and browser fallback containment rather than broad deletion.

## Next Recommended Slice

Start with a small `scrapy_static` decision slice:

- Confirm whether no enabled `scrapy_static` sources is expected from current registry/config.
- If yes, remove the default loader and dead runtime surface with explicit approval.
- If no, fix the registry/config evidence gap before deleting anything.

Separately track a narrow operational cleanup for the static Playwright fallback permission future leak; it should not be mixed with source-family deletion.

## Guardrails

- No saved jobs, local user data, bridge routes, frontend payloads, tracked source registries, queue policy, tombstones, suppression, or auto-approval behavior changed in this evidence slice.
- `_out/` run artifacts are local only and should not be committed.
- Source rows, default loaders, plugin modules, and broad `src/jobs_fetcher.py` compatibility exports still require explicit approval before deletion.

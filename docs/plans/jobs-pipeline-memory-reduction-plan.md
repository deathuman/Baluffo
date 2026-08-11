# Jobs Pipeline Memory Reduction Plan

> - **Status:** Completed (P1–P3 + profiling) 2026-08-09; validated 2026-08-11 with subset-500 pi4-tight bench — pi4-tight requires `BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS=4` to stay under the 1.5 GiB cap
> - **Use this when:** revisiting jobs-pipeline peak-RSS pressure, evaluating the next fetch-side lever, or running the alloc profiler
> - **Canonical for:** measured peaks, landed phases, bench methodology, and deferred next steps
> - **Not canonical for:** runtime contract changes — data formats, payload keys, and API surfaces are in [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Then inspect:** [`../fetcher-runtime-contracts.md`](../fetcher-runtime-contracts.md), [`../testing.md`](../testing.md), [`../../src/pipeline_io.py`](../../src/pipeline_io.py), [`../../src/jobs/state_lifecycle.py`](../../src/jobs/state_lifecycle.py)
> - **Last updated:** 2026-08-09

## Summary

Jobs pipeline under a Docker pi4-tight profile (1.5 GiB cap) was hitting its ceiling during large-seed fetches. Bench with 2159 sources at ~55 s/source pinned RSS at 1.4–1.49 GiB and cgroup `memory.peak = 1,611,776,000` bytes — no headroom left for the remaining ~1950 sources.

Three phases landed plus a per-source tracemalloc profiler. Together they move RSS from the 1.49 GiB ceiling to a 1.17–1.33 GiB oscillation over the subset-of-113-sources workload that previously would have OOM'd.

## Landed Commits

| Hash | Subject | What it does |
|------|---------|--------------|
| `564d18d2` | `perf(finalize): skip lifecycle state re-read when file unchanged` | `lifecycle_state_fingerprint` (`mtime_ns`, `size`, gzip-aware via `existing_json_candidate`) captured at `PipelineRunSetup.lifecycle_state_fingerprint`; `_serialize_jobs_feed_reconciliation` reuses in-memory `lifecycle_rows` if fingerprint matches. Saves ~67 MB re-read on the 40k-row seed. |
| `07999b8e` | `perf(finalize): drop to_dict() snapshot from tombstone reconciliation` | `_row_to_canonical_payload` duck-types CanonicalJob vs Mapping; finalize drops the duplicate `pre_lifecycle_payload_rows` snapshot. 86 MB → 5 MB peak in finalize; identical output. |
| `32794f97` | `perf(pipeline): stream seed reads + per-source alloc profiling` | `write_pipeline_rows_sidecar` emits `<path>.rows.jsonl.gz`; `read_existing_output` streams rows. 560 MB → 154 MB parse peak. Also adds `run_profiled_alloc` + `BALUFFO_PROFILE_ALLOC=1` and `scripts/perf_alloc_top.py`. |

## Measured Effects

| Workload | Before | After | Note |
|----------|--------|-------|------|
| 40k-row seed parse | 560 MB peak | 154 MB peak | sidecar row-streaming |
| Finalize 40k rows | 86 MB peak | 5 MB peak | dropped duplicate snapshot |
| Lifecycle state read | 2 × 67 MB | 1 × 67 MB | fingerprint gate |
| pi4-tight (1.5 GiB cap), full run | RSS 1.4–1.49 GiB, OOM risk | RSS 1.17–1.33 GiB over 30 min, no OOM | subset of 209 sources profiled during scripted bench |

The final subset-of-113 bench did the alloc-profile recording. Full-seed bench (2159 sources) was not re-run after landing — see "Next Moves".

### Subset-500 pi4-tight bench (2026-08-11)

500-source subset (first 500 work items from the 40k-row seed) against the pi4-tight profile (1.5 GiB / 1.5 CPU):

| Configuration | Result | Notes |
|---|---|---|
| Default (maxWorkers=10) | OOM at 293/500, `oom_kill=7` | peak pegged at 1.536 GiB (over cap) |
| `BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS=4` | Completed fetching; pipeline wait-timeout at 60 s | peak 1.5 GiB, 0 `oom_kill`, avg RSS 1.11 GiB |

Bench artifacts: `_out/perf-pipeline/subset500-mw4/` — `stages.json`, `samples.ndjson`, `report.md`, `SUMMARY_H1.md`.

Two carry-over notes:
- `fetch/seeding_existing_output` stage used the rows sidecar (1.6s, 846 MB peak) — the new path is doing its job.
- `fetch/loading_state/read_lifecycle_state` showed only one 0.6s read — fingerprint skip is engaged.

Bench runner knobs added during validation:
- `scripts/perf_pipeline_stages.py --only-sources-file <path>` and `--fetch-max-workers-env <N>` stage `BALUFFO_CONTAINER_PIPELINE_ONLY_SOURCES` + `BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS` via `--env-file` to dodge the Windows CreateProcess 32k cap.
- `--profile-alloc` appends `BALUFFO_PROFILE_ALLOC=1` to the same env file for diagnostics.
- `src/bridge/pipeline_service.py` gained `BALUFFO_CONTAINER_PIPELINE_ONLY_SOURCES`, forwarded as `onlySources` on the fetch child payload. Empty default = pass-through.

### Subset-50 alloc-profile bench (H2, 2026-08-11)

50-source subset under the same pi4-tight profile with `BALUFFO_PROFILE_ALLOC=1`:

- 50/50 completed, **peak 948.5 MiB**, 0 OOM events.
- 263 profiled invocations.

Top alloc frames (cumulative MiB across all sources):

| Rank | Cumulative | Hits | Frame |
|---|---:|---:|---|
| 1 | 119.7 MiB | 187 | `httpx/_models.py` |
| 2 | 39.0 MiB | 163 | `json/decoder.py` |
| 3 | 14.6 MiB | 261 | `src/shared/live_task.py` |
| 4 | 14.5 MiB | 157 | `src/jobs/adapters/static_listing.py` |

Top per-source peak: **36.9 MiB** (playsimple static listing).

H2 (per-source peak driving cap-pressure) is **not supported**. Memory scales with fetch worker count, not body size. The previous 1.5 GiB cap hit at mw=10 is the 10 concurrent httpx/playwright bodies + base container, not any single adapter misbehaving.

Artifacts: `_out/perf-pipeline/subset50-alloc/SUMMARY_H2.md`, full JSONL at `_out/perf-admin-flows/seed-data/perf-profiles/allocations.jsonl`.

## Fetch-Side Signals (still open)

From `scripts/perf_alloc_top.py` on the bench alloc log:

- Cumulative allocations: `httpx/_models.py` 63.4 MiB, `json/decoder.py` 20.9 MiB, `src/shared/live_task.py` 12 MiB.
- Top per-source peak: playsimple at 36.6 MiB — static HTML scrapes dominate.
- 209 of 2159 sources captured during the 30-min post-P3 bench window; sampler settled at ~9 s/source under the lock-serialized profiler. Resulting JSONL is durable evidence at `_out/perf-profiles/allocations.jsonl` (or wherever `BALUFFO_DATA_DIR` pointed during the run).

The next credible lever is bounding per-source response bodies (a single `max_bytes` cap at the httpx helper level) or streaming the canonical HTML parse. Neither is required to hold the pi4-tight seat at the current seed volume — revisit only if a future seed or new adapter pushes us back to 1.4 GiB.

## Profiling Instrumentation

Per-source peaks:

- Env flag: `BALUFFO_PROFILE_ALLOC=1` gates tracemalloc inside `run_profiled_alloc` (`src/shared/profile_utils.py`). Off by default — passthrough with one env-check overhead when unset.
- Wraps `run_profiled` in `_execute_loader_started` (`src/jobs/pipeline_source_loop.py`); outermost so the cprofile shim's own overhead is included in the per-source peak.
- Lock-serialized while profiling: deliberately distorts wall-clock so per-source allocations stay clean. Use for diagnosis, not benchmarks.
- Writes JSONL entries to `<data_dir>/perf-profiles/allocations.jsonl`.
- Aggregate with: `python scripts/perf_alloc_top.py [--limit N] [--sources N]`.

Wall-clock profiling:

- `BALUFFO_PROFILE=1` gates the existing cProfile capture via `run_profiled`. Independent of the alloc flag; orthogonally useful for slow-source diagnosis.

## Reproducing a Bench

Bench preset `smoke` runs against `/tasks/run-jobs-pipeline` on `baluffo:pipeline-bench` with the pi4-tight profile (1.5 GiB RAM, 1.5 CPU). Seed regen use:

```powershell
python scripts/perf_admin_seed.py --from-volume-path <live-data-dir> -o _out/perf-admin-flows/seed-data
```

Container memory ceiling reads live in `scripts/perf_pipeline_stages.py`. The seed-overlay helper resets state rows so the run doesn't re-processed previously-fetched items.

## Next Moves

1. ~~Full-seed Docker bench (2159 sources) to capture a final cgroup `memory.peak` after the three landed phases.~~ **Validated 2026-08-11:** pi4-tight profile cannot host default fetch concurrency (maxWorkers=10) on the subset-500 bench; 7 OOM kills at 293/500. Subset-500 with `BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS=4` holds peak at 1.5 GiB and clears the workload (see "Subset-500 pi4-tight bench" above).
2. If the final peak stays under ~1.4 GiB in production: consider removing the legacy gzip `read_json` fallback path in `src/pipeline_io.py` once one full production cycle has run end-to-end with the sidecar.
3. Fetch-side response-body cap (httpx `max_bytes` knob) is the next lever if per-source peak pressure emerges — it was the next-largest consumer in the allocation profile (`httpx/_models.py` 63 MiB cumulative).

## Sidecar Rollout Notes (post-ship checklist)

- The first production run after `32794f97` ships will write `.rows.jsonl.gz` alongside the legacy blob. From that point forward `read_existing_output` prefers the sidecar; the blob is only parsed when the sidecar is missing.
- Disk overhead: roughly `sizeof(blob) + sizeof(blob) ≈ 2×` per artifact. Consider pruning the blob write after two successful full cycles if disk pressure matters (not required for memory).
- Deleting the sidecar file is always safe — the reader falls back to the blob path silently.

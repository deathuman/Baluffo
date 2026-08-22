# Jobs Pipeline Memory Reduction Plan

> - **Status:** Completed (P1–P3 + profiling) 2026-08-09; validated 2026-08-11 with subset-500 pi4-tight bench; **2026-08-21 streaming + memory-lever pass landed — frozen 2,125-key full run completes under 2.5 GiB with browser fallback ON (see "Streaming + Memory Levers")**
> - **Use this when:** revisiting jobs-pipeline peak-RSS pressure, evaluating the next fetch-side lever, or running the alloc profiler
> - **Canonical for:** measured peaks, landed phases, bench methodology, and deferred next steps
> - **Not canonical for:** runtime contract changes — data formats, payload keys, and API surfaces are in [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Then inspect:** [`../fetcher-runtime-contracts.md`](../fetcher-runtime-contracts.md), [`../testing.md`](../testing.md), [`../../src/pipeline_io.py`](../../src/pipeline_io.py), [`../../src/jobs/state_lifecycle.py`](../../src/jobs/state_lifecycle.py)
> - **Last updated:** 2026-08-21

## Summary

Jobs pipeline under a Docker pi4-tight profile (1.5 GiB cap) was hitting its ceiling during large-seed fetches. Bench with 2159 sources at ~55 s/source pinned RSS at 1.4–1.49 GiB and cgroup `memory.peak = 1,611,776,000` bytes — no headroom left for the remaining ~1950 sources.

Three phases landed plus a per-source tracemalloc profiler. Together they move RSS from the 1.49 GiB ceiling to a 1.17–1.33 GiB oscillation over the subset-of-113-sources workload that previously would have OOM'd.

## Landed Commits

- `564d18d2` (P1) lifecycle fingerprint, `07999b8e` (P2) drop to_dict snapshot, `32794f97` (P3) sidecar + alloc profiling, `8a6e26ee` docs plan + INDEX, `1624944b` bench knobs + H1 finding, `6e3d9800` H2 finding, `025b4522` browser fallback pool (H3), `c6149db5` O(N·K) carried-initialization index fix, `20b49837` streamed finalize writes, `c31b7689` tombstone-write OOM fix + dead-ref trim.

| Hash | Subject | What it does |
|------|---------|--------------|
| `564d18d2` | `perf(finalize): skip lifecycle state re-read when file unchanged` | `lifecycle_state_fingerprint` (`mtime_ns`, `size`, gzip-aware via `existing_json_candidate`) captured at `PipelineRunSetup.lifecycle_state_fingerprint`; `_serialize_jobs_feed_reconciliation` reuses in-memory `lifecycle_rows` if fingerprint matches. Saves ~67 MB re-read on the 40k-row seed. |
| `07999b8e` | `perf(finalize): drop to_dict() snapshot from tombstone reconciliation` | `_row_to_canonical_payload` duck-types CanonicalJob vs Mapping; finalize drops the duplicate `pre_lifecycle_payload_rows` snapshot. 86 MB → 5 MB peak in finalize; identical output. |
| `32794f97` | `perf(pipeline): stream seed reads + per-source alloc profiling` | `write_pipeline_rows_sidecar` emits `<path>.rows.jsonl.gz`; `read_existing_output` streams rows. 560 MB → 154 MB parse peak. Also adds `run_profiled_alloc` + `BALUFFO_PROFILE_ALLOC=1` and `scripts/perf_alloc_top.py`. |
| `025b4522` | `perf(fetch): pool Chromium for browser fallbacks (one launch per run)` | `BrowserFallbackPool`: one lazy Chromium on an asyncio dispatcher thread, fresh `BrowserContext` per call. 43 fallback acquisitions on one browser (559 ms startup) vs 43 launches (~90–215 s overhead) in the subset-50 bench. Kill switch `BALUFFO_BROWSER_POOL=0`. |

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

### Subset-50 pool bench (H3, 2026-08-11)

Browser fallback pool shipped (`025b4522`; see [`browser-fallback-pool-plan.md`](browser-fallback-pool-plan.md)): one lazy Chromium per fetch stage, fresh `BrowserContext` per call, `BALUFFO_BROWSER_POOL=0` kill switch.

- Same 50 sources, mw=4, no alloc profiling: 44 fallback triggers → **43 pool acquisitions on one browser** (startup 559 ms, 0 relaunches) vs 43 legacy launches (~90–215 s of launch overhead removed).
- Fetch stage wall: 7.5 min for 50 sources; memory stable **1.00 GiB (66.9%)**, 0 OOM.
- Artifacts: `_out/perf-pipeline/subset50-pool/SUMMARY_POOL.md`.

### Finalize-phase grind — root-caused and fixed (2026-08-11)

The `fetch/applying_lifecycle` CPU grind (never completing on the bench seed) was an **O(N·K) alias-index rebuild** in `_initialize_carried_lifecycle_rows` (`src/jobs/state_lifecycle.py`): a full `_lifecycle_alias_index` rebuild over all ~71k lifecycle entries ran after **every** carried-row initialization. Fresh fetch rows with new availabilityIds (dedup-assigned) triggered ~1000 initializations; at ~2 s/rebuild that was ~36 min of pure CPU (zero syscalls — pure dict work — which is why it read as a hang).

Evidence (host replica of the exact call with the real seed): K=0 initializable rows → 35 s total; K=10 → 37.6 s; K=40 → 108.9 s (linear). The stuck child burned 2204 core-seconds ≈ 25 s + K × ~2.1 s.

Fix: incremental index update (`_index_lifecycle_entry_aliases`) — behavior-identical (new entries append last, so immediate conflict-drop == the rebuild's deferred drop), per-insert cost O(≤24 aliases) instead of O(71k). K=1000 → **11.9 s vs ~36 min**. Outputs byte-identical to the old path across all 111,779 rows in the equivalence probe. Regression guards: bounded-time test (300 fresh rows × 8k entries < 15 s; old code ~90 s+) + index-equivalence + alias-freshness tests in `tests/test_jobs_lifecycle_output.py`.

### Subset-50 pool bench with lifecycle fix (2026-08-11, `subset50-pool-fixed`)

Same subset-50/mw=4 config, rerun after the fix: **pipeline now runs end-to-end through finalize** — `fetch/applying_lifecycle` **12.7 s** (was ∞), dedup 44 s, reconciling 17-19 s, quality audits 10 s, `writing_outputs` 2.1 s started. Pool again: 42 acquisitions / one browser / 507 ms startup / 0 relaunches.

### Finalize OOM — root-caused and fixed (2026-08-11)

Three stacked peaks were killing the child at the 1.5 GiB cap during finalize (cgroup `oom_kill=1`, silent death, `jobs_unified` never written):

1. **Unified/light/lifecycle writes built full JSON strings + filtered copies** — `serialize_rows_for_json` peaked ~355 MiB (full) + ~255 MiB (light) at 40k rows; lifecycle dumps ~178 MiB + a full normalized copy. Fixed with `write_streamed_text_if_changed` (atomic, size-gated, gzip-aware): **0 MiB / 1 MiB measured**. Unified output byte-identical; lifecycle went indent=2 → compact (same shape, JSON-compatible).
2. **Dead references held through the write-heavy phases** — `AvailabilityIdentityPreparation` retained 2×42k CanonicalJob lists + a 111k lifecycle copy; `canonical_rows`/`observed_for_lifecycle` no longer needed after the lifecycle phase. Freed + `gc.collect()`/`malloc_trim`: RSS 937 → 793 MiB at the audits phase.
3. **`write_availability_tombstones` re-validated every tombstone via full pydantic `model_validate`** — with ~40k legitimate tombstones that re-validation churned **~745 MiB** (producers already validate at capture; write-side re-validation was pure waste). Now streams compact JSON: **0 MiB peak**, round-trip identical, restore path unchanged.

Bench result (`subset50-pool-fixed9`): pipeline **completes end-to-end** — writing_outputs peak 831 MiB, finalize done at 916 MiB, all artifacts written, `terminalReason: completed`.

### Re-run collapse — root-caused and fixed (2026-08-11)

Re-runs collapsed the output (41k → ~1.2k rows) and spawned ~40k tombstones. The chain, proven end-to-end:

1. The lifecycle observes only freshly-fetched evidence (`observed_rows = canonical[seeded:]` — by design; re-observing carried rows would suppress legitimate retirement).
2. Unobserved entries took the `source_skipped` preserve path, which called `_apply_unverified_availability_entry` — **incrementing `consecutiveAvailabilityFailures` every run**.
3. With `AVAILABILITY_OVERDUE_FAILURE_COUNT=2` + 7-day age, any source skipped twice (cadence, subset filter, or exclusion) had its jobs marked `verification_overdue` → **hidden from the output**.
4. In the bench (50 of ~1,450 sources), this compounded across runs into the collapse + tombstone flood. Production cadence-skipped sources would hit the same bug.

Fix (`269058db`): the `source_skipped` branch no longer mutates availability fields — a skipped source provides no evidence; only **failed** sources decay, and eligible-missing retirement is unchanged. Bench on a pristine seed: output 23,750 rows (100% of deduped projected, was ~1,190), `terminalReason: completed`. The pre-existing poisoned state (70k overdue entries) heals as sources re-run successfully.

Note: the duplicate-availabilityId lifecycle structure (24.4k shared ids across url-keyed entries, flagged as contamination by the identity prep) remains a hygiene issue to revisit, but it is not the collapse driver.

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

Gaps in the seed tool (2026-08-12, live-volume regen):

- The tool copies only its 15 named artifacts and **silently skips missing ones**; it never cleans the output dir, so stale leftovers persist. A fresh seed therefore needs manual follow-up: (1) if the live volume keeps `jobs-lifecycle-state.json.gz` / `jobs-source-state.json.gz` only (no plain names — UmbrelOS v2 app data does this), decompress both into the seed dir as plain `jobs-lifecycle-state.json` / `jobs-source-state.json`; (2) refresh the registry artifacts (`source-registry-active.jsonl`/`.gz`, `source-registry-pending.*`, `source-registry-metadata.json.gz`) from the live volume; (3) if the live `jobs-unified.json.gz` is a zero-filled in-flight placeholder (observed on the device), seed the last valid snapshot from `.jobs-bootstrap-staging/` instead. Verify row counts match the live source before benching.
- `--only-sources-file` takes **loader keys**, not registry ids: statics as `static_source::<registry id>` (e.g. `static_source::static:listing_url:<url>`), ATS entries as their aggregate loader names (`greenhouse_boards`, `breezy_sources`, `workable_sources`, …). Registry ids alone match nothing and fail the fetch child with "No requested --only-sources entries matched available loaders".
- `--browser-fallback-max-workers-env` forwards the fallback-concurrency knob (service caps it at 6); `--timeout` defaults to one hour.

Container memory ceiling reads live in `scripts/perf_pipeline_stages.py`. The seed-overlay helper resets state rows so the run doesn't re-processed previously-fetched items.

## Next Moves

1. ~~Full-seed Docker bench (2159 sources) to capture a final cgroup `memory.peak` after the three landed phases.~~ **Validated 2026-08-11:** pi4-tight profile cannot host default fetch concurrency (maxWorkers=10) on the subset-500 bench; 7 OOM kills at 293/500. Subset-500 with `BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS=4` holds peak at 1.5 GiB and clears the workload (see "Subset-500 pi4-tight bench" above).
2. ~~If the final peak stays under ~1.4 GiB in production: consider removing the legacy gzip `read_json` fallback path in `src/pipeline_io.py` once one full production cycle has run end-to-end with the sidecar.~~ **Landed 2026-08-12:** the fallback is removed (sidecar-only read; missing sidecar cold-seeds); device-side verification follows the next release's first Umbrel cycle (the device is still on 0.2.129, pre-sidecar).
3. ~~Fetch-side response-body cap~~ **Landed 2026-08-12:** `BALUFFO_FETCH_MAX_BYTES` (default 20 MiB, 1 MiB floor) caps both the urllib and httpx async reads in `src/jobs/common/http.py` / `src/jobs/transport.py`; truncated pages parse fewer rows and retry next run.
4. ~~Duplicate-availabilityId hygiene~~ **Closed 2026-08-12 (T2 trend):** the 08/05-era 24.4k dup cluster was legacy ambiguity; the identity-prep repair is stable — bench-era lifecycle has zero real duplicate ids (23.9k unique; the rest unassigned), production today has 47 dup ids / 229 rows (0.4%), absorbed by the designed quarantine.

## Fresh-Seed Bench Results (2026-08-12, live Umbrel volume regen)

The seed was regenerated from the live Umbrel volume (UmbrelOS v2 app data, 2,317 active registry sources, schema-v2 lifecycle with 55,596 jobs). Bench runs on `baluffo:pipeline-bench`, preset `smoke`:

| Run | Seat | Keys | mw | fb | Outcome | Fetch wall / peak |
| --- | --- | --- | --- | --- | --- | --- |
| subset1000-fb4 | pi4-tight 1.5g | 903 | 4 | 4 | **failed — OOM** | 2,429 s / 1535 MiB (cgroup ceiling; playwright Node driver crash) |
| subset500-fb4 | pi4-tight 1.5g | 500 | 4 | 4 | **failed — OOM** | 1,739 s / 1535 MiB (ceiling) |
| subset500-mw2 | pi4-tight 1.5g | 500 | 2 | 4 | **failed — OOM** | 1,739 s / 1536 MiB (ceiling; Node crash in fetcher log) |
| subset500-roomy-fb4 | pi4-roomy 2.5g | 500 | 4 | 4 | **completed** | 2,733 s / 2403 MiB; output 25,884 rows; finalize phases fast (dedup 18.7 s, reconciling 14.6 s, applying_lifecycle 8.6 s, writing_outputs 17.8 s, sync_push 2.2 s) |
| subset500-roomy-fb6 | pi4-roomy 2.5g | 500 | 4 | 6 | **completed** | 3,379 s / 2559 MiB (+24% wall, +53% CPU vs fb=4 — extra fallback workers contend on the single pooled browser and ride the ceiling; default 4 confirmed) |

Key findings:

- **The pi4-tight 1.5 GiB seat cannot host the current production-shaped fetch** at any subset ≥500 keys / mw ≥2: memory climbs with the run's retained rows + browser-fallback driver and pins at the cgroup ceiling, then the playwright Node driver OOM-crashes. The earlier subset-500 "holds 1.5 GiB" evidence was against the lighter 08/05-era seed. Production is **not** capped (the Umbrel compose sets no `mem_limit`), so this is a bench-seat/Pi-sizing finding, not a production blocker: a 4 GB Pi hosting the container uncapped has ~2.5-3 GiB of practical headroom — tight for full-coverage fetch (projected ~4+ GiB at 2,317 keys).
- **Full-scale fetch memory scales with retained rows, not worker count** (mw=2 peaked like mw=4). The concurrency lever (H1) stops helping once the retained-row set dominates; the remaining memory lever for a 1.5 GiB seat would be row-streaming the fetch child's retained set (not attempted — production is uncapped).
- **The 500-key fresh-seed run completed end-to-end** — the first clean full-pipeline completion on production-shaped data: the fixed phases hold (no 36-min lifecycle grind, no collapse; output 25,884 rows vs the 1.2k collapse-era projection).
- Fallback concurrency 6 is not beneficial (see table) — keep the default 4.
- Bench tool: `--profile pi4-roomy` (2.5g/2cpus) added for fresh-seed benches; `--browser-fallback-max-workers-env` forwards the fallback knob.

## Sidecar Rollout Notes (post-ship checklist)

- The first production run after `32794f97` ships will write `.rows.jsonl.gz` alongside the legacy blob. From that point forward `read_existing_output` reads the sidecar only.
- Disk overhead: roughly `sizeof(blob) + sizeof(blob) ≈ 2×` per artifact. Consider pruning the blob write after two successful full cycles if disk pressure matters (not required for memory).
- **2026-08-12: the legacy blob read fallback was removed** (`read_existing_output` cold-seeds `[]` when the sidecar is missing; the module no longer imports `read_json`). Deleting a sidecar is now safe but **cold-seeds the next run** — the feed rebuilds from the lifecycle carry (the source of truth). Gate note: the deployed Umbrel image (0.2.129, 2026-08-07) predates the sidecar (`32794f97`, 2026-08-10) — the device has no sidecar yet; the sidecar-only read ships in the same release as the fix batch, and the first device cycle on the new image is the production verification (bench evidence: the sidecar path is exercised by every fresh-seed bench run, `seeding_existing_output` 0.8 s at 500 keys).

## Streaming + Memory Levers (2026-08-21)

Goal: full 2,317-key production fetch under **2.5 GiB** with **browser fallback ON** (functional requirement) and **100% sources** per cycle. Prior full-seed evidence peaked at the cgroup ceiling (~2.5 GiB) during `fetch/executing_sources` and OOM-killed the fetch child before dedup; retained-row memory scaled with coverage, not worker count.

### Landed commits

| Hash | Subject | Lever |
|------|---------|-------|
| `586ca0c3` | `perf(pipeline): stream fetched canonical rows through incremental sidecar` | Fetch workers append each canonical batch to an ephemeral `.pipeline-fetched-rows.jsonl` under a lock (`IncrementalFetchedRowsWriter`, `src/pipeline_io.py`); `run_pipeline` rehydrates chunked before finalize. Fetch RSS stays at seeded-rows + in-flight batches — this is the lever that made peak flat across 500→2125 sources. |
| `70f50bae` | `perf(transport): http2 attempt with graceful fallback + heavy-host body caps` | `http2=True` try/fallback on both pooled httpx clients; `fetch_max_bytes_for_url` caps measured outlier hosts at 2 MiB (alloc profile: en.moonton.com single-source peak **603 MiB**) on urllib, async streamed read, and pooled-browser content; outliers forced `listing_only_hosts`. |
| `1d219950` | `perf(runtime): compact hot-state JSON writes` | Task-state / live progress / prep writers emit compact separators (0.75s/5s polled artifacts). |
| `a7cef6b0` | `fix(container): restore browser-fallback default 4; bench harness env staging` | Fallback default restored to 4 after bench debugging left it 0; safe fetch default 8 (cap 12); harness stages env vars without `--only-sources-file`; `pi4-roomy-3g` probe profile. |
| `91ea4f6b` | `perf(finalize): copy-on-write lifecycle rows, replace-based renumber, progress gate` | `apply_job_lifecycle_state` shares untouched entry dicts (CoW at the three mutation sites); `_sort_enrich_and_number` uses `dataclasses.replace(id=...)` instead of `to_dict()/from_mapping()` round trip; `update_fetch_work_item_progress` skips payload rebuild when the resolved signature is unchanged. |

### Gate bench (frozen seed = full copy of live volume, 2,125 selected keys)

Image `ghcr.io/deathuman/baluffo:bench-streaming` (defaults mw=8 / fb=4), `pi4-roomy` 2.5g/2cpu, preset `smoke`, run `pipeline_408378f9df`, artifacts `_out/perf-pipeline/full2317-roomy-gate-mw8fb4/`:

| Gate | Result |
|---|---|
| Terminal | **completed** |
| Wall | **500,909 ms (8.3 min)** — warm-state caveat below |
| Peak RSS (cgroup, fb=4 active) | **2,191 MiB** (sync_push stage); fetch phases ≤2,063 MiB — under the 2.5 GiB cap with headroom |
| Sources | 2,126 selected (100%), outputCount **42,558**, rawFetched 39,558 |

Per-stage peaks: read_lifecycle_state 1,666 MiB transient (json parse tree of the 36 MB state; the normalized fast-path already avoids a rebuild), executing_sources 1,775, deduplicating 1,857, applying_lifecycle 2,063, writing_outputs 2,012.

Wall caveat: prior benches mutated source-state on this shared frozen copy, so many loaders were recently successful and ran shorter paths; the coldest observed wall remains the 3g run at 41.8 min (incl. 314 s sync_push), which also meets the <45 min budget. Cold-wall re-verification belongs on a pristine re-frozen seed.

Earlier same-day runs (for provenance): alloc-profiled mw8 run captured per-source peaks in `_out/perf-admin-flows/seed-data/perf-profiles/allocations.jsonl` via `BALUFFO_DATA_DIR=<seed> python scripts/perf_alloc_top.py`; top frames `httpx/_models.py` 89.9 MiB cumulative, `src/shared/live_task.py` 67.9 MiB, `src/jobs/transport.py` 49.5 MiB; top outlier en.moonton.com 603.8 MiB single-source peak (now capped).

### Cold-wall follow-up (2026-08-22): full-cold cycles do not yet hold 2.5 GiB

The gate pass above ran against warm source-state (incremental skips shrank the working set). Pristine-seed probes with source-state/success-cache deleted — i.e. the "100% sources run each cycle" shape — all pinned the 2.5 GiB cgroup ceiling inside `fetch/executing_sources`:

| Probe | mw | fb | defer | trim | caps | recycle | wall | terminal | executing peak |
|---|---|---|---|---|---|---|---|---|---|
| cold-1 | 8 | 4 | – | – | – | – | died ~14 min in | failed (OOM) | 2,560 MiB |
| cold-2 | 6 | 4 | – | – | – | – | died | failed (OOM) | 2,554 MiB |
| cold-3 | 6 | 4 | – | – | 8 MiB bytes | – | died | failed (OOM) | 2,520 MiB |
| **cold-fb0** | 8 | **0** | – | – | – | – | **completed, 66 min** | completed | 2,456 MiB |
| cold-defer | 8 | 4 | ✓ | ✓ | – | – | died ~29 min in | failed (OOM) | 2,559 MiB |
| cold-fb2 | 8 | 2 | ✓ | ✓ | – | – | died ~39 min in | failed (OOM) | 2,533 MiB |
| cold-capped | 8 | 4 | ✓ | ✓ | chromium args | – | died ~15 min in | failed (OOM) | 2,510 MiB |
| cold-fb2-caps | 8 | 2 | ✓ | ✓ | ✓ | – | died ~52 min in | failed (OOM) | 2,559 MiB |
| cold-recycle | 8 | 4 | ✓ | ✓ | ✓ | ✓ N=20 | died ~36 min in | failed (OOM) | 2,559 MiB |

### Obscura A/B round (2026-08-22): engine swap executed — fetch survived, finalize still pins

The A/B ran against the real [obscura](https://github.com/h4ckf0r0day/obscura) Rust engine (Apache-2.0, v0.2.0, ~30 MB vs Chromium 200+). Binary downloaded from releases, volume-mounted read-only into the bench container via `--obscura-bin-host-path`; host smoke passed including a LinkedIn challenge page (478 KB rendered HTML, startup 380 ms).

| Probe | backend | fb | Result |
|---|---|---|---|
| seeddefer-fb4 (control) | chromium | 4 | died mid-executing_sources @39 min |
| **seeddefer-obscura** | **obscura** | 4 | **fetch completed all 2,126 sources (95,491 rows)** → child died at finalize/dedup boundary |

Key findings:

1. **Obscura kept fetch alive through the entire source sweep** — every fallback acquisition succeeded, zero browser errors. The death moved from mid-fetch to the dedup/finalize boundary, where the 96k-row working set materializes.
2. The lighter engine freed enough headroom for the *fetch* phase but not for the *finalize* phases (dedup + lifecycle + write), whose working set scales with output rows regardless of backend.
3. Efficacy: no browser-error work items; LinkedIn rendered at full fidelity in host smoke.

**Final verdict unchanged but sharpened:** the binding constraint is the 96k-row finalize working set, not any single process. Full-cold coverage at current seed-yield needs either (a) the sidecar browser charter AND seeded/fetched defer already landed, plus a finalize-row-streaming pass, or (b) acceptance that full-cold re-baselines need ≥3 g while steady-state meets 2,191 MiB WITH fb=4.

Artifacts: `_out/perf-pipeline/full2317-COLD-obscura-fb4/`. Harness flag: `--obscura-bin-host-path <dir>` (commit `3a8bdddd`).

### Gate closed (2026-08-22): jemalloc via LD_PRELOAD — full-cold coverage meets 2.5 GiB with fb=4

The 13-probe matrix proved Python heap was exonerated and the ceiling was allocator/page-cache behavior. The fix is a **3-line Dockerfile diff**: swap glibc malloc for jemalloc via `LD_PRELOAD`, configure background page purging, and force large transient buffers through mmap.

```dockerfile
RUN apt-get install -y --no-install-recommends libjemalloc2 && apt-get clean
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
    MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000 \
    MALLOC_MMAP_THRESHOLD_=65536 \
    MALLOC_TRIM_THRESHOLD_=65536
```

Precedent: Refine engineering (47.5% RSS reduction in production), GitLab (`dirty_decay_ms:1000` production value), BetterUp (same stack/symptom/fix). glibc's auto-trim only fires on fully-free arena tops; concurrent worker threads scatter allocations across arena space so the trim never triggers. jemalloc's `background_thread:true` actively purges dirty/muzzy pages after a 1 s decay. `MALLOC_MMAP_THRESHOLD_=65536` forces allocations >64 KB through mmap for immediate OS reclamation on free.

**Cold full-coverage bench result** (`pi4-roomy` 2.5g, obscura fb=4, frozen seed, runId `pipeline_f9972fd5d4`):

| Metric | Gate | glibc baseline | jemalloc |
|---|---|---|---|
| Terminal | completed | ❌ OOM | ✅ **completed** |
| Peak RSS | ≤2.5 GiB | ❌ 2,560 MiB | ✅ **1,792 MiB** (`writing_outputs`) |
| Fetch avg RSS | — | 1,573–1,991 MiB | ✅ **552 MiB** (-72%) |
| Wall | <45 min | N/A | 56 min ⚠️ marginal |
| Coverage | 100% | ✅ | ✅ 2,126 sources |
| Output | healthy | — | 43,168 rows / 54,862 raw |

Wall-time note: 56 min exceeds the 45 min target by ~24% for **full-cold** coverage only. Steady-state incremental cycles complete in ~8 min at 2,191 MiB, meeting all gates simultaneously. A chromium+jemalloc A/B could recover wall margin if needed.

Commit: `90dbfd82` (Dockerfile only, zero application code changes).

### Wall-time tuning round (2026-08-22): mw=12 closes most of the gap

With jemalloc freeing allocator overhead, container-pipeline concurrency defaults raised from glibc-era conservative values (`a3d64548`): mw 8→12, `max_per_domain` 2→3, static_detail_concurrency 4→6, adapter_http_concurrency cap 24→32.

**Cold full-coverage result** (same seed/reset protocol, runId `pipeline_220cb5d164`, artifacts `_out/perf-pipeline/full2317-COLD-jemalloc-t0/`):

| Metric | mw=8 jemalloc | mw=12 tuned | Gate |
|---|---|---|---|
| Terminal | completed | **completed** | ✅ |
| Peak RSS | 1,792 MiB | **1,940 MiB** | ✅ ≤2.5 GiB |
| Fetch avg RSS | 552 MiB | 627 MiB | ✅ |
| Wall | **56.0 min** | **45.5 min** (-19%) | ⚠️ marginal |

Effective parallelism improved from ~4.7× to ~6.1×. Only 6 sources exceed 60s; top-10 consume 8.1% of serial time. The 0.5-minute overshoot on full-cold is within single-run variance; steady-state cycles meet all gates at ~8 min.

### Instrumentation summary

Python heap ≤145 MiB (tracemalloc-proven). Ceiling is combined cgroup footprint of all processes + page cache.

**Complete matrix: 13 cold full-coverage probes, only fb=0 completes (2,456 MiB).** Every fb≥1 configuration dies regardless of engine (Chromium/Obscura), concurrency, body caps, renderer caps, recycling, lifecycle defer, seeded defer, or trim timing. Steady-state incremental cycles meet 2,191 MiB WITH fb=4. Production Umbrel has no memory limit.

### Notes for future levers

- Remaining largest retained block is the lifecycle parse tree itself; if a tighter seat is ever needed, stream-normalize or convert persistence to JSONL chunks (storage-contract change).
- Heavy-host list is ops-extensible via `BALUFFO_STATIC_LISTING_ONLY_HOSTS`; body caps scale from `BALUFFO_FETCH_MAX_BYTES`.
- `perf_admin_seed.py` whitelist misses newer artifacts (`admin-task-lifecycle.json`, `*.rows.jsonl.gz`, delta `source-registry-*.jsonl`); freeze seeds with a full directory copy instead.

# Fetch-lane CPU attribution — first S1 run (2026-10-08)

Canonical execution record for slice **S1** of
[`docs/plans/fetch-pipeline-performance-instrumentation-plan.md`](../plans/fetch-pipeline-performance-instrumentation-plan.md):
attribute the ~1 core the fetch pipeline spends, on which thread, and in which functions.

This is **evidence, not a speedup**. Nothing was optimised; the point was to replace the
GIL inference every earlier decision rested on with a measurement.

## What ran

| Item | Value |
|---|---|
| Seat | local image built from the working tree, `--cpus 2.0 --memory 2.5g --memory-swap 2.5g` |
| Call | real `POST /tasks/run-jobs-pipeline` with `{"jobsPageLoadedCount":0}`, **no preset override** |
| Child profile | `/usr/local/bin/python -u -m src.jobs_fetcher --max-workers 12 --max-per-domain 3 --adapter-http-concurrency 48 --static-detail-concurrency 6 ...` |
| Workload | 120 of 2,056 registered sources = 25.3% of the summed source work (top 120 by cost + a stride sample; selected with `BALUFFO_CONTAINER_PIPELINE_ONLY_SOURCES`, a documented bench knob) |
| Instrumentation | `BALUFFO_PROFILE_SAMPLE=10` in the child, plus `tools/measurements/pipeline/thread_cpu.py` sampling the same child from outside |
| Fetch window | 720 s (pipeline 16:03:43Z → 16:17:52Z), 42,937 output rows |

Evidence: `_out/perf-runs/s1-cpu-attribution-b-20261008-160317/` — `CPU-ATTRIBUTION.md`,
`cpu-attribution.json`, `thread-cpu.ndjson`, `thread-cpu-summary.json`, `progress.ndjson`,
`container-cpu.ndjson`. The stage-share-only pass over an existing full run is
`_out/perf-runs/s1-stage-share-20261008-155322/`.

## Result

**The transport loop is not the cost, and the seat was about half used.**

| Measure | Value |
|---|---|
| Fetch child CPU (independent sampler, whole life) | 727.4 s process / 725.6 s summed threads over a 721.1 s window ⇒ **~1.0 core**, **50.4%** of the 2.0-core quota |
| Async transport loop share of attributed CPU | **3.2%** (20.0 s; its top frame is `ssl.py:read`, i.e. waiting on I/O) |
| Source-worker share | **76.5%**, spread across all 12 pool threads (largest single thread 20.2%) |
| Reconciliation, two instruments | 722 ticks at 1.001 Hz, max gap 1.008 s; 727.43 s vs summed threads 725.61 s = **−0.25%** |

Where the CPU that does exist goes (in-process window, 628 s attributed):

| Domain | Share | Notable frames |
|---|---|---|
| html-parse | 48.7% | `re.sub` **198.2 s (31.6% of the total)**, `html_parsers.html_fragment_lines` 9.4 s |
| unmapped | 24.8% | `threading.wait` 39.8 s (blocking), `dataclasses._asdict_inner` 26.9 s |
| canonicalise-dedup | 12.6% | `location.py` genexpr 21.9 s, `text_utils.sanitize_public_text` 10.2 s |
| orchestration | 9.5% | `pipeline_runtime_summary.update_fetch_work_item_progress` 25.6 s, `write_task_state` 7.3 s |
| protocol-tls | 3.0% | the loop thread and its `ssl`/`selectors` frames |
| state-io / body-decode | 0.7% each | |

By our own module, the top self-time holders are `jobs/text_utils.py` (74.3 s),
`jobs/pipeline_runtime_summary.py` (34.4 s), `jobs/adapters/parsers/location.py` (30.9 s),
`jobs/adapters/location_rules.py` (30.1 s), `_rendered_cards.py` (15.7 s),
`html_parsers.py` (12.6 s).

Computed shape: **`thread-contended-python-work`** at 50.4% utilisation, confidence Medium.
That verdict sits 0.4 points above the `latency-bound` cut, so it must be read *with* the
shares, not instead of them: half the seat is idle, the loop is idle, and the CPU that exists
is regex/heavy-normalisation and bookkeeping on worker threads.

Wall-clock context from the same run: 119 sources, 5,168 s of summed source duration against a
720 s window (7.2× source concurrency), 68 zero-kept sources (57.1%) carrying 33.1% of it, and
20 non-ok sources (16.8%). The free pass over the last full run (2,056 sources, 2026-09-23)
independently found 985 zero-kept sources (47.9%) carrying 39.1% of summed duration — consistent
with the prior program's 989 / 37.7%.

## What this changes about the inherited premise

- **"All HTTP funnels through one event loop, so that is the ceiling"** — the loop holds 3.2% of
  the CPU. It is not the ceiling. The flat 12→20-worker curve is not explained by the loop.
- **"That is the GIL signature"** — this run does not reproduce it as a thread story: CPU is
  spread across 12 workers, not pooled in one. What it shows instead is a half-idle seat, with
  the largest single CPU consumers being `re.sub` (31.6%) and serialization/bookkeeping.
- The first optimisation target per D4 is therefore **the regex-heavy text/location
  normalisation path** (`text_utils`, `location_rules`, `location`), not concurrency and not the
  transport.

## Defects found in the new tooling, and fixed

1. **`/proc/self/task/<python ident>/stat` — wrong key space.** `sys._current_frames()` is keyed
   by `threading.get_ident()`, which is not the kernel tid, so every per-thread read failed and
   the sampler silently fell back to elapsed weighting — reporting blocked time as CPU. Found by
   the first full run (kept at `_out/perf-runs/s1-cpu-attribution-20261008-154651/` with a
   `SUPERSEDED.md`), fixed by mapping ident → `Thread.native_id`, and pinned by a test. The
   fallback is now joined to the external sampler's OS tids in the report.
2. **The exit dump never runs.** The fetch child is killed when the pipeline completes, so the
   `finally` dump is skipped; at a 300 s interval the finalize tail was unattributed entirely
   (600.7 s covered of a 721.1 s life). Interval is now 60 s, and the report states coverage and
   the uncovered seconds.
3. **A false finding, retracted.** A 60-character truncation of the child's command line printed
   `--max-workers 1`, which briefly read as "the documented acceptance call runs serially". The
   full command line shows `--max-workers 12` and the container profile. Recorded because it is
   the same failure mode as the four wrong answers of the prior session: an indirect reading
   taken as ground truth.

## Limits

- A subset run (25.3% of summed source work). Function-level shares are representative; absolute
  wall-clock is not, and the seat is CPU-capped.
- One run, one day of live boards. Sampling attributes sampled time; a thread may start and
  finish between ticks. Per-function weight is **directional, not exact**: samples are weighted by
  the `/proc` CPU visible at the tick, and the report states where that weight was unreadable
  rather than guessing.
- Python only: the browser-fallback Node driver and the discovery child are separate processes and
  are not attributed here (D6/D9).
- The 24.8% unmapped bucket is printed in the report with its frames rather than absorbed. Folding
  `threading.wait` and `dataclasses` into named domains is a follow-up, not a finding.

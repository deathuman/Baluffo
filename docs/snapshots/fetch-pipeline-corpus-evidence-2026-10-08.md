# Corpus replay harness — execution evidence (S2–S7, S0), 2026-10-08

Canonical execution record for slices **S2–S7 and S0** of
[`docs/plans/fetch-pipeline-performance-instrumentation-plan.md`](../plans/fetch-pipeline-performance-instrumentation-plan.md).
That plan keeps the design, the acceptance criteria and the decisions D1–D10; this file keeps the
receipts — what ran, what it measured, and what it could not see.

Everything here is local: epochs and studies live under `_out/perf-runs/`, are never committed, and
are cited by name so a disputed number can be re-read rather than re-argued.

## S2 — per-source fetch counters

`src/jobs/fetch_counters.py`. Gate `BALUFFO_FETCH_COUNTERS`, resolved **once at import**; when it is
off, `wrap_fetch_text` returns *the same callable object* it was handed, so the disabled path cannot
allocate, lock, or read the environment (asserted by test, not argued). Per source: `attempts`,
`succeeded`, `failed`, `repeatFetches`, `distinctUrls`, `statusCodes`, `errorClasses`,
`decodedChars`, `textBytes`.

The per-source `httpCounters` block reaches the **persisted** report through an additive whitelist
pass-through in `src/shared/fetch_report_normalization_primitives.py` — which a real end-to-end run
proved necessary; without it the counters existed only in memory. It also publishes the per-run
`cacheDecision` histogram the plan's no-skipping section asked for.

Ceiling (D1, always-on): **−0.77%**, inside the measured 1.63% noise floor — see S6.

## S3 — corpus store

`tools/measurements/corpus/store.py` + `epoch.py`. Key = method + canonical URL + the
response-changing header subset (`Accept`, `Cookie`, `Authorization`, `If-None-Match`,
`If-Modified-Since`). Bodies are gzipped with `mtime=0` so identical bytes hash-match; **failures are
first-class records** (status, exception class, message), and a miss *raises* rather than yielding an
empty body. Authority is the newest `recordedAt`; there is no pointer file.

**Layout was chosen after size was measured**, not predicted: a five-source subset records
**1 543 entries / 464 unique bodies — 28.5 MB compressed against 506 MB naive** (gzip ratio
0.1125), so content-addressing plus gzip removes ~89% of the raw volume.

## S4 — record mode

`tools/measurements/corpus/run_fetch.py --mode record`, driven through the existing
`resolve_fetch_text_impl` seam with **zero adapter changes**, at `force_refresh_all` (D5).

Reference epoch: `_out/perf-runs/corpus-subset-20261008-192418/` — 1 543 entries, 464 bodies,
**30 053 rows**, `offered == recorded == 1 547`, `unattributed == 0`. Coverage is stated rather than
implied: the report names discovery, browser fallback (D9) and the async httpx transport as out of
lane, so "we replayed the fetch lane" cannot be misread as "we replayed all traffic".

## S5 — replay mode and the egress bar

Replay serves from the corpus at `force_refresh_all`, so the fetch set is fixed and
clock-independent. The socket-level guard makes every outbound connect raise, so zero egress is
proven by construction.

Parity is checked on **four** units — row count, job-identity set, a run-independent row projection,
and per-source status counts — with the ledger showing every offered request served from the corpus
(0 misses, 0 unattributed) and a literal **`blockedRemote == 0`** on the guard.

Four harness defects were found by exercising the real surface rather than a fake one:

1. the rows artifact is gzip-backed, and a driver reading the plain name reported a confident zero;
2. `raise_for_entry` let a truthy status code shadow the recorded exception class, so a replayed
   `urlopen` 404 arrived as `HttpStatusError` and callers' `except HTTPError` stopped matching;
3. httpx entries were keyed by the **post-redirect** URL, which left 465 of 1 547 offers
   unattributed and the corpus unreplayable;
4. the guard's own loopback block killed `asyncio.run` in the static lane, failing four sources
   against the guard instead of the corpus.

All four are fixed and each is pinned by a test.

## S6 — determinism, and both D1 ceilings

Receipts: `_out/perf-runs/s6-determinism-final/determinism.json` (the graded window) and
`_out/perf-runs/s6-determinism-compact/determinism.json` (the later window below).

| Sample | Wall clock |
|---|---:|
| 1 | 74 466 ms |
| 2 | 73 269 ms |
| 3 | 73 447 ms |

**Verdict: deterministic** — median 73 447 ms, spread **1.63%** against the 15% limit, identical
rows, all four parity units equal to the recorded run on every sample, `blockedRemote == 0`
throughout.

| Arm | Median | Δ | Ceiling | Verdict |
|---|---:|---:|---:|---|
| always-on counters (`BALUFFO_FETCH_COUNTERS=1`) | 72 884 ms | **−0.77%** | ≤0.5% | within the 1.63% noise floor |
| opt-in profiler (`BALUFFO_PROFILE=1`) | 144 532 ms | **+96.8%** | ≤2% | **exceeds the ceiling** |

Two honest readings of that table:

- The counters arm is *inside the noise floor*, so the ≤0.5% ceiling is not contradicted and no tax
  is **resolvable** at this sample size. S2's disabled path is separately the same callable object,
  which is why the cheap path is believed rather than merely unmeasured.
- The opt-in arm misses its ceiling by two orders of magnitude — and it is the **pre-existing**
  cProfile hook the plan chose to extend, not new instrumented code. The actionable finding is that a
  ≤2% opt-in ceiling needs a sampling instrument, not cProfile (plan open question 4).

**A second window, and an artifact defect.** Re-running the same gate later measured
104 668 / 124 120 / 150 635 ms with **all four parity units still matching** — and it returned
**`not-deterministic`** (spread 37.03%), refusing to grade a busy window instead of grading it
anyway. That window's ceilings read −11.1% (counters, "within-noise" against a 37% floor) and +72.4%
(cProfile, "exceeds-ceiling"): same direction, no resolution. Receipt:
`_out/perf-runs/s6-determinism-compact/determinism.json`.

The same re-run fixed an artifact defect it had inherited: each stored sample carried the run's whole
projected row set — 11.8 MB per sample, a **53 MB** receipt for a three-sample check — although every
consumer compares four digests. The receipt is now **6.9 KB**, pinned by
`tests/tools/test_corpus_parity.py::test_the_stored_summary_digests_the_projection_instead_of_copying_it`.
The graded window's receipt (`s6-determinism-final/determinism.json`) predates that fix and is kept as
the record it graded.

**Not measured, and not claimed:** `BALUFFO_PROFILE_SAMPLE`, the all-threads sampler. It starts in
`src/jobs_fetcher.py`'s `__main__`, while the corpus driver runs the pipeline in-process, so an arm
probing it here would report a false "no overhead".

## S7 — one command, and an interruption

Exercised for real by S0, which is why the plan sequences S7 before it.

`ab.py run` drove the twelve replay arms as adjudicate's engine. When the study's process was
interrupted before it wrote its report, `--resume` rebuilt **all twelve finished arms** from their
own report windows — re-deriving parity units, rows and repeated-fetch exposure from the artifacts,
re-running nothing — and labelled the reuse in both the JSON and the markdown. The other half of the
path (a pair whose second arm never finished, so the pair is re-run in full) is pinned by test rather
than by hoping it happens. Receipts: `_out/perf-runs/s0-pool-claim/ab/ab-report.{json,md}`.

`explain.py --run <token-or-dir>` ranks a real run from its own report. On S0's first arm:

| Signal | Value |
|---|---:|
| canonicalization | 16.9 s (52.6% of summed source time) |
| `google_sheets` | 28.2 s (87.6% of summed source time) |
| zero-kept sources | 4.0 s (12.4%) |
| repeated-fetch exposure | 3.6 s (11.2%) |
| parallelism (summed source / wall) | 0.437 |

**Defect found and fixed in-slice.** The retry/backoff proxy tested `httpCounters.attempts > 1`,
which counts *requests*, not retries — a source fetching 1 200 URLs has 1 200 attempts and may have
retried nothing — so every arm read ~99% "retry exposure". It now reads `repeatFetches` (attempts
beyond the first for the same URL), the signal the counter's own docstring defines, and the reports
say "repeated-fetch" rather than claiming a retry count the seam cannot produce. The S0 arms'
corrected exposure is **9.2% / 9.3%**.

## S0 — adjudicating `61c43fab`

Receipts: `_out/perf-runs/s0-pool-claim/S0-VERDICT.{json,md}`.

Claim: *"perf(fetch): give the adapter HTTP pool room to hide latency (−11.3% wall)"* — four Docker
arms, frozen seed, live HTTP, `adapterHttpConcurrency` 32 vs 48.

Study: **6 pairs, alternating order**, epoch `corpus-subset-20261008-192418`, every arm
`force_refresh_all`, counters on, 30 053 rows each, all four parity units matched, `blockedRemote == 0`.

| Statistic | Value |
|---|---:|
| paired deltas | +0.444, −10.656, −0.522, +2.514, −1.159, +8.316 (%) |
| median paired delta | **−0.039%** |
| 95% CI (bootstrap over pair deltas) | **[−5.908%, +5.415%]** |
| arm medians (report window) | A 88 977 ms · B 90 762 ms |
| verdict | **inconclusive — `inconclusive-by-harness`** |

**Why the numbers cannot bear on the claim either way.** The claim's variable is
`adapter_http_concurrency`, and a replay supplies a custom `fetch_text`, which short-circuits
`resolve_fetch_text_impl` (`src/jobs/transport.py`) — so `AsyncHttpTextFetcher`, the only in-process
reader of the pool size, is never constructed. The runs' own reports say `fetchClient=custom`. Both
arms therefore ran with the variable inert: the arms agreeing is **evidence of the bypass**, not
evidence of no effect. That reachability check reads the product's own source at run time, so it
cannot rot silently into a confident "no effect".

**What the study does establish.** At six pairs on a four-source subset the harness resolves
engine-time effects above roughly ±5.5%, and the *reported* −11.3% sits outside that interval. It is
still not a refutation: replay removed the wire the claim is about, so it can speak to the claim in
neither direction. Per D2 there is no release note and no Basic Memory correction, because D2's
remedy is conditional on a refutation.

**Absolute replay wall clock drifts between windows.** The same four-source subset measured
73.3–74.5 s in S6's graded window, arm medians of 88 977 / 90 762 ms in S0's window two hours later
(pair samples climbing 73.6 → 93.8 s across that study), and 104 668–150 635 ms in a later re-run.
Counters do not explain any of it — S6 measured them at −0.77%. Within-window noise was ±1.63% in
the quiet window and 37% in the busy one; neither covers between-window drift. That is why the
verdict is computed from *paired deltas inside one window*, and why no absolute replay figure should
be quoted as a production number.

**What would settle it.** A live paired study, not a replay: two container arms on the same frozen
seed (`--cpus 2.0 --memory 2.5g`), `adapterHttpConcurrency` 32 vs 48, alternating order, ≥6 pairs,
wall clock as the metric, on a day the boards are stable. Until someone runs it, the −11.3% stands
**unverified rather than disproven**.

## Reproducing any of this

```bash
# one real record (bounded, labelled), then hermetic replays of it
python tools/measurements/corpus/run_fetch.py --mode record --scope subset \
    --label "subset epoch" --sources 4 --families google_sheets --max-workers 1
python tools/measurements/corpus/run_fetch.py --mode replay --scope subset \
    --corpus-dir _out/perf-runs/corpus-subset-<stamp>

# determinism + both D1 ceilings; a paired A/B (--resume reuses finished pairs); a run explainer
python tools/measurements/corpus/determinism.py --epoch <epoch-dir> --samples 3 --ceiling-runs 2
python tools/measurements/corpus/ab.py run --epoch <epoch-dir> --pairs 6 [--resume]
python tools/measurements/corpus/explain.py --run <token-or-dir> --top 8

# the S0 adjudication, on the harness, with the harness's reach stated
python tools/measurements/corpus/adjudicate.py --epoch <epoch-dir> --pairs 6
```

Command surface, artifact meanings and the egress bar are documented in
[`tools/README.md`](../../tools/README.md); fixture layout and test routing are in
[`docs/testing.md`](../testing.md).

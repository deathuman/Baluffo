# Discovery lane cost and the targets behind it — 2026-10-11

> - **Class:** measurement + one shipped bound
> - **Trigger:** the fetch lane has three measured wins and a hermetic harness; discovery has neither
>   a head-to-head harness nor a published cost ranking, so "attack discovery next" had no evidence
>   behind it.
> - **Status:** one bound shipped (probe batch), one lever refused on evidence, four targets ranked
> - **Verified against:** `b7c2fd9c` + the change in this snapshot
> - **Then inspect:** [`../plans/fetch-pipeline-performance-instrumentation-plan.md`](../plans/fetch-pipeline-performance-instrumentation-plan.md)
>   (the fetch lane's D1–D10 decisions and its corpus harness), [`../scraping-pipeline.md`](../scraping-pipeline.md)

## What was measured, and from where

No new harness was needed to find the cost: discovery already records per-stage wall clock
(`runtime.stageTimingsMs`, 11 keys) and the GameDevMap active audit already records per-phase totals.
Both are in artifacts that real runs left behind.

| Evidence | Source | What it shows |
|---|---|---|
| Last production-shaped run (2026-09-22, `preset=default`, aborted at 13.4 s) | `data/source-discovery-report.json` | `seedCareersScan` **12,615 ms of 13,398 ms (94%)**; sheetDirectory 363 ms; providerPatterns 14 ms; `probe` never reached |
| 17 run reports (2026-09-17 → 09-22) | `data/discovery-report-history/*.json.gz` | `probe` 13,153–13,830 ms inside 14.1–16.5 s totals (88–98%); one 629.7 s run whose `gamedevmap` stage alone is 611.8 s; one `gameprog` 46.5 s; one `seedCareersScan` 11.9 s + `gamesmap` 12.2 s |
| 10 coverage-run reports | `_out/coverage/*/source-discovery-report.json` | `probe` is 78–98% of every run: 1,062 ms (8 probes) … 15,246 ms (8 probes) … 31,563 ms (705 probes) |
| GameDevMap active audit, uncached | `summary.gamedevmapAudit.timingTotalsMs` in the run reports | total **950,937 ms**, split probe 285.9 s, homepage fetch 273.5 s, recovery wave 1 fetch 163.7 s, wave 2 fetch 91.0 s, homepage analysis 105.2 s, wave analyses 31.2 s, direct inference 0.12 s, merge 0.16 s |
| The same audit's counts | same | 342 active candidates from 1,277+ homepages; failure buckets `probe_failed` **5,477**, `zero_jobs` 3,353, `recovery_fetch_failed` 3,060; 27.5 MB artifact; `cacheHit: true` on later runs |
| Stage attribution | `data/source-discovery.log`, 2026-09-22 | waves 1–2 end 14:12:13.138, `Web search page fetch: fetched 4/4 pages` lands 14:12:20.750, `Seed careers scan complete` 14:12:20.777 — 7.6 s of *web-search* work billed to `seedCareersScan`, while `stageTimingsMs.webSearch` reads **0** |
| Web-search query phase, live | `_out/perf-runs/discovery/ws_query_bench.py` | 8 serial DuckDuckGo queries 1,663 / 2,823 ms; 12 serial 2,419 / 2,545 ms |
| Probe batch under a trickling server | `_out/perf-runs/discovery/probe_drip_bench.py` | 40.3 s against a 2 s timeout, all candidates *successful*; 2.2 s after the bound |
| End-to-end run *after* the bound | `python src/discovery_sanity_benchmark.py --preset quick` (isolated dir) | 725 candidates probed in a **72,121 ms** probe stage inside a 73,352 ms run — 98% probe, 15 failures, 20 queued, and **no candidate hit the new deadline** (229 s at that size) |

Two facts fall out of this immediately.

**Discovery is one stage's cost, and it is the probe phase.** In every standalone run the probe stage
is 78–98% of the wall clock, and inside the GameDevMap audit the same phase is 30% of a 15.8-minute
job. Its shape is a tail, not throughput: 8 probes took 15.2 s while 705 probes took 31.6 s — the
stage ends when the *slowest* candidate ends, and a candidate can spend `timeout_s` per request,
several requests, plus two retries.

**The GameDevMap active audit is the largest single item in the lane by an order of magnitude.**
950.9 s uncached, against 13–90 s for a whole ordinary discovery run, and the container bench
already carries a comment that it "dominates discovery when enabled" and disables it
(`scripts/perf_pipeline_stages.py`). Its own timing keys make it dissectible without new
instrumentation: probe 30%, homepage fetch 29%, the two recovery waves 27%, the two analyses 14%.

## Shipped: the probe batch is bounded now

`run_bounded_probe_batch_async` had no wall-clock bound on its **default** path. `httpx.Timeout` is a
per-operation budget whose read timeout re-arms on every chunk, so a board that trickles bytes held
the call past its own timeout and then returned *successfully* — measured **40.3 s against a 2 s
timeout** on a loopback drip server, with all three candidates counted as successes. The sibling
batch in `src/shared/http_batch.py` documents exactly this loophole (40.7 s, and a GameDevMap batch
parked 900 s) and already bounds itself; the probe batch did not, on the path that carries every
discovery probe and the audit's ~5.5k of them.

Two bounds, both following an existing shape rather than a new policy:

- **per fetch** — `asyncio.wait_for` around each call, with the sync branch's treatment extended to
  the default httpx branch, raising `fetch timed out after Ns` instead of asyncio's empty-message
  `TimeoutError` (an empty error is a failure row nobody can classify);
- **per batch** — `timeout_s * (candidates / effective_concurrency) + timeout_s`, the same formula
  and the same reasoning as `http_batch`: a candidate may make several requests plus retries, so the
  bound must not be `timeout_s`, and the concurrency actually available is what makes it fit.

Verified: `tests/source_discovery/test_probe_runtime_bounded_fetch.py` grew two guards — a real
trickling server on the default httpx path, and a batch whose candidates outlast the deadline. Both
**fail on the pre-change tree** (40 s park; 6.2 s park) and pass after. Healthy work is untouched: the
existing "healthy fetcher is unaffected" guard still passes, and the deadline for 705 candidates at
40-way concurrency is 223 s against a 31.6 s measured stage.

## Refused: making the search-query phase concurrent

The query phase is one HTTP round trip per query and little else, so it is structurally serial in
code (`for query, seed in queries: html = fetcher(url, timeout_s)`) and costs a real share of an
ordinary run: ~7.6 s at the 24-query cap, inside a 13.4 s run. Parallelising it was implemented,
measured, and **withdrawn**:

| Arm | 12 queries | Outcome |
|---|---|---|
| serial | 2,419 / 2,545 ms | 12/12 succeeded, both runs |
| 4 concurrent | 674 ms | 12/12 — 3.7× faster |
| 4 concurrent (next run) | 2,121 ms | **1/12** — the search host throttled the burst |
| any arm, after the bursts | 1,414 / 5,529 ms | **0/8, 0/12 — `HTTP Error 403` on every query, both arms, still blocked 40+ minutes later** |

A 403 costs the whole query set — zero links, zero candidates — to save ~5 s. Trading coverage for
latency is the wrong direction on this lane, and the throttling is not something a retry policy
fixes. The lever that remains is *fewer, higher-yield queries* (or a keyed search API), and the audit's
6-hour TTL cache already absorbs repeat runs.

## Targets, in attack order

| # | Target | Measured cost | Why it is first |
|---|---|---|---|
| T1 | **GameDevMap active audit** | 950.9 s uncached; 4.8 min probing, 4.6 min homepage fetch, 4.3 min waves, 2.3 min analyses | 10× every other item in the lane combined; already per-phase instrumented; the bench works around it by disabling the stage, so its cost is currently paid only by real runs |
| T2 | **Probe phase tail** | 78–98% of every standalone run; 15.2 s for 8 probes vs 31.6 s for 705 | the stage's clock is set by its slowest candidate, and `probe_failed` 5,477 in one audit says the slow ones mostly fail; the retry-success rate is *not* instrumented yet |
| T3 | **Shared directory audit** (seed careers + web search) | 12.6 s of a 13.4 s run: 25 pages 2.7 s, waves 2.3 s, web-search queries + 4 pages 7.6 s | the only stage with a directly measured sub-cost that is not yet attacked; the serial query loop is the piece |
| T4 | **Stage attribution** | `webSearch` reports 0 ms while its 7.6 s is billed to `seedCareersScan` | cheap to fix, and it is the reason the last three sessions read this run's profile wrong |

Two things worth building before T1/T2 are attacked, both modelled on the fetch lane: a
*discovery corpus* equivalent (the fetch lane's hermetic replay is what made its three wins
defensible, and this lane has nothing comparable — every number above is live), and a probe-to-probe
record of retry outcomes, because "which probes need their retries" is currently unanswerable and
decides whether T2 is a timeout policy or a scheduling one.

## Reproducing the numbers

```bash
# stage costs from real runs (no network)
python - <<'EOF'
import gzip, glob, json
for path in glob.glob("data/discovery-report-history/*.json.gz"):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        runtime = json.load(handle)["runtime"]
    print(path, runtime["totalDurationMs"], runtime["stageTimingsMs"])
EOF

# the query phase, live (interleaves arms; --pause lets a rate-limit window decay)
python _out/perf-runs/discovery/ws_query_bench.py --queries 8 --repeat 2 --pause 5

# the probe batch under a trickling server (before/after the bound)
python _out/perf-runs/discovery/probe_drip_bench.py --timeout 2 --candidates 3

# the shipped guards
python -m pytest tests/source_discovery/test_probe_runtime_bounded_fetch.py -q
```

The two dev scripts deliberately live under `_out/` (untracked): a `scripts/` edit is shipped
container code and trips the release gate, and neither script is product behaviour.

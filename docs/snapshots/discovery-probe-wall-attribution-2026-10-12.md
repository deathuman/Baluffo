# What the probe wave's wall is bound by, now that its lanes are honest — 2026-10-12

> - **Class:** measurement; the permit-order fix it priced has since shipped (see
>   [`discovery-probe-permit-order-2026-10-12.md`](discovery-probe-permit-order-2026-10-12.md))
> - **Trigger:** the lane-ceiling repair made the probe wave's lane count honest (peaks **36 / 34**
>   of the configured 40, no plateau, with the pre-repair pair retained as the control that still
>   shows the 20-lane fingerprint). Honest lanes left the obvious question open: if the lane count is
>   not the cap, what is? Three candidates were on the table — per-candidate work, host latency, and
>   event-loop blocking.
> - **Status:** measured. **Host latency is where the time is spent** (93% of every lane at 80 lanes
>   is inside a fetch's own window, mean 410 ms per fetch), **per-candidate work is not the wall**
>   (7.0% of lane work at 80 lanes; ~1.7 ms of CPU per candidate), and **the event loop is not
>   blocking** (mark lag p50 **1.07 ms**, p95 **12.4 ms**, over 250 ms in 0.1% of marks) — but the
>   loop's own **8.4 ms per candidate** puts a hard ceiling of ~**121 candidates/s** on this code
>   path, only 1.4× above what the 80-lane arm already did. The immediate binding constraint is
>   **permit allocation**: the wave's pool (40) runs **81-89% of its span pinned at 40** while only
>   **29.2** of those permits are probing — **6.8 permits on average (19% of pool-permit time) are
>   held by *static* candidates queued for their own 16-lane bucket**, which starves the 80%-of-
>   population provider bucket to **21.0** of its own 40. Acquiring the bucket permit *before* the
>   pool permit — a two-line swap, bench-local, exercised as a text-level patch of the shipped
>   function — measures the phase at **47.6 → 41.9 s** (pairs: **49.8 → 41.7**, **44.9 → 39.7**;
>   −12% to −16%) with the pool still the binding resource and **no bucket cap raised**.
>   *Superseded in part:* the loop item was then attacked, and the loop turned out not to be the phase's
>   ceiling — see [`discovery-probe-httpx-off-loop-2026-10-12.md`](discovery-probe-httpx-off-loop-2026-10-12.md),
>   where the loop's HTTP work moves to the batch pool (loop CPU 22.28 → 4.97 s, phase 52.28 → 45.46 s),
>   and the ceiling that remains turns out to be a **process** one: Python work is GIL-serialized, so
>   per-candidate CPU (~173 candidates/s), not the loop, is the limit above that.
> - **Verified against:** `9b8901f8` plus this session's uncommitted cuts (the lane repair in
>   `src/source_discovery/probe_runtime.py` is part of them). Nothing in this snapshot changes
>   tracked source.
> - **Then inspect:** [`../scraping-pipeline.md`](../scraping-pipeline.md) for the wave's shape,
>   [`discovery-lane-cost-2026-10-11.md`](discovery-lane-cost-2026-10-11.md) for the stage ranking
>   that put the probe first, [`discovery-directory-lane-ceiling-2026-10-12.md`](discovery-directory-lane-ceiling-2026-10-12.md)
>   for the lane repair this follows. The instruments are `_out/perf-runs/discovery/probe_wall_bench.py`
>   (phase clock, occupancy, loop lag and loop-thread CPU) and `probe_permits_bench.py` (permit
>   attribution), both untracked.

## The population, and the four hosts under it

3,038 candidates replayed from the audit's own artifact
(`_out/perf-runs/discovery/audit-runs/audit-lane-ceiling.json`, `timeout_s = 6`). The shape matters
more than the size, because it decides what a lane cap *is*:

| Split | Count | Share |
|---|---|---|
| candidates | 3,038 | — |
| distinct hosts | 612 | — |
| on `boards-api.greenhouse.io` / `api.lever.co` / `api.smartrecruiters.com` / `apply.workable.com` | 1,089 / 600 / 600 / 120 | **79%** |
| provider bucket (structured ATS) / static / teamtailor | 2,428 / 602 / 8 | 80% / 20% / 0.3% |
| HTTP 404 verdicts | 1,698 | **56%** |
| URLs answering 429 (identical in all three arms) | 238 | — |

So the probe's `provider` bucket is not "many independent hosts" — it is four API hosts carrying four
fifths of every run. That is the budget the caps are protecting, and it is why the numbers below are
stated per bucket rather than per lane.

## Where the lane wall goes

`probe_wall_bench.py`, three arms over the same population and timeout. `sync` passes a wrapping sync
fetcher (the urllib path, in the batch's own pool); `async` is the shipped default path (httpx, one
coroutine per lane) and `async-80` is the same at `total = 80 / static = 32`. Lane work is the sum of
per-candidate probe windows; the fetch share is the part of it inside a recorded fetch window.

| Arm | Wall | Cand/s | Lane work | Lanes held (mean of granted) | Fetch share of lane work | Packing floor | Loop-thread CPU (share of wall) | Mark lag p50 / p95 / max |
|---|---|---|---|---|---|---|---|---|
| `sync` (40, urllib in threads) | 70.93 s | 42.8 | 2,309.8 s | 32.7 of 40 | 55.6% | 57.7 s | 5.08 s (7.2%) | 0.15 / 0.43 / 271 ms |
| **`async` (40, shipped)** | **51.58 s** | **58.9** | 1,428.5 s | 27.8 of 40 | 79.5% | 35.7 s | 25.66 s (49.7%) | 0.22 / 4.59 / 532 ms |
| `async-80` (80 / 32) | 34.57 s | 87.9 | 1,390.7 s | **40.5 of 80** | **93.1%** | 17.4 s | 25.12 s (**72.7%**) | 1.07 / 12.37 / 508 ms |

Three readings, in the order the question asked for them.

- **Per-candidate work is not the wall.** At 80 lanes the non-fetch slice is 96.7 s of 1,390.7 s
  (**7.0%**); at 40 lanes it is 20.6%. What is left after a fetch is URL building, the HTML/JSON
  parse and the score — and the `sync` arm prices the *probe's own* CPU at **5.08 s for all 3,038
  candidates (~1.7 ms each)** by running the identical probe code with a fetcher that does its
  reading in worker threads. The probe's own work is 1.7 ms of a 470 ms lane.
- **Host latency is where the time is.** 93.1% of lane work sits inside a fetch's own window at 80
  lanes (79.5% at 40), 3,153 fetches for 3,038 candidates (1.04/candidate) at a mean 410 ms and p50
  251 ms per candidate. Nothing else in the phase is large. The lever on this is the number of
  fetches, not their speed: the 1,698 HTTP-404 candidates carry **750.9 s of the 1,390.7 s** lane
  work (54%), and dropping them from the replay prices the phase at 8.0 s of lane work at 80 lanes.
- **The loop is not blocking, but it is the next wall.** Lag stays in the milliseconds: p50 0.22-1.07
  ms, p95 4.6-12.4 ms, over 250 ms in 0.06-0.10% of 1,000 marks, and the ~0.5 s maxima are the only
  marks in their band (0.0% over 1,000 ms) rather than a pattern. But
  loop-thread CPU is **25.12 s / 25.66 s in both async arms** — the same absolute cost at 87.9 and
  58.9 candidates/s, i.e. **8.4 ms of loop CPU per candidate**, of which the `sync` arm shows ~1.7 ms
  is the probe and ~6.7 ms is the HTTP client's own response work (httpx reads and decodes on the
  loop; the urllib path does that in the pool). At 100% duty that is ~121 candidates/s — 1.4× the
  80-lane arm, which is exactly why that arm's wall tracks its **72.7%** duty.

## The cap nobody configured: pool permits held by bucket queues

Lane windows cannot see this, so `probe_permits_bench.py` instruments the semaphores themselves and
stamps each candidate four times — submitted, pool permit, bucket permit, done. "Probing" is the
window in which **both** permits are held; the difference is a candidate holding a pool permit while
it waits for its own bucket.

`_probe_one` in `src/source_discovery/probe_runtime.py` acquires them in this order:

```python
async with total_sem:        # the pool: the wave's lane budget (40)
    async with bucket_sem:   # per-family politeness: static 16, provider 40, teamtailor 15
```

which means a saturated bucket consumes pool permits from *other* buckets. On the shipped config:

| Order (`total = 40 / static = 16`) | Wall | Pool permits held | Probing | Permits held by a blocked candidate | Provider probe lanes | Static probe lanes |
|---|---|---|---|---|---|---|
| shipped (pool, then bucket) | 47.6 s | 36.0 of 40 (84.1% of span at exactly 40) | 29.2 | **6.8 (19% of pool-permit time, 323.2 s)** | 21.0 | 8.1 |
| bucket, then pool | **41.9 s** | 32.1 of 40 | 32.1 | **0** | 22.2 | 9.8 |

Every blocked permit measured here belongs to the **static** bucket (`static 323.2 s`, `provider 0.0
s`, `teamtailor 0.0 s`), and the shapes line up: 602 static candidates at a 16-lane cap means a
static batch arrives **deeper than 16**, the surplus holds pool permits while it queues, and since
`provider`'s own cap (40) equals the entire pool, provider cannot start at all until a static probe
retires. The `16`-total-lane plateau in the lane histograms is exactly that: 16 static probing plus
~24 static queueing, 40 permits gone, provider at zero. Averaged over the phase, provider — 80% of
the population — ran at **21.0 of its 40 permitted lanes**; the static bucket's wait reads mean
106 ms, p90 502 ms, max 1,460 ms.

The swap fixes the leak without touching a cap: the pool stays the binding resource (pinned at 40 for
69-84% of the span in both orders), the provider and static caps are unchanged, and no host sees a
higher peak per family. Here it was worth **−12% to −16%** of the phase wall on three paired runs
(47.6 → 41.9, 44.9 → 39.7, 49.8 → 41.7 s), reached by text-patching the shipped function
(`assert source.count(original) == 1`, so a drifting acquire order fails loudly).
**It has since shipped**, and been re-priced on the transport that replaced the async client: three
balanced pairs on the shipped path read **54.2 → 36.4**, **42.9 → 37.7**, **45.6 → 36.3 s**, with the
blocked-permit share of pool-permit time falling to zero and provider probe lanes rising to 24.0 —
see [`discovery-probe-permit-order-2026-10-12.md`](discovery-probe-permit-order-2026-10-12.md). The
numbers above are the pre-transport pair and are not comparable with those; the direction is.

## What it does not fix, in order of size

1. **The pool is the real lane count, and it is 40 while the buckets sum to 71.** With the ordering
   fixed, the phase is pool-bound (32.1 of 40 held), not bucket-bound: provider still averages 22.2 of
   the 40 lanes its own cap allows. Re-sizing the pool against the mix (16 + 40) would put in-flight
   near the buckets' sum, which is a **politeness** question and not a free win: the 40 provider lanes
   are 40 concurrent requests to four ATS hosts, and 238 URLs in this population already 429 *today*.
   (The identical 429 count across the 40- and 80-lane arms is **not** evidence that load does not
   matter: the provider cap was 40 in both, so both arms submitted the same load to those hosts.)
2. **Then the loop**, at ~121 candidates/s (~25 s for this population) for the reason above — the
   first constraint that costs *code*, not configuration.
3. **The fetch count** — 56% of the population is stable 404s carrying 54% of the lane work. This is
   the only lever on the currency itself, it relieves the loop in proportion, and it is a **product**
   decision (how fresh must a negative probe be?) rather than a performance one. This bench replays a
   frozen full population, so it overstates how often a production run re-probes the same 404; the
   freshness policy in the live candidate flow is the thing to measure before pricing it.

## Reproducer

```bash
# phase clock, occupancy, loop lag/CPU (three arms; each ~35-70 s, live network)
python _out/perf-runs/discovery/probe_wall_bench.py --arm sync
python _out/perf-runs/discovery/probe_wall_bench.py --arm async
python _out/perf-runs/discovery/probe_wall_bench.py --arm async-80

# permit attribution and the priced ordering swap (default httpx path)
PYTHONPATH=. python _out/perf-runs/discovery/probe_permits_bench.py --total 40 --static 16 --order shipped
PYTHONPATH=. python _out/perf-runs/discovery/probe_permits_bench.py --total 40 --static 16 --order bucket-first
```

## Limits

- The audit's probe phase is **three 1,000-candidate batches** (87.1 / 96.0 s re-recorded, 28.2-38.5 s
  each), while this bench replays the same population as **one** batch (47.6 s at the shipped order on
  the instrumented run). The bench's numbers are therefore a *floor* on the audit's phase: whatever
  three sequential batches cost in ramp and tail is real and is **not** decomposed here.
- Everything above is a **live replay of a recorded full population**, not a hermetic bench: the
  shipped arm alone measured 44.9 / 47.6 / 49.8 / 51.6 s across runs, so the ordering delta is
  quoted as a range over three pairs rather than a single number, and the provider/static lane means
  move ~5% between runs.
- The ordering fix is priced **in the bench only**. Nothing under `src/` changed, so the source guard
  and the lane contracts do not cover it; a real change needs its own test (an ordering contract that
  a blocked bucket cannot hold a pool permit) rather than a re-run of this bench.
- The loop ceiling (8.4 ms/candidate → ~121 candidates/s) is **derived**: it is measured CPU per
  candidate times duty cycle, never measured at a throughput where the loop actually saturates.
- Wall-clock questions like this drift with the network and with the four ATS hosts' behaviour, so the
  three candidate answers are ranked by *shape* (93% fetch / 7% work / 1.07 ms lag), and the ordering
  fix is the only claim that rests on a paired comparison.

## Earlier

This follows [`discovery-directory-lane-ceiling-2026-10-12.md`](discovery-directory-lane-ceiling-2026-10-12.md),
which made the lane count honest by repairing the shared-executor cap — the probe wave went from
pinned at 20 in-flight to peaks of 36/34 of its configured 40 with no plateau. That repair proved the
lane count was no longer the cap; it did not say what took its place, because the audit's probe phase
spends **87,065 / 96,049 ms** (`timings.totalsMs.probeMs` in `_out/perf-runs/discovery/audit-runs/audit-lane-ceiling{,-first}.json`,
where an earlier pair reads 86,932 / 87,940) in work whose composition had not been decomposed. This
snapshot does that decomposition. The two share one instrument family: occupancy from per-request
timestamps, computed from the phase's own artifacts.

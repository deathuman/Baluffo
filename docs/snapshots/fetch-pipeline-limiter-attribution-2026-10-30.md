# Fetch pipeline — neither the event loop nor the per-host gate is the bottleneck; the gate is a bounded, concentrated tax

**Date:** 2026-10-30 · **Lane:** fetch-pipeline, static sources · **Status:** measurement only; no
knob changed.

The 2×2 block ([`fetch-pipeline-inline-detail-fanout-2x2-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-2x2-2026-10-30.md))
ended on a scaling anomaly — doubling workers bought only **+18.6 %** throughput while inflating
per-source latency **+66 %** — and named three candidates: the single event-loop thread, the
per-domain gate of six, and the browser fallback pool. This block measures the first two directly
on the full population.

**Neither is the limiter.** The loop thread carries no more work than any other thread (top thread
**14.2 % / 8.5 %** of process CPU, none above 50 %), and Python as a whole uses **1.135 / 1.350
cores of 16**. The gate is real — **10.5 % / 13.4 %** of thread capacity, and the only candidate
that *grows* with concurrency — but removing it entirely would recover at most ~13 % of the
16-worker wall. What is left is round-trip latency.

The block also found a **measurement defect**: the product's own `staticDomainGateWaitMs` recorded
only **4.5 % / 6.1 %** of the gate wait the process paid.
[It is fixed in tree with this note](#the-instrument-why-it-read-low-and-what-changed), and the
corrected reading of every gate figure below is in that section.

## Design

Two arms, no knob between them, because a saturation reading is not a treatment contrast:

| slot | arm | workers | flag | lanes |
|---:|---|---:|---|---:|
| 1 | `y1-w8` | 8 | `0` | 10 |
| 2 | `y2-w16` | 16 | `0` | 10 |

Full 2 508-row population, the same aged seed as every block since 2026-10-29
(`649c7f53…23aef`), one arm at a time. Both arms are valid: exit 0, 2 508 executed rows, pinned
seed, non-zero exchanges.

**Instruments new to this block**, all driver-level with the product tree unmodified:

1. **Main-process CPU only** (`psutil.Process.cpu_times()` deltas), excluding browser children —
   the 2×2 arms recorded only a tree total, so a ~1-core process was invisible inside it.
2. **Browser-child CPU separately.**
3. **Per-thread CPU** (`Process.threads()` deltas) — the discriminator between a serialized
   dispatcher thread and work spread across workers.
4. **A proxy around the gate semaphore**, timing every `acquire()`. The product books its wait
   inside the per-source stats path, which the error path skips (501–828 error rows per arm here).

One pre-registration correction was recorded *before* results: I first read
`fetch_pages_batched`'s `asyncio.run(...)` as proof that each batch owns its own loop, which would
have retired the single-loop hypothesis by fiat. It does not — the prefetcher passes `sync_fetch`
only, and every such fetch re-enters `fetch_text_static_limited` → `_FetchTextLimiter` →
`fetch_text_impl`, which is the one `AsyncHttpTextFetcher` built by `resolve_fetch_text_impl`
(one `new_event_loop()` on a daemon thread, one client, pool `max(48 × 2, 48)` = 96). The earlier
notes' "16 × 6 = 96" arithmetic stands.

## Arms

| arm | workers | wall s | throughput /s | kept | failed | pages | requests | Python cores | child cores | top thread share | gate wait | gate ÷ capacity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `y1-w8` | 8 | 1 579.5 | 1.588 | 6 951 | 503 | 3 468 | 9 718 | **1.135** | 0.003 | **14.2 %** | 1 330 s | **10.5 %** |
| `y2-w16` | 16 | 1 394.5 | 1.798 | 7 055 | 501 | 3 265 | 9 429 | **1.350** | 0.004 | **8.5 %** | 2 985 s | **13.4 %** |

Thread spread: 1 578 / 1 920 distinct thread ids observed, mean live threads 27.0 / 43.5, **zero
threads above 50 % duty**, 7 / 1 above 10 %. There is no dispatcher thread carrying the work.

Contrast 16 ÷ 8: **throughput +13.2 %**, wall −11.7 %, **Python CPU +18.9 %**, gate wait **+124 %**
on **4.6 % fewer** acquires.

## The gate: bounded, concentrated, and under-reported

| | 8 workers | 16 workers |
|---|---:|---:|
| acquires timed | 4 709 | 4 491 |
| total wait | 1 330 s | **2 985 s** |
| mean wait per acquire | 282 ms | **665 ms** |
| worst single wait | 39.6 s | **131.6 s** |
| product `staticDomainGateWaitMs` | 60.4 s | 182.0 s |
| product ÷ proxy | **4.5 %** | **6.1 %** |

Three readings follow. The gate's cost is **bounded**: at 13.4 % of thread capacity, deleting the
gate entirely buys at most ~13 % of the 16-worker wall. It **scales with concurrency** and nothing
else here does: total wait more than doubled while acquires fell slightly, so it is queueing, not
extra traffic. And it is **concentrated**: the top hosts by exchanges are shared ATS estates, not
individual studios — `careers.playstation.com` 429 exchanges, `careers.wbd.com` 422,
`chessiverse.com` 257→131, `www.linkedin.com` 113→104, `careers.activision.com` 80, `jobs.ea.com` 70.
Sony's, Warner's and Activision's studios are many *sources* behind one host, so a 6-slot per-host
gate serializes them by construction — which is exactly what the gate is for.

### The instrument: why it read low, and what changed

`_FetchTextLimiter.__call__` timed every admission and then booked it inside
`_update_running_source`, which returned *before* the booking unless the **calling thread** carried
`thread_local.source_name` and that name was in `task_rows`. Static listing and detail fetches do
not run on the source's own thread: the lanes execute `sync_fetch` on the `ThreadPoolExecutor` that
`fetch_pages_batched` owns, and a `threading.local` does not cross a thread boundary. Those calls
arrived with an empty name and their wait was discarded, so the record only ever held the minority
of admissions made on the source thread. Two smaller leaks pointed the same way: the timed window
opened *before* `_mark_waiting`, charging each attributed call its own task-state write, and
admissions that waited under a millisecond were skipped, so the count was not an acquire count.

Fixed in tree with this note: booking is unconditional (an aggregate total plus an `unattributed`
bucket, so no admission can vanish), `execute_loader` binds each source's limiter with
`for_source(name)` before counting, and the window brackets `gate.acquire()` only. A re-run should
read close to the proxy for the namespaces both cover; the proxy stays the independent check. The
aggregate is readable in-process as `_baluffo_gate_wait_totals()` but is not persisted — a future
arm should read it after the run to prove attribution is complete (a non-zero `unattributed`
bucket is the signal that a call site still escapes the binding).

**Post-fix check, 12 boards, same driver and proxy**
(`_out/fetch-investigation/gate-instrument-smoke-20261030/`, 8 workers, 65.8 s wall, 241 kept,
2 failed): the product booked **551** admissions for **666 792 ms** and the proxy timed **551**
acquires for **666 826 ms** — count equal, time within **34 ms**, `unattributed` **0**. Two
consecutive runs reproduced both figures. Every admission is booked now; the loss the old path
caused can only have come from calls whose thread carried no source. (The millisecond total is a
wait *sum* across workers, not a share of that wall — see P3.)

**Confirmed at full population, 2026-10-31**
([`fetch-pipeline-gate-remeasure-2026-10-31.md`](fetch-pipeline-gate-remeasure-2026-10-31.md)): the
exact pair below re-run on the fixed build books **4 721 / 4 517** admissions against the proxy's
**4 721 / 4 517**, time within **−0.01 %**, unattributed **0**, and the persisted report recovers
**99.8 % / 99.4 %** of the proxy total instead of 4.5 % / 6.1 %. The eight-worker share and tail
replicate; the sixteen-worker arm landed in a slow window and is a regime reading, not a
replication.

**Corrected reading of these arms.** The recorded **60.4 s / 182.0 s** are floors — the waits the
instrument could attribute — not the gate's cost. The paid total is the proxy's **1 330 s /
2 985 s**, which is itself a floor for the whole pipeline because it counts `_FetchTextLimiter`
and not the async listing lane's own batch semaphores. "4.5 % / 6.1 % booked" is therefore a book
*rate*, not a share: capacity-normalized, the gate is **10.5 % / 13.4 %** of thread time.

## Predictions against outcomes

| # | prediction | outcome |
|---|---|---|
| P1 | Python CPU 0.80–1.20 cores in **both** arms | **fails** at 16: 1.135 and **1.350** — a ~1-core interpreter plus C-level parallelism, not a hard 1.0 ceiling |
| P2 | browser-child CPU ≤ 0.50 cores | **holds**, emphatically: **0.003 / 0.004** |
| P3 | gate wait ÷ wall < 2 % | **fails as written** (84 % / 214 %) — the metric was mis-specified: a multithreaded wait sum is not a fraction of wall. Capacity-normalized it is **10.5 % / 13.4 %** |
| P4 | one thread ≥ 50 % of process CPU, duty ≈ cores | **fails** decisively: top share **14.2 % / 8.5 %**, no thread above 50 % |
| P5 | throughput 16 ÷ 8 in 1.10–1.25 | **holds**: **1.132** |
| P6 | kept within ±3 % of the 2×2 same-config arms | **marginal miss**: +2.0 % (8 w) and **+3.5 % / +5.4 %** (16 w) against that block's 6 814 / 6 694 |

P3's failure is mine, not the pipeline's: I wrote the ratio without dividing by the worker count,
so a quantity that is bounded by capacity was compared to wall. The number it was meant to
produce — the gate's share of available thread time — is in the table above, and it is the one
this block uses.

## What this settles

- **The single event-loop thread is not the bottleneck.** No thread in the process carries more
  than 14 % of the CPU, and the process uses 1.35 of 16 cores while running 16 workers. A
  serialized dispatcher would look like one thread at ~1.0 core and a flat ceiling; neither is
  present.
- **The per-host gate is not the bottleneck either**, but it is the only concurrency-scaling tax
  left in the measurement: 10.5 % → 13.4 % of capacity, +124 % total wait for 2× workers, worst
  single wait 131.6 s. It is concentrated in shared ATS hosts, where 6 slots per host serializes
  a dozen studios.
- **The process is not CPU-bound** (1.35 of 16 cores) and the client pool is not the constraint in
  this configuration (flag off: ~16–32 desired in-flight against a 96-connection pool). What is
  left is **round-trip latency**: fewer pages were traversed at 16 workers (3 468 → 3 265) as more
  sources hit their per-source budget, so the extra workers partly bought truncation rather than
  throughput.

## Next levers, in the order this evidence supports

1. ~~Fix the gate instrument~~ — **done in tree with this note**, and **confirmed at full population
   on 2026-10-31** ([re-measurement](fetch-pipeline-gate-remeasure-2026-10-31.md)): every admission
   books, per source where the binding exists and in the aggregate otherwise, and product matches
   proxy to the unit. The 8-worker share replicated (11.7 %); the 16-worker share is
   regime-dependent (5.1 % in a three-times-slower window), so a single "13 %" is not a constant.
2. **Per-host gate keyed by tenant, not `netloc`, for the shared ATS estates** — if
   `careers.playstation.com` is really N studios, six slots is a queue for N× traffic. This is a
   politeness trade with the host and needs its own measured arm plus a 429 check, not a silent
   raise.
3. **Per-request latency attribution** (DNS, connect, TTFB, body) on the same population. With CPU
   and gate both bounded, that is where the 1 400 s actually goes.

## What this cannot show

- One arm per level. The CPU and thread readings are saturation readings and survive a window
  shift; the +13.2 % throughput is both arms in one window and is not a curve.
- The gate proxy times `_FetchTextLimiter` only. The async listing lane and the prefetcher's own
  batch semaphores are separate and not in the 4 491–4 709 acquires, so the true gate share is
  ≥ the figure quoted, not exactly it.
- Per-thread CPU across 1 578–1 920 thread ids includes pool churn; the top-share figure is robust
  to that (it is a ratio to process CPU), but "mean live threads" is not a concurrency measure.
- Browser children measured ~0 CPU here, which is at odds with the playwright fallback lines in
  the arm logs — the sampler may miss short-lived children. Nothing in the conclusions depends on
  it, but the browser path is not exonerated by this block, only unmeasured.

## Replication

```
python _out/fetch-investigation/fetch-limiter-20261030/study.py --dry-run
python _out/fetch-investigation/fetch-limiter-20261030/study.py    # 2 arms, ~48 min

# post-fix instrument check: 12 boards, ~66 s, prints booked totals next to the proxy's
python _out/fetch-investigation/gate-instrument-smoke-20261030/driver.py \
    --data-dir .tmp/gate-instrument-smoke \
    --sources-from _out/fetch-investigation/gate-instrument-smoke-20261030/sources.txt
```

Artifacts: `_out/fetch-investigation/fetch-limiter-20261030/` (design, driver, arm logs,
`manifest.json`); arms under `.tmp/study-fetch-limiter-20261030/arms/` with `arm.json`,
`gatewait.json` and `reqcount.json`. The post-fix check keeps its driver, source list and both
receipts in `_out/fetch-investigation/gate-instrument-smoke-20261030/`. The aborted first attempt is kept at
`.tmp/study-fetch-limiter-20261030/arms-void/y1-w8-firstattempt/` — its driver bug (a thread-CPU
accessor reading `.user` instead of `.user_time`) made every thread sample silently skip.

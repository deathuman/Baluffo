# Fetch pipeline — the gate instrument is now exact at population scale; the gate's *share* is a property of the latency window

**Date:** 2026-10-31 · **Lane:** fetch-pipeline, static sources · **Status:** measurement only;
no knob changed.

The limiter block ([`fetch-pipeline-limiter-attribution-2026-10-30.md`](fetch-pipeline-limiter-attribution-2026-10-30.md))
found that the product recorded only **4.5 % / 6.1 %** of the gate wait the process paid, fixed the
instrument in tree, and left one thing unproven: a 12-board check had shown product and proxy
agreeing, but not at **full population**, where the lanes, the fallbacks and ~500 error rows per
arm all run.

This block closes that gap by re-running the exact pair the old figures came from, on the fixed
build. **The instrument is now exact**: product and proxy agree on the admission **count to the
unit** and on the time to **−0.01 %**, with a **zero** unattributed bucket, in both arms. The
report the pipeline writes recovers **99.4–99.8 %** of the gate wait, against 4.5–6.1 % before.

The second reading is less convenient: **the gate's share of capacity is not a constant.** At
eight workers it replicates — **11.7 %** against 10.5 % — with the worst single wait **43.3 s**
against 39.6 s, on the same host. At sixteen workers the arm landed in a window that was **three
times slower per source** (+75 % wall at identical traffic), and there the gate's share **fell to
5.1 %** and the tail to **33.1 s**: when raw request latency triples, the fixed worker pool issues
admissions more slowly, fewer requests stack up behind any host's six slots, and the gate stops
being the binding constraint. A single "13 %" would have hidden that.

## Design

Two arms, no knob between them, replicating the 2026-10-30 schedule exactly: same population, same
aged seed (`649c7f53…23aef`), same flag (`BALUFFO_STATIC_INLINE_DETAIL_FANOUT=0`), same lanes (10),
8 then 16 workers.

| slot | arm | workers | flag | lanes |
|---:|---|---:|---|---:|
| 1 | `r1-w8` | 8 | `0` | 10 |
| 2 | `r2-w16` | 16 | `0` | 10 |

**Instruments**, both driver-level, `src/` untouched:

1. **Gate proxy**, unchanged in what it counts from the 2026-10-30 block — every `acquire()` on
   every semaphore `_domain_gate` hands out — so its totals remain the reference. Extended only to
   carry the **host** of the gate it wraps, which lets the worst single wait name an estate.
2. **Product totals** — `_baluffo_gate_wait_totals()` read from every limiter the pipeline builds
   and written at exit. This is the fixed instrument's own number and the arm report does not
   persist it in this form (the report persists the per-source sum, quoted separately below).
3. Physical-request counter, arm report, per-source attribution: unchanged.

**Gate share** is capacity-normalised: `total wait ÷ (process wall × workers)`. The wait total is a
sum across threads, so worker-seconds is the only denominator it can be a fraction of.

## Validity

Both arms: exit 0, **2 508 / 2 508** executed rows, pinned seed, non-zero exchanges.

| | `r1-w8` | `r2-w16` |
|---|---:|---:|
| wall s | 1 448.5 | 2 438.4 |
| source work s | 11 380.7 | **37 168.1** |
| throughput /s | 1.731 | 1.029 |
| pages | 3 480 | 3 203 |
| requests | 9 740 | 9 492 |
| kept | 7 114 | 7 094 |
| failed | 507 | 554 |

Traffic is effectively identical between the arms — acquires **4 721 / 4 517**, requests
**9 740 / 9 492**, kept **7 114 / 7 094** — while source work per row goes from **4.54 s** to
**14.82 s**. The 16-worker wall is not carrying more work; it is waiting longer for the same work.

## The instrument: exact, at population scale

| | `r1-w8` | `r2-w16` |
|---|---:|---:|
| proxy acquires | 4 721 | 4 517 |
| product booked | **4 721** | **4 517** |
| count delta | **0** | **0** |
| proxy total ms | 1 357 475.7 | 2 002 738.3 |
| product total ms | 1 357 300 | 2 002 527 |
| time delta | **−0.01 %** | **−0.01 %** |
| unattributed calls / ms | **0 / 0** | **0 / 0** |
| report's persisted per-source ms | 1 355 188 | 1 990 477 |
| persisted ÷ proxy | **99.8 %** | **99.4 %** |

Before the fix the same comparison read **4.5 % / 6.1 %**. The residual 0.2–0.6 % is the part of
the aggregate that lands on rows with no task entry to attribute to — error rows — which is why
the aggregate exists and why the persisted per-source figure is quoted separately.

## The gate: same share at 8, a different regime at 16

| | `r1-w8` | `r2-w16` | 2026-10-30 `y1-w8` | 2026-10-30 `y2-w16` |
|---|---:|---:|---:|---:|
| gate wait s | 1 357.5 | 2 002.7 | 1 330.0 | 2 984.6 |
| acquires | 4 721 | 4 517 | 4 709 | 4 491 |
| mean wait per acquire | 288 ms | **443 ms** | 282 ms | **665 ms** |
| worst single wait | 43.3 s | 33.1 s | 39.6 s | **131.6 s** |
| **share of capacity** | **11.7 %** | **5.1 %** | 10.5 % | **13.4 %** |

The eight-worker reading replicates in both directions, total and tail. The sixteen-worker reading
does **not**, and the covariate table says why: this arm cost **14.82 s of source work per row**
against the earlier 16-worker arm's **8.64 s** (+72 %) and **3.27×** the 8-worker arm in this same
block, while its traffic was the same. The mechanism is a fixed pool meeting slower hosts — at
most 16 requests are outstanding, so tripling each request's latency cuts the admission rate and
empties the queues that form behind the six slots. The gate's absolute wait therefore *fell* by a
third and its tail by three quarters while the wall rose by three quarters.

### Where the gate's cost sits

`careers.wbd.com` is the estate behind the tail, and it is the same in both arms: **410 acquires**,
each time the largest single share of wait — **853.5 s / 62.9 %** at 8 workers and
**1 113.4 s / 55.6 %** at 16. The worst *single* wait is a `careers.wbd.com` admission in both
arms (43.3 s and 33.1 s). Behind it: `careers.playstation.com` 190.8 / 372.6 s,
`careers.king.com` 88.2 / 157.8 s, `unity.com` 53.8 / 92.6 s. These are shared ATS estates where a
dozen studios are one `netloc`, which is what a per-host gate is for and also why it queues.

## Predictions against outcomes

| # | prediction | outcome |
|---|---|---|
| P1 | product count = proxy acquires exactly; time within 1 % | **holds, twice**: 0 delta, −0.01 % in both arms |
| P2 | unattributed 0 in both arms | **holds**: 0 calls / 0 ms |
| P3 | share 9–12 % at 8, 12–16 % at 16, rising with workers | **fails at 16**: 11.71 % is in band, 5.13 % is below, and the ordering reverses — explained by the window, not the instrument |
| P4 | worst wait 25–60 s at 8, 90–180 s at 16, ratio > 2 | **fails at 16**: 43.3 s holds, 33.1 s is below band, ratio 0.77 |
| P5 | walls within ±10 %; acquires ±10 %; kept ±5 % | **fails on wall at 16** (+74.9 %; 8 w −8.3 %); acquires **+0.3 % / +0.6 %** and kept **+2.3 % / +0.6 %** hold |
| P6 | the tail sits on a shared estate, not a single studio | **holds**: `careers.wbd.com`, 410 acquires, > half of all wait |

P3 and P4 failing together, in the same arm, with P5 naming the wall as the broken covariate, is a
consistent picture: the share is a ratio to a wall the window inflated. It is not evidence that the
instrument is wrong, and P1 is the test that would have shown that.

## What this settles

- **The gate instrument can be quoted as exact.** At population scale the product books every
  admission the proxy times, to the unit, and 99.4 %+ of the wait reaches the persisted report.
  Every gate figure taken before 2026-10-30 remains a floor; every figure taken on this build with
  the aggregate read is not.
- **The 8-worker share and tail replicate**: 11.7 % of capacity and 43.3 s worst single wait
  against 10.5 % and 39.6 s.
- **The gate's share is regime-dependent, and smaller when latency dominates.** 13.4 % at 16
  workers in a fast window; 5.1 % in a window three times slower per source, at the same traffic
  and the same acquire count. Quoting one number as "the gate's cost" is wrong in both directions.
- **The gate is still not the bottleneck.** Even its highest observed share (13.4 %) leaves ~87 %
  of worker time elsewhere, and in the slow window it is ~5 %. The wall is dominated by per-request
  latency, which is where the next lever stands.

## What this cannot show

- Two arms in one window each, sequential. The 16-worker arm is a *valid* run, not a comparable
  one; it prices the regime, it does not estimate a treatment.
- The proxy covers `_FetchTextLimiter` only. The async listing lane and the prefetcher's batch
  semaphores are outside it, so every share here is a **floor** for the whole gate system.
- One worst-wait observation per arm. The host is named; whether that host is pathological or
  merely the busiest cannot be told from a single sample.
- No CPU or per-thread sampling in this block. The 2026-10-30 block's CPU readings stand as the
  saturation evidence; nothing here re-measures them.

## Replication

```
python _out/fetch-investigation/gate-remeasure-20261031/study.py --dry-run
python _out/fetch-investigation/gate-remeasure-20261031/study.py   # 2 arms, 65 min here
python _out/fetch-investigation/gate-remeasure-20261031/analyse.py
```

Artifacts: `_out/fetch-investigation/gate-remeasure-20261031/` (design, driver, analysis,
`manifest.json`, `progress.jsonl`, logs); arms under `.tmp/study-gate-remeasure-20261031/arms/`
with `gatewait.json`, `gateproduct.json`, `reqcount.json`, `run.run.json`, `run.report.json`.
The pre-registration is in that directory's `design.md`; the wall band (P5) was written before
either arm ran and is the reason the 16-worker share is reported as a regime reading rather than a
failed replication.

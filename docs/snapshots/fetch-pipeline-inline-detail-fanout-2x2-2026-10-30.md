# Fetch pipeline — 2×2 on the full population: the fan-out buys a cohort, not throughput, and worker count is past its knee

**Date:** 2026-10-30 · **Lane:** fetch-pipeline, static sources · **Status:** measurement only;
`BALUFFO_STATIC_INLINE_DETAIL_FANOUT` stays default **off**.

The 8-worker block ([`fetch-pipeline-inline-detail-fanout-workers-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-workers-2026-10-30.md))
read as a substitution: eight workers with the fan-out matched sixteen without it, so the flag
could be traded for worker parallelism. That reading rested on a 1 253-source half-sample whose
own middle arms had been caught in a slow window, and it had no same-cell replicate at all. This
block tests it on the **full population** — five arms, each cell once, closing the cycle on a
repeat of the opening cell so the block prices its own drift.

The substitution does not survive. Eight workers with the flag is **13.4 % slower** than the
16-worker cell that opened the block and **19.3 % slower** than the same cell that closed it,
while the block's own drift runs the *other* way (−4.9 %). The flag's cohort benefit is real and
replicates — **DiD −9.6 pp** at 16 workers and **−14.2 pp** at 8 — but it is worth about **−1 to
−2 %** of wall and nothing of throughput.

The larger finding is in the worker contrast, and it reframes where the next saving can come
from: **doubling workers buys +18.6 % throughput and costs +66 % per-source latency.** Both
concurrency levels are ~99 % packed, and per-worker productivity nearly halves. Worker count is
past the knee.

## Design — five arms, because four cells cannot bound their own drift

One run per cell leaves no same-cell replicate, which is exactly the blindness that made the
previous block's guard pass while its middle arms sat in a slow window. So the block adds a fifth
arm that repeats the opening cell:

| slot | arm | workers | flag | role |
|---:|---|---:|---|---|
| 1 | `x1-16-off` | 16 | `0` | reference cell |
| 2 | `x2-16-on` | 16 | `1` | flag at 16 |
| 3 | `x3-8-on` | 8 | `1` | worker change with the flag on; the headline cell |
| 4 | `x4-8-off` | 8 | `0` | flag at 8 |
| 5 | `x5-16-off` | 16 | `0` | worker change with the flag off, and the reference repeated |

Every adjacency is a single-factor contrast and slot 5 against slot 1 is the drift estimate. The
pre-registration ([`design.md`](../../_out/fetch-investigation/inline-fanout-2x2-20261030/design.md))
fixed the estimand, the bands and the verdict rules before arm 1 started, including a 7 200 s
wall cap; one amendment was recorded after arm 1 (the population file carries 2 530 loader names,
the pipeline executes **2 508** static rows, so validity is asserted on executed rows).

## Population and validity

Full static population, 2 530 names → **2 508 executed rows per arm**, the same count both earlier
blocks reported. Every arm starts from a byte-identical aged seed
(`jobs-source-state.json.gz`, sha256 `649c7f53…23aef`), so no arm is cheaper because another
warmed a cache. **All five arms are valid**: exit 0, 2 508 sources, non-zero exchanges, pinned
seed, under the wall cap.

Coverage is flat inside the block: kept rows **6 694–6 814** (±0.9 %), feed rows **3 631–3 752**
(±1.7 %), multi-card kept **4 620–4 741** (±1.3 %). Pages visited rose at eight workers
(**3 421 / 3 471** against **3 250–3 299**), a worker effect rather than a flag one: with less
contention, fewer boards exhaust the source time budget before finishing detail traversal.

## Arms

| arm | workers | flag | wall s | source work s | throughput /s | median source ms | pages | requests | kept |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `x1-16-off` | 16 | 0 | 1 444.4 | 22 832.4 | 1.736 | 4 586 | 3 274 | 9 513 | 6 814 |
| `x2-16-on` | 16 | 1 | 1 349.2 | 21 114.8 | 1.859 | 4 742 | 3 250 | 9 769 | 6 667 |
| `x3-8-on` | 8 | 1 | 1 638.3 | 13 067.5 | 1.531 | 2 515 | 3 421 | 10 106 | 6 759 |
| `x4-8-off` | 8 | 0 | 1 654.1 | 13 192.7 | 1.516 | 2 605 | 3 471 | 9 695 | 6 708 |
| `x5-16-off` | 16 | 0 | 1 373.7 | 21 640.5 | 1.826 | 4 641 | 3 299 | 9 532 | 6 694 |

## Contrasts

Ratios are left over right over matched, error-free, non-stalled sources; the cohort split is the
**right** arm's own `detail_pages_visited`. DiD is `median(multi) − median(control)` in pp.

| contrast | wall | source work | multi-card | control | **DiD** |
|---|---:|---:|---:|---:|---:|
| flag at 16 (`x2`/`x1`) | −6.6 % | −7.5 % | **−8.0 %** | +1.7 % | **−9.6 pp** [−15.3, −0.3] |
| workers, flag on (`x3`/`x2`) | +21.4 % | −38.1 % | −46.6 % | −37.9 % | −14.0 pp [−20.8, −4.1] |
| flag at 8 (`x3`/`x4`) | −1.0 % | −1.0 % | **−14.7 %** | −0.6 % | **−14.2 pp** [−19.0, −8.1] |
| workers, flag off (`x4`/`x5`) | +20.4 % | −39.0 % | −47.9 % | −34.7 % | −20.1 pp [−24.6, −12.6] |
| **drift** (`x5`/`x1`) | **−4.9 %** | −5.2 % | +7.5 % | −0.1 % | **+7.6 pp** [+2.0, +14.2] |

Three things follow directly.

**The flag is wall-neutral, not a speed-up.** At 16 workers it is −6.6 % wall, of which the block's
drift accounts for −4.9 % — a net **−1.7 %**. At 8 workers it is **−1.0 %**. Its cohort effect is
large and consistent in sign, but the 445 multi-card boards in the population (283 matched for the
paired read) do not set the makespan, and
the cost lands on the boards that share their host gate. Requests per logical page move from
1.034 at 16 workers to 1.058 at 8 — the pre-registered 0.95–1.05 band is missed by 0.8 pp at 8,
which is a marginal miss, not evidence of page duplication.

**The drift floor is bigger than the interaction it was supposed to test.** The same-cell
replicate shows the multi-card cohort drifting **+7.6 pp** relative to controls across the block
(CI excludes 0). P2 predicted the flag's DiD would be at least 5 pp more negative at 8 workers
than at 16; the measured difference is **−4.6 pp**, inside a 7.6 pp floor. So the claim that
"halving workers doubles the gain" is **not resolvable** by this block, and the previous block's
ordering (bigger DiD at 16 on the same names) was equally unresolvable. What *does* sit clear of
the floor is the cohort gain itself: −9.6 pp and −14.2 pp, each roughly 2× the drift.

The 16-worker reference arm carries **22 832 s** of source work across **1 444 s** of wall —
1 427 s per worker, **98.8 %** of the arm. There is no idle headroom to reclaim, which is why
adding workers cannot buy what it looks like it should.

## Predictions against outcomes

| # | prediction | outcome |
|---|---|---|
| P1 | drift wall ≤ 20 %, per-source median ≤ 10 % | **holds**: −4.9 % wall, +1.2 % median overall, +7.5 % multi |
| P2 | `DiD(F8) − DiD(F16)` ≤ −5 pp | **fails**: −4.6 pp, and inside the +7.6 pp drift floor |
| P3 | `DiD(F8)` ≤ −5 pp | **holds**: −14.2 pp, CI excludes 0 |
| P4 | `x3/x1` **and** `x3/x5` within 0.85–1.15 | **fails**: 1.134 (inside) and **1.193** (outside) |
| P5 | requests per page 0.95–1.05 | **marginal miss**: 1.034 at 16, **1.058** at 8 |
| P6 | multi-card kept within ±5 % of the earlier 16-worker block | **fails as written**: −10.5 % to −12.8 % |
| P7 | `DiD(F16)` ≤ 0 | **holds**: −9.6 pp, CI upper end −0.3 |

P6 needs the qualification, because the pre-registered rule says a failure invalidates the arm.
Its purpose is to catch a **coverage loss caused by an arm**; within the block, multi-card kept
rows move **±2.6 %** across five arms, so no arm lost coverage. The failure is against the earlier
block's number (5 299 multi-card kept), and that gap is shared by every arm — live boards turned
over ~11 % between the two blocks. The rule's cross-block comparison is therefore unusable as a
coverage test, and the contrasts stand on the within-block check. That is a defect in the rule as
written, not a rescue of the data.

## The number that matters more: worker scaling is nearly exhausted

Both levels are perfectly packed, so the wall is not idle time:

| | wall s | source work s | work ÷ workers | occupancy | throughput /s | per-worker /s | median source |
|---|---:|---:|---:|---:|---:|---:|---:|
| 8 workers (on / off) | 1 638 / 1 654 | 13 068 / 13 193 | 1 633 / 1 649 | **99.7 %** | 1.53 / 1.52 | 0.190 | 2 515 / 2 605 ms |
| 16 workers (on / off ×2) | 1 349 / 1 444 / 1 374 | 21 115 / 22 832 / 21 641 | 1 320 / 1 427 / 1 353 | **97.8–98.8 %** | 1.86 / 1.74 / 1.83 | 0.113 | 4 742 / 4 586 / 4 641 ms |

Doubling workers returns **+18.6 %** throughput (1.523 → 1.807 mean) while inflating every
source's own duration by **+66 %** (13 130 → 21 863 s of work) and cutting per-worker productivity
by 41 %. Since both arms are ~99 % occupied, the extra workers are not filling idle capacity —
they are queueing on something shared. The candidates are already named in this lane: the single
event-loop thread that carries every async request, the per-host static gate of six
(`DEFAULT_STATIC_FETCH_MAX_PER_DOMAIN`), and the browser fallback pool. This block does not
separate them, but it does rule out "not enough workers" as the constraint.

## Decision

- `BALUFFO_STATIC_INLINE_DETAIL_FANOUT` stays **off**. On the full population it is −1 to −2 % of
  wall, +3–6 % of requests per page, and no throughput. Its cohort benefit is real but confined to
  boards that are not the makespan.
- **16 workers stays.** Eight cost 13–19 % more wall for no coverage gain, though they do halve
  per-source latency and visit 4–5 % more detail pages — a latency/depth trade, not a speed-up.
- The next lever is the **shared fetch path**, not more concurrency: a lanes-only arm (so the
  6-slot gate, not the worker count, is the knob) or an arm that shards the event-loop thread.
  More workers, more lanes per board, or the fan-out at another worker count will not move the
  makespan.

## Correction, 2026-10-30 — the "~99 % occupied" reading, and the named candidates

The follow-up block ([`fetch-pipeline-limiter-attribution-2026-10-30.md`](fetch-pipeline-limiter-attribution-2026-10-30.md))
measured the candidates this note left open, and two things here need narrowing.

- **"Occupancy" is near-tautological.** `work ÷ workers ≈ wall` holds whenever every worker always
  has a source, which is the normal state of this pipeline — so 97.8–99.7 % is not evidence that
  no headroom exists. The real reading is the CPU one: the process uses **1.135 cores at 8 workers
  and 1.350 at 16, of 16 available**, and browser children ~0.003. It is not CPU-bound; it is
  waiting on round trips.
- **The three candidates are resolved.** The single event-loop thread is *not* the carrier: no
  thread exceeds **14.2 %** of process CPU (0 above 50 %). The per-host gate is real but **bounded
  at 10.5 % / 13.4 %** of thread capacity, and concentrated in shared ATS hosts
  (`careers.playstation.com` 429 exchanges, `careers.wbd.com` 422). The browser pool measured
  ~0 CPU, so it is unmeasured rather than exonerated.

## What this cannot show

- One replicate per cell. The cycle bounds drift end to end and prices its own DiD floor
  (+7.6 pp), but a within-block trend and an arm effect are still partly confounded — which is
  why anything inside ±7.6 pp is reported as unresolved rather than as a null result.
- `x3` against `x1`/`x5` compares different times as well as different cells. P1 licenses the
  comparison; it does not make it an interleaved pair.
- Only two worker levels. The 8 → 16 slope is measured twice; a curve is not.
- Worker count moves active boards and in-flight lanes together, so this block still cannot
  separate "fewer boards" from "less contention per board".
- Aggregates are the production wall for this population and these lanes on this machine today.

## Replication

```
python _out/fetch-investigation/inline-fanout-2x2-20261030/study2x2.py --dry-run
python _out/fetch-investigation/inline-fanout-2x2-20261030/study2x2.py      # ~2 h, 5 arms
python _out/fetch-investigation/inline-fanout-2x2-20261030/analyse_2x2.py   # writes analysis-2x2.json
```

Artifacts: `_out/fetch-investigation/inline-fanout-2x2-20261030/` (design, driver, analyser,
`analysis-2x2.json`, `progress.jsonl`, per-arm logs); arms under
`.tmp/study-inline-fanout-2x2-20261030/arms/` (each with `run.report.json`, `arm.json`,
`reqcount.json`). Nothing in this block writes repo `data/`.

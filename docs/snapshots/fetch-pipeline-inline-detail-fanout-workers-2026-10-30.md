# Fetch pipeline — the inline detail fan-out at 8 workers: the cohort gain survives, the makespan does not improve

**Date:** 2026-10-30 · **Lane:** fetch-pipeline, static sources · **Status:** measurement only;
`BALUFFO_STATIC_INLINE_DETAIL_FANOUT` stays default **off**.

The 16-worker block ([`fetch-pipeline-inline-detail-fanout-live-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-live-2026-10-30.md))
put the flag **+2.55 % / +4.35 %** on wall, with a consistent cohort split: the multi-card boards
it can reach ran faster while boards it cannot touch ran 10–28 % slower. This block asked whether
that penalty was the cost of asking for too much concurrency: **four arms at 8 workers**, same
build, flag the only variable.

The answer is yes on the mechanism and no on the payoff. At 8 workers the unreachable cohorts
stop paying and the multi-card gain **doubles** (−14.5 % against −7 %) — but eight workers with
the fan-out only match what sixteen workers already do serially, so the pipeline's makespan does
not move.

## The contention, corrected

The 16-worker note first said "16 × 10 lanes oversubscribe a 48-connection fetcher". Both numbers
were wrong, and the corrected shape is what makes 8 workers the interesting level:

- Static fetches use `fetch_text_static_limited`, whose gate is
  `DEFAULT_STATIC_FETCH_MAX_PER_DOMAIN = 6` — so lanes are capped at six concurrent per host,
  whatever `static_detail_concurrency` says.
- `AsyncHttpTextFetcher`'s pool is `max_connections = max(48 × 2, 48)` = **96**, not 48.
- Hence 16 workers × 6 = **96**: the pool is reached exactly, not oversubscribed fourfold. The
  surplus is not pool queueing — `_InlineDetailPrefetcher.window` asks `fetch_pages_batched` for
  `per_host_concurrency = len(planned)` = 10 and lets the gate limit it, so **four threads per
  active board block in `BoundedSemaphore.acquire()`** while one event-loop thread carries every
  async request.

Gate wait itself never rose: **1.2 % / 0.9 %** of static detail time in the 16-worker on arms
against **3.2 % / 3.5 %** off. The fan-out made the gate cheaper to use and paid in blocked
threads and GIL pressure — which is why halving the worker count is the right probe.

## Population and design

Full population at 8 workers is ~47 min per arm, too long to counterbalance in one window. The
population is a **seeded 50 % sample of each detail cohort** (cohort taken from the 16-worker
`off-1` arm, where it is a property of the board, not the flag):

| cohort | population | sampled |
|---|---:|---:|
| multi-card (`detail_pages_visited ≥ 2`) | 445 | 222 |
| single-card | 426 | 213 |
| no detail | 1 637 | 818 |
| **total** | **2 508** | **1 253** |

`random.Random(20261030)`, output in the pinned `sources.txt` order. Four arms, strictly
sequential, `--workers 8`, lanes at the shipped 10, per-host gate 2 for adapters and 6 for static,
`BALUFFO_STATIC_FAILURE_BUDGET_S` unset. At 8 workers the static ceiling is **48** — half the pool
— and the blocked-thread population at most **32**, half the 16-worker block's 64.

## The trap this block fell into, and the estimator that survives it

Raw wall says the flag is **−64.7 % / −67.4 %**, with tight same-side replicates (2.1 % and 5.6 %).
That number is not the flag. Two of the three cohorts **cannot be touched by the change at all**:

- `_inline_detail_prefetcher` returns `None` below two rendered cards, so a single-card board has
  nothing to overlap;
- a board with zero detail pages has no detail work whatsoever.

Both moved as far as the multi-card cohort (sums **−61.5 %** and **−59.0 %** in pair 1, **−61.1 %**
and **−52.5 %** in pair 2, against the multi cohort's **−59.8 %** and **−44.5 %**). A contrast that
moves an untouched cohort is common mode: the block ran through the documented slow regime — one
board (`lanterns-studios.com`) held a worker for **1 182 s** against a 90 s budget — and the two
middle arms took the worst of it. The pre-registered P1 (same-side spread ≤ 30 %) passes here and
still could not catch it, because same-side replicates are adjacent in time inside the same
regime. **P1 is not a guard against a window that covers the middle of an ABBA block.**

What survives is a paired, cohort-controlled contrast: per source, the log duration ratio
between the arms; medians by cohort; and the **difference in differences** (multi-card minus the
pooled single-card and no-detail cohorts), bootstrapped over sources. Common-mode environment
cancels inside each pair, so the same estimator can also be applied to the 16-worker block
restricted to these same 1 253 names — the two worker counts then compare like for like.

## Result

| arm | flag | wall | work | rows | req | pages | stalls ≥300 s | threads mean/max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `on-1` | 1 | 722.9 s | 5 763.0 s | 2 199 | 5 010 | 1 711 | 0 | 42.0 / 188 |
| `off-1` | 0 | 2 047.5 s | 15 987.3 s | 2 783 | 4 729 | 1 634 | 3 | 27.4 / 167 |
| `off-2` | 0 | 2 169.3 s | 15 499.3 s | 2 276 | 4 684 | 1 451 | 2 | 112.4 / 209 |
| `on-2` | 1 | 708.2 s | 5 625.1 s | 2 152 | 4 853 | 1 391 | 0 | 110.0 / 208 |

Same-side wall spread 2.1 % (on) and 5.6 % (off); work spread 2.5 % and 3.2 %.

**Paired median per source, and the difference in differences:**

| pair | multi-card | single | no detail | control | **DiD** |
|---|---:|---:|---:|---:|---:|
| 8 w, `on-1 − off-1` | **−14.4 %** [−24.8, −8.3] | −2.9 % | −3.2 % | −3.2 % | **−11.6 pp** [−23.2, −3.4] |
| 8 w, `on-2 − off-2` | **−14.9 %** [−19.9, −8.0] | −1.0 % | −4.2 % | −3.8 % | **−11.6 pp** [−17.0, −3.1] |
| 16 w, same names, pair 1 | −6.9 % [−12.8, −1.9] | +16.6 % | +15.5 % | +15.9 % | −19.7 pp [−25.1, −13.8] |
| 16 w, same names, pair 2 | −7.3 % [−13.6, +1.8] | +11.9 % | +8.3 % | +9.7 % | −15.6 pp [−21.6, −6.6] |

Three things follow, and they replicate in both time orders:

1. **The mechanism works at both counts.** Multi-card boards gain −6.9 % / −7.3 % at 16 workers
   and −14.4 % / −14.9 % at 8. The gain is not noise: all four CIs sit below zero.
2. **The penalty is concurrency.** The unreachable cohorts pay **+15.9 % / +9.7 %** at 16 workers
   and **−3.2 % / −3.8 %** at 8 — inside the pre-registered ±5 % band, and the sign flips.
3. **The typical board flips from loss to win.** Paired median over *all* matched sources:
   **+10.0 % [+6.8, +13.3] / +5.8 % [+2.9, +9.7]** at 16 workers, **−4.6 % [−8.8, −1.9] /
   −4.9 % [−8.7, −2.1]** at 8.

## Why the makespan still does not improve

Per-source throughput across the two blocks:

| configuration | sources | wall | sources/s |
|---|---:|---:|---:|
| 16 workers, flag off | 2 508 | 1 389.6 s | 1.805 |
| 16 workers, flag on | 2 508 | 1 425.0 s | 1.760 |
| 8 workers, flag **on** | 1 253 | 722.9 s | **1.733** |
| 8 workers, flag off | 1 253 | 2 047.5 s | 0.612 |

Eight workers with the fan-out run at **1.733 sources/s** against sixteen workers' **1.805** — the
same throughput, to 4 %, in half the workers. The fan-out does not add capacity; it **substitutes
for worker parallelism**. On the makespan that matters to a batch run, enabling the flag and
halving the workers is a wash.

That is also why the 8-worker aggregate looked like a 3× win: the arm was **tail-bound**. With
eight workers a 300 s board is 3/8 of capacity for five minutes, so compressing it shortens the
run; with sixteen it is 1/16, and the other workers simply keep going while the extra threads
contend. The heavy tail really did collapse (`playstation-careers` 325.2 → 258.8 s at 16 workers
in the earlier block), it just does not set the makespan at 16.

## Coverage, wire and instruments

- **Rows.** Cross-side churn is *smaller* than same-flag churn: lost **90** and **120** rows
  against **784** between the two off arms and **114** between the two on arms. The single big
  mover is the Netease LinkedIn board (**640** rows between `off-1` and `off-2`), the same class of
  volatile source the 16-worker block recorded. `kept` totals: on **3 900 / 3 878**, off
  **4 561 / 4 000** — the off arms disagree by 14 %, the on arms by 0.6 %.
- **Requests per page:** **1.012×** and **1.081×**. The second is outside the pre-registered
  0.95–1.05 band, on 1 391 vs 1 451 pages, i.e. the bounded `lanes − 1` over-fetch visible at
  small page counts — not duplication.
- **`detailFetch` is still unusable under the fan-out.** The gate books each page's `fetchMs`
  additively, so overlapped waits sum: the on arms report more booked detail time than their own
  sources' wall. No conclusion here uses it.
- **Thread count does not survive as an instrument.** The smoke looked decisive (131 threads on
  vs 24 off on six sources) but the arms do not: `off-2` measured **112.4** mean threads against
  `off-1`'s **27.4**, same flag. P5 is unresolvable; the `BoundedSemaphore` blocking is inferred
  from the code and the cohort pattern, not from the sampler.

## Prediction verdicts

| # | prediction | outcome |
|---|---|---|
| P1 | same-side wall spread ≤ 30 % | **pass** (2.08 %, 5.61 %) — and blind to the window that dominated the block |
| P2 | aggregate wall contrast, both pairs | **reported, not usable** — −64.7 % / −67.4 % is a stall-tail artefact; the median board is −4.6 % / −4.9 % |
| P3 | control-cohort contrast within ±5 % | **fail as written** (−60.2 % / −56.8 %, tail-dominated sums); **pass on the paired median** (−3.2 % / −3.8 %, post hoc) |
| P4 | multi-card ≤ −8 % in both pairs | **pass** (−14.4 %, −14.9 %; sums −59.8 %, −44.5 %) |
| P5 | thread count on/off ≥ 1.10× mean | **unresolvable** — same-flag arms differ 4× |
| P6 | requests per page 0.95–1.05 | **fail** at pair 2 (1.081×) |

## What this cannot show

- The 8-worker block has **one arm per side per time slot**; the 16-worker block is a different
  window. The within-block paired contrasts are internally valid, but the 8-vs-16 comparison
  carries cross-window risk that only a 2 × 2 block removes.
- Throughput is compared on different populations (2 508 vs 1 253 sources). The mix is matched by
  construction (proportional half-sample) but not proven equivalent on work per source.
- Eight workers is one point on a curve. Six and twelve are unmeasured.
- The stall that dominated `lanterns-studios.com` (1 182 s against a 90 s budget) is the documented
  pagination-extension tail, not something this block introduced; it is also why aggregate wall at
  8 workers needs ≥5 min of trimming before it means anything.

## Decision

**Keep the flag off, and do not ship 8 workers on this evidence.** The change is real and it is
now well characterised — halving workers removes the penalty and doubles the multi-card gain — but
it substitutes concurrency rather than adding it, so the makespan is unchanged against the shipped
16-worker configuration.

The one experiment that would settle it is a **2 × 2 on the full population** (8/16 workers ×
flag on/off), counterbalanced, with the same paired-median DiD as the estimator. That prices the
substitution directly and would catch a window effect inside the block, which the current
same-side-spread guard does not.

## Correction, 2026-10-30 — the substitution does not survive the full population

That block has since been run ([`fetch-pipeline-inline-detail-fanout-2x2-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-2x2-2026-10-30.md)):
five arms, full 2 508-row population, closing on a repeat of the opening cell. On it, eight
workers with the fan-out is **+13.4 %** wall against the opening 16-worker cell and **+19.3 %**
against the closing one, with drift running **−4.9 %** in the other direction, so it does **not**
match sixteen without the flag. Two claims above are narrowed with it:

- "Halving workers doubles the multi-card gain" is **not resolvable**. The full-population gap
  between the flag's DiD at 8 and at 16 is **−4.6 pp**, inside the **+7.6 pp** drift the same-cell
  replicate measures over that block's five arms. The cohort gain itself replicates
  (**DiD −14.2 pp** at 8, **−9.6 pp** at 16); the ordering between worker counts does not.
- "The makespan is unchanged against the shipped 16-worker configuration" understates it: on the
  full population the 8-worker arm is 13–19 % **slower**, not level.

The mechanism read (contention, blocked threads, the 6-slot gate) still holds directionally —
sources are ~40 % faster at eight workers and per-source work falls 38–43 % — but the throughput
consequence drawn from it here was a sample artefact.

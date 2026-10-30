# Fetch pipeline — a failure budget for static sources

**Date:** 2026-10-29 · **Lane:** fetch-pipeline, static sources · **Status:** implemented; coverage
verified on the captured arms; **two live arm pairs found the treatment inert**, and the mechanism is
recorded below. Not shipped as a wall win.
A static source that has kept no job may spend **60 s** in failed attempts and is then abandoned
(`failure budget exceeded (60s)`), instead of sitting inside the pagination extension for up to
**630 s**. Replayed on the four captured counterbalanced arms, that is **76 / 12 / 56 / 75** touched
sources, **0** of them carrying a fetched row or a kept job, and **−14.2% / −0.7% / −13.4% / −16.7%**
of arm wall.

## Why: the slow regime was failure time

The 2026-10-29 four-arm ABBA block on the current and pre-change trees found arm wall is the sum
of per-source seconds over ~15 lanes, and that the median source is flat (5.7–6.6 s) while the mean
doubles. The movement is entirely in failing sources: every arm failed
the same 522–532 boards and kept 6,907–7,242 jobs, but error time ran **5,825 s** in the fast arm
against **13,177 / 15,969 / 16,219 s** in the slow ones — **12** boards over 60 s against **56–76**,
each stalling 100–500 s with `status=error` and no stage timings.

`STATIC_SOURCE_TIME_BUDGET_DEFAULT_S = 90` does not bound that. The budget is a ceiling, and
`_queue_discovered_pagination_pages` grants each discovered continuation page one extra base unit
(up to **6 × 90**), never revoked when the board stops making progress. The capture contains 526 s,
409 s and 345 s stalls, and the worst fitted exactly inside that earned extension.

## The policy

- A source that has **kept no job** may spend at most `B` seconds in failed attempts.
- `B = 60` by default (`BALUFFO_STATIC_FAILURE_BUDGET_S`; `0` disables). 75 and 90 replay worse on
  every arm.
- A batch is charged its **failed share** of its own clock (`elapsed × failed / total`), so a wholly
  failed batch pays in full and a live fanout pays only for its failing rows. Charging whole
  batches alone left the budget inert — see the Phase 1 attempt below.
- The first kept job exempts the source for the rest of its run, which makes "no kept board is lost"
  structural rather than empirical.
- It is a second stop condition beside `_page_budget_exhausted`. The 90 s budget, the pagination
  extension and their tests are untouched; `http_batch.py` and every report field's meaning are
  untouched.

Implementation: `static_failure_budget_s` on [`StaticSourceRuntimeConfig`](../../src/jobs/adapters/static_runtime_support.py),
`record_failed_attempt` / `failure_budget_exhausted` / `stop_for_failure_budget` on
[`StaticSourceContext`](../../src/jobs/adapters/static_runtime.py), the accounting at the listing
batch in [`static_listing_runner.py`](../../src/jobs/adapters/static_listing_runner.py) and at the
detail batch in [`static_listing_traversal.py`](../../src/jobs/adapters/static_listing_traversal.py).
Tests: [`tests/jobs_static/test_static_failure_budget.py`](../../tests/jobs_static/test_static_failure_budget.py).

## The guard, and the amendment made before any live arm

The pre-registration said the budget applies while a source "has not fetched a single row". The
first implementation approximated that with no kept job **and no detail-page read**. Replaying that
proxy exempted **24 / 11 / 22 / 27** of the error rows over 60 s — 40% of the tail — and every one of
them ended with `keptCount=0` and `fetchedCount=0`. It protected no job in any of the four arms.

The shipped guard is the coverage criterion itself: **a kept job exempts; a detail-page read does
not.** That is the only place the registered policy changed, and it is recorded with the numbers in
the study's pre-registration before any live arm ran.

## Phase 0 replay

The shipped predicate was replayed against the four captured arms by importing the real
`StaticSourceContext` and asking it per recorded row — not by re-deriving the rule.

| arm | touched | touched source-s | wall after | Δ |
|---|---:|---:|---:|---:|
| `cur-1` | 76 | 11,365 | 2,716 | −14.2% |
| `bef-1` | 12 | 884 | 1,526 | −0.7% |
| `bef-2` | 56 | 8,765 | 2,312 | −13.4% |
| `cur-2` | 75 | 11,707 | 2,344 | −16.7% |

Coverage invariants: **0** touched rows with `fetchedCount>0`, **0** with `keptCount>0`. Residual
error seconds **9,164 / 5,661 / 7,772 / 9,012**; worst single failure **526 / 94 / 345 / 408 s** →
60 s of charged failure time (a live row can overshoot by one attempt). The replay treats an error
row's whole duration as failure time, which is an upper bound once accrual is per-batch share.

One replay bug is worth keeping: the first pass charged every row's whole duration as failure time
and reported **97** trips on `cur-1` instead of 76, because `ok` rows never accrue failure time.
The replay now accrues for `status=error` rows only, matching the implementation.

## What this does not do

It does not collapse the bimodal regime. Between the slow and fast arms the source-second gap splits
**43% error rows (10,144 s) / 57% `ok` rows (13,584 s)**; this bounds only the first part. A slow arm
moves 12–17%, a fast arm under 1%. The rest is `ok` rows answering more slowly in that window, which
is network weather and not something a retry budget can remove.

## Phase 1 — a null record twice, and why the rule never fires

Both blocks run on one build with the kill switch as the only difference (`off` =
`BALUFFO_STATIC_FAILURE_BUDGET_S=0`, `on` = `60`), so the contrast is the policy and not a build.

| block | arm | budget | wall | kept | error s | err >60 s | max err | budget rows |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `p1` | `off-1` | 0 | 2,638.1 | 7,072 | 11,387 | 49 | 400 | 0 |
| `p1` | `on-1` | 60 | 1,406.8 | 7,440 | 5,136 | 11 | 92 | **0** |
| `p1b` | `off-3` | 0 | 1,615.2 | 7,059 | 5,681 | 11 | 130 | 0 |
| `p1b` | `on-3` | 60 | 3,299.6 | 7,214 | 17,272 | 87 | 280 | **0** |

`on-1` finished 47% faster than its control and `on-3` 104% slower, and **neither stopped a single
row**: 0 budget rows across 499 and 534 failures. Both arms are drawings of the weather, not of the
policy, and the analyser now says so instead of scoring them — with no touched row, only the
coverage endpoint is evaluable.

The first block's cause was the accounting: charging a batch only when **every** row failed meant a
mixed batch accrued nothing. That was fixed to the failed share of the batch clock
(`elapsed × failed / total`, commit `abf09e70`) and the second block still recorded zero. So the
cause is not the charge rule. Four measurements locate it:

1. **The wiring works.** A single-source run with the seams patched logs `CHARGE +1.2 total=1.2
   budget=60` then `CHECK total=1.2 ... -> False` on a failing board, and `CHARGE +0.3 ...
   exempt=True -> False` on a healthy one. Charges reach the accumulator and checks consult it.
2. **The stalled rows have no failure time to charge.** Across the 87 error rows over 60 s in
   `on-3`, the outer clock sums to **12,021 s** while their own `listing_fetch_ms` sums to **353 s**
   — most report **one** listing batch and **0 ms** of measured fetch. The seconds are outside the
   batch clock, so the rule has nothing to accrue.
3. **The batch deadline is not the overrun either.** A wedged-but-alive fetch (a thread that sleeps
   200 s) makes `fetch_pages_batched` return at its deadline — **30.0 s** sync, **30.3 s** async —
   with `total batch deadline exceeded (30s)` rows. The deadline is enforced promptly.
4. **The stalls are transient.** The 16 slowest boards from `on-3` re-run as a 16-worker slice
   finish in **8.0 s** (worst row 6.5 s), each batch returning in under a second.

So the slow seconds are exterior to batch fetch — the same wall-weather the four-arm block already
showed in `ok` rows — and a per-source failure budget bounds a quantity that is near zero on this
population. The rule cannot remove cost it never sees. It remains correct (coverage was never at
risk: 0 touched rows in both blocks, and no kept board is lost with the budget on), it is simply
not a wall lever here.

## Verification

`tests/jobs_static` — **681 passed**, including the 16 budget tests and the untouched
pagination-extension, pagination-follow and time-budget-default suites. Ruff check and format clean;
the LOC baseline covers the new leaves.

## Next

The failure budget is not shipped as a performance win; with two null blocks there is nothing to
claim. The open question is where the 180–280 s rows actually spend their time, since it is not in
`fetch_pages_batched` and not charged to any batch — the two candidates are the browser fallback
and the per-host gate wait. That needs a stage-timing probe on a stalled re-run, and it should be
pre-registered before any code changes, exactly as this lever was.

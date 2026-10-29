# Fetch pipeline — a failure budget for static sources

**Date:** 2026-10-29 · **Lane:** fetch-pipeline, static sources · **Status:** implemented; coverage
verified on the captured arms, live arm pair pending.
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
- Only a **wholly failed batch** is charged — a batch with one `ok` row is free, so a slow-but-live
  fanout is never punished.
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
60 s of charged failure time (a live row can overshoot by one attempt).

One replay bug is worth keeping: the first pass charged every row's whole duration as failure time
and reported **97** trips on `cur-1` instead of 76, because `ok` rows never accrue failure time.
The replay now accrues for `status=error` rows only, matching the implementation.

## What this does not do

It does not collapse the bimodal regime. Between the slow and fast arms the source-second gap splits
**43% error rows (10,144 s) / 57% `ok` rows (13,584 s)**; this bounds only the first part. A slow arm
moves 12–17%, a fast arm under 1%. The rest is `ok` rows answering more slowly in that window, which
is network weather and not something a retry budget can remove.

## Verification

`tests/jobs_static` — **678 passed**, including the 13 new budget tests and the untouched
pagination-extension, pagination-follow and time-budget-default suites. Ruff check and format clean.
Guardrail groups `loc`, `duplication`, `test-shape`, `dead-code` and `eol` pass; the LOC baseline is
ratcheted to `src 192,362` / `tests 266,601`.

## Next

Phase 1 is an ABBA pair on a single build with `BALUFFO_STATIC_FAILURE_BUDGET_S=0` against `=60`, so
the contrast is the policy rather than a build. The primary endpoints are report-level and nearly
noise-free — touched-set identity, error seconds, worst failure duration, kept and `outputCount`,
and the success tail above 90 s — because the block's measured minimum detectable wall effect is
**73.6%**, far above a 14% change.

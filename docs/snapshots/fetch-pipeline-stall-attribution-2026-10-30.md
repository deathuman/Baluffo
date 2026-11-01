# Fetch pipeline — where a slow static source row actually spends its time

**Date:** 2026-10-30 · **Lane:** fetch-pipeline, static sources · **Status:** measured; attribution only,
no behaviour change shipped.
Across two arms of **1 620** boards each, **81.7%** of all static source wall is HTTP fetch: listing
pages **37.8%**, detail pages **43.9%**. Browser preparation is **5.6%**, candidate extraction **4.2%**,
and the per-host gate **0.3%** (**75 s** in **26 234 s** of source work). Of the **38** rows that ran
60 s or longer, **27** are detail-fetch dominated and the other **11** are unbooked HTTP waits caught
by stack sampling; **none** is browser- or gate-dominated (the largest prepare share on any slow row
is **3%**, the largest gate share **9%** — both floors, and the gate number is corrected below).

## Why this was needed: the recorded evidence was unmeasured

The 2026-10-30 kill-switch pair showed the failure budget never fired on 534 failures, and the slow
rows carried `listing_fetch_ms≈0`, `listing_browser_fallbacks=0`, `domain_gate_wait_ms=0` and one
listing batch. Reading the code before this study found three independent reasons that is not
evidence about anything:

1. `execute_loader` applies stage timings only on the success path. Its `except _SourceExecutionFailure`
   branch skips `_apply_stage_timings`, so `stageTimingsMs` is `{}` on **every** error row — 0 of 534
   measured in `on-3`.
2. `domain_gate_wait_ms` is written by that same skipped function, so its `0` on an error row is the
   initial value, not a measurement.
3. `listing_prepare_ms` is booked by `StaticFetchRunner._prepare_listing_htmls` but
   `normalize_fetch_report_detail_stats` has no field for it, so it is dropped from every persisted
   artifact.

Both candidate causes named in the request — browser fallback and per-host gate wait — were therefore
untested, not refuted, by the arms.

## Method (pre-registered in `_out/fetch-investigation/stall-attribute-20261030/design.md`)

Two in-process arms of the current build: the p1b seed and population, **16 workers**, shipped
defaults (failure budget **60 s** active), **1 620** selectable loaders, **1 620** completed rows in
each. Probe-side only, no production change:

- a wrapper on `pipeline_source_loop.execute_loader` that records wall, status and the **full** detail
  stats before normalization (`listing_prepare_ms` included) plus `_staticDomainGateWaitMs` read from
  the live task row, so the error path cannot hide it;
- a 250 ms stack sampler over `sys._current_frames()`, attributed per source by thread ident, which
  names the waiter for any second a booked stage does not cover.

Arm walls: **773.6 s** and **909.7 s** process, **12 027 s** / **14 207 s** of source work — the fast
regime both times, which is the honest reading of the headline shares and not a claim about a slow
block.

## Population split

| stage | arm 1 | arm 2 | combined | share |
|---|---:|---:|---:|---:|
| listing fetch | 4 996 s | 4 920 s | 9 916 s | 37.8% |
| detail fetch | 5 371 s | 6 149 s | 11 520 s | 43.9% |
| browser prepare | 663 s | 812 s | 1 474 s | 5.6% |
| candidate extraction | 396 s | 698 s | 1 094 s | 4.2% |
| per-host gate wait | 16 s | 59 s | 75 s | 0.3% |
| unbooked remainder | 602 s | 1 629 s | 2 231 s | 8.5% |

Every row's wall is booked to **88–95%**; the remainder is the same HTTP wait the sampler catches on
the rows where a stage timer is missing.

> **Correction (same day): the gate row is a floor.** `_staticDomainGateWaitMs` was booked only where
the calling thread carried `thread_local.source_name`, and the batch lanes fetch from a pool where
that is empty; the full-population arms run later the same day measured the product booking
**4.5% / 6.1%** of the wait an independent proxy timed on the same gate. Applied here, **75 s** of
**26 234 s** is a book rate, not a share: the implied gate total is **≈4.6–6.3%** (≈1.2–1.7 ks),
not **0.3%**. The instrument is fixed in tree — see
[`fetch-pipeline-limiter-attribution-2026-10-30.md`](fetch-pipeline-limiter-attribution-2026-10-30.md).
> The other rows are unaffected: they come from stage timers that run on the source's own thread.

## The slow rows (≥ 60 s, by the pre-registered 60% rule)

**38** rows across both arms, **3 077 s** total. Their own split: detail fetch **2 201 s (71.5%)**,
listing fetch **484 s (15.7%)**, extraction **137 s (4.5%)**, browser prepare **10 s (0.3%)**, gate
**27 s (0.9%)**.

| verdict | rows |
|---|---:|
| detail fetch ≥ 60% of wall | 27 |
| listing fetch ≥ 60% of wall | 2 |
| unbooked HTTP wait, sampled network leaf ≥ 60% | 9 |
| browser prepare ≥ 60% | **0** |
| per-host gate ≥ 60% | **0** |

Twenty-five of the 38 are `ok` rows, so this tail is not a failure phenomenon: it is boards with many
detail pages. Only **2 of 38** recorded a successful browser fallback.

The dominant leaf, taken from the largest row (`careers.ea.com`, 274.0 s, 622 samples ≈ 156 s):

```
threading.py:359:wait <- _base.py:455:result <- transport.py:723:fetch_text
  <- pipeline_runtime_writers.py:232:__call__ <- fetch.py:75:fetch_with_retries
  <- static_runtime_support.py:302:_fetch_hop_chain
```

That is the caller blocked on an HTTP request's future inside the transport pool — not a semaphore, not
Playwright, not the task-state writer. The same leaf carries the 13 `error` slow rows, which is what the
arms' 87 unmeasured error stalls most likely were.

## What it rules out, and what is left

- **Browser fallback is not the slow tail.** It is real but small: 5.6% of wall, and its worst boards
  (three empty renders, ~38 s) are mid-size rows, not the 180–280 s ones. A render budget would not move
  this tail.
- **The per-host gate is not the slow tail**, though not for the reason the 75 s suggests. Corrected,
the gate is ≈5% of source wall, not 0.3%. What excludes it as the *tail* is per row: the 38 slow
rows' sampled leaves are `Future.result <- transport.fetch_text`, i.e. blocked on a request, not on
a semaphore. Any gate change still needs the fixed instrument before it is justified.
- **The failure budget was aimed at the wrong quantity.** It charges failed batch share; these seconds
  are ordinary fetch wall on rows that mostly succeed.
- What is left is **request volume × per-request latency**: **3 036** listing batches (1 518 per arm)
  took **9 916 s**, ~3.3 s per page at 16 lanes, and detail fetch took **11 520 s** on the minority of
  boards that visit detail pages at all. The levers that touch that are fewer requests (conditional
  detail reads, yield-linked detail caps) or more in-flight requests per source — not another timeout.

## Limits

- **1 620 of 2 530** population names are selectable from this seed; the other 910 no longer resolve to
  a loader (name-form drift against the seed registry), so this is 64% of the arms' population and the
  per-board rates are not directly comparable to `off-3` / `on-3`.
- Request counters are under-booked (`detail_pages_visited` reports 2 081 pages against 6 149 s of
  detail fetch), so seconds per page is a lower bound except for the listing side, where batch count is
  exact.
- These are attribution arms, not a counterbalanced pair: they say where seconds sit, not what a change
  would win.

## Next measurement

A request-count audit on the detail path — pages requested, retries, redirects and cache hits per
source — so "3.2 s per page" can be split into host latency and our own request overhead before any
cap or conditional-GET work is justified.

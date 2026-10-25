# Fetch pipeline — revalidation at population scale, and what the tagless cohort is worth

**Date:** 2026-10-25 · **Lane:** fetch-pipeline revalidation · **Status:** measured; two items shipped,
one closed by measurement.
**2,814 s of reads** replaced by 182 HEADs on the population, and **no new probe path** for tagless
boards: it costs more than the reads it avoids. Behaviour:
[`scraping-pipeline.md`](../scraping-pipeline.md#a-kept-static-board-asks-before-it-downloads).

This settles three of the items left open by the conditional-GET work: multi-page boards could only
ask page 1's question, a page that publishes no tag was probed blind, and the Ironbelly/LinkedIn
board oscillated between 143 rows and nothing.

## What shipped

**Per-page validators.** A 304 on page 1 says nothing about page 2, so a multi-page board now stores
one validator per page (`lastHttpPageValidators`) and asks each page with its own tag; the source
completes only when every page answers 304, and the sweep stops at the first page that answers 200
while holding a validator. Pinned by
[`test_static_revalidation_cycle.py`](../../tests/jobs_static/test_static_revalidation_cycle.py).

**The read clock as a conditional question.** A page that has never published an `ETag` /
`Last-Modified` is asked `If-Modified-Since: <the read it is waiting to replace>`. The date is
**ours**, so it is a request header only and is never persisted as a validator
([`conditional_revalidate_url`](../../src/jobs/transport.py),
[`_read_clock_since`](../../src/jobs/adapters/static_listing_flow.py)). It rides the HEAD the sweep
already sends — no request is added.

**A denied request is not an empty board.** LinkedIn answers `HTTP 999`; it fell through to `error`,
which read as "the board has no jobs" and retired the evidence instead of retrying.
`classify_static_fetch_exception` now returns `anti_bot_or_challenge` / `blocked_or_challenge` for
it ([`static_runtime_support.py`](../../src/jobs/adapters/static_runtime_support.py)), and
[`test_static_fetch_fallback_exceptions.py`](../../tests/jobs_static/test_static_fetch_fallback_exceptions.py)
pins both branches. An isolated live run of that board shows what the change does and does not do:
LinkedIn still denies it (the configured company page redirects to
`/company/ngpcareers?trk=similar-pages`, which answers 999), the source still collects nothing this
run, and the report now carries `browserFallbackRecommended: true` with
`failureBucket: needs_review` — a denial that is labelled and queued for the browser, not an empty
board whose evidence is retired. It does not make a denied board collect.

## The population

One seeded registry and state (2,508 static sources), every clock moved 900 min back so every board
is due, 16 workers, a data dir per arm, current tree — the `revalidation_study.py` procedure
documented in [`tools/README.md`](../../tools/README.md). The cold arm is a full-read workload by
construction; the steady arm is seeded **from the cold arm's state** and aged 200 min, so the boards
whose window is 180 min are due again while the zero-kept 12 h cohort is not.

| arm | wall | source work | kept | output | 304s | revalidated | skipped | failed |
|---|---|---|---|---|---|---|---|---|
| cold (all due) | 2 672.2 s | 40 834.1 s | 7 169 | 3 990 | 0 | 65 | 0 | 529 |
| steady (200 min later) | 573.2 s | 9 054.1 s | 7 523 | 4 611 | **182** | 472 | 1 291 | 434 |

Steady decisions: `skip_fresh` 1 291, `run_now` 452, `revalidate_only` 472, `listing_only` 293.

**What the 304s bought.** All 182 are priced by their own cold read: 2 960.0 s of full reads
(median 7.88 s per board) became 145.7 s of HEADs — **2 814.3 s removed**. Against the run's own
counterfactual (9 054.1 s − 145.7 s + 2 960.0 s = 11 868.4 s), that is **23.7% of the steady run's
source work**. The remaining 290 revalidations answered 200 and read as before, so the probe was
additive there and cost ~0.3 s each.

**What it did not buy.** The 78.5% cold→steady wall drop is not a revalidation result: 1 291 of the
2 508 boards were skipped by a window that predates this work. At 16 lanes the 2 814 s of removed
work is worth ~176 s of wall at best, and the two arms ran 45 min apart on live hosts, so the wall
movement is not attributed to the change. Yield moved 3 990 → 4 611 output rows on the same
population, which is board churn, not a probe effect — the arms are different workloads.

Validator coverage on this population: **1 789 of 4 910 state rows** carry a tag (36%);
2 472 zero-kept rows are tagless, and 959 of those have no success history at all — no read clock,
so no conditional question can reach them.

## The tagless cohort: ship the fallback, close the probe

The open item asked whether a tagless zero-kept board should be *given* a probe instead of a read.
The same 1 513 boards (zero-kept, tagless, a success clock, a priced read) were asked three
questions on 1 312 pages — a plain HEAD, the read-clock `If-Modified-Since`, and a 2015 date that a
server with any copy must consider stale:

| arm | requests | 304s | 200s | errors | boards all-unchanged | probe cost |
|---|---|---|---|---|---|---|
| plain HEAD | 1 312 | 0 | 1 263 | 7 | 0 | 1 519.2 s |
| read-clock IMS | 1 312 | **145** | 1 105 | 13 | **115** | 1 509.8 s |
| 2015-date control | 1 312 | 1 | 1 256 | 11 | 1 | 1 472.7 s |

**The verdict splits in two, and both halves matter:**

- **As a new probe path for tagless boards — CLOSED.** 1 134 of 1 179 boards answer 200 to a date
  older than any copy: they ignore `If-Modified-Since` entirely. Only 145 of 1 312 pages (11%) and
  115 of 1 179 boards (9.8%) would skip their read, whose total cost is 671.5 s, while the sweep
  costs 1 509.8 s — a net **−838.3 s per empty window**. Reading is cheaper than proving there is
  nothing to read.
- **Inside a sweep that already probes — SHIPPED.** The `plain` arm is the control for cost: a
  tagless page's unconditional HEAD answers 200 every single time, at the same 1.15 s per request.
  The read clock cannot add a request to a sweep that already sends one, so its 145 × 304s are
  671.5 s of avoided reads for free, and a page that does not honour the header simply answers 200
  and is read as before.

334 cohort boards had no URL to probe (no registry page and an empty `listingUrl`), so the measured
cohort is 1 179 of 1 513.

## Limits

- The two arms are **different workloads**, not a code A/B: they measure a due run against the run
  that follows it. The attributable number is the 304 cohort's own paired read-vs-HEAD cost, not the
  difference between the arms.
- Live hosts, 45 min apart, on the day's network; 529 / 434 failures are dead or refusing boards
  that a forced population run touches by design.
- The tagless probe asks every page it can build; a real sweep would ask only configured pages, so
  the per-page rates are the comparable figures, not the total request count.

## Artifacts

Local and gitignored: `.tmp/study-pop/` (cold arm, `census.json`, `tagless-probe.json`),
`.tmp/study-ste/` (steady arm), `.tmp/study-pop/cold.log`, `.tmp/study-ste/steady.log`. Reproduce
with `python tools/measurements/pipeline/revalidation_study.py seed|arm|census|probe|report` as
documented in [`tools/README.md`](../../tools/README.md).

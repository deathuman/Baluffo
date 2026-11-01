# Fetch pipeline — the detail lane is serial, and a third of its requests are redirects

**Date:** 2026-10-30 · **Lane:** fetch-pipeline, static sources · **Status:** measured; audit only,
no behaviour change shipped.

On one 1 620-board arm, the static pipeline's detail work is **2 274 page asks / 2 079 pages /
3 368 physical HTTP requests**, and **34.1%** of those requests are redirects. Per request the host
owns the clock — **68.7%** of detail call time is TTFB, **9.9%** is waiting for a response that never
came, **6.6%** is body transfer — but the phase is not running concurrently: booked detail shows an
effective **concurrency of 1.10**, and **70.2%** of the detail call time is the *inline rendered-card*
lane, which is a plain `for` loop over cards. Retries (**4.0%**), cache hits (**4.0%**) and the
per-host gate (**3.6%** of call time) are not the cost.

This is the request-count audit the previous snapshot asked for: "3.2 s per page" is now split into
host, transfer, redirect and our own plumbing, and the two named suspects are answered on the same
run that measured them.

## Method

Pre-registered in `_out/fetch-investigation/detail-request-audit-20261030/design.md`; probe and
reducer in the same directory. Population and code are the ones the 2026-10-30 attribution used:
the p1b seed aged **2 880 min**, the **1 620** of 2 530 names that still resolve to a loader,
**16 workers**, shipped defaults, current `main`, one arm (`audit-3`). In-process, probe-side only:

- `StaticHtmlFetcher.fetch_html_cached` and the listing job entries — logical asks, cache hits, and
  the lane attribution taken from the caller's frames (`_fetch_detail_job` → traversal,
  `_fetch_rendered_detail_rows` → inline, plugins, zero-kept guard);
- `_FetchTextLimiter.__call__` / its transport call — one attempt record per transport call, its
  outcome, and the limiter's wall minus the transport's (gate + per-attempt bookkeeping);
- `fetch_with_retries` — one hop per retry loop, so `retries = attempts − hops`;
- `httpx.AsyncClient.get` — physical exchanges per attempt, `response.history` length, and
  `response.elapsed` as TTFB; tagged through a `ContextVar` that `run_coroutine_threadsafe` copies
  into the fetch coroutine (verified before the arm, `check_assumptions.py`);
- `process_detail_html` — parse time inside the booked detail wall;
- a 50 ms lag probe on the shared async fetcher's loop.

Time buckets are measured around nested calls, so they do not overlap:
`ask = queue + noResponse + get(TTFB + body) + lane + retry + overhead`.

**Amendment, before the arm.** The product's own `BALUFFO_FETCH_COUNTERS=1` path was the intended
cross-check and could not be used: enabling it makes `_capture_response_headers` treat the counter
wrapper as request-aware and pass `request=` into `_FetchTextLimiter.__call__`, which accepts only
`(url, timeout)` — a `TypeError` that kills every source on a plugin capture path (reproduced on
`sheet_studios`). Counts here come from the probe's own seam, which is a superset (adds redirects,
asks and timings). The defect is unrelated to this measurement and is left as a finding, not fixed.

## Results

Stage totals, detail-like lanes (cumulative lane time, not wall):

| lane | asks | attempts | retries | gets | requests | redirects | ask s | TTFB s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| detail (traversal, batched) | 564 | 558 | 17 | 543 | 795 | 252 | 1 736 | 1 140 |
| detail_inline (rendered cards, serial) | 1 516 | 1 557 | 59 | 1 525 | 2 339 | 814 | 4 090 | 2 858 |
| plugin (booked to listing) | 130 | 84 | 4 | 78 | 125 | 47 | 236 | 184 |
| zero-kept guard (booked nowhere) | 64 | 74 | 11 | 74 | 109 | 35 | 254 | 155 |
| **detail-like total** | **2 274** | **2 273** | **91** | **2 220** | **3 368** | **1 148** | **6 316** | **4 338** |

Booked `detail_fetch_ms` is **5 294 s**; the traversal + inline lanes alone accumulate **5 826 s** of
call time, so effective concurrency is **1.10** (all detail-like lanes: **1.19**). `detail_pages_visited`
totals **2 079**, which matches the 2 080 traversal + inline asks almost exactly — the previous
snapshot's "pages are under-booked" was wrong; the seconds were, not the pages.

The cumulative detail call time splits as:

| bucket | share | seconds | what it is |
|---|---:|---:|---|
| TTFB | **68.7%** | 4 338 | connect + pool + host, to headers |
| no response | **9.9%** | 627 | failed/timed-out attempts (53 `RuntimeError`) |
| event-loop queue | **7.7%** | 489 | dispatch wait before `get` starts |
| body | **6.6%** | 418 | transfer + decode |
| retry backoff | **3.6%** | 225 | sleeps and retry-loop overhead |
| lane | **3.5%** | 219 | per-host gate + per-attempt bookkeeping |
| ask overhead | 0.0% | 0 | cache lookup / request build |

Per-page: traversal **3.08 s** and inline **2.70 s** of call time, of which **2.02 s** and **1.89 s**
is TTFB; each page costs **1.51** physical requests on average, and **1.29 s** TTFB per physical
request. The shared event loop lagged **19.9 ms** at p50 but **991 ms** at p95 and **9.2 s** at max.

### The serial inline lane is 70% of the detail cost

`_append_rendered_card_rows` loops over rendered cards and calls a real detail fetch for each, one at
a time, on the source thread. The inline lane therefore costs the *sum* of its page latencies:

- **9** sources verify **20+** cards (their inline time is **473 s**), **18** verify 10–19 (**457 s**),
  **60** verify 5–9 (**760 s**), and even the **245** sources with a single card pay **1 109 s** —
  **4.5 s** each.
- The shape is visible on the slow rows: `www.bandainamcostudios.com` runs **43** inline pages at
  **2.08 s** each for **89.4 s** booked (85% TTFB); `eipix.com` runs **29** pages, **58** requests,
  **90 s**, and keeps nothing.

Traversal is batched at `static_detail_concurrency` (10); inline is not batched at all. That — not a
gate or a timeout — is why the phase reports concurrency 1.1.

### A third of detail requests are redirects

**1 148** of 3 368 physical detail requests are redirect hops (34.1%): 32% of traversal requests, 35%
of inline, 38% of plugin. The pattern is concentrated: **394** of 1 620 sources see any redirect and
**329** have at least 40% of their requests as redirects — on those boards every detail URL gets its
own extra round trip (`eipix.com` 29 pages → 58 requests; `recruit.nexon.co.jp` 21 → 42;
`www.bandainamco-nexus.co.jp` 21 → 57). At the measured mean TTFB per physical request, those hops
bound the cost at **≈1 479 s**, **23%** of detail call time. Removing them requires canonicalizing
the URL before the first fetch; this arm did not record redirect shapes, so how many are
pre-emptable is not yet known.

### What is not the cost, and one accounting correction

- **Retries: 91 of 2 273 attempts (4.0%)**, one attempt per ask. P1 held.
- **Cache: 92 hits of 2 274 asks (4.0%)** — P3 missed (predicted ≤2%) but is structurally small;
  the report's `fetch_cache_hits` counts only the traversal lane (**23**), so the persisted report
  under-reports cache service by **69** hits (inline 18, plugin 50, guard 1).
- **Lane: 3.6% of call time (219 s)**, P6 held. The 2026-10-30 claim of 0.3% was measured only where
  `thread_local.source_name` exists — i.e. not in the batched lanes. At the right scope the number
  is ~4%, still not the bottleneck, so the conclusion survives and the number is corrected. That
  book-only-on-the-source-thread behaviour is now fixed in the gate instrument itself
  ([`fetch-pipeline-limiter-attribution-2026-10-30.md`](fetch-pipeline-limiter-attribution-2026-10-30.md)).
- **Booked detail is not the whole detail lane.** Plugin fetches (**236 s**) are booked to
  `listing_fetch_ms`, and the zero-kept guard's re-probes (**254 s**, 64 asks) are booked nowhere.
  On the single ≥180 s row (`careers.ea.com`, **250.6 s**) all 96 asks are the plugin lane — 55
  distinct URLs, 41 served from cache, 96 requests with 41 redirects — and `detail_fetch_ms` for the
  row is **0**.

### Slow rows (≥60 s)

**14** rows, all with a detail lane: **11** inline-dominated, **2** traversal-dominated, **1** plugin.
No row was gate- or retry-dominated. The 60–100 s band is the serial inline shape
(`www.bandainamcostudios.com` 99.5 s / 43 pages, `eipix.com` 97.3 s / 29 pages, `unity.com` 96.7 s /
7 pages, **69 s** booked across them — a slow host, not a long list); the one 250.6 s row is plugin
cache-miss churn.

## Verdicts against the pre-registration

| prediction | observed | verdict |
|---|---|---|
| P1 attempts ≤1.25×asks, retries ≤15% | 1.00×, 4.0% | holds |
| P2 redirects ≤5% of requests | 34.1% | **fails** |
| P3 cache hits ≤2% of asks | 4.0% | **fails** (small) |
| P4 TTFB ≥50%, client ≤25% | 68.7%, 14.8% | holds |
| P5 effective concurrency ≥4.0 | 1.10 | **fails** |
| P6 lane ≤10% of call time | 3.6% | holds |

## What this changes

The levers, in order of measured size:

1. **Fan out the inline rendered-card verification.** 4 090 s of call time sits in a serial loop that
   already has a batched, gated lane available, but only the multi-card share is addressable:
   **2 981 s** sits on sources with 2+ cards, while the **1 109 s** on 245 single-card boards is
   already one wave and cannot move. Fetching a page's cards concurrently and processing them in
   original order is output-neutral (same pages, same rows, same gates) and at 10 lanes would
   compress most of that 2 981 s — roughly half of the detail-like call time — without touching
   request count or coverage. It does change first-failure abort granularity, so that rule must be
   defined per page, not per wave.
2. **Canonicalize detail URLs before fetching.** A third of requests are redirects; measure the
   shapes first (status + `Location` for one populated arm), then pre-empt what is mechanical
   (`http→https`, `www`, trailing slash) and re-check kept counts.
3. **Leave retries, cache and the gate alone.** All are ≤4% and cannot move a 5.3 ks phase.

The previous snapshot's "fewer requests or more in-flight requests" now has a precise answer: the
request count is near-minimal (1.0 attempt/ask, 4% retries); the missing in-flight width is in the
inline lane, not the traversal pool.

## Limits

- One arm, one network window; not a counterbalanced pair, so this is a decomposition, not a
  build-effect estimate.
- TTFB is `response.elapsed`, which includes connection-pool wait and connect/TLS. Connection reuse
  is invisible, so a reused connection's TTFB excludes a handshake its first sibling paid.
- The redirect cost is an **upper bound** (every hop priced at the mean physical-request TTFB); the
  arm did not record `Location`, so any hop that would have happened after canonicalization anyway
  (e.g. a platform migration) is counted as avoidable when it may not be.
- 1 620 of 2 530 population names resolve to a loader from this seed; per-board rates are not
  directly comparable to the `off-3` / `on-3` arms.
- The probe is in-process: wrapping and context-variable costs are small but not zero, and the
  16-worker run shares one machine's CPU with Playwright fallbacks, whose wall is not instrumented.
- `detail_fetch_ms` carries the `_finish_detail_traversal` catch-up, so it is used as a wall to
  compare against cumulative lane time (concurrency), never as a bucket.

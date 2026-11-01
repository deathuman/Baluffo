# Inline detail fan-out — live ABBA block

**Date.** 2026-10-30 · **Tree.** `468a4195` + the uncommitted fan-out (`dirtyDiffSha256 20d9d748…`) · **Status.** flag still
**off** by default; this block is why.

## Question

`BALUFFO_STATIC_INLINE_DETAIL_FANOUT=1` overlaps the rendered-card detail fetches of one listing
page (lanes = `static_detail_concurrency`, default 10) instead of fetching them one card at a time
on the source thread. The 2026-10-30 detail audit priced that serial lane at 70.2% of booked detail
and named "more in-flight requests per source" as the remaining lever. This block measures that
lever on the live population: does it cut the cold full-population wall, and at what cost in
requests, rows, and the sources it does not touch?

## Design

Same build for every arm — the flag is the only difference, so no build drift can enter the
contrast. Four sequential arms in ABBA order (`on-1, off-1, off-2, on-2`), 16 workers, shipped
defaults, one byte-identical seed copy per arm. Population and seed are the 2026-10-30 block's:
2,530 names (1,620 resolve to a loader), state aged 900 min, 2,508 sources executed per arm.

Pre-registration and the post-smoke amendment: `_out/fetch-investigation/inline-fanout-ab-20261030/design.md`.
Artifacts: that directory plus `.tmp/study-inline-fanout-20261030/` (per-arm reports, `rows.json.gz`,
`reqcount.json`, `arm.json`). Raw reports kept per arm.

Instruments: wall = `runtime.phaseTimings[executing_sources]`; work = Σ per-source `durationMs`;
physical requests counted at `httpx.AsyncClient.request` / `httpx.Client.request`
(`1 + len(history)`) with `urllib.urlopen` separate; kept rows hashed per source from the unified
feed; load sampled every 2 s.

## A defect the smoke caught, fixed before the block

The first 20-source smoke showed the original prefetch paying for **every** planned card of a page
before the row loop started: **1 368 requests against the serial arm's 889 while visiting fewer
pages (587 vs 641)**. On a budget-bound board that is pages the serial lane never reaches. The
shipped shape is now `_InlineDetailPrefetcher`, advanced from inside the loop: card `i` pays only
for cards `[i, i + lanes)`, so over-fetch is bounded by `lanes − 1` and a page the batch never ran
is left to the loop (budget death still surfaces as the serial `TimeoutError`). The second smoke:
**266 rows on both sides, 894 vs 998 requests for 630 vs 705 pages — 1.42 vs 1.41 requests per page.**

## Result

| arm | flag | wall s | work s | conc. | exchanges | pages | req/page | feed rows | kept | failed | CPU s/work s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off-1` | 0 | **1 389.6** | 21 770.9 | 15.67 | 9 558 | 3 313 | 2.885 | 3 973 | 7 337 | 509 | 1.329 |
| `off-2` | 0 | **1 418.9** | 22 257.7 | 15.69 | 9 582 | 3 372 | 2.842 | 3 748 | 7 152 | 505 | 1.337 |
| `on-1` | 1 | **1 425.0** | 22 272.2 | 15.63 | 9 826 | 3 293 | 2.984 | 3 694 | 7 026 | 507 | 1.352 |
| `on-2` | 1 | **1 480.6** | 23 165.3 | 15.65 | 9 855 | 3 293 | 2.993 | 3 791 | 7 183 | 507 | 1.333 |

Same-side replicate spread: off **2.11%** wall / **2.24%** work, on **3.90%** / **4.01%**. The
block is tight enough to resolve a few-percent effect.

| pair | wall | work | feed rows | requests/page |
|---|---:|---:|---:|---:|
| `on-1 − off-1` | **+2.55%** | **+2.30%** | −7.02% | 1.034× |
| `on-2 − off-2` | **+4.35%** | **+4.08%** | +1.15% | 1.053× |

**At the shipped configuration the fan-out is a small, consistent loss: wall +2.6% and +4.4%,
work +2.3% and +4.1%, in both time orders.**

## Why — the effect is real but it is reallocation, not throughput

Cohort by `detail_pages_visited`, i.e. the only boards the flag can overlap versus the ones it
cannot touch at all (multi-card = the affected cohort; single-card and no-detail are control
cohorts the code path cannot reach):

| comparison | multi (≥2 pages) | single (1 page) | none (0 pages) |
|---|---:|---:|---:|
| `on-1` / `off-1` | **−14.5%** | **+27.5%** | **+9.3%** |
| `on-2` / `off-2` | **−11.4%** | **+17.6%** | **+14.7%** |
| `off-1` / `off-2` (same flag) | −2.0% | −4.2% | −1.5% |
| `on-1` / `on-2` (same flag) | −5.4% | +3.8% | −6.1% |

The affected cohort gains **11–15%**, well outside same-side noise, and with 440/442 matched
boards it is 247 and 236 boards faster respectively (total −1 215.9 s and −878.5 s;
`playstation-careers` 325.2 → 258.8 s). The controls meanwhile lose **10–28%** in both orders,
against a 1.5–6.1% same-side band. Those cohorts have no fan-out path at all, so the slowdown is
contention — though not the shape first written here. Static fetches do not reach the adapter gate:
they go through `fetch_text_static_limited`, whose per-host gate is
`DEFAULT_STATIC_FETCH_MAX_PER_DOMAIN = 6`. Lanes are therefore capped at six concurrent per host
whatever `static_detail_concurrency` says, and 16 workers × 6 = **96**, which is exactly
`AsyncHttpTextFetcher`'s pool (`max_connections = max(48 * 2, 48)`), not four times it. The cost is
that `_InlineDetailPrefetcher.window` asks `fetch_pages_batched` for
`per_host_concurrency = len(planned)` = 10 and lets the gate do the limiting, so **four threads per
active board sit in `BoundedSemaphore.acquire()`** for the length of a page fetch — at 16 workers,
up to 64 blocked threads behind one event-loop thread. Gate wait itself did not rise: it was
**1.2% / 0.9%** of static detail time in the on arms against **3.2% / 3.5%** off. The fan-out did
not congest the gate; it made the gate cheaper to use and paid in blocked threads.

The cohort split is also weather-robust. Re-read as a paired per-source median and differenced
against the cohorts the flag cannot touch, these same four arms give **−19.7 pp** [−25.1, −13.8]
and **−15.6 pp** [−21.6, −6.6] on the multi-card cohort — see
[`fetch-pipeline-inline-detail-fanout-workers-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-workers-2026-10-30.md)
— so the mechanism claim does not rest on the per-arm aggregate.

The booked detail figure moves the *other* way (`+30.6%`, `+40.6%`) and must not be read as a
mechanism: `fetchMs` is taken per page around a batched fetch, so under wider concurrency each
page's number includes its own queue wait and the sum over-counts. `playstation-careers` books
exactly **1 000 000 ms** of detail in every arm, including the 2026-10-30 arms, against a 258–325 s
source duration — a pre-existing clamp that is identical on both sides and cancels.

## Requests and rows

- **Requests per page**: 2.984 vs 2.885 (+3.4%) and 2.993 vs 2.842 (+5.3%). The `lanes − 1`
  over-fetch shows up as a few percent — pages paid for and then skipped by the loop — not as
  duplicated pages. It is the only wire cost, and it is bounded.
- **Rows**: cross-side churn is not worse than same-flag churn. Lost rows: 70 (pair 1) and 246
  (pair 2), against **421** between the two *off* arms and 102 between the two *on* arms. The
  largest movers are LinkedIn company boards (276 → 105 → 173 across arms) and `careers.ea.com`
  (59 → 0), i.e. sources that move between any two arms. No source lost rows to the fan-out that
  the same-flag control does not also lose.

## Prediction verdicts (pre-registered)

| # | prediction | outcome |
|---|---|---|
| P1 | same-side wall spread ≤30% | **pass** (2.11%, 3.90%) |
| P2a | feed-row totals within ±1% | **not resolvable** — the same-flag pair differs 5.7% (3 973 vs 3 748) |
| P2b | cross churn ≤ same-side churn + 25% | **pass** (70/246 vs 421) |
| P2c | no extra row loss | **pass** |
| P3 | requests per page 0.95–1.05 | **fail** at the bounds (1.034×, 1.053×) |
| P4 | wall saving 10–30%, both pairs | **fail** — both pairs slower (+2.55%, +4.35%) |
| P5 | work saving 12–30%, both pairs | **fail** — both pairs slower (+2.30%, +4.08%) |
| P6 | detail cohort falls only on multi-card | direction **confirmed** in source duration (−14.5%, −11.4%); the booked-detail metric is unusable under changed concurrency |
| P7 | CPU per work-second 0.9–1.2× | **pass** (1.352/1.333 vs 1.329/1.337) |

## Decision

**Keep `BALUFFO_STATIC_INLINE_DETAIL_FANOUT` off.** Per the pre-registered rule, a mixed or
positive sign is not a saving; here both pairs are slower on the metric that matters and the
cohort gain is paid for by the rest of the population.

The result also retires the lever the audit proposed. At 16 source workers the six-slot static gate
and the 96-connection pool are the same number, so *width inside a source* reallocates capacity
rather than adding any, and lanes above six buy blocked threads rather than overlap. The next
candidates are the ones that change totals:

- **fewer requests** — conditional detail reads and the redirect share (34.1% of physical requests,
  329 sources at ≥40%) are real work; the fan-out only moves it around;
- **a smaller worker count with lanes** — same total in-flight, each worker wider, so heavy boards
  stop monopolising the pool. Measured separately in
  [`fetch-pipeline-inline-detail-fanout-workers-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-workers-2026-10-30.md);
- **targeted use** — enable the fan-out only above a card threshold and only when the pool has
  headroom; not implemented, not measured.

## Limits

- One ABBA block on one machine, one network window. It is tight (≤4% spread) but not a repeated
  block; a second day could move the level.
- The cohort split uses `detail_pages_visited`, which includes traversal pages as well as inline
  cards, so a handful of "single-page" boards may still be affected. The matched multi-card
  comparison (440/442 boards) is the cleaner read and agrees with the cohort split.
- Requests are counted at client seams, not on the wire; connection reuse and browser-fallback
  traffic are not fully visible.
- The 1 000 000 ms detail clamp is real and unexplained here; it is constant across arms and does
  not affect wall or work, but it does hide the detail-time change on the largest board.

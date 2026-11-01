# Fetch pipeline — the inline detail lane can be fanned out without changing a row

**Date:** 2026-10-30 · **Lane:** fetch-pipeline, static sources · **Status:** shipped behind a flag
(`BALUFFO_STATIC_INLINE_DETAIL_FANOUT`, default **off**); parity proved offline, and the live ABBA
block then measured it as a small loss at 16 workers — see
[`fetch-pipeline-inline-detail-fanout-live-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-live-2026-10-30.md).

The 2026-10-30 detail-request audit found the static lane's largest addressable cost: the
rendered-card verification loop fetches each card's detail page on the source thread, one card at a
time, so a board pays the **sum** of its page latencies. It is **4 090 s** of call time on the
1 620-board arm, **2 981 s** of it on sources with 2+ cards (see
[`fetch-pipeline-detail-request-audit-2026-10-30.md`](fetch-pipeline-detail-request-audit-2026-10-30.md)).

This change adds a bounded concurrent prefetch of exactly the HTML that loop is about to fetch. The
loop itself is untouched — it still plans, emits, counts and aborts in card order — so the fan-out
**cannot** move a row, only the network wait.

## How output parity is structural, not tested-in

`_plan_rendered_row` is now the single place that decides what a rendered card will do (skip, emit,
verify, verify-provisionally, queue). Both readers run it:

- `_InlineDetailPrefetcher.window(i)` reads it ahead of card `i` to know which pages to pay for;
- `_append_rendered_row` re-reads it at the card's own turn, against the state that has evolved.

A prefetched page the loop then skips costs a request and nothing else, because the loop's decision
is still the authority. A card the prefetch never planned (budget, an unplanned link, a batch that
raised an unexpected shape) is fetched by the loop exactly as before. The prefetched document is
handed to `process_detail_link` as `prefetched_html` with its **real** `fetchMs` and `cacheHit`, so
the parse, `detail_fetch_ms` attribution and the shell-strike predicate see what the serial call
would have seen.

**The window is the load-bearing part.** The loop calls `window(i)` itself, so the prefetch can only
be `lanes − 1` cards ahead of the row about to be consumed. The first implementation walked the
*whole* page before the loop started; the 20-source smoke caught it paying **1 368 requests against
the serial arm's 889 while visiting fewer pages (587 vs 641)** on a 90-second board, because the
cards after budget death were fetched and discarded. The windowed shape pays at most `lanes − 1`
extra requests and leaves pages the batch never ran to the loop, so a budget death still surfaces as
the serial `TimeoutError` at the serial card.

Failure semantics stay per page: a failed prefetch stores the original exception object and the row
re-raises it at its own turn, inside the existing accounting (`detail_pages_visited`,
`detail_fetch_failed`, `ctx.errors`, re-raise to the caller's abort). The prefetcher stops scheduling
once a window reports a failure, since the serial lane aborts on the first failing card and never
requests the cards after it.

## Bounds

- Lanes are the source's existing `static_detail_concurrency`; fewer than 2 lanes, or fewer than 2
  cards, makes the prefetch a no-op.
- Every fetch is the same `StaticHtmlFetcher.fetch_html_cached` with the same `timeout_s` and
  `retries`, so it still passes through the shared per-host limiter (`max_per_domain`) and the
  per-request budget cap.
- Every window checks `remaining_budget_s()` and `failure_budget_exhausted()` before fetching.
- Default **off**: the concurrency shape is the one part no offline replay can settle. Enable with
  `BALUFFO_STATIC_INLINE_DETAIL_FANOUT=1`.

## Evidence in the suite

`tests/jobs_static/test_static_inline_detail_fanout.py` (6 tests):

- **Real-socket replay.** The whole lane runs twice over one loopback HTTP server — flag off then
  on — with the real fetcher, real `process_detail_link` and a detail page that parses to a row
  (`city == "Tokyo"` asserted, so the parser path is exercised, not just fallbacks). Emitted rows,
  `seen_links`, counters, errors and the sorted request set are identical.
- **Mock-level parity** on a 4-card board with a double that fetches through the real
  `fetch_html_cached` when no prefetched document is handed in; both arms also assert the four
  cards' pages were requested once each.
- **Failure parity.** An HTTP 500 on card 2 in both arms: same exception type and message, same
  `detail_pages_visited == 2`, `detail_fetch_failed == 1`, same `ctx.errors`.
- **Gates.** Disabled, or one lane, returns no prefetch.
- **Bounded lookahead.** A 12-card board whose budget dies after card 4: four cards consumed,
  requests ≤ `4 + lanes − 1`, and strictly fewer than the page's card count.
- **The seam.** A prefetched document is used without calling `fetch_html_cached` again and reports
  the prefetched `fetchMs`.

The existing static suites are unchanged: **687** tests in `tests/jobs_static` pass, including the
shell-strike, provisional-fallback, stage-attribution and rows-error-accounting suites that pin the
lane's public behaviour.

## Live result (same day)

A same-build 4-arm ABBA block (`on, off, off, on`, 2 508 sources/arm, same-side spread 2.1–4.0%)
measured the flag at **+2.55% / +4.35% wall** and **+2.30% / +4.08% work**. The mechanism works on
its own cohort — multi-card boards **−14.5% / −11.4%**, 247 and 236 of ~441 boards faster — but the
cohorts the flag cannot touch lose **10–28%**, because all sources share one 48-connection async
fetcher and 16 workers × 10 lanes oversubscribe it. Net: reallocation, not throughput. Requests per
page **+3.4% / +5.3%**; row churn no worse than the same-flag control. The flag stays **off**; the
lever is fewer requests or a lower worker count with lanes, not more in-flight per source. Full
numbers and verdicts: [`fetch-pipeline-inline-detail-fanout-live-2026-10-30.md`](fetch-pipeline-inline-detail-fanout-live-2026-10-30.md).

## What remains

- The single-card tail (1 109 s on 245 boards) is one wave already and cannot move; the addressable
  ceiling is the 2 981 s multi-card share, further capped by `max_per_domain`.
- The shape this block did *not* test: fewer workers with lanes, so total in-flight stays near the
  pool's capacity while each heavy board is wider. That is the only variant of this lever the
  contention result leaves open.
- `BALUFFO_FETCH_COUNTERS=1` still crashes a plugin capture path on its `request=` kwarg (found by
  the audit, not fixed here).

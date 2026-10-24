# Fetch pipeline — the zero-kept lane: what a 304 actually recovers

**Date:** 2026-10-24 · **Lane:** fetch-pipeline revalidation · **Status:** measured and shipped.
**~99 s net per empty episode** on the reference sample — one confirming read avoided — not the
largest remaining headroom. The decision change is in
[`_static_cache_decision`](../../src/jobs/state_incremental.py) and pinned by
[`test_state_incremental_empty_streak.py`](../../tests/test_state_incremental_empty_streak.py);
behaviour below.

This measures the open item left by the conditional-GET work (0.3.026): a zero-kept board's first
due check after it reads empty is a **full read**, because
[`get_incremental_cache_decision`](../../src/jobs/state_incremental.py) only trusts a probe once
`_zero_kept_streak >= DEFAULT_INCREMENTAL_EMPTY_SOURCE_MIN_ZERO_RUNS` (2). Would letting a 304
satisfy the empty window at streak 1 be worth it?

## What the lane already costs

Once past the empty window the lane is cheap by construction: a zero-kept board with a stored
validator is probed at its 12 h boundary and the 304 starts the next window
([behaviour](../scraping-pipeline.md#a-kept-static-board-asks-before-it-downloads)). So the only
read left to remove is the confirming one, and it happens **once per empty episode**, not once per
run.

In the warm arm of the run-level A/B (`after-run2`: 200 sources, 16 workers, 1 383.5 s of source
work, 92.7 s wall), the zero-kept rows cost 582.0 s and split as:

| bucket | boards | source work |
|---|---|---|
| no success history — failed reads, no validator to ask with | 32 | 530.9 s |
| skipped by the empty window (`cache_within_freshness_window`) | 83 | 0 s |
| probed and answered 304 | 14 | 19.3 s |
| `listing_only` rows that kept nothing this run | 7 | 31.8 s |

Only the 83 skipped boards are the proposal's population, and 48 of them publish no validator, so
a probe cannot help them at that boundary — only a read can teach them a tag.

## The pool

35 zero-kept boards carrying a validator from their own read, across 39 listing pages, due at the
12 h boundary. Their measured full reads, taken from the cold arm (`after-run1`) that actually
read each board:

| | |
|---|---|
| boards / pages | 35 / 39 |
| full reads | **173.7 s**, median 4.27 s, p90 7.97 s, max 15.7 s |

## The probes

The flow's own primitive, `conditional_revalidate_url` — a HEAD carrying the stored
`If-None-Match` / `If-Modified-Since` — over the pool's 39 pages, six at a time:

| | |
|---|---|
| pages answering 304 | 30 |
| pages answering 200 (changed) | 8 |
| pages answering 403 (probe unusable) | 1 |
| boards unchanged on every page | **27 / 35 (77%)** |
| probe cost | **25.7 s** (median 522 ms/request, mean 660 ms, p90 1.18 s) |

The sweep asked every page; the flow stops at the first 200 that holds a validator, so the cost
charged to changed boards below is an upper bound.

## Net

| | |
|---|---|
| 27 unchanged boards: reads 125.0 s → probes 15.7 s | **+109.3 s** |
| 8 changed boards: the probe is additive to the read they still need | **−10.0 s** |
| **net** | **+99.3 s** |
| share of the warm arm's source work | **7.2%** |
| wall-equivalent at 16 workers, if fully parallel | ~6.2 s of 92.7 s, realistically less |
| break-even | 14.8% of the pool unchanged (mean read 4.96 s vs mean probe 0.73 s) |

Per board: median read 4.16 s, median probe 0.52 s, median saving 3.5 s.

## What this does and does not buy

- **One read per empty episode, not a per-run gain.** Today the lane converges on probes after
  one confirming read: the read that keeps zero a second time lifts the streak to 2, and from then
  on each 12 h boundary is a probe whose own 304 refreshes the empty window. The proposal removes
  only that confirming read; after its 304 the board sits on the same 12 h probe cycle it would
  have reached anyway.
- **Most of the lane's spend is not recoverable.** 530.9 s of its 582 s belongs to boards with no
  success history. Their reads failed, they hold no validator, and a 304 would clear the failure
  without ever fetching them.
- **It is still positive at plausible churn.** The probes here ran ~2 h after the read (23% of the
  pool changed); a 12 h boundary should see more churn, and break-even is 14.8% unchanged.

So the plan's *Zero-kept sources dominate cost* row is a statement about a population, not about
recoverable work: worth doing for ~100 s an episode, but not where a run-level win lives.

**Shipped after this measurement.** A zero-kept board that has a validator now takes its first
due check as a HEAD, and a 304 satisfies the empty window: the row keeps its streak, sleeps 12 h
as `static_empty_fresh`, and probes again at the next boundary. A board with no validator still
reads in full, and a 200 falls through to the read exactly as before. Live check on three pool
boards: two answered 304 (excluded, no body read, parked for the empty window) and one answered
200 (full read, streak 1 → 2, parked by its deadline).

**Run-level check, same day.** Two A/B pairs over the same 200-board sample and the same aged
state (every clock 15 h back, so all boards are due), `ae6148a9` vs `5be089ef`, opposite run
order:

| pair | wall | source work | `notModifiedCount` |
|---|---|---|---|
| 1 | 406.7 → 294.4 s | 5 150.7 → 3 847.9 s | 14 → 36 |
| 2 | 323.5 → 189.0 s | 3 663.0 → 2 308.0 s | 13 → 40 |

The 35-board cohort the change moves from a read to a HEAD answered 304 on 22 boards in pair 1
(678.9 s of reads → 51.1 s of HEADs) and 25 in pair 2 (374.6 s → 31.0 s); the rest answered 200
and read as before, so the probe was additive there. Pair 1's read-speed control (boards read
in both arms) is 1.00, which makes its ~628 s the change-attributable work; pair 2's window ran
up to twice as fast overall, so its raw deltas overstate it. The totals move further than the
cohort because per-board costs here run to 100+ s, so one slow board landing in one window
moves the sum more than 35 HEADs do. Yield held: the cohort kept nothing in either arm, and
the only output dip came from one LinkedIn board that answered `js_required` in both after runs
while succeeding alone from the same seed through the same tree — a flaky redirect, worth
~60-85 s of the measured delta.

## Artifacts

Local and gitignored (`.tmp/run-level/`): `zero_pool.py` (pool and read costs), `zero_recover.py`
(pool sizing), `zero_sweep.py` (live probes), `zero-pool.json`, `zero-sweep.json`, and
`RESULTS.md` (the run-level A/B this sample comes from). The probe's own A/B lives in
`.tmp/probe-run-level/`: `measure.py`, `RESULTS.md`, the two pairs' reports, and the `ae6148a9`
worktree it ran the before arm from.

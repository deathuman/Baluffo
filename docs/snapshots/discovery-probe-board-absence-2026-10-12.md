# The probe stage stops re-asking a board that said it does not exist — 2026-10-12

> - **Class:** measurement + one shipped cut, with one lever refused on a counterexample
> - **Trigger:** [`snapshots/discovery-lane-cost-2026-10-11.md`](discovery-lane-cost-2026-10-11.md) ranked
>   **T1: the GameDevMap active audit** (950.9 s uncached, probe phase 285.9 s / 30%) as the lane's
>   largest item, and named its probe phase as the place the audit's clock is spent.
> - **Status:** shipped — **−35.7%** probe requests, **−42.5%** probe work, **−38.1%** probe-phase wall
>   clock on the audit's own candidate population, with 1,692 of 1,692 Greenhouse/Lever verdicts
>   unmoved
> - **Verified against:** `9b8901f8` (the change in this snapshot is uncommitted when it was measured)
> - **Then inspect:** [`discovery-lane-cost-2026-10-11.md`](discovery-lane-cost-2026-10-11.md) for the
>   lane ranking this acts on, [`../scraping-pipeline.md`](../scraping-pipeline.md) for the probe stage,
>   `src/source_discovery/probe.py` for the rule

## What the audit's probe phase actually spends its requests on

The audit probes **every candidate a batch produces** — the directory's own careers URLs, the provider
boards its recovered pages name, and a set of *pattern guesses* built from studio names
(`recovery_escalation`: four provider candidates per studio whose same-party recovery found nothing).

A 25-studio slice (batch size 25, live, pinned CSV) shows the mix and the cost shape:
**65 candidates probed, of which 60 (92%) are `recovery_escalation` guesses** — Greenhouse ×2 slug
spellings, Lever, SmartRecruiters or Workable per studio — and **43 of the 65 fail, every one of them
on `HTTP Error 404`**.

Instrumenting the audit bench's own per-phase keys on that slice (`report-baseline*.json`, one 25-row
batch each, three unthrottled runs plus the first throttled one):

| Phase, 25-studio slice | Cost, 3 clean runs | Throttled first run |
|---|---|---|
| probe | 2,021 / 2,542 / 2,810 ms (30–43% of the batch) | 11,724 ms (60%) |
| homepage fetch | 3,873 / 1,754 / 1,899 ms | 1,878 ms |
| recovery waves 1+2 | 1,554 / 1,600 / 1,766 ms | 5,596 ms |
| analyses, merge, direct inference | 340 / 359 / 404 ms | 361 ms |

The single largest line inside the probe phase was **the second request each failing candidate sends**:
a candidate's own provider endpoint is probed first (`endpoint_url` = `api_url`/`feed_url`/`board_url`),
and when that fails, `_fallback_probe_url` builds the provider's human-facing page from *the same*
`slug`/`account`/`company_id` and probes that too.

Measured across the 65 candidates: **107 requests, of which 42 (39%) were a sibling page attempted after
the endpoint had already answered 404 — and all 42 answered 404 as well.** None of the 42 changed a
verdict; they are 39% of the phase's requests spent restating an answer the board had already given.

## The cut: a definitive not-found settles the board

`_settles_the_board` now ends a candidate's probe when a **404/410** arrives from a URL carrying that
candidate's own provider token, **for the adapters whose listing endpoint answers authoritatively for
the board** — `_AUTHORITATIVE_BOARD_ENDPOINTS = {greenhouse, lever}`. A timeout, a 403 or a 5xx still
tries the sibling: those say the URL could not be read, not that the board is gone.

**Workable is deliberately excluded, and a counterexample is why.** Its widget API answers 404 for
`abhitechgames` while `https://apply.workable.com/abhitechgames` serves that board — reproduced
directly, and it was the one live case (in 244 candidates) where the sibling request changed a verdict
(zero-jobs → probe-failed). An adapter joins the set only on that evidence; trying the sibling remains
the default for everything else.

A side effect worth having: the reported failure reason now names the URL discovery actually asked
(`boards-api.greenhouse.io/v1/boards/<slug>/jobs`), where before it named the sibling page the studio
never advertised. Measured on the 244-candidate pair below: **62 of the 244 candidates name a different
URL** in their failure reason than the baseline did, and 63 changed their reported reason at all — the
odd one out is a throttled `static` candidate, not this change.

## The A/B, on the audit's own candidate population

The population is real: an uncached audit run (3,000 studio rows → 3 batches, pinned CSV, isolated
output) that **the audit itself timed at `probeMs` 108,141 ms inside a 456,073 ms job**, with 2,935
probed candidates, 1,774 probe failures, 92 active and 1,070 zero-job candidates. Its `allCandidates`
(2,935 rows) was then probed by both code versions, alternating, under the production probe path (the
default fetcher's async-httpx branch, which is what the orchestrator passes).

| Arm (same 2,935 candidates) | URL attempts | probe work (Σ candidate ms) | phase wall | failures | job hits |
|---|---|---|---|---|---|
| pre-change #1 | 4,727 | 1,895.8 s | 58,119 ms | 1,828 | 88 |
| pre-change #2 | 4,754 | 2,063.7 s | 59,626 ms | 1,819 | 88 |
| post-change #1 | 3,052 | 1,141.0 s | 36,384 ms | 1,839 | 87 |
| post-change #2 | 3,044 | 1,135.3 s | 36,508 ms | 1,830 | 88 |
| **delta (median)** | **−35.7%** | **−42.5%** | **−38.1%** | +0.6% | −0.6% |

The arms do not overlap on the wall clock (58.1/59.6 s against 36.4/36.5 s) and the request count is
near-deterministic (4,727 vs 4,754 across two identical baseline arms). Verdict drift is rate-limit
noise, and it can be shown to be exactly that:

- **1,692 of 1,692 Greenhouse and Lever verdicts are identical — in *every* pair of arms**, not only
  the adjacent ones, and 1,689 of those candidates fail in the baseline. The change removed 1,675
  sibling requests in the first baseline→candidate pair (1,710 in the second), and not one verdict of
  the adapters it targets moved.
- Every remaining verdict difference is `429 Too Many Requests` or `500` on `static` and `workable` —
  the two adapters the change does not touch, one of them excluded *because* the same test showed it
  rescuing a board. Pairwise difference counts over the four arms: 17, 6, 24 and 13.
- That is measurable without the change at all: **the two pre-change arms differ by 15 verdicts**, every
  one of them a 429. Throttling is present in all four arms (98–119 429 verdicts each), so it is a
  property of this host's rate limit rather than of running an arm second.
- The `failures` and `job hits` deltas are inside that same noise: medians 1,823.5 → 1,834.5 (+0.6%)
  and 88 → 87.5 (−0.6%).

Scaled to the audit's own per-phase key: `285,903 ms × 0.381 ≈ 109 s` of the 950.9 s job (**≈11%** of
the whole audit, **38%** of its probe phase). That is arithmetic on the measured phase fraction, not a
second full-audit run — see the limits below.

## What this does *not* claim: the probe phase is a tail, and a smaller batch hides the win

The cut removes requests and work; whether that shows up as wall clock depends on whether the phase is
**throughput-bound** (many waves of candidates at the concurrency cap) or **tail-bound** (one candidate
holding the clock). Measured both ways on the same change:

| Population | Requests | Work (Σ candidate ms) | Phase wall |
|---|---|---|---|
| 65 candidates (≈2 waves) | 107 → 65 (−39%) | 48.8 → 24.2 s | 4,187 → 1,461 ms (2 runs each) |
| 244 candidates (≈6 waves) | 307/308 → 245/246 (−20%) | 185.0/163.0 → 113.8/129.6 s | 13,417/22,482 → 13,420/22,252 ms — **no separation** |
| 2,935 candidates (≈73 waves) | 4,740 → 3,048 (−35.7%) | 1,980 → 1,138 s | 58,873 → 36,446 ms (−38.1%) |

At 244 candidates the wall clock *is* one candidate, and it is the same candidate in all four runs: a
`static` board (`abigames.com.vn/careers`) that no provider endpoint answers for. Its own duration was
**13,081 ms in the baseline and 13,134 ms in the candidate run** of pair A, and **22,197 ms / 21,993 ms**
in pair B, against phase walls of 13,417 / 13,420 and 22,482 / 22,252 ms. The change does not touch
`static`, so it cannot move that number — and it did not: the arms are 3 ms apart in pair A and 231 ms
apart in pair B while the request count falls a fifth.

The audit's own per-phase key says the same thing at that scale. Its `probeMs` for a 100-row batch
(244 candidates probed) is **8,165 / 9,150 ms** on 394 requests before the change and **22,481 /
13,783 ms** on 242 requests after it — the candidate arms are not slower *because* they ask less (the
same single board's latency moved from 13.1 s to 22.0 s between the pairs above), but the audit's own
clock is clearly not a function of the request count here. The 25-studio slice agrees: 107 attempts
fall to 64 while `probeMs` stays inside its own spread (2,021–2,810 ms clean, 11,724 ms throttled;
1,349–3,043 ms candidate).

The audit's full-run probe phase is 285.9 s for ~9,172 probes — **13× its largest measurable
single-candidate tail** — which is why it is on the throughput-bound side of that line, but the
244-candidate rows are the honest counterexample and the reason this snapshot reports a per-population
result rather than a single number.

**The tail is now the lane's next target, not this change's claim** — attacked the same day in
[`snapshots/discovery-probe-timeout-ladder-2026-10-12.md`](discovery-probe-timeout-ladder-2026-10-12.md),
which counted the ladder from the attempt logs kept here (2 rescues, both on the second attempt; 0 of 5
third attempts) and bounded a timed-out probe URL at one retry. The slow candidates mostly *fail*,
and they fail on the retry budget (3 attempts × `timeout_s` + 1.2/2.4 s backoff), which is the shared
fetch policy — so cutting it is a policy decision with coverage consequences, not a free one. A cut on
the request frequency was available and verdict-neutral; the tail is not, and it is what actually holds
the audit's probe phase.

## Limits

- The full uncached audit was **not** run end to end for this snapshot. The A/B uses the audit's own
  candidate population from 3 real batches and reproduces the phase (the same
  `run_bounded_probe_batch_async` over the same candidate list) rather than the whole 9-batch job; the
  audit's `probeMs` 285.9 s for the full job is the historical anchor, and the 109 s figure is scaled
  from the measured fraction. The 244-candidate rows are the artifact's first 244 `allCandidates` rows
  (ordered by candidate id, so `static`-heavy — 307–308 baseline requests rather than the 394 a
  CSV-ordered batch of the same size produces), and they are reported as their own population rather
  than folded into the headline.
- The audit bench's own probe fetcher is a Python callable, which takes the probe batch's
  worker-thread branch (≈12 concurrent) instead of the production async branch (40) — that is why the
  audit's recorded `probeMs` for those 3 batches (108.1 s) exceeds the 58.1 s the same population costs
  under the production path. Both arms of the A/B use the production path.
- Board pages and provider APIs are live: the arms were run minutes apart against a shared IP, so
  `429`/`500` noise exists in both (quantified above) and the post-change failure count is +0.6%.
- The web-search query benchmark is still unusable from this host (`HTTP 403` on both arms), so
  nothing here touches that lever.

## Reproducing the numbers

```bash
# the audit's own per-phase totals for the 3-batch population (isolated output; ~12 min live)
python _out/perf-runs/discovery/audit_probe_bench.py --arm full-baseline \
    --batch-size 1000 --max-batches 3 --timeout 6

# the arms need the pre-change tree beside the current one, so the baseline arm runs a worktree:
#   git worktree add _out/baseline-wt 9b8901f8
#   mkdir -p _out/baseline-wt/_out/perf-runs/discovery
#   cp _out/perf-runs/discovery/probe_urls_bench.py _out/baseline-wt/_out/perf-runs/discovery/
ROOT="$(git rev-parse --show-toplevel)"
POP="$ROOT/_out/perf-runs/discovery/population/audit-full-baseline.json"
OUT="$ROOT/_out/perf-runs/discovery/probe-runs"

# full population: baseline from the worktree, candidate from the current tree, twice each
(cd "$ROOT/_out/baseline-wt" && python _out/perf-runs/discovery/probe_urls_bench.py \
    --arm b1 --candidates "$POP" --limit 0 --out-root "$OUT")
python _out/perf-runs/discovery/probe_urls_bench.py --arm c1 --candidates "$POP" --limit 0

# the 244-candidate counterexample: the artifact's first 244 rows, both arms, twice each
(cd "$ROOT/_out/baseline-wt" && python _out/perf-runs/discovery/probe_urls_bench.py \
    --arm pop244-base-a --candidates "$POP" --limit 244 --out-root "$OUT")
python _out/perf-runs/discovery/probe_urls_bench.py \
    --arm pop244-cand-a --candidates "$POP" --limit 244 --out-root "$OUT"

# every delta and verdict claim in this snapshot, re-derived from the raw arm reports
python _out/perf-runs/discovery/verify_probe_ab.py

# the shipped guards (the two 404 guards fail on the pre-change tree)
python -m pytest tests/source_discovery/test_probe_failures.py -q
```

The four full-population A/B reports and the population artifact are kept under
`_out/perf-runs/discovery/population/` (untracked); the 244-candidate arms, the 65- and 25-studio arms
and the audit-bench reports are under `_out/perf-runs/discovery/probe-runs/` and
`_out/perf-runs/discovery/audit-runs/`. `verify_probe_ab.py` recomputes the table above from them and
fails if any number moves.

The dev harnesses live under `_out/perf-runs/discovery/` (untracked) on purpose: a `scripts/` edit is
shipped container code and trips the release gate, and these are measurement, not product behaviour.

# The homepage stage is lane-bound, not straggler-bound — 2026-10-12

> - **Class:** measurement, with one lever priced live and left unshipped
> - **Trigger:** the recovery wave's clock turned out to be three stragglers, not 364 requests. The
>   homepage fetch is the largest stage by a hair (**105,744 ms** against wave 1's 104,423 ms in the
>   same 362,059 ms run, plus 39,678 ms analysing what it read), so it got the same decomposition.
> - **Status:** answered — and it is the opposite shape. The 3,000 homepages are **volume**: the phase
>   wall is **1.84×** `Σ latency / 32 lanes` at the configured concurrency, its batches feed lanes for
>   **82-83%** of their wall, and only **25 of 3,000** requests (0.8%) outlive the 6 s budget. The
>   recorded occupancy (**17.4 of 32 lanes**) sits on the audit bench's **20-thread** sync-fallback
>   ceiling, and today's replay of that path peaks at exactly 20. Replaying the same 1,000-homepage
>   batch on the shipped async path costs **21.2-22.5 s** against the recorded **33.8 s**, and at 64
>   lanes **16.2 s**.
> - **Verified against:** `9b8901f8` plus the uncommitted board-absence, timeout-ladder,
>   secondary-wave and wave-floor work. Nothing here changes shipped behaviour.
> - **Then inspect:** `src/source_discovery/directory_fetch.py` (`fetch_directory_pages` and the
>   `fetcher is fetch_text` branch that picks async vs the `asyncio.to_thread` fallback),
>   `src/source_discovery/gamedevmap_active_dry_run.py` (`_fetch_gamedevmap_active_homepages`),
>   and `activeAuditHomepageFetchConcurrency` / `perHostConcurrency` in
>   `src/source_discovery/config.py`.

## Where the 105,744 ms goes

Recorded arm (`secondary-cut-narrow`), three batches of 1,000 homepages at
`activeAuditHomepageFetchConcurrency` **32**, per-host **3**, `timeout_s` **6**:

| Batch | Wall | Σ request time | Lanes busy | Mean latency | p50 | Read (HTTP 200) | Over 6 s | Lanes fed for |
|---|---|---|---|---|---|---|---|---|
| 0 | 33,834 ms | 579,463 ms | 17.1 of 32 | 579 ms | 270 ms | 764 (**76%**) | 8 rows | 82% of the wall |
| 1 | 36,788 ms | 639,797 ms | 17.4 of 32 | 640 ms | 295 ms | 785 (**79%**) | 10 rows | 83% |
| 2 | 35,122 ms | 620,368 ms | 17.7 of 32 | 620 ms | 319 ms | 777 (**78%**) | 7 rows | 83% |
| **phase** | **105,744 ms** | 1,839,628 ms | **17.4 of 32** | 613 ms | — | 2,326 (**78%**) | 25 rows | — |

The phase's clock is `Σ latency / lanes`, not a straggler: at the configured 32 lanes the work would
fit in **57,488 ms** and the phase takes 1.84× that, while the tail is a rounding error — 25 rows over
6 s (0.8%), 80 over 3 s (2.7%), and the slowest 100 requests own **23%** of the summed latency with
the other 2,900 owning 77%. Each batch's last completion *starts* 27.8-30.4 s into a 33.8-36.8 s wall,
so the queue is still feeding lanes almost to the end.

## The discriminator against the recovery wave

The wave-2 snapshot's central fact was that a batch's wall *was* one request — its last completion
started 7-2,837 ms in and then ran to 8,628 ms. That is not a property of fetch phases; it is a
property of a **job-starved** phase, and the secondary-wave cut is what starved it. Measured as the
share of a batch's wall spent still feeding lanes:

| Phase | Batch | Lanes fed for | Shape |
|---|---|---|---|
| Homepage fetch | 0 / 1 / 2 | **82% / 83% / 83%** | volume: the queue never drains |
| Wave 2, **post-cut** | 0 / 1 / 2 | 25% / 31% / **0.1%** | one request, then a quiet tail |
| Wave 2, **pre-cut** | 0 / 1 / 2 | **75% / 72% / 90%** | volume again (388-460 requests per batch, occupancy 16.9-19.1 of the same 20-thread fallback) |

So the cut moved that phase across the boundary: it went from a lane-bound phase to a job-starved one,
which is why its remaining wall had to be attacked with a deadline rather than with lanes. The
homepage stage is still on the lane-bound side, and the pre-cut wave is the control that shows the
shape follows the job count, not the phase.

## Why the lanes stop at 20

The recorded occupancy (17.4 of 32) is the audit bench's, not the network's. `fetch_directory_pages`
takes its async httpx branch only when the fetcher **is** `fetch_text`; the bench passes an
instrumented callable, so its batches fall to `asyncio.to_thread`, whose default executor is
`min(32, cpu_count + 4)` = **20** threads on this 16-CPU box. Replaying one recorded batch today:

| Arm | Lanes configured | Mean in flight | Peak | Wall (1,000 homepages) | Σ latency | Wall ÷ ideal | Read | certificate failures | 429s |
|---|---|---|---|---|---|---|---|---|---|
| `sync-20` (the bench's path) | 32 | 18.1 | **20** | 42,090 ms | 763,774 ms | 1.76× | 761 | **187** | 14 |
| `async-32` (shipped path) | 32 | 25.2 | **32** | 22,510 ms | 566,202 ms | 1.27× | 933 | 8 | 9 |
| `async-32-r2` | 32 | 24.9 | **32** | 21,223 ms | 528,296 ms | 1.29× | 888 | 8 | 56 |
| `async-64` (config counterfactual) | 64 | 43.1 | **64** | 16,169 ms | 696,379 ms | 1.49× | 886 | 8 | 57 |

Honest precision: unlike the wave-2 phase, this does **not** reproduce the recorded wall to a few
percent — the sync replay takes 42,090 ms against the recorded batch's 33,834 ms (+24%), with the same
job list and the same 1,000 rows. What the replay does establish is the ceiling's existence and shape:
the recorded phase's occupancy lands at 87% of the 20-thread fallback, today's run of that path peaks
at exactly 20 of 32 configured, and the async arms reach 32 and 64 at once. So the recorded 105,744 ms
is the fallback's ceiling *plus that day's weather*; the shipped path at 32 lanes costs 21.2-22.5 s
for the same batch on today's weather, i.e. ≈65 s for the phase, and 64 lanes ≈48 s.

The lever for this stage is therefore **lanes**, and lanes still pay at 64 (−25% for the batch, mean
43 of 64 in flight). The guardrail is throttling, not time: 429s went 9 → 56-57 across the arms, though
the 32-lane repeat saw 56 without a 64-lane arm being involved, so this replay does not show new
throttling — it shows the 429 rate is noisy enough that a lane increase needs its own watch.

## The tail here is productive, so the wave's fix would cost reads

A hard per-request deadline was free on the recovery wave (all six kept pages read, the over-budget
rows all failures). Here it is not: of the **80** recorded rows over 3 s, **57 are successful reads**
(2.5% of the 2,326 the phase reads), including the slowest rows of batches 1 and 2 (7,052 ms and
7,521 ms, both HTTP 200). Capping the budget barely helps — a 6 s cap removes 0.4% of the summed
latency, a 3 s cap 7%, a 2 s cap 14% — while a 1 s cap removes 30% of the work and would cut those
57 reads. The same mechanism has opposite prices on the two phases, which is the reason the wave-2
snapshot left its deadline unshipped.

## Found in the same replay: the fallback's trust store costs a fifth of the batch

The `sync-20` arm fails **187 of 1,000** homepages with a certificate error where the async arm fails
**8**, and reads **761** pages where the async arm reads **888-933** (+17% to +23%). The recorded run —
which took the fallback path — counted 512 certificate failures across the phase (188 / 154 / 170 per
batch), i.e. **~516 homepages per run dropped by the sync store alone**. That is the previous
snapshot's trust-anchor finding, reproduced here end-to-end as readable pages rather than as a
40-URL probe, and it is a coverage lever several times the size of any timing lever in this document.
It remains unshipped: it needs the shared fetchers' tests, and it is a repair rather than a cut.

## Limits

- **One batch replayed live** (batch 0, 1,000 URLs) across four arms; the phase figures are that
  batch's shape, and the recorded per-batch totals above come from the retained run, not from the
  replay.
- **The sync arm does not reproduce the recorded wall** (42,090 vs 33,834 ms, +24%). Today's hosts were
  slower on that path — its p50 is 414 ms against the recorded 270 ms — so the ceiling claim rests on
  the occupancy coincidence and the peak-in-flight identity, not on reproduction.
- **`async-64` is a counterfactual config**, passed to `fetch_directory_pages` as an argument; nothing
  in `config.py` changed, and 429 counts say the politeness cost of raising it is not settled here.
- **Live hosts move between arms.** The two 32-lane arms differ by 1.3 s of wall and 45 readable
  pages, which bounds how much of the 64-lane gain is signal.
- **The stage is bigger than this fetch.** Its analysis step (39,678 ms, CPU work on the same pages) is
  untouched here, and the two together are ~40% of the run.

## Reproducing the numbers

```bash
# the per-batch decomposition from the retained arm (phase, lanes, budget are arguments)
python _out/perf-runs/discovery/analyze_phase_floor.py \
    --phase homepage_fetch --lanes 32 --budget 6000

# the four live arms over one recorded batch (~2 min, 4,000 requests against live hosts)
python _out/perf-runs/discovery/homepage_floor_bench.py --batches 0

# every claim above, re-derived from the artifacts (fails if one moves)
python _out/perf-runs/discovery/verify_homepage_floor.py
```

The arms, their log (`homepage-floor-arms.log`) and the recorded arm they replay are kept under
`_out/perf-runs/discovery/` (untracked). The harnesses live there on purpose: a `scripts/` edit is
shipped container code and trips the release gate, and these are measurement, not product behaviour.

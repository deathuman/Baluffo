# The post-cut arm is 651-687 s: the worker fell to 48 s and is fully hidden, so the phase cuts bought no arm wall — 2026-10-28

> - **Class:** measurement — two production-setting full-population arms on the current build; no
>   shipped behaviour changes.
> - **Trigger:** the last full-arm reading predates the five output-identical phase cuts, and the
>   body-cap study closed the phase's last lever. The question was whether the ~98 s those cuts took
>   out of the worker reached the arm, and whether the worker is still hidden.
> - **Status:** **it is, and they did not.** On the identical **8,370**-row population (digest
>   `66165df36a10c023`, CSV `896da851…`, `remainingUrls` 0) the two arms read **686.7 s** and
>   **650.8 s** (mean **668.8**, spread **35.9**) against the overlap shipment's **622.3 / 635.6 s**
>   — not lower, and the pair's own spread is as large as the difference. The worker is **48.6 /
>   47.4 s** (was **146.4 / 148.6 s**), **7.1 / 7.3%** of the arm, and the parent waits **94 / 92 ms**
>   in total (**max 18 / 16 ms**), with **0 fallbacks** and **9/9** child batches. Every batch's
>   worker wall is smaller than the next batch's homepage fetch alone (smallest margin **4.4 s**), so
>   the worker is **fully hidden**: its **98-101 s** of cuts are off the critical path by construction.
> - **What the arm is made of now:** homepage fetch **245.4 / 210.2 s**, probe **210.2 / 211.2 s**,
>   recovery waves and analyses **224.2 / 224.1 s**, parent analysis **0.17 s**, merge/direct
>   inference **~0.1 s**. The movement against the overlap arms is all network — the probe is
>   **+47.3 / +39.2 s** and homepage fetch **+33.8 / −7.8 s**, partly offset by recovery analysis
>   **−8.6 / −6.5 s**.
> - **Verified against:** `45220c2e` (`main`) + docs-only working changes; live GameDevMap CSV
>   sha256 `896da851155b2da0b41b96da567134cc63e9eba7765bdba1b4485cbf3168a31a`; 32 lanes, per-host
>   3, timeout 6, 1,000-row batches, whole eligible population; arms
>   2026-10-28T14:37:37–14:59:58Z.
> - **Then inspect:** `_out/perf-runs/discovery/post-cut-arm-20261028/` — `design.md` (the
>   pre-registration), `run_arms.sh`, `arms/post{1,2}/arm.json`, each arm's
>   `data/gamedevmap-analysis-worker-receipt.json`, the artifact, `analyze.py`, `analysis.json`, and
>   the two arm logs.

## What was run

Two child arms, the shipped transport explicit, in order **post1 → post2**, at the production
acceptance settings: `--lanes 32 --batches 0 --batch-size 1000 --timeout 6 --no-trace`. Each arm is
isolated under its own `BALUFFO_DATA_DIR`; the whole eligible population is read from the live
GameDevMap CSV. No inline control was run: the overlap effect is already settled, and the questions
here are the current wall and the current margin.

The workload is identical to the overlap arms — same population count, digest, and CSV sha256 — so
the wall difference is not a different population.

## The wall

| Arm | Wall | vs 622.3 | vs 635.6 | Worker wall | Worker share | Max parent wait |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| post1 | **686.7 s** | **+64.4 s** (beyond 4.9%) | **+51.1 s** (beyond) | 48.6 s | 7.1% | **18 ms** |
| post2 | **650.8 s** | +28.5 s (inside) | +15.2 s (inside) | 47.4 s | 7.3% | **16 ms** |
| overlap p1-child | 622.3 s | — | — | 146.4 s | 23.5% | ~10 ms |
| overlap p2-child | 635.6 s | — | — | 148.6 s | 23.4% | ~10 ms |

The pair mean is **668.8 s**, **+39.8 s** above the overlap pair's **629.0 s**, and the pair's own
spread (**35.9 s**) is the same size as that difference. So the honest reading is **not "the arm got
slower"**: one arm is beyond the 4.9% bound and one is inside it, and the spread is the bound. What
the pair does establish is the direction: **the arm is not lower** despite a worker that is three
times smaller.

## Where the wall is

| Phase | post1 | post2 | overlap p1 | overlap p2 | post1 − p1 | post2 − p2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| homepage fetch | 245.40 s | 210.24 s | 211.59 s | 218.06 s | **+33.8 s** | −7.8 s |
| recovery wave 1 fetch | 154.00 s | 150.22 s | 166.73 s | 156.60 s | −12.7 s | −6.4 s |
| recovery wave 2 fetch | 57.70 s | 61.93 s | 53.09 s | 63.60 s | +4.6 s | −1.7 s |
| probe | 210.20 s | 211.21 s | 162.94 s | 172.06 s | **+47.3 s** | **+39.2 s** |
| recovery wave 1 analysis | 12.14 s | 11.71 s | 20.71 s | 18.18 s | −8.6 s | −6.5 s |
| recovery wave 2 analysis | 0.44 s | 0.33 s | 0.63 s | 0.44 s | −0.2 s | −0.1 s |
| homepage analysis (parent) | 0.17 s | 0.17 s | 0.24 s | 0.20 s | −0.1 s | 0.0 s |
| direct inference / merge | 0.08 s | 0.12 s | 0.18 s | 0.18 s | −0.1 s | −0.1 s |
| **phase sum** | **680.1 s** | **645.9 s** | **616.1 s** | **629.3 s** | **+64.0 s** | **+16.6 s** |
| arm wall | 686.7 s | 650.8 s | 622.3 s | 635.6 s | +64.4 s | +15.2 s |

The two phases that moved are the network-heavy ones: the probe in both arms (**+39-47 s**) and the
homepage fetch in one (**+33.8 s**). The recovery analysis is now **~12 s** where it was **18-21 s**,
consistent with the link index also compressing that phase. The parent's `homepage_analysis`
row is **0.17 s** in both arms: it is the collect, not the work.

## The worker is fully hidden

| Arm | Mode | Batches | Fallbacks | Worker wall | Worker CPU | Handoff | Wait total | Max wait |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| post1 | child | 9 | 0 | **48.59 s** | 48.23 s | 1,077 MB | **94 ms** | **18 ms** |
| post2 | child | 9 | 0 | **47.42 s** | 47.05 s | 1,056 MB | **92 ms** | **16 ms** |

Per-batch corroboration: the worker's wall for batch *n* against the homepage fetch of batch *n+1*
(the head it overlaps) leaves a margin of **8.4 s at the tightest** in post1 and **4.4 s** in post2
(batch 8 both times); every other batch has 17-28 s of slack. The last batch has no next head, but
its worker is **1.8 / 2.2 s** and the parent's wait is **0 ms**. The worker's own wall fell
**97.8 / 101.2 s** (**−66.8% / −68.1%**) from the overlap shipment while its handoff stayed
**1.08 / 1.06 GB**.

This is the pre-registered hidden test passed on both counts: max `waitMs` **18 / 16 ms** (threshold
100 ms) and worker share **7.1 / 7.3%** (threshold 15%).

## What this settles

**The five phase cuts are headroom, not arm wall.** The worker lost ~98-101 s and the parent never
waited more than 18 ms in any batch; by the pipeline's own shape, that time cannot appear in the arm.
The arm is now **650.8-686.7 s** on the production population, and it is made of:

* **homepage fetch** — **210-245 s** (32-36%);
* **probe** — **210-211 s** (31-33%), the largest single phase in both arms;
* **recovery waves and analyses** — **224 s** (33%);
* **analysis** — **48 s of worker CPU, fully hidden**; the parent's collect is **0.17 s**.

The body-cap verdict stands unchanged: no cap belongs, because the phase's savings are worker
headroom while the cap's costs are lost candidates and main-process recovery work. And the next
lever is now unambiguous: **the probe and the recovery/fetch path**, not the phase. The probe's
**+39-47 s** movement between windows is also the warning: at ~210 s and 31-33% of the arm, it is the
largest target but the hardest to resolve, because its wall is network weather.

## Limits

* Two arms, one window, live network. The pair's spread is **35.9 s** (5.4% of the mean), so the
  arm-level difference against the overlap arms is **not** resolved; the hidden test is the strong
  reading, and it is a within-arm fact.
* The population is identical (count, digest, CSV sha256, `remainingUrls` 0), but the two arms
  fetched different page sets live — homepages **7,488 vs 7,418**, failures **12,445 vs 12,176**,
  active candidates **356 vs 359** — so no output parity is claimed and none is used to judge the
  wall.
* The worker receipt's `beginMs` is not used: it is not monotonic in the recorded batches, and the
  hidden test rests on `waitMs`, the worker share, and the next-head margins instead.
* The probe's excess is not attributed beyond "network window": `probedCandidates` is **9,031 /
  9,042** against **8,996** in the overlap p1 arm, so the population probed is essentially the same;
  the per-candidate wall is what moved.
* The arms ran on the live CSV as of 2026-10-28T14:37Z; a later CSV or a different window is a
  different workload and should be re-measured, not scaled.

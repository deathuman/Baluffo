# The host hoist: 3.3 s off the provider walk, 191× the memo it replaces — 2026-10-28

> - **Class:** change + measurement — removes the repeated host normalization from the
>   provider-admission walk and prices it against the memo it replaces.
> - **Trigger:** [`snapshots/discovery-homepage-analysis-memos-2026-10-28.md`](discovery-homepage-analysis-memos-2026-10-28.md)
>   shipped the memo at **27.2 ns a call** (**0.79 s** over the phase's **29.19M** calls) and named
>   the structural cut as the next increment: normalize the host once per admission instead of
>   caching the same answer eighteen times.
> - **Status:** the hoist ships. On the recorded production admission population
>   (**370,833** distinct `(host, path)` pairs, **629,560** admissions) the walk falls
>   **6.002 → 2.724 s** (**9,534 → 4,326 ns** per admission), a **3.28 s** phase saving that is
>   **191.5×** the memo's per-call **27.2 ns** and **4.2×** its **0.79 s**. Adapter verdicts are
>   identical on every admission (one digest, `719cf3cf…`), and all **twelve** phase replays
>   reproduce the reference counts *and* per-collection content digests. The wall replay cannot
>   resolve the effect (pairs **−4.60 / +1.20 s** against same-build spreads of **8.90 / 3.10 s**),
>   and two process-CPU windows do not settle it either: the first puts both pairs in the hoist's
>   favour (**−1.84 / −3.79 s**) but its same-build spread (**1.95 s**) exceeds the smaller pair,
>   and the second flips sign (**−1.66 / +1.97 s**) after the same code drifted **3.4 s** between
>   two old runs. The per-admission reading is the evidence, as it was for the memo.
> - **Verified against:** `9c28c308` in a clean worktree (`.tmp/host-hoist/before-tree`) and this
>   tree, over the captured production arm of 2026-10-27 (eight batches of 1,000 and one of 370
>   results, population digest `66165df36a10c023`), 2026-10-28.
> - **Then inspect:** `_out/perf-runs/discovery/host-hoist-20261028/` — `design.md`
>   (pre-registration, both supplementary CPU windows and the decision), `phase.py`, `micro.py`,
>   `compare.py`, `admissions.json`, `report.txt`, the four `phase-*.json` wall runs, the eight
>   `phase-cpu-*.json` runs and four `micro-*.json` runs.

## What changed

`infer_provider_adapter` walks twelve adapter rows and their eighteen domain patterns on every
embedded link of every audited page. Each pattern match used to re-normalize the same host:
`host_matches_domain_pattern(host, pattern)` called `normalized_host` on the host, on the pattern
and again on the stripped domain — about **52** calls per admission, **29.19M** over a phase.

The walk now normalizes the host **once** per admission, the pattern table is normalized **once at
import**, and the comparison goes through two new leaf helpers in [`src/url_hosts.py`](../../src/url_hosts.py):
`host_matches_normalized_domain` and `host_matches_normalized_pattern`. The normalizing helpers stay
as they were and now delegate to the text-level ones, so `probe.py` and every other caller keep
their behaviour. The memo stays too — one call per admission still benefits, and other callers
(`url_host`, the candidate builders) use it — but the walk no longer pays it fifty-two times.

## The walk, priced per admission

`micro.py` runs one tree per process over the **recorded** admission sequence — every distinct
`(host, path)` the real phase passed to the walk, with its call count — in the order
`old, new, new, old`, three rounds per process, memo warm:

| Tree | Runs (s) | Mean s | ns/admission |
| --- | --- | ---: | ---: |
| memo era (`9c28c308`) | 5.531, 6.473 | **6.002** | **9,534** |
| hoisted (this tree) | 2.651, 2.796 | **2.724** | **4,326** |

* Saving **3.28 s** per phase, **5,208 ns** per admission, **191.5×** the memo's **27.2 ns** a call
  and **4.2×** its **0.79 s** phase price.
* Largest single-round spread across the four processes is **0.296 s**: the saving is **11.1×** it.
* Verdict digests are identical on both trees (`719cf3cf…`) over the expanded **629,560**
  admissions — the cut is a rewrite, not a behavioural change.

## The replay cannot see it; CPU corroborates it

Wall, nine batches per process, `old, new, new, old`:

| Run | Tree | Total s | Batch-1 repeat s |
| --- | --- | ---: | ---: |
| 1 | memo era | 94.43 | 12.02 |
| 2 | hoisted | 99.03 | 11.68 |
| 3 | hoisted | 102.13 | 12.00 |
| 4 | memo era | 103.33 | 10.39 |

Pairs in run order: **−4.60 s** and **+1.20 s**, against same-build spreads of **8.90 s** (old) and
**3.10 s** (new). Opposite signs, inside the spread: the wall reading is **under the instrument's
resolution**, exactly as it was for the memo, which this instrument put at −2.12 s and then +3.69 s.

The phase is CPU-bound (`cpu/wall` **0.988** over 16 production arms), so a supplementary reading
was fixed before it ran: process CPU in the same order. A second window was fixed after the first
one missed its magnitude rule, to test whether that spread repeated.

| Window | Run | Tree | CPU s | Wall s |
| --- | --- | --- | ---: | ---: |
| 1 | 1 | memo era | 92.20 | 92.79 |
| 1 | 2 | hoisted | 90.36 | 91.04 |
| 1 | 3 | hoisted | 88.41 | 88.77 |
| 1 | 4 | memo era | 92.20 | 92.96 |
| 2 | 5 | memo era | 85.44 | 86.35 |
| 2 | 6 | hoisted | 87.09 | 88.31 |
| 2 | 7 | hoisted | 86.88 | 88.20 |
| 2 | 8 | memo era | 88.84 | 89.97 |

Window 1's pairs are both positive — **−1.84 s** and **−3.79 s** — and the quiet pair is within
**0.5 s** of the micro's **3.28 s**, but the smaller pair does not exceed the larger same-build
spread: **1.84 s** against **1.95 s**, the spread coming from a loaded hoisted run (its batch 1
wall was **13.72 s**, **3.9 s** above its own warm repeat). Window 2 has the opposite shape: the
hoisted runs are tight (**0.22 s** apart) but the two memo-era runs drift **3.41 s**, and the pairs
split — **−1.66 s** (hoist slower) and **+1.97 s** (hoist faster).

So process CPU is not the sharper replay instrument the **0.988** `cpu/wall` ratio suggested: the
same code drifts **3-3.4 s** between runs of one tree, which is the size of the effect. Neither
window meets the pre-registered rule; recorded as *not resolving*, not as a pass.

Output identity holds in all twelve replays: the reference counts and per-collection content digests
are reproduced exactly by both trees, and every run's warm repeat of batch 1 matches its own first
pass.

## Instrument limits

* One machine, one corpus, one session; the wall replay moved **8.90 s** between two runs of
  identical code, so it cannot see a 3.3 s effect and was not expected to.
* The microbenchmark holds the population fixed, the memo warm and the allocator idle; it prices the
  walk, not the surrounding page parse, and excludes the allocations the phase makes per admission.
* The corpus covers **370,833** distinct admissions from one production arm. Verdict parity beyond
  it rests on the unit test that compares the table against the generic per-pattern helper plus the
  unchanged builders, not on the corpus.

## Where this leaves the homepage-analysis cost

The memo took **0.79 s**; its structural replacement takes **3.28 s**, and the repeated work is
gone rather than cached. The phase's remaining attributed costs from
[`snapshots/discovery-homepage-analysis-cost-2026-10-27.md`](discovery-homepage-analysis-cost-2026-10-27.md)
are the HTML `findall` self-time (**49 s**) and the per-link inference itself, which is what the next
increment has to attack.

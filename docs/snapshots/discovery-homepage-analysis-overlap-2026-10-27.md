# Homepage analysis leaves the audit's process: −126.5 s and −94.0 s off an arm, output-identical on fixed inputs — 2026-10-27

> - **Class:** change + measurement — implements the overlap the cost snapshot priced, measures it on
>   four counterbalanced full-population arms, and pins transport parity offline where the inputs can
>   be held fixed.
> - **Trigger:** [`discovery-homepage-analysis-cost-2026-10-27.md`](discovery-homepage-analysis-cost-2026-10-27.md)
>   closed with the overlap taken out of the process as the priced next fix (up to **~129 s/arm**,
>   **17%**), leaving the hand-off — roughly **130 MB** of bodies per batch — as its one unmeasured
>   unknown.
> - **Status:** both counterbalanced pairs put the worker arm faster: **748.8 → 622.3 s**
>   (**−126.5 s, −16.9%**) and **729.6 → 635.6 s** (**−94.0 s, −12.9%**). The phase's parent-side wall
>   falls **130.5 s → 235 ms** and **139.2 s → 199 ms** — per batch **7.2–21.4 s → 2–60 ms** — while the
>   worker's own wall carries **146.4 s / 148.6 s** and the caller waits **0.10 s** in total. The
>   unmeasured unknown is paid and small: **992 MB in 1.68 s** and **1,019 MB in 1.59 s**. Both pairs
>   **fail the pre-registered output-parity reading**, because the two live arms of each pair read
>   ±1% different page sets — a fetch-stage fact, with every mover named below — so transport parity is
>   settled where inputs cannot drift: **9/9** corpus batches return identical counts *and* identical
>   per-collection content digests through the in-process analysis and the real worker, with a
>   two-run inline control stable. A frozen runtime never uses the worker, so the packaged app keeps
>   the in-process phase.
> - **Verified against:** `f60de72e` plus this change; live GameDevMap CSV sha256 `896da851…`,
>   population digest `66165df36a10c023` (**8,370** rows, `remainingUrls` 0) in all four arms; 32 lanes,
>   per-host 3, timeout 6, 1,000-row batches; arms 2026-10-27T21:48–22:34Z.
> - **Then inspect:** `_out/perf-runs/discovery/overlap-arms-20261027/` — `design.md` (pre-registration,
>   including the two discarded builds), each arm's `arm.json` and
>   `data/gamedevmap-analysis-worker-receipt.json`, `report.txt` (the pre-registered reader),
>   `report-ms-vs-s.txt` (its first run, unit-bugged, kept), `compare.py`, `parity.json` +
>   `parity-offline.log` (the offline parity), and `run.log`.

## The change

`src/source_discovery/homepage_analysis_child.py` is a per-run worker that analyzes one batch while
the audit fetches the next. The batch is split at the analysis seam: the head prepares rows, fetches
the batch's homepages and hands the analysis over, and the tail collects the result and runs
everything that consumes it (recovery waves, merge, probe). `start(results)` returns after the
hand-off — the pickling happens on a side thread — and `collect(handle)` blocks for the reply, so the
fetch of batch *n+1* and the analysis of batch *n* overlap. The worker is handed the parent's own
analysis function and its own inputs, so the result is identical by construction; any failure
(spawn refused, worker died, timeout, undecodable reply) analyzes that batch in-process and retires
the transport for the rest of the run, so the audit can lose the overlap but never a batch. The
receipt beside the artifact (`gamedevmap-analysis-worker-receipt.json`) carries the worker's own wall
and CPU per batch, the hand-off bytes and the parent's wait, because the artifact's
`homepage_analysis` phase receipt now measures only the collect.

Two instrument defects were found by runs before the measured build, fixed, and recorded rather than
quietly folded in:

* **Pairing.** A first build paired each deferred result with the batch by a stack, so with the next
  head already running the last of nine batches reported **769 s** of `homepage_analysis` in an
  **831 s** arm. The pairing is now per batch.
* **Interval.** A second build created the phase receipt at the hand-off, which puts the rest of one
  batch's tail and the next batch's head inside a phase that is not running: **95 s** of "analysis"
  per batch in a **113 s** phase. The receipt is now created at the collect, where the parent is
  actually inside the phase.

Neither correction changes the audit's work or its outputs. Both are why every measured arm runs on
this build, and every discarded reading is kept in `design.md` as corroboration rather than used as
an arm.

## The four arms

Same build, same settings, whole eligible population; the order is counterbalanced so session
weather cannot read as the effect. Phase walls in ms, from each arm's own artifact
(`summary.phaseRuntime`):

| Phase | p1-inline | p1-child | p2-child | p2-inline |
|---|---:|---:|---:|---:|
| `homepage_analysis` (parent side) | **130,506** | **235** | **199** | **139,204** |
| `homepage_fetch` | 217,743 | 211,585 | 218,063 | 208,126 |
| `recovery_wave1_fetch` | 146,895 | 166,725 | 156,597 | 146,438 |
| `recovery_wave1_analysis` | 17,952 | 20,709 | 18,180 | 18,894 |
| `recovery_wave2_fetch` | 58,806 | 53,093 | 63,602 | 60,980 |
| `recovery_wave2_analysis` | 479 | 630 | 438 | 486 |
| `probe` | 171,034 | 162,938 | 172,059 | 150,162 |
| `direct_inference` / `merge` | 123 / 32 | 152 / 31 | 142 / 39 | 139 / 34 |
| **arm wall (s)** | **748.8** | **622.3** | **635.6** | **729.6** |
| phase rows as share of wall | 99.3% | 99.0% | 99.0% | 99.3% |

The per-batch analysis walls show the same thing at batch resolution
(`timings.batches[].homepageAnalysisMs`):

| Arm | b1..b9 analysis wall, ms | Sum |
|---|---|---:|
| p1-inline | 14,942 · 15,064 · 11,206 · 14,111 · 18,096 · 19,404 · 15,189 · 15,310 · 7,181 | 130,503 |
| p1-child | 17 · 60 · 28 · 26 · 28 · 25 · 28 · 17 · 2 | 231 |
| p2-child | 20 · 24 · 25 · 25 · 27 · 28 · 27 · 18 · 2 | 196 |
| p2-inline | 21,375 · 15,445 · 16,464 · 15,128 · 16,154 · 18,630 · 14,004 · 15,113 · 6,886 | 139,199 |

The worker receipt, per pair:

| Arm | mode | batches | offloaded / calls | fallbacks | worker wall | worker CPU | parent wait | hand-off |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| p1-child | `child` | 9 | 9 / 9 | 0 | 146.37 s | 144.92 s | 0.11 s | 992,036,734 B in 1.68 s |
| p2-child | `child` | 9 | 9 / 9 | 0 | 148.57 s | 146.06 s | 0.10 s | 1,018,890,833 B in 1.59 s |
| p1-inline | `inline` (`disabled-by-env`) | 9 | 0 / 9 | 0 | — | — | 0.0 s | 0 (parent inline 130.5 s) |
| p2-inline | `inline` (`disabled-by-env`) | 9 | 0 / 9 | 0 | — | — | 0.0 s | 0 (parent inline 139.2 s) |

## Readings fixed in advance, and what they returned

1. **Workload identity — pass.** All four arms: `count` **8,370**, digest **`66165df36a10c023`**,
   `remainingUrls` **0**, one CSV sha256 (`896da851…`).
2. **Output parity, per pair — fails as written, and is reported rather than explained away.** The two
   arms of a pair differ in their downstream counts and in a handful of boards:

   | Pair | counts that differ (inline vs child) | boards |
   |---|---|---|
   | p1 | `provider` 7,321/7,296 · `static` 1,705/1,700 · `rejected` 13,481/13,463 · `zeroJob` 3,161/3,141 · `homepagesFetched` 7,355/7,334 · `recoveryPages` 2,239/2,241 · `failures` 12,061/12,002 | 341/341, shared 339; only-inline `cloudimperiumgames.com`, `www.ypuniverse.com`; only-child `arsanesia.com`, `www.farfromhomegames.com` |
   | p2 | `active` 352/353 · `provider` 7,318/7,288 · `static` 1,706/1,713 · `rejected` 13,480/13,459 · `zeroJob` 3,157/3,146 · `homepagesFetched` 7,351/7,430 · `recoveryPages` 2,236/2,249 · `failures` 11,906/12,359 | 339/340, shared 336; only-inline `boards-api.greenhouse.io|…|wargamingen/jobs`, `recruit.8ing.co.jp`, `www.ypuniverse.com`; only-child `hk-devblog.com`, `lasershot.com`, `wargaming.com`, `www.2awesomestudio.com` |

   The divergence is fetch weather, readable in the same artifacts: `homepageFetchAttempts` is
   **8,369** in all four arms and `completedUrls` is the same 8,370 URLs, while `homepagesFetched` is
   **7,355 / 7,334 / 7,430 / 7,351** and the homepage-stage failures are **1,014 / 1,035 / 939 / 1,018**
   — the arms read different page sets, so counts derived from pages cannot match. `www.ypuniverse.com`
   moves in both pairs, which no transport can do. The pair is therefore not claimed as an
   output-parity pass; the parity question is answered where the inputs are fixed (§ below).
3. **Transport fact — pass.** Child arms: `mode: child`, `offloaded == calls` (9/9), `fallbacks: 0`.
   Inline arms: `mode: inline`, `disabledReason: disabled-by-env`, `inlined: 9`, and
   `parentInlineS` **130.5 / 139.2 s** against phase walls **130,506 / 139,204 ms**.
4. **Effect — pass on both pairs.** Wall delta, and its pre-registered bounds:

   | Pair | wall delta | phase removed | half the phase wall | worker wall within one phase wall of it |
   |---|---:|---:|---:|---|
   | p1 | **−126.5 s** (−16.9%) | 130.3 s | 65.3 s | 146.4 vs 130.5 s ✔ |
   | p2 | **−94.0 s** (−12.9%) | 139.0 s | 69.6 s | 148.6 vs 139.2 s ✔ |

   In p2 the arm delta is **45 s smaller** than the phase removed, because that child arm's *other*
   phases ran **43.9 s slower** than its inline partner's (probe 172.1 vs 150.2 s, `recovery_wave1_fetch`
   156.6 vs 146.4 s) — session weather inside the same-build drift bound the design recorded
   (**4.9%**, i.e. tens of seconds). The effect is well outside it in both pairs.
5. **Attribution — pass.** The phase's parent-side wall is gone, the worker's wall carries it, and no
   work is skipped: `homepage_fetch` reports **8,369** items in all four arms, every phase keeps its
   `items` and its share of the wall (99.0–99.3% coverage), each arm's analysis receipt has
   **occurrences 9** (one per batch), and every batch's span is its own — the mispaired build's
   **769 s** and the mis-intervalled build's **95 s/batch** do not appear.

## The reader's own bug, disclosed

`compare.py` was written before the arms ran, and its first run returned `supported: false` for both
pairs for a reason that has nothing to do with the effect: its criterion compared the wall delta in
**seconds** against *half the phase in **milliseconds*** (126.5 > 65,253). The pre-registered text is
same-unit (*"faster by more than half the inline arm's own `homepage_analysis` parent-side wall"* and
*"`childWallS` within one inline phase-wall of that wall"*); the reader now computes that, and its
as-run output is kept as `report-ms-vs-s.txt`/`report-ms-vs-s.json` beside `report.txt`/`report.json`
so the correction is visible rather than silent. The bug could only ever produce `false`, so it never
inflated a claim.

## Parity where the inputs are fixed

The live pairs cannot test what the worker does to the output, because they fetch different page
sets. `parity_offline.py` holds the inputs still: it replays the captured corpus of one production
arm (**8,369** results, **7,467** readable, digest `66165df36a10c023`) and runs each batch twice —
once through the in-process analysis, once through the real `HomepageAnalysisChild.start`/`collect` —
comparing each result's counts *and* the content digest of every collection it produces (providers,
static candidates, rejected rows, primary and secondary recovery jobs, browser recovery), so a moved
row is visible and not only a lost one.

| Check | Result |
|---|---|
| Batches compared | **9 / 9**, all `transport: child` |
| Batches with identical counts *and* content digests | **9 / 9** |
| Control: the inline path twice on batch 1 | stable (equal) |
| Worker receipt during the parity run | `mode: child`, 9/9 offloaded, 0 fallbacks |
| Per-batch cost of the same analysis, inline vs worker | 18.6/19.5 · 21.6/22.1 · 22.7/23.5 · 18.2/19.2 · 17.5/18.0 · 20.8/20.4 · 18.1/18.5 · 18.3/18.6 · 7.7/7.7 s — the worker is **−1.7% to +5.5%** of the inline time on the same batch (mean ≈ **+2%**: process start, pickling and the pipe) |

This is the one-digest pilot that preceded the transport
(`_out/perf-runs/discovery/homepage-analysis-overlap-20261027/pilot.json`, batch 1 in-process, in the
worker, and in a threaded worker, all `ff03871e048afe69`) at full corpus width, with a control.

## What this changes for the next fix

The cost snapshot's recommendations 2 and 3 — the output-identical hot-path cuts (**−19.9 s** for the
recovery queue's second provider pass, **−7.8 s** for the duplicated reason detail, **−7.2/−7.4 s** for
the memos) — were priced against the phase's own wall, i.e. against a phase that was on the arm's
critical path. With the overlap shipped they are **headroom in the worker**, not wall: the worker
runs **146.4 / 148.6 s** inside arms of **622.3 / 635.6 s**, so removing 28 s of its CPU shortens
nothing unless the worker becomes the binding path (it is not; the fetch and probe phases are). They
remain worth taking when the worker's wall matters — a smaller hand-off, more batches in flight, a
slower machine, or a future where the phase is no longer free.

## Limits

Four arms in one session window on one machine (idle beyond the audit's own lanes), so the pair
deltas carry the same-build drift the design recorded (**4.9%** bound; the two inline arms differ by
19.2 s and the two child arms by 13.3 s). The last batch's tail has no next head to hide behind, so
the ceiling is the phase wall minus one batch's analysis — visible as b9's 6.4 s worker wall in
p1-child. **A frozen runtime never uses the transport** (`multiprocessing` spawn re-launches
`sys.executable`, and the packaged launcher has no multiprocessing bootstrap), so the packaged
desktop app analyzes in-process and this win is currently a source-checkout/CI property, not a shipped
one; `BALUFFO_DISCOVERY_HOMEPAGE_ANALYSIS_CHILD=0` retires the transport for a run either way. The
live pairs' output divergence is attributed to fetch weather by mechanism and by mover, not by
per-page comparison — the offline parity run is what settles the transport, and it uses one captured
arm's population, not the live one.

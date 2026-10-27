# 64 lanes measures ~0.37 pp on the homepage 429 rate — the +2.42 pp acceptance reading does not reproduce — 2026-10-27

> - **Class:** measurement — answers the acceptance pass's per-host rate question with a
>   position-stratified design; no shipped behaviour changes.
> - **Trigger:** the 2026-10-27 acceptance pass read a **+2.42 pp** order-cancelled homepage 429 rise
>   across two counterbalanced pairs and failed its gate. Its own caveat was that the 32 arms sat at
>   the session ends and the 64 arms in the middle, and that two pairs cannot separate position from
>   lanes. The plan's reopen condition was a position-randomized or stratified rate study; this is
>   the stratified form.
> - **Status:** **the +2.42 pp reading is not reproduced.** Eight ABBA/BAAB quads (32 arms) on the
>   same 1,000-row slice measure a homepage lane effect of **+0.37 pp**, 95% **t** interval
>   **[−0.44, +1.17]** (host-cluster bootstrap **[+0.12, +0.64]**), with **6/8** quads positive
>   (sign **p = 0.289**). The acceptance pass's **+2.42 pp** lies outside the interval, so that rise
>   is attributed to session weather, not a lane cost. The pre-registered verdict is **`inconclusive-at-0.5pp`**:
>   the design rules out a **+2.4 pp** cost, not a **+0.5 pp** one. A secondary, exploratory timing
>   read finds the homepage phase **6.06 s** faster per 1,000 rows (95% CI **[−7.38, −4.73]**,
>   **8/8** quads), while whole-run is **−3.8 s** **[−11.0, +3.4]**. The config still stays at 32:
>   this study did not re-measure pages or boards, but the 3,000-row quad acceptance has since done
>   so ([result](discovery-64-lane-acceptance-quad-2026-10-27.md)): pages and boards pass, full-audit
>   timing is **−15.0 s** **[−32.5, +2.5]** (2/4 beyond the 32-lane spread), and the 4-quad rate is
>   **−0.15 pp** **[−1.95, +1.65]** `inconclusive-at-0.5pp`, so 32 stays.
> - **Verified against:** `f60de72e` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), population digest `bd60056d6d9cf325` (1,000 rows),
>   2026-10-27T08:39:17Z–09:23:06Z.
> - **Then inspect:** `_out/perf-runs/discovery/lane-rate-quad-20261027/` — `design.md`
>   (pre-registration), `manifest.json`, `progress.jsonl`, `rate-report.{json,txt}`, 32 arm records,
>   their `request-trace.jsonl` files, and the driver/analysis `quad_study.py` preserved beside them.

## Why a quad, not another pair

Every two-arm pair runs its arms at different times. The 2026-10-26 study measured the consequence:
the 64 − 32 homepage delta was **+3.17 pp** when 64 ran second and **−3.90 pp** when 32 ran second —
a **~3.5 pp** second-arm elevation, larger than any lane effect being priced. The 2026-10-27
acceptance pass then measured **+2.42 pp**, but its 64 arms sat in the middle of the session and its
own order caveat could not exclude the same drift.

A quad runs four arms in **low/high/high/low** (ABBA) or **high/low/low/high** (BAAB) order. For arm
rates `y1..y4` in time order:

```
mu    = mean(high-lane arms) - mean(low-lane arms)
drift = ((y2 - y1) - (y3 - y4)) / 2
```

`mu` is exact for a linear trend; ABBA and BAAB cancel it in opposite directions, and a balanced set
of the two orientations also cancels a quadratic trend. Design classification: **stratified**, not a
random shuffle of arm order. Each quad is a time stratum; the sequence inside it is symmetric; only
the orientation is randomized, **4/4**. That keeps the drift cancellation exact per observation
instead of averaging it out over many pairs. Eight quads were pre-registered with seed
**20261027** and the **4/4** split (the realized order was BAAB, BAAB, ABBA, BAAB, BAAB, ABBA, ABBA,
ABBA). A two-arm pair is one adjacent delta; the quad averages two adjacent deltas whose drift terms
have opposite signs, which is why its estimate survives a **+5.1 pp** same-lane swing (quad 6).

## Design and validity

Each arm is one isolated GameDevMap active dry run with its own `BALUFFO_DATA_DIR` and artifact path.
The population is the first **1,000** representative rows, **one** batch, not the acceptance pass's
3,000: the rate is per host, so extra rows add wall time and CSV-drift risk without adding independent
time windows. Only `activeAuditHomepageFetchConcurrency` differs (32 vs 64); per-host stays **3**,
`timeout_s` **6**, and recovery/probe/browser policy are defaults. Request trace is on and the lane
proof is read from each arm's own `phaseRuntime`.

All **32** arms completed with a valid lane proof (`configuredLanes` 32/64, `peakInFlight` reaching
the setting), **one** CSV SHA-256 and **one** population digest; no quad was invalid. Arms took
**68.6–102.7 s** (median **80.6 s**), **43.2 min** of arm wall (43.8 min elapsed). Each quad's four arms share **999**
common homepage hosts with **1,000** attempts per arm; the CSV and digest did not move all run.

## Result

Rates are per arm, in time order; `hits` are 429s per 1,000 attempts. `same` is `y4 - y1`, the
same-lane drift the quad removes.

| Quad | Order | Lanes (time order) | 429 rate (%) | 429 hits | μ (pp) | drift (pp) | same (pp) |
|---|---|---|---|---|---|---|---:|---:|---:|
| 1 | BAAB | 64, 32, 32, 64 | 1.4, 2.9, 1.3, 4.0 | 14, 29, 13, 40 | **+0.60** | +2.10 | +2.60 |
| 2 | BAAB | 64, 32, 32, 64 | 2.0, 1.1, 3.2, 2.8 | 20, 11, 32, 28 | **+0.25** | −0.65 | +0.80 |
| 3 | ABBA | 32, 64, 64, 32 | 1.3, 3.0, 2.2, 1.9 | 13, 30, 22, 19 | **+1.00** | +0.70 | +0.60 |
| 4 | BAAB | 64, 32, 32, 64 | 1.2, 3.7, 1.7, 1.6 | 12, 37, 17, 16 | **−1.30** | +1.20 | +0.40 |
| 5 | BAAB | 64, 32, 32, 64 | 4.0, 1.9, 1.5, 2.6 | 40, 19, 15, 26 | **+1.60** | −0.50 | −1.40 |
| 6 | ABBA | 32, 64, 64, 32 | 1.2, 2.9, 3.1, 6.3 | 12, 29, 31, 63 | **−0.75** | +2.45 | **+5.10** |
| 7 | ABBA | 32, 64, 64, 32 | 1.4, 1.2, 4.1, 1.8 | 14, 12, 41, 18 | **+1.05** | −1.25 | +0.40 |
| 8 | ABBA | 32, 64, 64, 32 | 2.0, 3.6, 1.7, 2.3 | 20, 36, 17, 23 | **+0.50** | +1.10 | +0.30 |

**Pooled:** **+0.37 pp**, t95 **[−0.44, +1.17]**, between-quad SD **0.96 pp**; host-cluster
bootstrap **[+0.12, +0.64]**; **6 positive / 2 negative**, sign **p = 0.289**. The decision interval
is the wider t interval, because the bootstrap prices host sampling but not time-window weather.
With `n=8` and that SD, the minimum detectable effect is about **1.0 pp**; the **0.5 pp** threshold
is below the design's resolution.

The acceptance pass's **+2.42 pp** is **2.5 half-widths** above this mean and outside the interval.
The 2026-10-26 study's **−0.37 pp** is inside it. Both are consistent with a small, near-zero lane
effect and a large position effect; the difference is which adjacent pair each study happened to
read.

**What a two-arm study would have seen.** The two adjacent deltas inside each quad differ by twice
the drift, and they bracket the quad estimate:

| Quad | First adjacent (pp) | Second adjacent (pp) | Mean (μ) |
|---|---:|---:|---:|
| 1 | −1.50 | +2.70 | +0.60 |
| 2 | +0.90 | −0.40 | +0.25 |
| 3 | +1.70 | +0.30 | +1.00 |
| 4 | −2.50 | −0.10 | −1.30 |
| 5 | +2.10 | +1.10 | +1.60 |
| 6 | +1.70 | −3.20 | −0.75 |
| 7 | −0.20 | +2.30 | +1.05 |
| 8 | +1.60 | −0.60 | +0.50 |

A two-arm design picks one of these columns depending on order. That is the whole explanation for
the **+2.42 pp** acceptance reading and the earlier **−0.37 pp** one: same population, different
weather inside the pair. Quad 6 is the clearest case — the last 32-lane arm saw **6.3%** while the
first saw **1.2%**, a **5.1 pp** same-lane swing, and the two adjacent deltas disagree by **4.9 pp**.

**Secondary phases** use the same estimator and are not the lane knob's primary claim:
recovery **+0.00 pp** [−0.05, +0.06], probe **−0.04 pp** [−0.24, +0.17]. Neither moves.

## Secondary timing read (exploratory)

The quad records each arm's homepage-phase wall, so the same drift cancellation can be applied to
time. This was not pre-registered and carries no gate; it is reported because it bears on the
acceptance pass's pair-2 timing failure:

| Measure | Per-quad 64 − 32 (ms or s) | Mean | 95% t interval | Quads faster at 64 |
|---|---|---:|---:|---:|
| Homepage phase (ms) | −2,871, −7,944, −7,223, −6,368, −4,756, −6,528, −6,732, −6,025 | **−6,056 ms** | **[−7,379, −4,732]** | **8/8** |
| Whole run (s) | −6.7, −1.1, −7.8, −14.4, −13.7, +6.7, −2.4, +8.9 | **−3.8 s** | **[−11.0, +3.4]** | 6/8 |

The homepage gain is consistent in every quad; the whole-run gain is diluted by the other phases
and does not clear zero. On 1,000 rows the 32-lane arms average **21.5 s** of homepage phase and the
64-lane arms **15.4 s**, so **6.1 s** is about **1.39×**. This makes the acceptance pass's **19.7 s** pair-2 gain sitting inside a **22.1 s**
32-lane spread look like pair-level noise rather than the gain failing to recur, but it does not
re-measure the 3,000-row gate.

## What this settles, and what it does not

- **The rate condition is re-read.** The acceptance pass's **+2.42 pp** was the reason the gate's
  third row failed. Under drift cancellation the effect is **+0.37 pp**, and the interval excludes
  **+2.42**. The page loss that tracked those 429s is therefore also attributed to weather, not a
  lane cost; the 3,000-row quad acceptance has since measured pages **+4.5** and boards **−0.2**
  with no consistent clean loss, so the inference holds.
- **The config does not flip.** The pre-registered **0.5 pp** question is **inconclusive**, and the
  design's MDE is about **1.0 pp**. A small positive cost cannot be excluded. The recorded
  acceptance gate still fails on pair 2's timing as taken; the 3,000-row quad acceptance has since
  re-run pages and boards (both pass) but full-audit timing remains **−15.0 s** **[−32.5, +2.5]**
  and the rate is inconclusive at 4 quads, so 32 stays.
- **The timing case is stronger than the acceptance pass showed.** The homepage gain is 8/8 at
  **−6.1 s** per 1,000 rows; whole-run is unresolved. That is consistent with the earlier
  **1.4–2.0×** homepage phase measurements and with the acceptance's pair-1 gain.
- **The reopening condition is satisfied for the rate half.** A fresh acceptance pass on **3,000**
  rows has now run with this quad design and measured pages and boards
  ([quad acceptance](discovery-64-lane-acceptance-quad-2026-10-27.md)). The rate objection to 64 is
  removed; the recorded page/board objection does not reproduce (the page floor is wide), while
  full-audit timing and rate precision keep 32.

## Limits

- One 1,000-row slice, one day, a local `f60de72e` tree — not the deployed container. The
  production 3,000-row population and host mix were not re-run in this study.
- Eight quads, so the between-window interval is wide; the **0.5 pp** threshold is under-powered.
  The design assumes an additive linear trend inside a quad; ABBA/BAAB cancel that, not arbitrary
  nonlinear weather.
- Hosts are paired, not randomized: each quad compares the same 999 hosts, but the unit of
  randomization is the time window.
- The trace records every attempt, including retries; a successful page fetch is 200 after
  `raise_for_status`, a 429 exactly. The rate is attempts, as in the earlier per-host reads.
- The quad driver and analysis are preserved under `_out/` beside the evidence, not promoted into
  `tools/measurements`; the numbers re-derive from the 32 `request-trace.jsonl` files and
  `manifest.json`.

## Reproducing the numbers

```bash
python _out/perf-runs/discovery/lane-rate-quad-20261027/quad_study.py run \
    --study _out/perf-runs/discovery/lane-rate-quad-20261027
python _out/perf-runs/discovery/lane-rate-quad-20261027/quad_study.py report \
    --study _out/perf-runs/discovery/lane-rate-quad-20261027
```

An arm record is immutable; `run` is resumable and skips an arm whose record exists after checking
its lane count. The manifest is written before the first arm. A different seed or quad count needs a
fresh `--study` directory.

## Boundaries

No production configuration, concurrency, timeout, retry or per-host limit changed. No shipped
`src/`, `frontend/` or `tests/` file changed. Each arm wrote only inside its own `BALUFFO_DATA_DIR`;
repo `data/` was untouched and no discovery audit artifact was created there. The run was local
only; no target-side or container run was started.

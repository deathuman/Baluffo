# A durable known-dead board memo cuts the probe 40.8% by not re-asking the 4,979 boards that already said 404 — 2026-10-28

> - **Class:** measurement + one shipped change — a durable JSON ledger beside the audit artifact
>   (`gamedevmap-known-dead-boards.json`, `BALUFFO_DISCOVERY_KNOWN_DEAD_DAYS`, default **7**,
>   `0` disables), keyed by **provider host + tenant**, recording only a **404/410 from the board's own
>   listing endpoint**. A skipped candidate still produces a rejection row, so the artifact reads
>   exactly as a live 404 would.
> - **Trigger:** the profile named the probe's real lever as candidate generation, not transport —
>   `api.lever.co` carries **46–53%** of probe lane time at a **~93%** 404 rate and delivers **none** of
>   the boards. The tail-cut study then closed the two transport levers (a shorter bound truncates live
>   boards; the breaker is board-safe only because Lever serves no board). Those dead boards were
>   re-probed on **every full re-audit** and never remembered.
> - **Status:** **ships at 64 lanes' current shape.** The ABBA over the frozen **9,050**-candidate
>   population reads baseline **236.2 / 196.8 s** (mean **216.5**) against variant **133.0 / 123.3 s**
>   (mean **128.2**) — **−40.8%**, and **−34.9%** against the fastest baseline — while no memoised
>   candidate is positive in a baseline arm and no baseline board is lost (**358** boards in all four
>   arms, variants a superset at **360**). Cold learning costs nothing: the learn arm reads **210.0 s**
>   against the **216.5 s** baseline mean (**−3.0%**, inside weather) and learns **4,979** boards.
>   Total probe CPU falls **56.1 → 39.8 s** (**−29.0%**). The pre-registered **R4** (`cpu per probed
>   item ≤ +10%`) fails literally at **+57.9%** — the denominator moves with the change, because the
>   phase's fixed **26.6 s** is now amortised over **45%** of the items — so the recorded verdict is
>   `cpu-trade` and the post-hoc reading that also uses total CPU is `ship`.
> - **Verified against:** `45220c2e` (`main`) + `src/source_discovery/known_dead_boards.py` and its
>   wrapper in `gamedevmap_active_dry_run.py`; live probe-only arms on the frozen **9,050**-candidate
>   population from the probe profile's `prof1` artifact, the audit's own **9 × 1,000** batches at 32
>   lanes and timeout 6; arms `l1 → b1 → v1 → v2 → b2` at 2026-10-28T17:25:13–17:40:14Z.
> - **Then inspect:** `_out/perf-runs/discovery/known-dead-memo-20261028/` — `design.md` (the
>   pre-registration, written before the change), `arms.py`, `run_arms.sh`, `run.log` (per-batch walls
>   and memo hits), `arms/{l1,b1,v1,v2,b2}/arm.json`, and `compare.txt` (the R1–R4 verdicts).

## What was run

The probe-only harness from `probe-tail-cuts-20261028/arms.py` over the same frozen **9,050**-candidate
list, each arm replaying the audit's own **9 × 1,000** batches through the shipped
`probe_with_known_dead` wrapper at 32 lanes and timeout 6, recording every candidate's verdict so board
preservation is checked candidate by candidate rather than in aggregate.

* **`l1`, learn** — memo enabled, file absent. A cold run that learns the population's dead boards. Its
  wall is the cold cost and is not the subject of the study.
* **`b1 → v1 → v2 → b2`** — ABBA on the same population. `b1`/`b2` run with
  `BALUFFO_DISCOVERY_KNOWN_DEAD_DAYS=0` (memo off); `v1`/`v2` run with `l1`'s memo copied into their own
  data dir, so no arm can poison another.

## The reading

| Arm | memo | wall | cpu | cpu/wall | probed | memo hits | boards | entries |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `l1` learn | cold | 210.0 s | 53.9 s | 25.7% | 9,050 | 0 | 359 | 0 → **4,979** |
| `b1` | off | 236.2 s | 55.7 s | 23.6% | 9,050 | 0 | 359 | 0 |
| `b2` | off | 196.8 s | 56.5 s | 28.7% | 9,050 | 0 | 359 | 0 |
| `v1` | on | 133.0 s | 40.0 s | 30.1% | 4,071 | **4,979** | 360 | 4,979 |
| `v2` | on | 123.3 s | 39.7 s | 32.2% | 4,071 | **4,979** | 360 | 4,979 |

Baseline mean **216.5 s**, variant mean **128.2 s**: **−88.3 s (−40.8%)**, arms spanning
**123.3–236.2 s**. Against the fastest baseline alone the cut is still **−34.9%**. The memo file is
**772 KiB** for **4,979** keys; reading it, looking up the whole population and rewriting it costs
**~4 ms** a run (load **1.7 ms**, **4,979** lookups **0.15 ms**, write **1.8 ms**) against a **210 s**
phase.

## R1 — board preservation holds, and the variants gain two boards

The **358** boards positive in both baseline arms are positive in both variant arms; no baseline board
is lost. The **4,979** memoised candidates contain **0** candidates that are positive in either baseline
arm, so nothing the memo skips was a live board on this population.

The variants carry **360** boards — **`lasershot.com/careers/`** and **`screenburn.com/jobs`** — and
neither is a memo key: a memo key is by construction a board whose own endpoint answered 404/410, so
both were probed in all four arms and answered positive twice. That is probe weather on a live page, in
the direction that costs the memo nothing (R1 forbids losses, not gains), and it is recorded rather than
absorbed.

## R4 — the rule that failed, and why it measured the wrong quantity

R4 was fixed as *process CPU per probed item must not rise by more than 10%*: **6.20 → 9.79 ms**
(**+57.9%**). It fails literally, and the artefact is in the denominator — items probed fall
**9,050 → 4,071**, so a fixed cost is divided over **45%** of the items. A two-point fit over
(items, cpu) gives **26.6 s + 3.26 ms per probed item** and reproduces both arms
(**26.6 + 29.5 = 56.1 s**; **26.6 + 13.3 = 39.8 s**): the fixed share of the per-item figure rises
**2.94 → 6.53 ms** (its share **47.4% → 66.7%**) and the marginal **3.26 ms** is unchanged. Total CPU,
which is what the phase actually pays, falls **56.1 → 39.8 s** (**−29.0%**).

So the pre-registered verdict is `cpu-trade` and the post-hoc verdict, R1–R3 plus total CPU, is `ship`.
The rule as written cannot pass a change that removes work: any cut measured per remaining item carries
the whole fixed cost in its numerator. A rule that prices a removal should read total CPU, or a
marginal cost that is fitted independently of the arms it judges — not a ratio whose denominator the
change defines.

## What this changes

`src/source_discovery/known_dead_boards.py` ships `board_key`, `definitive_not_found`, `load_memo`,
`save_memo`, `split_known_dead`, `record_definitive_not_founds` and `probe_with_known_dead`;
`gamedevmap_active_dry_run.py` loads the memo once per run beside the artifact, splits each batch before
the probe and records that batch's definitive 404/410s afterwards. Rules the code holds:

* Only **404/410** from an authoritative provider endpoint is recorded. A timeout, a **429** or a 5xx is
  not — those are conditions, not verdicts, and remembering them would turn a bad window into a lost
  board.
* Identity is **provider host + tenant** (`api.lever.co/<account>`, `boards-api.greenhouse.io/<slug>`),
  never a studio label, row URL or registry id — the same rule the registry uses.
* A missing, unreadable or corrupt file means **no suppression**, never an error.
* Entries older than `ttlDays` are ignored on read and pruned on write; the file is capped at 50,000
  entries.
* A skipped candidate receives one failure result per skip, so `probeFailures`, rejection rows and the
  run's counts read exactly as a live 404 would.
* The memo never promotes, demotes or registers anything, and it is not authoritative: deleting it
  costs probe time, never correctness.

The cost of that is staleness: a board that revives within **7 days** of its 404 is missed until the
entry expires, and there is no revival probe. 7 days is the bound chosen because the saving only lands
on a re-audit — an artifact that is fresh (TTL **360 min**, matching `configSignature`) skips the audit
entirely — so a longer TTL buys no further saving and only widens the window.

This is the shipping answer to the lever the profile named. The **1,801** Lever and **3,183** Greenhouse
boards that answer 404 are exactly the population the memo holds; on a re-audit they cost **4 ms**
instead of a request each. What remains in the probe's **4,071** attempts is the live and unresolved
45%, and the tail-cut study already showed the timeout bucket cannot safely shrink — so the next probe
lever is no longer Lever's 404s.

## Limits

* **Probe-only arms, not full audits.** The memo touches only the probe phase, so the run-level saving
  is this phase's **−88.3 s** against the last full-population arm's probe of **210.2 / 211.2 s** (arm
  mean **668.8 s**). A full-arm pair is the confirmation this study does not make.
* **The saving is the second run's.** A cold audit pays **l1**'s cost — measured at **210.0 s** against
  the **216.5 s** baseline mean, i.e. no measurable penalty — and the next full re-audit collects the
  cut. Nothing about a first run is faster.
* **One population.** The **9,050** candidates and their 404 rate are one frozen GameDevMap audit. A
  population with a different dead-board share moves the percentage, not the mechanism.
* **R4 is post-hoc.** `cpu-trade` is the verdict the rules produce; `ship` is a reading taken after the
  fact and recorded as such.
* **Two boards appeared the baselines missed.** Attributed to probe weather because both were probed in
  all four arms; it constrains nothing, but it does mean the four arms did not observe an identical
  world.

## Evidence

`_out/perf-runs/discovery/known-dead-memo-20261028/compare.txt`:

```
l1: wall 210.0 s | cpu 53.9 s (25.7%) | probed 9050 of 9050 | memo hits 0 | entries 0->4979 | boards 359
b1: wall 236.2 s | cpu 55.7 s (23.6%) | probed 9050 of 9050 | entries 0->0 | boards 359 | timeouts 22
b2: wall 196.8 s | cpu 56.5 s (28.7%) | probed 9050 of 9050 | entries 0->0 | boards 359 | timeouts 7
v1: wall 133.0 s | cpu 40.0 s (30.1%) | probed 4071 of 9050 | memo hits 4979 | boards 360 | timeouts 4
v2: wall 123.3 s | cpu 39.7 s (32.2%) | probed 4071 of 9050 | memo hits 4979 | boards 360 | timeouts 4

boards: stable 358 | v1 360 v2 360 | lost in v1 [] | lost in v2 []
memo-skipped candidates: 4979 distinct across the variant arms | positive in a baseline arm: 0
items probed: baseline 9050, variant 4071 | cpu per probed item 6.20 ms -> 9.79 ms (+57.9%)
total cpu 56.1 s -> 39.8 s (-29.0%); two-point fit over (items, cpu): fixed 26.6 s + 3.26 ms per item
R4 cpu per probed item <= +10%: False (+57.9%)
VERDICT (literal rules): cpu-trade
POST-HOC VERDICT (R1-R3 plus total cpu): ship
```

Focused checks: `pytest tests/source_discovery/test_known_dead_boards.py` and
`tests/source_discovery/test_gamedevmap_active_dry_run.py`; `ruff check` and `mypy` on the new module
and its wrapper.

## Related

* [`discovery-probe-profile-2026-10-28.md`](discovery-probe-profile-2026-10-28.md) — the profile that
  named Lever's negative probes as the lever this memo answers.
* [`discovery-probe-tail-cuts-2026-10-28.md`](discovery-probe-tail-cuts-2026-10-28.md) — the two
  transport cuts that could not ship, leaving candidate generation.
* [`discovery-post-cut-arm-2026-10-28.md`](discovery-post-cut-arm-2026-10-28.md) — the full-population
  arm whose probe phase is the denominator of this saving.

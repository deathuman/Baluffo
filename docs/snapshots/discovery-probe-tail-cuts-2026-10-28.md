# Neither probe tail cut ships: a shorter timeout truncates live boards, and the breaker is board-safe only because Lever serves no board — 2026-10-28

> - **Class:** measurement — a replay and a probe-only ABBA study, plus one env-gated, **default-off**
>   knob (`BALUFFO_DISCOVERY_PROBE_CIRCUIT_BREAKER_TIMEOUTS`) kept as the measured artifact; the
>   matching timeout knob was built for the study and **removed** once the replay rejected it. No
>   shipped default changes.
> - **Trigger:** the profile left the probe at **206.4 s** mean with **17.6 / 18.3%** of its lane time
>   in 6.4 s timeouts and a **37%** idle tail, and named two candidate cuts — a shorter per-call bound
>   and a per-wave host breaker. Both can drop a candidate board, so both had to be priced before
>   either shipped.
> - **Status:** **the timeout cut is rejected on replay and the breaker is board-safe but unproven
>   live.** Cutting the bound truncates real boards at every setting: **T=5 s loses 4 of 6** traced
>   200s, **T=4 s loses 8 of 14**, **T=3 s loses 22 of 43** — the "timeouts" are not all timeouts.
>   The breaker at **K=3** skipped **945 / 1,457** requests in the traced pair and **73 / 157** in the
>   live pair, **all of them Lever**, and lost **no board anywhere**: the population's **1,802** Lever
>   candidates yield **0** boards, so on this population it cannot. Its live cut, however, **could not
>   be demonstrated**: variant mean **187.2 s** against baseline **180.1 s** (**+3.9%**) with a
>   **166.0–194.2 s** arm spread, and only **2–7** timeouts in the window against the profile's
>   **123–149**. The replay bound at profiled weather is **34–54 s** (**17–25%** of the probe). The
>   finding that matters is underneath both: **Lever is 46–53% of probe lane time, ~93% of its
>   requests are 404s, and it delivers none of the 360–363 boards** — that is a candidate-generation
>   cost, not a transport one.
> - **Verified against:** `45220c2e` (`main`) + the default-off breaker in
>   `src/source_discovery/probe_runtime.py`; live probe-only ABBA on the frozen **9,050**-candidate
>   population from the profile's `prof1` artifact, 9 × 1,000 batches, 32 lanes, timeout 6; arms
>   `b1 → v1 → v2 → b2` at 2026-10-28T16:43:13–16:55:30Z.
> - **Then inspect:** `_out/perf-runs/discovery/probe-tail-cuts-20261028/` — `design.md` (the
>   pre-registration), `replay.py` + `replay.txt` (Phase 1), `arms.py`, `run_arms.sh`, `run.log`,
>   `arms/{b1,v1,v2,b2}/arm.json` and `results.jsonl`, and `compare.txt` (the R1–R4 verdicts).

## What was run

**Phase 1, offline.** The two traced profile arms' `probe_fetch` requests (9,631 / 9,635) are clustered
into waves by the existing 15 s gap rule and each candidate rule is applied to them as they were
ordered and timed. A request cut by a shorter bound costs `T + 0.47 s` (the observed overhead of the
6.47 s timeouts over the 6 s budget); a breaker skip frees its whole traced duration.

**Phase 2, live.** Only the variant with **zero 200s cut or skipped** advanced, so the timeout cut was
rejected without live traffic. Four probe-only arms in **ABBA** order over the frozen candidate list,
each replaying the audit's own **9 × 1,000** batches through `probe_candidates_async` with its phase
receipt at 32 lanes and timeout 6; `v1`/`v2` set the breaker to **3**, `b1`/`b2` leave both knobs off.
Every candidate's verdict is recorded, so the comparison is per-candidate, not aggregate.

A one-batch smoke arm ran first: **15.6 s** for batch 1 against prof1's **23.9 s**, so the window was
faster than the profile's and the live cut would have to be compared against its own baselines.

## Phase 1 — the timeout cut truncates boards

| Variant | prof1 saved | prof1 cut | boards lost | prof2 saved | prof2 cut | boards lost |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| T = 5 s | 126.2 s (3.2 s wall) | 140 | **4** | 158.1 s (4.0 s) | 164 | **6** |
| T = 4 s | 265.5 s (6.6 s) | 165 | **8** | 321.5 s (8.0 s) | 202 | **14** |
| T = 3 s | 428.2 s (10.7 s) | 202 | **22** | 520.2 s (13.0 s) | 263 | **43** |

Even the mildest setting costs live boards, and the wall it buys is **3–13 s** of a ~200 s phase. The
boards it costs are exactly the slow 200s: **4** requests over 5 s in prof1 and **6** in prof2, which is
precisely T=5's loss. They are **studio career pages**, not provider APIs — prof1's are
`q-games.com/recruit`, `swordcanes.co.jp/recruit`, `winkingworks.com/careers` and `virzoom.com`;
prof2's add `arsanesia.com` (twice), `kalanigames.com/careers.html` and `zariba.com/careers`. The
phase's timeout bucket is therefore not a pile of wasted waits: it mixes genuine timeouts with real
boards on slow hosts, and a global bound cannot tell them apart. The variant is rejected here, on the
pre-registered rule, and no live traffic was spent on it.

## Phase 1 — the breaker is board-safe on the traced pair

| Variant | prof1 saved | prof1 skipped | boards lost | prof2 saved | prof2 skipped | boards lost |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| K = 2 | 1,421.7 s (35.5 s wall) | 992 | 0 | 2,331.2 s (58.3 s) | 1,538 | 0 |
| K = 3 | 1,346.1 s (33.7 s) | 945 | 0 | 2,165.3 s (54.1 s) | 1,462 | 0 |
| K = 5 | 1,003.0 s (25.1 s) | 659 | 0 | 1,755.9 s (43.9 s) | 1,165 | 0 |

Every opening is on **`api.lever.co`** (6 host-waves in prof1, 9 in prof2; K=3 also opened
`jobs.lever.co` once in prof2, 5 requests). The skips are **852 / 1,348** 404s plus **93 / 114**
timeouts in the same host's wave — the breaker does not touch the 0.93 / 1.07 s Lever 404s that arrive
before it opens, it skips the ones behind them.

## Phase 2 — the live ABBA

| Arm | Breaker | Wall | CPU | Boards | zero / failed | Timeouts | Circuit-open | Lever lane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| b1 | 0 | **194.2 s** | 50.1 s (25.8%) | 363 | 3,222 / 5,465 | 7 | 0 | 2,172 s |
| v1 | 3 | **181.5 s** | 50.2 s (27.6%) | 361 | 3,204 / 5,485 | 4 | 73 | 2,168 s |
| v2 | 3 | **192.9 s** | 50.9 s (26.4%) | 360 | 3,211 / 5,479 | 4 | 157 | 2,312 s |
| b2 | 0 | **166.0 s** | 57.6 s (34.7%) | 361 | 3,214 / 5,475 | 2 | 0 | 1,931 s |

| Rule | Reading |
| --- | --- |
| **R1** board preservation (literal) | **False** — `Beatshapers` (static, 1 job) is positive in both baselines and zero-jobs in v2 |
| **R1** board preservation (causal) | **True** — all **230** skips are `lever`; **0** skipped candidates are positive in either baseline |
| **R2** wall cut ≥ 10% | **False** — variant **187.2 s** vs baseline **180.1 s** (**+3.9%**) |
| **R3** tail ≥ 30% down | **False** — Lever lane **2,168 / 2,312 s** vs **2,172 / 1,931 s**; timeouts **4 / 4** vs **7 / 2** |
| **R4** CPU per item ≤ +10% | **True** — **5.95 → 5.58 ms** (−6.1%) |

**The literal R1 fails on a board the breaker never touched.** The only lost candidate in either variant
arm is a static page that returns 1 job in both baselines and 0 in v2 — a page-level flip, and the same
noise the baselines show against each other (`flixinteractive` is gained in the variant arms). Nothing
skipped by the breaker was positive in any baseline arm, so the cause the breaker controls is clean and
the literal rule is confounded by static-page weather.

**R2 and R3 fail for want of a tail, not for want of a mechanism.** The window ran **2–7** timeouts
against the profiled pair's **123–149**, and the four arms themselves spread **166.0–194.2 s** (17%),
so a 3–5% effect is below this design's resolution: two arms a side cannot separate a small cut from
weather. The replay is what bounds the upside, at **34–54 s** when the profile's timeout weather
returns.

## Why no board was lost, and what that says about Lever

The breaker skipped **945 / 1,457** traced and **73 / 157** live candidates, every one of them Lever,
and the population's Lever candidates deliver **no boards at all**:

| Reading | Value |
| --- | ---: |
| Lever candidates in the probe population | **1,802** |
| Of those, probe `ok` | **1** |
| Of those, positive (jobs > 0) | **0** |
| Boards delivered by non-Lever adapters (b1) | 363 |
| Lever share of probe lane time (live b1 / traced prof1) | **46% / 53%** |
| Lever requests that are 404s (traced) | 1,792 / 1,906 and 1,780 / 1,923 |

So the breaker is not merely "unobserved to lose a board" on this population — it **cannot** lose one,
because there are none to lose. Lever is nonetheless the single largest consumer of probe lane time:
at 32 lanes its 2,172 s of lane time is **68 s** of the live arm's 194 s, and the 404 portion alone
(1,667 / 1,905 s traced) is **52 / 60 s** at 32 lanes. **The probe's cost is dead Lever candidates, not
its transport.** A bound, a breaker, or a lane budget cannot fix that; not generating them can.

## What this changes

* **The timeout cut leaves no knob behind.** It was built as a measurement knob and removed once the
  replay rejected it: every shorter setting truncates real boards, and a default-off footgun that
  costs 4–43 boards whenever someone switches it on is worse than losing the ability to re-run a
  rejected arm. The replay is reproducible from the traced arms without it.
* **The breaker stays default-off.** It is board-safe here and worth **34–54 s** at the profile's
  timeout weather, but the live pair could not demonstrate even 10%, and flipping a default on a
  single traced session plus an underpowered live pair is not evidence.
* **The next probe lever is candidate generation.** 1,802 Lever candidates, **one** of them a
  successful probe and **none** with jobs, **46–53%** of lane time. `probe.is_definitive_not_found`
  already knows a 404/410 from a provider's
  own authoritative endpoint settles the board — but only **within** one candidate's probe, to suppress
  its sibling request. Remembering that across runs (or filtering known-dead Lever slugs at generation)
  is the only lever in this phase that is both large and board-free by construction; it needs its own
  pre-registered study, because it changes what the audit generates rather than how it fetches.
* **The profile's "tail" reading is confirmed as weather-shaped.** The same phase, same population, 90
  minutes later, ran with **2–7** timeouts instead of **123–149** and finished 194 s. Any probe cut
  priced from one session is a cut priced from that session's weather.

## Limits

* Phase 2 is a **probe-only replay** of a frozen candidate list. It measures the probe phase, not the
  arm, and it does not include the homepage or recovery phases that decide which candidates exist.
* The live pair is **2 arms a side** against a **17%** session spread; R2/R3 failures are
  **underpowered**, not evidence of absence. The replay's 34–54 s bound is what the mechanism is worth
  at profiled weather.
* The replay prices **requests, not candidates**: a positive is visible only through its 200, and the
  trace covers the default `fetch_text_httpx` path (100% of `probeLifecycle.fetchesStarted` in the
  profiled arms).
* The live arms carry no per-request trace, so their Lever lane share comes from per-candidate
  `probeDurationMs` grouped by `api_url`, not from request intervals.
* Lever's zero-board density is **this population's**; a population with live Lever boards would carry
  a non-zero skip risk, which is why the knob is not switched on.
* The breaker counts timeouts per host **per batch**. The audit runs 1,000-row batches, so "per wave"
  means "per batch" in practice; a batch-size change moves its exposure.

## Tree cost

The breaker is **+34** lines of `src` (`probe_runtime.py`), ratcheted in
`tools/repo_health/loc_baseline.json` (`src` 191,924 → 191,958), and `tests` grows by the
**143**-line contract file, whose ratchet belongs with that file when it is staged. Nothing else in
`src` changed: the timeout knob's lines were removed with it.

## Evidence

Pre-registration `_out/perf-runs/discovery/probe-tail-cuts-20261028/design.md`; Phase 1
`replay.py` / `replay.txt`; Phase 2 `arms.py`, `run_arms.sh`, `run.log`,
`arms/{b1,v1,v2,b2}/arm.json` and `results.jsonl` (per-candidate verdicts), and `compare.txt`; smoke
`smoke/arm.json`. Knob and tests: `src/source_discovery/probe_runtime.py`
(`probe_circuit_breaker_timeouts`, and the skip in `_call_fetch`) and
`tests/source_discovery/test_probe_host_breaker.py` (four contract tests: skips its own host, leaves
other hosts alone, off unless asked, and reports a `circuit open` failure per candidate).

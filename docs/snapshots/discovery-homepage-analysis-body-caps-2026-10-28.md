# Body caps buy 5-19 s of homepage-analysis headroom and cost real candidates, so no cap ships — 2026-10-28

> - **Class:** measurement — prices body caps on the captured production arm, against the current
>   `~47 s` phase; no shipped behaviour changes.
> - **Trigger:** the cost snapshot priced caps at **−28.2 s / −44.9 s** against a **121.8 s** phase
>   and told the reader not to take them first; five output-identical cuts later, the js-shell
>   snapshot named caps the last remaining price. This study takes it.
> - **Status:** **no cap belongs.** Every level and both shapes lose real provider/static candidates
>   and add recovery fetch rows. Head+tail **1 MB** — the shipped truncation's own level, extended to
>   every pass — already loses **5 static** and **3 browser** candidates and adds **32** recovery
>   rows for **~6 s** of phase headroom. Head+tail **500 KB** saves **10.75 s** but loses **2
>   provider** and **37 static** candidates and adds **244** rows; **250 KB** saves **17.05 s** at
>   **10 / 89** candidates and **582** rows. The shipped `>1 MB` head+tail truncation stays where it
>   is — on the page outcome — and is neither extended nor lowered.
> - **Why the phase number does not decide it:** the phase runs in a child worker, so its wall is
>   **worker headroom, not arm wall**, while the added recovery rows are main-process fetch work.
>   The cap trades hidden time for critical-path work and real discovery, which the pre-registered
>   rules refuse.
> - **Verified against:** `45220c2e` (`main`), the captured arm of 2026-10-27 — **8,369** results,
>   **7,467** readable bodies, **1,110.7 MB**, digest `66165df36a10c023`, nine batches — replayed
>   on the same machine. Baseline bookends **51.90 s** and **54.72 s** (drift **+2.81 s**); each cap
>   is one full-phase arm.
> - **Then inspect:** `_out/perf-runs/discovery/homepage-analysis-body-caps-20261028/` —
>   `design.md` (the pre-registration), `replay_caps.py`, `compare.py`, the six arm JSONs and their
>   `-urls.json` identity dumps, `report.txt`, and `compare.json`.

## The corpus and the arms

The same captured arm every homepage-analysis measurement since 2026-10-27 uses: one production
run's normalized homepage results, bodies included. Size is the population — **129** bodies exceed
1 MB in bytes (**119** in characters, which is what the cap counts), **479** exceed 512 KB in bytes
(**474** in characters), **1,062** exceed 256 KB (**1,056**); bodies above 256 KB hold **697.6 MB**
of the **1,110.7 MB** total (**63%**).

Two cap shapes, applied to the body the phase receives:

* **head+tail** keeps the first and last `cap/2` characters, joined by a newline — the shape of the
  shipped truncation in `_append_analyzed_candidates`, which keeps the nav head and the footer tail,
  where careers links live. Levels **1 MB, 500 KB, 250 KB**.
* **prefix** keeps the first `cap` characters — the old 121.8 s study's shape, kept for
  comparability only. Levels **500 KB, 250 KB**.

Order: **baseline, head+tail 1 MB, head+tail 500 KB, head+tail 250 KB, prefix 500 KB, prefix
250 KB, baseline**. No cap was repeated, because none passed the eligibility rule that would have
made a repeat necessary; the closing baseline bounds the window instead.

## What the caps buy

| Arm | Pages capped | Phase wall | CPU | Saved vs bookended mean | Saved vs opening |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline (opening) | 0 | 51.90 s | 51.78 s | — | — |
| head+tail 1 MB | 119 | **47.08 s** | 46.88 s | **+6.23 s** | +4.82 s |
| head+tail 500 KB | 474 | **42.56 s** | 42.44 s | **+10.75 s** | +9.34 s |
| head+tail 250 KB | 1,056 | **36.26 s** | 35.83 s | **+17.05 s** | +15.64 s |
| prefix 500 KB | 474 | 44.59 s | 44.13 s | +8.72 s | +7.31 s |
| prefix 250 KB | 1,056 | 34.63 s | 34.39 s | +18.68 s | +17.27 s |
| baseline (closing) | 0 | 54.72 s | 53.22 s | — | — |

The window drifted **+2.81 s** from opening to closing (**+5.4%**), so every saving above is quoted
against the bookended mean **53.31 s**; the smaller column shows the reading if the opening
baseline is the only reference. Head+tail beats prefix at the same level on the clock (**10.75 vs
8.72 s** at 500 KB), but it loses more static identities (**37 vs 26**) even as it loses fewer
provider (**2 vs 6**) and browser (**6 vs 14**); neither passes.

Caps remain the largest lever on the phase — **up to 35%** of it — which is exactly why the cost
side, not the clock, decides.

## What the caps cost

The collections, in unique identities, against the uncapped baseline (**91** provider, **1,472**
static, **1,380** browser, **11,635** primary jobs, **22,300** secondary jobs, **1,181** rejected,
**8,368** boards):

| Arm | Provider | Static | Browser | Boards that lost every provider/static candidate |
| --- | ---: | ---: | ---: | ---: |
| head+tail 1 MB | 0 | **−5** | **−3** | **5** |
| head+tail 500 KB | **−2** | **−37** | −6 (8 lost, 2 gained) | **41** |
| head+tail 250 KB | **−10** | **−86** | −11 | **100** |
| prefix 500 KB | **−6** | **−26** | −14 | **33** |
| prefix 250 KB | **−14** | **−83** | −37 | **102** |

Every arm also **adds** recovery work: **+32** fetch rows at 1 MB, **+244** at 500 KB, **+582** at
250 KB (head+tail); **+194** and **+589** for the prefix arms. None removes a row.

The extra rows are priced on the same arm's own measurement: its recovery waves cost **152,211 +
56,727 = 208,938 ms** of fetch wall for **13,605** recovery fetch attempts, i.e. **15.36 ms** a job
at 72 lanes / 4 per host / 5 s timeout.

| Arm | Phase saved | New recovery rows | Priced fetch cost | Net, before lost candidates |
| --- | ---: | ---: | ---: | ---: |
| head+tail 1 MB | +6.23 s | +32 | +0.49 s | +5.74 s |
| head+tail 500 KB | +10.75 s | +244 | +3.75 s | +7.01 s |
| head+tail 250 KB | +17.05 s | +582 | +8.94 s | +8.11 s |
| prefix 500 KB | +8.72 s | +194 | +2.98 s | +5.74 s |
| prefix 250 KB | +18.68 s | +589 | +9.05 s | +9.63 s |

The "net" column is why the clock alone is misleading: even after paying for every added fetch, the
phase is ahead by **5.7-9.6 s**. What it does not price is what the cap destroyed.

Concretely, at the **1 MB** level, `https://www.buffalobuffalo.ca/careers` disappears and the board
is replaced by recovery jobs for `https://buffalobuffalo.ca/about/careers`, `/careers` and
`/company/careers`; Kalypso Media, Nautilus Mobile and Team Junkfish lose their static candidates
the same way. At **500 KB** the lost provider candidates are Ashby's `playgig` board and Workable's
`soul-machines-ltd`, beside static losses such as `https://unity.com/careers` and
`https://jobs.netflix.com/jobs`. At **250 KB** ten provider candidates go, including BambooHR's
`felixandpaul` and `gasketgames` and JazzHR's `hirezstudios`.

## The decision, by the pre-registered rules

1. **Eligible** requires **≥ 4.7 s** (10% of the **~47 s** phase the pre-registration was fixed
   against; this window's baselines were **51.90 / 54.72 s**, so 10% is **5.19 / 5.47 s**, which
   changes no arm's verdict) and **zero** lost provider/static candidates. **No arm passes** — the
   smallest loss is the 1 MB arm's 5 static, and every level loses more. The 1 MB arm's saving
   (**6.23 s** against the bookended mean) clears the floor; its output does not.
2. **Affordable** requires the priced fetch cost below half the saving. The 250 KB head+tail arm
   fails (**8.94 s vs 8.53 s**); the rest pass this rule only.
3. **Belongs** requires no net recovery work, because the phase is worker headroom and the rows are
   main-process work. **Every arm adds at least 32 rows**, so every arm fails.

**Verdict: no body cap belongs in the discovery pipeline.** The shipped `>1 MB` head+tail
truncation remains the only analysis-side cap, on the page outcome only. It is not extended to the other passes
(the 1 MB arm measures that change at −5 static, −3 browser and +32 rows for ~6 s of headroom), and
it is not lowered (the 500 KB arm is the same trade, larger).

This is not a claim that caps are uneconomic in phase seconds — they are the largest remaining
number. It is that the seconds land in the worker, while the costs land on discovery and on the
fetch path, and the pipeline's job is what it collects.

## Limits

* One capture arm, one machine, one window; the bookended baselines differ by **2.81 s** (5.4%), and
  each cap was read once. The savings are larger than the drift (the smallest, 4.82 s against the
  opening baseline, is 1.7× it), but no cap's saving is claimed as a resolved effect — the decision
  rests on output deltas and the direction of the fetch cost, both of which are categorical.
* The cap is applied where the replay hands bodies to the phase. A fetch-time cap would also cut
  transfer and storage, which this does not price; it would truncate every consumer, which is what
  the replay does for the phase.
* The **15.36 ms** job cost is an **upper bound** on added fetch work: a lost static candidate
  would itself have needed a probe fetch, so some new recovery rows replace work rather than add it.
  The lost candidate is the part that is not an upper bound.
* Identity keys: provider `id` (falling back to `api_url`, then `careersUrl`), static
  `listing_url` + its `pages`, browser `url`, jobs `url`, boards `sourceDirectoryEntryUrl`. A static
  candidate's identity includes its page set, so a board whose page list shortened counts as one
  lost identity; the board column (a board that lost **every** provider/static candidate) is the
  cleaner cost and is quoted first. Two directory entries for one studio are two boards here, which
  is the directory's unit, not the registry's host+tenant board identity.
* No live full-population arm was run for this study. The arm-level effect of a cap is therefore not
  measured; the asymmetry between worker headroom and main-process recovery work is the reason the
  rules weight it, not a measured arm delta.
* Lowering the existing 1 MB truncation **in place** (outcome path only) is not measured. It would
  cost less and buy less than the phase-entry arms; the nearest reading is the 1 MB arm, and the
  same two failures (lost outcome-visible links, added recovery rows) apply at smaller scale.

## What this closes

The cap question the cost snapshot opened and the js-shell snapshot deferred. The homepage-analysis
phase's remaining cost is now **required work plus the memos' bounded caches**; there is no cap,
no output-identical cut, and no memo left that is worth taking on this corpus. The next lever, if
one is wanted, is outside the phase: **reduce what the phase is asked to analyze**, not how much of
each body it reads. That is confirmed at arm level in
[`discovery-post-cut-arm-2026-10-28.md`](discovery-post-cut-arm-2026-10-28.md): the worker fell to
**48.6 / 47.4 s** while the arm stayed at **650.8-686.7 s**, fully hidden behind fetch/probe.

## Files

* Pre-registration and decision: `_out/perf-runs/discovery/homepage-analysis-body-caps-20261028/design.md`
* Replay: `replay_caps.py`; comparison and pricing: `compare.py`; printed record: `report.txt`
* Arms: `baseline1/2.json`, `ht1m/ht500/ht250/pre500/pre250.json`, each with a `-urls.json`
  identity dump

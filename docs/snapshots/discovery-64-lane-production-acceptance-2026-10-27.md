# Production full-audit acceptance: 64 keeps the homepage gain on all 8,370 rows but misses the full-audit spread test — keep 32 — 2026-10-27

> - **Class:** measurement — the plan's step-5 gate at production size (all eligible rows); no
>   shipped behaviour changes.
> - **Trigger:** the 3,000-row quad acceptance failed step 5 on full-audit timing (whole-run gain
>   beyond the 32-lane spread in only **2/4** quads) and left the question on a sample that covers
>   just the first **3,000** of the CSV's **8,370** eligible rows. The setting a config change
>   would govern is the whole population, so the gate is re-run where it would apply.
> - **Status:** **acceptance not met — keep 32.** Four full-population quads (**16** arms, 2 ABBA /
>   2 BAAB, seed `202610271`), each arm a complete **8,370**-row audit (9 batches), one CSV
>   `896da851…` and population digest `66165df36a10c023` across all 16 arms. 64's homepage phase
>   gain is **77.0 s** (95% CI **[−103.6, −50.5]**, **4/4** quads negative, **4/4** beyond the
>   32-lane spread) and all **16** arms completed the population. The full-audit wall gain is
>   **87.8 s** (95% CI **[−171.6, −3.9]**, **4/4** negative) but clears the 32-lane same-lane
>   spread in only **2/4** quads — quads 2 and 4 carry **101.0 s** and **55.5 s** of same-lane
>   weather against **62.2 s** and **32.5 s** of gain — so the plan's full-audit timing condition
>   still fails. Pages **pass** (homepages **−38.5** against a **60** floor; homepage failures
>   **+38.5**; every endpoint inside its floor), boards **pass** (active **0.0** against a **6**
>   floor; one clean loss in q2, `arsanesia.com`, rejected by both 64 arms on a blank-error
>   `homepage_fetch_failed`), and the rate gate **passes** for the first time (**+0.43 pp**, 95% CI
>   **[−0.16, +1.01]**, host bootstrap **[+0.31, +0.55]**), while the pre-registered **0.5 pp**
>   precision question stays `inconclusive-at-0.5pp`. The lane investigation is closed by this
>   pass: 32 stays, and the next route is homepage-analysis cost
>   ([profiled](discovery-homepage-analysis-cost-2026-10-27.md): **17.0%** of an arm, one core
>   saturated, with the priced fixes on the table).
> - **Verified against:** `f60de72e` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), population digest `66165df36a10c023` in all 16 arms,
>   2026-10-27T12:22:43Z–15:45:00Z. All 16 arms report `count == eligibleRows == 8370`, prove
>   `configuredLanes` and a peak of 32 or 64, and carry a request trace; 16 arms over **~3 h 22
>   min**, per-arm walls **649.1–862.5 s**.
> - **Then inspect:** `_out/perf-runs/discovery/lane-acceptance-production-20261027/` —
>   `design.md`, `manifest.json`, `progress.jsonl`, `arms/*/arm.json`, each arm's
>   `request-trace.jsonl`, `acceptance-report.json` / `.txt`, `rate-report.json` / `.txt`.

## Design

Four pre-registered quads, each running four arms over the same population in one of two orders:
**ABBA** 32/64/64/32 or **BAAB** 64/32/32/64. For arm values `y1..y4` in time order the lane
effect is `mu = mean(high-lane arms) − mean(low-lane arms)`, exact for a linear trend; the
balanced 2/2 orientation also cancels a quadratic trend. The population is the **whole eligible
population** (`--batches 0`): **8,370** rows as **9** batches of 1,000 (eight full batches and a
370-row remainder), versus the **3,000** rows the earlier passes sampled. Only
`activeAuditHomepageFetchConcurrency` differs (32 vs 64); per-host stays **3**, `timeout_s`
**6**, and every arm runs in its own `BALUFFO_DATA_DIR` with the request trace on.

The same estimator is applied to every endpoint: homepage phase wall, whole-run wall, homepages
and recovery pages, probed/provider/static candidates, active boards, technical failures,
coverage misses and homepage failures. The **same-lane floor** is the largest spread between the
two arms of one lane across the four quads; timing is tested against the **32-lane** spread. A
board is a lane loss only when present in **both** low-lane arms and in **neither** high-lane
arm; a **clean** loss excludes boards touching a host that answered 429 in any of the quad's four
arms, and each change is attributed with the lane-parity host-failure rule. A quad is valid only
when every arm completed the full eligible population (`count == eligibleRows`, one eligible
count across arms) in addition to the existing lane, trace, population and contiguity gates; all
four quads are valid.

## Timing

| Quad | Order | Home 32 s | Home 64 s | Home mu s | 32 spread | 64 spread | Gain > 32 spread | Wall 32 s | Wall 64 s | Wall mu s | 32 spread |
|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|
| 1 | ABBA | 242.0 | 145.2 | −96.8 | 27.1 | 12.1 | yes | 821.9 | 667.6 | −154.2 | 28.7 |
| 2 | BAAB | 232.0 | 147.8 | −84.2 | 10.1 | 14.6 | yes | 798.2 | 736.1 | −62.2 | 107.8 |
| 3 | ABBA | 226.3 | 159.2 | −67.1 | 3.0 | 23.4 | yes | 812.9 | 710.7 | −102.1 | 99.3 |
| 4 | BAAB | 211.3 | 151.4 | −59.9 | 5.0 | 8.3 | yes | 770.1 | 737.6 | −32.5 | 55.5 |

The homepage phase is **−77,024 ms** (95% CI **[−103,568, −50,480]**), negative in **4/4** quads
and beyond the 32-lane spread in **4/4** — the lane knob's own endpoint is decisive at production
size. The full-audit wall is **−87.8 s** (95% CI **[−171.6, −3.9]**), also negative in **4/4**,
but beyond the 32-lane spread in only **2/4**. The gate requires both, so **timing fails**, now
on the whole-run condition alone.

The two quads that fail are the ones whose own 32-lane arms disagree most: q2's 32-lane arms ran
**852.1 s** and **744.3 s** (spread **107.8 s**) while its 64-lane arms sat at **737.6 s** and
**734.5 s**; q4's 32-lane arms ran **797.8 s** and **742.3 s** (spread **55.5 s**) while its
64-lane arms sat at **714.3 s** and **760.8 s**. The lane separation itself is stable — the two
64-lane arms within a quad differ by at most **46.5 s** (37.0, 3.1, 38.4, 46.5) — but the two
32-lane arms differ by **107.8 s** and **55.5 s** in the two failing quads, more than the lane's
gain there. That
weather is exactly what the quad estimator removes from the *mean* (which stays −87.8 s with an
interval excluding zero) but what the gate's per-quad spread test still counts.

## Pages and candidates

| Endpoint | 32 mean | 64 mean | mu | Same-lane floor | Gate |
|---|---:|---:|---:|---:|---|
| Homepages fetched | 7,514.4 | 7,475.9 | −38.5 | 60 | pass |
| Recovery pages | 2,256.1 | 2,256.4 | +0.2 | 16 | pass |
| Probed candidates | 9,056.9 | 9,046.4 | −10.5 | 52 | pass |
| Provider candidates | 7,315.2 | 7,314.4 | −0.9 | 42 | pass |
| Static candidates | 1,741.6 | 1,732.0 | −9.6 | 21 | pass |
| Active boards | 344.6 | 344.6 | 0.0 | 6 | pass |
| Technical failures | 9,357.6 | 9,365.6 | +8.0 | 39 | pass |
| Coverage misses | 4,114.2 | 4,108.4 | −5.9 | 31 | pass |
| Homepage failures | 854.6 | 893.1 | +38.5 | 60 | pass |

Every endpoint is inside its same-lane floor; **pages pass**. The 64-lane arms fetched **38.5**
fewer homepages of ~**7,514** (−0.5%) and recorded **38.5** more homepage failures — the same
quantity with opposite signs, consistent with a small rate rise, and well inside the 60-page
churn floor. The two-pair acceptance's **−96/−34** page losses remain unreproduced: the
per-quad page deltas here are not even signed consistently by lane, and the floor test is
untroubled.

## Boards

| Quad | Order | Boards 32 | Boards 64 | Stable 32 | Stable 64 | Lost | Gained | Clean lost | Clean gained |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | ABBA | 344.5 | 344.5 | 339 | 342 | 0 | 0 | 0 | 0 |
| 2 | BAAB | 346.0 | 344.0 | 342 | 341 | 1 | 0 | 1 | 0 |
| 3 | ABBA | 346.0 | 344.0 | 341 | 344 | 0 | 0 | 0 | 0 |
| 4 | BAAB | 342.0 | 346.0 | 339 | 341 | 0 | 0 | 0 | 0 |

No board is a consistent lane loss. The active-board count is **0.0** against a **6**-board
floor and **boards pass**. Three of four quads have no stable identity change at all. The single
change is the first clean board loss seen in this investigation: in q2, `arsanesia.com`
(Arsanesia) is present in both 32-lane arms and absent from both 64-lane arms, and its host
never answered 429 in any of the four arms — both 64-lane arms rejected the row as
`homepage_fetch_failed` with a blank error, a tail fetch failure at 6 s. One board of ~**345**
in one quad is inside the board rule's floor and is not a lane mechanism: it is the same tail
event class as the two-pair acceptance's clean-host probe timeout, now on the homepage side. It
is reported rather than smoothed away; the plan's board condition (no regression beyond the
same-lane floor, no consistent clean loss) still passes.

## Rate

| Quad | Order | 32 rate | 64 rate | mu pp | Same-lane drift pp |
|---|---|---:|---:|---:|---:|
| 1 | ABBA | 3.82% | 4.23% | +0.41 | +0.10 |
| 2 | BAAB | 3.73% | 4.16% | +0.42 | −0.57 |
| 3 | ABBA | 3.76% | 4.65% | +0.89 | −0.76 |
| 4 | BAAB | 4.23% | 4.22% | −0.01 | −0.12 |

The quad mean is **+0.43 pp** (95% CI **[−0.16, +1.01]**, host bootstrap **[+0.31, +0.55]**;
**3/4** positive, sign p=0.625). This is the first pass in the investigation where the
**acceptance rate gate passes** — the interval upper is below the **+1.5 pp** pass bound and the
point estimate below **+1.0 pp** — so the +2.42 pp objection stays dead. It is not a zero-cost
finding: the bootstrap bounds a small positive cost, and the pre-registered **0.5 pp** precision
question remains `inconclusive-at-0.5pp`, with the eight-quad 1,000-row read (**+0.37 pp**,
**[−0.44, +1.17]**) still the tighter estimate. The secondary phases read **+0.47 pp** on the
recovery fetch and **+0.10 pp** on the probe, neither of which the lane knob changes.

## Phase attribution

The whole-run gain is mostly the homepage phase plus one secondary phase:

| Phase | 32 mean | 64 mean | delta |
|---|---:|---:|---:|
| `homepage_fetch` | 227.9 s | 150.9 s | **−77.0 s** |
| `probe` | 216.7 s | 198.1 s | −18.6 s |
| `recovery_wave1_fetch` | 149.8 s | 149.2 s | −0.6 s |
| `homepage_analysis` | 126.8 s | 130.7 s | +3.9 s |
| `recovery_wave2_fetch` | 59.6 s | 62.5 s | +2.9 s |
| analyses + merge | ~15.4 s | ~16.8 s | +1.4 s |

The homepage phase is the lane knob's endpoint and moves as designed. The probe phase's
**−18.6 s** is not a pre-registered endpoint and is reported as secondary: it is plausibly a
contention effect (the probe wave shares the process with the homepage fetch) or candidate-count
noise, and it is not claimed as a lane win. The remaining phases — recovery fetch, homepage
analysis, recovery wave 2 — move by single-digit seconds or not at all, which is why the
whole-run spread is dominated by weather rather than by the knob.

## Gates

| Plan step 5 condition | Measurement | Result |
|---|---|---|
| Paired fetch-wall gains recur beyond the 32-lane spread | homepage **−77.0 s**, 4/4 and 4/4 beyond; full audit **−87.8 s**, 4/4 but only 2/4 beyond, 95% CI **[−171.6, −3.9]** | **Fail** on the full-audit condition |
| Pages and downstream candidates do not regress | homepages **−38.5** (floor 60); recovery **+0.2** (16); probed **−10.5** (52); provider **−0.9** (42); static **−9.6** (21); boards **0.0** (6); technical **+8.0** (39); coverage **−5.9** (31); homepage failures **+38.5** (60) | **Pass** |
| Host-level throttling does not rise materially | homepage **+0.43 pp**, 95% CI **[−0.16, +1.01]**, bootstrap **[+0.31, +0.55]**; acceptance rate gate **passes**; **0.5 pp** question inconclusive | **Pass** at the acceptance bound |
| Board identities hold | **0** consistent losses; **1** clean loss in one quad (`arsanesia.com`, blank-error homepage fetch in both 64 arms), inside the 6-board floor | **Pass** |

**Verdict: acceptance not met — keep 32.** The lane question is now measured at both sizes it can
be measured at locally. The homepage phase — the only thing the setting changes — is robustly
faster at 64 in every quad and at every size (**−77.0 s** on 8,370 rows, **4/4** beyond the
32-lane spread; **−18.1 s** on 3,000 rows), pages, boards and the acceptance rate bound all pass,
and no lane-driven board or throttle cost is measurable. What still blocks the config is the
full-audit wall's own run-to-run spread: the lane gain is **−87.8 s** on average and negative
**4/4**, but the two arms of the same lane diverge by up to **107.8 s** across the ~13-minute
runs, so a per-quad "gain exceeds same-lane spread" test is not met in a stable 3 of 4 quads.
With the step-5 gate now executed at 3,000 rows and at the full eligible population, the plan's
decision rule resolves: **32 stays**, and the next investigation is homepage-analysis cost and
the other lane-independent phases — `homepage_analysis` at **126.8 s** (32) / **130.7 s** (64),
`probe` at **216.7 s** (32) / **198.1 s** (64), `recovery_wave1_fetch` at ~**150 s** both, none
of which the lane count improves. At 64 lanes the probe phase becomes the audit's largest stage.

## Limits

One population on one day, live hosts, local `f60de72e`, four quads, no deployed host. The
full-audit spread test is conservative by construction: it asks each quad to beat the same-lane
weather measured in that same quad, and weather can exceed the lane gain on a 13-minute run. The
**+0.43 pp** rate interval is not powered for **0.5 pp**; it bounds a small positive cost, not
zero. The clean board loss is a single tail event, inside the floor, and its cause is a blank
error on the homepage fetch rather than an identified mechanism — a single observation cannot
separate a lane effect from host weather, and the other three quads show no change. The 64-lane
arms' homepage-phase spreads (**8.3–23.4 s**) are still wider than the 32-lane arms'
(**3.0–27.1 s**), consistent with the earlier pass. The CSV can change between arms; the digest
gate held one population for all 16 arms here, but a future run needs the same check.

The tool that runs this pass is the extended quad runner in
`_out/perf-runs/discovery/lane-acceptance-production-20261027/quad_study.py` (`--batches 0`
audits the whole eligible population, and a quad is invalid unless every arm completed it);
the pre-registration is `design.md` beside it, and the evidence is `manifest.json`,
`progress.jsonl`, `arms/*/arm.json` and `arms/*/request-trace.jsonl`. The 3,000-row pass is in
[the quad acceptance snapshot](discovery-64-lane-acceptance-quad-2026-10-27.md), the two-pair
gate in [the acceptance snapshot](discovery-64-lane-acceptance-2026-10-27.md), the rate re-reads
in [the rate quad snapshot](discovery-homepage-lane-rate-quad-2026-10-27.md) and
[the parity snapshot](discovery-64-lane-parity-2026-10-26.md). This pass closes the
homepage-lane investigation plan, which is deleted with this record.

## Reproducing the numbers

```bash
# the study: four counterbalanced quads over the whole eligible population (~8,370 rows)
python _out/perf-runs/discovery/lane-acceptance-production-20261027/quad_study.py run \
  --study _out/perf-runs/discovery/lane-acceptance-production-20261027 \
  --quads 4 --batches 0 --batch-size 1000 --timeout 6 --seed 202610271 --acceptance

# re-render from the arm records without re-running anything
python _out/perf-runs/discovery/lane-acceptance-production-20261027/quad_study.py acceptance \
  --study _out/perf-runs/discovery/lane-acceptance-production-20261027
```

`run` is resumable: an arm whose record exists is skipped after its lane count is checked.

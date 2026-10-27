# 3,000-row quad acceptance: the earlier page/board losses do not reproduce; full-audit timing and rate stay unresolved — keep 32 — 2026-10-27

> - **Class:** measurement — runs the 3,000-row page/board gate the two-pair acceptance left open; no shipped behaviour changes.
> - **Trigger:** the two-pair acceptance failed step 5 at 3,000 rows on pair-2 timing, **96** and **34**
>   lost pages and **3** lost boards, while the 1,000-row rate quad re-read its **+2.42 pp** as
>   **+0.37 pp**. This pass re-measures pages, candidates, boards and timing on the same 3,000 rows
>   with the quad's time-trend correction, so all endpoints are judged on one design.
> - **Status:** **acceptance not met — keep 32.** Four 3,000-row quads (16 arms, 2 BAAB / 2 ABBA,
>   seed `20261027`) on one population (`08d24f4435c54bfb`, CSV `896da851…`) give 64 a **18,096 ms**
>   homepage-phase gain (95% CI **[−25,534, −10,658]**, **4/4** quads negative, **4/4** beyond the
>   32-lane spread), but the full-audit wall gain is **15.0 s** with a 95% interval **[−32.5, +2.5]**
>   and only **2/4** quads beyond the 32-lane spread, so the plan's full-audit timing condition is
>   not met. Pages are **+4.5** per arm against a **99**-page same-lane floor, every candidate
>   endpoint is inside its floor, active boards are **−0.2** against a **4**-board floor with **0**
>   consistent clean losses, and the one gained board is a **429** in both 32-lane arms. The homepage
>   rate is **−0.15 pp** (95% CI **[−1.95, +1.65]**, host bootstrap **[−0.31, +0.01]**), so the
>   recorded **+2.42 pp** does not recur, but four quads miss the pre-registered **+1.5 pp** pass
>   (`inconclusive-at-0.5pp`). The config stays at 32; the recorded page/board objection does not
>   reproduce (the page floor is wide), while the full-audit timing and rate precision remain.
> - **Verified against:** `f60de72e` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), population digest `08d24f4435c54bfb` in all 16 arms,
>   2026-10-27T09:59:57Z–11:09:09Z. All 16 arms prove `configuredLanes` and a peak of 32 or 64 and
>   carry a request trace; 16 arms over **69 min**, per-arm audit walls **220.7–288.2 s**.
> - **Then inspect:** `_out/perf-runs/discovery/lane-acceptance-quad-20261027/` — `design.md`,
>   `manifest.json`, `progress.jsonl`, `arms/*/arm.json`, each arm's `request-trace.jsonl`,
>   `acceptance-report.json` / `.txt`, `rate-report.json` / `.txt`.

## Design

Four pre-registered quads, each running four arms on the same population in one of two orders:
**BAAB** 64/32/32/64 or **ABBA** 32/64/64/32. For arm values `y1..y4` in time order, the lane
effect is `mu = mean(high-lane arms) − mean(low-lane arms)`, exact for a linear trend; the balanced
2/2 orientation also cancels a quadratic trend. The population is the first **3,000** representative
rows as three 1,000-row batches; only `activeAuditHomepageFetchConcurrency` differs (32 vs 64),
per-host stays **3**, `timeout_s` **6**, and every arm runs in its own `BALUFFO_DATA_DIR` with the
request trace on. Four quads is the smallest balanced set with a t interval; the 0.5 pp rate question
already has eight quads at 1,000 rows and is not re-opened here.

The same estimator is applied to every endpoint: homepage phase wall, whole-run wall, homepages and
recovery pages, probed/provider/static candidates, active boards, technical failures, coverage
misses and homepage failures. The **same-lane floor** is the largest spread between the two arms of
one lane, across the four quads; timing is tested against the **32-lane** spread, the plan's
reference. Boards are compared by **host + tenant** identity. A board is a lane loss only when it is
present in **both** low-lane arms and in **neither** high-lane arm; a board present in one high arm
is same-lane churn. A **429-free** loss excludes boards touching any host that answered 429 in any
of the quad's four arms, and each change is attributed with the lane-parity host-failure rule.

Pre-registered gates: timing passes only if the homepage phase and whole-run wall both have a
negative quad mean, a 95% upper bound below 0, at least 3/4 negative quads and 3/4 gains beyond the
32-lane spread; pages/candidates fail only outside their same-lane floor; boards fail on a count
below the floor or a consistent clean loss beyond it; rate passes if the interval upper is below
**+1.5 pp** and the point below **+1.0 pp**, fails if the lower bound is above **+0.5 pp**, and is
otherwise inconclusive.

## Timing

| Quad | Order | Home 32 ms | Home 64 ms | Home mu ms | 32 spread | 64 spread | Gain > 32 spread | Wall 32 s | Wall 64 s | Wall mu s |
|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|
| 1 | BAAB | 72,292 | 56,926 | −15,366 | 5,165 | 22,617 | yes | 253.4 | 248.5 | −5.0 |
| 2 | BAAB | 71,583 | 47,353 | −24,230 | 1,472 | 1,730 | yes | 285.5 | 260.2 | −25.3 |
| 3 | ABBA | 70,921 | 51,816 | −19,106 | 2,376 | 1,991 | yes | 259.6 | 235.9 | −23.7 |
| 4 | ABBA | 70,916 | 57,233 | −13,682 | 3,591 | 13,776 | yes | 264.5 | 258.4 | −6.1 |

The homepage phase is **−18,096 ms** (95% CI **[−25,534, −10,658]**), negative in **4/4** quads and
beyond the 32-lane spread in **4/4**. The full-audit wall is **−15.0 s** (95% CI **[−32.5, +2.5]**),
also negative in **4/4**, but only **2/4** beyond the 32-lane spread. The gate requires both: the
homepage condition passes, the full-audit condition fails, so **timing fails**.

The 64-lane arms are the noisier side in quads 1 and 4: their homepage spreads are **22.6 s** and
**13.8 s** against 32-lane spreads of 5.2 s and 3.6 s. Quad 1's second 64-lane arm ran the homepage
phase in **45.6 s**, while its first took **68.2 s**; the quad mean is still −15.4 s, but that
within-lane spread is exactly the run-to-run variation the gate refuses to price as a lane gain.

## Pages and candidates

| Endpoint | 32 mean | 64 mean | mu | Same-lane floor | Gate |
|---|---:|---:|---:|---:|---|
| Homepages fetched | 2,677.6 | 2,682.1 | +4.5 | 99 | pass |
| Recovery pages | 758.8 | 756.2 | −2.5 | 10 | pass |
| Probed candidates | 3,001.5 | 3,001.9 | +0.4 | 11 | pass |
| Provider candidates | 2,435.1 | 2,435.1 | 0.0 | 2 | pass |
| Static candidates | 566.4 | 566.8 | +0.4 | 11 | pass |
| Active boards | 107.0 | 106.8 | −0.2 | 4 | pass |
| Technical failures | 3,350.5 | 3,353.5 | +3.0 | 16 | pass |
| Coverage misses | 1,359.5 | 1,356.6 | −2.9 | 14 | pass |
| Homepage failures | 322.4 | 317.9 | −4.5 | 99 | pass |

The per-quad page deltas are **+55.0, −10.5, −18.0, −8.5**; the positive mean is one quad's gain,
not a recurring loss. The **−96** and **−34** page losses from the two-pair acceptance do not recur
(and 64's two arms here fetched **2,653–2,752** pages, bracketing the 32 arms' **2,636–2,752**).
Every primary endpoint is within its same-lane floor; **pages pass**.

## Boards

| Quad | Order | Boards 32 | Boards 64 | Stable 32 | Stable 64 | Lost | Gained | Clean lost | Clean gained |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | BAAB | 106.0 | 107.5 | 105 | 105 | 0 | 1 | 0 | 0 |
| 2 | BAAB | 107.5 | 107.0 | 106 | 105 | 0 | 0 | 0 | 0 |
| 3 | ABBA | 107.5 | 106.5 | 106 | 106 | 0 | 0 | 0 | 0 |
| 4 | ABBA | 107.0 | 106.0 | 105 | 104 | 0 | 0 | 0 | 0 |

No board is a consistent lane loss. The one gained board is `www.enhance-experience.com`: both
32-lane arms (q1a2, q1a3) answered **homepage 429** for it, while both 64-lane arms read it. Quads 2
and 4 have a stable-low set one larger than stable-high, but that extra board is present in one high
arm, so it is churn, not a lane loss. The active-board count is **−0.2** against a **4**-board floor,
and there are **0** clean losses; **boards pass**.

## Rate

| Quad | Order | 32 rate | 64 rate | mu pp | Same-lane drift pp |
|---|---|---:|---:|---:|---:|
| 1 | BAAB | 5.60% | 3.77% | −1.83 | +3.60 |
| 2 | BAAB | 4.20% | 4.60% | +0.40 | −0.73 |
| 3 | ABBA | 4.53% | 5.15% | +0.62 | +0.07 |
| 4 | ABBA | 4.18% | 4.40% | +0.22 | −1.63 |

The quad mean is **−0.15 pp** (95% CI **[−1.95, +1.65]**, host bootstrap **[−0.31, +0.01]**;
**3/4** positive, sign p=0.625). The wide interval is honest: quad 1's 64-lane arms moved **1.97% →
5.57%** (59 and 167 of 3,000) while its 32-lane arms read **5.33%** and **5.87%**, a **+3.60 pp**
same-lane drift that the quad removes. The recorded **+2.42 pp** sits outside this interval, so the
two-pair rise does not reproduce; but the interval's upper **+1.65 pp** misses the pre-registered
**+1.5 pp** pass by 0.15 pp, and the four-quad design is not powered for the **0.5 pp** question.
The rate gate is **inconclusive**, not a material-cost finding. The eight-quad 1,000-row read
(**+0.37 pp**, 95% CI **[−0.44, +1.17]**) remains the tighter rate estimate and is not contradicted
here.

## Gates

| Plan step 5 condition | Measurement | Result |
|---|---|---|
| Paired fetch-wall gains recur beyond the 32-lane spread | homepage **−18.1 s**, 4/4 and 4/4 beyond; full audit **−15.0 s**, 4/4 but only 2/4 beyond, 95% CI **[−32.5, +2.5]** | **Fail** on the full-audit condition |
| Pages and downstream candidates do not regress | homepages **+4.5** (floor 99); recovery **−2.5** (10); probed **+0.4** (11); provider **0.0** (2); static **+0.4** (11); boards **−0.2** (4); technical **+3.0** (16); coverage **−2.9** (14); homepage failures **−4.5** (99) | **Pass** |
| Host-level throttling does not rise materially | homepage **−0.15 pp**, 95% CI **[−1.95, +1.65]**, bootstrap **[−0.31, +0.01]**; upper misses **+1.5** | **Inconclusive** |
| Board identities hold | **0** consistent losses; **0** clean losses; one gained board is a 429 in both 32-lane arms | **Pass** |

**Verdict: acceptance not met — keep 32.** The two-pair acceptance's page and board failures do not
reproduce under the time-trend correction: the **−96/−34** page losses tracked 429s, and no board is
a consistent loss here — the two 429 losses were weather and the one 6 s timeout was a single-arm
probe event. The quad finds **+4.5** pages and no stable clean board loss. The direct homepage phase
gain is real and consistent (**18.1 s**, 4/4, interval excluding zero, 4/4 beyond the 32-lane
spread), but the **full-audit** gain is only **15.0 s** and its interval includes zero; the plan's
acceptance rule is about the full audit, so 64 is still not supported. The rate question is not
worse than the 1,000-row read, but four quads cannot clear the pre-registered **+1.5 pp** bound.

The next decision point is therefore not another 1,000-row rate pass: it is either a
production-sized full-audit acceptance (one deployable run whose whole-run gain clears the 32-lane
spread, plus the page/board checks this pass now supplies) or a homepage-analysis investigation,
since the **18 s** homepage gain is only about a quarter of the **71 s** homepage phase, and the
homepage-analysis stage is the next unaddressed cost.

## Limits

One population on one day, local `f60de72e`, four quads, 3,000 rows; no deployed host. The rate
interval is wide by construction and cannot resolve 0.5 pp. The page floor is 99 because the 32-lane
arms' own page churn is large; the observed +4.5 is far inside it, and even the two-pair
acceptance's **−96** would sit inside it — the gate catches a loss beyond 99 per arm, not a 3%
page loss. The **+4.5** point estimate and the absence of a consistent board loss are the
substantive evidence, not the floor test alone. The board rule requires both same-lane arms to agree;
it deliberately treats a board lost in one of two arms as churn. The 64-lane arms' own timing
variation (22.6 s and 13.8 s homepage spreads in quads 1 and 4) is real and included, and it is the
reason the timing gate is conservative. The 429-free screen is over-inclusive on sibling hosts.

The tool that reproduces the read is
[`tools/measurements/discovery/lane_parity.py`](../../tools/measurements/discovery/lane_parity.py);
the pre-registration and the 16-arm runner are preserved beside the evidence in
`_out/perf-runs/discovery/lane-rate-quad-20261027/quad_study.py` and
`_out/perf-runs/discovery/lane-acceptance-quad-20261027/design.md`. The prior rate result is in
[the rate quad snapshot](discovery-homepage-lane-rate-quad-2026-10-27.md), the two-pair gate in
[the acceptance snapshot](discovery-64-lane-acceptance-2026-10-27.md), and the investigation plan in
[the production acceptance snapshot](discovery-64-lane-production-acceptance-2026-10-27.md), which closed the homepage-lane investigation and deleted its plan.

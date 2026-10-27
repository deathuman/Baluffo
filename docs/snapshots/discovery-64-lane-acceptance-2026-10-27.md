# 64 lanes fails its recorded acceptance gate — pair 2 inside the noise, pages down — keep 32 — 2026-10-27

> - **Class:** measurement — runs the homepage-lane plan's step 5 acceptance gate; no shipped behaviour changes.
> - **Trigger:** the 2026-10-26 parity work closed R4's board and rate questions on that study, leaving
>   one decision: run the plan's full-audit acceptance on the matching build before proposing
>   `activeAuditHomepageFetchConcurrency: 64`.
> - **Status:** **acceptance not met — keep 32.** Two counterbalanced pairs on the same 3,000 rows give
>   64 a **2.02×** and **1.40×** homepage gain, but pair 2's **19.7 s** homepage gain and **3.1 s**
>   whole-run gain sit inside the 32-lane same-lane spread (**22.1 s** / **51.8 s**), so the timing case
>   does not recur beyond the noise. 64 fetched **96** and **34** fewer pages while homepage **429**s
>   rose **105** and **40**; the per-host, order-cancelled homepage rate is **+2.42 pp** (95% CI
>   **[+1.80, +2.98]**, **+127/−35** hosts); and **3** boards were lost (two homepage 429s, one
>   clean-host 6 s probe timeout) against **0** gained. The separate config change is not supported.
> - **Follow-up (same day):** a pre-registered **8-quad** ABBA/BAAB rate study re-read the homepage
>   rate with the within-pair trend removed: **+0.37 pp**, 95% **t** **[−0.44, +1.17]**, host
>   bootstrap **[+0.12, +0.64]**, **6/8** quads positive. The **+2.42 pp** above is outside that
>   interval, so the rate rise is attributed to weather, not a lane cost; the pre-registered verdict is
>   `inconclusive-at-0.5pp` (the design's MDE is ~**1.0 pp**). Its exploratory timing read is
>   **−6.06 s** per 1,000 rows on the homepage phase (**8/8** quads, 95% CI **[−7.38, −4.73]**),
>   while whole-run is **−3.8 s** **[−11.0, +3.4]**. The config still stays 32: this study did not
>   re-measure pages or boards, and the gate as taken still fails on pair 2's whole-run timing.
>   See [the quad snapshot](discovery-homepage-lane-rate-quad-2026-10-27.md).
> - **Verified against:** `f60de72e` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), population digest `08d24f4435c54bfb`, 2026-10-27T00:40:48Z–00:56Z.
> - **Then inspect:** `_out/perf-runs/discovery/lane-parity-acceptance/` — four arm records,
>   `report.json`, `report.txt`, `trace.txt`, `rate_analysis.py`, `rate-analysis.txt`, and each arm's
>   `request-trace.jsonl`.

## Design

Each arm is one isolated GameDevMap active dry run with its own `BALUFFO_DATA_DIR` and artifact path,
so no arm reads another's cache and repo `data/` is untouched. The population is the same pinned first
**3,000** representative rows as three 1,000-row batches; every arm audited the identical set
(`completedUrls` digest `08d24f4435c54bfb`) and the same CSV SHA-256. The only setting that differs is
`activeAuditHomepageFetchConcurrency`; per-host stays **3**, `timeout_s` **6**, and every other policy
is the default. Pair 1 ran 32 then 64; pair 2 ran 64 then 32. The request trace was on in all four
arms.

| Arm | Order | Lanes | Wall s | Homepage ms | Peak | Pages | Recovery pages | Boards | Probed / provider / static | Homepage 429 |
|---|---|---|---|---|---|---|---|---|---|---|
| `p1-32` | 1st | 32 | 282.6 | 91,337 | 32 of 32 | 2,743 | 761 | 109 | 3,012 / 2,435 / 577 | 60 (2.00%) |
| `p1-64` | 2nd | 64 | **212.7** | **45,325** | 64 of 64 | 2,647 | 761 | 107 | 3,000 / 2,435 / 565 | 165 (5.50%) |
| `p2-64` | 1st | 64 | **227.7** | **49,501** | 64 of 64 | 2,639 | 755 | 106 | 2,996 / 2,435 / 561 | 177 (5.90%) |
| `p2-32` | 2nd | 32 | 230.8 | 69,220 | 32 of 32 | 2,673 | 763 | 107 | 3,003 / 2,436 / 567 | 137 (4.57%) |

The lane knob is proven from each artifact's own receipt, not from the setting: `configuredLanes`
32 / 64 with `peakInFlight` reaching **32** and **64**, mean in flight **25.4 / 43.1** at 32 / 64 in
pair 1 and **25.3 / 42.6** in pair 2.

## Timing

| Pair | Wall 32 → 64 | Wall gain | Homepage 32 → 64 | Homepage gain |
|---|---|---|---|---|
| 1 (32 first) | 282.6 → 212.7 s | **69.9 s**, 1.329× | 91,337 → 45,325 ms | **46,012 ms**, 2.015× |
| 2 (64 first) | 230.8 → 227.7 s | **3.1 s**, 1.014× | 69,220 → 49,501 ms | **19,719 ms**, 1.398× |

The 32-lane same-lane spread is **51.8 s** on wall and **22,117 ms** on the homepage phase; the
64-lane spread is **15.0 s** and **4,176 ms**. Pair 1 clears the 32-lane spread on both measures.
Pair 2 clears neither: its homepage gain is **19.7 s** against a **22.1 s** spread, and its whole-run
gain is **3.1 s** against **51.8 s**. Pooled means are **256.7 → 220.2 s** (1.166×) and
**80,279 → 47,413 ms** (1.693×), but the acceptance rule is per pair, and one pair does not recur
beyond the noise.

## Pages and candidates

| Measure | p1 32 → 64 | p1 delta | p2 32 → 64 | p2 delta | 32 churn | 64 churn |
|---|---|---|---|---|---|---|
| Homepages fetched | 2,743 → 2,647 | **−96** | 2,673 → 2,639 | **−34** | 70 | 8 |
| Recovery pages | 761 → 761 | 0 | 763 → 755 | **−8** | 2 | 6 |
| Active boards | 109 → 107 | −2 | 107 → 106 | −1 | 2 | 1 |
| Probed candidates | 3,012 → 3,000 | −12 | 3,003 → 2,996 | −7 | 9 | 4 |
| Provider candidates | 2,435 → 2,435 | 0 | 2,436 → 2,435 | −1 | 1 | 0 |
| Static candidates | 577 → 565 | −12 | 567 → 561 | −6 | 10 | 4 |
| Zero-job candidates | 1,042 → 1,043 | +1 | 1,042 → 1,040 | −2 | 0 | 3 |
| Rejected for activation | 4,712 → 4,712 | 0 | 4,712 → 4,713 | +1 | 0 | 1 |
| Technical failures | 3,352 → 3,359 | +7 | 3,354 → 3,364 | +10 | 2 | 5 |
| Total failures | 4,778 → 4,537 | −241 | 4,662 → 4,588 | −74 | 116 | 51 |
| Coverage misses | 1,360 → 1,353 | −7 | 1,358 → 1,349 | −9 | 2 | 4 |

Every arm attempted the same **3,000** homepages. The page loss tracks the throttle exactly:
pair 1 adds **105** homepage 429s and loses **96** pages; pair 2 adds **40** 429s and loses **34**.
The 64-lane arms are also the tighter pair on pages (8 apart) while the 32-lane arms differ by 70,
so the loss is not the 64 arms wandering; both 64 arms sit below both 32 arms. The primary downstream
measure, active boards, moves **−2** and **−1**, at or inside the 2-board same-lane churn; provider
candidates are flat. Probed and static candidates fall beyond their floors in pair 1 (−12 against 9
and 10), recovery pages fall beyond the floor in pair 2 (−8 against 6), and technical failures rise
by **7** and **10** against a 5-count floor.

## Active boards

Boards are compared by **host + tenant** (`core_identity.board_identity_key`), never studio label or
id. **3** boards were lost and **0** gained across the two pairs:

| Pair | Boards 32 → 64 | Lost | Attribution |
|---|---|---|---|
| 1 | 107 → 105 | 2 | `www.2awesomestudio.com` and `www.finitude.dev`, both **homepage 429** |
| 2 | 105 → 104 | 1 | `gameartpioneers.com`, a **6 s probe timeout**; no 429 on its host |

The pre-registered 429-free screen excludes every board touching a host that answered 429 in either
arm. Pair 1 excludes **2 / 0** boards on **269** throttled hosts and leaves **105 vs 105**, symmetric
difference **0**. Pair 2 excludes **0 / 0** on **289** throttled hosts and leaves **105 vs 104**; the
one clean difference is the timeout, so the screen verdict is
`parity-not-supported-on-429-free-hosts`. That single loss is inside the 2-board same-lane churn and
is not a throttle event.

The strict report verdict is `parity-not-supported` on failure counts, not on the board deltas: pair 1's
failure-identity difference is **649** against a **502** floor, its total-failure delta is **241**
against **116** (with **32** holding the extra failures), and its homepage-failure delta is **96**
against **70** (with **64** holding the extra homepage failures); pair 2's failure-identity difference
is **687** against **502**. The board deltas themselves (**2** and **1**) are at or under the 2-board
floor.

## Per-host 429s

The traces are compared host with host on the common set — **2,990** homepage hosts, **566** probe
hosts and **2,056** recovery hosts — because a lane effect has to move a host's own rate, not the
aggregate mix.

| Phase | p1 32 | p1 64 | p2 64 | p2 32 | 64 − 32, order-cancelled | 95% CI | Hosts + / − |
|---|---|---|---|---|---|---|---|
| Homepage | 60/3,000 (2.00%) | 165/3,000 (5.50%) | 177/3,000 (5.90%) | 137/3,000 (4.57%) | **+2.42 pp** | **[+1.80, +2.98]** | **+127 / −35**, p<0.001 |
| Probe | 239/3,140 (7.61%) | 253/3,121 (8.11%) | 250/3,109 (8.04%) | 243/3,104 (7.83%) | +0.35 pp | [+0.00, +1.06] | +2 / −0, p=0.500 |
| Recovery | 254/4,722 (5.38%) | 240/4,706 (5.10%) | 369/4,794 (7.70%) | 302/4,746 (6.36%) | +0.53 pp | [+0.08, +0.98] | +13 / −6, p=0.167 |

The homepage result is the material one: **+2.42 pp** order-cancelled, a clustered bootstrap CI of
**[+1.80, +2.98]**, and **127** hosts whose own rate rose against **35** whose rate fell. The probe
phase is effectively flat (**2** hosts changed, both up); recovery is smaller and only borderline
significant. The four big APIs are unchanged in every arm: `apply.workable.com` at **100%** and
`api.lever.co`, `api.smartrecruiters.com`, `boards-api.greenhouse.io` at **0%**. The **33** hosts that
throttled 100% in all four arms are the fixed background, not the movers.

**Follow-up.** The quad study above re-read this rate with eight ABBA/BAAB quads and a linear-trend
correction: **+0.37 pp** **[−0.44, +1.17]**. The **+2.42 pp** reading here is outside that interval,
and the quad's same-lane drift reaches **+5.1 pp**, so the two-pair number is not a lane estimate.
The table above remains the recorded measurement; the quad reading supersedes it for the rate
question.

**Order caveat.** The order-cancelled estimate assumes the two orders cancel position, and this run
does not fully satisfy that. The 32 arms sat at the ends of the session and the 64 arms in the middle;
the raw homepage rate moved **2.00 → 5.50 → 5.90 → 4.57%** across the four arms. Both within-pair
transitions still move in the lane direction — up after 32 → 64 in pair 1, down after 64 → 32 in pair
2 — which a monotonic session trend alone does not explain, and the pair-2 drop is against that
trend. But the earlier 2026-10-26 study's 32-lane arms moved **2.17% → 8.13%** and its own
order-cancelled read was **−0.37 pp**, so two pairs cannot settle whether this run's **+2.42 pp** is
the lane setting or the session. That is why the gate fails on the measurement as taken and why the
plan's reopen condition is a position-randomized rate study.

## Acceptance gate

| Plan step 5 condition | Measurement | Result |
|---|---|---|
| Paired fetch-wall gains recur beyond the 32-lane spread | p1 wall **69.9 s** > **51.8 s** and homepage **46.0 s** > **22.1 s**; p2 wall **3.1 s** < **51.8 s** and homepage **19.7 s** < **22.1 s** | **Fail** |
| Pages and downstream candidates do not regress | pages **−96 / −34**; active boards **−2 / −1** (floor 2); provider **0 / −1**; probed and static pair 1 beyond their floors; recovery pages pair 2 **−8** > 6; technical failures **+7 / +10** | **Fail** (pages, with the primary board count inside churn) |
| Host-level throttling does not rise materially | homepage **+2.42 pp**, CI **[+1.80, +2.98]**, **+127 / −35** hosts | **Fail as recorded** — re-read by the quad study as **+0.37 pp** **[−0.44, +1.17]**; the **+2.42 pp** is outside it |

**Verdict: acceptance not met — keep 32.** The 64-lane speedup is real in pair 1 and in the pooled
means (**1.69×** homepage, **1.17×** wall), but the recorded pair 2 timing does not clear the
32-lane spread and pages fell **96** and **34**. The per-host rate rise that accompanied those page
losses is re-read by the quad study as **+0.37 pp**, not **+2.42 pp**, so the rate condition is
weather rather than a lane cost; the page condition remains as recorded and was not re-measured in
this pass.
The separate `activeAuditHomepageFetchConcurrency: 64` change should not be proposed on this
evidence. The 3,000-row quad acceptance has since re-measured pages and boards: both pass (**+4.5**
pages, **−0.2** boards, no consistent clean loss), but full-audit timing is **−15.0 s**
**[−32.5, +2.5]** (2/4 beyond the 32-lane spread) and the rate is **−0.15 pp** **[−1.95, +1.65]**
`inconclusive-at-0.5pp`, so 32 stays; see
[the quad acceptance](discovery-64-lane-acceptance-quad-2026-10-27.md).

## Reproducing the numbers

```bash
# the study: two counterbalanced pairs over the pinned first 3,000 rows
python tools/measurements/discovery/lane_parity.py study \
    --study _out/perf-runs/discovery/lane-parity-acceptance --pairs 2

# the registered verdict, with the 429-free screen
python tools/measurements/discovery/lane_parity.py report \
    --study _out/perf-runs/discovery/lane-parity-acceptance \
    --pairs p1:p1-32,p1-64 p2:p2-32,p2-64 --screen-429-free

# the per-phase, per-host trace table
python tools/measurements/discovery/lane_parity.py trace \
    --study _out/perf-runs/discovery/lane-parity-acceptance \
    --pairs p1:p1-32,p1-64 p2:p2-32,p2-64

# the per-host rate read, preserved beside the traces because it is not part of the tool
python _out/perf-runs/discovery/lane-parity-acceptance/rate_analysis.py \
    _out/perf-runs/discovery/lane-parity-acceptance

# the drift-cancelled rate re-read: 8 ABBA/BAAB quads on the first 1,000 rows
python _out/perf-runs/discovery/lane-rate-quad-20261027/quad_study.py run \
    --study _out/perf-runs/discovery/lane-rate-quad-20261027
```

An arm record is immutable; a re-run needs a fresh `--study` directory, and `--force` is reserved for
re-measuring a whole study. The arm logs, artifacts and traces stay under the study directory as
untracked evidence.

## Boundaries

No production configuration, concurrency, timeout, retry or per-host limit changed. No shipped
`src/`, `frontend/` or `tests/` file changed by this pass. Each arm wrote only inside its own
`BALUFFO_DATA_DIR`; repo `data/` was untouched and no discovery audit artifact was created there.
The pass was local only; no target-side run was started. "Matching build" here means the `f60de72e`
tree, not the deployed container: no `/ops/health` or container digest was read, so this is local
acceptance evidence, not a target-side acceptance.

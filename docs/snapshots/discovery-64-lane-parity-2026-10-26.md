# 64 lanes has no measured board cost and no measured host-rate cost — the board diff was 429 weather — 2026-10-26

> - **Class:** measurement — answers residual **R4** of the P6 closeout; no shipped behaviour changes.
> - **Trigger:** R4 recorded outcome parity as unproven from six pairs in which the 64-lane arm lost
>   boards **6/6** (two-sided sign test **p = 0.0312**). Its own closing condition was two more
>   counterbalanced pairs, or one production-sized 64-lane run with a per-host board diff.
> - **Status:** **no lane-driven board difference is measured.** Four counterbalanced pairs across two
>   same-day studies differ on **23 boards**, and **all 23** carry a **429** on the arm that lacks them:
>   12 Workable boards (11 throttled on `apply.workable.com`, one on its own homepage), plus **8** other
>   homepages and **3** probe 429s. A 64-lane arm also ran **clean** — the recorded pair 2 kept all four
>   Workable boards while a 32-lane arm lost them — so the lane count is neither necessary nor sufficient
>   for the loss. A P6-shaped three-pair re-run adds **13** changed boards, again **all 429**, but **7**
>   of them sit in probe and recovery phases the lane knob does not touch; its pre-registered verdict is
>   `parity-not-supported` and it isolates no lane cost.
>   A request trace over two oppositely-ordered pairs shows the 429s are **spread through 93-100% of
>   each phase**, not one throttle episode, and that the **same 32-lane arm's homepage 429 rate moves
>   2.2% → 4.2% between runs** — a swing larger than the reverse pair's 0.3 pp lane gap. The lane gap
>   is therefore not stable enough to price (§"Request trace").
>   A pre-registered **429-free screen** then re-judged four traced counterbalanced pairs over only the
>   boards whose hosts never answered **429** in either arm: every raw mover (**11 of 11**) is a 429, and
>   the clean board sets are **identical in all four pairs** (107/107, 105/105, 106/106, 103/103, symdiff
>   **0**), so no board moves where throttling is excluded. The separate question the screen cannot
>   answer — whether 64 lanes raise a host's own 429 rate — is then measured by comparing each host with
>   itself across the fresh study's four arms (§"Per-host rate"): the homepage order-cancelled delta is
>   **−0.37 pp** (95% CI **[−0.94, +0.10]**, upper bound **+0.10 pp**, about 3 requests in 3,000) and
>   the probe phase **−0.01 pp** with **0 of 541** hosts changing rate, so no host-level increase is
>   measured there either. The 2026-10-27 acceptance pass then measured a **+2.42 pp** homepage rise on a
>   fresh pair set, failed its gate, and left the config at 32
>   ([acceptance snapshot](discovery-64-lane-acceptance-2026-10-27.md)). A pre-registered
>   **8-quad** ABBA/BAAB re-read then measured **+0.37 pp** (95% CI **[−0.44, +1.17]**; **6/8**
>   quads positive), so the acceptance pass's **+2.42 pp** is outside the interval and is attributed
>   to weather, not a lane cost; the quad's same-lane drift reaches **+5.1 pp**. The **0.5 pp** question stays
>   `inconclusive` (MDE ~**1.0 pp**). See [the quad snapshot](discovery-homepage-lane-rate-quad-2026-10-27.md).
>   Timing reproduces across the four studies: the homepage phase is **1.39-1.96×** faster and whole-run
>   wall **1.04-1.40×**.
> - **Verified against:** `2f85d3db` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), 2026-10-26T13:28–13:50Z.
> - **Re-run:** the whole measurement again through `study --pairs 2` at `71bc5e11`,
>   2026-10-26T16:38:49Z-16:56Z, evidence in `_out/perf-runs/discovery/lane-parity-fresh/`. Population
>   digest `08d24f4435c54bfb` in all four arms — bit-identical to the four recorded here.
> - **P6-shaped re-run:** three counterbalanced pairs via `study --pairs 3`,
>   2026-10-26T19:27:18Z-19:54Z, evidence in
>   `_out/perf-runs/discovery/lane-parity-p6shape/`; same population digest and CSV. It repeats the
>   net board loss at 64 in **3/3** pairs (net **−7**) and every mover is a 429, but the strict rule
>   returns `parity-not-supported`; see §"P6-shaped re-run".
> - **429-free screen:** `report --screen-429-free` re-judges the pairs over only the boards whose
>   hosts never answered 429 in either arm. A fresh counterbalanced study
>   (`_out/perf-runs/discovery/lane-parity-429free/`, 2026-10-26T22:10-22:30Z, same digest and CSV) and
>   the two earlier traced pairs leave **0** clean board differences — all **11** raw movers are 429s —
>   so the board question closes on this population where throttling is absent. The rate question is
>   read separately on the fresh study's four arms and closes with it (§"Per-host rate").
> - **Per-host rate:** re-derived on 2026-10-27 from the fresh 429-free study's four arms, compared
>   host with itself because its pair 1 ran 32 then 64 and its pair 2 ran 64 then 32. All **2,990**
>   homepage hosts carry an identical URL set and request count in all four arms: the 64 − 32 delta is
>   **+3.17 pp** when 64 ran second and **−3.90 pp** when 32 ran second, leaving **−0.37 pp** (95% CI
>   **[−0.94, +0.10]**, upper bound **+0.10 pp**). The **541** common probe hosts are flat
>   (**0 of 541** changed), with `apply.workable.com` at **100%** and the other three big APIs at **0%**
>   in every arm. See §"Per-host rate".
> - **Quad re-read:** the 2026-10-27 acceptance pass's **+2.42 pp** is not reproduced. Eight
>   pre-registered ABBA/BAAB quads on the same 1,000-row slice give **+0.37 pp** (95% CI
>   **[−0.44, +1.17]**; host bootstrap **[+0.12, +0.64]**; **6/8** positive), and the within-quad
>   same-lane drift reaches **+5.1 pp**. The pre-registered verdict is `inconclusive-at-0.5pp`: a
>   **+2.4 pp** lane cost is excluded, a **+0.5 pp** one is not. See
>   [the quad snapshot](discovery-homepage-lane-rate-quad-2026-10-27.md).
> - **Correction:** an earlier revision of this note read the Workable losses as zero-job or
>   provider-inference outcomes. They are 429s. The artifact's probe-failure rows carry neither
>   `studio` nor `url`, so the report's studio match found nothing and fell through to a *sibling*
>   candidate's zero-job row. `lane_parity.py` now matches a rejection by probe identity and endpoint,
>   and counts it as the host failure it is; see §"The attribution defect".
> - **Then inspect:** `_out/perf-runs/discovery/lane-parity/` — four arm records, `report.json` and
>   the pre-registered `RULE.md`, with the re-run beside it in
>   `_out/perf-runs/discovery/lane-parity-fresh/`, the P6-shaped re-run in `lane-parity-p6shape/`, and
>   the two traced pairs with their `trace.txt` in `lane-trace-smoke/` and `lane-trace-reverse/`, and
>   the 429-free screen study in `lane-parity-429free/`, whose per-host rate read is preserved beside
>   its traces as `rate_analysis.py` / `rate-analysis.txt`. Every `report.json` was regenerated with the
>   corrected attribution and the screen read. The tool that reproduces them is
>   [`tools/measurements/discovery/lane_parity.py`](../../tools/measurements/discovery/lane_parity.py).

## Design

Each arm is one isolated GameDevMap active dry run with `BALUFFO_DATA_DIR` and its own artifact
path, so no arm reads another's cache and repo `data/` is untouched. Population is the first
**3,000** representative rows as three 1,000-row batches — every arm audited the identical set
(`completedUrls` digest `08d24f4435c54bfb`, CSV SHA-256 identical). The only setting that differs
is `activeAuditHomepageFetchConcurrency`; per-host stays **3**, `timeout_s` **6**, and recovery,
probe and browser-recovery policy are the defaults. Order is counterbalanced: pair 1 runs 32 then
64, pair 2 runs 64 then 32.

| Arm | Order | Lanes | Wall s | Homepage fetch ms | Peak in flight | Boards (identities / candidates) | Failures | Homepage failures |
|---|---|---|---|---|---|---|---|---|
| `p1-32` | 1st | 32 | 334.4 | 100,922 | 32 of 32 | 110 / 112 | 4,661 | 341 |
| `p1-64` | 2nd | 64 | **272.7** | **51,529** | 64 of 64 | 106 / 108 | 4,599 | 349 |
| `p2-64` | 1st | 64 | 251.3 | 53,868 | 64 of 64 | 111 / 113 | 4,616 | 335 |
| `p2-32` | 2nd | 32 | 289.8 | 77,377 | 32 of 32 | 109 / 111 | 4,525 | 360 |

The lane knob is proven from the artifact's own receipt, not from the setting: `configuredLanes`
32 / 64 with `peakInFlight` reaching **32** and **64**, mean in flight **24.0-25.9** and **41.5-41.8**.
Homepage fetch gains **1.96×** (pair 1) and **1.44×** (pair 2); whole-run wall **1.23×** and **1.15×**,
consistent with P6's 1.45-1.69× phase and 1.09-1.17× run figures.

## Outcome parity

Boards are compared by **host + tenant** (`core_identity.board_identity_key`), never studio label or
id. The failure measures use the whole-run `failureErrorCounts` and `failureCounts` aggregates; the
artifact's `failures` list is a 200-row sample and understated the homepage change by 7× in pair 1.

| Pair | Boards 32 → 64 | Net | Set diff | Failure identities | Total failures | Homepage failures |
|---|---|---|---|---|---|---|
| 1 | 110 → 106 | **−4** | 8 (6 lost, 2 gained) | 4,497 → 4,467, diff **1,004** | 4,661 → 4,599 (**+62** at 32) | 341 → 349 (**+8** at 64) |
| 2 | 109 → 111 | **+2** | 2 (0 lost, 2 gained) | 4,408 → 4,484, diff **482** | 4,525 → 4,616 (**−91**) | 360 → 335 (**+25** at 32) |

Same-lane churn supplies the floor, and it is not zero:

| Lane | Board set diff | Failure identity diff | Total failure delta | Homepage failure delta | Wall spread |
|---|---|---|---|---|---|
| 32 (`p1-32` vs `p2-32`) | 3 | 977 | 136 | 19 | 44.6 s |
| 64 (`p1-64` vs `p2-64`) | **9** | **611** | 17 | 14 | 21.4 s |

The strict pre-registered rule returns `parity-not-supported` on two failure-count excursions and
not on boards: pair 1's failure-identity difference of **1,004** is 2.8% over the 977 floor, and
pair 2's homepage-failure delta of **25** is 6 counts over the 19 floor — with **32 lanes** holding
the extra failures there, not 64.

## Per-host diff: every differing board is a 429

Each differing board is attributed to its endpoint's record in the arm that lacks it. After the
attribution fix, **23 of 23** differing boards across both studies carry a 429 on that side:

| Study | Pair | Differing | Workable endpoint 429 | Homepage 429 | Probe 429 |
|---|---|---|---|---|---|
| recorded | 1 | 8 | 4 | 4 | 0 |
| recorded | 2 | 2 | 0 | 1 | 1 |
| re-run | 1 | 8 | 4 | 3 | 1 |
| re-run | 2 | 5 | 3 | 1 | 1 |

The Workable column is the whole answer to "why do 1TK and the two Square Enix tenants probe
empty at 64 lanes": they do not. `https://apply.workable.com/api/v1/widget/accounts/<tenant>` was
answering **429 Too Many Requests** to the probe, and the rejection row names the tenant's public
page (`https://apply.workable.com/1tk`, `: Client error '429 …'`). At 32 lanes the same four boards
answer normally — 1TK **1-2** jobs, `square-enix` **5**, `square-enix-america` **1**, `team-17-digital`
**4** — in all four 32-lane arms. The recorded pair 2 shows the other side of it: a **64-lane** arm
kept all four, so the loss is not a property of the setting.

The Workable endpoint's probe failures per arm, in wall-clock order, are **58/110** (13:28),
**121/121** (13:34), **96/116** (13:38), **95/119** (13:43), then **63/117** (16:38), **117/118**
(16:43), **111/119** (16:47), **90/116** (16:51). The two arms where *every* Workable probe failed
are the two pair-1 64-lane arms — each the arm immediately after the first arm of its study. That is
a throttle episode ordered by time, with a clean 64-lane arm inside the same recorded study, not a
lane effect. Probe pressure cannot depend on the lane count either: the probe runs at a fixed
**40** total / **40** provider bucket with **24** fetch concurrency in every arm, the Workable
candidate count is flat (**114-123**), and `runtime.fetchConcurrency` is byte-identical across arms.
What the lane count changes is *when* the probe phase starts, which is how a board lands inside or
outside the host's throttle window.

## Re-run: the same verdict, pair 1 board for board, the same 429 explanation

The tool's `study` sequencing was the one path no study had exercised end-to-end — the arms above were
recorded by the one-off harness and only judged by `report`. Driving `study --pairs 2` at `71bc5e11`
took one unattended command and about 18 minutes: four arms in the pre-registered counterbalanced order
(the log's headers are `p1-32` 16:38, `p1-64` 16:43, `p2-64` 16:47, `p2-32` 16:51), each passing its
own lane proof — `configuredLanes` 32 or 64 with `peakInFlight` reaching the same number — before its
record was written. The population is the one above: digest `08d24f4435c54bfb`, 10,500 CSV rows, 8,370
eligible, the same 3,000 pinned, in **all eight arms across both studies**.

| Study | Pair | Boards 32 → 64 | Net | Set diff (lost / gained) | Wall s | 64-lane homepage | Phase gain |
|---|---|---|---|---|---|---|---|
| recorded | 1 | 110 → 106 | −4 | 8 (6 / 2) | 334.4 → 272.7 | 17.2 ms/row | 1.96× |
| recorded | 2 | 109 → 111 | +2 | 2 (0 / 2) | 289.8 → 251.3 | 18.0 ms/row | 1.44× |
| re-run | 1 | 111 → 107 | −4 | 8 (6 / 2) | 273.5 → 261.9 | 16.7 ms/row | 1.72× |
| re-run | 2 | 112 → 109 | −3 | 5 (4 / 1) | 261.1 → 242.6 | 17.4 ms/row | 1.40× |

Pair 1 came back **board for board identical**: the same six boards are 32-only (the two Square Enix
tenants, 1TK, Everplay, 2Awesome and Finitude) and the same two are 64-only (Fakefish and Fun Waffle).
That is reproducibility of *which hosts were throttled in which arm*, not of a lane effect: all eight
are 429s on the arm that lacks them, and the two "gains" are 429s on the 32-lane side.

Pair 2 did not reproduce: the first study gained Hardsuit Labs and Draconic at 64 lanes, the re-run
lost 1TK, the Square Enix pair and Draconic and gained only Fun Waffle. Both of the first study's
gains are also 429s on the 32-lane side, and Draconic's loss in the re-run is a 429 on its own probe.
Pooled, the re-run is **223 → 216** and the first study **219 → 217**, but neither pool is a board
set that lanes decided; they are the sum of which endpoints were throttled when.

Timing reproduces on the phase and not on the wall: the 64-lane homepage fetch costs **16.7** and
**17.4 ms/row** against **17.2** and **18.0**, so the phase gain is a property of the setting;
whole-run wall speedup is only **1.04×** and **1.08×** against **1.23×** and **1.15×**, because
recovery and 429 retries own most of the remainder and they are the weather.

## P6-shaped re-run: three pairs, every mover a 429

P6's six pairs cannot be re-read (their arms are absent), so the next check is P6's leg-A *shape*:
three counterbalanced pairs through the promoted tool (`study --pairs 3`) on the pinned 3,000 rows
(digest `08d24f4435c54bfb`, identical CSV), 2026-10-26T19:27:18-19:54Z, in
`_out/perf-runs/discovery/lane-parity-p6shape/`. Each arm proves its lane count from its own receipt
(**32/32** or **64/64** configured/peak).

| Pair | Order | Boards 32 → 64 | Net | Changed (lost / gained) | Wall s | Homepage fetch ms | Movers that are 429s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 32, 64 | 115 → 113 | −2 | 4 (3 / 1) | 297.3 → 242.0 | 91,111 → 50,213 | 4 / 4 |
| 2 | 64, 32 | 113 → 112 | −1 | 3 (2 / 1) | 330.2 → 235.8 | 72,170 → 51,950 | 3 / 3 |
| 3 | 32, 64 | 113 → 109 | −4 | 6 (5 / 1) | 284.7 → 253.9 | 75,307 → 53,437 | 6 / 6 |

The moved boards are **6 homepage**, **2 probe** and **5 recovery** 429s. The seven probe/recovery
movers matter: probe runs at a fixed **40** lanes in every arm and recovery policy is identical, so
those board losses cannot be caused by the homepage lane setting. Pair 3's net **−4** comes from
recovery — five `apply.workable.com` tenants 429'd on the 64 side (1TK, Hardsuit Labs, both Square
Enix tenants, Team17 as Everplay) against one homepage board 64 kept (Fun Waffle); pair 2's net
**−1** is one homepage loss with its two probe moves cancelling; pair 1's net **−2** is homepage. The
direction — 64 net behind in **3/3** pairs, **−7** pooled — therefore reproduces P6's signature
without isolating a lane effect.

The pre-registered rule returns **`parity-not-supported`**: p1's failure-identity difference
**761 > 488**, its total-failure delta **200 > 51** and homepage delta **99 > 51**; p2's total delta
**146 > 51**; p3's board symmetric difference **6 > 4** and total delta **58 > 51**. The board-only
reading is throttle weather again; the failure measures are mostly the same 429s under another name.

Limits: the arms run sequentially at different times, so this cannot separate "64 lanes raise the 429
rate" from "these arms landed in throttle episodes"; P6's own six stay unreadable; and three pairs
against a four-board floor is a small sample. It is not the production-sized run R4 asks for. The
request trace below does separate rate from episode, and finds the unpaired lane gap unstable.

## Request trace: a spread rate, not a throttle episode

The parity report names the arm that lost a board but says nothing about whether its 429s were one
host episode or a higher rate across the phase. Each arm now records `request-trace.jsonl` (phase,
host, status, start/end per attempt), and `lane_parity trace` reports per phase: requests, 429s,
hosts asked, **fully** (every request 429) versus **partially** throttled hosts, and the 429 window
as a share of the phase with its ten time bins. Two traced pairs ran in opposite order on the same
pinned population and CSV (`08d24f4435c54bfb`):

| Pair (order) | Requests 32 / 64 | Homepage 429 32 / 64 | Fully throttled 32 / 64 | 429 window 32 / 64 |
| --- | --- | --- | --- | --- |
| forward (32, 64) | 11,445 / 11,147 | 65 / 3,000 (**2.2%**) / 163 / 3,000 (**5.4%**) | 65 / 163 | 93% of span, 5/10 bins / 97%, 3/10 |
| reverse (64, 32) | 11,273 / 11,263 | 126 / 3,000 (**4.2%**) / 135 / 3,000 (**4.5%**) | 126 / 135 | 97%, 6/10 / 97%, 4/10 |

The homepage phase asks each of its ~2,990 hosts once, so "fully throttled" is the 429 count; the
429s are spread over 65 vs 163 distinct hosts in the forward pair and 126 vs 135 in the reverse.
Across both pairs every phase's 429s occupy **93-100%** of the phase's span and **3-6 of ten bins**
-- no phase is a single throttle episode. The other phases did not follow the lane count:

| Phase | Forward 32 → 64 | Reverse 32 → 64 |
| --- | --- | --- |
| homepage | 2.2% → 5.4% | 4.2% → 4.5% |
| recovery | 12.5% → 9.8% | 12.9% → 12.3% |
| probe | 7.6% → 7.7% | 8.1% → 8.2% |

`apply.workable.com` is 98.7-100% throttled in the probe phase of both arms in both pairs, the
host-level throttle the earlier attribution found, and it moves in neither direction with lanes.
The forward pair's 3.2 pp homepage gap does not reproduce: the **32-lane arm itself** moves from
2.2% to 4.2% between the two runs, a 2.0 pp swing, while the reverse pair's 64 − 32 gap is 0.3 pp.
Both arms run higher when they run second (32: 2.2% → 4.2%; 64: 4.5% → 5.4%), so order and time
slice are candidates for part of the swing — each arm moves **0.9-2.0 pp** between its two runs
against lane gaps of **3.2 pp** (forward) and **0.3 pp** (reverse). Two pairs cannot separate order
from lane either; what they show is that the gap is not stable enough to price.
Pooled the 32-lane arms are 191 / 6,000 (3.2%) and the 64-lane arms 298 / 6,000 (5.0%), but that
pools two runs of each arm at different times; it does not price a lane. The board result is
unchanged -- every mover is still a 429 -- and the trace shows why the board measure cannot resolve
it: the 429 rate of the *same* arm moves more between runs than the lane gap it would have to explain.

Evidence: `_out/perf-runs/discovery/lane-trace-smoke/` (20:08:50-20:17Z) and
`lane-trace-reverse/` (20:24:59-20:33Z), 2026-10-26, each arm proving its lane count from its own
receipt; reports are `trace.txt` beside each study. Re-run with:

```bash
python tools/measurements/discovery/lane_parity.py trace \
    --study _out/perf-runs/discovery/lane-trace-smoke --pairs p1:p1-32,p1-64
python tools/measurements/discovery/lane_parity.py trace \
    --study _out/perf-runs/discovery/lane-trace-reverse --pairs p1:r1-32,r1-64
```

## 429-free screen: no board moves where hosts do not throttle

The raw comparison cannot separate a lane effect from host throttling at this population: a moved
board can always be blamed on a 429, and the 429 on the lanes. The screen is the pre-registered third
read, shipped as `report --screen-429-free` (the same flag on `study`): it collects every host that
answered **429** in *either* arm's request trace, drops every board touching one — host match after
stripping `www.`, a subdomain linked to its parent in either direction, so it over-excludes rather
than under-excludes — and judges the survivors as a second read beside the raw one. A difference that
survives is not a 429, and a screen with no clean boards returns `429-free-population-empty` rather
than claiming parity. Four tests pin the exclusion, the difference that must not be excused,
`www`/subdomain linkage, and the empty-population verdict.

Four traced counterbalanced pairs now carry it: the two earlier traced pairs and a fresh study in
`_out/perf-runs/discovery/lane-parity-429free/` (2026-10-26T22:10-22:30Z, four arms in the
counterbalanced order `p1-32`, `p1-64`, `p2-64`, `p2-32`, each proving its own lanes from its own
receipt; population digest `08d24f4435c54bfb` and CSV `896da851…` in all four arms, so it is the
population above).

| Pair (order) | Raw boards 32 → 64 | Movers | Movers that are 429s | Throttled hosts | Excluded 32 / 64 | Clean boards | Clean symdiff | Screen verdict |
|---|---|---|---|---|---|---|---|---|
| smoke (32, 64) | 110 → 108 | 4 | 4 / 4 | 262 | 3 / 1 | 107 vs 107 | **0** | supported |
| reverse (64, 32) | 107 → 105 | 2 | 2 / 2 | 267 | 2 / 0 | 105 vs 105 | **0** | supported |
| fresh p1 (32, 64) | 109 → 106 | 3 | 3 / 3 | 277 | 3 / 0 | 106 vs 106 | **0** | supported |
| fresh p2 (64, 32) | 103 → 105 | 2 | 2 / 2 | 343 | 0 / 2 | 103 vs 103 | **0** | supported |

Every raw mover across the four pairs — **11 of 11** — carries its 429 on the arm that lacked the
board, and after the screen **no board difference remains in any pair**. The screen excluded exactly
the movers: no clean board was dropped and no move survived. The fresh study's raw rule also returns
`parity-supported` (no violations; same-lane churn **6** boards at 32 and **1** at 64) and its verdict
is `parity-supported-on-429-free-hosts`; the two single-pair trace studies return `inconclusive` raw
(floor undefined) and `parity-supported-on-429-free-hosts` screened. Timing reproduces: the fresh
pairs gain **91,355 → 51,914 ms** (1.76×) and **77,474 → 46,115 ms** (1.68×) on the homepage phase at
**1.22×** and **1.20×** whole-run wall.

What this does **not** settle:

- **Power.** The clean sets are **~100 boards per arm**; an effect below one board is invisible, and
  the excluded hosts are, by construction, the ones that throttled.
- **P6's six pairs** remain unreadable, and this is one population on one day. The board question is
  settled only in the sense the screen can settle it: no difference survives where throttling is
  absent.

Re-run with:

```bash
python tools/measurements/discovery/lane_parity.py report \
    --study _out/perf-runs/discovery/lane-parity-429free \
    --pairs p1:p1-32,p1-64 p2:p2-32,p2-64 --screen-429-free
```

## Per-host rate: 64 lanes raise no host's own 429 rate

The screen's open question was causal: does the wider arm provoke the throttling it is then blamed
for? The fresh study's four arms answer it, because its pair 1 ran 32 then 64 and its pair 2 ran 64
then 32. The homepage phase — the one `activeAuditHomepageFetchConcurrency` drives — is a clean
paired sample: all **2,990** hosts appear in all four arms with an **identical URL set** and an
**identical request count** (3,000 requests per arm). The probe phase has **541** hosts present in
all four arms, including the four that carry about **80%** of its requests.

| Arm (run position) | Homepage 429 / requests | Rate | 64 − 32 |
|---|---:|---:|---:|
| `p1-32` (1st) | 65 / 3,000 | 2.17% | — |
| `p1-64` (2nd) | 160 / 3,000 | 5.33% | **+3.17 pp** |
| `p2-64` (1st) | 127 / 3,000 | 4.23% | — |
| `p2-32` (2nd) | 244 / 3,000 | 8.13% | **−3.90 pp** |

The lane delta changes sign with position, so averaging the two pairs cancels the position effect:
**−0.37 pp**, cluster-bootstrap 95% CI **[−0.94, +0.10]** over the 2,990 hosts, one-sided upper bound
**+0.10 pp** — at most about **3 extra 429s per 3,000** homepage requests. Per host the order-cancelled
deltas are **+110 / −137 / 2,743 zero** (two-sided sign test **p = 0.098**), and **43** hosts answer
429 in all four arms; excluding those hard blocks the arms are **0.74 / 3.96 / 2.84 / 6.80%**, the
same position pattern. The probe phase is exactly flat, in the same arm order as the homepage table:

| Host | Requests per arm | 429s per arm | Rate |
|---|---:|---:|---:|
| `boards-api.greenhouse.io` | 1,092 / 1,091 / 1,091 / 1,089 | 0 / 0 / 0 / 0 | 0% |
| `api.smartrecruiters.com` | 599 / 599 / 599 / 599 | 0 / 0 / 0 / 0 | 0% |
| `api.lever.co` | 607 / 624 / 682 / 687 | 0 / 0 / 0 / 0 | 0% |
| `apply.workable.com` | 228 / 230 / 232 / 234 | 228 / 230 / 232 / 234 | 100% |

On the 541 common probe hosts the rates are **7.46 / 7.48 / 7.41 / 7.46%** in that order; order-cancelled
**−0.01 pp** [−0.09, +0.01], and **0 of 541** hosts changed rate. The 56 probe hosts outside the
common set carry **20 / 19 / 21 / 5** 429s but are asked in only some arms, so they cannot enter the
four-arm paired read; no host-level sample has the wider arm throttling more.

The recovery phase cannot answer the question and its aggregate rate is not one. Its 429s are
**616 / 479 / 597 / 215 off the common host set** — mostly hosts that answer 6/6 whenever asked — so
the **13.15 / 11.10 / 13.06 / 6.80%** phase totals measure which hard-blocked hosts each arm asked,
not a rate. On the 1,982 hosts present in all four arms the counts are **79/4,452, 79/4,452,
79/4,464, 113/4,552**; the +34 in `p2-32` is request depth: 26 hosts are asked 6 pages there against
2 in the other arms (2 429s each, +52), offset by 3 hosts asked 6 elsewhere and 2 there (−18), and
the deeper arm is the **32**-lane one.

Why the null is expected: `perHostConcurrency` is pinned at **3** in every arm, and a homepage host
has one URL, so the wider arm keeps more requests in flight across *different* hosts, not more
against one host. The only measured driver is run position — the second arm of either pair is about
**1.9-2.5×** likelier to see a host 429 — which is a time/warm-state effect, not a lane effect.

Limits: one population on one day, one pair per order, and the trace records a successful page fetch
as 200 after `raise_for_status` and a 429 exactly. The analysis is preserved beside the traces
(`rate_analysis.py`, `rate-analysis.txt`) and re-derives every number above from the four
`request-trace.jsonl` files on 2026-10-27; no new live traffic was sent.

**Quad re-read (2026-10-27).** Four arms leave one degree of freedom for the lane effect and none
for the position term; the **+3.17 / −3.90 pp** split above is the position term showing itself. An
eight-quad ABBA/BAAB study on the same population then estimated both at once: **+0.37 pp**
**[−0.44, +1.17]** for lanes, with same-lane drift up to **+5.1 pp**. That brackets this section's
**−0.37 pp** and excludes the acceptance pass's **+2.42 pp**, so the rate question is no longer a
reason to keep 32; the page/board gate remains the open check. See
[the quad snapshot](discovery-homepage-lane-rate-quad-2026-10-27.md).

## The attribution defect

`rejectedForActivation` rows for probe failures are built from the candidate (`src/source_discovery/
gamedevmap_rejection.py::_rejection`), which sets `sourceId`, `name`, `adapter` and the whole
`candidate` object — but leaves `studio` and `url` empty. `lane_parity._attribute` matched a rejection
by `studio == board.studio`, found none, and fell through to the zero-job list, which it matched by
**studio label**. A studio with several provider candidates then lends the missing board a sibling's
zero: 1TK's lost Workable board was explained by its SmartRecruiters row, and the two Square Enix
tenants share both a studio label *and* a board name, so either could stand in for the other. That is
where the "zero-job probe" and "provider-inference" readings in the earlier revision came from.

The fix, in `tools/measurements/discovery/lane_parity.py`:

- boards carry `sourceId` and the endpoint (`api_url` / `board_url`), and `_hosts_for` includes the
  endpoint host — a Workable board's careers URL is `http://eu.square-enix.com/jobs` or empty, which
  is why `apply.workable.com` never appeared as its host;
- rejections carry `sourceId`, `name`, `adapter` and their endpoint, and are matched by probe identity
  first, endpoint second;
- a zero-job row is matched by identity when both sides have one, and only by name otherwise — never
  by studio alone, so two tenants of one studio cannot borrow each other's result;
- a matched rejection with no `failureErrorCounts` hit counts as the host failure it is, because probe
  429s are not in that aggregate.

Two tests pin it: a 429 rejection beats a sibling's zero-job row, and two Square Enix tenants keep
their own zero. Regenerating both `report.json` files is what turned 23 attributed boards into 23
429s and moved `failedOnHost` from "no host failure" to the tenant's own 429.

## What this settles, and what it does not

- **R4's "price" reading is withdrawn.** The earlier revision read three losses and one gain as a
  board cost of about **2%** per pair, supported by **9 losses in 10 pairs** (p ≈ 0.021) once P6's six
  were pooled. Every board in that count that changed sides on this population is a 429 — 23 of 23
  after the fix — so the sign test counts throttle luck and arm order, not the lane setting. A 64-lane
  arm has also run clean (recorded pair 2), which no consistent lane cost would allow. The honest
  statement is that **no lane-driven board cost has been measured**, and that the board measure
  cannot separate a lane effect from host throttling at this population size. The 429-free screen
  (§"429-free screen") now does separate them: across four traced pairs **no board changes where no
  host throttled**, so R4's board question is settled on this population; the rate question is
  measured in §"Per-host rate" and no host-level increase survives **on that study**. The separate
  2026-10-27 acceptance pass measured **+2.42 pp** and failed its gate, so the config change is not
  supported ([acceptance snapshot](discovery-64-lane-acceptance-2026-10-27.md)).
- **The Workable core is host throttling, not a probe or inference difference.** The three boards are
  rejected with `429 Too Many Requests` from `apply.workable.com`; their pages read in both arms and
  their jobs are present whenever the probe is. The full-block arms are the two pair-1 64-lane arms,
  each right after the first arm of its study, and the recorded pair-2 64-lane arm is clean.
- **Failure-identity parity is not proven, only bounded.** Identity churn runs **14-22%** of the
  aggregate even between same-lane arms, and the same 429s that move boards also move these identities,
  so the measure is dominated by which URL returned which error on the day. Its two excursions are
  recorded above rather than rounded away.
- **The registry was empty.** This checkout has no `data/source-registry-active.json`, so every board
  found is new; the comparison is arm-to-arm on the same population, not against the populated registry
  P6 measured. Board counts land at **106-113** against P6's **114-117**, which is the closest check
  available that the arms are the same measurement.
- **P6's six pairs cannot be checked here.** They stand as recorded, on a different tree and population,
  and their arms are not in this checkout: on 2026-10-26 `_out/perf-runs/discovery/` held only the
  2026-10-26 lane-parity runs, so `p6-lane-study-20261017/` does not resolve and no per-board identity
  survives — the record keeps counts only (1-6 losses per pair, 6/6, p = 0.0312). If those losses were
  also 429s the 9-in-10 count disappears entirely, and this population cannot settle it. The P6 shape
  was re-run instead (above): it reproduces the direction with every mover a 429, but not P6's
  identities. R4's board and rate questions are both measured on this population: the screen settles
  the first where throttling is absent, and the per-host read settles the second with the hosts held
  fixed (§"Per-host rate"). What remains open is P6's six loss identities, whose arms are still absent.

## Acceptance pass (2026-10-27)

The plan's step 5 gate was run on `f60de72e` after this study: two counterbalanced pairs over the same
3,000 rows. It **fails**. Pair 2's homepage gain (**19.7 s**) is inside the 32-lane spread (**22.1 s**)
and its whole-run gain is **3.1 s** against **51.8 s**; 64 fetched **96** and **34** fewer pages as
homepage 429s rose **105** and **40**; and the per-host, order-cancelled homepage rate is **+2.42 pp**
(95% CI **[+1.80, +2.98]**, **+127/−35** hosts). The 2026-10-26 board and rate reads above remain the
evidence for that study; the 64-lane config change is not supported and 32 stays. Detail, arm records
and traces: [acceptance snapshot](discovery-64-lane-acceptance-2026-10-27.md).

## Reproducing the numbers

```bash
# the whole measurement: two counterbalanced pairs over the pinned first 3,000 rows, then a verdict
python tools/measurements/discovery/lane_parity.py study \
    --study _out/perf-runs/discovery/lane-parity --pairs 2

# or judge arms already recorded (the parity table, per-host attribution and floor)
python tools/measurements/discovery/lane_parity.py report \
    --study _out/perf-runs/discovery/lane-parity \
    --pairs p1:p1-32,p1-64 p2:p2-32,p2-64
```

The per-host rate read (§"Per-host rate") is an analysis over the same traces, preserved beside them
because it is not part of the shipped report:

```bash
python _out/perf-runs/discovery/lane-parity-429free/rate_analysis.py \
    _out/perf-runs/discovery/lane-parity-429free
```

A re-run needs a fresh `--study` directory: an arm record is immutable, and `--force` is reserved for
re-measuring a whole study. The reproduction above used
`--study _out/perf-runs/discovery/lane-parity-fresh`; the arms, their logs and `report.json` stay under
their study directory, both of them untracked evidence. Add `--screen-429-free` to either command to
judge the pairs over only the boards whose hosts never answered 429 (its own re-run is in the screen
section above). The tool is tracked in `tools/` on purpose:
`tools/**` is not shipped
container code, so it cannot trip the version/release gate, and this is measurement rather than
product behaviour.

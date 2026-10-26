# 64 lanes has no measured board cost — the board diff was 429 weather — 2026-10-26

> - **Class:** measurement — answers residual **R4** of the P6 closeout; no shipped behaviour changes.
> - **Trigger:** R4 recorded outcome parity as unproven from six pairs in which the 64-lane arm lost
>   boards **6/6** (two-sided sign test **p = 0.0312**). Its own closing condition was two more
>   counterbalanced pairs, or one production-sized 64-lane run with a per-host board diff.
> - **Status:** **no lane-driven board difference is measured.** Four counterbalanced pairs across two
>   same-day studies differ on **23 boards**, and **all 23** carry a **429** on the arm that lacks them:
>   12 Workable boards (11 throttled on `apply.workable.com`, one on its own homepage), plus **9**
>   homepage and **3** probe 429s on other hosts. A 64-lane arm also ran **clean** — the recorded pair 2 kept all four Workable boards
>   while a 32-lane arm lost them — so the lane count is neither necessary nor sufficient for the loss.
>   Timing reproduces: the 64-lane homepage phase is **1.4-2.0×** faster; whole-run wall is **1.04-1.23×**.
> - **Verified against:** `2f85d3db` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), 2026-10-26T13:28–13:50Z.
> - **Re-run:** the whole measurement again through `study --pairs 2` at `71bc5e11`,
>   2026-10-26T16:38:49Z-16:56Z, evidence in `_out/perf-runs/discovery/lane-parity-fresh/`. Population
>   digest `08d24f4435c54bfb` in all four arms — bit-identical to the four recorded here.
> - **Correction:** an earlier revision of this note read the Workable losses as zero-job or
>   provider-inference outcomes. They are 429s. The artifact's probe-failure rows carry neither
>   `studio` nor `url`, so the report's studio match found nothing and fell through to a *sibling*
>   candidate's zero-job row. `lane_parity.py` now matches a rejection by probe identity and endpoint,
>   and counts it as the host failure it is; see §"The attribution defect".
> - **Then inspect:** `_out/perf-runs/discovery/lane-parity/` — four arm records, `report.json` and
>   the pre-registered `RULE.md`, with the re-run beside it in
>   `_out/perf-runs/discovery/lane-parity-fresh/`. Both `report.json` files were regenerated with the
>   corrected attribution. The tool that reproduces them is
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
  cannot separate a lane effect from host throttling at this population size.
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
- **Not re-run: P6's six pairs.** They stand as recorded, on a different tree and population. If their
  six losses were also 429s, the 9-in-10 count disappears entirely; nothing here can settle that,
  because the P6 artifacts do not carry the rejection detail this attribution needs. R4 stays open
  rather than closed on the strength of this population.

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

A re-run needs a fresh `--study` directory: an arm record is immutable, and `--force` is reserved for
re-measuring a whole study. The reproduction above used
`--study _out/perf-runs/discovery/lane-parity-fresh`; the arms, their logs and `report.json` stay under
their study directory, both of them untracked evidence. The tool is tracked in `tools/` on purpose:
`tools/**` is not shipped
container code, so it cannot trip the version/release gate, and this is measurement rather than
product behaviour.

# 64 lanes costs a few boards, and the cost repeats — 2026-10-26

> - **Class:** measurement — answers residual **R4** of the P6 closeout; no shipped behaviour changes.
> - **Trigger:** R4 recorded outcome parity as unproven from six pairs in which the 64-lane arm lost
>   boards **6/6** (two-sided sign test **p = 0.0312**). Its own closing condition was two more
>   counterbalanced pairs, or one production-sized 64-lane run with a per-host board diff.
> - **Status:** **not free: three of the four pairs measured on this population lose boards at 64
>   lanes.** Pair 1 loses **4** both times — the same six boards lost and the same two gained — while
>   pair 2 gains **2** in the first study and loses **3** in the second. Pooled **442 → 433** across
>   four pairs and 12,000 row-audits (net **−9**, about **2%** of the boards an arm finds per pair),
>   and **9 of 10 pairs** counting P6's six. Both studies return `parity-not-supported`; the re-run is
>   the first to trip the board measure itself.
> - **Verified against:** `2f85d3db` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), 2026-10-26T13:28–13:50Z.
> - **Re-run:** the whole measurement again through `study --pairs 2` at `71bc5e11`,
>   2026-10-26T16:38:49Z-16:56Z, evidence in `_out/perf-runs/discovery/lane-parity-fresh/`. Population
>   digest `08d24f4435c54bfb` in all four arms — bit-identical to the four recorded here.
> - **Then inspect:** `_out/perf-runs/discovery/lane-parity/` — four arm records, `report.json` and
>   the pre-registered `RULE.md`, with the re-run beside it in
>   `_out/perf-runs/discovery/lane-parity-fresh/`. The tool that reproduces both is
>   [`tools/measurements/discovery/lane_parity.py`](../../tools/measurements/discovery/lane_parity.py),
>   promoted from the one-off harness after the study, re-run over these four records to confirm it
>   writes the figures below, then driven end-to-end for the re-run.

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

So the **9-board** floor exceeds pair 1's 8 and pair 2's 2, the direction reverses between these two
pairs — which is what the re-run below does not reproduce — and the 64-lane arms are the *steadier*
pair run-to-run (611 failure identities against 977, 17 total
failures against 136). The strict pre-registered rule still returns `parity-not-supported` here, on
two failure-count excursions and not on boards: pair 1's failure-identity difference of **1,004** is
2.8% over the 977 floor, and pair 2's homepage-failure delta of **25** is 6 counts over the 19
floor — with **32 lanes** holding the extra failures there, not 64.

## Per-host diff

Ten boards differ across these two pairs. Each is attributed to its host's record in the arm that
lacks it:

| Board | Present in | Other arm's record for that host |
|---|---|---|
| 2Awesome Studio | 32 only | `homepage_fetch` **429** at 64 |
| Finitude | 32 only | `homepage_fetch` **429** at 64 |
| 1TK (Workable) | 32 only | read fine at 64; probe found **0 jobs** |
| Beautiful Game Studios (Square-Enix Europe) ×2 boards | 32 only | no host failure at 64; provider inference differs |
| Everplay Group (Workable) | 32 only | no host failure at 64; provider inference differs |
| Fakefish | 64 only | `homepage_fetch` **429** at 32 |
| Fun Waffle Game Company | 64 only | `homepage_fetch` **429** at 32 |
| Hardsuit Labs (Workable) | 64 only | `homepage_fetch` **429** at 32 |
| Draconic Interactive | 64 only | no host failure at 32 |

Host-level 429s appear in the other arm for six of the ten boards, split 3 against each side, all at
the same per-host limit of 3. One of those is 1TK, where the hit is a recovery fetch on a different
Workable tenant while its own listing read fine and the probe found 0 jobs at 64 — a host signal, not
the board's own failure. The other four have no failure at all in the other arm: the Square Enix pair,
Everplay and Draconic differ as provider-inference outcomes on pages that read in both arms, the same
page-level variance the P6 closeout traced on the post-fix tree.

## Re-run: the same verdict, pair 1 board for board, a cost that does not alternate

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

Pair 1 came back **board for board identical**. The same six boards are 32-only — the two Square Enix
tenants, 1TK, Everplay, 2Awesome and Finitude — and the same two are 64-only, Fakefish and Fun Waffle,
with the same net **−4** and the same set diff of **8**. That is the strongest reproducibility available
here: not the same count, the same boards.

Pair 2 did not. The first study gained Hardsuit Labs and Draconic at 64 lanes (109 → 111, none lost);
the re-run lost 1TK, the Square Enix pair and Draconic and gained only Fun Waffle (112 → 109).
**Draconic gains at 64 lanes in one study and loses at 64 lanes in the next, three hours apart on the
same page.** Pooled, the re-run is one-way — **223 → 216**, net **−7** over 6,000 row-audits — where
the first study was net **−2**, and its floor is tighter: **5 / 626 / 38 / 29** boards / failure
identities / total failures / homepage failures against **9 / 977 / 136 / 19**.

The verdict is unchanged: `parity-not-supported`, here on pair 1 (set diff 8 against the 5-board floor,
failure identities 696 against 626, total failures 81 against 38). The 429 column is unchanged too: the
two static boards that died on 429 at 64 lanes in the first study, 2Awesome and Finitude, died the same
way again. What is new is that the board count now carries the cost in three of the four pairs, where
the first study's carried it in one of two: the differing set stays small — 2 to 8 boards of about
110 — but its direction does not alternate with the arm order.

Timing reproduces on the phase and not on the wall: the 64-lane homepage fetch costs **16.7** and
**17.4 ms/row** against **17.2** and **18.0**, so the phase gain is a property of the setting; whole-run
wall speedup is only **1.04×** and **1.08×** against **1.23×** and **1.15×**, because recovery and 429
retries own most of the remainder and they are the weather.

## What this settles, and what it does not

- **R4's answer is a price, not a verdict.** 64 lanes finds a few fewer boards and the direction
  repeats: three losses and one gain across the four pairs on this population, **9 losses in 10 pairs**
  counting P6's six (two-sided sign test **p ≈ 0.021**), net **−9** boards here. P6's six ran on a
  different tree and population, so that pooled count is a weak test, and four pairs alone decide
  nothing — but the reading that fits all of it is a small one-way board cost of about **2%** per pair,
  not the flat result the first study reported. That makes the lane count a throughput-for-coverage
  trade, and the **1.4-2.0×** homepage gain is what pays for it.
- **The repeat losses are probe outcomes, not fetch failures.** Across all four pairs the 429
  attributions split **6 against 64 lanes and 5 against 32**, so throttling does not explain the
  direction. What repeats is a small Workable core: 1TK and the two Square Enix tenants are 32-only in
  **three of the four pairs**, and their loss is a probe outcome rather than a fetch failure — 1TK is a
  zero-job probe whenever it is lost, the Square Enix pair differ as provider-inference or zero-job
  outcomes, and Everplay has no failure recorded at all. The two static boards lost in both pair 1s
  are 429s at 64, and Draconic is the one board that changes sides. That probe path, not another pair,
  is where the next measurement belongs.
- **Failure-identity parity is not proven, only bounded.** Identity churn runs **14-22%** of the
  aggregate even between same-lane arms, so that measure is dominated by which URL returned which error
  on the day. Its two excursions are recorded above rather than rounded away.
- **The registry was empty.** This checkout has no `data/source-registry-active.json`, so every board
  found is new; the comparison is arm-to-arm on the same population, not against the populated registry
  P6 measured. Board counts land at **106-113** against P6's **114-117**, which is the closest check
  available that the arms are the same measurement.
- **Not re-run: P6's six pairs.** They stand as recorded. These are four fresh pairs under the same
  population rule, from two studies, not a reanalysis.

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

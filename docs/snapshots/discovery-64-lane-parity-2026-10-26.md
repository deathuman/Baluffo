# 64-lane board parity holds: the loss was weather, not lanes — 2026-10-26

> - **Class:** measurement — answers residual **R4** of the P6 closeout; no shipped behaviour changes.
> - **Trigger:** R4 recorded outcome parity as unproven from six pairs in which the 64-lane arm lost
>   boards **6/6** (two-sided sign test **p = 0.0312**). Its own closing condition was two more
>   counterbalanced pairs, or one production-sized 64-lane run with a per-host board diff.
> - **Status:** **settled for boards.** Two fresh counterbalanced pairs over the same 3,000
>   representative rows: 64 lanes loses **4** net boards in pair 1 and gains **2** in pair 2
>   (pooled **219 → 217** across 6,000 row-audits), both diffs inside the **9-board** same-lane churn
>   floor. **5 of the 10** differing boards are 429s on that host, and they split **2 against 64,
>   3 against 32**. The one-way P6 pattern did not reproduce.
> - **Verified against:** `2f85d3db` (`main`), live GameDevMap CSV `896da851…` (1,884,186 chars,
>   10,500 rows, 8,370 eligible), 2026-10-26T13:28–13:50Z.
> - **Then inspect:** `_out/perf-runs/discovery/lane-parity/` — four arm records, `report.json` and
>   the pre-registered `RULE.md`; the harness is `_out/perf-runs/discovery/lane_parity_study.py`
>   with `lane_parity_report.py` beside it.

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

So the **9-board** floor exceeds pair 1's 8 and pair 2's 2, the direction reverses between pairs,
and the 64-lane arms are the *steadier* pair run-to-run (611 failure identities against 977, 17 total
failures against 136). The strict pre-registered rule still returns `parity-not-supported`, on two
failure-count excursions and not on boards: pair 1's failure-identity difference of **1,004** is
2.8% over the 977 floor, and pair 2's homepage-failure delta of **25** is 6 counts over the 19
floor — with **32 lanes** holding the extra failures there, not 64.

## Per-host diff

Ten boards differ across the two pairs. Each is attributed to its host's record in the arm that
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

Five boards are 429 casualties, split 2 against the 64-lane arm and 3 against the 32-lane arm, all at
the same per-host limit of 3. One is a zero-job probe at 64. Four are inference outcomes on pages
that read in both arms — the Square Enix pair, Everplay and Draconic — which is the same
page-level variance the P6 closeout traced on the post-fix tree, not throttling.

## What this settles, and what it does not

- **R4's board claim is settled.** The 6/6 direction was the small-n expression of a difference whose
  sign flips between pairs and whose size is under the same-lane floor. Across six P6 pairs plus these
  two, the pooled rate is 7 losses in 8 pairs, and a two-sided sign test cannot reach significance at
  that count — which is the point: the criterion needed eight pairs and a mechanism, and the mechanism
  is 429 weather on both arms.
- **Failure-identity parity is not proven, only bounded.** Identity churn runs **14-22%** of the
  aggregate even between same-lane arms, so that measure is dominated by which URL returned which error
  on the day. Its two excursions are recorded above rather than rounded away.
- **The registry was empty.** This checkout has no `data/source-registry-active.json`, so every board
  found is new; the comparison is arm-to-arm on the same population, not against the populated registry
  P6 measured. Board counts land at **106-113** against P6's **114-117**, which is the closest check
  available that the arms are the same measurement.
- **Not re-run: P6's six pairs.** They stand as recorded. These are two fresh pairs under the same
  population rule, not a reanalysis.

## Reproducing the numbers

```bash
# one arm over the pinned first 3,000 rows (isolated data dir + artifact)
python _out/perf-runs/discovery/lane_parity_study.py arm \
    --study _out/perf-runs/discovery/lane-parity --name p1-64 --lanes 64 --batches 3

# the parity table, the per-host attribution and the floor
python _out/perf-runs/discovery/lane_parity_report.py \
    --study _out/perf-runs/discovery/lane-parity \
    --pairs p1:p1-32,p1-64 p2:p2-32,p2-64
```

The arms, their logs and `report.json` stay under `_out/perf-runs/discovery/lane-parity/`
(untracked). The harnesses live there on purpose: a `scripts/` edit counts as shipped container code
and trips the version/release gate, and this is measurement, not product behaviour.

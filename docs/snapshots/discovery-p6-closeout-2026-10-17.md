# The P6 baseline, lane study and Browser proof now exist — 2026-10-17

> - **Class:** measurement and evidence closeout — one finding first recorded as a shipped-surface defect was later explained as expected behaviour (R3)
> - **Trigger:** the discovery performance plan's three operator gates (baseline, 32/64 lane study, Browser
>   proof) had never had qualifying evidence. The target recovered from its 504 window, so all three were
>   executed locally on one revision and independently checked.
> - **Status:** **local evidence present, production acceptance open.** Six gates exit 0 (three builders,
>   three independent checkers). The 64-lane timing case is strong; **outcome parity stays open only for
>   the rate question** — four counterbalanced pairs across two studies changed boards on 23 occasions
>   and every one is a host 429 on the arm that lacked the board, so no lane-driven board cost is
>   measured and the earlier ~2% "price" is withdrawn; a pre-registered **429-free screen** over four
>   traced pairs then leaves **0** board differences where no host answered 429 (**11 of 11** raw movers
>   are 429s), so the board question closes on this population
>   ([2026-10-26 follow-up](discovery-64-lane-parity-2026-10-26.md)).
>   The one recorded "defect" — a jobs list rendering **4** rows per page
>   while the build configures **10** — is the adaptive recalculator's floor of 4, not a defect (R3).
>   Six residuals remain open below.
> - **Verified against:** `37df08902414c2eb5e0e01c5c2e39b97af62814c` (`main`, 20 ahead of `origin/main`,
>   source version `0.3.019`). Deployed revision `af935dfca32a8506669dfbc74abce9b0d1d4b967` is a label
>   inferred from published OCI tags, not a read of the running container.
> - **Then inspect:** the closeout entry point `_out/perf-runs/discovery/p6-closeout.md` §8, and the three
>   roots under `_out/perf-runs/discovery/` — none of which is present in this checkout on 2026-10-26
>   (see Where the evidence lives).

## What ran, and what checked it

| Root | Contents | Builder | Independent checker | Exit | Count |
|---|---|---|---|---|---|
| `_out/perf-runs/discovery/p6-baseline-20261017/` | three leg-A runs (pinned 3,000-homepage audit, 3 × 1,000 rows) and three leg-B receipt sessions, provenance, isolation-defect record, pre/post state | `p6_baseline_verify.py` | `independent_assert.py` | 0 / 0 | 11 checks / 124 assertions |
| `_out/perf-runs/discovery/p6-lane-study-20261017/` | three counterbalanced 32/64 pairs (12 arms), a-priori plan, call logs, per-stage audits, sidecars | `lane_study_verify.py` | `independent_assert.py` | 0 / 0 | 13 checks / 47 assertions |
| `_out/perf-runs/discovery/p6-browser-proof-20261017/` | gate probes with full bodies, shipped page-load audit, console probe, page-size probe, request log, served modules, screenshots | `build_summary.py` | `independent_assert.py` | 0 / 0 | 62 assertions |

Each checker shares no code with its builder and re-derives every published number from the raw
artifacts. Combined figures are regenerated from the three summaries by
`_out/perf-runs/discovery/p6-closeout-numbers.py` into `p6-closeout-numbers.json`; nothing in this note
is quoted from memory.

## Baseline (leg A: pinned 3,000-homepage GameDevMap audit)

| Run | Wall s | Artifact total s | Homepages fetched / attempted | Active candidates | homepageFetchMs |
|---|---|---|---|---|---|
| 1 | 367.3 | 366.6 | 2,750 / 3,000 | 117 | 123,908 |
| 2 | 316.7 | 316.0 | 2,711 / 3,000 | 115 | 125,157 |
| 3 | 365.4 | 364.8 | 2,744 / 3,000 | 114 | 164,013 |

Three identical runs spread **316.7-367.3 s (16.0%)** on wall and **32.4%** on the homepage phase.
That spread is why no single-run comparison in this programme counts as a speedup, and it is the floor
the lane study must beat. Leg B carries **1,087 / 1,083 / 1,083** boards — a receipt-coverage session,
not a timing triple. The baseline root also records its own leg-B isolation defect: a pre-existing
untracked `data/sheet-directory-discovery-audit.json` was overwritten (`059a31308c41 → 31d72e51c270`,
not byte-recoverable); the three artifacts it created were removed and every later run proves its own
isolation.

## 32/64 lane study

Verdict by its a-priori rule: **`increase-to-64-supported`**.

| Leg | homepage gain per pair (ms) | homepage speedup | whole-run speedup | boards lost by 64 |
|---|---|---|---|---|
| A | 35,249 / 27,541 / 33,124 | 1.693× / 1.583× / 1.661× | 1.122× / 1.120× / 1.152× | 5 / 1 / 3 |
| B | 72,742 / 59,516 / 64,023 | 1.523× / 1.447× / 1.471× | 1.137× / 1.086× / 1.168× | 5 / 2 / 6 |

Every gain clears the 32-lane arm's own spread (**11,324 ms** leg A, **18,951 ms** leg B), and the lane
knob is proven from the shipped counter on all twelve arms.

The same-arm floors the parity rule uses are **6 boards / 959 failure identities / 319 total failures
/ 65 homepage-stage failures / 65 outcome rows** at leg A, and **10 / 2,506 / 317 / 102 / 102** at leg
B. Against them the timing case stands and the outcome case does not:

- **Boards were lost in all six pairs** (1-6 per pair, two-sided sign test **p = 0.0312**). Each loss
  is inside the 6-10 board churn, but six of six in one direction is not something a size threshold
  can dismiss.
- The **failure-identity criterion is near-saturated**: leg A pair 1 used **950 of 959** (99%), so it
  excludes a wholesale change in what failed and nothing finer.
- Homepage-stage failures trended up in five of six pairs (+10/+12/+58 at leg A, +25/−3/+80 at leg B),
  every rise inside that stage's own churn of 65 / 102.
- Leg B pair 2's total failure count rose **329** against a **317** tolerance while its homepage-stage
  failures **fell by 3**.

**Outcome parity revisited 2026-10-26 (R4).** Four counterbalanced pairs over the same 3,000
representative rows changed boards on **23** occasions in total, and **all 23** are **429**s on the arm
that did not keep them: **11** Workable boards throttled at `apply.workable.com`, one Workable board on
its own homepage, **9** other homepages and **3** static probes. The two arms in which *every* Workable
probe failed are the two pair-1 64-lane arms, each immediately after the first arm of its study; the
recorded pair-2 64-lane arm kept all four Workable boards, so the lane count is neither necessary nor
sufficient. An earlier reading of these pairs counted the repeat Workable losses as zero-job or
provider-inference differences and priced the wider lane at about **2%** of boards; that reading came
from a probe-failure row that carries neither studio nor URL, so the attribution fell through to a
sibling candidate's zero-job row. With the attribution fixed, **no lane-driven board cost is measured**
and R4 stays open — at this population size the board measure cannot separate a lane effect from
per-host throttling. The follow-up, its per-host table and the two failure-count excursions are
recorded in [the parity snapshot](discovery-64-lane-parity-2026-10-26.md).

## Browser proof against the deployed build

Gate: `/ops/health` **200 / healthy / 0.3.019** at `2026-10-17T03:12:16.948966+00:00`; `/ops/dashboard-health`
**200** at `03:12:23.041424+00:00`. The 504 window from `2026-10-16T23:56:54Z` to that recovery is
recorded in the baseline root's probe snapshots.

| Page | Cold DCL / LCP | Warm DCL | Console errors | Page errors | Failed requests | 4xx/5xx |
|---|---|---|---|---|---|---|
| `/admin.html` | 5,055 / 5,084 ms | 58 ms | 0 | 0 | 0 | 0 |
| `/jobs.html` | 9,623 / 9,636 ms | 69 ms | 0 | 0 | 0 | 0 |

Cold cost is the document transfer (`/admin.html` 4,406 ms, `/jobs.html` 9,332 ms of those DCLs), not
rendering. Driven as a first-time user: all five admin tabs select and render with live badges
(0 / 3 / 4 / 375 / 80); the jobs search narrows 40,008 → 528 and clears; pagination advances 1-4 → 5-8;
the Remote type filter gives 6,411.

**First recorded as a defect, explained 2026-10-26 (R3).** The jobs list rendered **4 rows per page**
(counted in the DOM on two pages) against the deployed build's own `itemsPerPage: 10`, which read as
10,002 pages for 40,008 jobs with no established cause. `itemsPerPage: 10` is only the value before
the page recalculates: `recalculateItemsPerPage` sets `max(4, min(25, floor(availableHeight /
rowHeight)))`, so 4 is the floor a short viewport with tall stacked-card rows reaches. The formula is
present at the revision the deployment labels identify (`af935dfc`, release 0.3.019), whose `state.js`
also lacks `pendingSavedJobActions` — the marker the pass used to call the served bundle not-HEAD's.
That divergence is real, but it does not touch this behaviour; the digest check in R1 is still what
would confirm the running container's identity. The arithmetic closes exactly: 40,008 jobs over
4 rows is the observed 10,002 pages. There is no code defect, and whether a floor of 4 suits a 40k-row
list is a UX decision rather than a fix.

## Residuals

| # | Residual | Closes with |
|---|---|---|
| R1 | Running container's digest never read; deployment identity is an inference from published labels + appVersion | one `docker inspect` on the Umbrel host (command in `p6-baseline-20261017/phase0-provenance.md`) |
| R2 | Target 504 window `2026-10-16T23:56:54Z` → recovery `2026-10-17T03:12:16Z`; cause unknown | container logs for that window, or an operator statement |
| R3 | ~~Jobs list renders 4 rows/page against `itemsPerPage: 10`; 10,002 pages~~ — **closed 2026-10-26**: `itemsPerPage: 10` is the pre-calculation default, and `recalculateItemsPerPage` floors at 4 (`max(4, min(25, floor(availableHeight / rowHeight)))`) at the revision the deployment labels identify (`af935dfc`); 40,008 / 4 = 10,002 is the expected result | nothing to fix; whether a floor of 4 suits a 40k-row list is a UX decision |
| R4 | **Outcome parity at 64 lanes: the board half is measured on 429-free hosts; the rate half is open** (6/6 board losses, p=0.0312; identity criterion at 99% of tolerance). Four later counterbalanced pairs changed boards on 23 occasions and every one is a 429 on the arm that lacked the board; a 64-lane arm also ran clean, so no lane-driven board cost is measured and the earlier ~2% "price" is withdrawn. A P6-shaped three-pair re-run on 2026-10-26 repeated the net board loss at 64 in 3/3 pairs (net −7) with 13/13 movers 429s, but 7 movers were in probe/recovery phases the lane knob does not change and the rule returned `parity-not-supported`. A pre-registered 429-free screen then re-judged four traced counterbalanced pairs over only the boards whose hosts never answered 429: all 11 raw movers are 429s and the clean board sets are identical in every pair (107/107, 105/105, 106/106, 103/103), so no board moves where throttling is excluded. The six losses themselves survive only as counts: their arms are absent from this checkout | the **rate** question — whether 64 lanes raise a host's own 429 rate — which the screen cannot answer because it removes throttled hosts by construction, so it needs a run that holds hosts constant while lanes change; and P6's six loss identities, whose arms remain absent |
| R5 | Leg B lane peaks are single-source (no wrapper on the runner path) | a shipped instrumentation seam, in its own change |
| R6 | Browser pass boundary slip: the first harness clicked "Retry now"; lifecycle stayed 0 current / 240 recent, harness rewritten and re-run | nothing target-side; the record stays |
| R7 | Baseline leg-B `data/` overwrite of `sheet-directory-discovery-audit.json`; not byte-recoverable | none — recorded in `legB-isolation-defect.md` |
| R8 | ~~The plan's P6 table still shows the 2026-10-16 "not run" statuses~~ — **closed 2026-10-26**: the plan was folded into [`measurement-methods.md`](../measurement-methods.md), [`scraping-pipeline.md`](../scraping-pipeline.md) and its own slice snapshots, then deleted, so this note is the P6 record and the stale table no longer exists | nothing further |

## Where the evidence lives

- `_out/perf-runs/discovery/p6-closeout.md` §8 — the entry point over all three roots, with the
  per-root gate table, the commit contents, and the ordered next-session handoff.
- `_out/perf-runs/discovery/p6-closeout-numbers.json` — every number above, regenerated from the roots.
- `_out/perf-runs/discovery/p6-baseline-20261017/baseline-summary.md`,
  `p6-lane-study-20261017/lane-study-summary.md`, `p6-browser-proof-20261017/summary.md` — the three
  pass-level summaries this note reconciles.
- `_out/perf-runs/discovery/lane-parity-p6shape/` — the 2026-10-26 P6-shaped re-run of three
  counterbalanced pairs (six arm records and `report.json`), the fresh evidence R4 now cites.
- **Not present in this checkout on 2026-10-26.** `_out/perf-runs/discovery/` then held only the
  2026-10-26 lane-parity runs; none of the three roots above resolves, so this note's numbers are the
  surviving record rather than something this checkout can re-derive. R4's per-board question — whether
  P6's six losses were also 429s — cannot be put to the arms until they are restored.

## Boundaries

No production configuration, concurrency, timeout, retry or per-host limit changed. No target-side
run was started; the target saw health probes, page loads, module GETs and read-only UI interactions
only. No shipped `src/`, `frontend/` or `tests/` file changed by these passes. Repo `data/` was
untouched by the lane study and the Browser pass; the baseline's leg-B isolation defect is R7.

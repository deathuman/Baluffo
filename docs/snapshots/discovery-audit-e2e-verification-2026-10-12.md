# The three-batch GameDevMap audit, re-run on the shipped tree — queue identity holds, the probe wall is inside the drift — 2026-10-12

> - **Class:** measurement (production-shaped verification, verdicts unchanged, one leg inconclusive)
> - **Trigger:** item 5 of [`../plans/discovery-probe-permit-allocation-plan.md`](../plans/discovery-probe-permit-allocation-plan.md) — the last gate on the probe transport and permit-order work: run the audit end to end and compare candidate outcomes, queue identity, phase timings, request/retry counts and rate-limit failures against the retained arms.
> - **Status:** measured. **Queue identity passes** — on the 1,045 candidates both arms queued, every queue/defer field is identical, and the two retained arms (same tree, five minutes apart) differ on one of 1,130. **The phase wall is inconclusive** — this arm's probe phase runs **8.1%** under the faster retained arm and **16.7%** under the slower, but the retained pair differ by **8.0% between themselves** on the same per-batch signature, so one arm cannot separate the change from the drift. No claim is made on the wall.
> - **Verified against:** `9b8901f8` (the checkout's own `APP_VERSION = "0.3.018"`) plus the 2026-10-12 working tree. The runtime delta against the retained arms is **exactly two files**, `src/source_discovery/probe_runtime.py` and `src/source_discovery/web_search_fetch.py`; every other modified path is older than the retained runs and therefore identical in both.
> - **Then inspect:** `_out/perf-runs/discovery/compare_audit_e2e.py` (phases), `compare_audit_verdicts.py` (candidates and queue identity), `fetch_log_batches.py` (per-batch occupancy and request latency), and the arms under `_out/perf-runs/discovery/audit-runs/` with `verdict-diff-vs-first.txt`, `verdict-diff-control.txt` and `per-batch-phases.txt`.

## What was run

Same driver and parameters as every retained arm, into an isolated output directory:

```
python _out/perf-runs/discovery/audit_probe_bench.py \
    --arm e2e-0.3.018-9b8901f8 --batch-size 1000 --max-batches 3 --timeout 6
```

| Arm | started (UTC) | code | statuses |
|---|---|---|---|
| `trust-anchor` | 08:25:27 | before the directory and probe lane pools | retained; a reference, not the control |
| `lane-ceiling` | 11:00:23 | pools in place | retained; half of the drift pair |
| `lane-ceiling-first` | 11:05:51 | **same tree as `lane-ceiling`** | retained; the other half |
| `e2e-0.3.018` (this arm) | 13:21:52 | the retained tree **+ 2 files** | the arm under test |

`lane-ceiling` and `lane-ceiling-first` are the same tree run five minutes apart. Everywhere below
that pair is **the drift control**: it is the only measurement in this set that holds the code still,
so any movement the arm under test shows has to be read against what it shows on its own. The
`trust-anchor` arm predates the lane pools and is quoted only for the trend across the day.

All four walked the same 3,000 studio homepages: `completedUrls` is 3,000 in every arm and the sets are **identical** (3,000 shared, 0 only-either). `runtime.configSignature` is byte-identical across all four, and the only `runtime` key that differs anywhere is `artifactSizeBytes`. The run is one three-batch slice of an 8,370-row walk (`remainingUrls` 5,370 in every arm), so "end to end" here means every phase of all three batches, not the full population.

The audit neither records nor measures CPU: the artifact and report carry no version or CPU field at all, so the in-situ CPU receipt that the plan calls "the claim" is not available from these arms — it stays the bench's. Version identity is provenance: the run's log header records `HEAD: 9b8901f8`, and `src/app_version.py` at that commit is `0.3.018`.

## What the delta is

A file-modification-time partition of the working tree against the retained runs (11:00-11:10 UTC) leaves exactly two runtime files newer than them:

- **`src/source_discovery/web_search_fetch.py`** — the discovery `fetch_text` now builds its TLS context from `src/shared/tls_https.py`'s union anchor; a blocking `fetch_text_httpx` twin exists for the pool; `fetch_text_with_retry` gains `timeout_attempts`, caps *timeout-class* failures at the probe's bound and leaves every other failure on the shared ladder.
- **`src/source_discovery/probe_runtime.py`** — the default path runs its request on the batch's own pool through a sync `httpx.Client`, and the shared client is closed by the last fetch out rather than under a fetch that outlived its per-call bound.

Both files sit on the **probe** wave. The homepage and recovery waves reach them only through `fetch_text`, and only through the TLS anchor — a superset of `urlopen`'s default context, so it can convert failures into reads and not the reverse. The retry ladder is unreachable from there: `directory_fetch.fetch_directory_pages` (the homepage wave) calls `fetch_text` directly, and `fetch_text_with_retry`'s only callers are the probe and the admin source check.

## Phase timings — and why this arm cannot price them

Totals, milliseconds:

| Arm | probe | homepage fetch | wave 1 | wave 2 | run total |
|---|---|---|---|---|---|
| `trust-anchor` | 92,512 | 127,198 | 140,846 | 25,651 | 443,335 |
| `lane-ceiling` | 96,049 | 95,221 | 55,752 | 25,156 | 326,607 |
| `lane-ceiling-first` | 87,065 | 74,614 | 55,348 | 15,692 | 275,265 |
| **this arm** | **79,972** | 91,153 | 48,940 | 20,880 | 296,391 |

Probe phase per batch, with the request count and the per-request latency the batch drew (occupancy is Σ request ms / the artifact's own phase wall, from each arm's fetch log):

| Arm | batch 1 | batch 2 | batch 3 | Σ | probe p50 | probe mean | probe requests |
|---|---|---|---|---|---|---|---|
| `lane-ceiling` | 38,451 | 29,349 | 28,249 | 96,049 | 138 | 388.7 | 3,731 |
| `lane-ceiling-first` | 30,205 | 28,723 | 28,137 | 87,065 | 140 | 365.5 | 3,664 |
| **this arm** | **27,918** | **26,867** | **25,187** | **79,972** | 143 | 377.8 | 3,649 |

The honest reading of that table is not the one it invites. This arm's worst batch (27,918) is below **all nine** retained batches, and every batch is below both retained arms — but the two *identical-code* arms show the same shape between themselves: `lane-ceiling-first` is below `lane-ceiling` in all three batches, by 21.4%, 2.1% and 0.4%, a **mean per-batch ratio of 0.920** against this arm's **0.918**. The control reproduces both the magnitude and the signature: it too moved the wall ~8-9% while its own median request duration got *worse* (138 → 140 ms; this arm: 140 → 143 ms).

So the phase that the delta touches is running inside the drift band of two runs of the same code, and the neighbouring phases are noisier still — the homepage wave moved 74,614 → 91,153 → 95,221 → 127,198 ms across arms whose homepages wave was untouched, tracking its own median request duration (298 → 426 → 460 ms) and its DNS failures. **Verdict: inconclusive on wall time.** What would settle it is either several arms per code state or an in-situ CPU receipt, since the bench's claim was loop CPU (**22.28 → 4.97 s**), which drifts far less than a wall.

## Request, retry and rate-limit counts — flat between arms

From the probe phase's own fetch log:

| Arm | requests | distinct URLs | retry attempts | candidate-level failures | 404s | 429s | timeouts | DNS | ok |
|---|---|---|---|---|---|---|---|---|---|
| `lane-ceiling` | 3,731 | 3,158 | 573 | 1,973 | 1,702 | 801 | 31 | 0 | 1,190 |
| `lane-ceiling-first` | 3,664 | 3,140 | 524 | 1,947 | 1,703 | 742 | 18 | 0 | 1,198 |
| **this arm** | 3,649 | 3,108 | 541 | 1,970 | 1,702 | 759 | 25 | 13 | 1,142 |

The freshness signal is unmoved: **1,702 / 1,703 / 1,702** HTTP 404s, and candidate-level failures within 1.3%. Retries and 429s sit between the two retained arms in every wave, as `lane-ceiling` / `lane-ceiling-first` / **this arm**: probe 429s 801/742/**759**; homepage 429s 11/135/**47**; wave 1 152/71/**97**; wave 2 304/136/**200**. 5xx counts are flat in the same three-arm order (probe 5/1/5, wave 1 30/28/26, wave 2 47/52/56). `probeFailures` reads 1,862 / 1,839 / **1,857** against `trust-anchor`'s 1,857. No unexplained rate-limit, 5xx or attempt-count difference.

## Candidate outcomes — the difference is which homepages resolved

Probed 3,038 / 3,024 / **2,992** (provider bucket flat within 0.1%: 2,436 / 2,435 / **2,433**; static 602 / 589 / **559**). Of 2,571 candidates present in all three runs, **2,556 (99.42%)** carry an identical verdict.

The population difference is not a selection decision. The studios missing from this arm (`94 Kelvin`, `ACE Team`, `Actrio Studio`, `Adala Studios` …) are exactly the ones whose homepage fetch **failed** here, and they turn up in this arm's homepage-level rejection surface instead (`94 Kelvin|https://94kelvin.is` → `homepage_fetch_failed`); the studios only *this* arm inferred (`2 Wedges`, `2D Boy`, `3DIVISION`, `4bit Games` …) are the ones that resolved for it. That mirror pair is present between the retained arms too, just smaller: 62 new-only `homepage_fetch_failed` keys in the control pair against 119 here.

This arm's homepage wave drew an unlucky resolver window, which is visible in its own failure classes: **314** DNS failures (`Errno 11002/11001 getaddrinfo failed`) against 24 and 22 in the retained arms, with its 429s *lower* than the control's (47 against 135). Every arm fetched 3,000 homepages; 2,809 / 2,689 / **2,504** succeeded.

## Queue and defer identity — the leg that passes

For the 1,045 candidates that reached a queue surface (`activeCandidates` ∪ `zeroJobCandidates`) in both this arm and `lane-ceiling-first`, these ten fields are **identical on every candidate**: `candidateState`, `deferred`, `deferCount`, `promotionLane`, `rankScore`, `quarantineReason`, `score`, `confidence`, `jobsFound`, `weakSignal`. The identical-code control pair differs on one candidate of 1,130 (`massive.se`: 0 → 24 jobs, `low` → `high` confidence) — i.e. this arm's queue identity is *cleaner* than the control's, not worse.

The verdict surface, on 2,655 shared candidate ids: 90 active → active, 955 zero-job → zero-job, 1,595 rejected → rejected, and **15 flips** — 6 `active → probe_failed`, 8 `zero_job → probe_failed`, 1 `probe_failed → zero_job`. All 15 are `static:listing_url:` candidates, and every one is named in the fetch log with a cause on the *target's* side:

| Cause in this arm | Candidates | Evidence |
|---|---|---|
| DNS: `getaddrinfo failed` (23-1,089 ms) | 8 | resolved 200 in both retained arms |
| HTTP 429, three attempts | 5 | also 429 in `lane-ceiling`, the same code as the control's other half, and 200 there |
| HTTP 403 | 1 | `braceyourselfgames.com`, 200 in both retained arms |
| slow host | 1 | `hermitworks.jp` (13,007 + 7,653 ms in the control, 5,127 ms here) |

The control pair flips **10 of 2,865 (0.35%)** the same way, in both directions. The 19 actives this arm did not keep are equally accounted for: 6 are the `probe_failed` rows above, 10 are candidates whose *homepage* failed on the same host in this arm, and 3 differ by identity rather than by decision (`arte.tv`'s rotating `_search_token`, and two whose candidate id this run never created).

## What this does not show

- **No wall claim.** One arm against two, inside a control band of the same size. The per-batch table is consistent with the delta helping, and equally consistent with an ordinary unlucky/serendipitous run: the drift pair shows the same 8% and the same flattening-then-worsening latency.
- **The normal discovery smoke that item 5 also asks for was not run *here*.** This is the audit leg only. It has since been run for real — see [discovery-smoke-verification-2026-10-12.md](discovery-smoke-verification-2026-10-12.md).
- **No in-situ CPU.** The audit records no CPU field, so the bench's loop-CPU receipt is not reproduced here; that remains the transport change's evidence.
- **The resolver window is an anomaly, not a condition of the change.** 314 of this arm's 441 recorded `homepage_fetch` failures are name resolution, against 24 and 22 in the retained pair; the same class also reaches the delta-free recovery waves here (39 and 64) and is absent at that level there. It is why this arm's population is smaller than either retained arm's (2,992 probed against 3,024 and 3,038), and it is not attributable to the code. (Two instruments count this wave: 441 failure *records* against 496 *rejected candidates* whose reason is `homepage_fetch_failed`, the second counting each refused studio.)
- **`_out/` is scratch.** The three comparison scripts live under `_out/perf-runs/discovery/` by design; the numbers above are reproducible from the arms in `audit-runs/`, but nothing here is a guardrail.

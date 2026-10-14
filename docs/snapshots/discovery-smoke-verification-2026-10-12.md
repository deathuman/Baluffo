# The normal discovery smoke on the shipped tree — flow and queue decisions hold, every probe failure is a remote condition, the wall moves 22% between two same-tree arms, and the audit path reports CPU and lanes in place — 2026-10-12

> - **Class:** measurement (production-shaped verification — the smoke leg of plan item 5, plus a supplementary arm that exercises the runtime receipt).
> - **Trigger:** item 5 of [`../plans/discovery-probe-permit-allocation-plan.md`](../plans/discovery-probe-permit-allocation-plan.md), whose audit leg is recorded and which left "**the normal discovery smoke**" open; [`../snapshots/discovery-audit-e2e-verification-2026-10-12.md`](discovery-audit-e2e-verification-2026-10-12.md) records that leg as not run.
> - **Status:** run, exit 0 on both legs. **Flow identity holds** — two smoke arms of the same tree generate the same **733** candidates, probe the same **725** over the same **728** deduped rows, and queue the same **5** sources through the same three adapters, with **0** static suppressions and every deferred row on `top_n_cap`. **Every probe failure is a remote condition** — **129 / 125** of 725, fully accounted as **91 / 86** DNS, **32 / 33** HTTP 429, and the *same six* named rows (three Greenhouse `missing adapter or URL`, one SSL hostname mismatch, one 406, one disconnect). **The wall is inside the drift again:** **29,231 → 22,718 ms** run total (**−22.3%**) between two runs of identical code **2 min 47 s** apart, so this leg prices nothing either. **The supplementary arm carries the receipt the audit could not**: the capped GameDevMap leg writes `phaseRuntime` per phase with process CPU and lane receipts — probe **17 of its 17** lanes in flight at peak, `laneOccupancyPct` **16.6 / 31.9**, and `probeLifecycle` clean in both arms (`fetchesBoundedOut` **0**, `fetchesOutstandingAtExit` **0**, `clientsUnclosed` **0**, `deadlineExits` **0**).
> - **Verified against:** the 2026-10-12 working tree (uncommitted discovery changes, `APP_VERSION` **0.3.018**). No file changed between any two arms of a leg.
> - **Then inspect:** `_out/perf-sanity-discovery-quick/`, `_out/perf-sanity-discovery-quick-arm2/`, `_out/perf-sanity-discovery-capped-gamedevmap/`, `_out/perf-sanity-discovery-capped-gamedevmap-arm2/` and the extracted tables in `_out/smoke-leg-tables.txt` (all `_out/`, i.e. scratch).

## What the smoke is

CI's `Benchmark` workflow runs it as the "Run discovery smoke benchmark" step ([.github/workflows/benchmark.yml](../../.github/workflows/benchmark.yml)); `npm run perf:discovery:quick` is the same command locally:

```
python src/discovery_sanity_benchmark.py --preset quick --timeout 10 --top 5 \
    --output-dir _out/perf-sanity-discovery-quick
```

`--preset quick` bounds the run by disabling `sheetDirectory`, `seedCareersScan`, `gamesmap`, `gameprog`, `gamedevmap` and `webSearch`, leaving the curated seed and the provider patterns. The leg is therefore the **normal** discovery path — generation, dedupe, probe, queue — and **not** the audit: the quick report contains no `phaseRuntime` and no `configuredLanes` occurrence at all, `gamedevmapAuditSummary` is `{}`, and the `gamedevmap` stage timing is 0 ms. That is why the second leg below exists.

## The smoke, twice on 2026-10-12 (UTC)

| Arm | started (UTC) | run total | probe stage | generated | probed | deduped | failed probes | probe misses | validated | queued | deferred |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A | 14:49:45 | 29,231 ms | 28,230 ms | 733 | 725 | 728 | 129 | 8 | 581 | 5 | 576 |
| B | 14:52:33 (+2 min 47 s) | 22,718 ms | 22,200 ms | 733 | 725 | 728 | 125 | 14 | 584 | 5 | 579 |

Both arms exit 0, and the probe stage is **96.6% / 97.7%** of the run — the same "probe is the run" shape the [lane-cost snapshot](discovery-lane-cost-2026-10-11.md) recorded across 28 real reports.

What is *identical* is the decision surface: the same generated, probed, deduped and queued counts, the same `{workday: 3, teamtailor: 1, smartrecruiters: 1}` queue, the same `deferredReasons` of `top_n_cap` only, `suppressedStaticCount` **0** in both with an empty `suppressedStaticByReason`, and the same three Greenhouse candidates refused for `missing adapter or URL`. What moves is the split between "probe failed" and "probe missed" (**129/8 → 125/14**) and the wall (**−22.3%**). Neither is attributable to code: the population the driver builds is deterministic, and the per-stage timing of two arms of one tree has now been measured to move 8.0% (audit, [e2e snapshot](discovery-audit-e2e-verification-2026-10-12.md)) and 22.3% (this smoke).

## Every probe failure is on the far side of the wire

`failedProbeCount` and the `probe`-stage failure records agree exactly, and the classes are the same in both arms:

| Arm | DNS `getaddrinfo failed` | HTTP 429 | 403 / 404 / 5xx | named single rows |
|---|---|---|---|---|
| A | 91 | 32 | 0 | 6: 3 × `missing adapter or URL`, `q-games.com` SSL hostname mismatch, `playedwithfire.com` 406, `vivastudios.com` server disconnected |
| B | 86 | 33 | 0 | the same six rows |

No timeout, no deadline and no pool error appears in either arm's failure records — the classes the working-tree probe changes could plausibly create are absent, which is the check this leg exists to make. The rate-limit share is the same shape the retained e2e arms saw on the probe wave (801/742/759 429s of ~3,700 requests), here concentrated on four ATS hosts; the DNS share is this box's resolver window, which the earlier audit arm also drew (314 homepage-level DNS failures against 24 and 22).

## The audit path, with the receipt (supplementary legs)

```
python src/discovery_sanity_benchmark.py --preset capped-gamedevmap --timeout 30 --top 10 \
    --output-dir _out/perf-sanity-discovery-capped-gamedevmap
```

Both arms exit 0. This is the same benchmark entrypoint with the heavier stage family enabled (20 rows, 10 homepage fetches, one batch), so the audit runs for real over the network and writes its receipt into `gamedevmap-active-source-dry-run.json`: one row per phase per batch at `timings.batches[].phaseRuntime`, rolled up per run at `summary.phaseRuntime` ranked by CPU, beside the untouched wall-shaped `timings.totalsMs` (the contract recorded in [the runtime-receipt snapshot](discovery-audit-runtime-receipt-2026-10-12.md)).

| Phase | wall A / B (ms) | CPU A / B (ms) | CPU/wall A / B | items | lanes | peak | mean | occupancy A / B | lane-busy A / B |
|---|---|---|---|---|---|---|---|---|---|
| probe | 2,013 / 838 | 328 / 219 | 16.3% / 26.1% | 17 | 17 | **17 / 17** | 2.83 / 5.42 | 16.6% / 31.9% | 5,694 / 4,538 |
| homepage_fetch | 2,119 / 1,766 | 297 / 297 | 14.0% / 16.8% | 5 | 4 | **4 / 4** | 1.89 / 1.94 | 47.2% / 48.6% | 4,000 / 3,430 |
| recovery_wave1_fetch | 4,267 / 1,503 | 234 / 266 | 5.5% / 17.7% | 12 | 8 | 5 / 5 | 1.65 / 2.30 | 20.6% / 28.8% | 7,039 / 3,459 |
| recovery_wave2_fetch | 3,354 / 1 | 188 / 0 | 5.6% / — | 4 / 0 | 8 / 0 | 1 / 0 | 0.95 / 0 | 11.8% / — | 3,176 / 0 |
| homepage_analysis | 97 / 92 | 94 / 94 | 96.9% / 102.2% | 0 | 0 | 0 | 0 | — | 0 |
| recovery_wave1/2_analysis, direct_inference, merge | ≤12 | ≤16 | — | 0 | 0 | 0 | 0 | — | 0 |

`inFlightAtFinish` is **0** for every phase in both arms, and `probeLifecycle` reads identically in both: 17 fetches started, `fetchesBoundedOut` 0, `fetchesOutstandingAtExit` 0 and `maxFetchesOutstandingAtExit` 0, `clientFetchesOutstandingAtExit` 0, `clientsRetiredByBatch` 1 with `clientsRetiredByLastFetch` 0, `clientsUnclosed` **0**, `deadlineExits` **0**, `poolThreadsAliveNow` 0, `maxClientCloseDeferredMs` 1 / 2.

The receipt's *lane* facts repeat across the two arms — the probe wave fills all 17 configured lanes at peak both times, the homepage wave fills all 4, `inFlightAtFinish` stays 0, nothing is bounded out and no client is left unclosed — while every **wall** number moves by up to 4× (run total 14,543 / 6,804 ms; audit 11,904 / 4,253 ms; recovery wave 1 4,270 / 1,506 ms; recovery wave 2 3,357 / 1 ms because the second arm had nothing left to ask). This is the same asymmetry the receipt was built for: the second arm's `recovery_wave2_fetch` shows 0 items on 0 lanes, so its 1 ms is a phase that did nothing rather than a phase that got fast.

## What this does not show

- **No wall claim.** Two arms of one tree differ by 22.3% here and 8.0% in the audit pair. The smoke leg confirms the shipped path runs and decides the same way; it cannot price the transport or the acquire-order change, and this snapshot does not try.
- **The CI comparator did not run.** `scripts/perf_compare.py --mode discovery` against `_out/perf-baseline/discovery-baseline.json` returns `{"status": "baseline_missing", ...}` with exit 0 ("benchmark is informational") in this checkout, because `_out/perf-baseline/` is scratch and absent here. The leg's evidence is therefore its own exit status and payload, not a baseline delta.
- **The quick preset cannot see the receipt.** Nothing in the quick report carries `phaseRuntime`, `configuredLanes` or lane occupancy; the CPU/lane evidence above comes from the capped GameDevMap leg, which is a 20-row/one-batch population, not the audit's 3,000-homepage walk.
- **CPU is process-wide.** A phase's `cpuMs` is an upper bound on its own work, so short analysis phases exceed 100% (96.9% / 102.2% on `homepage_analysis`, 1,600% on a 1 ms `recovery_wave2_analysis`) — the documented limitation of the receipt, visible here rather than papered over.
- **This box's resolver is unstable.** 91 / 86 of 725 probe failures are DNS, and the same window appears in the retained audit arm. A healthy-resolver run would have a smaller failure count, not a different *class* of failure.
- **No queue surface on the audit legs** (`queuedCandidateCount` 0, `activeCandidates` 0, `lostRecoveredActiveCandidates` 0, `browserRecoveryCandidates` 2 in both), so the queue-identity claim rests on the audit snapshot and this smoke, not on the receipt arms.
- **One arm pair per leg, on an uncommitted tree.** The plan's stop rule asks for several arms per code state before any wall number is treated as real; and the packaged acceptance — a deployed build at this checkout's `appVersion`, `/ops/health` verified — remains open.

# End-to-End Benchmarking and Responsiveness Plan

> Canonical for frontend responsiveness instrumentation, startup/perf traces, benchmark artifacts, trend tracking, and current optimization targets.
>
> Not canonical for pipeline timing payload schemas, fetch report schemas, source-sync closeout history, or task-progress UX history. Use the relevant source modules and archived closeout docs for those contracts.

## Intent

Baluffo should have enough performance signal to answer three questions quickly:

1. What is slow in page boot, rendering, bridge calls, and pipeline fetches?
2. Which regressions are real enough to gate or investigate?
3. Which optimization target is safest and highest-impact next?

The plan favors opt-in diagnostics and additive instrumentation. It must not change bridge route signatures, persisted job contracts, startup metric schemas, task-run UX, or source output behavior unless a separate compatibility decision is made.

## Current status

Implemented:

- Admin, Jobs, and Saved User Timing marks via `frontend/shared/perf-marks.js`.
- Startup-probe-gated Long Task observer in `probes/long-task-observer.js`.
- Opt-in Playwright perf traces via `playwright.perf.config.js` and `npm run test:frontend:perf`.
- Admin responsiveness quick wins: redundant boot refresh removal, registry signature digest, idle render deferral, and stale-render guards.
- Frontend/bridge counter instrumentation and benchmark summaries for startup, lifecycle, fetch, and render signals.
- Static outlier benchmark group: `npm run perf:fetch:static-outliers`.
- Benchmark artifact improvements: `sourcePolicySignals`, `sourceRegistrySignals`, `registryScopeSummary`, and `nextOptimizationTargets`.
- Full uncapped pipeline benchmark evidence collected and used to steer optimization work.

Recent validation:

- `python -m pytest tests/test_fetch_incremental_sanity_benchmark.py -q --color=no` -> `16 passed`.
- `cmd /c npm run perf:fetch:static-outliers` -> passed and wrote `_out/perf-sanity-fetch-static-outliers/benchmark-summary.json`.

## Latest benchmark evidence

Full uncapped pipeline run:

- Output jobs: `33642`.
- Source rows: `1916` total, `1531` successful, `385` failed.
- Wall clock: `1277528ms` (~21.3 min).
- Stage totals: `fetchAndParse=11233801ms`, `detailFetch=3108031ms`, `candidateExtraction=3061114ms`, `listingFetch=2947828ms`.
- Static dominates: `12961535ms` across `1892` static sources, with `384` static errors and `874` zero-kept static sources.

Latest focused static-outliers run:

- Total: `165848ms`.
- First run: `164786ms`.
- Second run: `1062ms`.
- `listingFetch=80077ms`, `candidateExtraction=54651ms`, `detailFetch=119957ms`.
- `registryScopeSummary`: `3` cross-host static sources, `7` off-listing-host pages.

Current ranked targets from the artifact:

1. Super Lucky Casino: `source_policy_review`, priority `100`, `requiresExplicitDecision=true`.
2. Atvis: `source_policy_review`, priority `100`, `requiresExplicitDecision=true`.
3. Netflix Games: `source_policy_review`, priority `90`, `requiresExplicitDecision=false`.
4. Koei Tecmo Vietnam: `source_scope_and_timeout_review`, priority `65`, `requiresExplicitDecision=true`.
5. Maliyo: `timeout_or_network_budget`, priority `30`, `requiresExplicitDecision=false`.

Important interpretation:

- Super Lucky keeps output but the active registry row starts at `superluckycasino.com` and includes `stillfront.com` parent-career pages. Treat this as a source-policy/output-contract decision, not a mechanical speed fix.
- Atvis keeps output but has site-changed evidence and a LinkedIn off-listing page. Treat as source-policy review.
- Koei keeps output and has timeout pressure plus cross-host `careerviet.vn` registry pages. Treat as combined source-scope and timeout review.
- Netflix is zero-kept `needs_review`; safer to review than kept-output sources, but existing persisted Netflix jobs mean avoid broad assumptions.
- Maliyo is the cleanest remaining mechanical timeout/network-budget target.

## Remaining work

Highest-value next work:

1. Decide source-policy handling for Super Lucky, Atvis, and Koei.
2. Improve Maliyo timeout/network diagnostics before lowering any budget.
3. Add a concise trend report that compares latest full lifecycle and static-outlier runs against previous artifacts.
4. Keep perf traces opt-in but document how to attach `_out/perf-traces/` artifacts to investigations.
5. Consider a CI smoke benchmark only after signals are stable enough to avoid noisy failures.

Deferred work:

- `BALUFFO_PROFILE=1` profiling hooks and pstats summaries.
- Sync operation timing parity with discovery/fetch timing: implemented for pull/push stage totals, sorted `stageTop`, `/sync/status` latest/history payloads, and completed sync task summaries.
- Full NDJSON trend gate and PR annotations.
- Virtualized jobs-feed rendering or workerized CSV parsing, only if frontend long-task/render counters justify it.
- Any source suppression, migration, or registry mutation for kept-output sources.

## Validation commands

Focused benchmark harness:

```powershell
python -m pytest tests/test_fetch_incremental_sanity_benchmark.py -q --color=no
```

Focused static outliers:

```powershell
cmd /c npm run perf:fetch:static-outliers
```

Frontend instrumentation checks:

```powershell
node --test tests/frontend/unit/perf-marks.test.mjs
node --test tests/frontend/unit/long-task-observer.test.mjs
node --test tests/frontend/unit/startup-metrics-effects.test.mjs
```

Opt-in Playwright perf traces:

```powershell
cmd /c npm run test:frontend:perf
```

Full lifecycle benchmark, when needed:

```powershell
python -m src.jobs.pipeline --output-dir _out/perf-full-uncapped-pipeline --timeout 30 --force-refresh-all --quiet
```

## Success criteria

This effort is ready to close when:

- Benchmark artifacts identify source-policy, source-scope, timeout, frontend render, and bridge latency targets without manual report spelunking.
- Kept-output source changes are clearly flagged with `requiresExplicitDecision=true` before implementation.
- Opt-in traces and startup metrics can explain Admin, Jobs, and Saved boot regressions.
- Static/full lifecycle benchmarks can produce comparable artifact summaries across runs.
- Remaining optimization work is a short prioritized queue, not a broad investigation.

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
- Benchmark artifact improvements: `sourcePolicySignals`, `sourceRegistrySignals`, `registryScopeSummary`, `nextOptimizationTargets`, `sourceDecisionMatrix`, `source-decision-matrix.md`, `source-decision-log-template.md`, `sourceDecisionTrend`, `source-decision-trend.md`, per-source `timeoutDiagnostics` with embedded static-error URL extraction, `sourcePolicyDecision` kept-output host breakdowns, and `slow_productive_static` classification.
- Storage-pressure benchmark metrics: discovery and fetch sanity benchmark payloads now include `storageMetrics` with hot artifact bytes, JSONL journal bytes, gzip bytes, source-sync snapshot size/headroom, and hot-path budget warnings. Repeated perf CI summaries retain median/min/max storage metrics so SQLite/WAL migration work can compare storage pressure across runs.
- Decision-first source conflict record for Super Lucky and Koei: `docs/plans/static-outlier-source-conflict-decisions.md`.
- Generic static registry scope conflict audit in the source-policy soak report: `sections.staticRegistryScopeConflicts`, including dry-run-only `patchProposals` and explicit CLI apply-safe support for selected `shadowed_cross_host` rows.
- Dry-run decision checkpoint for the current generic patch proposal result: `docs/plans/static-scope-conflict-dry-run-decisions.md`.
- Guarded apply-safe CLI exercised once for Capcom; the current generic scope conflict verification shows `conflictCount=0` and `patchProposalCount=0`.
- Full uncapped pipeline benchmark evidence collected and used to steer optimization work.

Recent validation:

- `python -m pytest tests/test_fetch_incremental_sanity_benchmark.py -q --color=no` -> `27 passed`.
- `cmd /c npm run perf:fetch:static-outliers` -> passed and wrote `_out/perf-sanity-fetch-static-outliers/benchmark-summary.json`.

## Latest benchmark evidence

Full uncapped pipeline run:

- Output jobs: `33642`.
- Source rows: `1916` total, `1531` successful, `385` failed.
- Wall clock: `1277528ms` (~21.3 min).
- Stage totals: `fetchAndParse=11233801ms`, `detailFetch=3108031ms`, `candidateExtraction=3061114ms`, `listingFetch=2947828ms`.
- Static dominates: `12961535ms` across `1892` static sources, with `384` static errors and `874` zero-kept static sources.

Latest focused static-outliers run:

- Total: `161798ms`.
- First run: `160735ms`.
- Second run: `1063ms`.
- `listingFetch=91314ms`, `candidateExtraction=34090ms`, `detailFetch=105678ms`.
- `registryScopeSummary`: `3` cross-host static sources, `7` off-listing-host pages.

Current ranked targets from the artifact:

1. Super Lucky Casino: `source_policy_review`, priority `100`, `requiresExplicitDecision=true`.
2. Netflix Games: `source_policy_review`, priority `90`, `requiresExplicitDecision=false`.
3. Koei Tecmo Vietnam: `source_scope_and_timeout_review`, priority `65`, `requiresExplicitDecision=true`.
4. Maliyo: `timeout_or_network_budget`, priority `30`, `requiresExplicitDecision=false`.

Important interpretation:

- Super Lucky keeps output but the active registry row starts at `superluckycasino.com` and includes `stillfront.com` parent-career pages. Treat this as a source-policy/output-contract decision, not a mechanical speed fix; the benchmark now adds `sourcePolicyDecision` evidence with kept-output host breakdowns from `jobs-unified.json`.
- Koei keeps output and has timeout pressure plus cross-host `careerviet.vn` registry pages. Treat as combined source-scope and timeout review.
- The generic conflict workflow is now the source-policy soak report's `staticRegistryScopeConflicts` section. Use it before adding more per-source fixes; the decision record remains the current Super Lucky/Koei review note.
- Atvis remains a source-policy follow-up if fresh evidence reintroduces it as a kept-output conflict.
- Netflix is zero-kept `needs_review`; safer to review than kept-output sources, but existing persisted Netflix jobs mean avoid broad assumptions.
- Maliyo is the cleanest remaining mechanical timeout/network-budget target; the benchmark now captures timeout URL samples, timeout/network counts, URL role counts, and detail timing, and classifies slow productive static rows before any budget change.
- Before any source-policy, source-scope, timeout, registry, or output behavior change, inspect the benchmark's `sourceDecisionMatrix` JSON, generated `source-decision-matrix.md` companion report, and `source-decision-log-template.md` operator template. They are diagnostics-only review surfaces and keep the recommended first pass as `preserve_current_behavior`.

## Remaining work

Highest-value next work:

1. Use `sections.staticRegistryScopeConflicts` and `docs/plans/static-scope-conflict-dry-run-decisions.md` to review source-scope conflicts before any apply-safe registry edit.
2. Review Maliyo timeout diagnostics before lowering any budget.
3. Add a concise trend report that compares latest full lifecycle, storage-pressure, and static-outlier runs against previous artifacts.
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
python -m pytest tests/test_runtime_storage_metrics.py tests/test_perf_ci.py -q --color=no
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
- Benchmark artifacts identify runtime storage pressure, registry journal growth, sync snapshot headroom, and hot artifact budget drift before those become lifecycle or sync blockers.
- Kept-output source changes are clearly flagged with `requiresExplicitDecision=true` before implementation.
- Opt-in traces and startup metrics can explain Admin, Jobs, and Saved boot regressions.
- Static/full lifecycle benchmarks can produce comparable artifact summaries across runs.
- Remaining optimization work is a short prioritized queue, not a broad investigation.

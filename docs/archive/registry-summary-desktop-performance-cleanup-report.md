# Registry Summary And Desktop Performance Cleanup Report

> - **Status:** Archived cleanup report (no active follow-up); retained for June 2026 registry-summary diagnostics and desktop performance benchmark evidence
> - **Last updated:** 2026-08-28 (archived from `docs/plans/` — it is a closeout record, not a plan)

Date: 2026-06-05T00:09:40+02:00

## Summary

This no-version-bump cleanup added exact registry summary diagnostics and ran an evidence-first desktop performance pass. No release tag, live Umbrel update, schedule change, or live registry edit was performed.

## Registry Cleanup

- Added `GET /registry/summary?view=exact` for normalized count diagnostics without source rows.
- Kept default `GET /registry/summary` as a cheap storage snapshot with `summaryExact: false` and `countBasis: "storage"`.
- Marked combined `GET /registry/sources` summaries as `summaryExact: true` and `countBasis: "normalized"` because they already perform one full state load.
- Updated Admin diagnostics copy to distinguish `storage snapshot counts` from `normalized counts`.

## Verification

- `python -m pytest tests/bridge/test_routes_get.py tests/bridge/test_registry_summary_routes.py tests/bridge/test_routes_smoke.py tests/bridge/test_registry_service.py tests/admin/test_admin_bridge_registry_sync_confidence.py tests/test_release_docs.py -q`: passed, 65 tests.
- `node --test --test-reporter=dot tests/frontend/unit/admin-registry-controller.test.mjs tests/frontend/unit/admin-registry-discovery-review-controller.test.mjs tests/frontend/unit/admin-registry-sync-render.test.mjs`: passed.
- `node --test --test-reporter=dot tests/frontend/unit/admin-auth-controller.test.mjs tests/frontend/unit/admin-ops-controller.test.mjs tests/frontend/unit/admin-ops-readiness-shell.test.mjs`: passed.
- `npm run test:frontend:unit`: passed.
- `npm run test:refactor:changed`: passed.
- `npm run lint:precommit:changed`: passed.
- `git diff --check`: passed.

## Performance Evidence

### Packaged Startup

Command: `npm run perf:startup:jobs:pair`

- Jobs cold: `Launch -> First Usable UI` 2569ms, passed threshold 18000ms.
- Jobs warm: `Launch -> First Usable UI` 2311ms, passed threshold 12000ms.
- Jobs cold readiness split: `Launch -> Site Ready` 580ms, `Site Ready -> Bridge Ready` 1053ms, `Bridge Ready -> Window Created` 490ms.
- Jobs warm readiness split: `Launch -> Site Ready` 545ms, `Site Ready -> Bridge Ready` 1070ms, `Bridge Ready -> Window Created` 493ms.
- Bottleneck classification: desktop bridge startup delayed, but below thresholds.
- Reports: `.tmp/packaged-desktop-smoke-pair/20260604-220503-562178/cold-report.json` and `.tmp/packaged-desktop-smoke-pair/20260604-220503-562178/warm-report.json`.

Command: `npm run perf:startup:admin:pair`

- Admin cold: `Launch -> First Usable UI` 2459ms, passed threshold 18000ms.
- Admin warm: `Launch -> First Usable UI` 2436ms, passed threshold 12000ms.
- Admin cold readiness split: `Launch -> Site Ready` 561ms, `Site Ready -> Bridge Ready` 1052ms, `Bridge Ready -> Window Created` 519ms.
- Admin warm readiness split: `Launch -> Site Ready` 537ms, `Site Ready -> Bridge Ready` 1083ms, `Bridge Ready -> Window Created` 501ms.
- Admin first interactive after Admin Ready was 1ms cold and 0ms warm.
- Admin Ops Health first render was 1814ms cold and 1869ms warm after Admin Ready; this is non-blocking for first interactive.
- Bottleneck classification: admin operations health render delayed, but first usable UI remains well below thresholds.
- Reports: `.tmp/packaged-desktop-smoke-pair/20260604-220803-501180/admin-cold-report.json` and `.tmp/packaged-desktop-smoke-pair/20260604-220803-501180/admin-warm-report.json`.

### Frontend Page Boot

Command: `npm run test:frontend:perf`

- Jobs boot: navigation 115ms, DOMContentLoaded 114.8ms, load 115ms, FCP 52ms, zero long tasks.
- Admin boot: navigation 122ms, DOMContentLoaded 121.9ms, load 122ms, FCP 84ms, zero long tasks.
- Saved boot: navigation 101.1ms, DOMContentLoaded 101.1ms, load 101.1ms, FCP 40ms, zero long tasks.
- Jobs measures: `jobs_feed_fetch` 347.2ms, `jobs_render` 56.2ms, `jobs_startup_preview_fetch` 8.6ms.
- Admin measures: `admin_auth_init` 15.2ms, `admin_dom_cache` 1ms.
- Saved measures: `saved_auth_init` 1.3ms, `saved_boot` 3.5ms, `saved_dom_cache` 0.4ms.
- Trace artifacts: `_out/perf-traces/jobs-boot-trace.zip`, `_out/perf-traces/admin-boot-trace.zip`, `_out/perf-traces/saved-boot-trace.zip`.

## Follow-Up

- `npm run perf:complete` was not run because the focused startup probes and frontend perf traces passed without a concrete backend or desktop startup regression.
- The main observed Admin opportunity is Ops Health first render after Admin Ready. It is currently non-blocking for first interaction, but the old Admin boot path still displayed `Loading operations health...` until `/ops/dashboard-health` returned.

## Umbrel Admin Readiness Follow-Up

Live Umbrel samples from `http://192.168.50.61:8877/` showed lightweight Admin routes returning quickly while `/ops/dashboard-health` varied from roughly `0.85s` to `3.1s`. The route remained functional, but the visible Ops area could sit on placeholder copy for the full request duration.

Frontend hardening added a neutral Ops readiness shell before the first dashboard snapshot:

- Admin auth now renders the Ops shell during boot instead of sending `Loading operations health...`.
- The Ops controller no longer resets the trends area to `Loading operations health...` when there is no cached dashboard-health snapshot.
- Successful `/ops/dashboard-health` responses still patch in real health, schedule, KPI, task, registry, and diagnostics data.
- Failed first dashboard-health responses still render the explicit `Ops health unavailable: ...` state.

This is a readiness UX fix only. Backend `/ops/dashboard-health` performance, route contracts, Umbrel metadata, source sync, discovery, and fetcher behavior were not changed. If the dashboard-health route remains a user-visible bottleneck after this shell fix ships, split or profile the route in a separate backend-performance plan.

## Backend Profiling Instrumentation Follow-Up

Live Umbrel route timing samples taken after the readiness-shell work showed static pages and registry/sync summaries are fast, while Ops routes remain the likely backend bottleneck:

- `/jobs.html`, `/admin.html`, and `/saved.html`: median roughly 28-30ms.
- `/registry/summary`, `/registry/summary?view=exact`, and `/sync/status`: median roughly 46-69ms.
- `/ops/dashboard-health`: median roughly 891ms.
- `/ops/health`: median roughly 854ms with a 2600ms max sample.
- `/ops/task-state?view=summary`: median roughly 967ms with a 4505ms max sample.

The instrumentation patch adds `GET /ops/performance-profile`, backed by bounded in-memory route and backend operation timing aggregates. Route labels redact query strings and dynamic path segments, and no raw request payloads, raw samples, user IDs, file paths, or source rows are exposed. Admin now lazy-loads these diagnostics in Ops so future profiling can rank slow routes and sub-operations before optimization work begins.

### Profiling Instrumentation Verification

- `npm run test:frontend:perf`: passed, 3 page boot traces.
- `npm run perf:startup:jobs:pair`: passed.
  - Cold first usable UI 4555ms; bottleneck `jobs render delayed`.
  - Warm first usable UI 2444ms; bottleneck `desktop bridge startup delayed`.
  - Reports: `.tmp/packaged-desktop-smoke-pair/20260605-073602-410791/cold-report.json` and `.tmp/packaged-desktop-smoke-pair/20260605-073602-410791/warm-report.json`.
- `npm run perf:startup:admin:pair`: passed.
  - Cold first usable UI 3337ms; Ops Health first render 752ms after Admin Ready.
  - Warm first usable UI 2835ms; Ops Health first render 2004ms after Admin Ready.
  - Reports: `.tmp/packaged-desktop-smoke-pair/20260605-073929-352642/admin-cold-report.json` and `.tmp/packaged-desktop-smoke-pair/20260605-073929-352642/admin-warm-report.json`.
- `npm run perf:complete`: completed and wrote `_out/perf-complete/20260605-074009-027221/summary.json`.
  - Discovery median 1804ms; fetch median 6595ms; frontend boot aggregate 620ms.
  - Startup jobs cold/warm first usable UI 2653ms/2463ms.
  - Startup admin cold/warm first usable UI 2824ms/3459ms.
  - The complete-run Admin warm subprobe reported `startupProfileStatus: failed` from a small app-window creation threshold miss (`Bridge Ready -> Window Created` 603ms against the 600ms warm threshold).
- `npm run perf:startup:admin:warm`: rerun passed after the complete-run variance.
  - Warm first usable UI 2657ms; Ops Health first render 264ms after Admin Ready.

Current conclusion: the user-visible page shells are no longer blocked, but Ops backend routes remain the next optimization target. Use `/ops/performance-profile` on a running desktop or Umbrel instance to rank the slow substeps before splitting or caching `/ops/dashboard-health` or `/ops/task-state?view=summary`.

## Integrated Performance Report Follow-Up

The existing complete benchmark infrastructure is the right umbrella for backend profile evidence; no separate benchmark runner is needed.

- Packaged runtime snapshots now preserve `/ops/performance-profile` beside Ops health, startup metrics, storage metrics, and storage health.
- Startup profile-only probes capture `performance-profile.startup.json` before their early return, so Jobs/Admin cold-warm startup evidence includes bridge route and operation timings.
- Packaged sync rehearsal captures `performance-profile.post-sync.json` after sync push/pull and before runtime teardown.
- `scripts/perf_complete.py` now aggregates those snapshots into `benchmarks.bridgeProfile`, ranking top routes, top backend operations, error rows, and suspect Ops/readiness routes.
- The complete summary also emits `optimizationTargets`, combining startup stages, frontend boot traces, bridge profile timings, discovery/fetch medians, and sync push/pull timings into one action list.
- Optional live bridge sampling is available with `python scripts/perf_complete.py --bridge-base-url <url>`. The default `npm run perf:complete` remains local/reproducible and does not depend on Umbrel or LAN state.

These profile values are informational first. Do not add pass/fail thresholds until enough trend data exists across multiple runs.

## Optimization And Profiling Gap Closure Follow-Up

The complete benchmark report now closes the first profiling gaps found after the initial integration:

- Source-sync push results include additive `detailTiming` with nested stage totals and top stages. The bridge still preserves the existing `pushRemote` timing field, while `perf:complete` also emits `benchmarks.syncDetail`.
- Source-sync no-op pushes skip shard rebuild when the remote sharded manifest is already committed and the local/remote semantic fingerprints match; the remote snapshot format and source-sync contracts are unchanged.
- Jobs desktop update startup still performs one fresh silent update check, but the forced check is scheduled as non-critical post-interactive work after cached status mounts, keeping updater network work out of the first-usable path.
- Desktop startup traces now split bridge readiness into bridge spawn, process start, startup-ready wait, and window creation sub-stages. An opt-in `BALUFFO_STARTUP_PARALLEL_BRIDGE=1` startup-probe experiment can compare parallel bridge startup without changing the default launch order.
- `perf:complete` now elevates fetch `sourceTimingSignals`, slow provider boards, source-policy optimization targets, and source decision rows into `benchmarks.fetch.sourceTiming` and the ranked `optimizationTargets` list.
- Live bridge sampling is now bounded and reusable. `python scripts/perf_complete.py --bridge-base-url <url>` records the live sample inside the complete report, while `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url <url>` captures the live read-only sample alone.

Live bridge sampling remains read-only. It records route status, duration, content type, body size, top-level JSON keys, and the bounded `/ops/performance-profile` aggregate; it does not store full HTML bodies, source rows, task payloads, or mutate live Umbrel state.

### Startup Experiment Evidence

Default packaged startup after the new trace events:

- Jobs pair: cold/warm first usable UI `2578ms`/`2312ms`; bridge startup-ready wait was `1039ms`/`1038ms`.
- Admin pair: cold/warm first usable UI `2596ms`/`2479ms`; bridge startup-ready wait was `1040ms`/`1042ms`.

Opt-in `BALUFFO_STARTUP_PARALLEL_BRIDGE=1` startup-probe experiment:

- Jobs pair: cold/warm first usable UI `3763ms`/`1813ms`; bridge startup-ready wait dropped to `535ms`/`532ms`, but cold first usable UI regressed due render variance.
- Admin pair: cold/warm first usable UI `2322ms`/`2062ms`; bridge startup-ready wait dropped to `538ms`/`534ms`.

Decision: do not promote parallel bridge startup to the desktop default yet. The experiment proves the bridge wait can be overlapped, but the Jobs cold regression means it needs more repeated trend evidence before changing launch semantics.

### Integrated Report Verification

Final complete benchmark with live bridge flag:

- Command: `python scripts/perf_complete.py --bridge-base-url http://192.168.50.61:8877`.
- Summary: `_out/perf-complete/20260605-093024-001425/summary.json`.
- Discovery median `1612ms`; fetch median `6295ms`; frontend boot aggregate `136ms`.
- Startup first usable UI: Jobs cold/warm `2397ms`/`2409ms`; Admin cold/warm `2503ms`/`2607ms`.
- `POST /app/check-for-update` was no longer in the startup bridge top routes after the post-interactive updater deferral.
- Sync push `6043ms`; `benchmarks.syncDetail` identified `writeShardedSnapshot` as the dominant nested stage at `5649ms`, followed by `readRemoteSnapshot` at `365ms`.
- Fetch timing identified `lever_sources` as the slowest source family in all three fetch runs, with slow provider boards led by `Roof Games (Lever)`.
- Live Umbrel bridge sample was captured but all endpoints, including `jobs.html` and `admin.html`, timed out at the 3s per-route cap from this machine. The sample is valid as bounded failure evidence but not useful for route ranking until the LAN target responds.

### Readiness Verification

- `npm run perf:startup:admin:pair`: passed after the readiness-shell patch.
  - Admin cold: `Launch -> First Usable UI` 2882ms, `Admin Ready -> Ops Health First Render` 124ms.
  - Admin warm: `Launch -> First Usable UI` 2886ms, `Admin Ready -> Ops Health First Render` 142ms.
  - Warm run retained a non-blocking warning for `Bridge Ready -> Window Created` at 782ms against the 600ms warning threshold; total usable UI remained under the 12000ms threshold.
  - Reports: `.tmp/packaged-desktop-smoke-pair/20260604-223722-442817/admin-cold-report.json` and `.tmp/packaged-desktop-smoke-pair/20260604-223722-442817/admin-warm-report.json`.
- `npm run test:frontend:packaged:admin-startup`: passed after tightening the smoke to wait for the lightweight summary requests it asserts.
  - Report: `.tmp/packaged-desktop-smoke/20260604-224153-345081-345082600/report.json`.

## Cross-Page Readiness Placeholder Follow-Up

The same readiness-shell rule was extended across the user-facing page shells:

- Jobs now opens with the stable list header shell instead of `Loading jobs...`, keeps the Data Sources list quiet until metadata succeeds, and renders `Source metadata unavailable.` only after the metadata fetch fails.
- Saved Jobs now keeps the initial saved-list and activity bodies quiet until auth/profile/subscription state is known. Guest, local-auth-starting, restoring-profile, and real load-failure states remain explicit.
- Jobs and Saved shared Admin bridge footer buttons keep the neutral `Admin` label while checking; they switch to `Admin Online` or `Admin Offline` only after a real health result.
- Admin boot now clears overview, sync, fetcher/discovery log, Action Center, discovery summary, and discovery review placeholders during first background loads, while foreground/manual loading and error states remain visible.
- Shared Admin bridge footer buttons retry the current desktop `bridgePort`/session bridge base before declaring the Admin bridge offline, which keeps packaged cross-page navigation from sticking on the neutral `Admin` checking label when a stale default bridge base is present.

Focused verification passed:

- `node --test --test-reporter=dot tests/frontend/unit/jobs-html.test.mjs tests/frontend/unit/jobs-source-metadata.test.mjs tests/frontend/unit/jobs-admin-bridge-state.test.mjs tests/frontend/unit/saved-admin-bridge-state.test.mjs tests/frontend/unit/saved-runtime-controllers.test.mjs tests/frontend/unit/admin-auth-controller.test.mjs tests/frontend/unit/admin-registry-controller.test.mjs tests/frontend/unit/admin-ops-readiness-shell.test.mjs`.
- `node --test --test-reporter=dot tests/frontend/unit/admin-bridge-button-watcher.test.mjs tests/frontend/unit/jobs-html.test.mjs tests/frontend/unit/jobs-source-metadata.test.mjs tests/frontend/unit/jobs-admin-bridge-state.test.mjs tests/frontend/unit/saved-admin-bridge-state.test.mjs tests/frontend/unit/saved-runtime-controllers.test.mjs tests/frontend/unit/admin-auth-controller.test.mjs tests/frontend/unit/admin-registry-controller.test.mjs tests/frontend/unit/admin-ops-readiness-shell.test.mjs`.
- `npm run test:frontend:unit`: passed.
- `npm run test:frontend:perf`: passed.
- `npm run test:refactor:changed`: passed.
- `npm run lint:precommit:changed`: passed.
- `git diff --check`: passed with non-blocking CRLF normalization warnings.
- `npm run test:frontend:packaged`: passed for Jobs startup, Saved auth/navigation/custom job flow, Admin navigation, bridge badge, and discovery launch smoke.
- `npm run test:frontend:packaged:admin-startup`: passed.

Packaging note: packaged desktop smokes must run sequentially. Running generic packaged and Admin startup smokes in parallel races on `dist/baluffo-portable` rebuild/removal and can produce false runner failures.

## Performance Deep Dive Follow-Up

This no-version-bump follow-up keeps profiling diagnostic-first and does not change source-sync contracts, provider policy, startup defaults, Umbrel metadata, release tags, or live Umbrel data.

- Source-sync sharded pushes now include bounded `remoteTiming` evidence for shard PUT, shard verification GET, manifest PUT, and shard GC phases. `perf:complete` elevates these rows through `benchmarks.syncDetail.remoteOperationTop`, `remoteSlowestRequests`, and the ranked `optimizationTargets` list.
- Fetch timing aggregation now groups slow provider-board rows by source/adapter and reports status/cache-decision buckets. This keeps slow valid provider work separate from policy states such as `site_changed` or cache decisions.
- Live bridge sampling now records timeout, TCP connect, first-byte/header wait, full-response duration, status, content type, size, and failure phase. `scripts/perf_bridge_profile_snapshot.py --timeouts 3,10,30` is the read-only reachability diagnostic for live Umbrel or desktop bridges.
- `scripts/perf_startup_bridge_ab.py --pairs 5 --pages jobs,admin` runs repeated default vs `BALUFFO_STARTUP_PARALLEL_BRIDGE=1` probes and writes a promotion decision summary. The env flag remains diagnostic-only unless repeated evidence clears the improvement threshold without cold-start regressions.

Focused verification for this slice:

- `python -m py_compile scripts/perf_complete.py scripts/perf_bridge_profile_snapshot.py scripts/perf_startup_bridge_ab.py src/source_sync_shard.py src/source_sync_snapshot.py src/bridge/sync_service.py`: passed.
- `python -m pytest tests/test_source_sync_sharded_push.py tests/test_source_sync_shard_io.py tests/test_perf_complete_sync_detail.py tests/test_perf_complete_fetch_timing.py tests/test_perf_complete_bridge_profile.py tests/test_perf_startup_bridge_ab.py -q`: passed.

Live Umbrel reachability evidence from this machine:

- `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url http://192.168.50.61:8877 --timeouts 3`: all eight read-only endpoints failed during `tcp_connect` at roughly 3s; report `_out/perf-complete/live/20260605-101433-546361/bridge-profile/live/live-bridge-sample.json`.
- `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url http://192.168.50.61:8877 --timeouts 10`: all eight endpoints again failed during `tcp_connect` at roughly 10s; report `_out/perf-complete/live/20260605-101513-928476/bridge-profile/live/live-bridge-sample.json`.
- A direct 30s TCP-connect check to `192.168.50.61:8877` failed before HTTP with the Windows socket error that the connected party did not respond.
- `Test-Connection 192.168.50.61` succeeded and `Test-NetConnection 192.168.50.61 -Port 80` succeeded from source address `192.168.50.245` over `Ethernet`.
- `Test-NetConnection 192.168.50.61 -Port 8877` failed while ping still succeeded, confirming a port-specific reachability issue rather than a general route issue.
- The in-app browser was on `http://192.168.50.61/` with title `Umbrel`, but a background navigation to `http://192.168.50.61:8877/` timed out.

Conclusion: live route profiling is blocked because the raw-LAN app port is not reachable from this Windows host. This is not evidence that `/ops/dashboard-health`, `/ops/health`, or Baluffo static routes are slow on Umbrel; no HTTP connection is being established to the app port.

### Deep-Dive Benchmark Evidence

Latest local benchmark run:

- `npm run perf:startup:jobs:pair`: passed. Jobs cold/warm first usable UI was `2549ms`/`2345ms`; bridge startup-ready wait was roughly `1033-1053ms`.
- `npm run perf:startup:admin:pair`: passed. Admin cold/warm first usable UI was `2680ms`/`2572ms`; cold Ops Health first render was `1930ms` after Admin ready and warm Ops Health first render was `139ms`.
- `npm run test:frontend:perf`: passed.
- `npm run perf:complete`: passed and wrote `_out/perf-complete/20260605-102429-470039/summary.json`.
  - Discovery median `1654ms`; fetch median `5617ms`; frontend boot aggregate `141ms`.
  - Startup first usable UI: Jobs cold/warm `4413ms`/`2407ms`; Admin cold/warm `2875ms`/`2506ms`.
  - Sync push `5908ms`; sync pull `1092ms`.
  - Sync detail confirmed `writeShardedSnapshot` as the dominant nested stage at `5539ms`, with remote timing split mainly across `verifyShard` (`2579ms`) and `pushShard` (`2557ms`).
  - Fetch timing confirmed `lever_sources` as the slowest source family, with `greenhouse_boards` next.

Startup parallel-bridge A/B evidence:

- Jobs repeated A/B run passed. Default medians were cold/warm `8144ms`/`8062ms`; parallel medians were `7632ms`/`7595ms`, improving by `512ms`/`467ms`.
- Admin repeated A/B run did not pass. Default completed 5/5 samples with medians cold/warm `8358ms`/`8199ms`; parallel completed 0/5 samples and the packaged smoke reports missed the required Admin embedded runtime events.
- The A/B helper now refuses default-promotion recommendations unless both default and parallel arms complete all samples and report non-zero cold/warm medians.
- Decision: keep `BALUFFO_STARTUP_PARALLEL_BRIDGE=1` diagnostic-only. The Jobs result is encouraging, but Admin instability blocks promotion.

Incident note: during the failing Admin parallel probes, a Chromium profile dialog appeared. A process scan found no lingering Baluffo, Chrome, or Chromium processes afterward, and the smoke artifacts show normal browser process exit with missing Admin runtime events. Treat this as a benchmark/probe artifact unless it reproduces outside the parallel-bridge experiment.

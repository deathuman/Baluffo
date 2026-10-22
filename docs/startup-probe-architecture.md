# Startup Probe Architecture

> - **Status:** Active
> - **Use this when:** changing packaged startup timing, probe orchestration, launcher timing, or startup metrics
> - **Canonical for:** startup measurement ownership, event flow, command surface, and runtime-vs-probe boundaries
> - **Not canonical for:** general testing guidance or release sequencing
> - **Then inspect:** `src/ship/startup_telemetry.py`, `src/ship/desktop_app/{launcher_flow,launcher_diagnostics,launcher_recovery,startup_ready,startup_watchdog}.py`, `src/ship/desktop_app/{launcher,startup}.py`, `src/ship/desktop_app/browser.py`, `src/ship/startup_probe_policy.py`, `src/packaged_desktop_smoke.py`, and `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`
> - **Last updated:** 2026-10-21

This document is the single source of truth for Baluffo's packaged startup measurement architecture. Keep probe policy and startup-flow explanations here instead of repeating them in command docs or release notes.

## Canonical Commands

Use these repo-native entrypoints for packaged startup timing:

- `npm run perf:startup:cold`
- `npm run perf:startup:warm`
- `npm run perf:startup:pair`
- `npm run perf:startup:admin:cold`
- `npm run perf:startup:admin:warm`
- `npm run perf:startup:admin:pair`

The unqualified startup aliases keep measuring the Jobs page for compatibility. Use the `perf:startup:admin:*` aliases when validating the Admin non-blocking startup flow. The lower-level `probe:desktop:startup:*` commands exist, but docs should prefer the `perf:*` names unless a release checklist specifically needs the probe form.

## Ownership Boundaries

Baluffo's startup measurement stack has three layers:

1. **Runtime telemetry**
   - Lives in shipped code.
   - Owns startup event append/read helpers, URL readiness probes, and browser/page startup metric transport.
   - Always records launcher and child-runtime phases for desktop launches (see
     [Desktop-lite startup tracing](#desktop-lite-startup-tracing)); browser/page
     metrics stay probe-gated.
   - Primary code: `src/ship/startup_telemetry.py`, bridge startup-metric storage, browser-side startup metric hooks.

2. **Desktop launcher runtime**
   - Lives in shipped code.
   - Owns site/bridge launch ordering, browser launch acceptance, handoff, reveal detection, and runtime readiness.
   - Should depend on small telemetry helpers, not probe-policy decisions.
   - Primary code: `src/ship/desktop_app/{launcher_flow,launcher_diagnostics,launcher_recovery,startup_ready,startup_watchdog}.py`, `src/ship/desktop_app/{launcher,startup}.py`, `src/ship/desktop_app/browser.py`.

3. **Startup probe policy + analysis**
   - Probe-only logic.
   - Owns required event sets, strict managed-browser policy, probe failure categorization, and startup summary refinement.
   - Should not leak into normal desktop behavior beyond simple config flags passed into the launcher/runtime.
   - Primary code: `src/ship/startup_probe_policy.py`, `src/ship/startup_profile.py`, `src/packaged_desktop_smoke.py`, and `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`.

## Desktop-Lite Startup Tracing

A field stall is only diagnosable if the child runtimes report where they are, so
shipped desktop launches write `desktop_*` phase rows into
`desktop-startup-metrics.jsonl` without any probe flag:

- `startup_telemetry.desktop_startup_trace_enabled()` gates child-runtime tracing.
  It is on for `BALUFFO_STARTUP_PROBE=1`, for
  `BALUFFO_DESKTOP_STARTUP_TRACE=1`, and by default for a desktop launch
  (`BALUFFO_DESKTOP_MODE=1` with `BALUFFO_DATA_DIR`).
- `BALUFFO_DESKTOP_STARTUP_TRACE_DISABLED=1` turns it off again; the probe flag
  still wins, so probe runs never lose evidence.
- The journal rotates once at `STARTUP_METRIC_MAX_BYTES` (24 MiB) to
  `desktop-startup-metrics.jsonl.1`, best effort, so always-on tracing cannot
  grow the runtime data directory without a bound.
- Browser/page events (`jobs_*`, `saved_*`, `admin_*`) remain probe-gated; the
  launcher's handoff signals consume them when present.

Bridge phase events emitted on the desktop-shipped path:

| Event | Emitted by | Meaning |
| --- | --- | --- |
| `desktop_bridge_child_start` | `runtime_launcher.run_bridge_server` | Bridge child process entered the launcher shim. |
| `desktop_bridge_entrypoint_start` | `runtime_launcher.run_bridge_server` | Layout resolve, repair, startup check, and quarantine finished; `bootMs` is the shim cost. |
| `desktop_bridge_configured` | `admin_bridge.main` | Runtime paths configured; carries `databaseBytes`, `walBytes`, and `storageMaintenanceDeferred`. |
| `desktop_bridge_sync_config_refreshed` | `admin_bridge.main` | Sync config load finished. |
| `desktop_bridge_registry_ready` | `admin_bridge.main` | `ensure_active_registry()` finished. |
| `desktop_bridge_api_built` | `admin_bridge.main` | Bridge API object built. |
| `desktop_bridge_socket_bound` | `bridge/server/httpd.py` | Listener bound; the port answers connection attempts from here. |
| `desktop_bridge_accept_loop_started` | `bridge/server/httpd.py` | Requests are served from here; `on_started` work runs on its own thread. |
| `desktop_bridge_first_request_served` | `bridge/server/httpd.py` | First accepted connection after bind. |
| `desktop_bridge_started_hooks_started` / `_finished` | `bridge/server/httpd.py` | Post-bind startup work bracket; `_finished` carries `elapsedMs`. |
| `desktop_bridge_first_paint_barrier_opened` | `bridge/startup_work_budget.py` | The first-paint work budget was satisfied, so heavy startup work may begin. Carries `reason` (`request-quiet`, `budget-expired`, or `wait-timeout`), `waitedMs` since bind, `acceptedRequests`, and the configured `quietMs`/`budgetMs`. |
| `desktop_bridge_startup_work_admitted` | `admin_entrypoint_runtime.finish_bridge_startup_hooks` | The post-bind startup chain was released by the budget; carries `admission` and the barrier's `reason`/`waitedMs`. A `reason` of `wait-timeout` means the budget, not the page, admitted it. |
| `desktop_bridge_deferred_startup_work_started` / `_finished` | `bridge/startup_work_budget.py` | One queued background item; `label` names it (currently `availability_overdue_catchup`), `_finished` carries `durationMs`, `ok`, and `error`. |
| `desktop_bridge_startup_step_started` / `_finished` | `admin_task_runtime.on_bridge_started` | One row per startup step, with `step` and (on finish) `durationMs`. A step that never returns leaves its `_started` row as the tail. The first step, `compaction_hold_seed`, also carries `seededFromActiveTask`, `held`, `holdChanged`, `activeTaskCount`, `activeTaskTypes`, and `activeRunIds` on its `_finished` row. |
| `desktop_bridge_storage_maintenance_resumed` / `_resume_failed` | `admin_entrypoint_runtime.finish_bridge_startup_hooks` | SQLite maintenance released after the post-bind startup work; carries `stores` and `scheduled`, or `error` when the release failed. "Released" is not "rebuilt": a released VACUUM is still held while a task row is active (`held-active-task`, released by the last task's terminal transition) and then waits for a quiet write path, reporting `skipped-busy` until one arrives (see [`storage-contract.md`](storage-contract.md#maintenance-scheduling-rules)). The held state itself is reported as `storageMaintenanceDeferred` on `desktop_bridge_configured` and as `deferred` in `/ops/storage-health`. |

Layout/site startup events outside the bridge child:

| Event | Emitted by | Meaning |
| --- | --- | --- |
| `desktop_launch_jobs_report_gate` | `runtime_launcher.quarantine_stale_jobs_row_artifacts` | The jobs success gate ran because row artifacts existed. `reader` is `summary-sidecar` (sidecar plus an 8 kB run-identity prefix) or `full-report` (the fallback that parses the whole report); the row also carries `process` (`site` or `bridge`, the launch half that consulted the gate), `success`, `durationMs`, `reportRunId`, `reportFinishedAt`, and `reportMetadataOk`/`feedArtifactsOk`. Emitted once per startup that consults the gate, in both the site and bridge processes. Packaged smoke fails the run when any row reports `full-report` (record-only mode does not relax it), and also when only one process journals either this row or the skipped row below, or when the rows carry no `process`, so a half that stops reporting cannot hide a sidecar regression. The jobs-pipeline fixture seeds a matching sidecar so the fixture itself takes the fast path; that lane requires a reader row from both processes, since a skip there would mean the fixture never exercised the sidecar. |
| `desktop_launch_jobs_report_gate_skipped` | `runtime_launcher.quarantine_stale_jobs_row_artifacts` | The process checked a data dir and found no jobs row artifacts to read. Carries `process`, `dataDir`, `reason` (`no_artifacts`, or `restored_false_quarantined_runtime_artifacts` when a false quarantine was undone), and `restored`. Emitted from the same call site as the gate row, so a start with nothing to read still journals one row per process and data dir; the smoke counts it as that half reporting, which is what separates "no artifacts to read" from a journal that stopped. |
| `jobs_row_artifact_quarantine_failed` | `runtime_launcher.safe_quarantine_stale_jobs_row_artifacts` | The quarantine raised (caught `RuntimeError`/`OSError`/`shutil.Error`/`TypeError`/`ValueError`) and the launch continued without it. Carries `process`, `dataDir`, and `error`. Packaged smoke fails the run as scenario `jobs-row-artifact-quarantine` (step `jobs-row-artifact-quarantine`) listing every crashed half with its own recorded error, rather than reporting those halves as missing gate rows; `--profile-record-only` does not relax it. |

`desktop_bridge_startup_step_*` steps are `compaction_hold_seed`,
`runtime_evidence_journal_cleanup`, `registry_journal_compaction`,
`stale_task_cleanup`, `startup_sync_schedule`, and `pipeline_schedule_polling`.
The whole chain is expected in the low hundreds of milliseconds; a step that
reports seconds is a stall, not normal work. `startup_sync_schedule` used to be
the outlier at 12–14 s: the sync run's lifecycle transition mirrored the bounded
`taskRuns` projection into SQLite one commit per row (~90 ms each on a mature
runtime database). It now mirrors in a single transaction, so the same step
measures ~0.2 s — see
[`storage-contract.md`](storage-contract.md#migration-safety). Note that page
first render never waits on this chain: the jobs page paints from the startup
snapshot as soon as the site serves it, and the bridge steps run on their own
thread behind the accept loop.

`compaction_hold_seed` runs before every other step (and before the post-bind
maintenance release) and re-derives the rebuild hold from the task lifecycle
ledger. Its `_finished` row is the startup-journal answer to "was a rebuild held
because a task was already running when this bridge started?":
`seededFromActiveTask` is true when the ledger had an active row,
`activeTaskTypes`/`activeRunIds` name it, and `held` is the resulting hold state.
Without that row a launch-time held rebuild is only visible later as
`compaction.status: "held-active-task"` in `/ops/storage-health`.

## Packaged Bridge Spawn Window

`desktop_bridge_process_started` -> `desktop_bridge_child_start` is the frozen
child's own cold start. `Popen` itself returns in ~2 ms; the rest is the
PyInstaller bundle booting and importing the desktop-app entry chain before
`run_bridge_server` runs. A 2026-10-18 profile, using trivial-entry control exes
built from the same spec to isolate each phase, measured:

| Phase | Cost |
| --- | --- |
| `Popen` (parent side) | ~2 ms |
| Bootloader + interpreter to the first runtime hook | ~40-48 ms |
| PyInstaller's regular runtime hooks + entry-script import | ~125-130 ms |
| `desktop_app` / `launcher` / `runtime_launcher` imports | ~40 ms |

The runtime-hook phase was dominated by `pyi_rth_setuptools`, which imports
`setuptools` before the entry script while nothing else in the graph imports it,
so the hook was feeding itself. Excluding `setuptools` and `_distutils_hack`
from the portable build drops the hook: the window measured 235 ms -> 148 ms
(median, packaged `Baluffo.exe __child_bridge__`), and the launcher's own trace
224 ms -> 140 ms. `Site Ready -> Bridge Ready` did not move because the spawn is
only about a fifth of that stage; the rest is the
`desktop_bridge_entrypoint_start` -> `desktop_bridge_configured` import of the
ship-version `admin_bridge.py` and the readiness poll.

### Disk-loaded bridge import

`run_bridge_server` runs `runpy.run_path` on the ship version's `admin_bridge.py`
with `sys.path` pointed at that version directory, so the bridge stack is
imported from disk rather than from the frozen PYZ. PyInstaller's bootstrap sets
`sys.dont_write_bytecode`, so a packaged run cannot cache that compilation: on a
clean install the phase recompiles its whole disk import closure on every
launch. Mark-instrumented `admin_bridge.py` on the 2026-10-18 portable build
(median of 3 runs) measured:

| Shipped bytecode | `entrypoint_start` -> `configured` |
| --- | --- |
| None (clean install before the fix) | ~719-740 ms |
| Timestamp-keyed `__pycache__` | ~302 ms |
| Hash-keyed, checked | ~342 ms |
| Hash-keyed, unchecked (now shipped) | ~297-305 ms |

`build_ship_bundle.build_bundle` now compiles `app/versions/<version>/src/**`
into `__pycache__` with `UNCHECKED_HASH` invalidation, after deleting any
`__pycache__` copied from the developer checkout. Timestamp-keyed bytecode would
not survive delivery because the updater extracts version directories with
fresh mtimes; unchecked hashes are safe here because a version directory's
sources are immutable once built, since updates land in a new version directory.
The generated bytecode also keeps `co_filename` relative to the version root, so
it does not depend on the build machine's checkout path.

With that in place the cold jobs probe measured `Site Ready -> Bridge Ready`
1082 ms -> 581 ms, and the import phase 384 ms -> 240 ms in that run.

Executing the bytecode is what is left, and the closure is what sets its price.
Precompiled-but-eager the shipped bridge executed 590 disk modules; the modules
the bridge only needs once a request (or a task) asks for them were 145 of those.
Deferring them — the source-discovery and jobs-adapter closures, the jobs-feed
and lifecycle-state helpers, `source_sync` and its shard/crypto closure, the job
HTML parsers and jobs transport, `psutil`, and `importlib.resources` — brings
the packaged run to 445 modules and `entrypoint_start` -> `configured` to a
median 113 ms (four runs: 108-127 ms) on the same machine, with the smoke
harnesses for discovery, conflict adjudication, source check, `/sync/status`,
and `/ops/storage-health` all passing against the built bundle. The rest of the
launch is unchanged: the frozen bundle's own boot is ~40 ms, the accept loop
starts ~6 ms after the bridge is configured, and the readiness poll that follows
first-request-served is ~160 ms, so the same change moves
`Site Ready -> Bridge Ready` by roughly a third of the import saving.

Deferral only stays honest if the moved names keep their shape. Names that answer
a root attribute, such as `admin_bridge._html_extractor` or
`admin_bridge.source_sync_module`, stay in
`src.bridge.admin_root_lazy_exports.LAZY_ROOT_EXPORTS`, which is what the module
`__getattr__` consults. Names that tests patch on the importing module — the
lifecycle writers in `job_availability_service`, for example — stay real
module-level callables, resolved through that module's `_deferred()` helper on
first call, because a function-body import would move the patch point and leave
the test exercising nothing.

## Parallel Spawn And Feed Presence

Two launch costs that multiplied rather than added are gone as of 2026-10-19.

**Parallel spawn.** The site and bridge used to start serially: the bridge was
spawned only after `desktop_site_ready`, so its whole cold start sat behind the
site's. Both bind ports that `ensure_runtime_ports` checked before either spawns,
so nothing in the bridge depends on the site; the window still waits for the
slower of the two. The launcher now spawns the bridge immediately after the site
and records `desktop_bridge_spawn_parallel_with_site` (mode `parallel`).
`BALUFFO_STARTUP_PARALLEL_BRIDGE=0` restores the serial path, which also remains
reachable through `desktop_bridge_spawn_deferred_until_site_ready`. The port-race
retry terminates both children before respawning, so a retried launch still runs
exactly one bridge.

**Cheap feed presence.** `jobs_cold_start_required` only needs to know whether
`jobs-unified-light.json.gz` and `jobs-unified.json.gz` contain at least one row,
but it answered by decompressing and parsing both files in full. On the live
41,916-row payload (43 MB uncompressed, 10.6 MB gzipped) one call cost
1,587-1,766 ms (three samples, warm page cache), paid on every cold-start path
that asks: the launcher's config build, the site child, and the bridge child. The
check now reads a bounded 256 KB decompressed prefix under `gzip.open` and
decides from the opening token (`[` / `{"jobs": [`), falling back to the full
parse whenever the prefix cannot settle the answer. The same call now costs
1.1-1.6 ms, and the result matches the full parse on the live files.

Packaged `Baluffo.exe` launches on the same machine. The probe below runs with
`--profile-only`, whose runtime data directory carries no feed artifacts, so it
measures the spawn path and not the feed check; the live-data launch seeds the
four live feed/report files into an otherwise empty data directory and measures
both.

| Event | Old path (recorded earlier) | Live-data launch (new) | Cold jobs probe (new) |
| --- | --- | --- | --- |
| Launch -> site spawned | serial, after the feed parse | 7 ms | 7-8 ms |
| Launch -> site ready | ~3.0 s (site child parsed both feeds) | 565 ms | 551 ms |
| Launch -> bridge spawn | after site ready | 10 ms | 10 ms |
| Launch -> bridge ready | not recorded | 1,234 ms | 693-761 ms |
| Launch -> window | 5,978 ms (last recorded real launch) | 1,235 ms | 1,234-1,334 ms |
| Launch -> first usable UI | 6.1 s (page clock, same launch) | not measured (no browser) | 2,327 ms |

The probe reports `Site Ready -> Bridge Spawn Started` 0 ms and
`Site Ready -> Bridge Ready` 143 ms, and passes every threshold. The window now
waits on the bridge's readiness gate, not on the site finishing first: in the
live-data launch that gate ran 565 -> 1,234 ms (`desktop_bridge_ready_before_window`
landed 1,234 ms), while the bridge itself had served its first request at 480 ms
of its own clock. The readiness poll's interval, not the spawn order, is what is
left in that gap.

## Window Gate And Serve-First Startup

The desktop window no longer waits for the bridge's full startup chain.

- The launcher waits `BRIDGE_GATE_TIMEOUT_S` (4 s) after spawning the bridge. A
  bridge that reaches `startupReady` inside that budget behaves exactly as before
  and emits `desktop_bridge_ready_before_window`.
- If the gate expires, the launcher emits `desktop_bridge_gate_deferred` and opens
  the window anyway, then emits `desktop_window_open_bridge_deferred`. The page
  starts its own bridge bootstrap retries (`DESKTOP_BOOTSTRAP_RETRY_WINDOW_MS`,
  25 s) and shows bridge state instead of an empty page; readiness is completed in
  the background by `publish_success_marker_when_ready_async`.
- `config.startup_probe` or `BALUFFO_DESKTOP_STRICT_STARTUP=1` restores the old
  block-until-ready behavior (`READY_TIMEOUT_S`) and on timeout writes
  `desktop-bridge-startup-timeout.txt` plus raises
  `DesktopStartupReadyTimeout`.
- Readiness classification distinguishes `bridge_unbound` (nothing listening) from
  `bridge_bound_not_accepting` (the port completes TCP handshakes but the health
  endpoint does not answer), and a classified timeout carries the newest
  `desktop_bridge_*` phase rows as `startupPhases` in the exception payload.
- Both timeout paths render those phases through
  `format_startup_timeout_diagnostics`: the strict launcher gate, and the
  background success-marker worker that outlives a deferred window
  (`desktop_bridge_startup_timeout`, also recorded in the startup journal).

The bridge serves before its post-bind startup work: `on_started` runs on
`baluffo-bridge-on-started` after the accept loop starts, so a slow journal,
stale-task, or sync-scheduling step delays those side effects, not the page. It
also no longer *competes* with the first page: see the work budget below.

## First-Paint Work Budget

Heavy startup work is admitted through `bridge/startup_work_budget.py` instead of
starting the moment the API object exists. Before the budget, the overdue
availability catch-up forked ten workers at API construction (pre-listen), and
the post-bind hook chain began immediately, so those parses and store writes ran
while the page issued its first request burst and the request threads queued
behind them.

The acceptance rule is deliberately simple: the accept loop already sees every
arrival, so it feeds the cumulative `acceptedCount` to the budget, and the
barrier opens once the count has stalled for `quietMs` (350 ms, above the accept
loop's 0.25 s poll) — or once `budgetMs` (2 s) elapses, whichever comes first. The
second rule is what keeps a launch with no page (headless, container smoke) from
stalling. A connection with no request also counts as an arrival: the budget
tracks clients, not parsed routes.

Admission has two consequences:

- Queue work runs after the barrier. `availability_overdue_catchup` is the
  current occupant; it is serialized on `baluffo-startup-work`, so the heavy
  lifecycle/registry parse happens once and never in parallel with itself.
- The post-bind startup chain waits for the barrier before its first step, bounded
  by the same budget. Its journal rows therefore start later than
  `desktop_bridge_socket_bound` *by design*; `desktop_bridge_startup_work_admitted`
  records which side of the barrier released it.

The downloaded portable artifact's digest follows the same rule from the other
direction: it is warmed in the background when the API is built, and a status
poll waits for it for at most `ARTIFACT_VERIFY_BUDGET_MS` (200 ms) before
answering from the last known status. An unverified artifact is never claimed,
and the install gate still verifies fully before staging.

`BALUFFO_STARTUP_WORK_BUDGET_MS` overrides the budget for diagnostics: `0`
admits heavy work at bind (the pre-budget behavior, useful as a control run) and a
large value holds it back longer.

## What Stays in the Shipped Package

Do **not** remove startup telemetry from the regular package entirely.

The packaged startup probes are only trustworthy because they measure the real shipped app path. That means the following stay in product code:

- launcher startup trace events
- browser/page startup metric hooks
- bridge persistence for startup metrics
- runtime URL readiness probing

What should stay out of the normal launcher path:

- strict startup-probe browser selection policy
- startup-probe required event lists
- probe-only pass/fail classification
- packaged-smoke artifact/report orchestration

Keep `src/packaged_desktop_smoke.py` as the CLI and root patch surface. Put concrete packaged-smoke helper changes in `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py` unless the command surface itself needs to change.

## Event Flow

The packaged startup measurement path is:

1. The desktop launcher emits runtime startup events.
2. Browser pages emit startup metrics once `startupProbe=1` enables the browser-side transport.
3. The bridge stores those browser-origin metrics under the runtime data directory.
4. Jobs startup profiles wait for `jobs_auth_ready`, `jobs_first_render`, and `jobs_first_interactive`; Admin startup profiles use `admin_ready`, `admin_first_interactive`, and `admin_ops_health_first_render` for the visible Operations Health path.
5. Packaged smoke collects startup metrics from the runtime data and bridge endpoints.
6. Startup profile analysis renders the stage timings and summary report. The summary also carries `jobsReportGate` (`quarantineFailureCount`, `quarantineFailureProcesses`) so a release report states whether the gate wrapper crashed and in which halves, even when the smoke failure payload is the only other place that error appears.

When `desktop_bridge_ready_before_window` is present, startup profile analysis splits the pre-window segment into `Site Ready -> Bridge Ready` and `Bridge Ready -> Window Created`. Older or incomplete traces without that event fall back to the legacy `Site Ready -> Window Created` stage. A deferred launch instead records `desktop_window_open_bridge_deferred`, which marks the window as opened before bridge readiness by design.

Important event families:

- launcher/runtime events: `desktop_*`
- browser/page events: `jobs_*`, `saved_*`, `admin_*`
- report outputs: `startup-profile-summary.json`, packaged smoke `report.json`

## Page Clock, Page Spans, And Probe Attempts

Page rows carry three clocks, and only one of them is trustworthy for ordering:

- `ts` is the **bridge arrival** time. A queued startup-metric POST can delay it by
  hundreds of milliseconds, so it must not order page events.
- `browserCreatedAtMs` is the older per-emit wall clock. Rows written before the
  page clock existed still fall back to it.
- `payload.pageTsMs` is `performance.timeOrigin + performance.now()` at emit time:
  epoch milliseconds, directly comparable with the launcher's `ts`. The bridge
  promotes it to the row's top-level `browserTsMs`, which is what
  `startup_profile` already preferred for placement. `jobs` and `admin` startups
  stamp it on every row (`frontend/*/app/runtime/effects.js` via the shared
  `pageClockMs` helper), and probe-origin rows stamp `browserCreatedAtMs` from the
  same clock in `probes/startup-probe.js`.

Without that page clock a page event lands at page-boot time on the launch
timeline, and a stage whose end precedes its start is clamped to zero. The summary
therefore also carries an additive `pageStages` block (`Page Boot -> Local Data
Ready`, `Local Data Ready -> First Render`, `Fetch-Report Probe`,
`Fetch-Report Probe -> First-Run Gate`, `Local Data Ready -> Auth Ready`,
`Page Boot -> First Interactive`). Those spans read page events only, keep
`negativeGap`, and are printed under `Page-clock spans:`; they are evidence, not
gates (the launch stage table and its thresholds are unchanged).

`classification` is first chosen from the launch stage table (the slowest stage
marked `slow`, else the slowest stage overall). A page span may **re-attribute**
that verdict when its interval *covers the blamed stage's whole interval* on the
same clock and is longer: the summary then names the span (`jobs report probe
delayed` rather than `local auth bootstrap delayed` for the stage that merely
neighbours the wait) and records the evidence in an additive `bottleneck` object
(`source`: `page` / `launch`, `key`, `label`, `startEvent`, `endEvent`,
`durationMs`, and for a page span `covers`: the stage key it replaced).

Every page profile carries both fields -- `jobs`, `admin`, and the `desktop-probe*`
pages -- so a consumer can rely on `pageStages` and `bottleneck` being present. A
probe page emits no page-boot or local-data events, so its spans report
`status: unavailable` and its bottleneck stays `launch`: the empty evidence is
stated rather than absent.

Three guards keep the refinement from inventing work:

- Only the **tightest** covering span wins. `Local Data Ready -> First Render`
  (806 ms in a cold launch) covers the same interval as the 805 ms report probe
  inside it, and the narrower span is the one that names real work.
- A span that **starts at the blamed stage's own start event** is that stage
  re-measured, not evidence about it, so it never wins.
- A span whose end precedes its start (`negativeGap`) owns no time, and the `ok` /
  `metrics incomplete` / `insufficient data` verdicts are never overridden -- a page
  span re-attributes a named cause, it does not create one. The page total
  (`Page Boot -> First Interactive`) is excluded for the same reason the launch
  total is: it would name the whole page for any stage inside it.

The jobs page also emits `jobs_report_probe_start`, one
`jobs_report_probe_candidate` per attempted URL (`url`, `status`, `ok`, `ms`), and
`jobs_report_probe_done` (`ms`, `found`, `candidateCount`) around
`fetchJobsReport`. The report probe sits on the first-render path, and before this
span that wait was absorbed into the launch total with no owner.

`ms` alone cannot say whether the listener answered late or the answer sat in the
browser, so each candidate row also carries `visibility`
(`document.visibilityState`), `resourceStartMs`, `resourceResponseStartMs`,
`resourceResponseEndMs`, and `resourceConnectMs` from the browser's own Resource
Timing entry (`null` when it is unavailable, or when the row is written before the
body finished). A row whose `ms` is large while the site trace shows a
single-digit `durationMs`/`flushMs` is a browser-side wait, not a site defect.

`wait_for_url` records `desktop_url_probe_attempt` for **every** attempt
(`attempt`, `ok`, `status`, `durationMs`, `sinceStartMs`, `error`), not only the
first failure. A first attempt that blocks 500 ms inside `getresponse()` and then
succeeds is a different defect from a listener that was not bound, and the old
trace could not tell them apart.

Two rules keep the probe from billing a client-side stall to the site (both held
by `tests/test_startup_probe_readiness.py`):

- The loopback probe bounds **only** the connect (`connectMs`) with
  `LOOPBACK_PROBE_CONNECT_TIMEOUT_S` (100 ms) and keeps the full request budget for
  the answer, so a stalled first connect is retried instead of reported as a slow
  site.
- `launch_desktop_app` starts `warm_loopback_connect` on a side thread right after
  the site child spawns, so the process's first loopback connect (image and filter
  evaluation in a freshly written executable) is paid before the probe needs it.
  `desktop_site_connect_warmed` marks when that connection landed.

Site responses are traced on both sides of the write: `desktop_site_request_complete`
stops when the handler has written the body, and `desktop_site_request_flushed`
(`durationMs`, `flushMs`) stops after the response was flushed to the socket. When
a page row reports a long `ms` for a request whose `flushMs` is `0`, the delay is
in the browser's delivery of that response -- check `visibility`, the resource
timing fields, and the page's own timer rows (`jobs_auth_ready` fires on schedule
in those runs) before touching the site server.

## Page-Side Hoist For The First-Render Requests

`jobs.html` loads `frontend/jobs/early-data.js` as a classic script ahead of the
module entry. It starts the two requests the first render waits for -- the
startup snapshot (`data/jobs-unified-startup.json`) and, in the desktop app, the
fetch report (`data/jobs-fetch-report.json`) -- while the browser is still
fetching the boot modules (~115 files in a cold packaged page load).

Issued from the graph instead, both requests queue behind that burst. Profiled
cold starts spent 0.96--1.12 s inside the Fetch-Report Probe span on a file the
site answered in 2--3 ms, with the module graph itself taking ~1.7 s before
`jobs_page_boot_start`. `data-source.js` takes each hoisted response with
`take(url)` and otherwise fetches on the caller's own clock, so the hoist is an
optimisation with no correctness role: an unused, consumed, failed, or
stale-URL hoist just falls back to the previous path.

Adoption is visible in the trace as `hoisted: true` on the
`jobs_report_probe_candidate` row. On a hoisted row `ms` measures only the
hand-off from boot to the already-started request, so it is near zero by
construction -- read `resourceResponseStartMs` / `resourceResponseEndMs` for the
real network window, and the page-clock spans for the wait that disappeared.

The report is also not read on the first-render path. `initJobsFeed` starts the
probe and paints the startup snapshot; the first-run gate is evaluated when the
report lands, so a page that already has rows never waits for it -- with the
report held back by 800 ms, rows still appeared at ~205 ms. When that late
verdict asks for a bootstrap it runs *behind* the rendered rows: the empty
`first_run_bootstrap` state and its `Preparing first-run jobs` modal are skipped
there (they belong to a launch that has nothing to show), the status line carries
the explanation, and the feed refreshes when the bootstrap completes. The trace
marks the case with `late: true` and `rowsRendered` on
`jobs_first_run_gate_evaluated`, plus `jobs_first_run_gate_applied_late`.

A page with no snapshot still awaits the report before deciding, because there
there the verdict *is* the first paint: that is the launch that shows the bootstrap
state, and it stays exactly as it was.

### The container page inlines the hoist

The container jobs page cannot keep the classic tag: it ships one bundled asset
and no module files, so the build inlines `early-data.js` into `jobs.html` with
the same dataset contract (`scripts/build_container_frontend.mjs`). Skipping it is not
free, because there is no other head start to fall back on -- the snapshot
request would wait for the single asset's download and parse.

Measured through the real container handler against an isolated data dir, with
the same built asset on both sides and only the inline hoist toggled (readiness =
first job row, `data/jobs-unified-startup.json` deleted before each run for the
cold rows):

| condition | hoist stripped | hoist inlined | snapshot request starts at |
| --- | --- | --- | --- |
| cold snapshot, 1x (n=8) | 760 ms | 701 ms | 83 ms → 21 ms |
| cold snapshot, 4x (n=6) | 1,150 ms | 921 ms | 373 ms → 39 ms |
| warm snapshot, 1x (n=6) | 106 ms | 92 ms | 86 ms → 47 ms |
| warm snapshot, 4x (n=6) | 434 ms | 438 ms | 355 ms → 46 ms |

What that means, and what it does not: the gain is the window between parse and
module evaluation, so it scales with how long the graph takes to run -- 59 ms at
1x, 229 ms at 4x on the cold rows. The warm 4x row is flat on purpose: there the
asset parse (~430 ms) outlasts the entire snapshot wait, so no hoist can move the
first row; at that point the bundle itself is the critical path. The remaining
cold cost is server-side, not page-side: when the startup feed is missing the
route regenerates it from the light feed (~660 ms, the 0.96--1.12 s span above).
A normal fetch run writes `jobs-unified-startup.json` at finalize, so that path is
the compatibility case, not the steady state.

The build refuses to ship without the hoist: `inlineEarlyDataScript` throws for
the jobs page when the classic tag is gone, instead of returning the page
unchanged the way it does for admin and saved. The desktop-only update surface is
replaced by an inert stub in the same build, and the page boots a composition
that never imports the offline client (328,836 → 253,681 bytes, gzip 96,337 →
74,993; see [One local-data client per runtime](#one-local-data-client-per-runtime)),
which is what makes the container page's single asset parse as fast as it does;
`tests/frontend/unit/container-frontend-bundle.test.mjs` drives the real bundler
for all of it.

### The container admin page is split; the jobs page is not

A container page ships one asset per page, so a surface the page reaches only
after its first paint is parsed before that paint unless the build splits it.
Admin is where that pays:
the Ops surface is 273,122 bytes and nothing on the first paint touches it, so
the shell -- the entry plus the static chunk the HTML preloads -- is 303,552
bytes instead of the 578,398-byte single asset. The split adds a shared chunk of
86,118 bytes that the entry still imports (and the HTML preloads, so it is not
discovered only after the entry parses), and the Ops chunk is fetched when the
Ops section loads. Verified against the real container handler: entry, preloaded
chunk and Ops chunk all 200 from `/container-assets/assets/`, and the Ops panels
hydrate with no page errors.

Jobs is the measured-negative case and stays one asset, although the desktop
build splits it. The only surface there is the pipeline controller: splitting it
moves 5% of the shell onto a second request and the first row does not move. The
table below is that comparison, measured on the build before the local-data
compositions landed (which removed the same offline client from both arms):

| jobs page | shell | gzip on the critical path | first row, 4x (n=10 pairs) |
| --- | --- | --- | --- |
| one asset | 295,169 B | 86,877 B, one request | 394 ms |
| split | 280,135 B | 83,274 B, two requests | 394 ms |

The split is not free: the controller shares `app/pipeline.js` with the shell, so
esbuild lifts that into a third asset the entry imports statically. Over ten
alternating pairs the medians were 394 ms and 394 ms (paired median 4 ms), and
the individual pairs ranged from -42 ms to +35 ms -- the 5% shell reduction sits
below the noise floor and does not pay for a second request. Cold runs cannot
settle it either: with the startup feed deleted the ~660 ms server-side
generation dominates, and the two medians there (876 ms against 821 ms, n=6,
unpaired) differ by less than the 702--992 ms spread of the split arm alone.

The build keeps the decision honest rather than remembering it: `SHELL_BUDGETS`
gives each page a ceiling, and a page that declares `lazySurfaces` fails the build
if one of them lands in the shell (a static import put it back) or in no lazy
chunk (the declaration went stale). Only static chunks get a `modulepreload`;
preloading a lazy chunk would put its bytes back on the first paint.

### One local-data client per runtime

Three runtimes serve these pages: a plain browser tab (development, IndexedDB), a
container deployment, and the packaged desktop app (both server-backed through the
bridge). The page modules were already shared; the local-data boot was not.
`app-client.js` imported the browser client and the desktop client and picked one
at runtime, so every page parsed both -- 37 KB of offline database code on a page
that always had a server, and the desktop window's heartbeat and close signals on
a page that never had a window. The build then stubbed each one out for the
container, which worked but left the same graph in the packaged desktop pages,
where the browser client is equally unreachable.

The boot is a composition now:

- `frontend/shared/local-data/bridge-client.js` -- the bridge API, the bootstrap
  retry and the auth state publication. It imports `desktop/lifecycle.js` but not
  the session lifecycle; the desktop session is a pair of callbacks.
- `frontend/shared/local-data/desktop-client.js` -- the bridge client plus
  `desktop/lifecycle-session.js` (heartbeat, close signal, active-work close
  veto), handed in as `startLifecycle` / `onDesktopSessionLost`.
- `frontend/shared/local-data/app-client.container.js` and
  `app-client.desktop.js` -- the two shipped compositions. Neither imports the
  other runtime's client, so the graph, not a stub, is what keeps it out.
- `frontend/<page>/page-entry.js` -- the page body, naming no runtime at all, and
  `frontend/<page>/index.js` -- the development entry that composes the body with
  the dispatcher, which still supports both modes.

Each builder composes a page body with its runtime's composition as a synthetic
entry (`pageEntrySource`, passed to esbuild as `stdin`), so the shipped artifact
is exactly one composition plus one body. `app-client.js` is development-only
now: the repo pages load it, the shipped pages do not.

| page | hosted shell before | hosted shell after | gzip before → after |
| --- | --- | --- | --- |
| jobs | 295,169 B | 253,681 B | 86,877 → 74,993 (-13.7%) |
| saved | 267,602 B | 220,381 B | 74,119 → 60,717 (-18.1%) |
| admin | 347,239 B | 298,860 B | 98,245 → 84,435 (-14.1%) |

Those `after` columns measure this change; a later shrink of the shared token
registry and the profile dialog took the container pages to 245,061 (jobs),
211,747 (saved) and 291,725 (admin shell) -- see [The modules every page shares
are smaller than they were](#the-modules-every-page-shares-are-smaller-than-they-were).

The hosted side removes what the build's earlier stubs removed, now absent by
construction: the saved-jobs domain (10 KB) and its siblings -- backup service 4.3 KB, the
IndexedDB adapter 3.6 KB, backup import/export 3.5 KB, attachments 3.0 KB, the
admin domain 2.9 KB, activity 1.9 KB, auth 1.3 KB -- plus 4 KB of desktop session
code. What it does not buy is a faster first row. Alternating the two built pages
against the real container handler, warm snapshot: at 4x CPU the first row moved
by a paired median of 4 ms (443 ms against 444 ms, n=10 pairs, individual pairs
-39 to +123 ms), and the 4 KB session removal was flat too (409 ms against 418 ms,
13 ms median, n=10). At 40 ms RTT and 200 KB/s both were flat (+8 ms, n=8, and
+9 ms, n=10); only at ~200 kbps did the offline stack show, as 368 ms of transfer
(9,467 ms against 9,834 ms, all 8 pairs between 355 and 379 ms). So on a hosted
page this is a smaller download, not a faster first row on the machine that builds
it.

The packaged desktop pages were carrying the 37 KB too, and there the fix is
visible. Built before and after against the same packaged site handler, alternating
eight runs per page:

| page | shell before | shell after | JS transferred before → after |
| --- | --- | --- | --- |
| jobs | 283,557 B | 245,561 B | 335,966 → 297,970 B (-38.0 KB) |
| saved | 268,018 B | 224,934 B | 272,103 → 229,019 B (-43.1 KB) |
| admin | 347,582 B | 303,373 B | 624,789 → 580,580 B (-44.2 KB) |

Those `after` columns measure this change; the shared-module shrink that followed
put the packaged shells at 235,965 (jobs), 216,300 (saved) and 296,244 (admin).

The transferred number includes the lazy chunk each page fetches after boot, which
is why admin is larger than its shell. At full CPU the difference is inside the
run-to-run noise; at 4x CPU the jobs page paints its rows in 468 ms instead of 479
(paired median 23 ms, n=8), the admin page settles in 536 ms instead of 561 (25
ms) and the saved page finishes booting in 274 ms instead of 292 (15 ms). The
before arm is the pre-composition build, reproduced by patching only the entry
back to `index.js`; it lands within 0.1% of the sizes the previous build recorded,
so the pair is faithful.

Two guards keep the compositions from quietly merging again.
`assertContainerComposition` and `assertDesktopComposition` read the esbuild
metafile of every page -- shell and lazy chunks -- and fail the build if a page
reaches the other runtime's modules (`app-client.js`, `browser-client.js`,
`desktop-client.js`, `lifecycle-session.js`, `indexeddb-adapter.js`).
`frontend_structure_guardrails.mjs` checks the same boundary in the source tree:
`bridge-client.js` may not import the browser client or the session lifecycle, and
each composition may import exactly one runtime.

After the split the unrun code left on a hosted page is the desktop navigation
bypass in `desktop/navigation.js` -- a page still uses `navigateDesktopPage` for
its links, but the bypass flag it arms is consumed only by the lifecycle handlers
that no longer ship. That is under a kilobyte, and it is the point at which the
shell is essentially all code the page can execute.

Verified against the real container handler: all three pages boot in container
mode, `window.JobAppLocalData` exposes the bridge methods (`saveJobForUser`,
`getSavedJobKeys`, `getCurrentUser`, `signIn`), the jobs page renders its rows and
quick filters, the saved page renders its sections, the admin Ops panels fill in,
and no page errors or failed container-asset requests were recorded. The packaged
pages were served the same way through the package's own handler: rows render, no
page errors, and `confirmed_active_work_close` is still in each shipped asset (the
session lifecycle is what the desktop keeps, and the hosted build drops).

## Bundled Desktop Pages

A ship build rewrites each version's `admin.html`, `jobs.html` and `saved.html`
to load one esbuild asset each (`scripts/build_desktop_frontend.mjs`, run by
`scripts/desktop_frontend_bundle.py` before the version folder is hashed), the
way the container build already bundles its pages. The packaged site answers one
request per module through `SimpleHTTPRequestHandler`, so the unbundled graphs
cost 118 script requests / ~705 KB (jobs), 169 / ~1.2 MB (admin) and 97 / ~553 KB
(saved); each page now loads one entry asset. The hashed name keeps updates from
serving a stale asset, and the unbundled modules stay on disk for development and
`runtime-bootstrap` repairs.

`early-data.js` stays on the bundled jobs page as its own classic script, so its
two requests still start at parse time, ahead of the single asset fetch. The
container build has no separate file to request, so it inlines the same hoist
into `jobs.html` instead -- see [The container page inlines the
hoist](#the-container-page-inlines-the-hoist) for the measured effect.

### The split is enforced by the build, not by a test

`scripts/build_desktop_frontend.mjs` carries each page's budget and its lazy
surfaces (`PAGE_BUDGETS`) and refuses the ship build, before writing any asset,
in two cases:

- the shell -- the entry plus the static chunks the HTML preloads -- is over the
  page's byte budget (a few KB of headroom today: Admin 293 KiB against 296,244
  bytes, Jobs 235 KiB against 235,965, Saved 216 KiB against 216,300), or
- a declared lazy surface is in no lazy chunk, which is the shape a static
  import of it takes: the module moves back into the shell and disappears from
  the chunk's inputs.

The check reads the esbuild metafile, so it identifies modules instead of
guessing from the minified output, and the error names the surface or the exact
overage. A failing build writes nothing, so the packaged page keeps working.
Adding weight to a first paint stays allowed, but it has to be a visible edit to
that budget table rather than a quieter start.

### The Admin page splits into a shell and a lazily loaded Ops surface

Admin is the first page that splits. Its entry is the shell — page chrome, the
bridge badge, the stored-profile overview and the action center — and the Ops
surface (health controller, five tab panels, their renderers) is reached through
a dynamic `import()` in `frontend/admin/app/ops/lazy.js`, which esbuild emits as
`frontend/admin/chunks/admin-<hash>.js`. Three things make the split real rather
than cosmetic:

- The shell imports render leaves (`render/overview.js`, `render/logs.js`,
  `render/sources.js`) instead of the `render.js` barrel. The barrel re-exports
  the Ops renderers, so a shell import of it drags ~300 KB of tab code straight
  back onto the boot path.
- `createLazyAdminOpsController` buffers only the calls that shape the first
  paint (readiness shell, placeholders, bootstrap payload, bridge badge) and
  replays the last call per method when the chunk lands. Everything else is
  promise-shaped and awaits the chunk, so call sites did not change.
- The chunk loads on `opsController.start()`, which the composition calls right
  after `markAdminStartupBootstrapSettled()`, with a 1.5 s fallback if the
  bootstrap never settles. Loading it on the first idle slot instead put its
  parse back inside the shell's boot and measured *slower* than the single-asset
  page (169 ms vs 131 ms at 1x), which is why the trigger is the bootstrap and
  not `requestIdleCallback`.

The chunks the entry imports statically — the ones holding modules both halves
need — are `modulepreload`ed from `admin.html`. Without that link they are a
second serialized round trip after the entry parses, which cost ~44 ms of DOM
content load time.

Measured against the packaged site handler on one version tree with the original
pages restored on the other side, alternating run order (readiness = admin source
status settled, `bridgePort=9` so the stubbed bridge settles in a few ms):

| variant | JS before settle | requests | settled 1× (n=24) | settled 4× (n=15) | FCP 1× | DCL 1× |
| --- | --- | --- | --- | --- | --- | --- |
| single asset | 607 KB | 3 | 152 ms | 596 ms | 124 ms | 66 ms |
| shell + lazy ops | 347 KB (261 entry + 86 shared) | 5 | 144 ms | 534 ms | 120 ms | 65 ms |

At full CPU the difference is inside the run-to-run spread — the settle is
dominated by asset latency and first paint, not by entry parse — so the honest
claim is the throttled one: a quarter-speed machine parses the 43% smaller shell
and settles ~60 ms sooner. The Ops chunk is fetched at ~15 ms here only because
a stubbed bridge settles the bootstrap immediately; a real launch starts it
later, so the deferral is larger than this harness shows.

Both variants settle with the same status text and, against a live bridge, render
the same totals, KPIs, run history, tab badges, dedup lists and registry-conflict
triage.

### The admin shell defers its below-the-fold markup

Splitting JS off the boot path left the page's own static markup in the first
layout: 210 elements, 4,486 px of document, 44 controls, and every element the
below-fold sections will ever need. Deferring that markup is worth more than any
remaining JS split, and the ablation says so before any code was written (one
section removed per arm, scripts blocked, same styled tree):

| arm (elements) | `Layout` 1x | delta | `Layout` 4x | delta |
| --- | --- | --- | --- | --- |
| full (210) | 25.6 ms | -- | 190.9 ms | -- |
| no discovery (138) | 18.5 ms | -7.2 | 137.1 ms | -53.9 |
| no ops (169) | 24.3 ms | -1.3 | 170.6 ms | -20.4 |
| no sync (192) | 25.0 ms | -0.6 | 173.3 ms | -17.6 |
| no fetcher (191) | 26.6 ms | +0.9 | 187.9 ms | -3.1 |
| no inspector (204) | 26.4 ms | +0.8 | 193.6 ms | +2.7 |
| no hidden Ops tabs (198) | 26.3 ms | +0.7 | 190.1 ms | -0.8 |
| everything below the fold removed (54) | 10.5 ms | -15.2 | 67.1 ms | -123.9 |

The last two rows are the same lesson twice: markup that is `display: none`
(inspector overlay, the four hidden Ops tab panels) costs nothing to lay out, so
hiding it is free; everything else is priced by what it contains, and the ceiling
for dropping the whole below-fold half is 65% of the pass at 4x.

`content-visibility: auto` does **not** capture that: it lays the skipped
subtrees out during the same frame as they approach the viewport, so the trace
shows two passes whose sum is unchanged (3 sections 189.9 ms, 4 sections
185.6 ms at 4x, against 190.9 for the full page). The shipped rule uses
`content-visibility: hidden` plus an explicit reveal instead, on the two sections
that start clearly below a 1366x900 viewport:

```css
.admin-page .admin-deferred {
  content-visibility: hidden;
}
```

`#admin-discovery-section` and `#admin-sync-section` carry `admin-deferred` in
`admin.html`; `revealDeferredSections` (`runtime/deferred-sections.js`, called at
the end of `bootAdminPage`) removes the class in the frame **after** the first
paint. Nothing is rewired: the nodes, their cached refs and their listeners exist
the whole time, and hidden-then-revealed is exactly the `display: none` case the
page already relies on. `contain-intrinsic-size` keeps the scroll height stable
for the single frame the sections are hidden, and the reveal re-seats the
discovery log to its tail because a hidden subtree reports `scrollHeight` 0.

Measured against a live bridge with an isolated data dir (both arms hydrate 39
pending rows and settle to the same status), alternating run order, fresh browser
per load:

| metric | before | after | delta | pairs favouring after |
| --- | --- | --- | --- | --- |
| first paint, 4x (n=10) | 306 ms | 178 ms | **-128 ms** | 10/10 |
| `Layout` before first paint, 4x | 189.9 ms | 108.1 ms | -81.8 ms | 10/10 |
| `Layout` total, 4x | 367.9 ms | 378.4 ms | +13 ms | -- |
| readiness (status settled), 4x | 601 ms | 611 ms | +6 ms (deltas -15..+103) | 5/10 |
| first paint, 1x (n=6) | 88 ms | 44 ms | -44 ms | -- |
| `Layout` before first paint, 1x | 26.6 ms | 16.8 ms | -9.8 ms | -- |

The work moves rather than disappears -- total layout is 13 ms *higher* because
the reveal relayouts the whole document rather than the two sections alone -- and
the readiness metric is a wash at 4x: the shell is on screen 128 ms sooner, the
"Loaded ..." status is not. Both arms end with identical geometry after the
reveal (`#admin-content` 4,259.4 px at the same y, pending sources 823.2 px, 39
rows) and no page errors. Ops stays in the first layout on purpose: it starts at
585 px, inside the viewport, so hiding it would blank visible content to buy its
20 ms.

### The Jobs page splits into a shell and two deferred surfaces

Jobs splits the same way, around what its first rows need. The entry keeps the
cached-snapshot path, the feed controller, filters, events and list rendering;
two surroundings load as their own chunks:

- The pipeline controller (status poll, run trigger), reached through
  `frontend/jobs/app/runtime/lazy-pipeline.js`. The facade keeps both call sites'
  contract: a status watch returns immediately and starts polling once the chunk
  lands, and a run click that lands while the chunk is in flight runs the
  pipeline instead of being dropped.
- The desktop update surface (`desktop-update-surface.js`: panel, model, click
  diagnostics and the release-notes dialog only it opens), started from
  `markStartupRendered` the moment the feed has painted rows, with a 1.5 s
  fallback from boot so an empty or slow feed still mounts the panel.

The shell is 183 KB plus 53 KB of preloaded static chunks (236 KB) against the
326 KB single asset, and 46 KB of update/pipeline code is deferred. Against the
packaged site handler with a stubbed bridge, alternating run order, readiness =
first job row:

| variant | JS before rows | requests | rows 1× (n=15) | rows 4× (n=15) |
| --- | --- | --- | --- | --- |
| single asset | 326 KB | 4 | 97 ms | 512 ms |
| shell + 2 lazy chunks | 283 KB + 46 KB deferred | 9 | 92 ms | 505 ms |

The paired difference is −6 ms at 1× and −11 ms at 4× (median; 10 and 13 of the
15 pairs favour the split), and the same marks that produce those rows move with
it: `jobs_preview_ready` −5 ms and −10 ms. The margin is small next to the
run-to-run spread (±30 ms at 1×) and an earlier series on a busier box measured
+2 ms, so the honest claim is a few milliseconds — the row lands on the startup
snapshot fetch and the first paint, not on entry parse, and 43 KB less blocking
parse buys only as much as the smaller parse is worth. What is not marginal is
what moved off the path: the update panel and the pipeline watch arrive at 107 ms
and 131 ms *after* the rows (versus boot for the single asset), and Admin's much
larger ops split shows the same shape with a bigger payload.

Against a live bridge both builds render the same state: no page errors, the same
update panel title, body, meta and primary button (fetched from the bridge by the
lazy surface), and the same enabled Update jobs button from the lazy status
watch.

### There is no shell/lazy duplication left to lift out

With esbuild splitting on, each packaged page emits every module exactly once.
Attributing the metafile per output (`bytesInOutput`) finds **zero** modules
present in more than one output of a page -- not between the entry and a lazy
chunk, not between two lazy chunks. A module the entry and the Ops chunk both
need lands in the static chunk both import (`admin-SE5RL23T.js`, 81 KB), which is
the shared chunk this question is usually asking for; it already exists. Jobs has
five chunks (38.0 KB, 30.9 KB, 14.6 KB, 11.1 KB, 3.8 KB) and the same holds:
14.6 KB is the pipeline controller alone, and the 11.1 KB chunk is
`app/pipeline.js` plus a text helper -- the code the pipeline chunk and the shell
both need, emitted once.

What *is* duplicated is across pages, because each page is its own build: 47
modules appear in two or all three bundles, worth 70,575 bytes on the jobs page,
66,670 on saved and 57,830 on admin (the profile dialog 9.1 KB, the desktop
local-data API 8.8 KB, the tooltip controller, the session lifecycle, the
runtime context).

Lifting those into chunks the pages share was tried and measured, by building all
three entries in one esbuild invocation (15 shared chunks) and comparing each
page's first-load graph -- entry plus its static chunks:

| page | per-page build | three-entry build | requests | reusable on a second page |
| --- | --- | --- | --- | --- |
| admin | 296,244 B | 301,167 B | 2 → 10 | 62,387 B |
| jobs | 235,965 B | 239,966 B | 4 → 11 | 74,273 B |
| saved | 216,300 B | 222,368 B | 1 → 10 | 71,931 B |

The trade does not favour the split. Every page gets 4-6 KB *larger* on its first
paint (esbuild can no longer inline a module a single page uses into its entry),
and pays 7-9 more requests for it, while the win -- 62-74 KB of that graph
becoming a chunk a second page already holds -- only arrives on navigation, where
`STATIC_CACHE_CONTROL` (`public, no-cache`) turns those bytes into revalidations
instead of a body. The budgets above were set against the per-page numbers, so
adopting it would also mean raising all three ceilings. This is the same verdict
the container jobs split got on a smaller scale, for the same reason: a second
request and a higher first-paint floor are not paid for by a saving that shows up
on the second page load. Reducing those 47 modules is a bundle-size job, not a
chunking one, and the two that weighed most were taken that way.

### The modules every page shares are smaller than they were

Two modules in all three bundles carried weight no page needed in full. Shrinking
them took 7-10 KB off each shell, and `PAGE_BUDGETS` came down with them (Admin
293 KiB, Jobs 235 KiB, Saved 216 KiB):

- `frontend/local-data/profile-name-dialog.js` built the same overlay, panel,
  heading, description, teardown, Escape and overlay dismissal and deferred
  focus four times -- once per prompt. One frame and one mount helper replace
  the copies; each prompt supplies only its body and the value a dismissal
  resolves to (`null` for a text answer, `false` for a confirmation). The DOM
  contract is unchanged: the same `local-auth-dialog-*` ids and classes, the
  same `data-local-auth-dialog` overlay. 11,613 -> 9,061 bytes per page, and
  four copies of the handling collapse to one.
- `frontend/shared/ui/selectors.js` was one frozen table of ~300 `data-ui`
  tokens, so every page carried the jobs, saved and admin tables whether or not
  it resolved a handle from them. The tables moved to
  `frontend/shared/ui/tokens/` -- `global`, `jobs`, `saved`, `admin` -- and each
  page imports the ones it resolves: jobs takes global + jobs, saved global +
  saved, admin global + admin. `selectors.js` stays the documented entry point
  and still exposes the combined `UI_TOKENS`, which is what the token contract
  test walks; no page module imports it any more, so the other tables are simply
  not in the bundle.

Together: admin 303,373 -> 296,244 bytes (-2.3%), jobs 245,561 -> 235,965
(-3.9%), saved 224,934 -> 216,300 (-3.8%), at the same request count as before.

What that is not is a faster first row. Two version trees were built from the
same source and differ only in those two modules (the `before` arm restores
`selectors.js` and the dialog, and rebuilds to 303,373 / 245,561 / 224,934 bytes,
the sizes recorded before the shrink, so the pair is faithful), then served
through the real packaged site handler -- alternating 12 pairs at full CPU and 14
at 4x CPU. Readiness is the first jobs row, the admin source status settling and
the saved page's `saved_boot_end`:

| page | ready 1x, before → after | paired median (95% CI) | ready 4x, before → after | paired median (95% CI) |
| --- | --- | --- | --- | --- |
| jobs | 88 → 86 ms | +2 ms (-4…+6) | 443 → 459 ms | -6 ms (-21…+18) |
| admin | 105 → 92 ms | +1 ms (-1…+44) | 532 → 533 ms | 0 ms (-8…+12) |
| saved | 54 → 54 ms | +1 ms (-2…+5) | 279 → 270 ms | +3 ms (0…+12) |

Positive means the before arm finished later. Nothing here separates the arms:
the sign flips between the two CPU levels, every interval but one straddles zero,
and the pairs swing by 100 ms or more in both directions. The reason is the size
of what was removed. Compiling each page's shell asset in the same throttled
Chromium (30 alternating reps) takes 2.5 ms at 1x and 13.6 ms at 4x for jobs;
the 7,026 bytes its entry loses are 0.1 ms and 0.4 ms of that (admin 0.1 / 0.2,
saved 0.1 / -0.1). The page's readiness is ~90 ms at 1x and ~450 ms at 4x, and
most of it elapses before the page reads its snapshot at all (the next
subsection splits that time), so a sub-millisecond parse saving cannot surface.
Request counts are unchanged (9, 5 and 3 scripts per page), first paint at 4x is
unchanged (256 / 364 / 236 ms), and
`ScriptDuration` for the whole session moves by 1-3 ms in the right direction but
with a 15 ms spread. The packaged handler serves the assets uncompressed
(`Content-Length` equals the file size), so this is a real byte saving on disk and
on the wire; at this scale it is not visible time on the machine that builds it.
That is the case for the shrink: bytes that no page needs, not a latency feature.

### The 4x first row is the shell's first layout, not the snapshot read

The jobs page has row elements at ~92 ms at 1x and ~490 ms at 4x, and the first
frame that has laid those rows out at ~113 ms and ~618 ms. Splitting that clock
with a Chrome trace of the packaged page (6 runs per CPU level, against the real
packaged handler, boundaries taken from the page's own `performance.mark`s --
`jobs_startup_preview_fetch_start` / `_fetch_done` / `_parse_start` /
`_parse_done` / `_render_start` / `_render_done` / `jobs_preview_ready` -- plus
`MutationObserver` and double-`requestAnimationFrame` marks for the first row and
its first painted frame):

| stage (page clock) | 1x | 4x |
| --- | --- | --- |
| navigation → preview read start | 75.5 ms | 388.5 ms |
| read start → read done | 2.0 ms | 10.5 ms |
| read done → parse start | 1.0 ms | 2.0 ms |
| parse | 0.0 ms | 1.0 ms |
| parse done → render start | 4.0 ms | 26.0 ms |
| render | 8.0 ms | 55.0 ms |
| render done → row elements | 0.6 ms | 4.7 ms |
| row elements → first painted frame | 22.6 ms | 129.4 ms |
| **first painted row** | **113.2 ms** | **618.3 ms** |

The page does not start reading its snapshot until 389 ms at 4x: 80% of the way
to row elements, 63% of the way to the first painted row. Inside that first
stage the trace attributes, over the same runs:

| engine event | 1x | 4x |
| --- | --- | --- |
| `Layout` | 21.7 ms | 150.3 ms |
| `UpdateLayoutTree` | 8.8 ms | 38.6 ms |
| `ParseHTML` | 3.0 ms | 19.9 ms |
| `v8.evaluateModule` (the 183 KB shell) | 2.3 ms | 13.3 ms |
| `EvaluateScript` (the three classic scripts) | 1.6 ms | 6.4 ms |

So the largest single stage is the static shell's **first style and layout**:
189 ms of 618 ms at 4x (31%) and 31 of 113 ms at 1x (27%). It is not app work.
A control run of the same page with *every* script request aborted -- HTML and
CSS only, 144 elements, 1,383 px tall -- still spends 136.9 ms in `Layout` and
37.3 ms in `UpdateLayoutTree` at 4x (21.0 + 6.5 ms at 1x), reproducing 92% of
the 4x number and 90% of the 1x one with no JavaScript on the page. The second
largest is the row geometry itself: 129 ms at 4x (23 ms at 1x), nearly all
`Layout` (90 ms), which lands after the row elements exist and before the first
frame that can show them. Together the two style/layout phases are 318 of 618 ms
at 4x, just over half of the visible-row path. The module shell's evaluation is
13 ms (2%), the snapshot read the page has been hoisting since parse is 10.5 ms,
and the render that builds the row markup is 55 ms (9%).

`loadEventEnd` lands at 277 ms of the 618 at 4x (45%) and 55 of 113 at 1x (48%),
so the first half of the path is document load: HTML and CSS parse, the shell's
first layout, and the module graph. The 340 ms that follow at 4x are app boot up
to the read (111 ms), the read and its parse (14 ms), the normalize gap and the
row-markup render (81 ms), and the row layout and paint (134 ms). The `snapshot
read` column in the boot-order table below measures navigation to the read
*finishing* (this table's first two rows together), so it reads 369 ms while the
read itself is 10 ms.

Two caveats for reading the table. The 4x column is a 5.5x stretch of the 1x
column, not 4x, so shares are comparable but durations should not be scaled
between rows. And the double-`requestAnimationFrame` mark is the first frame the
browser schedules after the insertion, not proof the compositor presented it
before the trace ended; it is a closer proxy for "the user can see rows" than
the DOM-insertion number the A/B above uses, and it is 129 ms later at 4x.

#### That first layout is a cold renderer, not expensive CSS

The 21 ms `Layout` above is one pass -- `dirtyObjects=139 totalObjects=139` -- over
a 144-element shell. Ablating the page (script-blocked, one arm per row, 3-5 runs,
native CPU, a fresh browser per run so the renderer is cold) splits it:

| arm | change | `Layout` | `UpdateLayoutTree` |
| --- | --- | --- | --- |
| full | -- | 20.8 ms | 6.2 ms |
| same page, **second load** in the same renderer | caches warm | **0.8 ms** | 2.9 ms |
| `tiny` | 5 objects, no CSS | 3.0 ms | 0.2 ms |
| `bare` | 141 objects, no CSS, no text | 5.2 ms | 3.7 ms |
| `nocss` | 141 objects, 992 chars of text, no CSS | 12.2 ms | 4.0 ms |
| `notext` | 144 objects, CSS, no text | 6.8 ms | 6.6 ms |
| controls `{ font-family: inherit }` | buttons and options get the page font | 15.2 ms | 5.4 ms |
| controls `{ font: inherit }` | same, control sizes too | 16.6 ms | 5.4 ms |
| same rule in `jobs.css`, no `!important` | HEAD stylesheet vs the shipped edit, two 5-load batches | 19.5-20.1 -> 14.7-15.3 ms | 6.0 -> 4.5-4.9 ms |
| `* { font-family: Georgia }` | one family | 11.2 ms | 4.3 ms |
| `* { font-weight: 700 }` | one weight (text wraps more) | 11.2 ms | 6.8 ms |
| `* { font-weight: 400 }` | one weight (text wraps less) | 13.4 ms | 6.6 ms |
| `* { font-size: 18px }` / `14px` | one size | 15.2 / 16.0 ms | 6.3 / 6.9 ms |
| `* { letter-spacing: normal }` | one spacing | 20.9 ms | 6.3 ms |
| `* { box-shadow: none }` | placebo universal rule | 21.8 ms | 6.6 ms |

Read the **second load** row first: the same document tree, laid out again in the
same renderer process, costs 0.8 ms. **96% of the shell's first layout is
one-time**: building the layout tree, instantiating fonts, filling the shaping and
rule caches, JIT-warming Blink's layout code. A five-element page with no CSS
already pays 3.0 ms of it, and 141 empty unstyled boxes pay 5.2. No rule deletion
removes that floor.

The part that scales with the page is font resolution: the first paint has to
instantiate a typeface and fill a shaping cache for every run it paints. The shell
realizes **23** distinct (family, weight, size) triples. Fifteen come through the
page's own stacks -- 32 distinct `rem` sizes and five weights (500/600/650/700/800)
on `base.css:298` and `jobs.css:14`. The other **eight come from the UA
stylesheet**: 35 of the shell's 66 text-bearing elements -- 14 `button`s and all 21
`option`s -- never inherit the page font, so they resolve Arial at eight sizes
between 11.52 and 14.4 px and three weights. `components.css` carries `font:
inherit` only on three dialog-specific selectors, so nothing in the shell gives the
controls a family.

That is the one rule worth editing, and it is now in `styles/jobs.css`:

```css
.jobs-page button,
.jobs-page select,
.jobs-page input,
.jobs-page textarea {
  font-family: inherit;
}
```

Measuring HEAD's stylesheet against that edit -- same arm harness, scripts blocked,
a fresh browser per load -- puts the pass at 19.50 and 20.07 ms median before and
15.28 and 14.74 ms after, across two 5-load batches (one before-run at 29.0 ms is
the outlier of the set); the biggest style recalc goes from 6.03 ms to 4.87. The
`!important` arm in the table above (20.8 -> 15.2 ms, 141.0 -> 118.9 ms at 4x) is
the same change without fighting specificity. `select` is what reaches the options: they inherit
from it, and an explicit `option` rule changes nothing. The fuller `font: inherit`,
which also restyles the controls' sizes, lands at 16.6 ms and 110.6 ms -- worse at
1x because the inherited sizes wrap more, better at 4x because it also collapses
the sizes.

What the milliseconds follow is the **typeface**, not the triple count. The rule
leaves the shell at 22 distinct triples, not 16: the controls' eight (size, weight)
combinations still differ from the page text's, they are only Segoe UI now, and
exactly one of them coincides with an existing run. Writing the same rule with a
*different* family recovers only part of the win -- Georgia on the controls 18.07
ms, Arial on the controls 19.42 ms, against 15.28 for the page's own family (n=3
each) -- so the ~4 ms is the second typeface disappearing, and a rule that merely
rewrites which family the controls use buys nothing. Same mechanism as the
`* { font-family: Georgia }` row: one font resource for the whole shell instead of
two, at whatever sizes.

The visual cost is one extra pixel of control height. Segoe UI's line box is taller
than Arial's at these sizes, so the filter bar grows 1.44 px (each `select` and
`input` +0.72 px), the container +3.44 px and the document 1383 -> 1387 px. The
header block is pixel-identical (x, y, width and height all unchanged); its "Sign
in" button trades 1.31 px of width for 2 px of height inside an unchanged 70.33 px
header. Nothing clips, wraps or overlaps differently in either region.

The same rule now sits in `admin.css` and `saved.css`, scoped by each page's own
body class (`admin.html` gained `class="admin-page"`; `saved.html` already had
`saved-page`). Their shells are heavier than jobs -- 210 and 175 elements, 44 and
52 controls -- and every control on both was Arial, except saved's textarea and
`datetime-local` input, which had fallen through to the UA's `monospace`. Paired
and interleaved (eight pairs per page, arm order alternating, a fresh browser per
load, scripts blocked) the first `Layout` goes 31.70 -> 28.44 ms on admin, faster
in 7 of 8 pairs, and 19.88 -> 16.41 ms on saved, faster in 8 of 8; distinct
(family, weight, size) triples drop 21 -> 20 and 30 -> 28. The interleaving is not
optional on these pages: eight admin loads of one arm spread over 29.5-34.5 ms, so
the first batch -- which ran one arm and then the other -- reported admin as
*slower*, which is what made the paired re-run necessary.

The visual cost is larger here than on jobs, because these labels are long enough
to set the buttons' widths instead of leaving them pinned by short text. Admin's
header, action center, section nav and toolbar are pixel-identical; its content
panel grows 9.15 px, the ops tabs and action buttons gaining 3-4 px of height.
Saved's header does not move, the quick-action row and the list under it grow 3 px,
and the open Add Custom Job panel is 16 px taller. The widest change is a 9-11 px
*narrower* "Back to All Jobs" / "Retry failed" button inside a right-aligned row
with slack; no control on either page changes its line count, and none gains
horizontal or vertical overflow. In the Add Custom Job panel, the notes textarea
and reminder input stop rendering monospace and match the rest of the form.

Type-scale breadth costs the same way rather than through any component's layout:
pin a single family (11.2), weight (11.2) or size (15.2) and the pass drops from
20.8. That the win is font instantiation and not geometry is settled by direction:
all-bold and all-18px both make the text wrap *more* than the mixed scale and both
still beat it. At 4x the single-family arm takes `Layout` from 141.0 to 95.2 ms
and style recalc from 38.0 to 27.3, against 4.1 ms warm.

What is *not* the cost: the two page stacks themselves. Editing `jobs.css`'s stack
to `"Segoe UI", sans-serif` -- the obvious fix -- moved nothing (20.5 ms), because
the children still carry `base.css`'s stack. `letter-spacing` (20.9),
`font-variant-numeric` (21.2) and `box-shadow` (21.8 -- the placebo that proves the
`!important` override itself costs ~1 ms) are noise. Removing `white-space: nowrap`
or `text-overflow: ellipsis` makes layout *worse* (22.8 and 22.9 ms) by letting
more text wrap. So no single component's CSS is expensive: the pass is cold font
resolution spread over every styled run, and the only rule that was clearly wrong
was the controls' missing family.

The 96% that is cold start is also why 4x inflates this stage 6.8x (141 ms) rather
than 4x: compiled-code warm-up is what throttling stretches most. Beyond the
control rule, the lever is preventing a cold first layout before the rows exist --
painting something earlier, or cutting the number of styled text runs the shell's
first paint has to resolve.

### A fullwidth plus in the save button cost 40% of the row layout

The row's 129 ms at 4x is text, not geometry. With the same 134 elements and no
text nodes the list costs exactly what an empty list costs (117.9 ms both at 4x;
the five rows add 104.7), laying the rows out as blocks instead of the
six-column grid changes nothing (+1.8), and neither does removing the border or
the nested flex centring (both inside the run-to-run spread); `contain: layout
style` on each row is *worse* (+5.7 at 1x). The text cost is not per-run shaping
either: collapsing the
rows' 41 runs onto one (size, weight) triple takes the list from 222.6 to 149.4
ms at 4x, so it is the font resources the runs resolve.

| arm (5 rows in the static shell) | `Layout` 1x | `Layout` 4x |
| --- | --- | --- |
| rows, as shipped | 29.6 ms | 222.6 ms |
| same rows, no text nodes | 18.0 ms | 117.9 ms |
| empty list | 17.3 ms | 117.9 ms |
| rows as blocks (no grid tracks) | 30.0 ms | 224.3 ms |
| all row text on one font triple | 20.9 ms | 149.4 ms |
| save glyph `＋` -> `+` | 19.5 ms | 144.0 ms |

That last row is the bug. `JobRow.js` rendered the save button as `＋` (U+FF0B,
FULLWIDTH PLUS SIGN). Segoe UI has no U+FF0B, so Blink resolved each of the five
glyphs from a **Microsoft YaHei** fallback -- `CSS.getPlatformFontsForNode`
reports YaHei for `＋` and Segoe UI Black for `+` on the same button -- and a
second font resource had to be instantiated for the first layout that shows the
rows. The character was the only change.

Paired, alternating, same local startup snapshot, five rows in both arms:

| metric | before | after | delta | pairs favouring after |
| --- | --- | --- | --- | --- |
| first painted row frame, 4x (n=8) | 592.5 ms | 552.5 ms | **-52 ms** | 8/8 |
| `Layout` before that frame, 4x | 195.2 ms | 153.4 ms | -42 ms | 8/8 |
| first painted row frame, 1x (n=6) | 164.5 ms | 150.5 ms | -9 ms | 5/6 |

The row *insertion* does not move (483.5 -> 484.0 ms at 4x): the fallback is paid
when the rows are laid out for their first paint, the stage the earlier trace
priced at 129 ms. The button is visually unchanged at 2x.

### The other glyphs the UI font could not render

The fullwidth plus was one instance of a class of bug, so the rest were audited
the same way. Ground truth came from the font files rather than from guesswork:
the `cmap` tables of every Segoe UI face (`segoeui*.ttf`, `seguibl*.ttf`) were
parsed directly, and the results were confirmed in the page with
`CSS.getPlatformFontsForNode`.

Segoe UI **carries** `•` `·` `Δ` `×` `…` `—` `–` `→` `↔` `←` `↑` `↓` `●` `○` `▪`
and every Latin/Greek letter used in the UI. It **lacks** `✓` `✔` `✕` `✖` `▶`
`▸` `▾` `◆` `★` `⚠` `⏰` `❌` `📋` `🔄` `☑` `＋`. Three of those were actually
rendered:

| glyph | where | rendered from |
| --- | --- | --- |
| `✓` U+2713 | jobs save button (saved), saved phase step (`phase-step-check`) | Segoe UI Symbol |
| `▸` `▾` U+25B8/U+25BE | admin discovery-log disclosure marker | Cambria Math |
| `＋` U+FF0B | jobs save button (unsaved) | Microsoft YaHei |

The arrow is the interesting one: a serif math face shaped a chevron in the
middle of a sans-serif admin panel, and nothing in the source looked suspicious
because the CSS used `\25B8`/`\25BE` escapes. The emoji and warning glyphs in
`action-center.js` appear only in a comment — that panel was already converted to
inline SVG.

All three are now drawn instead of typed: the tick is a 24x24 `currentColor` path
(`renderCheckIcon` in `ui-helpers.js`) emitted as an inline `<svg>` by the saved row
(`JobRow.js`) and the completed phase step (`tracking-ui.js`), and the disclosure
marker is a `border-top` triangle rotated `-90deg` while closed. Both measure as
zero font resolution — `CSS.getPlatformFontsForNode` returns an empty font list for
the button and the step node, where it used to name a second family.

The tick was also priced against masking it instead, and that comparison is worth
keeping only as a closed question. Renderer memory, delta over a same-page control,
median of four fresh-browser runs, private / working set at 60 occurrences: typed
`✓` 0.43 / 3.52 MB, inline `<svg>` 0.92 / 4.92 MB, CSS mask 0.62 / 4.38 MB. Both
drawn forms resolve no font, so the choice between them is pure allocation: the mask
saves about 7 KB per row over inline markup, and nothing that a page showing ten to
twenty-five rows can notice. The inline `<svg>` was kept because it needs no
stylesheet token, and a missing token would paint a solid square where the mark
belongs — the mask's failure mode, not the markup's. The measurement that matters
is the one the drawn tick already won: the typed glyph needs a fallback face.

`tests/frontend/unit/glyph-font-coverage.test.mjs` pins the property: no page
markup or CSS `content:` string may contain a codepoint from the missing set, the
marker must stay a border triangle, and both tick painters must draw the shared
path rather than type the character. User-supplied job text is unaffected — a title
with an emoji still falls back, but that is data, not UI.

### The same mark on the packaged page

The drawn tick was checked on the shipped artifact: `Baluffo.exe` rebuilt from this
tree, its own bundled pages, the jobs page driven through the app's own Edge window
over CDP, and a data dir holding 60 real postings plus a finished fetch report. A
1249x1285 window shows 10 rows, 3 of them saved (the profile has 4; the page's own
order decides which land on page 1).

Each saved button carries exactly one inline `<svg>` — `viewBox="0 0 24 24"`, 13.59
px square, `fill: currentColor` — with no masked pseudo-element, and
`CSS.getPlatformFontsForNode` returns an empty font list, so the mark resolves no
second typeface. The saved rows add two nodes each (`svg` + `path`): 275 nodes in
`#jobs-list` with three saved rows, against 269 on the same page with none.

Memory does not separate the two drawings: three launches with 3 ticks sat at 73.59
/ 79.16 / 69.06 MB renderer private (median 73.59), a 10.1 MB launch spread, where
the mask build's three 3-tick launches sat at 72.21 / 74.22 / 73.15 MB. The earlier
same-page ten-tick swap on the mask build moved the renderer by +0.51 / +0.11 /
+0.39 MB inside its own 0.32-0.57 MB sampling spread; there is nothing left to
measure between the two. The claim worth keeping is the font one — the typed glyph
needed a fallback face, the drawn path needs none — and the simpler markup is what
ships.

### The jobs page paints its rows before it binds anything

Boot used to be one synchronous block: cache the DOM, start the bridge watch,
build the quick-filter chrome, bind page and window events, start auth, and only
then let `initJobsFeed` reach the startup snapshot. None of that can change what
the first rows look like, so it now runs in a follow-up task that the first
rendered rows release (`markStartupRendered` → `startBootFollowUp`), with boot's
1.5 s fallback covering a page that never paints. Boot itself now does only the
DOM cache, the list delegation, the URL state read and the feed call, so the
snapshot read is the first thing it waits on. (`early-data.js` already started
the request at parse time; what moves is the read, the parse and the render.)

The filter values are synced by the render itself (`applyStateToFilters`), so
the visible selects are correct in the first row paint; the quick presets, the
click handlers and the auth pill catch up one task later. Auth resolves behind
the rows and re-renders them with the profile's saved/seen marks, the same as an
async auth callback did before.

Measured on one tree with the previous boot restored on the other side,
alternating run order, readiness = first job row (n=15 at 4×, n=20 at 1×):

| variant | first row 1× | first row 4× | snapshot read 1× | snapshot read 4× |
| --- | --- | --- | --- | --- |
| boot before rows | 103 ms | 494 ms | 79 ms | 369 ms |
| rows before boot | 94 ms | 466 ms | 77 ms | 367 ms |

The paired median is −7 ms at 1× (18 of 20 pairs) and −38 ms at 4× (14 of 15),
with `jobs_preview_ready` moving with the row. The quick presets now render
~22 ms after the rows at 1× (~123 ms at 4×) instead of before them, which is the
work the row no longer queues behind. The read-start column moves less than the
row because the requests are hoisted at parse time already; what the row used to
wait on is the main-thread work in front of the read, not the network.

Measured A/B against the packaged site handler on one ship version tree (bundled
vs. the same tree with the original pages restored), nine alternating runs per
variant, readiness = first jobs row / admin source status settled / saved boot
end, unbundled → bundled: jobs 182 → 101 ms, admin 260 → 162 ms, saved 122 → 58
ms; at 4× CPU throttling, 686 → 488, 730 → 573 and 374 → 284 ms. First paint moves by about 10 ms in
either direction, and the last script response ends at ~11-13 ms bundled versus
109-226 ms unbundled.

Measured with `scripts/serve_static_site.py` (the packaged request handler)
against a real ship version dir, both variants serving the same startup snapshot
and painting the same 5 rows, alternating run order:

| page | script requests | script KB | rows painted, median |
| --- | --- | --- | --- |
| bundled | 4 | 326 | 85-98 ms |
| unbundled | 118 | 705 | 158-172 ms |

The ranges are two series (7 and 9 runs). The last script finishes at ~12 ms
bundled versus ~125 ms unbundled, which is the whole difference: the page's own
boot work is unchanged. At 4x CPU throttling the relative gap shrinks (477 ms vs
577 ms median) because parsing and running the 326 KB bundle replaces fetching
115 modules, but the bundled page still wins. First contentful paint is
unchanged (~50-60 ms): the shell paints before either script graph arrives, so
this buys time to rows, not to the page frame.

## Startup Metric Schema

Startup diagnostics are retained as JSONL rows in `data/desktop-startup-metrics.jsonl` under the configured runtime data directory. The same rows are exposed by `GET /desktop-local-data/startup-metrics?limit=` and captured by packaged smoke as `startup-metrics.json`.

Rows are versioned support artifacts:

- `schemaVersion`: currently `1`.
- `ts`: bridge/runtime timestamp for the row.
- `event`: stable event name; this remains the compatibility key for required-event checks and startup summaries.
- `category`: support-oriented grouping derived from `event`.
- `fields`: runtime launcher trace details for `desktop_*` events written by shipped startup telemetry.
- `payload`: browser/page metric details for bridge POST rows from `jobs_*`, `saved_*`, and `admin_*` pages.
- `browserTsMs`: optional top-level browser-created timestamp preserved from `browserCreatedAtMs`.

Current categories are `launch`, `browser`, `port_retry`, `bridge`, `site`, `window`, `handoff`, `recovery`, `shutdown`, `page`, `probe`, and `unknown`. Categories are additive diagnostics only; do not use them to replace stable event names.

## Artifact and Reporting Rules

- Use repo-local artifact roots only:
  - `.tmp/packaged-desktop-smoke/`
  - `.tmp/packaged-desktop-smoke-pair/`
  - `data/packaged-desktop-smoke-report.json`
- Do not introduce `%LOCALAPPDATA%\Temp` dependencies for startup perf workflows.
- Keep report shape and event names stable unless compatibility work is intentional and verified across runtime, smoke runner, and tests.

## Drift Control Rules

When changing startup measurement code:

- Update this document first if the ownership boundary changes.
- Prefer importing shared startup helpers over copying event groups or probe classifications into another file.
- Prefer one canonical doc link from `docs/testing.md`, `CONTRIBUTING.md`, and release/process docs over repeated prose.
- Keep helper names aligned with the concepts above:
  - `startup_telemetry`
  - `desktop launcher runtime`
  - `startup probe policy`

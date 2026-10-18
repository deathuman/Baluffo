# Startup Probe Architecture

> - **Status:** Active
> - **Use this when:** changing packaged startup timing, probe orchestration, launcher timing, or startup metrics
> - **Canonical for:** startup measurement ownership, event flow, command surface, and runtime-vs-probe boundaries
> - **Not canonical for:** general testing guidance or release sequencing
> - **Then inspect:** `src/ship/startup_telemetry.py`, `src/ship/desktop_app/{launcher_flow,launcher_diagnostics,launcher_recovery,startup_ready,startup_watchdog}.py`, `src/ship/desktop_app/{launcher,startup}.py`, `src/ship/desktop_app/browser.py`, `src/ship/startup_probe_policy.py`, `src/packaged_desktop_smoke.py`, and `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`
> - **Last updated:** 2026-10-17

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
6. Startup profile analysis renders the stage timings and summary report.

When `desktop_bridge_ready_before_window` is present, startup profile analysis splits the pre-window segment into `Site Ready -> Bridge Ready` and `Bridge Ready -> Window Created`. Older or incomplete traces without that event fall back to the legacy `Site Ready -> Window Created` stage. A deferred launch instead records `desktop_window_open_bridge_deferred`, which marks the window as opened before bridge readiness by design.

Important event families:

- launcher/runtime events: `desktop_*`
- browser/page events: `jobs_*`, `saved_*`, `admin_*`
- report outputs: `startup-profile-summary.json`, packaged smoke `report.json`

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

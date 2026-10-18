# Desktop Startup Bridge Gate And Maintenance Deferral

> - **Status:** Implemented (2026-10-17); packaged launch confirmed 2026-10-17; the `startup_sync_schedule` stall and the first-page-load stall are both root-caused and fixed. Revisit the two remaining watchers below when the next portable rebuild ships, or when a request thread first imports the availability validator under load.
> - **Class:** performance
> - **Trigger:** desktop launches reaching 20.9–23.3 s against a 25 s readiness deadline, one `DesktopStartupReadyTimeout`/`bridge_unbound` at 30.4 s, and a 14.16 s startup stall in the running bridge with zero storage activity
> - **Use this when:** desktop startup is slow, the readiness gate times out, the window opens before the bridge answers, or SQLite maintenance contends with startup
> - **Canonical for:** the startup visibility/deferral/gate change set and its measurement plan
> - **Not canonical for:** current runtime behavior (see [`../startup-probe-architecture.md`](../startup-probe-architecture.md) and [`../storage-contract.md`](../storage-contract.md)) or release sequencing
> - **Then inspect:** [`../startup-probe-architecture.md`](../startup-probe-architecture.md), [`../storage-contract.md`](../storage-contract.md), [`../TROUBLESHOOTING.md`](../TROUBLESHOOTING.md), [`../../src/ship/startup_telemetry.py`](../../src/ship/startup_telemetry.py), [`../../src/bridge/server/httpd.py`](../../src/bridge/server/httpd.py), [`../../src/storage/baluffo_store.py`](../../src/storage/baluffo_store.py), [`../../src/ship/desktop_app/launcher_flow.py`](../../src/ship/desktop_app/launcher_flow.py)
> - **Last updated:** 2026-10-17

## Evidence

Measured from the install's own artifacts, not inferred from code:

- 45 launches in `%APPDATA%\Baluffo\desktop-startup-metrics.jsonl`:
  `bridge spawn → ready` min 1.92 s, p25 3.20 s, median 5.71 s, p75 13.83 s,
  max 23.27 s. Launch → window shown across 53 launches: median 6.99 s, max
  28.97 s.
- The site and browser are not the problem: site spawn → ready is 3.0–5.1 s, and
  window creation after the bridge reports ready is ~0.4 s.
- `/ops/storage-health` on the slow launch reported a 1.44 GB database with a
  1.16 GB freelist (79% free) and a 392 MB WAL; the database shrank to 305 MB
  during that launch.
- Bridge PID timeline: spawn 15:44:19.3 → owner session registered 15:44:24.37 →
  first storage write 15:44:26.47 → **14.16 s with no storage activity** → sync
  worker writes at 15:44:40.64. A fast launch of the same code shows the same
  write followed 58 ms later by the sync worker.
- A 09-29 20:54 launch recorded three `task-runs` reads at 10,303 / 10,320 /
  10,325 ms: startup blocked on the database.
- The packaged smoke with a fresh data directory completes site → bridge ready in
  1,053 ms and shows the window at 2,108 ms on the same code.

Root cause: `BaluffoStore.write()` scheduled a WAL checkpoint (392 MB WAL ≥ the
256 MB truncate threshold) and a VACUUM (1.16 GB freelist ≥ both compaction
thresholds) from the first startup write. Both run on a background thread but hold
the store write lock for their whole duration, so every later startup write waited
behind them. The window could not open because the launcher blocked on
`wait_for_desktop_startup_ready` until the bridge reported `startupReady`.

Diagnosability gap: `startup_trace_target()` only enabled child-runtime events when
`BALUFFO_STARTUP_PROBE=1`, so the bridge's own startup chain had zero rows in real
launch metrics. The stall was invisible in the field.

## Phases

1. **Visibility (shipped behavior-neutral).** Desktop launches write child-runtime
   phase rows: `desktop_bridge_child_start`, `_entrypoint_start`, `_configured`,
   `_sync_config_refreshed`, `_registry_ready`, `_api_built`, `_socket_bound`,
   `_accept_loop_started`, `_first_request_served`, `_started_hooks_*`, and one
   `_startup_step_started`/`_finished` pair per post-bind startup step. The journal
   rotates once at 24 MiB so always-on tracing is bounded.
2. **Maintenance deferral.** The bridge holds WAL checkpoints and VACUUMs from
   process start (`defer_maintenance`, `set_storage_maintenance_deferred`) and
   releases them once post-bind startup work finishes
   (`resume_storage_maintenance`). Each store runs its deferred work once, after a
   5 s grace. Deferred decisions are recorded as `status: "deferred"` with sizes.
3. **Serve-first startup.** The accept loop starts before `on_started`; that work
   runs on `baluffo-bridge-on-started`, and unexpected failures are logged as
   `admin_bridge_on_started_unexpected_failed` instead of killing the listener.
4. **Short window gate.** The launcher waits `BRIDGE_GATE_TIMEOUT_S` (4 s) for
   readiness. On expiry it records `desktop_bridge_gate_deferred`, opens the window
   (`desktop_window_open_bridge_deferred`), and leaves readiness to the background
   success-marker wait. `config.startup_probe` or
   `BALUFFO_DESKTOP_STRICT_STARTUP=1` keeps the old blocking gate and on timeout
   writes phase-aware diagnostics.
5. **Honest classification.** `bridge_bound_not_accepting` separates "nothing
   listening" from "bound but not answering", and a classified timeout carries the
   newest `desktop_bridge_*` rows as `startupPhases`.

## Expected Result

- The window is no longer gated on the bridge's entire startup chain. A launch that
  used to sit at 21 s because a 14 s VACUUM held the write lock should bind and
  answer within the child-boot time (~5–8 s measured), and the window opens at the
  site-ready point or after the 4 s gate, whichever comes first.
- A slow launch degrades to "page shows bridge state and self-heals" instead of a
  dead window, a 30 s error, or a `bridge_unbound` misdiagnosis.

## Verification Plan

1. `python -m pytest tests/storage/test_baluffo_store.py tests/storage/test_baluffo_store_write_path.py tests/bridge/test_task_lifecycle_service.py tests/bridge/test_task_lifecycle_storage.py tests/bridge/test_storage_health.py tests/bridge/test_server_handler_timing.py tests/desktop_app tests/admin/test_admin_bridge_thin_wrappers.py tests/test_runtime_launcher.py -q`
2. `python -m pytest tests -q -m "not slow and not packaging and not release and not windows"`.
3. `npm run lint:repo-guardrails`.
4. Packaged confirmation (done 2026-10-17 on the bloated runtime database,
   433–740 MB with the maintenance deferral active): three launches before and
   three after, same install, same data directory.

   | Metric (3 launches) | Before | After |
   | --- | --- | --- |
   | Launch → window | 17.2 / 22.5 / 25.2 s | 5.4 / 5.4 / 5.8 s |
   | Bridge spawn → ready | 14.5 / 20.1 / 22.4 s | 3.1 / 3.1 / 3.5 s |
   | Bridge spawn → accept loop | not instrumented | 2.7 / 2.7 / 3.1 s |
   | Launch → first jobs content | 23.9 / 26.3 / 33.5 s | 16.6 / 16.6 / 18.0 s |

   Per-phase rows from one after-launch (ms after launch): child start 1889,
   entrypoint 2049, `_configured` 3218 (`storageMaintenanceDeferred: true`),
   `_sync_config_refreshed` 3223, `_registry_ready` 3581, `_api_built` 4747,
   `_socket_bound` 4761, `_accept_loop_started` 4763, `_first_request_served`
   4918, window created 5794, `_startup_step_finished startup_sync_schedule`
   16715, `_storage_maintenance_resumed` 16717, `_started_hooks_finished` 16718.
   The resume lands ~0.5 ms *before* `_started_hooks_finished` because that row is
   emitted by the httpd thread wrapper after `on_started()` — here
   `finish_bridge_startup_hooks`, which resumes maintenance in its own `finally` —
   returns; an earlier version of this plan told the operator to expect the
   opposite order, which the emission points make impossible. `_api_built` (1.5 s)
   is the largest pre-bind step and `_registry_ready` the second (0.36 s).

## First Page Load (traced 2026-10-17)

The first page load was still waiting seconds, and the suspicion was a request
thread holding the SQLite write lock. It is not the write lock. Traced by
dumping every thread stack (`faulthandler.dump_traceback_later`) from a bridge
started from source against the live data dir while a scripted replay of the
page's request set ran (`_out/startup-trace/`, local-only):

| Request (page load, 0.5 s after bind) | Before | After |
| --- | --- | --- |
| `/desktop-local-data/session` | 13 557 ms | 137 ms |
| `/ops/task-state?view=summary` | 13 493 ms | 92 ms |
| `/tasks/run-jobs-pipeline-status` | 13 445 ms | 59 ms |
| `/desktop-local-data/saved-job-keys` | 13 146 ms | 438 ms |
| `/ops/health` | 13 517 ms | 94 ms |
| `/app/update-status` | 15 526 ms | 971 ms first, 5 ms after |
| Whole page-request set | 15 560 ms | 1 000 ms |

What actually held the request threads, from the stacks:

- **The availability catch-up parsed the lifecycle artifact once per target.**
  `JobAvailabilityService.start_overdue_catchup` admits up to ten targets at
  once, and every worker's `_prepare_target` called
  `read_job_lifecycle_state` — a multi-tens-of-megabytes gzip JSON parse plus a
  per-row normalization pass, four to six of them concurrently. The bridge's
  request threads waited behind that CPU/GIL and disk work. Fixed by reusing one
  parse per `(path, mtime_ns, size)` fingerprint
  (`JobAvailabilityService._lifecycle_rows`, same invalidation signal the
  identity cache already uses; it hands back a fresh mapping so the evidence
  commit path cannot poison the reuse).
- **`/app/update-status` re-hashed the downloaded portable artifact on every
  poll** (`_reconcile_downloaded_artifact_status` → `compute_sha256`): 217 MB of
  reads on a request thread, on a page that polls it. Fixed by
  `cached_artifact_sha256`, keyed on the artifact's identity (path, size,
  mtime_ns) so any write still forces a fresh digest.
- Secondary: the startup sync pull's `ensure_active_registry` gzip+JSON parse of
  the source registry, and `availability_validator._fetch` importing `requests`
  lazily while several workers hold the import lock.

Evidence that the write lock was never the holder: no thread stack sat in
`BaluffoStore.write()` or waited on its `_write_lock`, and the store's own
recorded operations during the stall were 0–7 ms (`taskRuns.current` 0–1 ms,
`taskRuns.recent` 7–80 ms). The stall was process-wide CPU/I-O contention, which
is why even the file-backed `/desktop-local-data/*` routes were slow.

Post-fix end-to-end check on the packaged runtime (same install and data
directory, page opened 1.3 s after bind): the page's first request set answered
in 1.0 s, local data initialized on the first poll, the update badge read
"Up to date", `jobs_init_start` → `jobs_first_render` took 67 ms (16 ms on the
page's own clock), and the filters and saved-jobs controls were interactive. The
job list itself stayed empty because that manual bridge/site pair served the
install's `ship/data` instead of the runtime data directory, so no feed
artifacts were present — the first-run bootstrap path, not a stall.

## Open Follow-Ups

- ~~The deferred run still performs one VACUUM after serving, which briefly blocks
  writes on a 1.44 GB database.~~ Done (2026-10-17): the rebuild waits for 30 s of
  write silence (`vacuum_idle_s`) with a 600 s budget, records `skipped-busy`
  instead of stalling writers, and databases at or above 512 MiB must clear a 0.35
  freelist ratio instead of 0.20 — see
  [`../storage-contract.md`](../storage-contract.md#maintenance-scheduling-rules).
  A later pass (2026-10-17) added the stronger gate: while any task lifecycle row
  is active the rebuild is held outright (`held-active-task`, seeded at startup so
  a task that predates the bridge is covered) and the last terminal transition
  releases it and re-schedules. What remains is by design: a session that writes
  continuously keeps the freelist and postpones the rebuild until it goes quiet,
  so a long fetch run still ends with a large database until the app idles.
- The evidence points at maintenance, but the per-step timings now recorded will
  confirm which step held the earlier 10 s reads on databases that predate
  compaction (2026-10-14).
- ~~**First content is the bottleneck.**~~ Resolved (2026-10-17). The 16.6–18.0 s
  content column in the table above predates the short window gate shipping; relaunches
  on the merged code measured window 5.37/5.38/5.79 s with first content
  5.42/5.42/5.83 s (`jobs_first_run_gate_evaluated` ~20 ms, render 30 ms) while
  `startup_sync_schedule` was *still running* at 11.9–13.7 s in the background. The
  page was not waiting on that step.
- The step itself was still real work: `schedule_startup_sync_pull()` measured
  13.75 s, of which `config_status` was 10 ms and the sync run's
  `task_lifecycle.start_run()` was 14.0 s. Inside that, the JSON write was 20 ms
  and the SQLite mirror was 15 s: `_mirror_rows_to_storage()` re-upserted all 168
  bounded `taskRuns` rows **one commit each** (~90 ms per `BEGIN IMMEDIATE` commit
  on the 473 MB runtime database). Fixed by mirroring the projection in one
  transaction (`TaskRuntimeStore.upsert_task_runs`, `executemany` inside a single
  `BaluffoStore.write()`; canceled-is-terminal preserved in the upsert predicate).
  The same journal step now measures **230–350 ms**, and the post-bind chain
  finishes ~0.3 s after bind instead of ~14 s. This also cuts every task
  start/finish transition, not just startup.
- Packaged recheck (rebuilt portable, same install and data directory):
  `startup_sync_schedule` **251 ms**, `desktop_bridge_started_hooks_finished`
  8.00 s after launch, window 7.95 s, `jobs_first_render` 8.17 s. First content
  tracks the window (`jobs_init_start` + 60 ms), so the remaining launch-cost
  variance is the site/bridge spawn path, not the post-bind steps. The packaged
  bridge child run alone reports the same step at 299 ms.
- The startup sync worker's own start is still only in the bridge log
  (`sync_task_started`) and the storage write timeline, not a metrics row; the
  `startup_sync_schedule` step duration is the recorded proxy. Adding a dedicated
  row would need room in the thin `admin_bridge` root, which is at its line cap.
- Two first-load costs are measured but not yet removed, both small next to the
  fixed ones: the startup sync pull's `ensure_active_registry` gzip+JSON parse of
  the source registry still runs on a worker thread inside the first seconds, and
  `availability_validator._fetch` imports `requests` lazily, so the first several
  workers serialize on the import lock. Revisit when a request thread is first
  seen waiting on that import, or when the registry artifact grows again.
- The launch-cost variance that remains is the site/bridge spawn path (window
  ~5.4–9.1 s after launch on the same install); re-measure it when the next
  portable rebuild ships, since it is not touched by any step in this plan.
- ~~The first `/app/update-status` after a launch still pays one artifact digest.~~
  Resolved (2026-10-18) by the first-paint work budget: the digest is warmed on a
  background thread when the API is built and a status poll only *waits* for it,
  bounded by `ARTIFACT_VERIFY_BUDGET_MS` (200 ms). See below.

## First-Paint Work Budget (2026-10-18)

Thread dumps during the first page load showed the remaining stall class clearly:
the request threads were not waiting on the SQLite write lock at all — they were
behind heavy startup work that had started the instant the API existed. The two
owners were the overdue availability catch-up (ten workers, each parsing the
lifecycle artifact, forked in `BridgeApi.__post_init__` *before* the listener
bound) and the artifact digest re-hashed on the update-status request thread.

`src/bridge/startup_work_budget.py` makes the ordering explicit: the accept loop
feeds the cumulative accepted-request count to a barrier, which opens once the
count has been quiet for 350 ms or once the 2 s budget expires. Queued work
(`availability_overdue_catchup`) runs on one serialized worker after the barrier,
and the post-bind startup chain waits for the same admission before its first
step. The digest runs in the background; a poll waits at most 200 ms and answers
from the last known status if the digest is not ready, so an unverified artifact
is never claimed and the install gate still verifies fully.

Measured on the live data directory, source bridge on the packaged entrypoint's
code path, page request set replayed 0.5 s after bind (`PROBE_PORT=8899`, one run
each, alternating):

| Metric | Budget 0 (control) | Budget 2000 (shipped) |
| --- | --- | --- |
| Whole request set | 1 448 / 1 614 / 1 648 ms | 504 / 779 / 1 028 ms |
| `/desktop-local-data/session` | 1 376 / 1 540 / 1 577 ms | 489 / 674 / 926 ms |
| `/desktop-local-data/saved-job-keys` | 1 062 / 1 178 / 1 262 ms | 121 / 304 / 530 ms |
| `/ops/health` | 1 397 / 1 565 / 1 598 ms | 453 / 727 / 969 ms |
| `availability_overdue_catchup` | starts at bind, 1.75–1.89 s | starts after the barrier, 1.22–1.59 s |

`/app/update-status` on the same runs: 696 ms on the first poll (the catch-up was
still overlapping it), 128 ms on the second, then 4 ms — with
`downloadState: downloaded` / `installState: ready` from the first poll onward, so
the badge never flickers while the digest reads.

The probe's readiness check is itself a TCP connection, so the barrier counted it
as a client and opened ~600 ms after bind, mid-replay. A real page opens its
requests immediately after its first connection, so the barrier lands after the
burst; the table above is therefore conservative.

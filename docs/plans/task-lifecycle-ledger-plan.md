# Task Lifecycle Ledger Closeout Plan

> - **Status:** Active closeout plan
> - **Use this when:** finalizing Admin task lifecycle authority, Current Runs / Recent Runs consistency, task progress projection, pipeline child ownership, or the last packaged lifecycle smoke before moving on
> - **Canonical for:** the remaining task lifecycle ledger closeout work and the consolidated task/progress operational console follow-up
> - **Not canonical for:** route payload contracts, report schemas, fetch/discovery output contracts, or release procedures
> - **Then inspect:** [`admin-bridge-api.md`](../admin-bridge-api.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`testing.md`](../testing.md), [`runtime-storage-and-sync-architecture-plan.md`](runtime-storage-and-sync-architecture-plan.md), `src/bridge/task_lifecycle.py`, `src/bridge/ops_api.py`, and `src/bridge/pipeline_service.py`
> - **Last updated:** 2026-05-08

## Summary

This page is the single active tracker for closing the Admin task/progress lifecycle work.

The older task/progress operational console tracker is closed: Admin task/progress logs now use the shared task-run presenter for run-state labels, compact history remains compact, and detailed operator evidence stays behind analysis/disclosure surfaces. That work has no active follow-up here except where it depends on correct lifecycle rows and progress evidence.

The lifecycle ledger work is mostly implemented. In the current JSON-backed runtime, `data/admin-task-lifecycle.json` is the backend authority for task identity, liveness, parent/child ownership, start/end timestamps, and terminal state. Reports, live task artifacts, logs, and sync runtime files are evidence sources for display progress and operator detail; they do not own lifecycle state.

The final goal is to remove the remaining ambiguity between lifecycle authority and operational evidence, add the last regression coverage for the 2026-05-08 packaged findings, and perform one real packaged pipeline validation before this plan is archived. This closeout does not block the later SQLite/WAL migration in [`runtime-storage-and-sync-architecture-plan.md`](runtime-storage-and-sync-architecture-plan.md); that plan may move the same lifecycle authority from JSON into SQLite `task_runs` after its own cutover gates pass.

## Current State

Done:

- `TaskLifecycleService` exists in `src/bridge/task_lifecycle.py` and persists lifecycle rows to `data/admin-task-lifecycle.json`.
- Fetch, discovery, sync, and pipeline launch/terminal paths create or finish lifecycle rows instead of using `admin-run-history.json` as lifecycle authority.
- Pipeline child ownership is explicit through `parentRunId` / `parentTaskType`, with child rows attached to the parent pipeline.
- Pipeline discovery/fetch waits use quiet-evidence timeouts plus a distinct absolute safety-cap terminal reason instead of failing only because a nominal duration elapsed.
- Bridge startup no longer imports legacy history/state rows into the lifecycle ledger as normal runtime behavior; legacy import is explicit migration/test tooling.
- `/ops/history` reads recent lifecycle rows, and `/ops/task-state` starts from current lifecycle rows.
- Admin Current Runs no longer keeps missing backend active rows alive for an extra frontend sample.
- Active fetch rows in `/ops/task-state` use the shared fetch live projection, so Current Runs and Selected Run Analysis receive the same fetch progress shape as `/ops/task-live/fetch`.
- Fetch live projection merges same-run `jobs-fetch-tasks.json`, `jobs-fetch-report.json`, and lifecycle identity; it recomputes `ratio` from counts and keeps key count fields monotonic across evidence sources.
- Older adjacent JSON journals no longer shadow newer canonical JSON files in `load_json_object()`. A journal overlay only wins when the journal file is newer than the canonical JSON.
- Pipeline child liveness rejects lifecycle rows whose recorded `ownerPid` is no longer running, unless newer report/task evidence is observed through the normal evidence path.
- Fetch lifecycle closeout treats source-level `failedSources` as operational evidence, not task failure. Only explicit failed/error status or `summary.error` fails the fetch lifecycle row.
- The shared task-run presenter adoption from the operational console tracker is complete through `frontend/shared/task-run-view-model.js`, `frontend/admin/app/fetcher/report.js`, `frontend/admin/app/fetcher/watch.js`, and `frontend/admin/app/discovery/progress.js`.

Still intentionally present:

- `admin-task-state.json` and `admin-run-history.json` helpers remain for explicit migration, tests, and maintenance compatibility. They must not become normal lifecycle authority again.
- Fetch/discovery reports, fetch task files, logs, sync live task files, and bridge events remain valid operational evidence artifacts.
- Downloadable diagnostics export remains outside this lifecycle closeout unless a separate support workflow asks for it.

## 2026-05-08 Packaged Findings

The packaged run exposed two concrete failure classes.

Fetch progress drift:

- Fetcher Output showed fresher counts than Current Runs / Selected Run Analysis.
- The row path was using stale lifecycle progress while the fetcher log and report/task artifacts had newer evidence.
- Code inspection now shows the fetch path has been moved through `ops_task_fetch_live.build_fetch_live_payload()`, with count-based ratio recomputation.
- Remaining work is route-level regression coverage and packaged validation, not another frontend merge workaround.

Pipeline stuck after fetch completion:

- A fetch child completed, but stale `jobs-fetch-report.jsonl` evidence caused the bridge to miss terminal `jobs-fetch-report.json` and later mark the child `owner_inactive_without_terminal_report`.
- Code inspection now shows older journals cannot override newer canonical JSON, pipeline child liveness checks owner PID, and fetch completion with source-level failures closes as completed.
- Remaining work is a regression that exercises the route/pipeline path with stale journal plus terminal JSON, then a real packaged run proving the parent pipeline advances out of `stage=fetch`.

## Remaining Closeout

Only these steps should block final closure.

1. Finish runtime evidence IO hardening.
   - Preferred final state: fetch/discovery reports, fetch task files, sync live task files, lifecycle rows, logs, and bridge event/output files use a runtime-evidence JSON reader that reads the requested canonical file directly unless that artifact explicitly opts into journal semantics.
   - Minimum acceptable state: keep the current newer-journal-only overlay behavior, document it as intentional for runtime artifacts, and cover the packaged stale-journal failure at route/pipeline level.

2. Add the last regression coverage for the packaged failures.
   - Cover stale `jobs-fetch-report.jsonl` plus newer terminal `jobs-fetch-report.json` through `/ops/fetch-report?view=live` and the pipeline wait path.
   - Cover completed fetch reports with normal `failedSources > 0` closing as lifecycle `succeeded/completed`, not `failed`.
   - Cover `/ops/task-state` active fetch rows matching the shared live fetch projection when lifecycle progress is stale.

3. Finish active task-state projection parity.
   - Fetch is already routed through shared live projection.
   - Either route active discovery and sync summary rows through their shared live projections too, or document why fetch-only enrichment is intentional.
   - Pipeline child summaries must not display stale copied lifecycle progress when fresher child evidence exists.

4. Narrow legacy lifecycle helper exposure.
   - Audit production routes and task launch paths for `admin-task-state.json` / `admin-run-history.json` reads, writes, prune calls, and duplicate-start checks.
   - Keep old-run recovery behind explicit migration/test/maintenance functions with names that make the boundary obvious.
   - Remove stale constructor parameters and facade wiring after the production call graph no longer needs them.

5. Run the final packaged validation.
   - Build the portable app.
   - Run a real jobs pipeline long enough to exceed the old nominal discovery/fetch timeout window.
   - Confirm discovery remains current while live, fetch progress in Current Runs / Selected Run Analysis / Fetcher Output converges within one `/ops/task-state` poll, `/ops/fetch-report?view=live` reads the current report, the parent pipeline advances out of fetch after terminal report evidence, and all child/parent rows land in Recent with terminal timestamps.

6. Close the docs after validation.
   - Update [`admin-bridge-api.md`](../admin-bridge-api.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and [`testing.md`](../testing.md) only if the final IO/projection changes alter their contracts or verification commands.
   - Archive this page after the packaged validation passes and no production lifecycle authority remains outside the current lifecycle backend. For the present closeout that backend is `data/admin-task-lifecycle.json`; a later storage migration may replace it with SQLite `task_runs` through the runtime-storage plan.

## Closure Criteria

This work is done when:

- In the current JSON-backed runtime, `data/admin-task-lifecycle.json` is the only production authority for task identity, liveness, parent/child ownership, timestamps, and terminal state.
- Reports and task artifacts can enrich progress and summaries but cannot independently mark a task live, failed, canceled, or orphaned.
- `/ops/task-state`, `/ops/history`, `/ops/task-live/<taskType>`, and selected-run analysis agree on the same active run identity and terminal state.
- Active fetch progress is computed from same-run evidence counts and cannot move backward because of stale lifecycle snapshots.
- Stale runtime `.jsonl` files cannot shadow newer canonical runtime JSON in the fetch report route or pipeline wait path.
- Normal source-level fetch failures remain operator evidence and retry input, not task-level lifecycle failure.
- Legacy `admin-task-state.json` and `admin-run-history.json` behavior is either removed from production paths or explicitly isolated as migration/test/maintenance tooling.
- Focused backend/frontend regression tests and one packaged pipeline smoke cover the 2026-05-08 failures.

## Historical Routing

- The old operational console closeout is now a short historical pointer at [`../archive/task-progress-operational-console-closeout.md`](../archive/task-progress-operational-console-closeout.md).
- The unindexed underscore note [`task_lifecycle_ledger_closeout_plan.md`](task_lifecycle_ledger_closeout_plan.md) is superseded by this page and should not be extended.

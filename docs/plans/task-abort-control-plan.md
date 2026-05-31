# Task Abort Control Plan

> - **Status:** Implemented baseline, refinement-ready
> - **Use this when:** adding or refining abort support for discovery, fetch, first-run bootstrap fetches, or jobs pipeline tasks
> - **Canonical for:** abort scope, lifecycle safety rules, public API shape, known loopholes, implementation sequencing, and verification gates
> - **Not canonical for:** pause support or standalone sync abort
> - **Last updated:** 2026-06-01 (abort report race clarification)

## Summary

Add user-requested task abort support only for these lifecycle-managed tasks:

- `discovery`
- `fetch`
- first-run bootstrap fetches, represented as `taskType: "fetch"` with bootstrap metadata
- `pipeline`

Pause is out of scope. Standalone sync abort is out of scope. If a pipeline is already in `sync_push`, the abort request is accepted and recorded, the active sync push is allowed to finish safely, and only the pipeline parent is marked `canceled` after sync completes.

Expected implementation complexity is medium-high: about 4.5-6 engineering days including backend race hardening, process-control safety, frontend controls, docs, route inventory updates, and packaged desktop smoke gates.

The high-risk part is lifecycle authority, not the route. Delayed watchers, report finalizers, startup cleanup, history projection, or storage mirroring must never convert a user-aborted task back into `failed`, `succeeded`, or `orphaned`.

## Decisions Locked

- Frontend confirmation: Jobs and Admin Ops must both confirm before posting an abort request.
- Terminal-report race: if matching terminal report evidence already exists before abort intent is recorded, abort returns `409` and normal finalization owns closeout.
- Pipeline child terminal race: when aborting a pipeline, child terminal evidence that already exists before child abort intent is recorded is finalized normally; only the pipeline parent cancellation continues.
- Bridge-restart PID fallback: after a restart, terminate by PID only when command/run identity can be verified. If identity cannot be verified, keep abort intent active, warn, and let cleanup close it when the PID exits.

## Scope And Public Interface

Add one bridge endpoint:

```http
POST /tasks/abort
```

Request body:

```json
{
  "taskType": "fetch",
  "runId": "fetch_abc123",
  "reason": "optional user text"
}
```

Supported `taskType` values:

- `fetch`
- `discovery`
- `pipeline`

Rejected `taskType` values:

- `sync`
- any unknown task type

Response rules:

- `200`: abort accepted, already aborting, already canceled, or pipeline sync-stage abort deferred.
- `400`: unsupported task type, missing `runId`, malformed body, or unknown task type.
- `404`: no matching lifecycle row exists.
- `409`: matching run is already terminal with a non-canceled status, or matching terminal report evidence existed before abort intent was recorded.

Response body shape:

```json
{
  "ok": true,
  "abortAccepted": true,
  "aborted": false,
  "deferred": false,
  "taskType": "fetch",
  "runId": "fetch_abc123",
  "state": "aborting",
  "terminalReason": "user_abort_requested",
  "warnings": []
}
```

Do not support abort-by-type. `runId` is required so the abort cannot accidentally target a newer task of the same type.

Do not add a new canonical lifecycle status named `aborting`. `data/admin-task-lifecycle.json` only supports `queued`, `running`, `succeeded`, `failed`, `canceled`, and `orphaned`. Represent in-progress abort as:

- `status: "running"`
- `stage: "aborting"` or `stage: "abort_pending_sync"`
- `summary.abortRequestedAt`
- `summary.abortReason`
- `taskProgress.active: true`
- `taskProgress.phaseKey: "aborting"`

Only after termination completes should the row become:

- `status: "canceled"`
- non-empty `finishedAt`
- `terminalReason: "user_abort_requested"` or a more specific abort reason
- `taskProgress.active: false`

## Implementation Sequence

1. Add a long-lived bridge-local `TaskAbortService`.
   - Keep it under `src/bridge/` as a leaf service.
   - Own it from bridge runtime/root service state so it survives `TaskLaunchApi` helper construction.
   - Wire it through `BridgeApi`, `build_bridge_api`, and `post_routes_admin.py`.
   - Keep route validation small and explicit.

2. Add a long-lived `TaskProcessRegistry`.
   - Key entries by `(taskType, runId)`.
   - Store `Popen`, PID, started time, command metadata, and task subtype metadata such as bootstrap staging directory.
   - Update `TaskLaunchApi.run_background_script` so launch callers can register the `Popen` handle while preserving the existing PID return contract.
   - Prefer the registered `Popen` for termination.
   - Use PID fallback only for active lifecycle rows whose command/run identity can be verified after restart.

3. Preserve process identity for pipeline-owned children.
   - Current pipeline child attachment can make a process-backed child look `ownerKind: "pipeline"`.
   - Do not remove the ability to locate or terminate the child process when attaching parent metadata.
   - Either preserve process `ownerKind` and add separate parent ownership metadata, or add explicit execution-owner fields while keeping parent linkage separate.

4. Add an atomic lifecycle abort-intent operation before enabling the route.
   - Implement a method such as `request_abort_run(...)` inside the lifecycle service.
   - Under the lifecycle lock, validate the row is active, verify the target `runId`, check for terminal evidence, and write abort intent in one operation.
   - Do not implement abort intent as separate read plus heartbeat calls.
   - Return a stable result object describing accepted, already aborting, already canceled, missing, or terminal-race states.

5. Harden terminal lifecycle writes.
   - Make `canceled` sticky in `TaskLifecycleService`.
   - Make SQLite task runtime terminal writes follow the same precedence.
   - Ensure legacy mirroring and startup cleanup cannot overwrite a canceled row.
   - Keep `heartbeat_run` behavior unchanged: heartbeats may observe terminal rows but must not reactivate them.

6. Make terminal projection canceled-aware.
   - Lifecycle `canceled` must be authoritative in task-state and history projection.
   - Fetch/discovery report summary builders must not reclassify canceled lifecycle/report evidence as `ok`, `warning`, or `error`.
   - Terminal canceled runs should render as canceled in Admin Ops and Jobs completion surfaces.

7. Implement safe process-group termination.
   - Windows: include process-group creation where available and terminate via bounded `taskkill /T /F`.
   - POSIX: launch abortable children in a new session/process group, send TERM, then bounded KILL if needed.
   - Do not import desktop-app composition/root modules into bridge helpers for termination. Use a bridge-local leaf helper or carefully shared leaf module.

8. Implement two-phase process abort.
   - Phase A: atomically record abort intent while lifecycle remains active.
   - Phase B: terminate the registered process tree and wait briefly for process exit.
   - Phase C: after process death, write final canceled lifecycle row and repair the report/task-progress evidence.
   - If termination is still in progress after the bounded wait, return `state: "aborting"` and leave the row active so desktop active-work detection remains correct.

9. Handle bridge restart during abort.
   - Persist abort intent in lifecycle summary/progress, not only in memory.
   - Startup cleanup must detect active rows with abort intent.
   - If the verified process is still alive, replay termination.
   - If the process is dead, close as `canceled`, not `orphaned`.
   - If process identity cannot be verified, keep the row active, include a warning, and let cleanup close it when the PID exits.

10. Make fetch closeout abort-aware.
    - `watch_fetch_lifecycle` must check abort intent before converting dead owners into `failed`.
    - `close_fetch_lifecycle_from_report` must not mirror/promote a successful report if abort was requested before terminal report evidence existed.
    - Aborted fetch reports should be repaired to terminal canceled evidence with inactive progress.

11. Make bootstrap closeout abort-aware.
    - Treat bootstrap as `taskType: "fetch"` with bootstrap metadata.
    - Do not promote staged artifacts after abort.
    - Clean the bootstrap staging directory once the process exits.
    - Clear `_active_bootstrap_processes` and repair the fetch report/tasks evidence as canceled.

12. Make discovery closeout abort-aware.
    - `watch_discovery_run_for_auto_sync` must close as canceled when abort intent exists.
    - Skip discovery auto-approval and auto-sync for aborted runs.
    - Repair discovery report evidence to terminal canceled with inactive progress.

13. Add cooperative pipeline abort.
    - Add lock-protected abort state to `PipelineRuntime` or the pipeline service status model.
    - Add a specific `PipelineAbortRequested` exception or equivalent non-error path.
    - Check abort before each stage starts.
    - Check abort while waiting for discovery/fetch child reports.
    - On pipeline abort during an active discovery/fetch child, call the child abort path.
    - On direct abort of a pipeline-owned child, propagate cancellation to the parent pipeline.

14. Handle pipeline `sync_push` exactly as scoped.
    - Do not attempt to stop the sync task.
    - Mark the pipeline row active with `stage: "abort_pending_sync"`.
    - Continue waiting for sync completion.
    - After sync terminal evidence arrives, mark the pipeline parent `canceled`.
    - The sync child keeps its normal terminal status.

15. Avoid overpromising registry adjudication abort.
    - If abort is requested before the registry adjudication stage starts, skip it.
    - If abort is requested after the existing adjudication call has started, do not attempt to kill that independent work.
    - Cancel the pipeline after the call returns and include a `registry_adjudication_not_aborted` warning when relevant.

16. Update frontend controls.
    - Admin Ops current rows show Abort only for active `fetch`, `discovery`, and `pipeline` rows with a run id.
    - Do not show Abort for `sync`.
    - Jobs page keeps the existing live update/progress label as the default button text for active supported work.
    - On fine-pointer mouse hover and keyboard focus, the Jobs update button label changes to `Abort update`.
    - On mouse leave or blur, the Jobs update button restores the live update/progress label.
    - Clicking the Jobs update button while it is in abort-reveal state opens the abort confirmation and then posts `/tasks/abort`.
    - On coarse-pointer or touch devices, do not make abort hover-only; expose a visible compact abort affordance next to the progress-bearing update control.
    - Jobs page remains non-abortable for standalone sync-only blocking work.
    - Confirm before posting an abort request from either UI.
    - Show `Aborting...` while lifecycle remains active after accepted abort.

17. Update docs and route inventory.
    - Update `docs/admin-bridge-api.md`.
    - Update `docs/DATA_CONTRACT.md` for abort progress markers and canceled terminal evidence expectations.
    - Update `tools/repo_health/bridge_route_inventory.py`.
    - Update `tools/repo_health/generate_system_map.py` and any generated/checkable system map expectations if needed.
    - Update `src/bridge/config.py` task route summary.
    - Update `docs/testing.md` if the required verification lane changes.

## Loopholes And Required Fixes

| Loophole | Required fix |
|----------|--------------|
| Late watcher overwrites `canceled` as `failed` or `succeeded` | Make `canceled` sticky across lifecycle, SQLite runtime, legacy mirroring, cleanup, and projection paths |
| Abort intent races with watcher terminalization | Add an atomic lifecycle `request_abort_run` operation |
| Abort marks row terminal before process exits | Use two-phase abort and keep row active while termination is in progress |
| Desktop close sees false idle during abort | Keep `status: "running"` and active task progress until the process or pipeline is actually terminal |
| Process registry is attached to short-lived helper instances | Own the registry from long-lived bridge runtime/root service state |
| Pipeline child attachment hides the child process identity | Preserve execution-owner/process metadata separately from parent linkage |
| PID-only process kill can hit the wrong process after PID reuse | Prefer registered `Popen`; after restart, terminate by PID only after command/run identity verification |
| POSIX child spawns descendants outside the killed PID | Launch abortable children in a new session/process group and terminate the group |
| Bridge restarts during abort | Persist abort intent in lifecycle summary/progress and have startup cleanup replay cancel-or-terminate semantics |
| Child report finishes at the same time as abort | Abort repair may overwrite finished evidence only after abort intent is authoritative. If terminal child evidence already exists before child abort intent, normal child finalization wins and the pipeline parent may still cancel. |
| Report summary builders reclassify canceled as ok/error | Make lifecycle canceled authoritative in history/task projections and report display |
| Pipeline waits too long after child abort | Make report waits abort-aware and inspect child canceled lifecycle state |
| Direct child abort leaves parent pipeline active | Propagate pipeline-owned child cancellation to parent pipeline |
| Pipeline sync-stage abort accidentally kills sync | Never stop standalone sync; defer parent cancellation until sync completes |
| Registry adjudication is treated as abortable | Skip if not started; if already running, wait for return and warn |
| New route omitted from route inventory | Update route inventory and system map alongside route/docs changes |
| Jobs `Abort update` label hides live progress updates | Keep progress as the default label, reveal `Abort update` only on hover/focus, and provide a visible coarse-pointer fallback |

## Backend Acceptance Criteria

- `/tasks/abort` rejects unsupported `sync` abort with a stable `unsupported_task_abort` error.
- `/tasks/abort` rejects missing `runId` and does not abort by task type alone.
- An active process-owned fetch abort progresses from active aborting state to terminal canceled.
- An active bootstrap abort does not promote staging output.
- An active discovery abort does not auto-approve candidates or trigger auto-sync.
- A pipeline abort before sync produces terminal canceled without a generic error status.
- A pipeline abort during `sync_push` keeps sync running and cancels only the pipeline parent after sync terminal evidence arrives.
- Direct abort of a pipeline-owned discovery/fetch child cancels the parent pipeline.
- Late success/failure/orphan writers cannot change terminal canceled lifecycle rows.
- Terminal canceled rows remain canceled in `/ops/task-state`, run history, Admin Ops, and Jobs completion state.
- `/ops/task-state?view=summary` keeps aborted-in-progress tasks visible until termination completes.

## Frontend Acceptance Criteria

- Admin Ops shows Abort only for active supported current runs.
- Admin Ops does not show Abort for sync rows.
- Jobs update control can abort an active pipeline/fetch/discovery/bootstrap task.
- Jobs update control keeps live update/progress text visible by default while abort is available.
- Fine-pointer hover and keyboard focus reveal `Abort update`; leaving hover or focus restores the live update/progress text.
- Coarse-pointer and touch layouts expose abort through a visible compact affordance, not hidden hover-only behavior.
- Jobs update control remains non-abortable for sync-only blocking work.
- Jobs and Admin Ops both ask for confirmation before posting abort.
- Accepted abort shows an in-progress abort state until lifecycle becomes terminal.
- Terminal canceled runs render as canceled, not failed.

## Test Plan

Run focused tests first:

- Route tests for supported, unsupported, missing `runId`, missing row, terminal row, already-aborting, already-canceled, terminal-report-too-late, and sync-deferred cases.
- Lifecycle tests proving atomic abort intent rejects terminal races and `canceled` cannot be overwritten by finish/fail/orphan paths.
- SQLite task runtime tests proving storage upserts cannot overwrite terminal canceled rows.
- Process registry tests proving registry lifetime across service calls.
- Process termination tests with mocked `Popen`, mocked Windows `taskkill`, mocked POSIX process-group TERM/KILL, verified PID fallback, and unverified PID warning.
- Startup cleanup tests proving abort-intent rows close as canceled, not orphaned, after dead process evidence.
- Fetch watcher tests proving abort requested plus dead PID becomes canceled, not failed.
- Bootstrap watcher tests proving aborted runs clean staging and skip promotion.
- Discovery watcher tests proving aborted runs skip auto-approval and auto-sync.
- Pipeline tests for abort before child start, during discovery, during fetch, during registry adjudication, during sync push, and direct child abort propagating to parent.
- Task-state and history projection tests proving aborting rows stay active and terminal canceled rows leave current runs while still rendering as canceled in recent history.
- Frontend unit tests for Admin Ops abort rendering, confirmation, disabled/in-flight state, and Jobs `Abort update` behavior.

Then run broader gates:

- `python -m pytest` on the nearest bridge/task lifecycle suites.
- `npm run test:frontend:unit` or the narrower affected frontend unit suites.
- Bridge route inventory test.
- `npm run test:frontend:packaged:active-task-close-rehearsal` because abort changes active critical task lifecycle visibility.

## Assumptions

- No new Python or Node dependencies.
- No new canonical lifecycle status.
- No standalone sync abort.
- No pause support.
- No automatic abort on desktop window close; existing active-work close/shutdown intent remains separate.
- Abort is user-requested cancellation, not a retry or rollback mechanism.
- Partial external side effects are not rolled back unless the existing task already owns a safe transaction boundary, such as bootstrap staging promotion.
- Repo source, tests, docs, and AGENTS.md remain canonical over memory notes.

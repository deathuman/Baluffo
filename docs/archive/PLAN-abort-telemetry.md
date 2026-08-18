# Plan: `/tasks/abort` non-blocking + abort path telemetry

> - **Status:** Superseded (archived 2026-08-17)
> - **Use this when:** following old links or git history that referenced this precursor plan
> - **Canonical for:** nothing; use the implemented baseline in [`task-abort-control-plan.md`](../plans/task-abort-control-plan.md)
> - **Not canonical for:** abort behavior, telemetry contracts, or task-lifecycle scope
> - **Then inspect:** [`task-abort-control-plan.md`](../plans/task-abort-control-plan.md)
> - **Last updated:** 2026-08-17

This precursor plan recorded the original `/tasks/abort` benchmark and root-cause investigation. The follow-up
[`../plans/task-abort-control-plan.md`](../plans/task-abort-control-plan.md) is the implemented baseline and
is the active reference for abort and abort-path telemetry behavior.

## Context

Local container benchmark (`_out/perf-admin-flows/20260802-172657-495192/`) on pi4-tight, image 0.2.128, seeded with 2 674 active sources / 567 conflict rows / 37 MB job lifecycle state:

| Leg | Result |
| --- | --- |
| `POST /tasks/run-fetcher {preset:retry_failed}` | 200 in ~1.2 s (admits, spawns background fetcher) |
| `POST /tasks/abort {taskType:"fetch", runId:<id>}` | **No response within 90 s sampler timeout** |

The admin "Stop fetch" button is unusable on a live run.

## Root cause

`abort_task` in `src/bridge/task_abort_service.py` is fully synchronous on the request thread:

1. `_validate_abort_request` — reads `admin-task-lifecycle.json` (fast).
2. `request_abort_run(...)` — flips the row to `running/aborting` inside `_TASK_LIFECYCLE._lock` (fast).
3. `_abort_process_run(request)`:
   - `_propagate_child_abort_to_pipeline` — pipeline RPC (usually fast).
   - `_abort_process_task` → `process_registry.terminate(..., timeout_s=3.0)`.
     - POSIX path: SIGTERM, `wait(timeout=3s)`, escalate SIGKILL, `wait(1s)` → **up to 4 s**.
     - If `exited` is True, `_cancel_fetch(run_id)` runs `repair_fetch_canceled_evidence` (rewrite + normalize the full fetch report JSON) **on the request thread** → hundreds of ms to seconds on a 40 MB report.
4. Then `_lifecycle_abort_response` / `_response`. Then the trailing `bridge_log("info", "task_abort_requested", ...)`.

Each step individually is bounded; the sum is not. With 1.5 CPU the `repair_fetch_canceled_evidence` leg alone can take tens of seconds on a large report because it rewrites `jobs-fetch-report.json` + `jobs-fetch-tasks.json` and normalizes for contract. Worse, the HTTP thread is blocked while the fetcher child hash-joins over `jobs-source-state.json` (lock contention on `_TASK_LIFECYCLE._lock` when the worker reports progress).

There is also no telemetry on any of these legs. We couldn't tell from `/ops/performance-profile` what actually ate the 90 s without manually instrumenting: the only current signal is the outer `record_route_duration` from `server/handler.py`, which never fired because the response never finished.

## Plan

Two patches, one PR each, both shippable on their own.

### Patch 1 — Async abort

Restructure `abort_task` to return as soon as the abort intent is durably recorded, and run the process-kill + report-repair off the request thread.

Diff sketch (`src/bridge/task_abort_service.py`):

- Keep `_validate_abort_request` and `request_abort_run` synchronous (they're fast and need to flip the row to `aborting` *before* we answer).
- Replace the inline call to `_abort_process_run` with a one-shot background job:

```python
self._run_async_abort(request, stage)  # fire-and-forget
return 202, self._response(
    ok=True,
    task_type=request.task_type,
    run_id=request.run_id,
    state="aborting",
    abort_accepted=True,
    aborted=False,             # not yet — see /ops/task-live/<type>?view=summary
    deferred=True,
)
```

- `_run_async_abort` runs `_abort_process_run` (or `_abort_pipeline`) on a daemon thread, then `bridge_log("task_abort_completed", ...)` with `durationMs` per stage. All errors are already captured into `warnings` by `_abort_process_task` — bubble them via the lifecycle row's `summary.abortWarnings` and the bridge log.

- Tests: existing abort tests are green either way (they drive the full sequence then poll for the terminal row). New test:POST abort returns `<500 ms` and `state="aborting"` with a stub process that hangs for 5 s.

- Compatibility: UI polls `/ops/task-live/fetch?view=summary` already; the contract stays `{ok, abortAccepted, state, runId, taskType, deferred, warnings}`. `aborted:true` only in the "already terminal / already canceled" branches, which still respond synchronously.

### Patch 2 — Per-stage abort telemetry

Right now the only signal is the outer `record_route_duration` in `server/handler.py`, and it never fires when the request hangs. Add the smallest instrumentation that would have caught this benchmark run.

- Add a `record_operation_duration("abort.<stage>", duration_ms, status="ok"|"error")` call at each existing seam inside `abort_task` and the async worker:
  - `abort.validate`
  - `abort.flip_lifecycle_row`
  - `abort.propagate_to_pipeline`
  - `abort.process_terminate` (per attempt: `posix_sigterm`, `posix_sigkill`, `windows_terminate`)
  - `abort.cancel_evidence` (`repair_fetch_canceled_evidence` / `repair_discovery_canceled_evidence`)
  - `abort.finalize` (the trailing `bridge_log` + response write)
- Reuse `src/bridge/performance_profile.py` — it already has `record_operation_duration`, `_operation_samples`, percentiles, and is bounded.
- Surface them in `/ops/performance-profile` under `operationTimings.operations` (no schema change, the consumer is generic).
- Add a new task-type tag on the lifecycle row during abort so `/ops/task-live/<type>?view=summary` shows `summary.abortStage: "<stage>"` and `summary.abortStageElapsedMs` while the async leg runs. The Admin UI then sees *what* is taking long without waiting for full termination.

Wire-up rule: `record_operation_duration` accepts a label and duration; do *not* introduce a tags/kwargs API. Pass the stage in the label string. Keeps the diff ~25 LOC.

### Verification

- Local: `npm run perf:admin:flows:roomy -- --image <local-build> --data-volume _out/perf-admin-flows/seed-data`. Success = `admin.fetcher.trigger` flow `abort` leg `< 1 000 ms` in both cold and warm runs, and `/ops/performance-profile` shows the new `abort.*` operation rows.
- Existing: `npm run test:py:extended` and packaged smokes for task abort (the `task-abort-schedule-rehearsal` lane in `release:preflight`).
- CI: Tests + Build Container green on the PR; the Manual `perf-admin-flows` workflow against the fresh GHCR image confirms no 90 s stalls at runtime.

## Out of scope (deferred)

- Pagination on `/registry/conflicts` (≈11 MB). Cache already shipped in 0.2.127; warming on startup is a separate small patch.
- `/admin/ops-tab-counts?view=summary` persistent cache. Bigger but not on the abort critical path.
- Sampler hardening (`Content-Length: 0` on body-less POSTs, distinguishing gateway-generated 504s from bridge-generated ones).

Each is small enough that ordering should follow this plan's PR order.

## Rollback

Patch 2 is purely additive and self-contained; revert is one PR.
Patch 1 has a single behavior change visible to older clients: `aborted` reports `false` for ~1 fetcher-progress tick while the kill lands. Documented in release notes; rollback = revert PR, no data migration.

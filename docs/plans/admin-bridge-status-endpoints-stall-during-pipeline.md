# Admin Bridge Status Endpoints Stall During Pipeline

Date: 2026-05-07

## Problem

During a live jobs pipeline run, the pipeline itself can continue progressing while admin status endpoints stall. This makes the Admin UI look unhealthy or contradictory: run history and pipeline status can show live work, while the bridge/status surfaces time out or report stale/offline state.

## Live Evidence

Observed during packaged build `0.1.33` with pipeline run `pipeline_209f8ed0e0`.

- Pipeline status endpoint was responsive:
  - `GET /tasks/run-jobs-pipeline-status` returned in about `0.19s`.
  - Reported `active: true`, stage `fetch`, progress `2/3`, label `Running fetch...`.
- Fetch child was actively progressing:
  - Fetch run `fetch_240e329556`.
  - Heartbeat was fresh at `2026-05-07T11:10:18Z`.
  - Progress at the time: `1,228 / 2,178` sources resolved, `939` queued, `11` running, `1,095` ok, `133` error.
  - `jobs-fetcher.log` showed new `START`, `DONE`, and `ERROR` rows arriving continuously.
- Admin status endpoints were not healthy:
  - `GET /ops/health` timed out after `30s`.
  - `GET /ops/task-state` timed out after `30s`.
  - These routes timed out while the pipeline status route remained fast.

## Related UI Symptom

The bridge pill previously used `/registry/summary` to decide online/offline state. That could mark the bridge offline even when the bridge was alive. This has been changed to use `/ops/health`, but this investigation shows `/ops/health` itself can stall during heavy fetch work, so the underlying status-route performance issue remains.

## Additional UI State Bug: Stale Discovery Current Run

The Run History UI can also show stale discovery state after the pipeline has already moved on. In the observed UI, `Current Runs` showed both `discovery` and `pipeline` as `Stalled`, with discovery progress like `Probing 872 candidate(s) (74%) | stage 10/11`. The selected pipeline analysis also showed `Pipeline step 1/3`, `Discovery: Probing 872 candidate(s) (33%)`, `No recent heartbeat`, and `No completed runs yet`.

That was wrong for the live backend state:

- The discovery child had already completed successfully in lifecycle state.
- The pipeline had advanced to the `fetch` stage.
- `GET /tasks/run-jobs-pipeline-status` was returning the active pipeline as `stage: fetch`.
- Fetch heartbeat and `jobs-fetcher.log` showed active progress.

This suggests the Run History/current-runs view is reading stale history or lifecycle projections, or it is failing to reconcile child task completion with parent pipeline stage transitions when heavier Admin/Ops endpoints time out.

## Suspected Cause

The bridge process is responsive for narrow, in-memory pipeline status but slow or blocked for heavier Admin/Ops routes. Likely contributors:

- `/ops/health` and `/ops/task-state` may read, normalize, or summarize large JSON artifacts while the fetch worker is writing large reports.
- Large files observed during the run include:
  - `jobs-fetch-report.json` around multiple MB.
  - `jobs-fetch-tasks.json` around multiple MB.
  - large discovery report/history artifacts.
- These routes may be doing too much synchronous file or report work on the bridge request path.

## Separate Issue Seen During Same Run

Packaged Playwright fallback appears broken for at least one source path:

- `breezy_sources` failed with a missing executable error for `chromium_headless_shell`.
- This means sources requiring browser fallback may fail or be under-covered in packaged builds.
- This is separate from the bridge status timeout, but it was visible in the same pipeline run.

## Desired Behavior

- `/ops/health` should remain lightweight and fast even during discovery/fetch/pipeline runs.
- `/ops/task-state` should return current active task rows quickly, ideally from small lifecycle/task-state artifacts or in-memory state.
- Heavy report summaries should be separated from bridge liveness and current task status.
- Run History should reconcile pipeline stage and child task status so completed discovery work does not remain displayed as the current active phase.
- Admin UI should degrade partial sections independently instead of blocking core liveness/current-run status.

## Proposed Fix Direction

1. Profile `/ops/health` and `/ops/task-state` during an active pipeline run.
2. Split liveness/current task state from heavy report-derived sections.
3. Make `/ops/health` avoid large report reads on the critical path.
4. Make `/ops/task-state` use lightweight lifecycle/task-state files only, with strict fallback behavior.
5. Fix Run History/current-runs reconciliation so a completed discovery child is not shown as the active pipeline phase once the parent has advanced to fetch.
6. Add regression tests that simulate large fetch/discovery reports and assert fast health/task-state responses.
7. Add a packaged check for Playwright browser fallback availability or make browser fallback optional/degraded with a clearer packaged diagnostic.

## Acceptance Criteria

- During an active fetch stage, `GET /tasks/run-jobs-pipeline-status`, `GET /ops/health`, and `GET /ops/task-state` all return within a small timeout.
- The bridge pill does not show offline while current-run endpoints are responsive.
- Run History shows the pipeline in `fetch` after discovery completes, rather than leaving discovery as a stalled current run.
- Run history can lag or partially degrade without blocking bridge health, but stale rows must be labeled as stale instead of presented as the current active phase.
- Packaged browser fallback either works or reports a non-fatal, explicit degraded capability state.

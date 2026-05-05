# Task/Progress Operational Console Closeout

> - **Status:** Archived closeout
> - **Use this when:** checking why the Admin task/progress console migration closed
> - **Canonical for:** the closeout decision for shared task-run presenter adoption, compact history rendering, analysis-panel behavior, and run-state presentation
> - **Not canonical for:** runtime task contracts, bridge routes, sync governance, or future diagnostics-export work
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), and [`admin-health-dashboard-console-closeout.md`](admin-health-dashboard-console-closeout.md)
> - **Last updated:** 2026-05-05

## Closeout

The active task/progress operational console tracker is closed.

The final implementation follow-up removed the remaining controller-local task-run status summaries from the user-facing Admin task/progress logs. Fetcher report logging, fetcher live heartbeat/summary logging, fetcher terminal completion logging, and discovery live heartbeat/summary logging now ask the shared task-run presenter for their run-state labels.

The shared presenter entrypoint is `buildTaskRunLogLabel` in `frontend/shared/task-run-view-model.js`. Controller-local evidence logs remain intentionally local: fetcher failures, failure buckets, slow source/stage details, discovery phase transitions, queue bursts, and failure clusters describe discrete operator evidence rather than the overall task-run state.

Downloadable diagnostics export remains intentionally deferred and was not part of this closeout.

## Closed Criteria

- Live status, run timeline, diagnostics, and completed-run history read from the shared task-run interpretation.
- Compact rows stay compact, and deeper evidence stays behind analysis or disclosure panels.
- Fetch, discovery, pipeline, and sync task types render consistently.
- Fetcher report, fetcher live, fetcher completion, and discovery live run-state log summaries use shared task-run view-model output.
- `normalizeOpsRuns` and `deriveAdminRunsModel` remain owned by the shared Admin runs model.

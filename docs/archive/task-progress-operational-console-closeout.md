# Task/Progress Operational Console Closeout

> - **Status:** Archived pointer
> - **Use this when:** checking why the old Admin task/progress operational console tracker is no longer active
> - **Canonical for:** historical closure of shared task-run presenter adoption only; later lifecycle closeout lives in [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md)
> - **Not canonical for:** current lifecycle ledger follow-up, runtime task contracts, bridge routes, sync governance, or diagnostics-export work
> - **Then inspect:** [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-05-12

## Closeout

The standalone task/progress operational console tracker is closed and was later folded into the archived lifecycle ledger closeout.

Completed historical scope:

- Fetcher report logging, fetcher live heartbeat/summary logging, fetcher terminal completion logging, and discovery live heartbeat/summary logging use the shared task-run presenter for run-state labels.
- The shared presenter entrypoint remains `buildTaskRunLogLabel` in `frontend/shared/task-run-view-model.js`.
- `normalizeOpsRuns` and `deriveAdminRunsModel` remain owned by the shared Admin runs model.
- Controller-local evidence logs remain local because they describe discrete operator evidence rather than overall task-run state.

No separate operational-console follow-up remains here. The later lifecycle/progress closeout is archived in [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md).

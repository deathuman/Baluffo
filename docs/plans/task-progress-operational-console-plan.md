# Task/Progress Operational Console Plan

> - **Status:** Active pending-work tracker
> - **Use this when:** finishing the Admin task/progress console migration and the remaining operator UX polish
> - **Canonical for:** shared task-run presenter adoption, compact history rendering, analysis-panel behavior, and run-state presentation
> - **Not canonical for:** runtime task contracts, bridge routes, or sync governance (use `admin-bridge-api.md` and `source-sync-production-readiness-plan.md`)
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), and [`../archive/admin-health-dashboard-console-closeout.md`](../archive/admin-health-dashboard-console-closeout.md) only for the older overview boundary
> - **Last updated:** 2026-05-05

## Remaining work

- `frontend/admin/app/fetcher/report.js` still needs the last legacy summary-string fallback removed so report logging comes entirely from the shared task-run presenter.
- Downloadable diagnostics export remains deferred.

## Close criteria

- Live status, run timeline, diagnostics, and completed-run history all read from the same task-run interpretation.
- Compact rows stay compact, and deeper evidence stays behind analysis or disclosure panels.
- Fetch, discovery, pipeline, and sync task types render consistently.
- `report.js` log summaries and table tooltips match the shared view model output.
- `normalizeOpsRuns` and `deriveAdminRunsModel` do not drift.

## Validation targets

```powershell
node --test tests/frontend/unit/task-run-view-model.test.mjs
node --test tests/frontend/unit/runs.test.mjs
node --test tests/frontend/unit/task-progress.test.mjs
node --test tests/frontend/unit/ops-history*.test.mjs
npm run lint:precommit
```

# Task/Progress Operational Console Plan

> - **Status:** Active compatible plan
> - **Use this when:** improving Admin task/progress UX, reducing frontend interpretation drift, and making long-running task behavior explicit for operations
> - **Canonical for:** shared task-run presenter design, live-vs-history rendering split, stale task handling UI, and operational triage UX for fetch/discovery/sync tasks
> - **Not canonical for:** task/task-state runtime contracts, bridge route definitions, or task execution engine behavior (use `admin-bridge-api.md`, `admin-task-state` domain code, and pipeline docs)
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`admin-health-dashboard-console-plan.md`](admin-health-dashboard-console-plan.md), [`../testing.md`](../testing.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

## Verdict

The task/progress system is structurally strong, but Admin still behaves like a report table rather than an operational console.

The current foundation is correct for an internal telemetry system:

- Live and historical task data are separated by contract intent.
- Run summaries are derived and do not own runtime truth.
- Long-running tasks are runId-based with bounded lifecycle fields.
- Frontend polling, event dedupe, and reattach behavior already exist.

The remaining risk is usability, operational clarity, and duplication across frontend modules.

This plan is compatible with [`admin-health-dashboard-console-plan.md`](admin-health-dashboard-console-plan.md) by separating depth from overview. The health-dashboard plan owns the Operations Health layout and compact Discovery / Fetch / Sync lane. This task/progress plan owns the shared task-run presenter, compact Current Runs rows, stale/orphaned display states, selected-run analysis, run timelines, and run-scoped diagnostics.

## Current implementation strength

### Backend contract shape

- `/ops/task-live/<taskType>` is the detailed live surface and includes `workItems`, `recentEvents`, `taskProgress`, lifecycle fields, and output summaries.
- `/ops/task-state` is the compact summary contract with a top-level `tasks` array.
- `admin-run-history.json` is explicit summary data, not source-of-truth liveness data.
- Task ownership is now `runId`-driven.
- Shared live-task normalizer already aligns `taskProgress`, `workItems`, `recentEvents`, lifecycle fields, `summary`, and `outputs`.

### Frontend foundation

- Guarded polling and bounded event signature tracking exist.
- Reattach-after-refresh behavior is present.
- Log chunk loading is supported.
- Event dedupe and heartbeat fallback are implemented.
- Tests already cover restore behavior, event dedupe, polling backoff, canonical `workItems`, and progress accessibility state.

So the engine is not broken. The operational gap is presentation quality and consistency.

## Main gaps

### 1) Progress interpretation is fragmented across modules

Current fragmentation points:

- `frontend/admin/app/fetcher/report.js`
- `frontend/admin/app/fetcher/watch.js`
- `frontend/admin/domain/progress.js`
- `frontend/admin/render/ops-history.js`
- Sync specific renderers using dedicated logic

Recommended improvement: shared presenter model.

```text
frontend/shared/task-run-view-model.js

input: taskType + payload (task-live, history row, report payload)
output: title | status | severity | primaryMetric | secondaryMetric | progressLabel | progressRatio | progressMode | currentTarget | failureSummary | diagnosticHints
```

### 2) Runs surface is not task-aware enough

Current columns in `renderAdminOpsHistory` are generic and biased.

Recommended structure:

- Current Runs: compact table rows with task-aware status chips, selected-run analysis, bounded copy diagnostics, and no progressbar/card layout.
- Completed Runs: existing table shape preserved, with row expansion for task details.
- Operations Health: compact task-status lane only, owned by [`admin-health-dashboard-console-plan.md`](admin-health-dashboard-console-plan.md), and fed by the same task-run interpretation where useful. Detailed task evidence stays in Run History and Selected Run Analysis.

Example compact row content:

```text
fetch | running | 7m 12s | Executing sources (50%) | 1 | started timestamp
```

```text
discovery | running | 3m 40s | Probing candidates (50%) | 2 | started timestamp
```

```text
sync | running | 22s | Sync push (active/pending/rejected) | 0 | started timestamp
Pushing source registry · active 120 / pending 14 / rejected 8
Remote: deathuman/Baluffo · main
```

### 3) Log surface mixes operator feedback, run summaries, and diagnostics

Current human-facing logs combine:

- live status text
- run completion summaries
- raw support diagnostics

Recommended improvement: three explicit areas in Admin:

- Live Status (operator-readable current state)
- Run Timeline (important business lifecycle steps)
- Diagnostics (raw, copyable, collapsible event details)

### 4) Stale/probably-dead tasks need explicit state

Current detection exists but rendering is ambiguous.

Suggested display states:

- queued
- running
- finishing
- completed
- completed_with_warnings
- failed
- stalled
- orphaned
- cancelled / cleared

Recommendation: add a derived status layer (frontend and/or shared presenter) rather than ad hoc renderer-specific branching.

### 5) Progress counts are not enough to explain delays

The fetcher report already carries meaningful runtime signals:

- slowest sources
- slow stages
- adapter timing
- zero-kept sources
- failure buckets
- static/provider quality signals

Move this into persistent run analysis panels instead of only log lines.

Compatibility note: the health-dashboard plan also groups latest fetch-health metrics into dashboard sections. This plan should own run-scoped analysis attached to a specific current or completed run, while the health-dashboard plan owns the latest aggregated health view. Where possible, both should use the same normalization helpers, but they should not duplicate renderer ownership.

### 6) Pipeline stages are visually disconnected

Ops health correctly polls discovery, fetch, sync, and recommendations, but the UI should represent causal flow clearly.

Recommended pipeline lane for Admin:

```text
Discovery        Fetch           Sync
Completed        Running         Waiting
7 queued         6/12 sources    Push after fetch
```

This turns task state into an operational picture instead of isolated status tokens.

Compatibility note: the health-dashboard plan implements a compact version of this lane in Operations Health. This task/progress plan implements the richer version only after `frontend/shared/task-run-view-model.js` exists, so both surfaces can share labels, progress ratios, and severity decisions.

## Compatibility with Admin Health Dashboard Plan

| Area | Task/progress plan owns | Health-dashboard plan owns | Integration rule |
|------|--------------------------|----------------------------|------------------|
| Task interpretation | Shared `task-run-view-model` for live/history/report payloads | Health-specific view model for `/ops/health` and `/ops/fetcher-metrics` sections | Health can consume the shared task model for its compact lane, but task model should not depend on health dashboard code. |
| Current tasks | Compact rows, stale/orphaned states, timelines, diagnostics | Compact Discovery / Fetch / Sync lane | Do not build full task cards inside `#admin-ops-fetcher-metrics`. |
| Fetch analysis | Run-scoped analysis for current/selected completed run | Latest health-oriented fetch metrics grouped in the dashboard | Share formatters where useful; keep render surfaces separate and avoid repeating row-level task details in Health. |
| Diagnostics | Current/last run event diagnostics, copy/export | Normalized section-summary copy for health sections | Avoid two raw-payload copy controls for the same event data. |
| Source-policy/dedup review | Only task progress/status context | Source Policy queue, health summaries, and separate Dedup Lists panel | Do not move review mutations into task rows, selected-run analysis, or task timelines. |

Execution order is flexible:

- If the health-dashboard plan lands first, its compact task lane should use `deriveAdminRunsModel()` and stay deliberately shallow.
- If this task/progress plan lands first, the health-dashboard plan should consume `frontend/shared/task-run-view-model.js` for the compact lane instead of adding a second task interpretation layer.
- If both are implemented in the same milestone, create the shared task presenter first, then the health dashboard can depend on it for task lane labels only.

## Implementation plan

### Progress

- [x] First detailed Admin task-console slice: shared frontend task-run presenter plus compact Current Runs table rows.
- [x] Completed-run row expansion with read-only native details disclosures.
- [x] Stalled/orphaned remediation guidance in compact status-chip tooltips.
- [x] Run-scoped diagnostics copy added as bounded clipboard JSON.
- [x] Selected-run analysis panel added below compact run tables.
- [x] Timeline panel added inside Selected Run Analysis.
- [ ] Downloadable run diagnostics export remains deferred.

The larger Current Runs card direction was reversed by `e44e7405`; keep this surface compact and table-based.

### Step 1 — Create shared task run view model

Add a single converter for all task payload shapes.

Target files:

- `frontend/shared/task-run-view-model.js`
- `frontend/shared/task-progress.js`
- `frontend/admin/domain/progress.js`
- `frontend/admin/domain/runs.js`

The shared model should be small and task-focused. It should not import Admin health-dashboard modules or understand `/ops/fetcher-metrics` section layout.

### Step 2 — Preserve compact Current Runs rows

Keep current and completed history as compact tables.

Target files:

- `frontend/admin/render/ops-history.js`
- `frontend/admin/domain/runs.js`

This step is separate from the compact task-status lane in Operations Health. Health may show "Fetch running, 6/12 sources" while this plan owns the dense run table, completed-run details, and later run-scoped diagnostics.

### Step 3 — Add stalled/orphaned derived states

Use existing signals (`heartbeatAt`, `finishedAt`, `task_state`, completion projection) to drive explicit UI statuses:

- running
- stalled
- orphaned
- completed
- failed
- warning

Do not mutate lifecycle in GET routes.

### Step 4 — Promote analysis into structured Admin panels

Promote key report diagnostics into viewable sections:

- slowest sources
- slow stages
- failure buckets
- quality and confidence signals

Attach these panels to the selected run. Selected-run analysis and its bounded timeline panel are implemented as read-only evidence below the compact run tables. The dashboard-level latest fetch-health grouping remains in [`admin-health-dashboard-console-plan.md`](admin-health-dashboard-console-plan.md).

### Step 5 — Keep a single event stream via `recentEvents`

Do not reintroduce task-specific event shapes outside shared normalizers.

Continue using normalized `taskProgress`, `workItems`, `recentEvents` and extend model outputs from this stream.

## Milestone: Admin Ops Compact Observability v1

1. Shared task run view model introduced
2. Compact Current Runs table rows preserved
3. Completed Runs table kept with row expansion
4. Stalled/orphaned state rendered explicitly
5. Bounded run diagnostics copy added for current and completed rows
6. Selected-run analysis panel added below compact run tables
7. Timeline panel added inside Selected Run Analysis
8. Downloadable diagnostics export deferred

Health-dashboard dependency boundary:

```text
Operations Health overview: admin-health-dashboard-console-plan.md
Detailed task console: this plan
Shared task labels/progress/severity: frontend/shared/task-run-view-model.js
```

## Success criteria

- All task renderers consume the same shared model output.
- Fetch/discovery/sync live and completed states are visually consistent.
- Stalled and orphaned conditions are explicit and actionable.
- Operators can distinguish live status, completion summary, and diagnostics without mixed log streams.
- Run analysis preserves key runtime details that currently survive only in log text.
- Health-dashboard latest metrics and run-scoped analysis do not diverge in labels or severity.
- Backend contracts stay unchanged during this milestone.

## Validation targets

Baseline verification should include both existing and new admin/task tests.

```powershell
python -m pytest tests/admin test_source_policy_soak_report.py tests/test_source_sync.py
node --test tests/frontend/unit/admin-source-policy-review-render.test.mjs tests/frontend/unit/admin-sync-controller.test.mjs tests/frontend/unit/admin-task-progress* tests/frontend/unit/ops-history*.test.mjs
npm run lint:precommit
```

Adjust scope as the milestone is implemented; keep checks focused on touched admin modules first.

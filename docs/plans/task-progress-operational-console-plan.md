# Task/Progress Operational Console Plan

> - **Status:** Active next-step tracker
> - **Use this when:** improving Admin task/progress UX, reducing frontend interpretation drift, and making long-running task behavior explicit for operations
> - **Canonical for:** shared task-run presenter design, live-vs-history rendering split, stale task handling UI, and operational triage UX for fetch/discovery/sync tasks
> - **Not canonical for:** task/task-state runtime contracts, bridge route definitions, or task execution engine behavior (use `admin-bridge-api.md`, `admin-task-state` domain code, and pipeline docs)
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../testing.md`](../testing.md), and [`source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

## Verdict

The task/progress system is structurally strong, but Admin still behaves like a report table rather than an operational console.

The current foundation is correct for an internal telemetry system:

- Live and historical task data are separated by contract intent.
- Run summaries are derived and do not own runtime truth.
- Long-running tasks are runId-based with bounded lifecycle fields.
- Frontend polling, event dedupe, and reattach behavior already exist.

The remaining risk is usability, operational clarity, and duplication across frontend modules.

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

- Current Runs: card-based cards with high-signal status, target, stage, progress, and quick diagnostics.
- Completed Runs: existing table shape preserved, but with row expansion for task details.

Example card states:

```text
Fetcher
Running · 7m 12s
Executing sources · 6/12 resolved · 42 jobs · 1 failed
Current: Studio X / Greenhouse
[progress bar]
```

```text
Discovery
Running · 3m 40s
Probing candidates · 18/50 probed · 7 queued · 2 failed
Current: Provider pattern scan
```

```text
Sync Push
Running · 22s
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

Move this into persistent Admin analysis panels instead of only log lines.

### 6) Pipeline stages are visually disconnected

Ops health correctly polls discovery, fetch, sync, and recommendations, but the UI should represent causal flow clearly.

Recommended pipeline lane for Admin:

```text
Discovery        Fetch           Sync
Completed        Running         Waiting
7 queued         6/12 sources    Push after fetch
```

This turns task state into an operational picture instead of isolated status tokens.

## Implementation plan

### Step 1 — Create shared task run view model

Add a single converter for all task payload shapes.

Target files:

- `frontend/shared/task-run-view-model.js`
- `frontend/shared/task-progress.js`
- `frontend/admin/domain/progress.js`
- `frontend/admin/domain/runs.js`

### Step 2 — Replace generic current table with live cards

Keep completed history as a compact table.

Target files:

- `frontend/admin/render/ops-history.js`
- `frontend/admin/domain/runs.js`

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

### Step 5 — Keep a single event stream via `recentEvents`

Do not reintroduce task-specific event shapes outside shared normalizers.

Continue using normalized `taskProgress`, `workItems`, `recentEvents` and extend model outputs from this stream.

## Milestone: Admin Operational Console v1

1. Shared task run view model introduced
2. Current Runs cards implemented
3. Completed Runs table kept with row expansion
4. Stalled/orphaned state rendered explicitly
5. Latest fetch run analysis panel added
6. Diagnostics copy/export added for current and last run

## Success criteria

- All task renderers consume the same shared model output.
- Fetch/discovery/sync live and completed states are visually consistent.
- Stalled and orphaned conditions are explicit and actionable.
- Operators can distinguish live status, completion summary, and diagnostics without mixed log streams.
- Run analysis preserves key runtime details that currently survive only in log text.
- Backend contracts stay unchanged during this milestone.

## Validation targets

Baseline verification should include both existing and new admin/task tests.

```powershell
python -m pytest tests/admin test_source_policy_soak_report.py tests/test_source_sync.py
node --test tests/frontend/unit/admin-source-policy-review-render.test.mjs tests/frontend/unit/admin-sync-controller.test.mjs tests/frontend/unit/admin-task-progress* tests/frontend/unit/ops-history*.test.mjs
npm run lint:precommit
```

Adjust scope as the milestone is implemented; keep checks focused on touched admin modules first.

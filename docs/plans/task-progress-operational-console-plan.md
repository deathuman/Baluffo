# Task/Progress Operational Console Plan

> - **Status:** Active compatible plan
> - **Use this when:** improving Admin task/progress UX, reducing frontend interpretation drift, and making long-running task behavior explicit for operations
> - **Canonical for:** shared task-run presenter design, live-vs-history rendering split, stale task handling UI, and operational triage UX for fetch/discovery/sync/pipeline tasks
> - **Not canonical for:** task/task-state runtime contracts, bridge route definitions, task execution engine behavior, or sync-governance policy (use `admin-bridge-api.md`, `admin-task-state` domain code, pipeline docs, and [`source-sync-production-readiness-plan.md`](source-sync-production-readiness-plan.md))
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../archive/admin-health-dashboard-console-closeout.md`](../archive/admin-health-dashboard-console-closeout.md), [`../testing.md`](../testing.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-04

## Verdict

The task/progress system is structurally strong, but Admin still behaves like a report table rather than an operational console. Sync-side governance, counters, and repo policy hardening stay tracked separately in [`source-sync-production-readiness-plan.md`](source-sync-production-readiness-plan.md).

The current foundation is correct for an internal telemetry system:

- Live and historical task data are separated by contract intent.
- Run summaries are derived and do not own runtime truth.
- Long-running tasks are runId-based with bounded lifecycle fields.
- Frontend polling, event dedupe, and reattach behavior already exist.

The remaining risk is usability, operational clarity, and duplication across frontend modules.

This plan is compatible with the archived [`admin-health-dashboard-console-closeout.md`](../archive/admin-health-dashboard-console-closeout.md) by separating depth from overview. The archived health-dashboard work owns the Operations Health layout, compact Discovery / Fetch / Sync lane, and tabbed review surfaces for Discovery Review, Source Policy Review, and Dedup Lists. This task/progress plan owns the shared task-run presenter, compact Current Runs rows, stale/orphaned display states, selected-run analysis, run timelines, and run-scoped diagnostics.

The current direction is healthier than the earlier card-shaped experiments: keep the compact task rows as the baseline, keep deeper evidence on demand, and keep review-heavy surfaces separated so the high-frequency console stays dense instead of sprawling.

## Current implementation strength

### Backend contract shape

- `/ops/task-live/<taskType>` is the detailed live surface and includes `workItems`, `recentEvents`, `taskProgress`, lifecycle fields, and output summaries. Discovery live progress is wave-aware: stage index/total, current target, generated/survived counts, probe counts, and updated heartbeats are additive support fields.
- `/ops/task-state` is the compact summary contract with a top-level `tasks` array. Serves all four task types (fetch, discovery, pipeline, sync) but only when `active === true`.
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

## Current state vs plan gap assessment

| Item | Status | Notes |
|------|--------|-------|
| Shared task-run presenter (`task-run-view-model.js`) | Done | Exists with `buildTaskRunView`, `buildTaskRunDiagnostics`, `buildTaskRunAnalysis`. Export surface matches plan spec. |
| Compact Current Runs table rows | Done | `ops-history.js:renderAdminOpsHistory` uses the shared view model for compact rows. |
| Completed-run row expansion | Done | `<details>` disclosures with read-only meta/summary/hints. |
| Stalled/orphaned remediation guidance | Done | `deriveStatus` plus `remediationHint` in view model, surfaced in status-chip tooltip. |
| Run-scoped diagnostics copy | Done | `buildTaskRunDiagnostics` produces bounded JSON; copy button wired in `ops-history.js`. |
| Selected-run analysis panel | Done | `renderSelectedRunAnalysis` in `ops-history.js`, fed by `buildTaskRunAnalysis`. |
| Timeline panel inside selected-run analysis | Done | `buildTimelineEntries` capped at 5, sorted by timestamp. |
| View model test coverage | Done | 314-line test file covering live progress, terminal states, stalled/orphaned, null handling, diagnostics boundedness, timeline fallback. |
| Downloadable diagnostics export | Deferred | Clipboard copy is the only export path. |
| `report.js` migration to shared view model | Not started | Still uses `domain/progress.js` directly. |
| `domain/progress.js` consolidation | Not started | Still has its own `deriveTaskProgressView`, `deriveFetcherProgressModel`, `deriveDiscoveryProgressModel`. |
| Pipeline progress formatting | Missing | `formatTaskProgressCounts` only handles fetch/discovery/sync. Pipeline runs silently show empty progress. |
| `normalizeOpsRuns` / `deriveAdminRunsModel` dedup | At risk | Both in `runs.js` independently normalize run rows — drift risk. |

Fragmentation risk remains real. Today `ops-history.js` uses the shared view model, while `report.js` and `domain/progress.js` still operate independently. Progress bar rendering and run-level rendering can disagree on labels, severity, and progress ratio for the same backend payload.

## Main gaps

### 1) Progress interpretation is fragmented across modules

Current fragmentation points:

- `frontend/admin/app/fetcher/report.js`
- `frontend/admin/app/fetcher/watch.js`
- `frontend/admin/domain/progress.js`
- `frontend/admin/render/ops-history.js`
- Sync specific renderers using dedicated logic

Recommended improvement: shared presenter model. The shared model already exists at `frontend/shared/task-run-view-model.js`. The migration gap is in `report.js` and `domain/progress.js`.

### 2) Runs surface is not task-aware enough

Current columns in `renderAdminOpsHistory` are generic and biased.

Recommended structure:

- Current Runs: compact table rows with task-aware status chips, selected-run analysis, bounded copy diagnostics, and no progressbar/card layout.
- Completed Runs: existing table shape preserved, with row expansion for task details.
- Operations Health: compact task-status lane only, documented in the archived [`admin-health-dashboard-console-closeout.md`](../archive/admin-health-dashboard-console-closeout.md), and fed by the same task-run interpretation where useful.

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

Suggested display states: queued, running, finishing, completed, completed_with_warnings, failed, stalled, orphaned, cancelled/cleared.

The shared view model already implements `deriveStatus` returning `stalled`, `orphaned`, `completed_with_warnings`, etc. The gap is that `domain/progress.js` and `report.js` do not consume these derived states.

### 5) Progress counts are not enough to explain delays

Move fetcher runtime signals (slowest sources, slow stages, adapter timing, failure buckets, quality signals) into persistent run analysis panels instead of only log lines.

### 6) Pipeline stages are visually disconnected

Recommended pipeline lane for Admin showing Discovery → Fetch → Sync causal flow. The health-dashboard plan implements a compact version; this plan implements the richer version after the shared view model is in place.

## Remaining risks and follow-ups

- Tabbed review surfaces now expose actionable counts/status badges; keep the count semantics aligned as review workflows evolve.
- Selected-run analysis should stay bounded by default; Summary and Timeline can open first, while counts, work examples, event examples, and diagnostics stay collapsed.
- Discovery heartbeat refresh is still coupled to the auto-sync watcher path; split lifecycle monitoring from post-completion sync later if that coupling starts causing bugs.
- Manual `?v=` cache-buster bumps are noisy and should eventually be replaced with a generated asset version or build-time hash.
- Tabbed review surfaces should use consistent empty/error copy so the Admin shell feels coherent when data is missing, stale, or unavailable.
- `buildTimelineEntries` is intentionally private (not exported) — consumed only through `buildTaskRunAnalysis`. Keep it private unless a renderer outside `ops-history.js` needs direct timeline construction.

## Implementation plan

### Progress

- [x] First detailed Admin task-console slice: shared frontend task-run presenter plus compact Current Runs table rows.
- [x] Completed-run row expansion with read-only native details disclosures.
- [x] Stalled/orphaned remediation guidance in compact status-chip tooltips.
- [x] Run-scoped diagnostics copy added as bounded clipboard JSON.
- [x] Selected-run analysis panel added below compact run tables.
- [x] Timeline panel added inside Selected Run Analysis.
- [x] View model unit test suite (314 lines).
- [ ] Downloadable run diagnostics export remains deferred.
- [ ] Pipeline progress formatting (F1).
- [ ] `normalizeOpsRuns` / `deriveAdminRunsModel` conformance test (F2).
- [ ] Max row cap with expand toggle in ops-history (F4).
- [ ] `domain/progress.js` delegation to shared view model (F5).
- [ ] Heartbeat proximity `stallProximity` / `approaching` severity (F6).
- [ ] Empty/null task state controller-level detection with loading indicator (F7).
- [ ] Additional edge-case test scenarios (F8).
- [ ] Table column tooltip alignment with view model outputs (F9a).
- [ ] Report.js log summary migration to view model (F9b).
- [ ] Progress staleness visual cue on progress bar element (F9c).

The larger Current Runs card direction was reversed by `e44e7405`; keep this surface compact and table-based.

### Step 1 — Create shared task run view model

Status: DONE. `frontend/shared/task-run-view-model.js` exists with `buildTaskRunView`, `buildTaskRunDiagnostics`, `buildTaskRunAnalysis`. Tests exist at `tests/frontend/unit/task-run-view-model.test.mjs`.

### Step 2 — Preserve compact Current Runs rows

Keep current and completed history as compact tables. `ops-history.js:renderAdminOpsHistory` uses the shared view model.

### Step 3 — Add stalled/orphaned derived states

Status: DONE. `deriveStatus` in `task-run-view-model.js` uses heartbeat proximity and lifecycle fields.

### Step 4 — Promote analysis into structured Admin panels

Status: DONE. `buildTaskRunAnalysis` + `renderSelectedRunAnalysis`.

### Step 5 — Keep a single event stream via `recentEvents`

Continue using normalized `taskProgress`, `workItems`, `recentEvents` and extend model outputs from this stream.

## Milestone: Admin Ops Compact Observability v1

1. Shared task run view model introduced ✅
2. Compact Current Runs table rows preserved ✅
3. Completed Runs table kept with row expansion ✅
4. Stalled/orphaned state rendered explicitly ✅
5. Bounded run diagnostics copy added for current and completed rows ✅
6. Selected-run analysis panel added below compact run tables ✅
7. Timeline panel added inside Selected Run Analysis ✅
8. View model unit tests added ✅
9. Downloadable diagnostics export deferred
10. Pipeline progress formatting added
11. `normalizeOpsRuns` / `deriveAdminRunsModel` conformance test added
12. Max row caps with expand toggles in ops-history
13. `domain/progress.js` delegates labels to shared view model
14. Heartbeat proximity (`approaching` severity) visual treatment
15. Empty/null task state shows loading indicator
16. 8 additional edge-case test scenarios
17. Table column tooltips aligned with view model outputs
18. Report.js log summaries use shared view model
19. Progress staleness visual cue on progress bar element

Health-dashboard dependency boundary:

```text
Operations Health overview: ../archive/admin-health-dashboard-console-closeout.md
Detailed task console: this plan
Shared task labels/progress/severity: frontend/shared/task-run-view-model.js
```

## Extension: Concrete missing features and validations

The following items are concrete gaps surfaced during codebase inspection.

### F1 — Pipeline task type progress formatting

`frontend/shared/task-progress.js:formatTaskProgressCounts` only handles `fetch`, `discovery`, and `sync`. Pipeline is a real task type served by `/ops/task-state` with its own `build_pipeline_live_payload` at the backend. Its `taskProgress` carries: `currentStep/totalSteps` (determinate ratio), `baselineOutputCount`, `finalOutputCount`, and `stage` phaseKey. The summary carries `updatesFound` and `refreshRecommended`. Pipeline does NOT have a `/ops/task-live/pipeline` route (returns 404), so it only appears through task-state compact rows.

**Required:**
- Add `formatPipelineCounts` in `task-progress.js`:
  ```js
  function formatPipelineCounts(counts, progress) {
    const step = compactNumber(counts?.currentStep || 0);
    const total = compactNumber(counts?.totalSteps || 0);
    const baseline = compactNumber(counts?.baselineOutputCount || 0);
    const final = compactNumber(counts?.finalOutputCount || 0);
    return `step ${step}/${total} | output ${final} (baseline ${baseline})`;
  }
  ```
- Update `formatTaskProgressCounts` to dispatch `"pipeline"`.
- Add `derivePrimaryLabel` for pipeline: `"Pipeline"`.
- Add `deriveSecondaryLabel` for pipeline: `"step ${currentStep}/${totalSteps}"`.
- Add test case for pipeline task type in `task-run-view-model.test.mjs`.

### F2 — `deriveAdminRunsModel` and `normalizeOpsRuns` conformance test

Both functions in `frontend/admin/domain/runs.js` independently normalize run rows — same logic, different implementations. Risk of silent drift.

**Required (test-only, no prod changes):**
- Build a realistic payload with a mix of live/completed runs for all 4 types.
- Feed to both `deriveAdminRunsModel` and `normalizeOpsRuns`.
- Assert identical `displayStatus`, `isLive`, `elapsedMs` (within 1ms tolerance) for equivalent rows.
- Add test to `tests/frontend/unit/runs.test.mjs` (create file if missing).

### F3 — Input schema validation (silent tolerance)

Design principle: the view model must never throw for missing or malformed fields. No validation guard needed. Removed from implementation scope.

### F4 — Max row cap with expand toggle in ops-history

`renderAdminOpsHistory` currently renders all `currentRows` and `visibleCompletedRows` unconditionally. No DOM bound.

**Required:**
- Cap `currentRows` at 10 in the renderer. Log a warning if exceeded.
- Cap `visibleCompletedRows` at 5.
- Below each capped section, add a `<details class="admin-ops-expand-capped">` toggle: "Show all N runs".
- When expanded, render all remaining rows.
- Caps apply at render time only — the domain model preserves full data in `olderCompletedRows`.
- Add CSS class for the toggle: `admin-ops-expand-capped`.

### F5 — `domain/progress.js` delegation to shared view model

`domain/progress.js` provides simpler progress-bar models (`{active, determinate, ratio, label}`) while the shared view model provides richer run-level views. They are complementary but can diverge on the same payload. `report.js` calls into `domain/progress.js` in 5 places.

**Required:**
- When a full row object is available (has `taskProgress`, `taskType`, lifecycle fields), use the shared view model for label derivation:
  ```js
  if (report?.taskProgress && report?.taskType) {
    const view = buildTaskRunView({ ...report, taskType: "fetch" });
    return { active: view.status !== "waiting", determinate: view.progressMode === "determinate", ratio: view.progressRatio, label: view.progressLabel };
  }
  return deriveTaskProgressView(deriveLegacyFetcherTaskProgress(report, { running }), ...);
  ```
- Same pattern for `deriveDiscoveryProgressModel`.
- In `report.js:appendFetcherProgressFromReport` (lines 249-253), replace manually assembled log summary text with `buildTaskRunView(report).progressLabel`.
- In `report.js:loadLatestFetcherReport` (lines 101-103), same replacement.
- Leave `deriveFetcherFailureSummary`, `deriveFetcherTaskProgress`, `deriveDiscoveryTaskProgress`, `deriveDiscoveryLifecycleCounts` in place — they handle legacy report shapes.

### F6 — Heartbeat proximity telemetry (`approaching` severity)

`STALLED_AFTER_MS` is hardcoded at 10 minutes. Operators have no visibility into tasks approaching the stall threshold.

**Required (view model additions):**
```js
heartbeatStaleness: active && heartbeatMs > 0
  ? Math.min(1, Math.max(0, (nowMs - heartbeatMs) / STALLED_AFTER_MS))
  : 0,
stallProximity: heartbeatStaleness >= 0.75 && status === "running" ? "approaching" : null,
```

**Visual treatment when `stallProximity === "approaching"`:**
- Severity stays `"warning"` (not `"critical"` — not stalled yet).
- Status chip gets CSS class `admin-status-chip-approaching`.
- Tooltip on chip: `"Heartbeat aging (${Math.round(heartbeatStaleness * 100)}%)"`.
- CSS: amber border pulse animation (keyframes fade in/out on left border), rather than full background change — distinguishing from the stalled state.

**Tests:**
- Heartbeat 8min old (80% of 10min) → `stallProximity === "approaching"`.
- Heartbeat 6min old (60%) → `stallProximity === null`.

### F7 — Empty/null task state handling

When `/ops/task-state` returns null, undefined, or `{tasks: null}`, the renderer silently shows "No current runs" — indistinguishable from "all tasks completed".

**Required (controller-level detection):**
- In `frontend/admin/app/ops/task-state.js`, after the `/ops/task-state` fetch:
  ```js
  state.waitingForTaskState = taskStatePayload === null || taskStatePayload === undefined;
  ```
- In `ops-history.js:renderAdminOpsHistory`:
  - If `waitingForTaskState` is true and there are no rows:
    ```html
    <div class="admin-ops-loading">Waiting for task state...</div>
    ```
  - CSS: mild fade-pulse animation on the text (no spinner).
  - Once the first response arrives (`waitingForTaskState` becomes false and rows are empty): show `'<div class="no-results">No current runs.</div>'` as today.
- `deriveAdminRunsModel` unchanged — `getTaskStateRows` already returns `[]` for non-array input.

### F8 — Additional view model test scenarios

Existing tests cover main paths. Add these to `tests/frontend/unit/task-run-view-model.test.mjs`:

| Scenario | Expected behavior |
|----------|-------------------|
| NaN in `taskProgress.ratio` | Coerce to 0, mode falls back to indeterminate |
| Negative `heartbeatAt` offset | Treated as no heartbeat, status becomes stalled if active |
| `workItems` = `null` | Treated as empty, no work-item timeline entries |
| `summary` = `null` | All counts default to 0, labels return empty strings |
| Unknown `taskType` string | title = "Task", progressLabel = "", no counts formatting |
| `active: true` without `startedAt` | elapsedMs = 0, status = running (not stalled) |
| `finishedAt` with `active: true` | `active` is ignored, task is treated as completed |
| Stalled task with no `heartbeatAt` field | Missing heartbeat treated as oldest possible = stalled |

### F9 — Progress bar vs output log consistency

Three concrete items to align what the progress bar, compact table columns, log output, and detail panels show.

**9a — Table column tooltips aligned with view model**

Compact table text stays unchanged (raw counts, minimal format). Add `title` attributes reflecting the view model's richer descriptive text:

| Column | Text stays | `title` attribute |
|--------|-----------|-------------------|
| Failed | Raw number | `view.failureSummary` (or empty) |
| Output / Review queue | Compact text | `view.progressLabel` (or empty) |
| Sync output | `Sync push (10/3/1)` | `view.secondaryLabel` (= "active 10 / pending 3 / rejected 1") |

This ensures hovering reveals the exact same text the detail panel shows, without changing the compact table layout.

Target files: `frontend/admin/render/ops-history.js:toRowView`.

**9b — Report.js log summaries use shared view model**

`report.js:appendFetcherProgressFromReport` and `report.js:loadLatestFetcherReport` manually assemble summary strings like:
```
`Fetcher: ${resolvedSources}/${selectedSourceCount} sources resolved, running ${runningSources}...`
```
Replace with `buildTaskRunView({ ...report, taskType: "fetch" }).progressLabel`. This ensures log text and UI progress label always agree.

**9c — Progress staleness visual cue on progress bar element**

Backend may stop updating `taskProgress` while a task is still active. Add detection and visual treatment:

```js
// In buildTaskRunView:
progressUpdatedAt: String(progress?.updatedAt || "").trim(),
progressStale: (status === "running" || status === "finishing") && progress?.updatedAt
  ? (nowMs - parseTimeMs(progress.updatedAt)) > (STALLED_AFTER_MS / 2)
  : false,
```

**Visual treatment when `progressStale === true`:**
- CSS class `admin-ops-progress-stale` on the compact row.
- Left border: `2px solid #d4a017` (amber) — subtle, non-alarming.
- Text in the output/progress column: opacity drops to `0.65`, color shifts to a muted amber tone.
- No background change, no icon — keeps the compact row dense.
- Tooltip on progress label: `"Progress stale (last update Xs ago)"`.

**Severity tiers (F6 + F9c combined):**
| Condition | Chip | Visual cue |
|-----------|------|------------|
| Running, healthy heartbeat | `running` / green | No decoration |
| `stallProximity === "approaching"` | `warning` / amber chip | Amber border pulse |
| `progressStale === true` | Unchanged (still running) | Amber left-border, muted opaque text |
| `status === "stalled"` | `stalled` / critical | Red chip + remediation tooltip |
| `status === "failed"` | `failed` / red | Red chip |

## Success criteria

- All task renderers consume the same shared model output.
- Fetch/discovery/sync/pipeline live and completed states are visually consistent.
- Stalled and orphaned conditions are explicit and actionable.
- Operators can distinguish live status, completion summary, and diagnostics without mixed log streams.
- Run analysis preserves key runtime details that currently survive only in log text.
- Health-dashboard latest metrics and run-scoped analysis do not diverge in labels or severity.
- Backend contracts stay unchanged during this milestone.
- Pipeline tasks have visible progress in the console.
- `normalizeOpsRuns` and `deriveAdminRunsModel` have a conformance test guarding against drift.
- Table columns stay compact; detail text is accessible via tooltip and detail panel.
- Report.js log summaries are derived from the same view model as the UI.
- Heartbeat proximity is visible before a task fully stalls.
- Progress staleness is visually signaled on the progress bar element.
- Empty/null task state shows a loading indicator rather than silent empty tables.
- View model silently tolerates missing fields on partial data.

## Validation targets

```powershell
python -m pytest tests/admin test_source_policy_soak_report.py tests/test_source_sync.py
node --test tests/frontend/unit/admin-source-policy-review-render.test.mjs tests/frontend/unit/admin-sync-controller.test.mjs tests/frontend/unit/task-run-view-model.test.mjs tests/frontend/unit/task-progress.test.mjs
node --test tests/frontend/unit/ops-history*.test.mjs
node --test tests/frontend/unit/runs.test.mjs
npm run lint:precommit
```

Specific assertions per extension:

```powershell
# F1: pipeline progress formatting
node --test "tests/frontend/unit/task-progress.test.mjs" --grep "pipeline"
node --test "tests/frontend/unit/task-run-view-model.test.mjs" --grep "pipeline"

# F2: conformance test
node --test "tests/frontend/unit/runs.test.mjs" --grep "conformance|normalizeOpsRuns.*deriveAdminRunsModel"

# F6: heartbeat proximity
node --test "tests/frontend/unit/task-run-view-model.test.mjs" --grep "approaching|heartbeat.*stale"

# F8: edge case scenarios
node --test "tests/frontend/unit/task-run-view-model.test.mjs" --grep "NaN|null.*workItems|unknown.*taskType|without.*startedAt|finishedAt.*active|no.*heartbeatAt"
```

Adjust scope as the milestone is implemented; keep checks focused on touched admin modules first.

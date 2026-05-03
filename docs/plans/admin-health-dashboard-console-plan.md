# Admin Health Dashboard Console Plan

> - **Status:** Active validated plan
> - **Use this when:** improving Admin Operations Health readability, turning the metrics wall into a console view, making alerts/diagnostics easier to triage, and defining the health dashboard's compact operator layout
> - **Canonical for:** Ops health layout plan, sectionization strategy, copy/export and progressive disclosure behavior for `/ops/health` and `/ops/fetcher-metrics` rendering, and the health dashboard's compact task/status lane
> - **Not canonical for:** bridge contracts, runtime behavior, or actual UI implementation details (use `admin-bridge-api.md`, `DATA_CONTRACT.md`, and `frontend/admin` source files)
> - **Then inspect:** [`admin-bridge-api.md`](../admin-bridge-api.md), [`architecture-ai-map.md`](../architecture-ai-map.md), [`task-progress-operational-console-plan.md`](task-progress-operational-console-plan.md), [`frontend/admin/app/ops/health.js`](../../frontend/admin/app/ops/health.js), [`frontend/admin/render/ops-summary.js`](../../frontend/admin/render/ops-summary.js), [`frontend/admin/domain/runs.js`](../../frontend/admin/domain/runs.js), [`frontend/admin/render/ops-history.js`](../../frontend/admin/render/ops-history.js), [`styles/admin.css`](../../styles/admin.css)
> - **Last updated:** 2026-05-03

## Verdict

The current Operations Health experience is functionally correct but currently reads as a single report dump. The UI is especially hard to parse where `/ops/fetcher-metrics` currently emits runtime, source-health, dedup, social, and source-policy signals into one long section.

This plan keeps data contracts unchanged and focuses on frontend rendering structure. The right next move is a small Admin console refactor: normalize existing payloads into view-model sections, render those sections compactly, preserve existing action handlers, and keep backend lifecycle ownership untouched.

This plan is compatible with [`task-progress-operational-console-plan.md`](task-progress-operational-console-plan.md) by drawing a clear boundary: this document owns the Operations Health overview and metrics layout, while the task/progress plan owns detailed live-run cards, stale/orphaned task states, run timelines, and the shared task presenter. The health dashboard may show a compact Discovery / Fetch / Sync lane, but it should not implement the full Current Runs card system.

## Validation Findings

Validation against the current code confirms the diagnosis:

- [`frontend/admin/app/ops/health.js`](../../frontend/admin/app/ops/health.js) already fetches `/ops/health`, `/ops/history?limit=80`, `/ops/task-state`, `/ops/fetcher-metrics?windowRuns=80`, and `/source-policy/recommendations` together.
- [`frontend/admin/render/ops-summary.js`](../../frontend/admin/render/ops-summary.js) owns `renderAdminOpsFetcherMetrics()` and currently assembles a long flat sequence of KPI cards and full-row diagnostic text blocks.
- [`frontend/admin/render/ops-history.js`](../../frontend/admin/render/ops-history.js) already renders the run model separately, so task-lane work in this plan should reuse the existing current-run model instead of replacing run history.
- Source-policy review actions and dedup review actions are already wired through the Ops health controller; this plan should preserve those handlers and only improve their display context.
- Existing frontend tests import `renderAdminOpsFetcherMetrics()` through stable render exports, so the public renderer export should stay stable during the first implementation slice.
- [`task-progress-operational-console-plan.md`](task-progress-operational-console-plan.md) separately owns the deeper task/progress console. Health-dashboard work should reuse `deriveAdminRunsModel()` for a compact lane now, and can later consume `frontend/shared/task-run-view-model.js` if that plan lands first.

## Goals

1. Replace wall-of-text health output with a sectioned operational console.
2. Keep source-of-truth contracts unchanged: `/ops/health`, `/ops/fetcher-metrics`, `/ops/task-state`, `/ops/history`, and `/source-policy/recommendations`.
3. Preserve existing source-policy and dedup review actions while making their context visible in organized sections.
4. Add low-friction diagnostics access with bounded rows and progressive disclosure.
5. Keep the dashboard dense and operator-focused, not decorative.

## Scope

### In scope

- Admin-only dashboard layout and rendering in `frontend/admin`.
- New view-model/formatter layer under `frontend/admin/domain`.
- Sectioned rendering for `#admin-ops-fetcher-metrics`.
- A compact task-status lane that reuses existing run-model data and stays read-only.
- Styling additions for compact section density in [`styles/admin.css`](../../styles/admin.css).
- Focused frontend unit tests for rendering boundaries, bounded lists, action preservation, and diagnostics-copy behavior.

### Out of scope

- Backend route shape changes.
- GET route lifecycle mutations.
- Rewriting source-policy business logic or the dedup evaluation engine.
- New telemetry contract fields.
- Changing `/ops/history` ownership.
- Replacing the full Runs table with cards.
- Implementing detailed Current Runs cards, stale/orphaned task state resolution, or run timelines; those belong to [`task-progress-operational-console-plan.md`](task-progress-operational-console-plan.md).
- Moving source-policy review queue mutations into a different endpoint or payload shape.

## Validated Ownership Map

| Concern | Current owner | Plan action |
|---------|---------------|-------------|
| Ops payload fan-in and polling | [`frontend/admin/app/ops/health.js`](../../frontend/admin/app/ops/health.js) | Pass the existing payload bundle into a view model; keep polling and action handlers unchanged. |
| KPI/status cards | [`frontend/admin/render/ops-summary.js`](../../frontend/admin/render/ops-summary.js) | Preserve top-level KPI cards, but reduce long prose rows. |
| Fetcher metrics wall | [`frontend/admin/render/ops-summary.js`](../../frontend/admin/render/ops-summary.js) | Split into named sections with bounded rows. |
| Current/completed run table | [`frontend/admin/render/ops-history.js`](../../frontend/admin/render/ops-history.js), [`frontend/admin/domain/runs.js`](../../frontend/admin/domain/runs.js) | Reuse for a compact task-status lane; do not replace the history renderer in this slice. |
| Source-policy review queue | [`frontend/admin/render/source-policy-review.js`](../../frontend/admin/render/source-policy-review.js), [`frontend/admin/app/ops/health.js`](../../frontend/admin/app/ops/health.js) | Keep the queue as its own action surface; cross-link or summarize it from the health console. |
| Dedup review actions | [`frontend/admin/render/ops-summary.js`](../../frontend/admin/render/ops-summary.js), [`frontend/admin/app/ops/health.js`](../../frontend/admin/app/ops/health.js) | Preserve row action wiring after sectionization. |
| Styling | [`styles/admin.css`](../../styles/admin.css) | Add page-owned compact section/card styles. |
| Tests | `tests/frontend/unit/admin-*.test.mjs` | Extend render/controller tests around new structure and stable action wiring. |

## Compatibility with Task/Progress Plan

| Area | Health dashboard plan owns | Task/progress plan owns | Integration rule |
|------|----------------------------|--------------------------|------------------|
| Task status | Compact Discovery / Fetch / Sync lane inside Operations Health | Full Current Runs cards, stale/orphaned states, progress presenter, run timeline | Health lane should consume existing `deriveAdminRunsModel()` now and shared task presenter later. |
| Run analysis | Latest health-oriented fetch metrics grouped into scannable sections | Per-run analysis attached to current/completed run detail | Keep health analysis aggregated and latest-run focused. |
| Diagnostics copy | Section-level normalized health summaries | Current/last run diagnostics and event payloads | Avoid duplicating raw event export controls in the health dashboard. |
| Source-policy/dedup actions | Contextual summaries plus existing action queues | Task status and progress UX only | Do not move review actions into task cards. |
| Rendering ownership | `renderAdminOpsFetcherMetrics()` and health-specific section components | `renderAdminOpsHistory()` and task presenter consumers | Preserve stable public exports until both plans intentionally converge. |

## Implementation Plan

### 1. Introduce an ops-health view model

Add a pure frontend module:

```text
frontend/admin/domain/ops-health-view-model.js
```

The module should have no DOM writes and no bridge calls. It should map existing payloads into render-ready groups:

```text
/ops/health status, alerts, schedule, and kpis
/ops/fetcher-metrics latest run and history
/ops/task-state current task rows, through the existing run model
/source-policy/recommendations summary state
dedup evidence and review-state summaries
```

It should emit stable buckets with labels, counts, severity, bounded examples, and optional diagnostics payload slices:

```text
headline status
runtime summary
failure summary
source-health highlights
dedup review state
source-policy signal state
social signal summary
diagnostics
```

### 2. Split Operations Health into sections

Convert `#admin-ops-fetcher-metrics` from long-form text blocks into compact named sections:

```text
Runtime
Failures
Source Health
Dedup Review
Source Policy Signals
Social Signals
Diagnostics
```

Keep the existing alert, KPI, schedule, trends, and run-history areas intact.

Target layout:

```text
Operations Health
  Alerts
  KPI strip
  Schedule / sync summary
  Current task lane
  Fetcher metrics console
    Runtime
    Failures
    Source Health
    Dedup Review
    Source Policy Signals
    Social Signals
    Diagnostics
  Source Policy Review queue
  Trends
  Runs
```

### UI layout model for Step 2

The Operations Health screen should read top-to-bottom as an operator triage flow:

```text
What is wrong?
What is running?
What changed in the latest fetch?
What needs review?
What evidence supports that?
What happened recently?
```

#### First viewport: status and action context

The first viewport should avoid the current long text wall and show only high-signal state:

```text
[Alerts]

[Status KPI strip]
Ops Status | Last Successful Fetch | Fetch Success | Failed Source Ratio | Pending Approvals | Last Sync

[Pipeline lane]
Discovery: idle/completed/running
Fetch: idle/completed/running
Sync: idle/completed/running

[Schedule / Sync summary]
Fetcher schedule | Discovery schedule | Last run | Registry sync counts
```

Design notes:

- Alerts stay first because they answer whether the operator must act now.
- KPI cards stay compact; each card should be one label, one value, and at most one small qualifier.
- The pipeline lane is a row of three compact status cells, not large cards. It should show task type, status, one progress phrase, and one count line.
- The pipeline lane should not include raw logs, event timelines, or stale-task resolution controls in this plan.

#### Main console: latest fetch health

The main metrics area should be a two-column console on desktop and a single column on mobile:

```text
Runtime                 Failures
Source Health           Source Policy Signals
Dedup Review            Social Signals
Diagnostics             full-width support strip
```

Recommended section behavior:

- `Runtime`: duration, median/average/window, slowest stage, slowest source, high-cost low-yield source.
- `Failures`: failed source count, grouped detail failures, top failure buckets, browser fallback recommendation count.
- `Source Health`: zero-kept sources, sources needing attention, top productive sources, unstable/failed providers.
- `Source Policy Signals`: runtime-suppressed static sources, provider coverage review, overlap audit, conservative cleanup readiness.
- `Dedup Review`: audit gate, current-run merges, provider/static disagreement blockers, review queue count, top examples.
- `Social Signals`: social kept/unique/dropped counts when present; hidden or collapsed when absent.
- `Diagnostics`: copy normalized section summaries, expose raw-ish bounded data behind `<details>`, and avoid becoming the primary reading path.

Each section should follow the same visual rhythm:

```text
Title + severity chip
2-4 metric chips
Top 3 examples or "No issues"
Optional details disclosure
Optional copy diagnostics button
```

#### Review and history below the console

Review queues and historical context should sit below the main console:

```text
Source Policy Review queue
Trends
Runs
```

The Source Policy Review queue remains a dedicated action surface because it mutates local review state. Trends and Runs stay below the current health view because they support investigation after the operator understands the latest status.

#### Responsive behavior

- Desktop: KPI strip uses dense auto-fit cards; pipeline lane stays one row when possible; metrics sections use two columns.
- Tablet: pipeline lane can wrap to two rows; metrics sections can remain two columns if readable.
- Mobile: all sections stack; task lane cells use short labels and avoid wide tables.
- Long source names and URLs must wrap inside their section without expanding the page horizontally.
- `<details>` disclosures should preserve scroll position and should not trigger full dashboard rerenders on open/close.

### 3. Preserve action wiring while sectionizing

Dedup review buttons currently attach inside `renderAdminOpsFetcherMetrics()` using `options.onDedupReviewAction`.

Source-policy review and migration-link buttons attach through `renderAdminSourcePolicyReview()`.

The refactor must keep these callbacks stable and covered by unit tests. Avoid new global event delegation unless it clearly reduces complexity.

### 4. Add progressive disclosure and diagnostics copy

Render top items compactly and expose longer slices through `<details>` or explicit "Show more" controls.

Add a minimal copy action for normalized section JSON/payload slices. The first version should copy normalized bounded section summaries, not full raw bridge payloads. If full raw copy is later needed, it should be explicit, bounded, and redacted.

### 5. Add a compact task-status lane

Use the existing derived run model from [`frontend/admin/domain/runs.js`](../../frontend/admin/domain/runs.js).

Show Discovery / Fetch / Sync as compact status cells in Operations Health. Do not replace [`frontend/admin/render/ops-history.js`](../../frontend/admin/render/ops-history.js) in this milestone.

The lane should expose only overview fields:

```text
task type
display status
progress label
primary count
started/elapsed or last completed time
```

Detailed task cards, stalled/orphaned resolution, run timelines, and event diagnostics should remain deferred to [`task-progress-operational-console-plan.md`](task-progress-operational-console-plan.md).

### 6. Keep docs and rollout boundary aligned

Keep this dedicated active plan doc registered in [`docs/INDEX.md`](../INDEX.md). Do not touch runtime contracts in this pass.

## Progress Checklist

- [x] Add dedicated active plan doc.
- [x] Register it in [`docs/INDEX.md`](../INDEX.md).
- [x] Validate current route contracts and frontend ownership.
- [x] Add `frontend/admin/domain/ops-health-view-model.js`.
- [x] Refactor `/ops/fetcher-metrics` rendering to consume sectioned view-model groups.
- [x] Preserve dedup review button wiring after sectionization.
- [x] Keep Source Policy Review as a separate action queue and add only summary/context in health.
- [x] Add progressive disclosure for long example lists.
- [x] Add diagnostics-copy behavior for normalized/bounded section payloads.
- [x] Add compact task-status lane using the existing run model.
- [x] Keep detailed task-card/stale-state behavior deferred to the task/progress plan.
- [x] Add/adjust styling for compact section cards and long-list readability.
- [x] Add unit test coverage for section rendering.
- [x] Add unit test coverage for bounded lists, copy behavior, and action handlers.
- [x] Add unit test coverage for the compact task-status lane.
- [ ] Reconcile smoke selectors that assume `#admin-ops-fetcher-metrics` is populated.

## Risks and Constraints

### Source-policy queue must stay actionable

The existing `#admin-source-policy-review` block is already a dedicated action queue. The health-dashboard refactor should summarize source-policy signals in the console, but the review queue should remain independently visible and independently testable.

### Dedup rendering has existing coverage

Several frontend unit tests import `renderAdminOpsFetcherMetrics()` through stable render exports. The implementation can add helpers, but should keep the public renderer export stable unless tests and docs are updated in the same change.

### Copy diagnostics can leak too much context

The safest first version copies normalized section summaries, not full raw bridge payloads. If raw copy is still needed, the implementation should make it explicit, bounded, and redacted.

### Task lane overlaps the task/progress plan

This plan should only add a compact health-dashboard lane. Detailed current-run cards and stale/orphaned task UX belong to [`task-progress-operational-console-plan.md`](task-progress-operational-console-plan.md).

If the task/progress plan lands first, this plan should consume its shared task-run view model for the compact lane instead of creating duplicate task interpretation logic.

### Section cards should stay dense

This is an operational console, not a landing page. Use compact panels, stable grid tracks, predictable labels, and bounded examples.

## Validation Targets

Frontend unit tests:

```text
tests/frontend/unit/admin-render.test.mjs
tests/frontend/unit/admin-source-health-render.test.mjs
tests/frontend/unit/admin-dedup-*-render.test.mjs
tests/frontend/unit/admin-ops-controller.test.mjs
```

Smoke selector to preserve:

```text
#admin-ops-fetcher-metrics should still render a non-loading state.
```

Contract checks:

```text
No backend endpoints added, removed, or reshaped.
GET routes remain read-only.
Existing review-action callbacks still call the same bridge routes.
```

## Deferred Ideas

- Backend-provided section summaries.
- Persisted operator dashboard preferences.
- Full replacement of the Runs table with cards.
- Stale/orphaned task state UX.
- New metrics or telemetry fields.

## Assumptions

- The dashboard rewrite is intentionally UI/UX-only for this step.
- Existing payload fields remain stable enough for frontend-only normalization without contract updates.
- The new layout should stay in the readability and triage-speed lane and not alter source-of-truth semantics.

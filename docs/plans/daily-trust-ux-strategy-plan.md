# Daily-Trust UX Strategy Plan

> - **Status:** Active strategy plan
> - **Use this when:** improving Baluffo's frontend/admin experience so it feels trustworthy for daily use, especially around operational health, recovery, diagnostics, exploration, and workflow polish
> - **Canonical for:** prioritized daily-trust UX direction, dependency evaluation stance, loophole guardrails, and acceptance gates for trust-oriented UI work
> - **Not canonical for:** bridge route payload contracts, saved-job data model contracts, storage authority behavior, packaging/release behavior, or approval to add dependencies
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../storage-contract.md`](../storage-contract.md), [`../testing.md`](../testing.md), [`saved-job-tracker-improvements-plan.md`](saved-job-tracker-improvements-plan.md), and [`ai-modification-safety-improvements-plan.md`](ai-modification-safety-improvements-plan.md)
> - **Last updated:** 2026-05-13

## Confidence boundary

The original strategy should not be treated as 100% safe as written. It mixed good product direction with possible scope creep, dependency creep, duplicate diagnostics surfaces, and overly broad rollback language.

The revised strategy is confidence-gated instead:

- Product impact is validated through implementation, screenshots, tests, and user feedback.
- Repo safety is enforced through known contracts, narrow reuse of existing endpoints, and explicit dependency approval.
- "100% confident" means no known repo-policy, data-contract, rollback, dependency, or diagnostics loophole remains unaddressed in the plan.

## Product references

Use these products as pattern references, not architecture templates:

- Chronicle: structured information density, professional hierarchy, low-clutter presentation quality.
- Metabase: filter widgets, saved questions/views, default values, apply/clear semantics, and exploration without duplicate dashboards.
- Hoppscotch: developer-focused history, collections, favorites, environments, and keyboard-first workflows.
- Activepieces: workflow discoverability, run debugging, step status, and view-only run evidence panels.
- Sentry: issue details, breadcrumbs, tags, activity, event navigation, actionable diagnostics, and bounded context for triage.

Baluffo should borrow stable interaction patterns from these tools while preserving its current vanilla HTML/CSS/JS frontend, Python bridge contracts, local-first storage, and desktop runtime constraints.

## Repo-grounded constraints

- Do not add Python or Node dependencies without explicit approval.
- Treat bridge and route signature changes as compatibility work. Search route call sites and frontend payload builders before changing signatures.
- Keep task lifecycle state runId-owned and avoid making diagnostics lifecycle authority.
- Keep storage rollback behavior internal unless there is an explicit, existing user-safe action.
- Respect the prior Admin health dashboard closeout: detailed evidence belongs in existing Ops, Run History, Source Policy, Dedup, and Storage surfaces.
- Keep monitor-only dedup/source diagnostics from becoming blocking workflow gates.
- Avoid changing saved-job row contracts unless the saved-job tracker plan and data contracts are updated together.

## Prioritized plan

### P0 - Workspace Action Center

Build an attention-focused Action Center instead of another broad health dashboard.

- Show only 3-5 items that need attention now.
- Source from existing `/ops/health`, `/ops/task-state`, `/ops/task-live/*`, `/ops/storage-health`, startup metrics, alerts, sync state, and fetch report summaries.
- Deep-link to existing evidence surfaces instead of duplicating dense panels.
- Group items by user intent: keep browsing jobs, recover a failed operation, inspect runtime health, or copy diagnostics for support.

Acceptance gates:

- No duplicate dense Operations Health panel.
- Every item has a clear state, reason, and next action.
- Advanced evidence stays behind disclosure or a deep-link.

### P0 - Honest inline recovery

Add recovery actions only where the underlying operation is already safe and understood.

- Use **Retry** for idempotent or already-supported retry flows.
- Use **Restore** for rejected/deleted/tombstoned source flows that already support restore semantics.
- Use **Revert** only where a real inverse exists, such as existing saved-job undo flows.
- Use **Copy diagnostics** when there is no safe automatic recovery.
- Do not imply rollback for destructive actions such as wiping an account or deleting selected sources.

Acceptance gates:

- Every recovery label maps to a real backend/frontend behavior.
- No destructive operation gains a fake rollback affordance.
- Existing busy-state, task-start, completion, and log-polling behavior remains compatible.

### P1 - Context-aware right inspector

Add a reusable right-side inspector shell for selected entities.

Supported v1 entities:

- Job
- Saved job
- Source
- Registry conflict
- Task run
- Alert
- Storage/runtime surface

V1 behavior:

- Read-only by default.
- Reuse existing payloads first.
- Show selected entity summary, tags/status, recent events, related actions, and links to evidence.
- Allow only existing safe actions, such as retry, restore, reattach, acknowledge, or copy diagnostics.

Acceptance gates:

- Inspector adapters are typed and bounded by entity kind.
- The inspector does not become a second page-sized dashboard.
- Selecting rows across Jobs, Saved, and Admin preserves page context.

### P1 - Sticky search, recent history, and saved views

Improve exploration using existing Jobs URL persistence and filter state.

- Keep search/filter state visible and clearable.
- Add bounded recent views for common Jobs/Admin/Saved contexts.
- Add saved filter presets at the profile/preferences level.
- Keep saved views separate from canonical saved-job rows in v1.
- Support smart defaults and prefilled forms where context is obvious, such as selected source, selected status, or current task type.

Acceptance gates:

- Saved views do not change the saved-job data contract.
- Recent history is bounded and clearable.
- Search/filter state works with browser navigation and existing URL persistence.

### P1 - Explain-this-state overlays

Add compact explanations for confusing states.

Initial targets:

- Fetch/discovery/sync running, blocked, stale, failed, or detached.
- Source conflict and source policy review states.
- Dedup readiness and monitor-only diagnostics.
- Storage authority mode, rollback state, and health warnings.
- Desktop startup readiness and bridge availability.

Rules:

- Use redacted, bounded diagnostics only.
- Explain what happened, why Baluffo thinks so, and what the next safe action is.
- Never mutate state from the overlay itself unless invoking an existing safe action.

Acceptance gates:

- No secrets, tokens, or sensitive credentials appear in copied diagnostics or overlays.
- Diagnostic data remains explanatory, not authoritative.

### P2 - System map and visual breadcrumbs

Add a generated system map page that helps contributors and operators understand Baluffo.

Show:

- Frontend pages and their bridge surfaces.
- Bridge routes and owning modules.
- Task flows for fetch, discovery, sync, storage, startup, and local data.
- Queues, evidence files, runtime storage surfaces, and compatibility exports.
- Risk markers for compatibility roots, packaging, release, and public data contracts.

Pair this with visual breadcrumbs in UI flows:

- Page -> entity -> run/source/job -> evidence.
- Nested task -> phase -> work item -> diagnostic event.

Acceptance gates:

- Generated snapshots derive from checked route/module inventory where possible.
- Route additions are paired with inventory/docs updates.
- The map does not become a manually maintained diagram that can drift silently.

### P2 - Demo fixtures and contributor snapshots

Create isolated demo fixtures for important daily-trust workflows.

Scenarios:

- Successful fetch with fresh jobs.
- Failed fetch with retryable source failures.
- Storage rollback or unhealthy authority mode.
- Source conflict requiring review.
- Sync offline, failed, and recovered states.
- Saved-job activity and revert flow.
- Empty first-run state.

Rules:

- Use isolated fixture/test data roots only.
- Do not write into real `data/`.
- Reference fixtures from tests or allowlist them in repo guardrails.

Acceptance gates:

- Fixtures power Playwright or frontend unit coverage.
- Demo states remain deterministic and easy to refresh.

### P3 - Extension registry, hotkeys, tags, and favorites

Add these only after the P0/P1 trust loop is stable.

- Extension registry v1 is read-only metadata over existing adapters/modules.
- No dynamic plugin loading or execution in v1.
- Hotkeys start as a command palette and ignore editable fields.
- Tags and favorites start as profile preferences, not canonical saved-job row changes.
- Rollback affordances stay limited to existing safe operations.

Acceptance gates:

- Hotkeys do not conflict with inputs, textareas, selects, or browser/system shortcuts.
- Tags/favorites do not collide with saved-job tracker contract work.
- Extension registry cannot execute arbitrary code.

## Dependency evaluation

No new dependency is part of v1.

Candidate stance:

- **Jotai:** defer. Only relevant if localized frontend state becomes too hard to reason about in vanilla modules.
- **XState:** defer. Only relevant for complex task/workflow UI state with explicit state charts and tests.
- **React Scan:** reject for now. Current frontend is not React.
- **Ky:** defer. Consider only if existing fetch wrappers become repetitive enough to justify dependency approval.
- **UnoCSS:** defer. Consider only if existing CSS/tokens become unmaintainable and the build/tooling cost is approved.
- **Tiptap:** defer. Notes/comments/docs should not introduce a rich editor until plain text/Markdown UX is proven insufficient.
- **LogTape:** defer. Existing bridge diagnostic events already provide lightweight observability; identify a concrete gap first.
- **Mock Service Worker:** possible later for isolated UI development, but only after proving Playwright/fixture mode is insufficient.

Any dependency proposal must include:

- The specific problem it solves.
- Why existing repo patterns are insufficient.
- Bundle/build/test impact.
- Migration and rollback cost.
- Explicit user approval before adding it.

## Loophole fixes

- **Health dashboard re-bloat:** use an Action Center with deep-links, not another dense dashboard.
- **Inspector monolith:** use a shared shell with small entity adapters.
- **Saved-view contract collision:** persist v1 saved views as preferences, not saved-job rows.
- **Fake rollback:** only expose real Retry, Restore, Revert, or Copy Diagnostics actions.
- **Diagnostic leakage:** use existing redaction and bounded payload rules.
- **Safe mode hiding problems:** make safe/debug boot visibly read-only and mutation-free by default.
- **System map drift:** generate from inventories and tests where possible.
- **Fixture pollution:** keep demo data isolated from real runtime data.
- **Hotkey accessibility:** ignore editable controls and keep command access discoverable.
- **Dependency creep:** require explicit approval and an ADR-style justification.
- **Existing active plan collisions:** route saved-job model work through the saved-job tracker plan and route inventory/snapshot work through the AI modification safety plan.
- **Performance/layout regression:** verify dense UI at desktop and constrained widths with unit and Playwright coverage.

## Test plan

Use the narrowest checks that match the touched area:

- Frontend unit tests for Action Center prioritization, inspector adapters, saved view persistence, recent-history bounds, action labeling, and hotkey suppression.
- Python/bridge tests only when new payload fields or routes are introduced.
- Playwright smoke coverage for Admin, Jobs, and Saved across normal, empty, failed, running, and recovery states.
- Repo guardrails for generated fixtures, route inventory, and docs discoverability.

Baseline commands to consider:

```text
npm run test:frontend:unit
npm run test:frontend
npm run test:py
npm run lint:repo-guardrails
```

Use broader checks when bridge routes, storage authority, packaging, or release-sensitive paths are touched.

## Completion definition

A daily-trust UX slice is complete only when:

- The UI answers "what happened, why, and what can I safely do next?"
- Existing contracts remain compatible or are updated with tests and docs.
- New surfaces reuse existing evidence before adding routes.
- Failure and recovery states are covered by tests or deterministic fixtures.
- Screenshots or Playwright checks show no layout overflow, nested-card bloat, or inaccessible controls.
- No new dependency was added without explicit approval.

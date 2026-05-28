# Daily-Trust UX Strategy Plan

> - **Status:** Archived — fully implemented 2026-05-28
> - **Use this when:** reviewing the implementation history of daily-trust UX improvements (workspace Action Center, right inspector, recovery actions, saved views, explain-this-state overlays, system map, demo fixtures)
> - **Canonical for:** archived daily-trust UX strategy record
> - **Not canonical for:** current frontend behavior; see active docs and source files instead
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../storage-contract.md`](../storage-contract.md), [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-28

## Confidence statement

This plan has been loophole-audited against the codebase on 2026-05-28. Every known gap between the plan's assumptions and the actual codebase state has been found and fixed below. 20 loopholes were identified and closed across data sources, signal triggers, dismissal semantics, delivery ordering, UI patterns, rollback policy, and adjacent features.

"100% confident" means no known repo-policy, data-contract, rollback, dependency, or diagnostics loophole remains unaddressed in the plan.

## Refined scope (2026-05-28)

The plan was reviewed item-by-item on 2026-05-28 and scoped down to the following cuts. Items not listed here are intentionally out of scope.

### Scoped in — delivery order

| Order | Item | Tier | Scope |
|-------|------|------|-------|
| 1 | **Workspace Action Center** | P0 | Widget on Admin page, always expanded, max 3 items, 30s auto-refresh, auto-resolve + manual dismiss with 4h TTL, compact cards, one-click Retry with toast, Copy diagnostics = raw JSON. Signals: stale fetch (12h threshold), failed sources (partial failures only, not when full fetch failed), sync status (`lastResult` or `config.ready`), storage health (`healthy` flag or diagnostic failures). Shows single "✓ All systems operational" when healthy. |
| 2 | **Copy all diagnostics button** | P0 | "Copy all diagnostics" button at the bottom of the Action Center widget. Serializes the 3 parallel endpoint responses (`/ops/health`, `/sync/status`, `/ops/storage-health`) into a single JSON blob on clipboard. |
| 3 | **Admin section nav bar** | P0 | Sticky nav quick-links below the Admin page title linking to each major section (Ops, Fetcher, Discovery, Sync). Pure CSS/HTML with scroll-into-view. |
| 3 | **Discovery pending count in section header** | P0 | Add `(N pending)` badge to the "Source Discovery" section header, sourced from the already-loaded discovery candidates count. |
| 5 | **Fetcher log error highlighting** | P0 | Colorize error-level fetcher log lines with a distinct background; make them click-to-expand for full error details. |
| 6 | **Context-aware right inspector** | P0 | Slide-out overlay on selection. Admin-only for v1 (Jobs/Saved pages have no selection model). Entity adapters for sources, registry conflicts, task runs, alerts, and storage surfaces. |
| 7 | **Honest inline recovery** | P0 | Retry/Restore/Revert/Copy actions in the inspector only. No inline recovery buttons on feed rows. |
| 8 | **Sticky search, history, saved views** | P1 | localStorage only (no bridge changes). Saved views as profile preferences, not saved-job rows. |
| 9 | **Explain-this-state overlays** | P1 | Click-to-reveal expandable card (not hover tooltip). Explain what happened, why, and next safe action. No state mutation from overlay. |
| 10 | **System map & visual breadcrumbs** | P2 | Generated from route/module inventory. For both operator and contributor use. Deferred past P0/P1. |
| 11 | **Demo fixtures** | P2 | Isolated test data for Playwright/UI tests. Build alongside or after P0/P1 features they test. |

### Scoped out (dropped)

- Extension registry v1
- Hotkeys / command palette
- Tags and favorites
- Any P3 items

## Loophole audit (2026-05-28)

Each loophole was validated against the actual codebase. The fix describes what changed in this plan.

### Action Center loopholes

**L1 — Consolidated data source (closed):** The plan said "source from existing /ops/health, /ops/task-state, fetch report summaries, and sync state." This would require 3-4 separate frontend API calls per poll cycle. All four planned signals already have dedicated endpoints with exactly the data needed. Fix: the frontend calls 3 endpoints in parallel on each poll — `GET /ops/health` (stale fetch + failed sources), `GET /sync/status` (sync), `GET /ops/storage-health` (storage). No new bridge endpoint is needed and no `/ops/health` extension is required. Each signal maps to a single field check from its endpoint response.

**L2 — Stale fetch threshold undefined (closed):** The plan said "Last successful fetch > threshold hours ago" but did not define the threshold. The existing backend constant `STALE_FETCH_HOURS = 12` (`ops_health.py:14`) is the canonical threshold. Fix: use the same 12-hour threshold. The Action Center checks `GET /ops/health` → `kpis.lastSuccessfulFetchAge` and compares to 12h.

**L3 — Signal overlap deduplication (closed):** When a full fetch fails entirely (no successful output), both "stale fetch" AND "failed sources" would trigger simultaneously, showing two redundant items. Fix: these two signals are mutually exclusive in the Action Center. "Stale fetch" covers the case where there IS no recent successful fetch (including total failure). "Failed sources" only appears when there IS a recent successful fetch (within 12h) but some sources within it failed. If the last fetch had zero successful output, show "stale fetch" only. Detection: check `kpis.lastSuccessfulFetchAge < 12h`; if false, it's stale fetch; if true, check `kpis.failedSourceRatioLatest > 0` for failed sources.

**L4 — Sync status trigger ambiguous (closed):** The plan said "Sync offline, failed, or pending" without defining precise triggers. Fix: the sync status signal triggers when any of:
- `GET /sync/status` → `runtime.lastResult` is `"error"` AND `runtime.lastAction` is recent (<24h since last action)
- `GET /sync/status` → `config.enabled` is true AND `config.ready` is false (sync configured but not ready)
- `GET /sync/status` → `runtime.lastError` is a non-empty string
Not triggered by: sync being disabled by choice, or sync having no history yet (never run).

**L5 — Storage health trigger ambiguous (closed):** The plan listed "unhealthy authority mode, no recent backups, WAL mode issue" without priority. Fix: triggers when `GET /ops/storage-health` → `storage.healthy` is false, OR any diagnostic entry in `storage.diagnostics` has `ok: false`. The `storage.authorityModes` field (JSON vs SQLite) is informational ONLY — authority mode differences are expected during transitions and are not action items. Backups age is also informational — the storage layer manages backups internally.

**L6 — Dismiss vs auto-resolve race (closed):** The plan said "hides until the same condition re-triggers" — but conditions are re-evaluated on every poll. If a user dismisses "stale fetch" while the fetch is still stale, the condition is still true on the next poll, so the item would never reappear. Fix: dismiss has a **4-hour TTL**. The dismissed item stays hidden for 4 hours from the dismiss timestamp, then re-evaluates. On next poll after the TTL expires, if the condition is still active, the item reappears. This effectively acts as a snooze. Auto-resolve (condition clears) still works immediately — it checks at every poll and removes the item regardless of dismiss state.

**L7 — Dismiss persistence undefined (closed):** The plan didn't specify where dismiss state lives. Fix: dismissed state is stored in localStorage with key pattern `baluffo_action_dismissed_{alertId}` and an ISO timestamp value. Example: `baluffo_action_dismissed_stale_fetch = "2026-05-28T12:00:00Z"`. No bridge changes. On page load, the dismiss state is read from localStorage. This means dismiss survives page refresh but not browser data clear or a different browser.

**L8 — Item ordering undefined (closed):** The plan didn't define priority when 3+ signals are active. Fix: order by:
1. **Critical alerts first** — if any signal has a critical severity in its source endpoint, it renders first. All warning-level items after.
2. **Within same severity, fixed priority:** (a) storage health (data integrity risk), (b) stale fetch (data freshness), (c) sync status (sync continuity), (d) failed sources (partial quality).
3. If more than 3 items are active after ordering, only the first 3 are shown. A `View all → Ops Health` link appears at the bottom of the widget.

**L9 — "View all" target undefined (closed):** The plan said "deep-link to Ops Health" but Ops Health is a section on the Admin page, not a separate page. Fix: "View all → Ops Health" scrolls the Admin page to the `#admin-content` section and activates the Ops tabs by clicking the "Overview" tab button (`admin-ops-tab-overview-btn`). This reveals the full alerts list and all ops panels.

**L10 — Concurrent action conflicts (closed):** If the user clicks Retry on the Action Center and also clicks "Run Jobs Fetcher" on the existing Fetcher section simultaneously, both try to start a task. The bridge already handles this correctly — the second call returns `{started: false, alreadyRunning: true}`. Fix: the Action Center Retry button handles `alreadyRunning: true` in the response by showing a toast "Fetch is already running" instead of the success toast. No additional backend changes needed.

### Inspector and recovery loopholes

**L11 — Delivery order mismatch (closed):** The plan listed Honest Inline Recovery as P0 but placed it after the P1 inspector in delivery order, with the note "actions live in the inspector." A P0 feature cannot depend on a P1 component. Fix: promote the inspector to P0. The inspector ships first (Order 2), then recovery actions within it (Order 3). Both are P0 and ship in the same overall slice — they just build in sequence.

**L12 — Entity data sources undefined (closed):** The plan said "reuse existing payloads" without specifying which payloads for each entity. Fix: precise mapping:
- **Source**: from source registry row (already rendered in Admin tables) + `GET /ops/health` → source-level diagnostics from the fetch report's `sources[]` array
- **Registry conflict**: from `GET /registry/conflicts?view=summary`
- **Task run**: from `GET /ops/task-state?view=summary` → current running/recent tasks
- **Alert**: from `GET /ops/health` → `alerts[]` array for the selected alert
- **Storage surface**: from `GET /ops/storage-health` → `storage` object and `diagnostics[]`
- Job entity: considered out of scope for v1 (see L13 below)

**L13 — Inspector scope: Admin-only for v1 (closed):** The plan said the inspector works "across Jobs, Saved, and Admin." But Jobs feed rows and Saved job rows have no selection mechanism. Adding one is a separate feature. Fix: v1 inspector is **Admin-only**. The Admin page already has selectable rows in source tables (pending/active/rejected), conflict tables, and task views. The inspector attaches to existing click handlers on these Admin elements. Jobs and Saved page integration is deferred until a selection model exists there.

### Explain-this-state loophole

**L14 — Tooltip UX anti-pattern for multi-line content (closed):** The plan specified "hover tooltip only" for explain-this-state overlays. Multi-line explanations in hover tooltips are a known UX anti-pattern — the tooltip disappears when the cursor moves to read it. Fix: use a **click-to-reveal expandable card** instead of hover. The existing popup overlay pattern (`frontend/shared/ui/popup-presentation.js`) is the right foundation. Hover tooltips remain available for single-word or single-icon hints (via the existing `data-tooltip` system), but multi-line diagnostics use a compact click-to-expand card that stays open until dismissed.

### General loopholes

**L15 — Poll interval undefined (closed):** The plan said "auto-refresh" without specifying interval. Fix: 30-second poll interval. This is slower than the existing Ops Health poll (2s active, 10s idle) because Action Center conditions change on the scale of minutes (fetch runs take minutes), and 30s avoids unnecessary bridge load. The Action Center shares the same bridge client and respects online/offline state — no polling when offline.

**L16 — No rollback plan for Action Center (closed):** The plan didn't specify how to revert if the Action Center causes layout regressions. Fix: the Action Center is a self-contained widget injected into the Admin page DOM and controlled by a single JS module. Reverting means:
1. Remove the Action Center widget element from `admin.html`
2. Remove the Action Center controller import from `frontend/admin/app/runtime.js` composition
3. No bridge changes means zero backend rollback
4. If dismiss state in localStorage causes issues, clear keys matching `baluffo_action_dismissed_*`

### Adjacent feature loopholes

**L17 — Admin nav bar section coverage (closed):** The nav bar lists all major Admin sections with hardcoded links in HTML. If sections are renamed or removed, nav links drift silently. Fix: the nav bar targets four stable section IDs (`admin-ops-panel`, `admin-fetcher-panel`, `admin-discovery-panel`, `admin-sync-panel`). These have not changed since the Admin page was created. If a section is removed, the corresponding link becomes a no-op (click does nothing) rather than throwing an error. No JS maintenance needed.

**L18 — Discovery pending count loading order (closed):** The discovery pending count badge reads from state that may not be loaded yet when the badge first renders, briefly showing nothing before updating. Fix: the badge is controlled by the existing discovery controller. It starts hidden and only appears when the controller sets a non-zero pending count. The same `admin-discovery-summary` element that shows "No discovery report loaded yet" handles this lifecycle — the badge follows the same pattern.

**L19 — Fetcher log error click-to-expand with live region (closed):** The fetcher log is a `role="log"` live region. Adding click-to-expand interactivity inside a live region can confuse screen readers because the live region announces content changes. Fix: expanded error details use a `<div>` inside the log row with `aria-hidden="true"` when collapsed and `aria-hidden="false"` when expanded. The live region (`aria-live="polite"`) only announces new log rows, not expand/collapse state changes within existing rows. The toggle state is managed by a CSS class on the row element, not by DOM insertion/removal.

**L20 — Copy all diagnostics partial endpoint failure (closed):** If one of the 3 source endpoints (`/ops/health`, `/sync/status`, `/ops/storage-health`) fails during the poll cycle, the "Copy all diagnostics" button would have incomplete data. Fix: the button serializes whatever data is available in memory from the most recent poll. Failed endpoints are represented as `{"error": "endpoint failed: <message>"}` placeholders in the JSON blob. The clipboard JSON always includes a `"_meta": {"generatedAt": "<iso>", "partial": true}` field when any endpoint failed. This ensures the copy always succeeds and the recipient can see what data might be missing.

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
- Dismiss state uses localStorage only — no bridge alert state changes from Action Center dismissals.

## Prioritized plan

### P0 - Workspace Action Center

Build a compact attention-focused widget on the Admin page.

**Placement:** Widget on the Admin page, always expanded (not collapsible). Existing admin nav links remain below (`Ops Health ▸`, `Run History ▸`, etc.).

**Max items:** Up to 3 items shown. Ordered by severity (critical first), then by priority: storage health > stale fetch > sync status > failed sources. If more signals are active, a `View all → Ops Health` link appears at the bottom that scrolls to the Ops section and activates the Overview tab.

**Poll interval:** 30 seconds. Respects online/offline state — no polling when offline.

**Signals (4 total) — each sourced from a single existing endpoint:**

| Signal | Source endpoint | Trigger condition | Actions |
|--------|---------------|-------------------|---------|
| Stale fetch | `GET /ops/health` → `kpis.lastSuccessfulFetchAge` | Age > 12h (`STALE_FETCH_HOURS`). Not shown when the last fetch had zero output (that's a total failure, covered by stale fetch only). | `[▶ Review in Ops]` `[🔄 Run Jobs Fetcher]` `[✕ Dismiss]` |
| Failed sources | `GET /ops/health` → `kpis.failedSourceRatioLatest` | Ratio > 0 AND `lastSuccessfulFetchAge` < 12h (only shown when there IS a recent successful fetch with partial failures). | `[▶ Review failed]` `[🔄 Retry failed]` `[✕ Dismiss]` |
| Sync status | `GET /sync/status` | Triggered when: `runtime.lastResult === "error"` and last action < 24h ago, OR `config.enabled === true` and `config.ready === false`, OR `runtime.lastError` is non-empty. Not triggered when sync is disabled by choice or has no history. | `[▶ Review sync]` `[🔄 Retry sync]` `[✕ Dismiss]` |
| Storage health | `GET /ops/storage-health` | Triggered when: `storage.healthy === false`, OR any `storage.diagnostics[]` entry has `ok: false`. Authority modes (`storage.authorityModes`) are informational and do NOT trigger. | `[▶ Storage health]` `[📋 Copy diagnostics]` `[✕ Dismiss]` |

**Card design:** Compact — icon, 1-2 line summary, action buttons. Sources from 3 parallel endpoint calls (`/ops/health`, `/sync/status`, `/ops/storage-health`) on each poll. Deep-link to existing evidence surfaces instead of duplicating dense panels.

**Dismissal (closed L6, L7):**
- Auto-resolve: when the condition clears, the item disappears on the next poll (within 30s).
- Manual dismiss: stored in localStorage as `baluffo_action_dismissed_{signalId}` with an ISO timestamp value. The item stays hidden for **4 hours** from the dismiss timestamp, then re-evaluates on the next poll. If still active after 4h, it reappears. If the condition cleared during the 4h window, it disappears via auto-resolve.
- No bridge alert state is modified by Action Center dismissals.

**Action behavior:**
- Retry — one-click, starts immediately with a confirmation toast. Uses the same `retry_failed` preset as the existing Admin "Retry Failed Sources" button. If the bridge responds with `alreadyRunning: true`, shows "Fetch is already running" toast instead.
- Review — deep-link to the relevant Admin tab (scrolling to Ops section and activating the correct tab).
- Copy diagnostics — copies raw JSON of the relevant payload section to clipboard.
- Dismiss — hides the item for 4 hours (see L7 above).

**Healthy state:** When no signals are active, show a single compact green `✓ All systems operational` line.

**Acceptance gates:**
- No duplicate dense Operations Health panel.
- Every item has a clear state, reason, and next action.
- Advanced evidence stays behind disclosure or a deep-link.
- All 3 parallel endpoint calls complete within 5s or the widget shows a loading state.

**Rollback plan (closed L16):**
- Remove the Action Center widget element from `admin.html`
- Remove the Action Center controller import from `frontend/admin/app/runtime.js` composition
- Zero backend rollback needed (no bridge changes)
- Clear `baluffo_action_dismissed_*` keys from localStorage if dismiss state causes issues

### P0 - Copy all diagnostics button

Add a "Copy all diagnostics" button at the bottom of the Action Center widget. This lets the user grab the full operational state for pasting into a GitHub issue or diagnostics context with one click.

**Implementation:**
- Button labeled "Copy all diagnostics" placed below the Action Center items or the "All systems operational" line
- On click, the button serializes the 3 parallel endpoint responses already fetched by the Action Center (`GET /ops/health`, `GET /sync/status`, `GET /ops/storage-health`) into a single JSON blob
- JSON blob is written to clipboard via `navigator.clipboard.writeText()`
- After copying, a toast "Diagnostics copied" confirms the action
- No additional bridge calls — reuses data already in memory from the Action Center's poll cycle

**No new bridge endpoints or backend changes needed —** all three source endpoints already exist and are already called by the Action Center on every poll.

**Acceptance gates:**
- Copy includes all available signals, not just the active ones
- Copy succeeds even if one of the 3 endpoints failed (copies what's available with a note)
- Clipboard content is valid JSON

### P0 - Admin section nav bar

Add a sticky nav quick-links bar below the Admin page title, linking to each major scrollable section. This helps users navigate the Admin page without scrolling through every section sequentially, especially once the Action Center widget is added at the top.

**Implementation:**
- Pure HTML/CSS with `<a>` links using `href="#section-id"` for scroll-into-view
- `position: sticky; top: 0; z-index: 10` within the admin content area
- Sections linked: Operations Health, Fetcher Output, Source Discovery, Source Sync
- Stored Profiles Overview is excluded (always at top, too small to need a nav link)
- Each link scrolls smoothly to the corresponding `<section>` element
- Links render consistently in the nav bar even if a section is absent (click does nothing gracefully)

**CSS only — no JS controller needed.** The nav bar is a static element in `admin.html` with CSS sticky positioning and scroll-behavior.

**Acceptance gates:**
- Nav bar is sticky below the page title and above the Action Center widget
- Links scroll to the correct section without changing URL hash (or using hash if admin already handles it)
- Nav bar does not overlap or hide content when scrolling

### P0 - Discovery pending count in section header

Add a `(N pending)` badge to the "Source Discovery" section header showing how many pending sources are awaiting review.

**Implementation:**
- The discovery candidate count is already computed by the discovery controller and rendered in the pending sources table
- Add a small inline badge element to the section header that reads from the existing pending count state
- Badge only renders when count > 0 (no `"(0 pending)"` noise)
- Count displayed with `Intl.NumberFormat`; values > 999 shown as `"999+"` to avoid layout overflow
- Badge updates when the discovery report is loaded or refreshed

**No new data sources needed —** the pending count is already available in the existing discovery state.

**Acceptance gates:**
- Badge is not shown when pending count is 0
- Badge value stays consistent with the pending sources table count
- No layout shift when badge appears/disappears

### P0 - Fetcher log error highlighting

Colorize error-level and warning-level fetcher log lines with distinct backgrounds, and make error lines click-to-expand for full error details.

**Implementation:**
- The fetcher log already assigns CSS classes by log level: `admin-fetcher-line error`, `admin-fetcher-line warn`, `admin-fetcher-line info`, etc. (from `appendAdminLogRow` at `frontend/admin/render/logs.js:17`)
- No level-specific CSS styling exists currently
- Add CSS for `.admin-fetcher-line.error` and `.admin-fetcher-line.warn`:
  - Warning level: subtle yellow/amber background left-border
  - Error level: subtle red/pink background left-border
- Make error/warn log lines click-to-expand:
  - Error/warn lines get a click handler that toggles a hidden child `<div>` with the full error context
  - The expansion shows the raw error text and any available diagnostic details
  - Click handler is attached in `appendAdminLogRow` based on the event level
- CSS transition for smooth expand/collapse animation

**No backend changes needed —** all log levels and messages are already provided by the existing fetcher bridge and local logging.

**Acceptance gates:**
- Error lines are visually distinct from info/muted lines by background color
- Click-to-expand works on both local log error lines and server-sourced error lines
- Expanding does not disrupt the `role="log"` live region or auto-scroll behavior
- Expansion state is per-row (expanding one error does not expand others)

### P0 - Context-aware right inspector

Add a reusable right-side slide-out overlay for selected entities. Admin-only for v1 (Jobs/Saved pages have no selection model).

Uses a dedicated slide-out component (not the existing centered `popup-presentation.js`). The component positions a panel from the right edge with a fixed overlay backdrop behind it.

Supported v1 entities (with exact data sources):
- **Source** — from source registry row (existing DOM element click handler) + `GET /ops/health` → source diagnostics from fetch report
- **Registry conflict** — from `GET /registry/conflicts?view=summary`
- **Task run** — from `GET /ops/task-state?view=summary`
- **Alert** — from `GET /ops/health` → `alerts[]`
- **Storage surface** — from `GET /ops/storage-health`

Job and Saved Job entities are out of scope for v1 — they require a selection model that doesn't exist yet.

V1 behavior:
- Read-only by default.
- Show selected entity summary, tags/status, recent events, related actions, and links to evidence.
- Allow only existing safe actions, such as retry, restore, reattach, acknowledge, or copy diagnostics.

**Acceptance gates:**
- Inspector adapters are typed and bounded by entity kind.
- The inspector does not become a second page-sized dashboard.
- Selecting rows within the Admin page preserves page context.

### P0 - Honest inline recovery

Add recovery actions only where the underlying operation is already safe and understood. Actions live in the context inspector (not inline on feed rows).

- Use **Retry** for idempotent or already-supported retry flows (`retry_failed` fetcher preset for sources; existing task retry for task runs).
- Use **Restore** for rejected/deleted/tombstoned source flows that already support restore semantics (`POST /registry/restore-rejected`, `POST /registry/restore-deleted`).
- Use **Revert** only where a real inverse exists, such as existing source rollback (`POST /registry/rollback`) or saved-job undo flows.
- Use **Copy diagnostics** when there is no safe automatic recovery. Copies raw JSON of the entity's payload.
- Do not imply rollback for destructive actions such as wiping an account or deleting selected sources.

**Acceptance gates:**
- Every recovery label maps to a real backend/frontend behavior.
- No destructive operation gains a fake rollback affordance.
- Existing busy-state, task-start, completion, and log-polling behavior remains compatible.

### P1 - Sticky search, recent history, and saved views

Improve exploration using existing Jobs URL persistence and filter state. Persisted via localStorage only — no bridge changes.

- Keep search/filter state visible and clearable.
- Add bounded recent views (max 10 entries, LRU eviction) for common Jobs/Admin/Saved contexts.
- Add saved filter presets at the profile/preferences level (localStorage).
- Keep saved views separate from canonical saved-job rows in v1.
- Support smart defaults and prefilled forms where context is obvious, such as selected source, selected status, or current task type.
- Desktop mode: localStorage works identically in desktop runtime. The desktop flag (`window.BALUFFO_FRONTEND_RUNTIME_CONFIG.runtime.desktop`) is read-only informational; both modes use the same localStorage keys.

**Acceptance gates:**
- Saved views do not change the saved-job data contract.
- Recent history is bounded at 10 entries and clearable.
- Search/filter state works with browser navigation and existing URL persistence.

### P1 - Explain-this-state overlays

Add compact explanations for confusing states using a click-to-reveal expandable card pattern (NOT hover tooltip — multi-line text in hover tooltips is a known UX anti-pattern). Never mutate state from the overlay itself.

Initial targets:
- Fetch/discovery/sync running, blocked, stale, failed, or detached.
- Source conflict and source policy review states.
- Dedup readiness and monitor-only diagnostics.
- Storage authority mode, rollback state, and health warnings.
- Desktop startup readiness and bridge availability.

Rules:
- Short hover tooltip (via existing `data-tooltip` system) for single-word or icon-only hints.
- Multi-line explanation: click-to-reveal expandable card using the existing popup overlay pattern (`frontend/shared/ui/popup-presentation.js`).
- Use redacted, bounded diagnostics only.
- Explain what happened, why Baluffo thinks so, and what the next safe action is.
- Never mutate state from the overlay itself unless invoking an existing safe action.

**Acceptance gates:**
- No secrets, tokens, or sensitive credentials appear in copied diagnostics or overlays.
- Diagnostic data remains explanatory, not authoritative.

### P2 - System map and visual breadcrumbs

Add a generated system map page that helps contributors and operators understand Baluffo. Deferred past P0/P1.

Show:
- Frontend pages and their bridge surfaces.
- Bridge routes and owning modules.
- Task flows for fetch, discovery, sync, storage, startup, and local data.
- Queues, evidence files, runtime storage surfaces, and compatibility exports.
- Risk markers for compatibility roots, packaging, release, and public data contracts.

Pair this with visual breadcrumbs in UI flows:
- Page -> entity -> run/source/job -> evidence.
- Nested task -> phase -> work item -> diagnostic event.

**Acceptance gates:**
- Generated snapshots derive from checked route/module inventory where possible.
- Route additions are paired with inventory/docs updates.
- The map does not become a manually maintained diagram that can drift silently.

### P2 - Demo fixtures and contributor snapshots

Create isolated demo fixtures for important daily-trust workflows. Build alongside or after P0/P1 features they test.

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

**Acceptance gates:**
- Fixtures power Playwright or frontend unit coverage.
- Demo states remain deterministic and easy to refresh.

### Scoped out (was P3)

The following items from the original plan were reviewed and dropped:

- Extension registry v1 (read-only metadata catalog)
- Hotkeys / command palette
- Tags and favorites as profile preferences

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

All 20 identified loopholes are closed in-line above. This section summarizes them:

- **L1: Consolidated data source** — 3 parallel endpoint calls on each 30s poll, no new bridge endpoint needed.
- **L2: Stale fetch threshold** — 12h, matching `STALE_FETCH_HOURS` from `ops_health.py:14`.
- **L3: Signal overlap dedup** — stale fetch and failed sources are mutually exclusive; failed sources only shown when a recent successful fetch exists.
- **L4: Sync status trigger** — precise: `lastResult` error + recent action, or `config.ready` false while enabled, or non-empty `lastError`.
- **L5: Storage health trigger** — precise: `storage.healthy` false or any diagnostic `ok: false`. Authority mode is informational only.
- **L6: Dismiss vs auto-resolve race** — 4-hour TTL on manual dismiss. Auto-resolve still checks condition on every poll.
- **L7: Dismiss persistence** — localStorage key `baluffo_action_dismissed_{signalId}` with ISO timestamp value.
- **L8: Item ordering** — critical first, then storage > stale > sync > failed. Max 3 shown.
- **L9: "View all" target** — scrolls to `#admin-content`, activates Overview ops tab.
- **L10: Concurrent action conflicts** — already-handled by bridge (`alreadyRunning`); frontend shows appropriate toast.
- **L11: Delivery order mismatch** — inspector promoted to P0, recovery ships after inspector within same slice.
- **L12: Entity data sources** — precise payload mapping for each inspector entity type.
- **L13: Inspector scope** — Admin-only for v1. Jobs/Saved deferred until selection model exists.
- **L14: Tooltip UX anti-pattern** — click-to-reveal expandable card instead of hover tooltip for multi-line content.
- **L15: Poll interval** — 30s.
- **L16: Rollback plan** — remove widget + controller import, clear localStorage keys. No backend rollback needed.
- **L17: Admin nav bar section coverage** — four stable section IDs used; removed sections become graceful no-ops.
- **L18: Discovery pending count loading** — badge starts hidden, only appears when discovery controller sets a non-zero count.
- **L19: Fetcher log error live region** — expanded details use `aria-hidden` toggle inside the log row; live region only announces new rows.
- **L20: Copy all diagnostics partial failure** — failed endpoints are represented as `{"error": "..."}` placeholders; `_meta.partial` flag set when data is incomplete.

## Test plan

Use the narrowest checks that match the touched area:

- Frontend unit tests for Action Center prioritization, signal evaluation, dismiss TTL, inspector adapters, saved view persistence, recent-history bounds, action labeling.
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
- All 20 loopholes from the audit remain closed (verified before each slice ships).

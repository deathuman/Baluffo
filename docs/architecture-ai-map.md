# Baluffo Architecture AI Map

Scan-first architecture guide for AI-assisted coding. Use this to load minimal context while keeping edits inside the right subsystem boundaries.

## 1) System boundary map

```text
jobs.html / saved.html / admin.html
  -> frontend/{jobs|saved|admin}/index.js
  -> frontend/{jobs|saved|admin}/app.js
  -> frontend/{jobs|saved|admin}/app/runtime.js
      -> page modules (app/*.js + actions/services/state-sync/render/domain/data-source)
      -> shared helpers (frontend/shared/*, root utils)
      -> UI registry (frontend/shared/ui/selectors.js)

admin bridge (local HTTP API): src/admin_bridge.py
  -> src/bridge/ (modular components)
      -> sync_state.py: SYNC_STATUS, locks, state management
      -> sync_service.py: SyncService class for sync operations (wired into admin_bridge)
      -> registry_service.py: RegistryService for active/pending/rejected state
      -> discovery_service.py: DiscoveryService for discovery task + auto-sync watch
      -> pipeline_service.py: PipelineService for jobs pipeline task + status
      -> routes/: HTTP route handlers (GET/POST)
  -> remaining: server startup/wiring + some legacy compatibility wrappers
jobs feed + discovery/sync scripts: src/jobs_fetcher.py, src/source_discovery.py, src/source_sync.py

desktop launcher/runtime: src/ship/desktop_app.py
  -> spawns local site + bridge
  -> opens browser app window
  -> watches heartbeat/activity and shutdown flow

runtime data roots:
  - repo/runtime: data/
  - desktop package runtime: ship/data/
  - desktop local user data: ship/data/local-user-data/
```

## CLI Scripts

| Script | Location | Purpose |
|--------|----------|---------|
| `src/jobs_fetcher.py` | `src/` | Build unified jobs feed |
| `src/source_discovery.py` | `src/` | Discover candidate sources |
| `src/admin_bridge.py` | `src/` | Local admin HTTP server |
| `src/jobs/pipeline.py` | `src/jobs/` | Core job processing |
| `scripts/orchestrator.py` | `scripts/` | Build/verify orchestration |
| `scripts/build_ship_bundle.py` | `scripts/` | Create ship bundle |
| `scripts/build_portable_exe.py` | `scripts/` | Create portable EXE |
| `scripts/backup_e2e_validate.py` | `scripts/` | Validate backup flow |

For bridge API endpoints, see `docs/admin-bridge-api.md`.

## 2) Frontend topology (current)

### Jobs page
- Entry: `frontend/jobs/app.js` -> `frontend/jobs/app/runtime.js`
- Core app modules:
  - `app/feed.js`: startup/manual refresh flow and auto-refresh signal handling
  - `app/filters.js`: filter normalization, option rendering, quick-filter behavior
  - `app/cache.js`: IndexedDB cache and "seen" job keys
  - `app/pipeline.js`: bridge pipeline status/polling helpers
  - `app/startup.js`: URL state parse/build, startup scheduling
  - `app/dom.js`, `app/auth.js`, `app/pagination.js`: DOM refs/auth/pager helpers
- Runtime still composes legacy page modules where needed: `render.js`, `domain.js`, `data-source.js`, `services.js`, `state-sync/index.js`, `actions.js`.

### Saved page
- Entry: `frontend/saved/app.js` -> `frontend/saved/app/runtime.js`
- Core app modules:
  - `app/notes.js`: debounced note saves and edit-state guards
  - `app/attachments.js`: attachment validation/upload/preview rendering
  - `app/activity.js`: timeline scope, filtering, pulse, rendering
  - `app/view-state.js`: saved list filtering/sorting primitives
  - `app/dom.js`, `app/auth.js`, `app/custom-job.js`, `app/render-cycle.js`
- Runtime composes existing `render.js`, `domain.js`, `data-source.js`, `services.js`, `state-sync/index.js`, `actions.js`.

### Admin page
- Entry: `frontend/admin/app.js` -> `frontend/admin/app/runtime.js`
- Core app modules:
  - `app/auth.js`: admin unlock/lock flow
  - `app/fetcher.js`, `app/discovery.js`, `app/sync.js`, `app/registry.js`: feature controllers
  - `app/ops.js`: ops summary/controller helpers
  - `app/busy-state.js`: busy flags and UI lock states
  - `app/dom.js`, `app/sources.js`
- Runtime composes `render.js`, `domain.js`, `data-source.js`, `services.js`, `state-sync/index.js`, `actions.js`.

## Backend topology (current)

### Bridge module (`src/bridge/`)
Extracted from `admin_bridge.py` to reduce God Object complexity:

- **`sync_state.py`**: State management for sync operations
  - `SYNC_STATE_LOCK`, `SYNC_CONFIG_LOCK`: Threading locks
  - `ACTIVE_SYNC_RUNS`, `ACTIVE_SYNC_THREADS`: Task tracking
  - `SYNC_STATUS`, `SYNC_CONFIG`: State variables
  - `SyncState` class: Encapsulates state management with configurable paths
  - Backward-compatible module-level functions

- **`sync_service.py`**: Core sync business logic
  - `SyncService` class with dependency injection
  - Configuration methods: `load_saved_sync_settings()`, `update_saved_sync_settings()`, `test_sync_config()`
  - Status methods: `get_sync_status_payload()`
  - Sync operations: `sync_pull_sources()`, `sync_push_sources()`, `startup_sync_pull()`
  - Task management: `start_sync_task()`, `sync_task_running()`, `wait_for_sync_tasks()`

- **`registry_service.py`**: Registry business logic (active/pending/rejected)
  - `ensure_active_registry()`, `load_state()`, `persist_state()`, `normalize_state()`, `summarize_state()`
  - `move_entries()` helper

- **`discovery_service.py`**: Discovery task orchestration and optional sync auto-push watch
  - `trigger_discovery_task()`, `watch_discovery_run_for_auto_sync()`

- **`pipeline_service.py`**: Jobs pipeline orchestration and status payload
  - `start_task()` / `get_status_payload()`

- **`routes/`**: HTTP route handlers extracted from `admin_bridge.py`
  - `routes/get_routes.py`: GET routes
  - `routes/post_routes.py`: POST routes

### Remaining in `admin_bridge.py`
- HTTP server + request handler class, plus wiring to route handlers
- Service composition/wiring for `SyncService`, `RegistryService`, `DiscoveryService`, `PipelineService`
- Utility functions: `bridge_log()`, run history management, runtime config/paths, data normalization

## Data Model Overview

- **`data/jobs-unified.json`**: The main aggregated jobs feed. This is the primary data source for the Jobs UI.
- **`data/jobs-unified.csv`**: A CSV version of the aggregated jobs feed.
- **`data/jobs-fetch-report.json`**: A report on the last run of the jobs fetcher, including which sources were successful and which failed.
- **`data/source-registry-active.json`**: A list of the active job sources that the fetcher will use.
- **`data/source-registry-pending.json`**: A list of new job sources that have been discovered but not yet approved.
- **`data/source-registry-rejected.json`**: A list of job sources that have been rejected.
- **`data/source-discovery-report.json`**: A report on the last run of the source discovery process.
- **`data/source-discovery-candidates.json`**: A list of candidate job sources that have been discovered.
- **`data/local-user-data/profiles.json`**: A list of user profiles.
- **`data/local-user-data/session.json`**: The current user session.
- **`data/local-user-data/users/{uid}/saved-jobs.json`**: A list of jobs that a user has saved.
- **`data/local-user-data/users/{uid}/activity.json`**: A log of a user's activity.
- **`data/local-user-data/users/{uid}/attachments.json`**: A list of attachments that a user has uploaded.

## Data Flow

```
Sources (Google Sheets, Remote OK, Greenhouse, etc.)
    ↓
Adapters (static, provider_api, social) → fetch & parse
    ↓
Pipeline → aggregate, normalize
    ↓
Dedup → deduplicate by dedupKey
    ↓
Output → jobs-unified.json (primary), jobs-unified.csv, jobs-fetch-report.json
    ↓
Frontend (jobs.html) ← reads unified feed
```

Key stages:
- **Sources:** Defined in `source-registry-active.json`, fetched by adapter type
- **Adapters:** `src/jobs/adapters/` - fetch and parse each source type
- **Pipeline:** `src/jobs/pipeline.py` - core processing
- **Dedup:** `src/jobs/dedup.py` - collapse duplicates
- **Output:** `data/jobs-unified.*` - primary feed for Jobs UI

## 3) Task -> minimal files

| Task | Start here (minimal) | Then load only if needed |
|---|---|---|
| Jobs filter/search behavior | `frontend/jobs/app/filters.js` | `frontend/jobs/app/runtime.js`, `frontend/jobs/render.js` |
| Jobs feed refresh/startup | `frontend/jobs/app/feed.js` | `frontend/jobs/app/runtime.js`, `frontend/jobs/services.js` |
| Jobs auth/session UX | `frontend/jobs/app/auth.js` | `frontend/jobs/app/runtime.js`, `frontend/jobs/services.js` |
| Saved notes behavior | `frontend/saved/app/notes.js` | `frontend/saved/app/runtime.js`, `frontend/saved/services.js` |
| Saved attachments flow | `frontend/saved/app/attachments.js` | `frontend/saved/app/runtime.js`, `frontend/saved/services.js` |
| Saved timeline/activity | `frontend/saved/app/activity.js` | `frontend/saved/app/runtime.js` |
| Admin unlock/ops/fetch/discovery/sync | `frontend/admin/app/{auth,ops,fetcher,discovery,sync}.js` | `frontend/admin/app/runtime.js`, `frontend/admin/services.js` |
| Job processing pipeline | `src/jobs/pipeline.py` | `src/jobs/adapters`, `src/jobs/canonicalize.py`, `src/jobs/dedup.py` |
| Bridge API/runtime behavior | `src/admin_bridge.py` | `frontend/admin/services.js`, `frontend/jobs/services.js`, `frontend/saved/services.js` |
| Bridge sync state management | `src/bridge/sync_state.py` | `src/bridge/sync_service.py`, `src/admin_bridge.py` |
| Bridge sync operations | `src/bridge/sync_service.py` | `src/source_sync.py`, `src/admin_bridge.py` |
| Bridge registry operations | `src/bridge/registry_service.py` | `src/admin_bridge.py`, `src/source_registry.py` |
| Bridge discovery operations | `src/bridge/discovery_service.py` | `src/admin_bridge.py`, `src/source_discovery.py` |
| Bridge pipeline operations | `src/bridge/pipeline_service.py` | `src/admin_bridge.py` |
| Bridge HTTP routes | `src/bridge/routes/get_routes.py` | `src/bridge/routes/post_routes.py`, `src/admin_bridge.py` |
| UI Selection & Interaction | `frontend/shared/ui/selectors.js` | `frontend/*/app/dom.js`, `frontend/*/app/runtime.js` |
| Desktop startup/runtime behavior | `src/ship/desktop_app.py` | `tests/test_desktop_app.py`, `src/ship/runtime_launcher.py` |
| Add new filter to jobs page | `frontend/jobs/app/filters.js` | `frontend/jobs/render.js`, `frontend/jobs/app/runtime.js` |
| Add new field to custom job form | `frontend/saved/app/custom-job.js` | `frontend/saved/render.js`, `frontend/saved/app/runtime.js` |

## 4) Runtime contracts (safe-edit guardrails)

- Desktop single-instance is strict: if another healthy session exists, launcher raises `"Baluffo is already running..."` and must not open another browser window.
- Desktop startup contract:
  - start local site + bridge child processes
  - wait for page URL readiness (`jobs.html?...desktop=1...`)
  - wait for bridge health (`/ops/health`) before steady state when available
- Browser launch contract:
  - prefer Chromium app mode (`--app=` + dedicated profile)
  - keep fallback to default browser for primary startup if app-mode launch fails
  - recovery path after app-process exit can reopen default browser
- Session/watchdog contract:
  - session metadata stored in `desktop-session.json`
  - watchdog tracks browser heartbeat and bridge last activity
  - idle/heartbeat timeout closes session and tears down child processes

## 5) Fast verification matrix

| Change area | Fastest verification |
|---|---|
| Frontend module wiring/syntax | `node --check frontend/jobs/app.js frontend/saved/app.js frontend/admin/app.js` |
| Full Workspace Verification | `npm run verify` (Orchestrated Rebuild + All Tests) |
| Frontend behavior/unit coverage | `npm run test:unit` |
| Desktop launcher/runtime behavior | `python -m pytest tests/test_desktop_app.py` |
| Packaged desktop smoke contract | `npm run test:smoke` |
| Bridge behavior changes | `python -m pytest tests/admin/` |
| Bridge sync state/service | `python -m pytest tests/admin/test_admin_bridge_ops_sync.py` |

## 6) Related deep-dive docs

- Release and packaging process: `docs/RELEASE.md`

## Key Libraries and Frameworks

- **Playwright:** Used for frontend smoke regression tests.
- **PyInstaller:** Used to package the application as a portable Windows executable.
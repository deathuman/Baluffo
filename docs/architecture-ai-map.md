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
      -> API client (frontend/shared/api-client.js), state hub (frontend/shared/state-hub.js), shared UI components (frontend/shared/ui/)

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
  -> source discovery: `import src.source_discovery` loads the package (src/source_discovery/);
      CLI and full run_discovery still delegate to the legacy script (source_discovery.py) via orchestrator.
  -> Package modules: config, sheet_directory, web_search, provider_patterns, static_candidates,
      scoring, io_runtime, reporting, probe, orchestrator.

desktop launcher/runtime: src/ship/desktop_app/__init__.py
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
  - `app/countries.js`: country/region metadata and bound filter helpers
  - `app/cache.js`: IndexedDB cache and "seen" job keys
  - `app/pipeline.js`: bridge pipeline status/polling helpers
  - `app/sources.js`: source preset lists and thin data-source wrappers
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

### Frontend state and API (state-hub, api-client)

- **State hub** (`frontend/shared/state-hub.js`): Cross-module state; `get(key)`, `set(key, value)`, `subscribe(key, callback)`.
  - **Keys and set/read locations:** `jobsFeedCount` (set: jobs/app/feed.js after refresh), `jobsLastUpdated` (set: jobs/app/feed.js), `savedCount` (set: saved/app/runtime.js in subscribeToSavedJobs), `savedLastUpdated` (set: saved/app/runtime.js), `authStatus` (optional; set: admin auth modules). Add new shared state by defining a key and documenting set/read in state-hub header and here.
- **API client** (`frontend/shared/api-client.js`): Bridge HTTP; `normalizeBaseUrl`, `getBridgeErrorMessage`, `fetchBridge`, `fetchJson`, `fetchText`, `postJson`. Callers pass `baseUrl` from admin config. Bridge paths used by frontend: see `docs/admin-bridge-api.md`; jobs/saved/admin call fetch/pipeline, saved-jobs, ops, sync, discovery, etc. from their services or runtime.

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
  - Status methods: `get_sync_status_payload()`, `sync_config_status()`, `set_sync_status()`
  - Sync operations: `sync_pull_sources()`, `sync_push_sources()`, `startup_sync_pull()`
  - Task management: `start_sync_task()`, `sync_task_running()`, `wait_for_sync_tasks()`

- **`sync_task_flow.py`**: Shared sync-task worker flow
  - keeps pull/push run-history/status/logging assembly in one place
  - used by both `src.admin_bridge` compatibility wrappers and `SyncService`

- **`registry_service.py`**: Registry business logic (active/pending/rejected)
  - `ensure_active_registry()`, `load_state()`, `persist_state()`, `normalize_state()`, `summarize_state()`
  - `move_entries()` helper
  - source identity/url helpers now exposed there too, so `BridgeApi` can wire registry POST behavior from the typed service instead of entrypoint glue

- **`discovery_service.py`**: Discovery task orchestration and optional sync auto-push watch
  - `trigger_discovery_task()`, `watch_discovery_run_for_auto_sync()`

- **`pipeline_service.py`**: Jobs pipeline orchestration and status payload
  - `start_task()` / `get_status_payload()`

- **`source_checker.py`**: Static source validation (check_static_source and helpers); used by admin bridge for source testing/validation.

- **`task_history.py`**: Run history and task-state persistence; `TaskHistoryManager` (load/save/append/upsert run_history, prune_started_rows_for_type, clear_task_state). Admin bridge wires it via `_get_task_history_manager()` and resets it in `configure_runtime_paths`.

- **`html_extractor.py`**: Job-link extraction from HTML/scripts (_extract_embedded_job_urls, script URL extraction); used by admin bridge and static adapter.

- **`routes/`**: HTTP route handlers extracted from `admin_bridge.py`
  - `routes/get_routes.py`: GET routes
  - `routes/post_routes.py`: POST routes

- **`request_utils.py`**: Request/response helpers (`read_json_from_request`); used by admin_bridge handler for POST body.
- **`api.py`**: `BridgeApi` composition object; now defaults registry identity/url helpers to `src.source_registry` so POST registry routes do not depend on entrypoint stubs for those basics.
- **`source_check_api.py`**: Source-check orchestration (`normalize_manual_static_studio_fields`, `trigger_source_check`); `admin_bridge.py` keeps thin wrappers for route/test stability.
- **`task_launch_api.py`**: Fetch/discovery task-launch orchestration (`run_background_script`, `build_fetcher_args_from_payload`); `admin_bridge.py` delegates immediately.
- **`ops_api.py`**: Ops/report orchestration (`failed_source_names_from_latest_report`, `sync_history_from_reports`, `compute_ops_health`, `compute_fetcher_metrics`); keeps report/history/ops assembly out of `admin_bridge.py`.

- **`source_helpers.py`**: Source/URL helpers (`infer_studio_name_from_host`, `find_existing_source_by_url`, `find_existing_static_source_by_studio_domain`); used by add_manual_source and registry flows.

- **`source_check_http.py`**: HTTP fetch and URL helpers for source checking (`try_fetch_with_playwright`, `normalize_error_code`, `suggest_alternate_career_urls`, `discover_redirect_career_candidates`, `looks_like_browser_challenge_page`, `build_check_failure_details`); used by admin_bridge when building callables for `source_checker.check_static_source`.

### Remaining in `admin_bridge.py`
- HTTP server + request handler class, plus wiring to route handlers
- Service composition/wiring for `SyncService`, `RegistryService`, `DiscoveryService`, `PipelineService`, source checker, task history
- Registry auto-sync wrappers only; the persist/start logic now lives in `src/bridge/registry_sync_flow.py`
- Sync task wrappers only; the shared worker logic now lives in `src/bridge/sync_task_flow.py`
- Source-check wrappers only; orchestration now lives in `src/bridge/source_check_api.py`
- Fetch/discovery task-launch wrappers only; orchestration now lives in `src/bridge/task_launch_api.py`
- Ops/report wrappers only; orchestration now lives in `src/bridge/ops_api.py`
- Bridge API sync-status wiring now comes from `SyncService` directly instead of `admin_bridge.py` lambda/wrapper glue.
- Utility functions: `bridge_log()`, runtime config/paths, data normalization (run history lives in `bridge/task_history.py`)
- Request/response: delegated to `bridge/request_utils.py`. Source/URL helpers: delegated to `bridge/source_helpers.py`. Source-check fetch/error/URL helpers: delegated to `bridge/source_check_http.py`.

### Admin bridge surface (where to edit what)

Responsibilities still in `admin_bridge.py` and where they are used:

| Responsibility | Functions / area | Used by / callers |
|----------------|------------------|-------------------|
| **Request/response** | `read_json_from_request(handler)` | Request handler `do_POST` (single place that reads POST body) |
| **Config / runtime** | `RuntimeConfig`, `configure_runtime_paths`, `resolve_runtime_config`, `bridge_log`, `_normalize_log_level`, `_normalize_log_format`, `startup_banner` | Server startup, logging, path setup |
| **Sync config & metrics** | `load_saved_sync_settings`, `append_startup_metric`, `read_startup_metrics`, `resolve_effective_sync_config`, `refresh_sync_config`, `get_saved_sync_config_payload`, `update_saved_sync_settings`, `load_sync_runtime_state`, `save_sync_runtime_state`, `test_sync_config` | GET/POST sync routes, desktop startup metrics |
| **Registry (wrappers)** | `ensure_active_registry`, `normalize_state`, `load_state`, `summarize_state`, `persist_state`, `persist_state_and_auto_sync`, `move_entries` | `bridge/routes/get_routes.py` (state, registry), `post_routes.py` (move, persist) |
| **Source/registry helpers** | Implementation in `bridge/source_helpers.py`. admin_bridge imports and uses for POST add manual source and registry flows. | POST add manual source, registry flows |
| **Source-check (fetch/HTML)** | Implementation in `bridge/source_checker.py` and `bridge/source_check_http.py`. admin_bridge still provides `_fetch_html_with_fallback`, `_html_has_extractable_job_data`, `_fetch_static_page_with_alternates` (wrappers that call bridge and discovery), and `check_static_source` / `trigger_source_check` that delegate to bridge. | `post_routes.py` (`trigger_source_check`); `check_static_source` |
| **Run history / task state** | Implementation in `bridge/task_history.py` (TaskHistoryManager) and `bridge/run_history_api.py` (load/save/append/upsert/prune/clear, task_running_from_state, report_is_stale_in_progress). admin_bridge keeps `_get_task_history_manager()` and thin wrappers that delegate to run_history_api. | Routes (ops, run history), handler (task state), discovery/sync finish paths |
| **Alerts / schedule** | Implementation in `bridge/ops_health.py`, with `src/bridge/ops_api.py` owning the dependency assembly and delegating alert/schedule helpers through stable `admin_bridge.py` wrappers. | Ops health, fetcher metrics |
| **Ops / health / reports** | Report normalization in `bridge/report_normalizer.py`; orchestration for failed-source filtering, history reconciliation, ops health, and fetcher metrics now lives in `bridge/ops_api.py`, with `admin_bridge.py` exposing thin compatibility wrappers. | GET `/ops/health`, `/ops/summary`, report normalization for routes |
| **Sync status (wrappers)** | `_set_sync_status`, `get_sync_status_payload`, `sync_pull_sources`, `sync_push_sources`, `startup_sync_pull`, `sync_task_running`, `wait_for_sync_tasks`, `_sync_guard`, `_mark_discovery_sync_finished` | POST sync routes, startup, discovery completion |
| **Fetcher / discovery run** | `build_fetcher_args_from_payload`, `run_background_script` | POST trigger fetch, trigger discovery |
| **Desktop / session** | `mark_desktop_session_activity`, `parse_iso` | Handler (activity), run history parsing |
| **Route dispatch** | Request handler class `do_GET` / `do_POST` calling `get_routes.handle_get`, `post_routes.handle_post` | Incoming HTTP |

Further shrinkage: extract to bridge modules with injected deps; keep `api.xxx` callable in admin_bridge if routes need it (thin wrappers that delegate).

### Refactoring status (current)
Backend refactoring is directionally strong and materially narrower than the early audit baseline, but not fully settled. **Done:** shared utils/regex/exceptions (`src/shared/`), bridge extractions (source_checker, task_history, html_extractor, runtime_state, run_history_api), jobs extractions (social/provider parsers, normalizers, text_utils, game_detection), pipeline helper extraction (`src/jobs/pipeline_bootstrap.py`, `src/jobs/pipeline_loader_selection.py`, `src/jobs/pipeline_runtime.py`), static adapter helper extraction (`src/jobs/adapters/static_helpers.py`), dead-code removal, coerce centralization; adapter validation uses `AdapterValidationError` from `src/exceptions.py`; **API client** (`frontend/shared/api-client.js`) for backend calls; **state hub** (`frontend/shared/state-hub.js`) for cross-module state (jobsFeedCount, jobsLastUpdated, savedCount, savedLastUpdated—new shared state can be added via new keys and set/read locations documented in that file); **shared UI components** in `frontend/shared/ui/`; **static plugin family** in `src/jobs/adapters/plugins/static/` (plugins selected by host/source_identity; core validation and adapter flow still enters through `src/jobs/adapters/static.py`, with dense internal behavior delegated to `static_helpers.py`). **How to add a static plugin:** add a module under `src/jobs/adapters/plugins/static/` with `can_handle(ctx)` and `run(..., pages, source_row, parse_jobpostings_from_html=..., **kwargs)` returning `Sequence[RawJob]`, then register it in `register.py` (see docstring there). New static sites are added by implementing such plugins; new shared frontend state by adding keys and wiring in state-hub. **Pydantic (Phase 2):** Core contracts are validated with Pydantic at pipeline output (`src/core/contracts.py` before writing `jobs-unified.json`) and at bridge boundaries (e.g. saved-jobs/save in `bridge/routes/post_routes.py`). Schemas: `src/core/schemas.py` (CanonicalJobSchema, SavedJobSchema, ManifestSchema). See `docs/DATA_CONTRACT.md` and `src/core/` for the runtime source of truth; new fields require schema + doc update.

### Transitional boundaries (allowed for now)

- `src.admin_bridge` remains the composition root and CLI entrypoint, but mutable server-adjacent state should live in `src/bridge/server/runtime_state.py` or `src/bridge/sync_state.py`.
- `src.admin_bridge` should keep sync and registry behavior as thin wrappers over `src/bridge/*` modules; do not add new business-logic helper clusters there.
- `src/jobs/adapters/_runtime.py` keeps `_runtime.facade()` as a legacy compatibility boundary; do not spread it to new jobs modules.
- `src/jobs/common/__init__.py` is now a curated compatibility surface for the `src.jobs_fetcher` facade; new package-internal code should prefer direct `src/jobs/common/*` submodule imports.
- `frontend/local-data/services.js` may keep `window.JobAppLocalData` as the compatibility boundary until the local-data runtime is fully formalized.

**Deliberately not done:** `jobs/validation.py` and `bridge/contracts.py` (intent satisfied by normalizers/text_utils/state).

**Follow-up / optional work**
- **Scrapy phase 2 (pipelines):** Moving validation and dedupe into a Scrapy pipeline is a larger refactor (spider would yield Items; pipeline would write into the runner’s container or a shared store). Deferred; when needed, treat it as a separate plan so contract and tests stay clear.
- **Generic static plugin (P3.3):** If the inline fallback in `src/jobs/adapters/static.py` grows again, consider turning it into a low-priority "generic_static" plugin so static.py is mostly dispatch + scrapy.
- **Jobs common compatibility surface:** `src/jobs/common/__init__.py` is still broad and re-export heavy; prefer reducing that surface before adding more helper modules elsewhere.
- **State→DOM helper (P4.4):** Optional; apply when 2–3+ "subscribe to state key then set element" patterns appear. Not needed yet (status is fetch/event then setStatusText, not state-key subscription).
- **html_parsers extraction:** Done. Implementation lives in `jobs/adapters/html_parsers.py`; re-exported via `jobs/parsers` and `jobs/common` for backward compatibility.
- **Further admin_bridge shrinkage:** Latest follow-up completed the source-check, task-launch, and ops/report extractions. Remaining work should stay focused on narrow wiring seams, not broad file movement.
- **Pydantic integration:** GET saved-jobs validates each row with SavedJobSchema (lenient: invalid rows skipped, logged). Other endpoints use Pydantic only where contract shape clearly matters; response models deferred. CanonicalJob and frontend contract unchanged.

**Phase 3 (static adapter):** Scrapy path lives in `static_scrapy.py`; orchestration and plugin dispatch in `static.py`; site plugins in `plugins/static/` (see subsection below).

**Phase 4 (frontend):** Saved app backup logic in `saved/app/backup.js`; slice runtime `view.js` helpers implement `setStatusText` locally to keep runtime slice-local; state-hub and api-client documented below.

### Static adapter and plugins (how to add a static plugin)

- **Orchestration:** `src/jobs/adapters/static.py` — `run_static_studio_pages_source` (and shards), `run_scrapy_static_source` (from `static_scrapy.py`). For each source, host is derived from the first page URL; plugin is selected by `AdapterPluginContext(family="static", source_identity=host)`. Optional Playwright fallback for listing-page fetch only (see **Scraping pipeline** below).
- **Internal helpers:** `src/jobs/adapters/static_helpers.py` owns report bootstrap, runtime config, cached fetch/time-budget behavior, detail-link heuristics, and detail-page processing. Keep new static internals there before growing `static.py` again.
- **Scrapy path:** `src/jobs/adapters/static_scrapy.py` — subprocess runner, envelope parsing, job normalization; used when source type is scrapy_static. Sources come from the browser fallback queue and run with `use_browser=True` (Scrapy-Playwright when installed).
- **Plugins:** `src/jobs/adapters/plugins/static/` — one module per site (e.g. `example_com.py`, `example_org.py`, `littlechicken.py`; `larian.py` exists but is unregistered so larian.com uses the fallback and its heuristics). Each provides `can_handle(ctx)` and `run(..., pages, source_row, parse_jobpostings_from_html=..., **kwargs)` returning `Sequence[RawJob]`. Registered in `register.py`.
- **To add a static plugin:** (1) Add a module under `src/jobs/adapters/plugins/static/` with `can_handle(ctx)` (e.g. `ctx.source_identity == "example.org"`) and `run(...)` that fetches/parses and returns `RawJob` dicts. (2) Register it in `register.py` with `default_registry.register(SimpleAdapterPlugin(name=..., family="static", priority=90, can_handle_fn=..., run_fn=...))`. (3) See `register.py` docstring and this map for the full contract.

**Scraping pipeline and Playwright:** Flow (discovery → pipeline → static/scrapy_static → browser queue → Scrapy-Playwright), where Playwright is used (source check, discovery probe fallback, static listing fallback, Scrapy-Playwright), and how to compare job counts before/after: **docs/scraping-pipeline.md**.

## Data Model Overview

- **`data/jobs-unified.json`**: The main aggregated jobs feed. This is the primary data source for the Jobs UI.
- **`data/jobs-unified.csv`**: A CSV version of the aggregated jobs feed.
- **`data/jobs-fetch-report.json`**: A report on the last run of the jobs fetcher, including which sources were successful and which failed.
- **`data/source-registry-active.json`**: A list of the active job sources that the fetcher will use.
- **`data/source-registry-pending.json`**: A list of new job sources that have been discovered but not yet approved.
- **`data/source-registry-rejected.json`**: A list of job sources that have been rejected.
- **`data/source-discovery-report.json`**: A report on the last run of the source discovery process.
- **`data/source-discovery-candidates.json`**: A list of candidate job sources that have been discovered. See **docs/DATA_CONTRACT.md** §7 for the source discovery contract (stable APIs and report/candidates shape).
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
- **Pipeline helpers:** `src/jobs/pipeline_bootstrap.py`, `src/jobs/pipeline_loader_selection.py`, `src/jobs/pipeline_runtime.py` - package-private orchestration support
- **Jobs compatibility layer:** `src/jobs/common/__init__.py` - curated compatibility surface; prefer `src/jobs/common/config.py`, `fetch.py`, `http.py`, `social.py`, `sources.py`, `url.py` for package-internal imports
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
| Bridge discovery operations | `src/bridge/discovery_service.py` | `src/admin_bridge.py`, `src/source_discovery/` (package), `src/source_discovery.py` (CLI/legacy) |
| Bridge pipeline operations | `src/bridge/pipeline_service.py` | `src/admin_bridge.py` |
| Bridge HTTP routes | `src/bridge/routes/get_routes.py` | `src/bridge/routes/post_routes.py`, `src/admin_bridge.py` |
| UI Selection & Interaction | `frontend/shared/ui/selectors.js` | `frontend/*/app/dom.js`, `frontend/*/app/runtime.js` |
| Desktop startup/runtime behavior | `src/ship/desktop_app/__init__.py` | `tests/test_desktop_app.py`, `src/ship/runtime_launcher.py` |
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
| Desktop launcher/runtime behavior | `python -m pytest tests/test_desktop_app.py -q` |
| Packaged desktop smoke contract | `npm run test:smoke` |
| Bridge behavior changes | `python -m pytest tests/admin/ -q` |
| Bridge sync state/service | `python -m pytest tests/admin/test_admin_bridge_ops_sync.py -q` |
| Jobs pipeline / jobs_fetcher | `python -m pytest tests/test_jobs_fetcher.py -q` |
| Source discovery | `python -m pytest tests/test_source_discovery.py -q` |

For more targeted runs and fixture list, see `docs/testing.md`.

## 6) Related deep-dive docs

- Release and packaging process: `docs/RELEASE.md`

## Key Libraries and Frameworks

- **Playwright:** Used for frontend smoke regression tests.
- **PyInstaller:** Used to package the application as a portable Windows executable.

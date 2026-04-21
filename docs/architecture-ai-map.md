# Baluffo Architecture AI Map

> **Use this when:** locating the correct subsystem, choosing edit boundaries, mapping task-to-files
> **Canonical for:** system boundaries, task routing, verification matrix
> **Not canonical for:** endpoint payloads, data schema details
> **Then inspect:** the minimal source files listed in the task table, plus the matching contract doc if shape changes are involved

---

## 1) System boundary map

```text
jobs.html / saved.html / admin.html
  -> styles/base.css + styles/components.css + styles/<page>.css
  -> frontend/{jobs|saved|admin}/index.js
  -> frontend/{jobs|saved|admin}/app.js
  -> frontend/{jobs|saved|admin}/app/runtime.js
      -> page modules (app/*.js + actions/services/render/domain/data-source)
      -> shared (frontend/shared/*, selectors.js, api-client.js, state-hub.js)

src/dev_admin_supervisor.py (Baluffo launcher)
  -> starts site + bridge + owned browser session
  -> tears down on session exit

src/admin_bridge.py (stable thin entrypoint / wiring-only composition root)
  -> src/bridge/ (services: sync, registry, discovery, pipeline, routes)
  -> src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py

src/jobs_fetcher.py (stable thin CLI facade)
  -> src/jobs/ (pipeline, adapters, dedup)
src/jobs/adapters/static.py (stable static adapter surface)
  -> src/jobs/adapters/static_{runtime,listing,detail,sources}.py
  -> src/jobs/adapters/static_helpers.py + plugins/static/*
src/source_discovery.py (stable thin CLI entrypoint)
  -> src/source_discovery/ (package)

src/ship/desktop_app/ (desktop runtime package)
  -> launcher.py orchestrates site + bridge + browser startup
  -> startup.py owns readiness, handoff, heartbeat, and watchdog flow
  -> browser.py / session.py / _windows.py / config.py own focused helpers

src/packaged_desktop_smoke.py (stable packaged smoke entrypoint)
  -> src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py

src/ship/desktop_update.py (stable updater surface)
  -> src/ship/desktop_update_{shared,state,service}.py
```

---

## 2) CLI Scripts

| Script | Purpose |
|--------|---------|
| `src/jobs_fetcher.py` | Build unified jobs feed |
| `src/source_discovery.py` | Discover candidate sources (delegates to package) |
| `src/dev_admin_supervisor.py` | Baluffo launcher (site + bridge + browser) |
| `src/admin_bridge.py` | Bridge-only entry (expert/manual mode, wiring only) |
| `src/jobs/pipeline.py` | Pipeline entry flow |
| `src/source_discovery/` | Discovery package modules |
| `src/packaged_desktop_smoke.py` | Packaged smoke CLI and rehearsal entry flow |
| `scripts/build_ship_bundle.py` | Create ship bundle |
| `scripts/build_portable_exe.py` | Create portable EXE |

---

## 3) Task -> minimal files

| Task | Start here | Then only if needed |
|------|------------|---------------------|
| Jobs filter/search | `frontend/jobs/app/filters.js` | `frontend/jobs/app/runtime.js` |
| Jobs feed refresh | `frontend/jobs/app/feed.js` | `frontend/jobs/services.js` |
| Saved notes | `frontend/saved/app/notes.js` | `frontend/saved/services.js` |
| Saved attachments | `frontend/saved/app/attachments.js` | `frontend/saved/services.js` |
| Admin ops | `frontend/admin/app/{auth,ops,fetcher,discovery,sync}.js` | `frontend/admin/services.js` |
| Bridge API | `src/bridge/*.py` | `src/bridge/routes/*.py` |
| Admin bridge entrypoint/runtime wiring | `src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py` | `src/admin_bridge.py` only for root-surface compatibility work |
| Discovery behavior | `src/source_discovery/orchestrator.py`, `orchestrator_{runtime,generation,probe,finalize}.py`, `runtime_metrics.py`, `stage_control.py`, `reporting.py` | `src/source_discovery.py` only for CLI compatibility |
| Bridge sync | `src/bridge/sync_service.py` | `src/source_sync.py`, `src/source_sync_config.py`, `src/source_sync_snapshot.py`, `src/source_sync_crypto.py`, `src/bridge/sync_state.py` |
| Bridge registry | `src/bridge/registry_service.py` | `src/source_registry.py`, `src/bridge/registry_tombstones.py` |
| Jobs pipeline / fetcher behavior | `src/jobs/pipeline.py`, `src/jobs/pipeline_timing.py`, `src/jobs/pipeline_finalize.py`, other `src/jobs/*` leaf modules | `src/jobs_fetcher.py` only for CLI or compatibility-surface changes |
| Static adapter behavior | `src/jobs/adapters/static_{runtime,listing,detail,sources}.py`, `src/jobs/adapters/static_helpers.py` | `src/jobs/adapters/static.py` only for root-surface compatibility work |
| Local-data page wiring | `frontend/<page>/services.js` | `frontend/local-data/services.js` only when the shared local-data API changes |
| Desktop runtime | `src/ship/desktop_app/launcher.py` | `src/ship/desktop_app/{startup,browser,session,_windows,config}.py`, `src/ship/runtime_launcher.py` |
| Packaged smoke / updater | `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py`, `src/ship/desktop_update_{shared,state,service}.py` | `src/packaged_desktop_smoke.py` and `src/ship/desktop_update.py` only for CLI/public-surface compatibility work |
| UI selectors | `frontend/shared/ui/selectors.js` | - |

---

## 4) Frontend topology

**Jobs page:** `frontend/jobs/app.js` -> `runtime.js` -> `app/filters.js`, `app/feed.js`, `app/cache.js`, `app/pipeline.js`, `app/sources.js`

**Saved page:** `frontend/saved/app.js` -> `runtime.js` -> `runtime/state.js`, `runtime/events.js`, `app/notes.js`, `app/attachments.js`, `app/activity.js`, `app/view-state.js`

**Admin page:** `frontend/admin/app.js` -> `runtime.js` -> `app/auth.js`, `app/fetcher.js`, `app/discovery.js`, `app/sync.js`, `app/registry.js`, `app/ops.js`

**Shared:** `frontend/shared/state-hub.js` (cross-module state), `frontend/shared/api-client.js` (bridge HTTP), `frontend/shared/config/admin-config.js` (frontend-safe runtime config), `frontend/shared/local-data/` (desktop/browser local-data clients)

**Styles:** `styles/base.css` owns tokens and page foundations, `styles/components.css` owns shared UI primitives, and `styles/{jobs,saved,admin}.css` own page-specific polish. Change shared styling in the shared layer first; only touch page CSS when the selector is clearly page-owned.

---

## 5) Backend topology

**Bridge services (`src/bridge/`):**
- `sync_service.py`, `sync_state.py` - sync operations
- `registry_service.py` - canonical active/pending/rejected state, tombstone filtering, local persistence
- `registry_tombstones.py` - local delete ledger and restore helpers
- `discovery_service.py` - discovery task orchestration
- `pipeline_service.py` - jobs pipeline task
- `routes/get_routes.py`, `routes/post_routes.py` - HTTP handlers
- `ops_api.py`, `task_history.py`, `source_check_api.py` - ops/report/orchestration

**Still in `admin_bridge.py`:** bridge startup entrypoint, `build_bridge_api(...)`, stable compatibility wrappers, and root monkeypatch seams

**Admin bridge entrypoint helpers:**
- `admin_entrypoint_runtime.py` - bridge log emission, runtime-path rebinding, startup metrics, owner-session lifecycle helpers
- `admin_entrypoint_services.py` - cached sync/registry/discovery/pipeline/updater/task-launch/ops builders
- `admin_registry_api.py` - manual-source add/update flow, source-check glue, registry-state persistence helpers
- `admin_task_runtime.py` - sync/task runtime helpers, report waits, fetch-task launch/runtime glue

**Jobs package (`src/jobs/`):**
- `pipeline.py` - pipeline entry flow
- `pipeline_timing.py`, `pipeline_finalize.py` - timing aggregation and late-stage output/report assembly
- `adapters/` - static, provider_api, social fetchers
- `canonicalize.py`, `dedup.py` - normalization
- `common/` - leaf helpers (`config`, `contracts`, `heuristics`, `parsing`, etc.); `common/__init__.py` is compatibility-only

**Discovery package (`src/source_discovery/`):**
- `orchestrator.py` - public run flow and test patch surface over `orchestrator_{runtime,generation,probe,finalize}.py`
- `runtime_metrics.py`, `stage_control.py`, `reporting.py` - runtime bookkeeping, stage toggles, report helpers
- `gamesmap.py`, `gamedevmap.py`, `gameprog.py`, `sheet_directory.py`, `web_search.py` - domain generators

**Sync helpers:**
- `source_sync.py` - compatibility and test patch surface
- `source_sync_config.py`, `source_sync_snapshot.py`, `source_sync_crypto.py` - config resolution, snapshot I/O, and crypto/JWT helpers
- `adapters/static.py` - root static adapter surface over `static_runtime.py`, `static_listing.py`, `static_detail.py`, `static_sources.py`, and `static_helpers.py`

---

## 6) Data model

| File | Purpose |
|------|---------|
| `data/jobs-unified.json` | Primary aggregated feed |
| `data/jobs-unified.csv` | CSV fallback |
| `data/jobs-fetch-report.json` | Last fetch diagnostics |
| `data/source-registry-active.json` | Approved sources |
| `data/source-registry-pending.json` | Discovered, not approved |
| `data/source-registry-rejected.json` | Rejected sources, local-only |
| `data/source-registry-tombstones.json` | Local-only delete ledger keyed by source identity |
| `data/source-sync.json` | Remote sync snapshot v2 (`active` and `pending` only) |
| `data/source-discovery-report.json` | Last discovery run |
| `data/local-user-data/profiles.json` | Desktop-local profile registry |
| `data/local-user-data/session.json` | Desktop-local current session |
| `data/local-user-data/users/{uid}/*` | Desktop-local per-user saved jobs, notes, attachments |

---

## 7) Runtime contracts

- **Desktop single-instance:** If healthy session exists, raise error - do not open another window
- **Desktop startup:** start site + bridge -> wait for page URL readiness -> wait for `/ops/health` before steady state
- **Session/watchdog:** store metadata in `desktop-session.json`, track browser heartbeat, close on idle timeout

---

## 8) Fast verification matrix

| Change area | Fastest verification |
|-------------|----------------------|
| Frontend syntax | `node --check frontend/jobs/app.js` |
| Frontend unit | `npm run test:unit` |
| Bridge behavior | `python -m pytest tests/admin/ -q` |
| Pipeline/fetcher | `python -m pytest tests/test_jobs_fetcher_*.py -q` |
| Desktop launcher | `python -m pytest tests/desktop_app/ -q` |
| Packaged desktop smoke | `python -m pytest tests/packaged_desktop/ -q` |
| Full verification | `npm run verify` |

See [`testing.md`](testing.md) for more commands.

---

## 9) Thin boundaries (don't move blindly)

- `src/packaged_desktop_smoke.py` - stable packaged smoke entrypoint and test patch surface; keep implementation in `src/ship/packaged_smoke/*.py`
- `src/ship/desktop_update.py` - stable updater surface; keep implementation in `src/ship/desktop_update_{shared,state,service}.py`
- `src/admin_bridge.py` - stable thin entrypoint; add new bridge logic to `src/bridge/*.py` or `src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py`
- `src/source_discovery.py` - stable thin CLI entrypoint; add discovery logic to `src/source_discovery/*.py`
- `src/jobs_fetcher.py` - stable thin CLI facade; add pipeline logic to `src/jobs/*`
- `src/jobs/adapters/static.py` - stable static adapter surface; keep generic listing/detail/runtime logic in `src/jobs/adapters/static_{runtime,listing,detail,sources}.py`
- `src/source_sync.py` - permanent thin sync integration surface; keep new sync logic in `src/source_sync_*` helpers
- `src/jobs/common/__init__.py` - package marker only; prefer `src.jobs.common.<leaf>` or package-submodule imports
- `frontend/local-data/services.js` - transitional local-data boundary; page code should go through slice-local `services.js`

---

## 10) Key libraries

- **Playwright** - frontend smoke tests
- **PyInstaller** - portable Windows executable
- **Scrapy + Playwright** - scraping fallback

---

*Last updated: 2026-04-21*

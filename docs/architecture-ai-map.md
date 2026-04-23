# Baluffo Architecture AI Map

> **Use this when:** locating the correct subsystem, choosing edit boundaries, mapping task-to-files
> **Canonical for:** system boundaries, task routing, verification matrix
> **Not canonical for:** endpoint payloads, data schema details
> **Then inspect:** the minimal source files listed in the task table, plus the matching contract doc if shape changes are involved
>
> Start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) first. Boundary-charter docs are supporting historical/planning records, not part of the default AI read path.

---

## 1) System boundary map

```text
jobs.html / saved.html / admin.html
  -> styles/base.css + styles/components.css + styles/<page>.css
  -> frontend/{jobs|saved|admin}/index.js
  -> frontend/{jobs|saved|admin}/app.js
  -> frontend/{jobs|saved|admin}/app/runtime.js
      -> page modules (app/*.js + actions/services/render/domain/data-source)
      -> runtime leaves (frontend/{jobs|saved|admin}/app/runtime/*.js + frontend/saved/app/admin-bridge-state.js)
      -> shared (frontend/shared/*, selectors.js, api-client.js, state-hub.js)

src/dev_admin_supervisor.py (Baluffo launcher)
  -> starts site + bridge + owned browser session
  -> tears down on session exit

src/admin_bridge.py (stable thin entrypoint / wiring-only composition root)
  -> src/bridge/ (services: sync, registry, discovery, pipeline, routes)
  -> src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py

src/jobs_fetcher.py (stable thin CLI facade)
  -> src/jobs/ (pipeline, adapters, dedup)
  -> src/jobs/fetcher_compat_{exports,runtime}.py
src/jobs/adapters/static.py (stable static adapter surface)
  -> src/jobs/adapters/static_{runtime,listing,listing_flow,detail,sources}.py
  -> src/jobs/adapters/static_{runtime_support,detail_heuristics}.py
  -> src/jobs/adapters/static_helpers.py (compat shim) + plugins/static/*
src/source_discovery.py (stable thin CLI entrypoint)
  -> src/source_discovery/ (package)

src/ship/desktop_app/ (desktop runtime package)
  -> launcher.py orchestrates site + bridge + browser startup
  -> startup.py owns readiness, handoff, heartbeat, and watchdog flow
  -> browser.py / session.py / _windows.py / config.py own focused helpers

src/packaged_desktop_smoke.py (stable packaged smoke entrypoint / monkeypatch surface)
  -> src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals}.py
  -> src/ship/packaged_smoke/rehearsal_{sync,update,browser}.py

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
| Jobs filter/search | `frontend/jobs/app/filters.js`, `frontend/jobs/app/runtime/query.js` | `frontend/jobs/app/runtime.js` only for page-entry wiring/export changes |
| Jobs page runtime wiring | `frontend/jobs/app/feed.js`, `frontend/jobs/app/runtime/{composition,boot,page-flow,events,feed-controller,list-view,pipeline-controller,startup-preview,auth-controller}.js` | `frontend/jobs/app/runtime.js` only when the stable page-entry root must change |
| Jobs feed refresh | `frontend/jobs/app/feed.js` | `frontend/jobs/services.js` |
| Saved page runtime wiring | `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes,activity-controller,attachments-controller,custom-job-controller,render-controller,auth-controller}.js`, `frontend/saved/app/admin-bridge-state.js` | `frontend/saved/app/runtime.js` only when the stable page-entry/export root must change |
| Saved notes | `frontend/saved/app/notes.js` | `frontend/saved/services.js` |
| Saved attachments | `frontend/saved/app/attachments.js` | `frontend/saved/services.js` |
| Admin runtime wiring | `frontend/admin/app/runtime/{composition,overview,events,state,view,effects,actions}.js` | `frontend/admin/app/runtime.js` only for page-entry wiring/export changes |
| Admin registry | `frontend/admin/app/registry/{ui,load,mutations}.js` | `frontend/admin/app/registry.js` only for stable controller/export changes |
| Admin ops | `frontend/admin/app/ops/{format,task-state,health,bridge-status}.js`, `frontend/admin/app/{auth,fetcher,discovery,sync}.js` | `frontend/admin/app/ops.js` only for stable controller/export changes |
| Bridge API | `src/bridge/*.py` | `src/bridge/routes/*.py` |
| Admin bridge entrypoint/runtime wiring | `src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py` | `src/admin_bridge.py` only for root-surface compatibility work |
| Discovery behavior | `src/source_discovery/orchestrator.py`, `orchestrator_{runtime,generation,probe,finalize}.py`, `runtime_metrics.py`, `stage_control.py`, `reporting.py` | `src/source_discovery.py` only for CLI compatibility |
| Bridge sync | `src/bridge/sync_service.py`, `src/source_sync_{config,runtime,snapshot,crypto}.py` | `src/source_sync.py` only for root-surface compatibility work, plus `src/bridge/sync_state.py` |
| Bridge registry | `src/bridge/registry_service.py` | `src/source_registry.py`, `src/bridge/registry_tombstones.py` |
| Jobs pipeline / fetcher behavior | `src/jobs/pipeline.py`, `src/jobs/pipeline_timing.py`, `src/jobs/pipeline_finalize.py`, `src/jobs/fetcher_compat_{exports,runtime}.py`, other `src/jobs/*` leaf modules | `src/jobs_fetcher.py` only for CLI or compatibility-surface changes |
| Static adapter behavior | `src/jobs/adapters/static_{runtime,listing,detail,sources}.py`, `src/jobs/adapters/static_{runtime_support,detail_heuristics}.py` | `src/jobs/adapters/static_helpers.py` only for shim/patch-surface compatibility and `src/jobs/adapters/static.py` only for root-surface compatibility work |
| Local-data page wiring | `frontend/<page>/services.js` | `frontend/local-data/services.js` only when the shared local-data API changes |
| Desktop runtime | `src/ship/desktop_app/launcher.py` | `src/ship/desktop_app/{startup,browser,session,_windows,config}.py`, `src/ship/runtime_launcher.py` |
| Packaged smoke / updater | `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`, `src/ship/desktop_update_{shared,state,service}.py` | `src/packaged_desktop_smoke.py` and `src/ship/desktop_update.py` only for CLI/public-surface compatibility work |
| UI selectors | `frontend/shared/ui/selectors.js` | - |

---

## 4) Frontend topology

**Jobs page:** `frontend/jobs/app.js` -> `runtime.js` -> `app/feed.js`, `app/filters.js`, `app/cache.js`, `app/pipeline.js`, `app/sources.js`, `runtime/{events,feed-controller,list-view,pipeline-controller,query,startup-preview,auth-controller}.js`

**Saved page:** `frontend/saved/app.js` -> `runtime.js` -> `runtime/{state,events,auth-controller,activity-controller,attachments-controller,custom-job-controller,render-controller}.js`, `app/{notes,attachments,activity}.js`, `app/admin-bridge-state.js`, `app/view-state.js`

**Admin page:** `frontend/admin/app.js` -> `runtime.js` -> `runtime/{composition,overview,events,state,view,effects,actions}.js`, `app/{auth,fetcher,discovery,sync}.js`, `app/registry/{ui,load,mutations}.js`, `app/ops/{format,task-state,health,bridge-status}.js`

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
- `ops_api.py` - stable OpsApi surface over `ops_history_projection.py`, `ops_task_live.py`, and `ops_live_payload.py`
- `source_check_api.py` - source probe/check helpers

**Still in `admin_bridge.py`:** bridge startup entrypoint, one-line `build_bridge_api(...)` wrapper, stable compatibility exports, and root monkeypatch seams

**Admin bridge entrypoint helpers:**
- `admin_entrypoint_runtime.py` - bridge log emission, runtime-path rebinding, startup metrics, owner-session lifecycle helpers
- `admin_entrypoint_services.py` - cached sync/registry/discovery/pipeline/updater/task-launch/ops builders
- `admin_entrypoint_api.py` - `BridgeApi` bootstrap/dependency assembly behind the stable root wrapper
- `admin_registry_api.py` - manual-source add/update flow, source-check glue, registry-state persistence helpers
- `admin_task_runtime.py` - sync/task runtime helpers, report waits, fetch-task launch/runtime glue

**Jobs package (`src/jobs/`):**
- `pipeline.py` - pipeline entry flow
- `pipeline_timing.py`, `pipeline_finalize.py` - timing aggregation and late-stage output/report assembly
- `state.py`, `state_incremental.py` - jobs state, cadence, and incremental freshness helpers
- `fetcher_compat_exports.py`, `fetcher_compat_runtime.py` - lazy compatibility exports and root-backed wrapper seams behind `src/jobs_fetcher.py`
- `adapters/` - static, provider_api, social fetchers
- `canonicalize.py`, `dedup.py` - normalization
- `common/` - leaf helpers (`config`, `contracts`, `heuristics`, `parsing`, etc.); `common/__init__.py` is compatibility-only

**Discovery package (`src/source_discovery/`):**
- `orchestrator.py` - public run flow and test patch surface over `orchestrator_{runtime,generation,probe,finalize}.py`
- `runtime_metrics.py`, `stage_control.py`, `reporting.py` - runtime bookkeeping, stage toggles, report helpers
- `gamesmap.py`, `gamedevmap.py`, `gameprog.py`, `sheet_directory.py`, `web_search.py` - domain generators

**Sync helpers:**
- `source_sync.py` - stable thin compatibility and test patch surface
- `source_sync_config.py` - packaged config resolution, validation, and raw GitHub request setup
- `source_sync_runtime.py` - runtime state, auth manager, rate limiting, DPAPI, and JSON request flow
- `source_sync_snapshot.py` - snapshot normalization, transition backfill, merge ranking, and remote read/write helpers
- `source_sync_crypto.py` - private-key encryption, PEM/ASN.1 parsing, and JWT signing helpers
- `adapters/static.py` - root static adapter surface over `static_runtime.py`, `static_listing.py`, `static_listing_flow.py`, `static_detail.py`, `static_sources.py`, `static_runtime_support.py`, `static_detail_heuristics.py`, and the `static_helpers.py` compatibility shim

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

- `src/packaged_desktop_smoke.py` - stable packaged smoke entrypoint and test patch surface; keep implementation in `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`
- `src/ship/desktop_update.py` - stable updater surface; keep implementation in `src/ship/desktop_update_{shared,state,service}.py`
- `src/admin_bridge.py` - stable thin entrypoint; add new bridge logic to `src/bridge/*.py` or `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`
- `src/source_discovery.py` - stable thin CLI entrypoint; add discovery logic to `src/source_discovery/*.py`
- `src/jobs_fetcher.py` - stable thin CLI facade; keep lazy export routing in `src/jobs/fetcher_compat_exports.py`, root-backed wrapper seams in `src/jobs/fetcher_compat_runtime.py`, and new pipeline logic in `src/jobs/*`
- `src/jobs/adapters/static.py` - stable static adapter surface; keep generic listing/detail/runtime logic in `src/jobs/adapters/static_{runtime,listing,listing_flow,detail,sources}.py`
- `src/source_sync.py` - permanent thin sync integration surface; keep new sync logic in `src/source_sync_{config,runtime,snapshot,crypto}.py`
- `src/jobs/common/__init__.py` - package marker only; prefer `src.jobs.common.<leaf>` or package-submodule imports
- `frontend/local-data/services.js` - transitional local-data boundary; page code should go through slice-local `services.js`

**Leaf modules that are still safe extraction targets**
- `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, `src/bridge/ops_live_payload.py`
- `src/jobs/fetcher_compat_{exports,runtime}.py`
- `src/jobs/adapters/static_listing_flow.py`
- `src/jobs/state_incremental.py`
- `frontend/saved/app/runtime/*.js`

---

## 10) Key libraries

- **Playwright** - frontend smoke tests
- **PyInstaller** - portable Windows executable
- **Scrapy + Playwright** - scraping fallback

---

*Last updated: 2026-04-23*

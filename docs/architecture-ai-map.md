# Baluffo Architecture AI Map

> **Use this when:** locating the correct subsystem, choosing edit boundaries, mapping task-to-files
> **Canonical for:** system boundaries, task routing, verification matrix
> **Not canonical for:** endpoint payloads, data schema details
> **Then inspect:** the minimal source files listed in the task table, plus matching contract doc if shape changes involved

---

## 1) System boundary map

```
jobs.html / saved.html / admin.html
  -> frontend/{jobs|saved|admin}/index.js
  -> frontend/{jobs|saved|admin}/app.js
  -> frontend/{jobs|saved|admin}/app/runtime.js
      -> page modules (app/*.js + actions/services/render/domain/data-source)
      -> shared (frontend/shared/*, selectors.js, api-client.js, state-hub.js)

src/dev_admin_supervisor.py (Baluffo launcher)
  -> starts site + bridge + owned browser session
  -> tears down on session exit

src/admin_bridge.py (composition root)
  -> src/bridge/ (services: sync, registry, discovery, pipeline, routes)

src/jobs_fetcher.py -> src/jobs/ (pipeline, adapters, dedup)
src/source_discovery.py -> src/source_discovery/ (package)

src/ship/desktop_app/__init__.py (desktop runtime)
  -> spawns site + bridge, opens browser, watches heartbeat
```

---

## 2) CLI Scripts

| Script | Purpose |
|--------|---------|
| `src/jobs_fetcher.py` | Build unified jobs feed |
| `src/source_discovery.py` | Discover candidate sources (delegates to package) |
| `src/dev_admin_supervisor.py` | Baluffo launcher (site + bridge + browser) |
| `src/admin_bridge.py` | Bridge-only entry (expert/manual mode) |
| `src/jobs/pipeline.py` | Core job processing |
| `src/source_discovery/` | Discovery package modules |
| `scripts/build_ship_bundle.py` | Create ship bundle |
| `scripts/build_portable_exe.py` | Create portable EXE |

---

## 3) Task→ minimal files

| Task | Start here | Then only if needed |
|------|------------|---------------------|
| Jobs filter/search | `frontend/jobs/app/filters.js` | `frontend/jobs/app/runtime.js` |
| Jobs feed refresh | `frontend/jobs/app/feed.js` | `frontend/jobs/services.js` |
| Saved notes | `frontend/saved/app/notes.js` | `frontend/saved/services.js` |
| Saved attachments | `frontend/saved/app/attachments.js` | `frontend/saved/services.js` |
| Admin ops | `frontend/admin/app/{auth,ops,fetcher,discovery,sync}.js` | `frontend/admin/services.js` |
| Bridge API | `src/bridge/*.py` | `src/bridge/routes/*.py` |
| Bridge sync | `src/bridge/sync_service.py` | `src/bridge/sync_state.py` |
| Bridge registry | `src/bridge/registry_service.py` | `src/source_registry.py` |
| Jobs pipeline | `src/jobs/pipeline.py` | `src/jobs/adapters/`, `src/jobs/canonicalize.py` |
| Desktop runtime | `src/ship/desktop_app/__init__.py` | `src/ship/runtime_launcher.py` |
| UI selectors | `frontend/shared/ui/selectors.js` | — |

---

## 4) Frontend topology

**Jobs page:** `frontend/jobs/app.js` → `runtime.js` → `app/filters.js`, `app/feed.js`, `app/cache.js`, `app/pipeline.js`, `app/sources.js`

**Saved page:** `frontend/saved/app.js` → `runtime.js` → `app/notes.js`, `app/attachments.js`, `app/activity.js`, `app/view-state.js`

**Admin page:** `frontend/admin/app.js` → `runtime.js` → `app/auth.js`, `app/fetcher.js`, `app/discovery.js`, `app/sync.js`, `app/registry.js`, `app/ops.js`

**Shared:** `frontend/shared/state-hub.js` (cross-module state), `frontend/shared/api-client.js` (bridge HTTP)

---

## 5) Backend topology

**Bridge services (`src/bridge/`):**
- `sync_service.py`, `sync_state.py` — sync operations
- `registry_service.py` — active/pending/rejected state
- `discovery_service.py` — discovery task orchestration
- `pipeline_service.py` — jobs pipeline task
- `routes/get_routes.py`, `routes/post_routes.py` — HTTP handlers
- `ops_api.py`, `task_history.py`, `source_check_api.py` — ops/report/orchestration

**Still in `admin_bridge.py`:** HTTP server, service wiring, thin wrappers

**Jobs package (`src/jobs/`):**
- `pipeline.py` — core processing
- `adapters/` — static, provider_api, social fetchers
- `canonicalize.py`, `dedup.py` — normalization
- `common/` — config, contracts, heuristics, parsing

---

## 6) Data model

| File | Purpose |
|------|---------|
| `data/jobs-unified.json` | Primary aggregated feed |
| `data/jobs-unified.csv` | CSV fallback |
| `data/jobs-fetch-report.json` | Last fetch diagnostics |
| `data/source-registry-active.json` | Approved sources |
| `data/source-registry-pending.json` | Discovered, not approved |
| `data/source-registry-rejected.json` | Rejected sources |
| `data/source-discovery-report.json` | Last discovery run |
| `data/local-user-data/users/{uid}/*.json` | Per-user saved jobs, notes, attachments |

---

## 7) Runtime contracts

- **Desktop single-instance:** If healthy session exists, raise error — do not open another window
- **Desktop startup:** start site + bridge → wait for page URL readiness → wait for `/ops/health` before steady state
- **Session/watchdog:** store metadata in `desktop-session.json`, track browser heartbeat, close on idle timeout

---

## 8) Fast verification matrix

| Change area | Fastest verification |
|-------------|----------------------|
| Frontend syntax | `node --check frontend/*/app.js` |
| Frontend unit | `npm run test:unit` |
| Bridge behavior | `python -m pytest tests/admin/ -q` |
| Pipeline/fetcher | `python -m pytest tests/test_jobs_fetcher_*.py -q` |
| Desktop launcher | `python -m pytest tests/test_desktop_app.py -q` |
| Full verification | `npm run verify` |

See [`testing.md`](testing.md) for more commands.

---

## 9) Transitional boundaries (don't move blindly)

- `src/admin_bridge.py` — composition root; add new logic to `src/bridge/*.py`
- `src/jobs/adapters/_runtime.py` — keep `_runtime.facade()` boundary
- `src/jobs/common/__init__.py` — shared barrel; prefer direct `src/jobs/common/*` imports
- `frontend/local-data/services.js` — keep `window.JobAppLocalData` abstraction

---

## 10) Key libraries

- **Playwright** — frontend smoke tests
- **PyInstaller** — portable Windows executable
- **Scrapy + Playwright** — scraping fallback

---

*Last updated: 2026-04-07*

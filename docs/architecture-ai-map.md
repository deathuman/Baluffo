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

admin bridge (local HTTP API): scripts/admin_bridge.py
jobs feed + discovery/sync scripts: scripts/jobs_fetcher.py, scripts/source_discovery.py, scripts/source_sync.py

desktop launcher/runtime: scripts/ship/desktop_app.py
  -> spawns local site + bridge
  -> opens browser app window
  -> watches heartbeat/activity and shutdown flow

runtime data roots:
  - repo/runtime: data/
  - desktop package runtime: ship/data/
  - desktop local user data: ship/data/local-user-data/
```

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
| Bridge API/runtime behavior | `scripts/admin_bridge.py` | `frontend/admin/services.js`, `frontend/jobs/services.js`, `frontend/saved/services.js` |
| Desktop startup/runtime behavior | `scripts/ship/desktop_app.py` | `tests/test_desktop_app.py`, `scripts/ship/runtime_launcher.py` |
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
| Frontend behavior/unit coverage | `npm run test:frontend:unit` |
| Desktop launcher/runtime behavior | `python -m pytest tests/test_desktop_app.py` |
| Packaged desktop smoke contract | `python -m pytest tests/test_packaged_desktop_smoke.py` |
| Bridge behavior changes | `python -m pytest tests/test_admin_bridge_ops.py` |

## 6) Related deep-dive docs

- Release and packaging process: `docs/RELEASE.md`
- Ship bundle packaging runbook: `docs/ship-bundle-runbook.md`
- Portable EXE runbook: `docs/portable-executable-runbook.md`
- Deployment/update flow: `docs/deployment-and-update-guide.md`

## Key Libraries and Frameworks

- **Playwright:** Used for frontend smoke regression tests.
- **PyInstaller:** Used to package the application as a portable Windows executable.
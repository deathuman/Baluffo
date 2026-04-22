# AI Assistant Guide — Baluffo Repository

> **Primary AI entrypoint.** Read this first. Then load only the smallest additional docs needed.

---

## Routing header

- **Use this when:** starting a code task, choosing edit boundaries, or finding the right subsystem
- **Canonical for:** task routing, file ownership, verification commands, high-signal shortcuts
- **Not canonical for:** data contracts, endpoint payloads, deep subsystem details
- **Then inspect:** the minimal source files listed in the task table, plus the matching contract doc if shape changes are involved

---

## What this repo is

**Local-first game jobs aggregator** with three parts:

1. **Frontend:** plain HTML/CSS/JS ES modules (`jobs.html`, `saved.html`, `admin.html`)
2. **Backend:** Python for fetching, discovery, sync, and local HTTP bridge
3. **Desktop:** Windows packaging/runtime that launches site + bridge locally

**Frontend styling layout**
- Shared CSS lives under `styles/base.css` (tokens, foundations) and `styles/components.css` (shared UI primitives).
- Page-scoped CSS lives under `styles/jobs.css`, `styles/saved.css`, and `styles/admin.css`.
- Do not reintroduce a root `styles.css` compatibility shim; keep shared rules shared and page polish page-scoped.

**Not** a React/Vite app. Not a cloud backend.

---

## Read order

1. This guide — task routing and edit boundaries
2. [`architecture-ai-map.md`](architecture-ai-map.md) — system boundaries and file ownership
3. One contract doc if data/shape changes:
   - [`DATA_CONTRACT.md`](DATA_CONTRACT.md) — payload/schema
   - [`admin-bridge-api.md`](admin-bridge-api.md) — bridge endpoints
   - [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md) — fetcher presets
4. One deep-dive if needed: [`testing.md`](testing.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), [`scraping-pipeline.md`](scraping-pipeline.md)
5. [`AGENTS.md`](../AGENTS.md) only when you need hard repo guardrails

**Do not load boundary-charter docs by default.** They are refactor/planning records, not the primary AI routing path. Use them only when you are intentionally working inside that specific cleanup lane and the canonical docs above are not enough.

## Docs-first boundaries

Baluffo is docs-first, not docs-only. Start with the smallest authoritative doc set, then read code for executable detail, verification, or when the docs do not own the question.

Canonical docs are authoritative only for the surface they declare. Use the routing docs for edit location, contract docs for stable payload or API shape, and the codebase for implementation details and runtime truth outside those declared surfaces.
If Serena memory and repo docs ever diverge, the repo docs stay canonical.

---

## Common wrong assumptions

| Wrong assumption | Reality |
|------------------|---------|
| Frontend is React/Vue | Vanilla ES modules, no framework |
| `src/admin_bridge.py` is the place for new logic | Prefer `src/bridge/*.py` modules; admin_bridge is wiring-only composition root |
| `src/source_discovery.py` == `src/source_discovery/` | CLI/legacy vs package modules |
| `src/jobs_fetcher.py` is where new pipeline logic belongs | Treat it as a thin CLI facade; add pipeline logic in `src/jobs/*` |
| Desktop and browser modes behave the same | Startup, session, heartbeat differ |
| Desktop local data uses browser `localStorage` / IndexedDB directly | Desktop pages use the bridge-backed file store under `data/local-user-data/` |
| Bridge changes need only backend tests | Verify both Python backend and frontend tests |
| UI selectors can be guessed | Always use `frontend/shared/ui/selectors.js` |
| Build scripts can import composition roots | Use leaf modules (`src/bridge/*`, `src/core/*`) or data-file reads |
| Endpoint payloads can be assumed | Check [`docs/admin-bridge-api.md`](admin-bridge-api.md) first |
| Running Baluffo launcher reflects code changes | Restart `npm run dev:bridge` after fixes |
| Contract changes are isolated | Update implementation + schemas + tests + docs |
| `rejected` is just a delete sentinel | Local delete is tombstone-backed; restore paths are explicit and sync only carries active/pending rows |

---

## Fast mental model

| Area | Entry points | Core modules |
|------|---------------|---------------|
| **Frontend** | `jobs.html`, `saved.html`, `admin.html` | `frontend/{jobs,saved,admin}/app/*.js`, `frontend/shared/`, `frontend/jobs/state.js`, `frontend/jobs/parsing-utils.js`, `frontend/saved/zip-utils.js` |
| **Backend** | `src/admin_bridge.py`, `src/jobs_fetcher.py` | `src/bridge/`, `src/jobs/`, `src/source_discovery/` |
| **Static Adapter** | `src/jobs/adapters/static.py` | `src/jobs/adapters/static_{runtime,listing,detail,sources}.py`, `src/jobs/adapters/static_{runtime_support,detail_heuristics}.py`, `src/jobs/adapters/static_helpers.py`, `src/jobs/adapters/plugins/static/` |
| **Desktop** | `src/ship/desktop_app/launcher.py` | `src/ship/desktop_app/{startup,browser,session,_windows,config}.py`, `src/ship/`, `scripts/` |
| **Packaged Smoke / Updater** | `src/packaged_desktop_smoke.py`, `src/ship/desktop_update.py` | `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py`, `src/ship/desktop_update_{shared,state,service}.py` |

---

## Where to start editing

| Task | Start here | Then only if needed |
|------|------------|---------------------|
| Frontend behavior | `frontend/<page>/app/*.js` | `frontend/<page>/app/runtime.js` |
| Jobs page runtime behavior | `frontend/jobs/app/feed.js`, `frontend/jobs/app/filters.js`, `frontend/jobs/app/runtime/{composition,boot,page-flow,events,feed-controller,list-view,pipeline-controller,query,startup-preview,auth-controller}.js` | `frontend/jobs/app/runtime.js` only for page-entry wiring/export changes |
| Saved page runtime behavior | `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes,activity-controller,attachments-controller,custom-job-controller,render-controller,auth-controller}.js`, `frontend/saved/app/admin-bridge-state.js` | `frontend/saved/app/runtime.js` only for page-entry wiring/export changes |
| Shared/page CSS | `styles/{base,components,<page>}.css` | The owning HTML entrypoint only if stylesheet includes need to change |
| Bridge/API | `src/bridge/*.py` | `src/bridge/routes/*.py`; for ops/report/live-task work start with `src/bridge/ops_api.py`, `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, and `src/bridge/ops_live_payload.py` |
| Admin bridge entrypoint/runtime wiring | `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py` | `src/admin_bridge.py` only when the stable wrapper or root patch seam must change |
| Discovery behavior | `src/source_discovery/orchestrator.py`, `orchestrator_{runtime,generation,probe,finalize}.py`, `runtime_metrics.py`, `stage_control.py`, `reporting.py` | `src/source_discovery.py` only for CLI compatibility |
| Registry sync / tombstones | `src/source_registry.py`, `src/bridge/registry_service.py`, `src/source_sync_snapshot.py`, `src/source_sync_config.py` | `src/bridge/registry_tombstones.py`, `src/source_sync_runtime.py`, `src/source_sync_crypto.py`, `src/source_sync.py` only for root-surface compatibility work, `src/bridge/routes/post_routes.py` |
| Jobs pipeline / fetcher behavior | `src/jobs/pipeline.py`, `src/jobs/pipeline_timing.py`, `src/jobs/pipeline_finalize.py`, `src/jobs/state.py`, `src/jobs/state_incremental.py`, other `src/jobs/*` leaf modules | `src/jobs_fetcher.py` only for CLI or compatibility-surface changes |
| Static adapter / scraping behavior | `src/jobs/adapters/static_{runtime,listing,listing_flow,detail,sources}.py`, `src/jobs/adapters/static_{runtime_support,detail_heuristics}.py` | `src/jobs/adapters/static_helpers.py` only for import-compatibility / monkeypatch shims, and `src/jobs/adapters/static.py` only when the root adapter surface or root patch seams must stay stable |
| Local-data page wiring | `frontend/<page>/services.js` | `frontend/local-data/services.js` only when the shared local-data API changes |
| Schema/contracts | `src/core/schemas.py` | `src/core/contracts.py`, `src/jobs/common/contracts.py` |
| Desktop/runtime | `src/ship/desktop_app/launcher.py` | `src/ship/desktop_app/{startup,browser,session,_windows,config}.py`, `src/ship/runtime_launcher.py` |
| Packaged smoke / updater | `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py`, `src/ship/desktop_update_{shared,state,service}.py` | `src/packaged_desktop_smoke.py` and `src/ship/desktop_update.py` only when the root compatibility surfaces or CLI/public contracts must stay stable |

**Compatibility surfaces**:
- `src/admin_bridge.py` is a stable thin entrypoint for bridge startup and compatibility wrappers; add new bridge logic in `src/bridge/*`.
- Admin bridge runtime/path/session/bootstrap glue now lives in `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`; use `src/admin_bridge.py` only for stable wrapper or monkeypatch-surface changes.
- `src/source_discovery.py` is a stable thin CLI entrypoint; discovery logic belongs in `src/source_discovery/*`.
- `src/ship/desktop_app/__init__.py` is now a thin facade; desktop runtime implementation belongs in the focused modules under `src/ship/desktop_app/`.
- `src/packaged_desktop_smoke.py` is the stable packaged smoke entrypoint and monkeypatch surface; implementation belongs in `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py`.
- `src/ship/desktop_update.py` is the stable desktop updater surface; implementation belongs in `src/ship/desktop_update_{shared,state,service}.py`.
- `src/source_sync.py` is a permanent thin sync integration surface; config lives in `src/source_sync_config.py`, runtime/auth/request flow lives in `src/source_sync_runtime.py`, snapshot merge/read-write logic lives in `src/source_sync_snapshot.py`, and crypto/PEM/JWT helpers live in `src/source_sync_crypto.py`.
- `src/jobs_fetcher.py` is a stable thin CLI facade; new pipeline logic belongs in `src/jobs/*`.
- `src/jobs/adapters/static.py` is the stable static adapter surface; generic listing/detail/runtime orchestration belongs in `src/jobs/adapters/static_{runtime,listing,detail,sources}.py`, runtime/reporting helpers live in `src/jobs/adapters/static_runtime_support.py`, detail heuristics live in `src/jobs/adapters/static_detail_heuristics.py`, and `src/jobs/adapters/static_helpers.py` stays a thin compatibility shim.
- `src/jobs/common/__init__.py` is a package marker only; do not add root-symbol exports there. Import `src.jobs.common.<leaf>` or use package-submodule imports.
- `_runtime.facade()` is retired; do not recreate adapter runtime facades.
- `frontend/local-data/services.js` is a transitional local-data boundary; page code should go through slice-local `services.js`.

**Stable roots vs safe leaf targets**
- Stable patch-safe roots stay thin: `src/admin_bridge.py`, `src/source_discovery.py`, `src/jobs_fetcher.py`, `src/jobs/adapters/static.py`, `src/packaged_desktop_smoke.py`, `src/ship/desktop_update.py`, `frontend/jobs/app/runtime.js`, `frontend/saved/app/runtime.js`, `frontend/admin/app/runtime.js`, `frontend/admin/app/registry.js`, and `frontend/admin/app/ops.js`.
- Safe extraction targets are the owning leaves behind those roots: `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, `src/bridge/ops_live_payload.py`, `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`, `src/source_sync_{config,runtime,snapshot,crypto}.py`, `src/jobs/adapters/static_listing_flow.py`, `src/jobs/state_incremental.py`, `frontend/jobs/app/runtime/{composition,boot,page-flow,events,feed-controller,list-view,pipeline-controller,query,startup-preview,auth-controller}.js`, `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes,activity-controller,attachments-controller,custom-job-controller,render-controller,auth-controller}.js`, `frontend/admin/app/runtime/*.js`, `frontend/admin/app/registry/*.js`, and `frontend/admin/app/ops/*.js`.

---

## Verification matrix

| Change area | Fastest check |
|-------------|----------------|
| Frontend syntax/wiring | `node --check frontend/jobs/app.js` |
| Bridge changes | `python -m pytest tests/admin/ -q` |
| Pipeline/fetcher | `python -m pytest tests/test_jobs_fetcher_*.py -q` |
| Full verification | `npm run verify` |

See [`testing.md`](testing.md) for full test commands.

---

## High-signal commands

- `npm run dev:bridge`
- `npm run dev:pipeline`
- `npm run verify`

For the narrowest verification matrix, fixture layout, and test-to-source map, see [`testing.md`](testing.md).

---

## AI editing rules

- Load minimal context; start narrow
- Prefer leaf modules over composition roots
- Treat `src/jobs/common/__init__.py` as a package marker, not a normal extension point
- Update implementation + schemas + tests + docs together
- When the task is documentation maintenance or doc ownership, follow [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md)
- Follow task-to-files table before guessing
- When uncertain about ownership after reading this guide, check [`architecture-ai-map.md`](architecture-ai-map.md)
- Treat boundary-charter docs as supporting historical/planning context, not as the default routing source

---

*Last updated: 2026-04-22*

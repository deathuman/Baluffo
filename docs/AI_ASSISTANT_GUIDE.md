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

---

## Common wrong assumptions

| Wrong assumption | Reality |
|------------------|---------|
| Frontend is React/Vue | Vanilla ES modules, no framework |
| `src/admin_bridge.py` is the place for new logic | Prefer `src/bridge/*.py` modules; admin_bridge is wiring-only composition root |
| `src/source_discovery.py` == `src/source_discovery/` | CLI/legacy vs package modules |
| `src/jobs_fetcher.py` is where new pipeline logic belongs | Treat it as a thin CLI facade; add pipeline logic in `src/jobs/*` |
| Desktop and browser modes behave the same | Startup, session, heartbeat differ |
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
| **Desktop** | `src/ship/desktop_app/__init__.py` | `src/ship/`, `scripts/` |

---

## Where to start editing

| Task | Start here | Then only if needed |
|------|------------|---------------------|
| Frontend behavior | `frontend/<page>/app/*.js` | `frontend/<page>/app/runtime.js` |
| Bridge/API | `src/bridge/*.py` | `src/bridge/routes/*.py` |
| Discovery behavior | `src/source_discovery/orchestrator.py`, `runtime_metrics.py`, `stage_control.py`, `reporting.py` | `src/source_discovery.py` only for CLI compatibility |
| Registry sync / tombstones | `src/source_registry.py`, `src/bridge/registry_service.py`, `src/source_sync_config.py`, `src/source_sync_snapshot.py` | `src/bridge/registry_tombstones.py`, `src/source_sync.py`, `src/source_sync_crypto.py`, `src/bridge/routes/post_routes.py` |
| Jobs pipeline / fetcher behavior | `src/jobs/pipeline.py`, `src/jobs/pipeline_timing.py`, `src/jobs/pipeline_finalize.py`, other `src/jobs/*` leaf modules | `src/jobs_fetcher.py` only for CLI or compatibility-surface changes |
| Local-data page wiring | `frontend/<page>/services.js` | `frontend/local-data/services.js` only when the shared local-data API changes |
| Schema/contracts | `src/core/schemas.py` | `src/core/contracts.py`, `src/jobs/common/contracts.py` |
| Desktop/runtime | `src/ship/desktop_app/__init__.py` | `src/ship/runtime_launcher.py` |

**Compatibility surfaces**:
- `src/admin_bridge.py` is a stable thin entrypoint for bridge startup and compatibility wrappers; add new bridge logic in `src/bridge/*`.
- `src/source_discovery.py` is a stable thin CLI entrypoint; discovery logic belongs in `src/source_discovery/*`.
- `src/source_sync.py` is a permanent thin sync integration surface; config, snapshot, and crypto ownership live in `src/source_sync_config.py`, `src/source_sync_snapshot.py`, and `src/source_sync_crypto.py`.
- `src/jobs_fetcher.py` is a stable thin CLI facade; new pipeline logic belongs in `src/jobs/*`.
- `src/jobs/common/__init__.py` is a package marker only; do not add root-symbol exports there. Import `src.jobs.common.<leaf>` or use package-submodule imports.
- `_runtime.facade()` is retired; do not recreate adapter runtime facades.
- `frontend/local-data/services.js` is a transitional local-data boundary; page code should go through slice-local `services.js`.

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
- Follow task-to-files table before guessing
- When uncertain about ownership after reading this guide, check [`architecture-ai-map.md`](architecture-ai-map.md)

---

*Last updated: 2026-04-13*

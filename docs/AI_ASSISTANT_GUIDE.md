# AI Assistant Guide — Baluffo Repository

> **Primary AI entrypoint for this repository.** Read this document first, then load only the smallest additional docs needed for the task.

---

## What this repo is

Baluffo is a **local-first game jobs aggregator** with three major parts:

1. **Static multi-page frontend** in plain HTML/CSS/JavaScript ES modules
2. **Python orchestration and local HTTP bridge** for fetching, discovery, sync, admin operations, and local data flows
3. **Windows desktop packaging/runtime** that launches the site and bridge locally

This is **not** a React/Vite app and **not** a conventional web backend serving a cloud product.

---

## Read order for AI coders

Load docs in this order unless your task clearly needs something else:

1. [`../README.md`](../README.md) — product overview and top-level structure
2. [`architecture-ai-map.md`](architecture-ai-map.md) — system boundaries, task-to-file routing, runtime guardrails
3. One canonical contract doc:
   - [`DATA_CONTRACT.md`](DATA_CONTRACT.md) for payload/schema changes
   - [`admin-bridge-api.md`](admin-bridge-api.md) for bridge endpoints
   - [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md) for fetcher/runtime presets
4. One subsystem deep dive if needed:
   - [`testing.md`](testing.md)
   - [`scraping-pipeline.md`](scraping-pipeline.md)
   - [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md)
   - [`LOCAL_SETUP.md`](LOCAL_SETUP.md)
5. [`../AGENTS.md`](../AGENTS.md) — repo workflow guardrails, edit discipline, verification expectations

Do **not** load many docs by default. Start narrow and pull in task-specific references only as needed.

---

## Common wrong assumptions

- **Frontend is vanilla ES modules**, not React, Vue, or Vite.
- **`src/admin_bridge.py` is a composition root**, not the preferred place for new backend business logic. Prefer `src/bridge/*` modules for behavior changes where possible.
- **`src/source_discovery.py` and `src/source_discovery/` are not the same thing**:
  - `src/source_discovery.py` is a CLI/legacy entry layer
  - `src/source_discovery/` contains the package modules and task logic
- **Desktop runtime and browser/local mode differ**. Startup, session, heartbeat, bridge readiness, and local data flows can be mode-specific.
- **Contract changes are cross-cutting**. If you change data shape, also check:
  - `src/core/*`
  - `src/jobs/common/contracts.py`
  - related tests
  - relevant docs in `docs/`
- **Some files are compatibility or transitional boundaries**. Do not "clean up" by moving logic blindly without checking [`architecture-ai-map.md`](architecture-ai-map.md).
- **Never guess UI selectors** — always use `frontend/shared/ui/selectors.js` as the source of truth for all UI element handles.
- **Build/packaging scripts must NOT import composition roots** — avoid importing `src.jobs`, `src.admin_bridge`, or other top-level re-export modules from build scripts. Use leaf modules like `src/bridge/*`, `src/core/*`, or direct data-file reads instead.
- **Don't assume endpoint payloads** — always check [`docs/admin-bridge-api.md`](admin-bridge-api.md) first before making assumptions about bridge API structures.
- **Don't assume local bridge reflects code changes** — if behavior looks stale after a fix, restart the bridge (`npm run dev:bridge`) before concluding the fix failed.
- **Bridge changes require both frontend AND backend verification** — when modifying bridge routes or payloads, verify both the Python backend tests and any affected frontend tests.

---

## Fast mental model

### Frontend
- Entry pages:
  - `jobs.html`
  - `saved.html`
  - `admin.html`
- Main code:
  - `frontend/jobs/`
  - `frontend/saved/`
  - `frontend/admin/`
  - `frontend/shared/`
  - `frontend/local-data/`

Each page typically flows through:
- `index.js`
- `app.js`
- `app/runtime.js`
- supporting `actions.js`, `services.js`, `render.js`, `domain.js`, `data-source.js`, `state-sync/`, and `app/*` modules

**Frontend import structure**: The `app/runtime.js` files import many modules (often 20+) to wire up the entire page. However, most behavior changes should go to specific modules in `app/*.js` (e.g., `filters.js`, `feed.js`, `cache.js`) rather than the runtime orchestration file itself. Only edit runtime.js when you need to add new module wiring or orchestration-level changes.

### Backend / local control plane
- `src/admin_bridge.py` — entrypoint and composition root
- `src/bridge/` — bridge services, routes, sync/discovery/pipeline/ops logic
- `src/jobs_fetcher.py` — unified jobs feed generation
- `src/jobs/` — jobs pipeline, adapters, canonicalization, dedupe
- `src/source_discovery/` — source discovery package
- `src/source_sync.py` — sync behavior

### Desktop/runtime
- `src/ship/` — desktop launcher/runtime/packaging support
- `scripts/` — build and orchestration helpers

---

## Where to start editing by task

### Frontend behavior changes
Start in:
- `frontend/<page>/app/*.js`
- then `frontend/<page>/app/runtime.js`
- then `frontend/<page>/{services,render,domain}.js` only if needed

### Bridge/API behavior changes
Start in:
- `src/bridge/*.py`
- `src/bridge/routes/*.py`
- inspect `src/admin_bridge.py` only for wiring/composition needs

### Jobs pipeline changes
Start in:
- `src/jobs/pipeline.py`
- then nearby modules under `src/jobs/`
- use [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) or [`scraping-pipeline.md`](scraping-pipeline.md) when source extraction is involved

### Schema/contract changes
Start in:
- [`DATA_CONTRACT.md`](DATA_CONTRACT.md)
- `src/core/schemas.py`
- `src/core/contracts.py`
- `src/jobs/common/contracts.py`

### Desktop/runtime changes
Start in:
- `src/ship/desktop_app/__init__.py`
- nearby runtime launcher/session files
- relevant desktop/runtime tests

---

## Files that need extra caution

Prefer **wiring-only** changes unless the task specifically requires otherwise:

- `src/admin_bridge.py`
- compatibility surfaces called out in [`architecture-ai-map.md`](architecture-ai-map.md)
- broad page runtime files like `frontend/jobs/app/runtime.js` unless the task is truly orchestration-level

Before editing a large orchestration file, check whether the behavior already belongs in a smaller helper/service module.

---

## Canonical docs by purpose

| Need | Canonical doc |
|------|---------------|
| System boundaries, task routing, stable/transitional guardrails | [`architecture-ai-map.md`](architecture-ai-map.md) |
| Data shape between pipeline, bridge, and frontend | [`DATA_CONTRACT.md`](DATA_CONTRACT.md) |
| Admin Bridge endpoint surface | [`admin-bridge-api.md`](admin-bridge-api.md) |
| Testing strategy and narrowest useful checks | [`testing.md`](testing.md) |
| Scraping/browser fallback flow | [`scraping-pipeline.md`](scraping-pipeline.md) |
| Source adapter/plugin inventory | [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) |
| Local setup/runtime expectations | [`LOCAL_SETUP.md`](LOCAL_SETUP.md) |
| Build/release operations | [`RELEASE.md`](RELEASE.md) |

If multiple docs seem to overlap, prefer the one marked canonical for the specific concern.

---

## Verification guidance

Use the **smallest relevant verification first**.

Examples:
- Frontend syntax/wiring: targeted JS checks or frontend unit tests
- Bridge changes: targeted `tests/admin/` or bridge-focused tests
- Pipeline changes: targeted pipeline/fetcher tests
- Desktop/runtime changes: targeted desktop tests
- Cross-cutting changes: `npm run verify`

See [`testing.md`](testing.md) and the verification matrix in [`architecture-ai-map.md`](architecture-ai-map.md).

---

## High-signal commands

| Goal | Command |
|------|---------|
| Start Admin Bridge | `npm run dev:bridge` |
| Run Jobs Pipeline | `npm run dev:pipeline` |
| Full build | `npm run build` |
| Full verification | `npm run verify` |
| Python tests | `npm run test:py` |
| Local pre-commit gate | `npm run lint:precommit` |
| Full pre-commit sweep | `npm run lint:precommit:all` |
| CI pre-commit sweep | `npm run lint:precommit:ci` |
| Frontend unit tests | `npm run test:unit` |
| Frontend smoke tests | `npm run test:smoke` |

`npm run verify` includes the repo's local changed-files pre-commit gate, so it is the safest single command to run before pushing changes to `main`.
Use `npm run lint:precommit:all` when you want the local full-repo sweep.
Use `npm run lint:precommit:ci` when you want CI-parity lint coverage, including the pre-push hooks, while skipping generated `data/*` artifacts. The current `mypy` hook is intentionally scoped to `src/python_version_guard.py` and `src/pipeline_io.py` until the broader typed surface is cleaned up.

---

## AI editing rules of thumb

- Prefer **minimal-context loading** over reading many files/documents.
- Prefer editing **leaf modules** over composition roots when possible.
- If a change affects contract shape, update:
  - implementation
  - schemas
  - tests
  - docs
- When refactoring, preserve transitional boundaries called out in the architecture doc unless the task is explicitly a refactor plan.
- When uncertain about ownership, follow the task-to-files table in [`architecture-ai-map.md`](architecture-ai-map.md) before making assumptions.

---

*Last updated: 2026-03-23*

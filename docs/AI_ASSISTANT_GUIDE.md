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

1. [`README.md`](../README.md) — product overview
2. This guide — task routing and edit boundaries
3. One contract doc if data/shape changes:
   - [`DATA_CONTRACT.md`](DATA_CONTRACT.md) — payload/schema
   - [`admin-bridge-api.md`](admin-bridge-api.md) — bridge endpoints
   - [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md) — fetcher presets
4. One deep-dive if needed: [`testing.md`](testing.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), [`scraping-pipeline.md`](scraping-pipeline.md)
5. [`AGENTS.md`](../AGENTS.md) — repo guardrails

---

## Common wrong assumptions

| Wrong assumption | Reality |
|------------------|---------|
| Frontend is React/Vue | Vanilla ES modules, no framework |
| `src/admin_bridge.py` is the place for new logic | Prefer `src/bridge/*.py` modules; admin_bridge is composition root |
| `src/source_discovery.py` == `src/source_discovery/` | CLI/legacy vs package modules |
| Desktop and browser modes behave the same | Startup, session, heartbeat differ |
| Bridge changes need only backend tests | Verify both Python backend and frontend tests |
| UI selectors can be guessed | Always use `frontend/shared/ui/selectors.js` |
| Build scripts can import composition roots | Use leaf modules (`src/bridge/*`, `src/core/*`) or data-file reads |
| Endpoint payloads can be assumed | Check [`docs/admin-bridge-api.md`](admin-bridge-api.md) first |
| Running Baluffo launcher reflects code changes | Restart `npm run dev:bridge` after fixes |
| Contract changes are isolated | Update implementation + schemas + tests + docs |

---

## Fast mental model

| Area | Entry points | Core modules |
|------|---------------|---------------|
| **Frontend** | `jobs.html`, `saved.html`, `admin.html` | `frontend/{jobs,saved,admin}/app/*.js`, `frontend/shared/` |
| **Backend** | `src/admin_bridge.py`, `src/jobs_fetcher.py` | `src/bridge/`, `src/jobs/`, `src/source_discovery/` |
| **Desktop** | `src/ship/desktop_app/__init__.py` | `src/ship/`, `scripts/` |

---

## Where to start editing

| Task | Start here | Then only if needed |
|------|------------|---------------------|
| Frontend behavior | `frontend/<page>/app/*.js` | `frontend/<page>/app/runtime.js` |
| Bridge/API | `src/bridge/*.py` | `src/bridge/routes/*.py` |
| Jobs pipeline | `src/jobs/pipeline.py` | `src/jobs/adapters/`, `src/jobs/canonicalize.py` |
| Schema/contracts | `src/core/schemas.py` | `src/core/contracts.py`, `src/jobs/common/contracts.py` |
| Desktop/runtime | `src/ship/desktop_app/__init__.py` | `src/ship/runtime_launcher.py` |

**Caution files** (prefer wiring-only): `src/admin_bridge.py`, transitional boundaries in architecture doc, broad page runtime files.

---

## Verification matrix

| Change area | Fastest check |
|-------------|----------------|
| Frontend syntax/wiring | `node --check frontend/*/app.js` |
| Bridge changes | `python -m pytest tests/admin/ -q` |
| Pipeline/fetcher | `python -m pytest tests/test_jobs_fetcher_*.py -q` |
| Full verification | `npm run verify` |

See [`testing.md`](testing.md) for full test commands.

---

## High-signal commands

| Goal | Command |
|------|---------|
| Start Baluffo | `npm run dev:bridge` |
| Run jobs pipeline | `npm run dev:pipeline` |
| Full build | `npm run build` |
| Full verification | `npm run verify` |
| Python tests | `npm run test:py` |
| Frontend unit | `npm run test:unit` |
| Frontend smoke | `npm run test:smoke` |
| Local pre-commit | `npm run lint:precommit:changed` |

`npm run verify` = CI-parity gate, safest before pushing.

---

## AI editing rules

- Load minimal context; start narrow
- Prefer leaf modules over composition roots
- Update implementation + schemas + tests + docs together
- Follow task-to-files table before guessing
- When uncertain about ownership, check [`architecture-ai-map.md`](architecture-ai-map.md)

---

*Last updated: 2026-04-07*
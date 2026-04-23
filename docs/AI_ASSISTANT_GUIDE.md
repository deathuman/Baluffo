# AI Assistant Guide - Baluffo Repository

> - **Status:** Active
> - **Use this when:** starting a code task, choosing edit boundaries, or finding the right subsystem
> - **Canonical for:** task routing, minimal read order, common repo misconceptions, and AI editing rules
> - **Not canonical for:** data contracts, endpoint payloads, or deep subsystem ownership detail
> - **Then inspect:** [`architecture-ai-map.md`](architecture-ai-map.md) for task-to-files routing, plus one matching contract or workflow doc
> - **Last updated:** 2026-04-23

Read this first. Then load only the smallest additional docs needed.

## What this repo is

Baluffo is a local-first game jobs aggregator with three layers:

1. Frontend: plain HTML/CSS/JS ES modules (`jobs.html`, `saved.html`, `admin.html`)
2. Backend: Python for fetching, discovery, sync, and the local HTTP bridge
3. Desktop: Windows packaging/runtime that launches the site and bridge locally

This is not a React/Vite app and not a cloud backend.

## Read order

1. This guide
2. [`architecture-ai-map.md`](architecture-ai-map.md) only when you need task-to-files routing, ownership detail, or compatibility-surface classification
3. One matching contract or workflow doc:
   - [`DATA_CONTRACT.md`](DATA_CONTRACT.md)
   - [`admin-bridge-api.md`](admin-bridge-api.md)
   - [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md)
   - [`testing.md`](testing.md)
   - [`LOCAL_SETUP.md`](LOCAL_SETUP.md)
4. [`AGENTS.md`](../AGENTS.md) only for repo guardrails and prompt-routing rules

**Do not load boundary-charter docs by default.** They are refactor/planning records, not the primary AI routing path. Use them only when you are intentionally working inside a specific archived cleanup lane and the active docs are not enough.

## Docs-First Boundaries

Baluffo is docs-first, not docs-only. Start with the smallest authoritative doc set, then read code for executable detail, verification, or when the docs do not own the question.

Canonical docs are authoritative only for the surface they declare. Use routing docs for edit location, contract docs for stable payload or API shape, workflow docs for maintenance process, and the codebase for implementation detail outside those declared surfaces.

If Serena memory and repo docs ever diverge, the repo docs stay canonical.

## Common Wrong Assumptions

| Wrong assumption | Reality |
|------------------|---------|
| Frontend is React/Vue | Vanilla ES modules, no framework |
| `src/admin_bridge.py` is the place for new logic | Prefer `src/bridge/*.py`; `src/admin_bridge.py` stays a thin compatibility surface |
| `src/source_discovery.py` owns discovery implementation | It is a thin CLI surface over `src/source_discovery/*` |
| `src/jobs_fetcher.py` is where new pipeline logic belongs | Treat it as a thin CLI facade; new pipeline logic belongs in `src/jobs/*` |
| Desktop local data uses browser `localStorage` directly | Desktop mode uses the bridge-backed file store under `data/local-user-data/` |
| Bridge changes only need backend tests | Verify both Python backend and frontend/runtime callers as needed |
| UI selectors can be guessed | Use `frontend/shared/ui/selectors.js` |
| Endpoint payloads can be assumed | Check [`admin-bridge-api.md`](admin-bridge-api.md) first |

## Verification Shortcuts

| Change area | Fastest check |
|-------------|----------------|
| Frontend syntax/wiring | `node --check frontend/jobs/app.js` |
| Bridge changes | `python -m pytest tests/admin/ -q` |
| Pipeline/fetcher | `python -m pytest tests/test_jobs_fetcher_*.py -q` |
| Full verification | `npm run verify` |

See [`testing.md`](testing.md) for the full verification matrix.

## AI Editing Rules

- Load minimal context; start narrow.
- Prefer leaf modules over composition roots.
- `src/jobs/common/__init__.py` is a package marker only.
- `_runtime.facade()` is retired and should not be recreated.
- Update implementation, schemas, tests, and docs together when contracts or workflow move.
- When the task is documentation maintenance or doc ownership, follow [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md).
- Archived refactor/history docs are supporting context, not default routing sources.

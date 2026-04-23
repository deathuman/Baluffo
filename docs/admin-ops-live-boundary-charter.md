# Admin Ops Live Boundary Charter

## Title

Admin ops live-payload boundary cleanup and renderer split

## Goal

Make the admin ops live-task lane easier to navigate without changing contracts. `src/bridge/ops_task_live.py` stays the stable surface used by `src/bridge/ops_api.py`, while the bulky fetch, discovery, and projected current-task assembly logic moves into focused helper modules. On the frontend, `frontend/admin/render/ops.js` stays a small compatibility surface while the summary/history render logic lives in dedicated leaves.

## Target Boundary

- Primary subsystem: admin ops live payloads and ops history rendering
- Entry file(s): `src/bridge/ops_task_live.py`, `frontend/admin/render/ops.js`
- Ownership boundary being clarified: fetch/discovery/projection live-task builders vs stable route-facing surface; summary/history renderers vs stable render export surface
- What becomes easier after this change: routing future edits, keeping tests/docs truthful, and reducing token waste when touching ops observability code

## Why Now

- Current pain: `ops_task_live.py` and `frontend/admin/render/ops.js` had become the next clear admin observability monoliths after the bridge-entrypoint cleanup
- Why this is worth doing now: the backend live-task contract and its main renderer are coupled enough that keeping them small together reduces drift for both humans and AI
- Why this should stay narrow: route shapes, `taskProgress`, and controller APIs already work and should not be redesigned in this pass

## In Scope

- Split fetch/discovery/projection live-task builders into `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py`
- Keep `src/bridge/ops_task_live.py` as the stable integration surface used by `src/bridge/ops_api.py`
- Split `frontend/admin/render/ops.js` into `ops-summary.js`, `ops-history.js`, and `ops-shared.js`
- Update routing docs and thin-surface guardrails

## Out of Scope

- Route signature or payload-shape changes
- Admin controller behavior changes in `frontend/admin/app/ops.js`
- `src/bridge/ops_live_payload.py` primitive helper redesign

## Stability Impact

- Runtime behavior touched: admin ops live payload assembly and ops history rendering internals only
- Persisted state touched: none
- Packaging or desktop behavior touched: none
- Compatibility concern: `OpsApi.get_task_live_payload(...)`, `OpsApi.build_current_task_state_payload(...)`, `/ops/task-live/<taskType>`, `/ops/task-state`, and `frontend/admin/render.js` exports must remain unchanged
- Rollback trigger: any change in `taskProgress`, `workItems`, `recentEvents`, current-task row semantics, or admin ops renderer output beyond structural ownership

## AI Accessibility Impact

- Source-of-truth file after refactor: `src/bridge/ops_task_live.py` stays the stable entry, but implementation ownership moves to `ops_task_fetch_live.py`, `ops_task_discovery_live.py`, and `ops_task_projection.py`; `frontend/admin/render/ops.js` stays the stable export surface, but rendering ownership moves to `ops-summary.js`, `ops-history.js`, and `ops-shared.js`
- Expected search path for future edits: `ops_api.py` -> `ops_task_live.py` -> focused live-task helper; `frontend/admin/render.js` -> `frontend/admin/render/ops.js` -> focused renderer leaf
- Docs or registry to update: `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, `docs/INDEX.md`, `docs/admin-bridge-api.md`, Serena `routing_and_boundaries`
- Any transitional seam being kept temporarily: thin compatibility surfaces on `src/bridge/ops_task_live.py` and `frontend/admin/render/ops.js`

## Implementation Shape

- Modules to shrink, split, or simplify: `src/bridge/ops_task_live.py`, `frontend/admin/render/ops.js`
- Interfaces or contracts to formalize: stable live-task entrypoints and stable admin render exports
- Existing abstractions to reuse: `src/bridge/ops_live_payload.py`, `src/shared/live_task`, `frontend/shared/task-progress.js`
- New abstraction to avoid unless proven necessary: a new ops root facade or controller layer

## Verification

- Cheapest syntax/check step: `python -m py_compile src/bridge/ops_task_live.py src/bridge/ops_task_fetch_live.py src/bridge/ops_task_discovery_live.py src/bridge/ops_task_projection.py`
- Cheapest focused test step: `python -m pytest tests/admin/test_admin_bridge_live_payloads.py tests/bridge/test_routes_get.py -q`
- Broader verification required only if: docs or renderer structure checks change, or route-facing payload assertions fail

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

`src/bridge/ops_task_live.py` remains the stable module surface used by `src/bridge/ops_api.py`. `frontend/admin/render/ops.js` remains a thin compatibility re-export surface so existing imports and tests do not need target changes.

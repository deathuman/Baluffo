# Admin Ops Live Boundary Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/bridge/ops_task_live.py` stayed the stable live-task surface used by `src/bridge/ops_api.py`.
- Fetch, discovery, and projected current-task assembly moved into focused helpers such as `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py` and `src/bridge/ops_live_payload.py`.
- `frontend/admin/render/ops.js` stayed as a compatibility surface while the summary/history rendering work moved into `frontend/admin/render/{ops-summary,ops-history,ops-shared}.js`.

## Final Owning Surfaces

- Stable backend root: `src/bridge/ops_task_live.py`
- Stable frontend root: `frontend/admin/render/ops.js`
- Owning leaves: `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py`, `src/bridge/ops_live_payload.py`, and `frontend/admin/render/{ops-summary,ops-history,ops-shared}.js`

## Current Routing

Current edit routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../admin-bridge-api.md`](../../admin-bridge-api.md).

## Historical Notes

- Route-facing payload shape and `taskProgress` behavior were intentionally preserved in this pass.
- Later follow-up state is summarized in [`../history/runtime-first-cleanup-handoff.md`](../history/runtime-first-cleanup-handoff.md).

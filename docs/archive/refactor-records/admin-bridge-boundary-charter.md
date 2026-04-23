# Admin Bridge Boundary Cleanup

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/admin_bridge.py` was reduced to a stable bridge startup and compatibility root.
- Runtime-path rebinding, cached service builders, bridge API bootstrap, manual-source flow, and task/runtime helpers moved behind `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`.
- Existing route signatures, manual-source/source-check behavior, task launch/live-status flow, and the root monkeypatch surface were preserved.

## Final Owning Surfaces

- Stable root: `src/admin_bridge.py`
- Owning leaves: `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`

## Current Routing

Current edit routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../admin-bridge-api.md`](../../admin-bridge-api.md).

## Historical Notes

- This pass intentionally kept `src/admin_bridge.py` as both the startup entrypoint and the test patch surface.
- Later follow-up state is summarized in [`../history/runtime-first-cleanup-handoff.md`](../history/runtime-first-cleanup-handoff.md).

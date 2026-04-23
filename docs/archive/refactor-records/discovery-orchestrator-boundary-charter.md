# Discovery Orchestrator Boundary Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/source_discovery/orchestrator.py` was reduced from a closure-heavy run flow into focused helper modules.
- Runtime/progress bookkeeping, generation, probe/recovery, and final persistence/report work moved into `src/source_discovery/orchestrator_{runtime,generation,probe,finalize}.py`.
- The public discovery API, bridge-owned lifecycle/report behavior, and discovery output contracts were preserved.

## Final Owning Surfaces

- Stable roots: `src/source_discovery.py`, `src/source_discovery/orchestrator.py`
- Owning leaves: `src/source_discovery/orchestrator_{runtime,generation,probe,finalize}.py`, with supporting ownership in `runtime_metrics.py` and `stage_control.py`

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../DATA_CONTRACT.md`](../../DATA_CONTRACT.md) when output shape is relevant.

## Historical Notes

- `src/source_discovery.py` remained the thin CLI entrypoint.
- The package export surface stayed stable; the point of the pass was smaller orchestration ownership, not a new public discovery API.

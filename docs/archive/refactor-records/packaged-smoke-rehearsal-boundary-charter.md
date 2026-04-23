# Packaged Smoke Rehearsal Boundary Cleanup

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/packaged_desktop_smoke.py` stayed as the packaged smoke CLI and monkeypatch surface.
- Generic helpers, startup-metric/report handling, orchestration, and rehearsal-family implementations moved into focused modules under `src/ship/packaged_smoke/`.
- CLI flags, packaged report shape, artifact semantics, and root patch targets were preserved.

## Final Owning Surfaces

- Stable root: `src/packaged_desktop_smoke.py`
- Owning leaves: `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../startup-probe-architecture.md`](../../startup-probe-architecture.md).

## Historical Notes

- The root kept a small set of patch-safe compatibility aliases on purpose.
- This pass narrowed packaged smoke ownership without reopening updater or release-flow contracts.

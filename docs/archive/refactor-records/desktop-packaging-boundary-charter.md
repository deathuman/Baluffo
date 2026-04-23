# Desktop Packaging Boundary Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/packaged_desktop_smoke.py` stayed as the packaged smoke CLI and root patch surface while helper ownership moved behind the `src/ship/packaged_smoke/` package.
- `src/ship/desktop_update.py` stayed as the stable updater import surface while updater shared/state/service logic moved behind `src/ship/desktop_update_{shared,state,service}.py`.
- Packaged rehearsal behavior, updater contracts, and release-facing flows were kept stable during the split.

## Final Owning Surfaces

- Stable roots: `src/packaged_desktop_smoke.py`, `src/ship/desktop_update.py`
- Owning leaves: `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py` and `src/ship/desktop_update_{shared,state,service}.py`

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), [`../../startup-probe-architecture.md`](../../startup-probe-architecture.md), and [`../../RELEASE.md`](../../RELEASE.md).

## Historical Notes

- Root-module monkeypatch compatibility was intentionally preserved even though it left a thin indirection seam in place.
- This pass was structural cleanup, not a release or updater redesign.

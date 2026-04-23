# Packaged Smoke Rehearsal Boundary Cleanup

## Goal

Make the packaged smoke docs and code agree by keeping `src/packaged_desktop_smoke.py` as the stable CLI and monkeypatch surface while moving generic helper logic, startup-metric/report handling, orchestration, and rehearsal-family implementations into focused modules under `src/ship/packaged_smoke/`.

## Target Boundary

- Primary subsystem: packaged smoke runner and packaged rehearsal helpers
- Entry file(s): `src/packaged_desktop_smoke.py`, `src/ship/packaged_smoke/rehearsals.py`
- Ownership boundary being clarified: root CLI and patch-surface compatibility vs helper-module implementation ownership
- What becomes easier after this change: finding the right packaged smoke edit location, preserving test patch seams, and updating startup/rehearsal logic without reopening the root monolith

## Why Now

- Current pain: `src/packaged_desktop_smoke.py` and `src/ship/packaged_smoke/rehearsals.py` had both drifted into large mixed-responsibility files even though the routing docs described them as thin surfaces.
- Why this is worth doing now: packaged smoke remains one of the highest-noise remaining lanes for both humans and AI, and it still drives release-gating confidence.
- Why this should stay narrow: this pass preserves CLI flags, report shape, artifact semantics, and root monkeypatch targets instead of reopening updater or release-flow contracts.

## In Scope

- Split generic JSON/HTTP/report helpers into `src/ship/packaged_smoke/common.py`
- Split startup metric/probe helpers into `src/ship/packaged_smoke/startup_metrics.py`
- Move root smoke orchestration into `src/ship/packaged_smoke/orchestrator.py`
- Split rehearsal implementations into `rehearsal_sync.py`, `rehearsal_update.py`, and `rehearsal_browser.py`
- Reduce `src/ship/packaged_smoke/rehearsals.py` to a compatibility shim
- Update AI-routing docs and add a thin-root contract check

## Out of Scope

- `src/ship/desktop_update.py` or `src/ship/desktop_updater.py` contract changes
- `package.json` packaged smoke command changes
- Re-sharding `tests/packaged_desktop/test_rehearsal_flows.py`

## Stability Impact

- Runtime behavior touched: packaged smoke orchestration, startup-profile reporting, and rehearsal flow delegation
- Persisted state touched: none intentionally beyond existing smoke artifacts and reports
- Packaging or desktop behavior touched: packaged smoke verification only; shipped runtime behavior should stay unchanged
- Compatibility concern: `src.packaged_desktop_smoke` remains the root monkeypatch surface used by packaged smoke tests
- Rollback trigger: any change to CLI flags, artifact/report keys, or rehearsal pass/fail semantics

## AI Accessibility Impact

- Source-of-truth file after refactor: `docs/AI_ASSISTANT_GUIDE.md` plus `docs/architecture-ai-map.md` for packaged smoke routing
- Expected search path for future edits: root facade first, then `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`
- Docs or registry to update: `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, `docs/INDEX.md`, `docs/startup-probe-architecture.md`, and Serena routing memory
- Any transitional seam being kept temporarily: root-backed monkeypatch aliases on `src/packaged_desktop_smoke.py`

## Implementation Shape

- Modules to shrink, split, or simplify: `src/packaged_desktop_smoke.py`, `src/ship/packaged_smoke/rehearsals.py`
- Interfaces or contracts to formalize: packaged smoke root aliases, startup probe/report ownership, rehearsal-family boundaries
- Existing abstractions to reuse: `build_env.py`, `runtime.py`, startup probe policy/profile helpers, and the packaged smoke test suite
- New abstraction to avoid unless proven necessary: a second packaged smoke facade or a generic rehearsal registry layer

## Verification

- Cheapest syntax/check step: `python -m py_compile src/packaged_desktop_smoke.py src/ship/packaged_smoke/*.py`
- Cheapest focused test step: `python -m pytest tests/packaged_desktop/ -q`
- Broader verification required only if: release docs, workflow entrypoints, or packaged smoke contracts drift

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

Keep `src/packaged_desktop_smoke.py` as the CLI and patch-safe root even if that means a few compatibility aliases remain intentionally thin but non-minimal for now.

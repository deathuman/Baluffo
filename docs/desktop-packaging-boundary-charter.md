# Desktop Packaging Boundary Charter

Use this tracker for the packaged smoke and desktop updater modularization pass.

## Title

Desktop Packaging and Updater Boundary Cleanup

## Goal

Reduce the two remaining desktop-packaging monoliths without changing release behavior, updater contracts, or rehearsal outcomes. Keep `src/packaged_desktop_smoke.py` as the executable CLI and test patch surface, keep `src/ship/desktop_update.py` as the stable updater import surface, and move the implementation details behind those root modules so humans and AI editors can find the right file in one or two searches.

## Target Boundary

- Primary subsystem: Packaged smoke runner, desktop update service, packaged rehearsal helpers
- Entry file(s): `src/packaged_desktop_smoke.py`, `src/ship/desktop_update.py`
- Ownership boundary being clarified: build/env helpers, runtime wait/probe helpers, packaged rehearsal flows, updater shared helpers, updater state/handoff helpers, updater service orchestration
- What becomes easier after this change: targeted packaged-smoke fixes, safer updater edits, smaller AI context windows, clearer test patch seams

## Why Now

- Current pain: both root modules mixed CLI/public compatibility surfaces with hundreds of lines of concrete implementation detail
- Why this is worth doing now: the app is stable end-to-end, so this is the right time to reduce token waste and routing ambiguity without redesigning behavior
- Why this should stay narrow: packaging and updater paths are high risk; preserving contracts matters more than making the root files perfectly minimal in one pass

## In Scope

- Create a repo-tracked charter for this cleanup lane
- Extract packaged smoke helpers into `src/ship/packaged_smoke/{build_env,runtime,rehearsals}.py`
- Extract updater helpers into `src/ship/desktop_update_{shared,state,service}.py`
- Keep the root smoke runner and root updater module as the compatibility surfaces
- Update AI routing docs so future edits prefer the new helper modules

## Out of Scope

- Changing packaged smoke CLI flags or package.json commands
- Changing updater manifest/install-plan schema or handoff semantics
- Refactoring `src/admin_bridge.py`, Saved runtime, or static adapter code in this pass
- Sharding `tests/test_desktop_update.py`

## Stability Impact

- Runtime behavior touched: packaged smoke orchestration, updater fetch/download/install state transitions, packaged rehearsal flow wiring
- Persisted state touched: desktop updater install state, install plan, handoff marker, packaged rehearsal local data verification
- Packaging or desktop behavior touched: yes, but behavior-preserving only
- Compatibility concern: packaged smoke tests patch `smoke.*` helpers directly, and updater tests patch `du.*` helpers directly
- Rollback trigger: any regression in `tests/packaged_desktop/`, `tests/test_desktop_update.py`, `tests/test_desktop_updater.py`, or desktop handoff coverage

## AI Accessibility Impact

- Source-of-truth files after refactor:
  - `src/ship/packaged_smoke/build_env.py`
  - `src/ship/packaged_smoke/runtime.py`
  - `src/ship/packaged_smoke/rehearsals.py`
  - `src/ship/desktop_update_shared.py`
  - `src/ship/desktop_update_state.py`
  - `src/ship/desktop_update_service.py`
- Expected search path for future edits:
  - packaged build/env and artifact hygiene -> `build_env.py`
  - packaged runtime launch/wait/node-smoke helpers -> `runtime.py`
  - packaged rehearsal logic and helper servers -> `rehearsals.py`
  - updater JSON/network/signature/path helpers -> `desktop_update_shared.py`
  - updater status/handoff/install-plan helpers -> `desktop_update_state.py`
  - updater orchestration/service logic -> `desktop_update_service.py`
- Docs or registry to update:
  - `docs/AI_ASSISTANT_GUIDE.md`
  - `docs/architecture-ai-map.md`
  - `docs/startup-probe-architecture.md`
  - `docs/INDEX.md`
- Transitional seam being kept temporarily:
  - extracted helper modules resolve through the root module object so existing `smoke.*` and `du.*` monkeypatches keep working

## Implementation Shape

- Modules to shrink, split, or simplify:
  - `src/packaged_desktop_smoke.py`
  - `src/ship/desktop_update.py`
- Interfaces or contracts to formalize:
  - root smoke runner is the CLI + monkeypatch surface
  - root updater module is the public import + monkeypatch surface
- Existing abstractions to reuse:
  - current packaged smoke helper function boundaries
  - current updater `DesktopUpdatePaths` / `DesktopUpdateService` contracts
- New abstraction to avoid unless proven necessary:
  - a new dependency injection framework or a second public facade layer

## Verification

- Cheapest syntax/check step:
  - `python -m py_compile src/packaged_desktop_smoke.py src/ship/packaged_smoke/build_env.py src/ship/packaged_smoke/runtime.py src/ship/packaged_smoke/rehearsals.py src/ship/desktop_update.py src/ship/desktop_update_shared.py src/ship/desktop_update_state.py src/ship/desktop_update_service.py`
- Cheapest focused test step:
  - `python -m pytest tests/packaged_desktop/ tests/test_desktop_update.py -q`
- Broader verification required only if:
  - release-facing docs drift, updater helper handoff wiring changes, or desktop launcher integration is affected

## Acceptance Criteria

- `src/packaged_desktop_smoke.py` stays as the executable CLI and root patch surface while delegating implementation to helper modules
- `src/ship/desktop_update.py` stays as the stable updater module while delegating implementation to helper modules
- No packaged rehearsal or updater behavior regression
- AI routing docs point future edits to the helper modules instead of the monoliths
- Future AI or human editors can find the right packaged/updater implementation file in one or two searches

## Notes

- Root-module monkeypatch compatibility is intentionally preserved even though it leaves a temporary indirection seam in place.
- This pass is structural polish, not a behavior redesign.

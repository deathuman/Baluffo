# Narrow Refactor Charter

## Title

Desktop update cross-stack debloat

## Goal

Reduce token waste and AI drift in the desktop update lane by shrinking the stable root surfaces on both sides of the stack: the portable updater helper executable and the Jobs page desktop-update UI. Keep the current updater behavior, route signatures, and install semantics intact while moving implementation ownership into smaller helper modules and updating the canonical docs to point future edits at the right leaves.

## Target Boundary

- Primary subsystem: portable desktop updater helper executable and Jobs desktop-update UI/runtime
- Entry file(s): `src/ship/desktop_updater.py`, `frontend/jobs/app/desktop-update.js`
- Ownership boundary being clarified: stable root patch/export surfaces vs helper leaves for updater UI/release/install logic and Jobs desktop-update model/DOM/controller logic
- What becomes easier after this change: future updater or Jobs update-UI edits can land in one helper module instead of re-expanding the executable root or UI compatibility surface

## Why Now

- Current pain: `src/ship/desktop_updater.py` and `frontend/jobs/app/desktop-update.js` had become large enough that routing docs were no longer truthful and the updater lane was absorbing too much context
- Why this is worth doing now: desktop update behavior spans packaging, bridge routes, release verification, and the Jobs runtime, so keeping the ownership map crisp pays off quickly
- Why this should stay narrow: the pass is structural only and preserves updater behavior, route signatures, release/install semantics, and current UI CTA/state transitions

## In Scope

- Split updater helper executable logic behind `src/ship/desktop_updater.py`
- Split Jobs desktop-update UI logic behind `frontend/jobs/app/desktop-update.js`
- Update canonical docs to route updater and desktop-update work to the new leaves
- Add thin-surface guardrails for the backend executable root and the frontend export root

## Out of Scope

- Any `src/ship/update_manager.py` behavior change
- Any `/app/*update*` route signature or payload change
- Any package.json, release-policy, or installer UX redesign
- Any unrelated bridge, pipeline, or desktop runtime refactor

## Stability Impact

- Runtime behavior touched: updater helper UI/diagnostics flow, release recovery helpers, install/relaunch verification flow, and Jobs desktop-update controller wiring
- Persisted state touched: none semantically; updater state files and install-plan semantics stay the same
- Packaging or desktop behavior touched: yes, but only through compatibility-preserving helper splits inside the existing updater lane
- Compatibility concern: `src.ship.desktop_updater` remains the helper executable/test patch surface, and `frontend/jobs/app/desktop-update.js` remains the stable import surface for Jobs runtime/tests
- Rollback trigger: any regression in updater helper install/relaunch recovery, route-driven desktop update flows, or Jobs desktop-update CTA/state rendering

## AI Accessibility Impact

- Source-of-truth file after refactor: `docs/AI_ASSISTANT_GUIDE.md` and `docs/architecture-ai-map.md` for routing, `docs/RELEASE.md` for updater/release ownership, `docs/admin-bridge-api.md` for route surface
- Expected search path for future edits: helper leaves first, root surfaces only for compatibility work
- Docs or registry to update: `AI_ASSISTANT_GUIDE.md`, `architecture-ai-map.md`, `INDEX.md`, `RELEASE.md`, `admin-bridge-api.md`, Serena `routing_and_boundaries`
- Any transitional seam being kept temporarily: the root-backed patch seams on `src/ship/desktop_updater.py` and the thin re-export root `frontend/jobs/app/desktop-update.js`

## Implementation Shape

- Modules to shrink, split, or simplify: `src/ship/desktop_updater.py`, `frontend/jobs/app/desktop-update.js`
- Interfaces or contracts to formalize: updater helper root patch seams and the Jobs desktop-update export surface
- Existing abstractions to reuse: `src/ship/update_manager.py`, `src/ship/desktop_update.py`, bridge app-update routes, and the existing Jobs desktop-update unit tests
- New abstraction to avoid unless proven necessary: any extra root facade or barrel layer beyond the named helper leaves

## Verification

- Cheapest syntax/check step: `python -m py_compile src/ship/desktop_updater.py src/ship/desktop_updater_*.py` and `node --check frontend/jobs/app/desktop-update.js frontend/jobs/app/desktop-update-*.js`
- Cheapest focused test step: `python -m pytest tests/test_desktop_updater.py tests/test_desktop_update.py -q` plus `node --test tests/frontend/unit/jobs-desktop-update.test.mjs`
- Broader verification required only if: updater route wiring, packaged rehearsal flows, or release/docs guardrails regress

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

- `src/ship/update_manager.py` remains the canonical updater-capability owner.
- `src/ship/desktop_updater.py` stays intentionally patch-friendly so the existing updater tests do not need new targets.
- `frontend/jobs/app/desktop-update.js` stays intentionally tiny and stable so Jobs runtime imports and unit tests do not need to move.

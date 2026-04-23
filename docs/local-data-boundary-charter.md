# Narrow Refactor Charter

## Title

Local-data boundary cleanup and contract truthfulness

## Goal

Reduce token waste and AI drift in the local-data stack by shrinking the stable compatibility roots, moving implementation ownership into focused helper modules, and updating the canonical docs so they describe the real persisted and runtime contracts instead of the old monolith layout.

## Target Boundary

- Primary subsystem: desktop local-data storage and shared desktop local-data runtime
- Entry file(s): `src/local_data_store.py`, `frontend/shared/local-data/desktop-client.js`
- Ownership boundary being clarified: root compatibility surfaces vs helper modules for profiles, saved jobs, attachments, backup, navigation, lifecycle, and API wiring
- What becomes easier after this change: future edits can find the right local-data owner in one or two searches without expanding the root facades

## Why Now

- Current pain: `src/local_data_store.py` and `frontend/shared/local-data/desktop-client.js` had become large enough that docs were drifting behind the code and AI routing was getting noisy
- Why this is worth doing now: local-data is a cross-stack contract surface that shows up in desktop flows, bridge routes, saved-job behavior, and packaged rehearsals
- Why this should stay narrow: the pass is structural only and preserves route signatures, saved-job behavior, and backup/import-export semantics

## In Scope

- Split backend local-data storage helpers behind `src/local_data_store.py`
- Split shared desktop local-data runtime helpers behind `frontend/shared/local-data/desktop-client.js`
- Align canonical docs and route/schema validation with the real local-data contracts
- Add thin-root guardrails for the backend facade and shared desktop runtime root

## Out of Scope

- New local-data product behavior
- Route signature changes for `/desktop-local-data/*`
- Backup format version changes
- Any unrelated bridge, updater, pipeline, or frontend feature refactors

## Stability Impact

- Runtime behavior touched: local desktop sign-in, saved jobs, attachments, activity, backup/import-export, and desktop lifecycle wiring
- Persisted state touched: none semantically; this pass documents and validates the existing persisted shapes
- Packaging or desktop behavior touched: only through compatibility-preserving local-data imports and rehearsals
- Compatibility concern: `src.local_data_store` and `frontend/shared/local-data/desktop-client.js` remain stable patch/import surfaces
- Rollback trigger: any regression in desktop sign-in, saved-job mutation flows, backup hydration, or packaged rehearsal local-data usage

## AI Accessibility Impact

- Source-of-truth file after refactor: `docs/DATA_CONTRACT.md` for local-data shapes, `docs/admin-bridge-api.md` for route surface, `docs/AI_ASSISTANT_GUIDE.md` and `docs/architecture-ai-map.md` for edit routing
- Expected search path for future edits: helper modules first, root facades only for compatibility-surface work
- Docs or registry to update: `DATA_CONTRACT.md`, `AI_ASSISTANT_GUIDE.md`, `architecture-ai-map.md`, `INDEX.md`, `LOCAL_SETUP.md`, `admin-bridge-api.md`, `runtime-first-cleanup-handoff.md`, Serena `routing_and_boundaries`
- Any transitional seam being kept temporarily: `frontend/local-data/services.js` stays the transitional local-data boundary for feature slices

## Implementation Shape

- Modules to shrink, split, or simplify: `src/local_data_store.py`, `frontend/shared/local-data/desktop-client.js`
- Interfaces or contracts to formalize: persisted saved-job rows, activity rows, attachment rows, backup payload v2, and the stable JS runtime method surface
- Existing abstractions to reuse: `frontend/local-data/runtime-contract.js`, bridge route handlers, packaged rehearsal imports, and existing local-data normalization rules
- New abstraction to avoid unless proven necessary: any extra facade or barrel layer beyond the helper modules named in this charter

## Verification

- Cheapest syntax/check step: `python -m py_compile src/local_data_store.py src/local_data_store_*.py src/core/schemas.py src/bridge/routes/get_routes.py` and `node --check frontend/shared/local-data/desktop-client.js frontend/shared/local-data/desktop/*.js`
- Cheapest focused test step: `python -m pytest tests/test_local_data_store.py tests/bridge/test_routes_get.py tests/bridge/test_routes_post.py -q` plus the local-data frontend unit batch
- Broader verification required only if: packaged rehearsal local-data imports or desktop lifecycle behavior regress

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

- `src.local_data_store` remains a stable compatibility surface for bridge, packaged smoke, and tests.
- `frontend/shared/local-data/desktop-client.js` remains the stable desktop-local runtime bootstrap and `window.JobAppLocalData` surface.
- `frontend/local-data/services.js` remains intentionally transitional so feature slices do not import the shared desktop runtime directly.

# Admin Bridge Boundary Cleanup

## Goal

Thin `src/admin_bridge.py` into a stable entrypoint and compatibility surface while moving runtime/path rebinding, cached service builders, manual-source glue, and task/runtime helpers under `src/bridge/*`. The refactor must preserve bridge route signatures, task launch/live-status behavior, manual-source/source-check flows, owner-session shutdown semantics, and the existing root monkeypatch surface used by tests and scripts.

## Target Boundary

- Primary subsystem: admin bridge startup, runtime wiring, and compatibility wrappers
- Entry file(s): `src/admin_bridge.py`, `src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py`
- Ownership boundary being clarified: bridge entrypoint and patch surface vs. bridge leaf-service implementation
- What becomes easier after this change: route compatibility work, task/live-status fixes, and manual-source flow edits can be found in one or two focused bridge modules instead of a monolith

## Why Now

- Current pain: `src/admin_bridge.py` still concentrates service construction, runtime-path rebinding, manual-source glue, and task/runtime helpers alongside stable wrappers
- Why this is worth doing now: admin bridge is still one of the largest backend compatibility surfaces and its test coverage was concentrated in one very large runtime file
- Why this should stay narrow: bridge payloads and startup behavior are compatibility-sensitive, so the goal is structural clarity without endpoint or lifecycle churn

## In Scope

- Split admin bridge runtime/config/session helpers into focused `src/bridge/admin_entrypoint_*` modules
- Keep `src/admin_bridge.py` as the stable route-facing and monkeypatch-safe surface
- Replace `tests/admin/test_admin_bridge_ops_runtime.py` with focused runtime shard files
- Update AI-routing docs and suite-contract guardrails for the new bridge boundary

## Out of Scope

- Route signature or payload changes
- Frontend feature work or broad admin UI refactors
- Packaging, updater, or release-path changes

## Stability Impact

- Runtime behavior touched: bridge startup/config, manual-source/source-check glue, task/live-status wrappers, owner-session exit checks
- Persisted state touched: existing bridge runtime/report/history files only through unchanged contracts
- Packaging or desktop behavior touched: desktop owner-session lifecycle is preserved; no packaging flow change
- Compatibility concern: tests and scripts patch `src.admin_bridge` names directly, so root wrappers must stay present and patch-safe
- Rollback trigger: any change in bridge payloads, task start/busy/log-polling behavior, or manual-source/source-check outcomes

## AI Accessibility Impact

- Source-of-truth file after refactor: `src/admin_bridge.py` for stable entrypoints, `src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py` for implementation ownership
- Expected search path for future edits: AI guide / architecture map -> focused bridge helper module -> `src/admin_bridge.py` only if the root compatibility seam itself must change
- Docs or registry to update: `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, `docs/INDEX.md`, Serena `routing_and_boundaries`
- Any transitional seam being kept temporarily: root wrapper functions and root-bound helper-module patch seams

## Implementation Shape

- Modules to shrink, split, or simplify: `src/admin_bridge.py`, `tests/admin/test_admin_bridge_ops_runtime.py`
- Interfaces or contracts to formalize: stable root wrappers, root-bound helper module ownership, runtime-test shard layout
- Existing abstractions to reuse: `src/bridge/config.py`, `bootstrap.py`, `ops_api.py`, `task_launch_api.py`, `sync_service.py`, `registry_service.py`, `discovery_service.py`, `pipeline_service.py`
- New abstraction to avoid unless proven necessary: new bridge composition roots or duplicate route/service facades

## Verification

- Cheapest syntax/check step: `python -m py_compile src/admin_bridge.py src/bridge/admin_entrypoint_runtime.py src/bridge/admin_entrypoint_services.py src/bridge/admin_registry_api.py src/bridge/admin_task_runtime.py`
- Cheapest focused test step: `python -m pytest tests/admin/ -q`
- Broader verification required only if: bridge route docs, suite guardrails, or frontend admin live-status contracts move

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

The root `src/admin_bridge.py` module remains both the bridge startup entrypoint and the monkeypatch surface used by tests. Helper modules are intentionally root-bound rather than imported directly by callers.

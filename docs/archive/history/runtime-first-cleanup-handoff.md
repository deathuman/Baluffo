# Runtime-First Cleanup Handoff

> Operational handoff note for the runtime-first cleanup lane after the admin, jobs/saved, `source_sync`, `admin_bridge`, packaged-smoke, jobs-fetcher, admin-ops, and source-discovery generator/reporting follow-up waves landed on `main`.
> This is a practical pickup document, not a canonical contract doc and not part of the default AI read path.

## Purpose

Use this note to resume older cleanup context without rediscovering what already landed, which regressions were found during the follow-up waves, and which debt was intentionally deferred at that time. The current stop-point for the cleanup campaign is [`final-leaf-closeout-program.md`](final-leaf-closeout-program.md); treat this handoff as sequencing history behind that end-state tracker.

## Current Status

As of April 23, 2026, the first runtime-first cleanup wave, the admin follow-up wave, the jobs/saved runtime follow-up wave, the backend `source_sync` and `admin_bridge` follow-up waves, the packaged smoke follow-up wave, the `jobs_fetcher` follow-up wave, the admin ops live-payload follow-up wave, the source-discovery generator/reporting follow-up wave, and the final leaf-closeout wave are the current merged cleanup state for `main`. This handoff note remains useful as historical sequencing context, but the canonical routing docs stay `AI_ASSISTANT_GUIDE.md`, `architecture-ai-map.md`, `INDEX.md`, and the final closeout tracker.

This follow-up landed:

- `src/source_discovery/gamesmap.py`, `src/source_discovery/reporting.py`, and `src/source_discovery/web_search.py` are now stable thin compatibility surfaces backed by `gamesmap_{cache,parsing,candidates}.py`, `reporting_{progress,candidates,backlog}.py`, and `web_search_{fetch,extract,candidates}.py`.
- `src/source_sync.py` is now back to a thin stable sync facade under the root budget while preserving the current import surface for bridge, packaged smoke, and test callers.
- `src/source_sync_snapshot.py` now owns snapshot transition backfill, canonicalization, and merge-ranking helpers directly instead of calling those helpers back through the root surface.
- `src/source_sync.py` no longer carries dead duplicated ASN.1/PEM parser helpers; PEM parsing and JWT-signing internals stay owned by `src/source_sync_crypto.py`.
- `tests/test_suite_contract.py` now enforces the `source_sync` thin-root budget, required leaf imports, root clock-helper exposure, and snapshot-helper ownership.
- `frontend/admin/render.js` is now a thin stable re-export surface for leaf render modules under `frontend/admin/render/`.
- `frontend/admin/domain.js` is now a thin stable re-export surface for leaf domain modules under `frontend/admin/domain/`.
- `frontend/admin/app/runtime.js` now stays under the entrypoint budget and delegates controller assembly to `frontend/admin/app/runtime/composition.js`, while keeping boot/state/event-binding wrappers on the stable root.
- `frontend/admin/app/fetcher.js` and `frontend/admin/app/discovery.js` now keep their stable controller/export surfaces while delegating preset/log/report/watch responsibilities into leaf modules under `frontend/admin/app/fetcher/` and `frontend/admin/app/discovery/`.
- `frontend/admin/app/registry.js` is now a stable controller root backed by `frontend/admin/app/registry/{ui,load,mutations}.js`.
- `frontend/admin/app/ops.js` is now a stable controller root backed by `frontend/admin/app/ops/{format,task-state,health,bridge-status}.js`.
- `frontend/jobs/app/runtime.js` is now a stable page-entry root backed by `frontend/jobs/app/runtime/{composition,boot,page-flow}.js`, while existing leaf controllers under `frontend/jobs/app/runtime/` remain the owning targets for page behavior.
- `frontend/saved/app/runtime.js` is now a stable page-entry/export root backed by `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes}.js`, alongside the existing saved runtime controllers.
- `tests/frontend/unit/structure-cleanup.test.mjs` now enforces the tighter admin runtime budget plus jobs/saved runtime budgets and root-shape assertions for the delegated composition/leaf boundaries.
- `src/admin_bridge.py` is now a thinner stable bridge entry/export root backed by `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`.
- `src/bridge/admin_entrypoint_api.py` now owns the `build_bridge_api(...)` dependency graph instead of keeping that large bootstrap assembly in the root.
- `tests/test_suite_contract.py` now enforces the lower `admin_bridge.py` root budget and prevents the runtime/session, registry, service-getter, and sync/task alias families from drifting back into root-owned defs.

No user-facing UI, route, payload, or persisted data contracts changed in this wave.

## Regression Found During Follow-Up

The follow-up surfaced one compatibility regression from the earlier `source_sync.py` thinning:

- `src/source_sync.py` had stopped exposing `now_iso`, even though extracted sync leaves still called the root helper surface.

That regression is fixed on `main` and covered by the suite-contract test noted above.

## Verification Baseline

Verified in this follow-up session:

- `python -m pytest tests/admin/test_admin_bridge_runtime_config.py tests/admin/test_admin_bridge_task_launch.py tests/admin/test_admin_bridge_report_history.py tests/admin/test_admin_bridge_ops_health.py tests/admin/test_admin_bridge_live_payloads.py tests/admin/test_admin_bridge_thin_wrappers.py tests/admin/test_admin_bridge_ops_sync.py tests/test_suite_contract.py -q`
- `npm run lint:precommit:changed`

## Deferred Follow-Up Order

If another cleanup wave continues from here, keep the order narrow and compatibility-first:

1. Pick the next cleanup hotspot fresh from the current compatibility-surface backlog; `src/jobs_fetcher.py` is already landed and should not be treated as the next pending wave.
2. Preserve the new `admin_bridge` ownership split: entrypoint/runtime/session glue in `src/bridge/admin_entrypoint_runtime.py`, service/cache builders in `src/bridge/admin_entrypoint_services.py`, bridge bootstrap assembly in `src/bridge/admin_entrypoint_api.py`, registry/manual-source flow in `src/bridge/admin_registry_api.py`, and sync/task runtime flow in `src/bridge/admin_task_runtime.py`.
3. Preserve the new `source_sync` ownership split: config in `src/source_sync_config.py`, runtime/auth/request state in `src/source_sync_runtime.py`, snapshot normalization/merge in `src/source_sync_snapshot.py`, and PEM/JWT internals in `src/source_sync_crypto.py`.
4. Continue backend helper dedup only where behavior is truly identical and existing root/module boundaries stay intact.
5. Preserve the new frontend leaf boundaries if more cleanup continues: `frontend/jobs/app/runtime/{composition,boot,page-flow}.js`, `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes}.js`, and the admin `runtime/`, `registry/`, `ops/`, `fetcher/`, and `discovery/` leaves.
6. Do not reopen jobs/saved `types.js` extraction in the near term; the remaining ROI is low after the typedef cleanup already landed.

## Resume Checklist

On another machine:

1. Start from `main` or a descendant of the April 22, 2026 cleanup follow-up state; do not look for an unmerged "current working tree" version of this work.
2. Verify Python, Node, and repo test tooling are available.
3. Review `git status --short` before making new changes.
4. Re-run the cleanup verification baseline before extending the cleanup further:
   - `python -m pytest tests/admin/test_admin_bridge_runtime_config.py tests/admin/test_admin_bridge_task_launch.py tests/admin/test_admin_bridge_report_history.py tests/admin/test_admin_bridge_ops_health.py tests/admin/test_admin_bridge_live_payloads.py tests/admin/test_admin_bridge_thin_wrappers.py tests/admin/test_admin_bridge_ops_sync.py tests/test_suite_contract.py -q`
   - `npm run lint:precommit:changed`

## Related Docs

- [`AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md)
- [`architecture-ai-map.md`](../../architecture-ai-map.md)
- [`final-leaf-closeout-program.md`](final-leaf-closeout-program.md)
- [`testing.md`](../../testing.md)
- [`../tools/mcp/SERENA.md`](../../../tools/mcp/SERENA.md)

## Former Deferred Specialized Owners

This handoff is historical. The five roots that were intentionally deferred here were
later split into focused leaf modules and are now compatibility facades under 500 LOC:

- `src/source_registry.py`
- `src/ship/update_manager.py`
- `src/jobs/adapters/social_parsers.py`
- `src/source_discovery/core.py`
- `src/ship/packaged_smoke/runtime.py`

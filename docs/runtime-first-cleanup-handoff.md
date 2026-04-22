# Runtime-First Cleanup Handoff

> Operational handoff note for the runtime-first cleanup lane after the admin and jobs/saved follow-up waves landed on `main`.
> This is a practical pickup document, not a canonical contract doc and not part of the default AI read path.

## Purpose

Use this note to resume later cleanup work without rediscovering what is already merged, which regressions were found during the follow-up, and which debt remains intentionally deferred.

## Current Status

As of April 22, 2026, the first runtime-first cleanup wave, the admin follow-up wave, and the jobs/saved runtime follow-up wave are already merged to remote `main`. This handoff note is still relevant because it records the landed cleanup state and the intentionally deferred next order; it is not replaced by the canonical routing docs.

This follow-up landed:

- `src/source_sync.py` remains the stable sync facade and again re-exports `now_iso` from `src.shared.utils` so extracted leaves keep working through the root surface.
- `tests/test_suite_contract.py` now locks that root-clock-helper exposure without depending on a brittle exact import line.
- `frontend/admin/render.js` is now a thin stable re-export surface for leaf render modules under `frontend/admin/render/`.
- `frontend/admin/domain.js` is now a thin stable re-export surface for leaf domain modules under `frontend/admin/domain/`.
- `frontend/admin/app/runtime.js` now stays under the entrypoint budget and delegates controller assembly to `frontend/admin/app/runtime/composition.js`, while keeping boot/state/event-binding wrappers on the stable root.
- `frontend/admin/app/fetcher.js` and `frontend/admin/app/discovery.js` now keep their stable controller/export surfaces while delegating preset/log/report/watch responsibilities into leaf modules under `frontend/admin/app/fetcher/` and `frontend/admin/app/discovery/`.
- `frontend/admin/app/registry.js` is now a stable controller root backed by `frontend/admin/app/registry/{ui,load,mutations}.js`.
- `frontend/admin/app/ops.js` is now a stable controller root backed by `frontend/admin/app/ops/{format,task-state,health,bridge-status}.js`.
- `frontend/jobs/app/runtime.js` is now a stable page-entry root backed by `frontend/jobs/app/runtime/{composition,boot,page-flow}.js`, while existing leaf controllers under `frontend/jobs/app/runtime/` remain the owning targets for page behavior.
- `frontend/saved/app/runtime.js` is now a stable page-entry/export root backed by `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes}.js`, alongside the existing saved runtime controllers.
- `tests/frontend/unit/structure-cleanup.test.mjs` now enforces the tighter admin runtime budget plus jobs/saved runtime budgets and root-shape assertions for the delegated composition/leaf boundaries.

No user-facing UI, route, payload, or persisted data contracts changed in this wave.

## Regression Found During Follow-Up

The follow-up surfaced one compatibility regression from the earlier `source_sync.py` thinning:

- `src/source_sync.py` had stopped exposing `now_iso`, even though extracted sync leaves still called the root helper surface.

That regression is fixed on `main` and covered by the suite-contract test noted above.

## Verification Baseline

Verified in this follow-up session:

- `node --test --test-reporter=dot tests/frontend/unit/jobs-runtime-events.test.mjs tests/frontend/unit/jobs-runtime-feed-controller.test.mjs tests/frontend/unit/jobs-runtime-jobs-list-events.test.mjs tests/frontend/unit/jobs-runtime-list-view.test.mjs tests/frontend/unit/jobs-runtime-query.test.mjs tests/frontend/unit/jobs-runtime-startup-preview.test.mjs tests/frontend/unit/jobs-runtime-state.test.mjs tests/frontend/unit/jobs-runtime-url-persistence.test.mjs tests/frontend/unit/saved-runtime-controllers.test.mjs tests/frontend/unit/saved-admin-bridge-state.test.mjs tests/frontend/unit/saved-phase-time.test.mjs tests/frontend/unit/saved-timeline.test.mjs tests/frontend/unit/structure-cleanup.test.mjs`
- `npm run test:unit`
- `npm run lint:precommit:changed`

## Deferred Follow-Up Order

If another cleanup wave continues from here, keep the order narrow and compatibility-first:

1. Keep backend compat-barrel work deferred to the next dedicated wave, especially `src/jobs_fetcher.py` and any deeper `src/source_sync.py` thinning beyond the stable facade.
2. Continue backend helper dedup only where behavior is truly identical and existing root/module boundaries stay intact.
3. Preserve the new frontend leaf boundaries if more cleanup continues: `frontend/jobs/app/runtime/{composition,boot,page-flow}.js`, `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes}.js`, and the admin `runtime/`, `registry/`, `ops/`, `fetcher/`, and `discovery/` leaves.
4. Do not reopen jobs/saved `types.js` extraction in the near term; the remaining ROI is low after the typedef cleanup already landed.

## Resume Checklist

On another machine:

1. Start from `main` or a descendant of the April 22, 2026 cleanup follow-up state; do not look for an unmerged “current working tree” version of this work.
2. Verify Python, Node, and repo test tooling are available.
3. Review `git status --short` before making new changes.
4. Re-run the cleanup verification baseline before extending the cleanup further:
   - `node --test --test-reporter=dot tests/frontend/unit/jobs-runtime-events.test.mjs tests/frontend/unit/jobs-runtime-feed-controller.test.mjs tests/frontend/unit/jobs-runtime-jobs-list-events.test.mjs tests/frontend/unit/jobs-runtime-list-view.test.mjs tests/frontend/unit/jobs-runtime-query.test.mjs tests/frontend/unit/jobs-runtime-startup-preview.test.mjs tests/frontend/unit/jobs-runtime-state.test.mjs tests/frontend/unit/jobs-runtime-url-persistence.test.mjs tests/frontend/unit/saved-runtime-controllers.test.mjs tests/frontend/unit/saved-admin-bridge-state.test.mjs tests/frontend/unit/saved-phase-time.test.mjs tests/frontend/unit/saved-timeline.test.mjs tests/frontend/unit/structure-cleanup.test.mjs`
   - `npm run test:unit`
   - `npm run lint:precommit:changed`

## Related Docs

- [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md)
- [`architecture-ai-map.md`](architecture-ai-map.md)
- [`testing.md`](testing.md)
- [`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md)

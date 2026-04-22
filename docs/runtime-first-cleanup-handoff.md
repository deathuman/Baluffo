# Runtime-First Cleanup Handoff

> Operational handoff note for the runtime-first cleanup lane after the first wave and this follow-up wave landed on `main`.
> This is a practical pickup document, not a canonical contract doc and not part of the default AI read path.

## Purpose

Use this note to resume later cleanup work without rediscovering what is already merged, which regressions were found during the follow-up, and which debt remains intentionally deferred.

## Current Status

As of April 22, 2026, the first runtime-first cleanup wave is already merged to remote `main`. This follow-up wave stayed focused on historical debt reduction, stable root preservation, and guardrails against regrowth.

This follow-up landed:

- `src/source_sync.py` remains the stable sync facade and again re-exports `now_iso` from `src.shared.utils` so extracted leaves keep working through the root surface.
- `tests/test_suite_contract.py` now locks that root-clock-helper exposure without depending on a brittle exact import line.
- `frontend/admin/render.js` is now a thin stable re-export surface for leaf render modules under `frontend/admin/render/`.
- `frontend/admin/domain.js` is now a thin stable re-export surface for leaf domain modules under `frontend/admin/domain/`.
- `frontend/admin/app/runtime.js` now stays under the entrypoint budget and delegates controller assembly to `frontend/admin/app/runtime/composition.js`, while keeping boot/state/event-binding wrappers on the stable root.
- `frontend/admin/app/fetcher.js` and `frontend/admin/app/discovery.js` now keep their stable controller/export surfaces while delegating preset/log/report/watch responsibilities into leaf modules under `frontend/admin/app/fetcher/` and `frontend/admin/app/discovery/`.
- `frontend/admin/app/registry.js` is now a stable controller root backed by `frontend/admin/app/registry/{ui,load,mutations}.js`.
- `frontend/admin/app/ops.js` is now a stable controller root backed by `frontend/admin/app/ops/{format,task-state,health,bridge-status}.js`.
- `tests/frontend/unit/structure-cleanup.test.mjs` now enforces the tighter admin runtime budget plus root budgets/shape assertions for `frontend/admin/app/{registry,ops}.js` and the delegated runtime-composition boundary.

No user-facing UI, route, payload, or persisted data contracts changed in this wave.

## Regression Found During Follow-Up

The follow-up surfaced one compatibility regression from the earlier `source_sync.py` thinning:

- `src/source_sync.py` had stopped exposing `now_iso`, even though extracted sync leaves still called the root helper surface.

That regression is fixed on `main` and covered by the suite-contract test noted above.

## Verification Baseline

Verified in this follow-up session:

- `node --test --test-reporter=dot tests/frontend/unit/admin-registry-controller.test.mjs tests/frontend/unit/admin-ops-controller.test.mjs tests/frontend/unit/admin-auth-controller.test.mjs tests/frontend/unit/admin-live-task-restore.test.mjs tests/frontend/unit/admin-fetcher-controller.test.mjs tests/frontend/unit/admin-discovery-controller.test.mjs tests/frontend/unit/admin-domain.test.mjs tests/frontend/unit/admin-render-log.test.mjs tests/frontend/unit/structure-cleanup.test.mjs`
- `npm run test:unit`
- `npm run lint:precommit:changed`

## Deferred Follow-Up Order

If another cleanup wave continues from here, keep the order narrow and compatibility-first:

1. Keep backend compat-barrel work deferred to the next dedicated wave, especially `src/jobs_fetcher.py` and any deeper `src/source_sync.py` thinning beyond the stable facade.
2. Continue backend helper dedup only where behavior is truly identical and existing root/module boundaries stay intact.
3. If the admin slice needs another pass, preserve the new `runtime/`, `registry/`, `ops/`, `fetcher/`, and `discovery/` leaf boundaries and keep the stable roots thin.
4. Do not reopen jobs/saved `types.js` extraction in the near term; the remaining ROI is low after the typedef cleanup already landed.

## Resume Checklist

On another machine:

1. Start from `main` or a descendant of the April 22, 2026 cleanup follow-up state; do not look for an unmerged “current working tree” version of this work.
2. Verify Python, Node, and repo test tooling are available.
3. Review `git status --short` before making new changes.
4. Re-run the cleanup verification baseline before extending the cleanup further:
   - `node --test --test-reporter=dot tests/frontend/unit/admin-registry-controller.test.mjs tests/frontend/unit/admin-ops-controller.test.mjs tests/frontend/unit/admin-auth-controller.test.mjs tests/frontend/unit/admin-live-task-restore.test.mjs tests/frontend/unit/admin-fetcher-controller.test.mjs tests/frontend/unit/admin-discovery-controller.test.mjs tests/frontend/unit/admin-domain.test.mjs tests/frontend/unit/admin-render-log.test.mjs tests/frontend/unit/structure-cleanup.test.mjs`
   - `npm run test:unit`
   - `npm run lint:precommit:changed`

## Related Docs

- [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md)
- [`architecture-ai-map.md`](architecture-ai-map.md)
- [`testing.md`](testing.md)
- [`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md)

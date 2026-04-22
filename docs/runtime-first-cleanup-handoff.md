# Runtime-First Cleanup Handoff

> Operational handoff note for resuming the current runtime-first cleanup lane on another machine.
> This is a practical pickup document, not a canonical contract doc and not part of the default AI read path.

## Purpose

Use this note to resume the current cleanup wave safely from a different machine without rediscovering the same context, verification baseline, or priority order.

## Current Status

The current working tree already contains the first runtime-first cleanup wave. The goal of this pass is line reduction and boundary cleanup without changing user-facing behavior, payloads, or persisted contracts.

What has already landed in the worktree:

- `src/source_sync.py` was thinned back into a compatibility facade.
- `src/source_sync_runtime.py` was added to hold sync runtime/auth/rate-limit state and request helpers behind the stable root.
- `src/shared/json_io.py` was added for tolerant JSON reads used by matching backend call sites.
- Shared helper dedup landed for `parse_iso`, tolerant JSON reads, and simple int-or-default coercion where semantics matched.
- `frontend/admin/app/fetcher-summary.js` was added to pull pure fetcher summary/retry helpers out of the main controller file.
- `frontend/jobs/app/runtime.js` and `frontend/saved/app/runtime.js` were trimmed by removing unused local typedef blocks and repeated bridge-base constants.

## Remaining Work

Continue in this order:

1. Finish the admin frontend cleanup cluster, especially `frontend/admin/render.js` and `frontend/admin/app/runtime.js`.
2. Decide whether jobs/saved `types.js` extraction is still worth doing now that the unused typedef blocks were removed.
3. Continue backend helper dedup only where behavior is truly identical.
4. Defer `src/jobs_fetcher.py` compat-barrel pruning to the next cleanup wave.

## Verified Baseline

Verified in this session:

- Targeted backend cleanup tests passed:
  - `python -m pytest tests/test_source_sync.py tests/admin/test_admin_bridge_ops_sync.py tests/admin/test_admin_bridge_thin_wrappers.py -q`
- Targeted frontend cleanup coverage passed.
- `npm run test:unit` passed.
- `npm run lint:precommit:changed` passed.

Broad Python-suite note:

- `npm run test:py` is not a blocker for this handoff note.
- The remaining failure observed in the broad run was `tests/test_runtime_launcher.py::test_build_site_request_handler_traces_probe_requests`.
- That test passed when rerun in isolation, so treat it as unrelated/flaky unless new evidence says otherwise.

## Known Caveats

- `data/source-approval-state.json` is only a newline-normalization diff from the verification tooling.
- `tools/mcp/SERENA.md` was updated separately and should remain intact.

## Resume Checklist

On another machine:

1. Restore the correct branch and working tree.
2. Verify Python, Node, and repo test tooling are available.
3. Review `git status --short` before making new changes.
4. Re-run the focused cleanup verification commands before continuing:
   - `python -m pytest tests/test_source_sync.py tests/admin/test_admin_bridge_ops_sync.py tests/admin/test_admin_bridge_thin_wrappers.py -q`
   - `npm run test:unit`
   - `npm run lint:precommit:changed`

## Related Docs

- [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md)
- [`architecture-ai-map.md`](architecture-ai-map.md)
- [`testing.md`](testing.md)
- [`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md)

# Static Adapter Boundary Charter

> Historical/planning record for a focused cleanup lane. Start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) and [`architecture-ai-map.md`](architecture-ai-map.md) before using this document for lane-specific compatibility context.

Use this tracker for the static adapter modularization pass.

## Title

Static Adapter Boundary Cleanup

## Goal

Reduce `src/jobs/adapters/static.py` from a single large production monolith into a stable root surface over focused helper modules without changing loader names, report payloads, browser-fallback behavior, or static-source extraction outcomes. Keep `src.jobs.adapters.static` as the public adapter entrypoint and keep `src.jobs_fetcher` compatibility behavior intact.

## Target Boundary

- Primary subsystem: Static source orchestration, listing fetch/candidate extraction, detail traversal, shard and loader registration
- Entry file(s): `src/jobs/adapters/static.py`, `src/jobs_fetcher.py`
- Ownership boundary being clarified: runtime/context state, listing flow, detail traversal, loader/shard plumbing
- What becomes easier after this change: targeted static-source fixes, lower AI token cost, safer edits to listing vs detail logic, faster source-specific debugging

## Why Now

- Current pain: `src/jobs/adapters/static.py` mixed runtime setup, plugin fast-path logic, generic listing extraction, detail traversal, and loader registration in one file
- Why this is worth doing now: the app is stable end-to-end and the static adapter is still the largest remaining production monolith
- Why this should stay narrow: static-source behavior is high-churn and heavily tested, so preserving contracts matters more than chasing perfect abstraction

## In Scope

- Create a repo-tracked charter for the static adapter cleanup lane
- Extract explicit runtime and per-source context into `src/jobs/adapters/static_runtime.py`
- Extract plugin fast-path and generic listing flow into `src/jobs/adapters/static_listing.py`
- Extract detail traversal into `src/jobs/adapters/static_detail.py`
- Extract shard and dynamic-loader plumbing into `src/jobs/adapters/static_sources.py`
- Keep `src/jobs/adapters/static.py` as the stable root surface and orchestration entrypoint
- Update AI routing docs for the new static helper ownership map

## Out of Scope

- Changing fetcher CLI flags, loader names, shard names, or report schema
- Changing provider/plugin registration or source-registry data contracts
- Refactoring discovery, admin bridge, Saved runtime, or packaging/updater code in this pass
- Broadly re-sharding `tests/jobs_static/test_static_source_execution.py`

## Stability Impact

- Runtime behavior touched: static listing fetch, JS-shell/browser fallback, detail traversal, shard routing, dynamic loader construction
- Persisted state touched: static source diagnostics and fetch report details only, behavior-preserving
- Packaging or desktop behavior touched: no
- Compatibility concern: callers and tests still enter through `src.jobs.adapters.static` and `src.jobs_fetcher`; some tests patch root `static.*` helpers directly
- Rollback trigger: any regression in `tests/jobs_static/`, `tests/test_jobs_fetcher.py`, `tests/test_jobs_fetcher_pipeline.py`, or `tests/test_jobs_fetcher_quality.py`

## AI Accessibility Impact

- Source-of-truth files after refactor:
  - `src/jobs/adapters/static_runtime.py`
  - `src/jobs/adapters/static_listing.py`
  - `src/jobs/adapters/static_detail.py`
  - `src/jobs/adapters/static_sources.py`
  - `src/jobs/adapters/static_helpers.py`
- Expected search path for future edits:
  - runtime/context state -> `static_runtime.py`
  - plugin fast path and listing extraction -> `static_listing.py`
  - detail traversal and adaptive stop behavior -> `static_detail.py`
  - shard and loader naming/wrappers -> `static_sources.py`
  - low-level fetch/detail heuristics -> `static_helpers.py`
- Docs or registry to update:
  - `docs/AI_ASSISTANT_GUIDE.md`
  - `docs/architecture-ai-map.md`
  - `docs/adapter-plugin-inventory.md`
  - `docs/scraping-pipeline.md`
  - `docs/INDEX.md`
- Transitional seam being kept temporarily:
  - listing and detail helpers resolve selected monkeypatchable functions through the root `src.jobs.adapters.static` module so existing root patches remain valid

## Implementation Shape

- Modules to shrink, split, or simplify:
  - `src/jobs/adapters/static.py`
- Interfaces or contracts to formalize:
  - `src.jobs.adapters.static` remains the stable root surface
  - `src.jobs_fetcher` remains the lazy compatibility surface
  - shard loader names and `static_source::...` dynamic loader naming stay unchanged
- Existing abstractions to reuse:
  - `static_helpers.py` runtime/detail utility functions
  - existing plugin registry and plugin metadata contract
  - current `fetch_pages_batched` and diagnostics/report flow
- New abstraction to avoid unless proven necessary:
  - new adapter frameworks or new public wrapper layers around the static adapter

## Verification

- Cheapest syntax/check step:
  - `python -m py_compile src/jobs/adapters/static.py src/jobs/adapters/static_runtime.py src/jobs/adapters/static_listing.py src/jobs/adapters/static_detail.py src/jobs/adapters/static_sources.py`
- Cheapest focused test step:
  - `python -m pytest tests/jobs_static/ -q`
- Broader verification required only if:
  - loader routing, jobs-fetcher compatibility, or static report semantics change

## Acceptance Criteria

- `src/jobs/adapters/static.py` becomes a thin orchestration surface with stable exports
- helper ownership is obvious in one or two searches
- root-module compatibility and test patch seams remain valid
- no static extraction, detail traversal, browser fallback, or loader naming regression
- AI routing docs point future edits to the focused helper modules

## Notes

- `static_helpers.py` remains the low-level utility owner in this pass; the goal is to move orchestration and boundary logic, not to reclassify every existing helper.
- This pass is behavior-preserving structural cleanup only.

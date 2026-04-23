# Static Adapter Boundary Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/jobs/adapters/static.py` was reduced from a large adapter monolith into a stable orchestration surface over focused helper modules.
- Runtime/context state, listing flow, detail traversal, loader/shard plumbing, and helper ownership moved behind modules such as `static_{runtime,listing,listing_flow,detail,sources}.py` plus `static_{runtime_support,detail_heuristics}.py`.
- Loader names, report payloads, browser-fallback behavior, static-source extraction outcomes, and jobs-fetcher compatibility stayed stable.

## Final Owning Surfaces

- Stable root: `src/jobs/adapters/static.py`
- Owning leaves: `src/jobs/adapters/static_{runtime,listing,listing_flow,detail,sources}.py` and `src/jobs/adapters/static_{runtime_support,detail_heuristics}.py`

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), [`../../adapter-plugin-inventory.md`](../../adapter-plugin-inventory.md), and [`../../scraping-pipeline.md`](../../scraping-pipeline.md).

## Historical Notes

- `static_helpers.py` remained the low-level utility owner in this pass; the goal was to move orchestration and boundary logic, not to reclassify every helper.
- The stable adapter entrypoint and test patch seams were intentionally preserved.

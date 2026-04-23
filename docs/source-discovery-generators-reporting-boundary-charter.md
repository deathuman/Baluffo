# Source Discovery Generators and Reporting Boundary Cleanup

## Goal

Reduce AI drift and token waste in the source-discovery subsystem by thinning the remaining large generator and reporting leaves without changing discovery report, candidate, pending, failure, or M5 backlog contracts. The stable package exports and orchestrator patch seams stay intact while Gamesmap, discovery reporting, and web-search implementation ownership move into focused helper modules.

## Target Boundary

- Primary subsystem: `src/source_discovery/`
- Entry file(s): `src/source_discovery/gamesmap.py`, `src/source_discovery/reporting.py`, `src/source_discovery/web_search.py`
- Ownership boundary being clarified: Gamesmap parsing/cache/candidate generation, discovery progress/candidate/backlog reporting, and web-search fetch/extract/candidate inference
- What becomes easier after this change: discovery edits route to smaller leaves, report/candidate behavior is easier to trace, and future AI edits stop reloading the same monoliths

## Why Now

- Current pain: `gamesmap.py`, `reporting.py`, and `web_search.py` were still large enough to waste context and blur ownership
- Why this is worth doing now: discovery already has orchestrator phase boundaries, so this is the next highest-ROI debloat inside one subsystem
- Why this should stay narrow: it avoids reopening orchestrator, bridge, schema, ranking, or CLI behavior

## In Scope

- Split Gamesmap cache/parsing/candidate generation into focused helper modules
- Split discovery reporting into progress, candidate-stream, and M5 backlog helpers
- Split web-search fetch/extract/candidate logic into focused helper modules
- Update routing docs and structural guardrails to match the new ownership

## Out of Scope

- Discovery schema or payload redesign
- Queue balancing or ranking-policy changes
- Orchestrator flow refactors beyond import rewiring

## Stability Impact

- Runtime behavior touched: discovery helper internals only
- Persisted state touched: none
- Packaging or desktop behavior touched: none
- Compatibility concern: `src.source_discovery.__init__` stays the stable export surface and `src.source_discovery.orchestrator` stays the monkeypatch surface for flow tests
- Rollback trigger: any change in discovery report/taskProgress/candidate/backlog payloads or current Gamesmap/web-search outcomes

## AI Accessibility Impact

- Source-of-truth file after refactor: the focused leaves under `src/source_discovery/`
- Expected search path for future edits: routing docs -> relevant Gamesmap/reporting/web-search helper module -> orchestrator only if flow wiring changes
- Docs or registry to update: `AI_ASSISTANT_GUIDE.md`, `architecture-ai-map.md`, `INDEX.md`, `runtime-first-cleanup-handoff.md`, and `DATA_CONTRACT.md` if ownership notes were stale
- Any transitional seam being kept temporarily: thin roots in `gamesmap.py`, `reporting.py`, and `web_search.py`

## Implementation Shape

- Modules to shrink, split, or simplify: `gamesmap.py`, `reporting.py`, `web_search.py`
- Interfaces or contracts to formalize: stable export surfaces for Gamesmap helpers, reporting helpers, and web-search helpers
- Existing abstractions to reuse: orchestrator phase helpers, directory fetch helpers, page analysis, runtime metrics, and discovery config
- New abstraction to avoid unless proven necessary: no new discovery service layer or schema wrapper

## Verification

- Cheapest syntax/check step: `python -m py_compile` on the modified discovery modules
- Cheapest focused test step: targeted discovery/helper pytest selection plus discovery bridge/route coverage
- Broader verification required only if: payload contracts, CLI flags, or bridge-triggered discovery ownership change

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

This is a structural cleanup only. `src/source_discovery.__init__` remains the stable export surface, `src/source_discovery.orchestrator` remains the test patch seam, and docs stay canonical over any AI memory summary.

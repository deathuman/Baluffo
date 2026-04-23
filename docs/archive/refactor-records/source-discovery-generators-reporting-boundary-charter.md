# Source Discovery Generators and Reporting Boundary Cleanup

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/source_discovery/gamesmap.py`, `src/source_discovery/reporting.py`, and `src/source_discovery/web_search.py` stayed as stable compatibility surfaces.
- Gamesmap cache/parsing/candidate generation moved into `gamesmap_{cache,parsing,candidates}.py`.
- Discovery progress/candidate/backlog reporting moved into `reporting_{progress,candidates,backlog}.py`, and web-search fetch/extract/candidate inference moved into `web_search_{fetch,extract,candidates}.py`.

## Final Owning Surfaces

- Stable roots: `src/source_discovery/gamesmap.py`, `src/source_discovery/reporting.py`, `src/source_discovery/web_search.py`
- Owning leaves: `gamesmap_{cache,parsing,candidates}.py`, `reporting_{progress,candidates,backlog}.py`, and `web_search_{fetch,extract,candidates}.py`

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../DATA_CONTRACT.md`](../../DATA_CONTRACT.md) when report or candidate shape matters.

## Historical Notes

- `src/source_discovery.orchestrator` remained the test patch seam and `src/source_discovery.__init__` remained the stable export surface.
- Later follow-up state is summarized in [`../history/runtime-first-cleanup-handoff.md`](../history/runtime-first-cleanup-handoff.md).

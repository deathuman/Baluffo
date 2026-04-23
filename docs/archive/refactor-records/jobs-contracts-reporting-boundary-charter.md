# Jobs Contracts and Reporting Boundary Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/jobs/common/contracts.py` stayed as the stable jobs contract surface while payload normalization ownership moved into `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py`.
- `src/jobs/reporting.py` stayed as the stable reporting surface while summary, queue, breakdown, and social helpers moved into `src/jobs/reporting_{summary,queues,breakdowns,social}.py`.
- Report payloads, task-state behavior, browser-fallback semantics, and fetcher compatibility stayed stable.

## Final Owning Surfaces

- Stable roots: `src/jobs/common/contracts.py`, `src/jobs/reporting.py`
- Owning leaves: `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py` and `src/jobs/reporting_{summary,queues,breakdowns,social}.py`

## Current Routing

Current routing lives in the active wiki, especially [`../../DATA_CONTRACT.md`](../../DATA_CONTRACT.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../scraping-pipeline.md`](../../scraping-pipeline.md) when reporting behavior overlaps adapter flows.

## Historical Notes

- The stable roots were intentionally kept importable and boring.
- This pass reduced token waste and duplicated shaping logic without teaching callers new module paths.

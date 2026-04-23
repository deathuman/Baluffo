# Jobs Pipeline Boundary Charter

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/jobs/pipeline.py` and `src/jobs/state.py` were reduced from implementation-heavy roots into stable compatibility surfaces over focused helper leaves.
- Earlier helper ownership moved into modules such as `src/jobs/pipeline_{run_setup,execution_flow,finalize}.py`, `src/jobs/state_{source_state,lifecycle,incremental}.py`, and related source-state helpers.
- Pipeline behavior, persisted state semantics, report contracts, and fetcher compatibility remained stable during the split.

## Final Owning Surfaces

- Stable roots: `src/jobs/pipeline.py`, `src/jobs/state.py`
- Later owning leaves: `src/jobs/pipeline_runtime_{writers,summary}.py`, `src/jobs/pipeline_source_{loop,results,progress}.py`, and `src/jobs/state_source_{records,browser,migration}.py`

## Current Routing

This earlier root-thinning record is superseded by the later end-state captured in [`../history/final-leaf-closeout-program.md`](../history/final-leaf-closeout-program.md). Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), and [`../../scraping-pipeline.md`](../../scraping-pipeline.md).

## Historical Notes

- `src.jobs.pipeline` and `src.jobs.state` were intentionally kept as stable public surfaces.
- The main historical value of this record is that it marks the earlier phase before the later closeout wave split the heavier internals further.

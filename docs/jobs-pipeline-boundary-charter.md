# Jobs Pipeline Boundary Charter

> Historical/planning record for a focused cleanup lane. Start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) and [`architecture-ai-map.md`](architecture-ai-map.md) before using this document for lane-specific compatibility context.

## Title

Jobs Pipeline and State Boundary Debloat

## Goal

Reduce `src/jobs/pipeline.py` and `src/jobs/state.py` from implementation-heavy roots into stable compatibility surfaces over focused helper leaves without changing pipeline behavior, fetcher compatibility, persisted state semantics, or report contracts.

## Target Boundary

- Primary subsystem: pipeline entry orchestration, loader/runtime setup, source execution flow, source-state persistence, and lifecycle state
- Entry file(s): `src/jobs/pipeline.py`, `src/jobs/state.py`, `src/jobs_fetcher.py`
- Ownership boundary being clarified:
  - runtime setup -> `src/jobs/pipeline_run_setup.py`
  - source execution flow -> `src/jobs/pipeline_execution_flow.py`
  - late-stage output/report assembly -> `src/jobs/pipeline_finalize.py`
  - source-state persistence and browser-fallback helpers -> `src/jobs/state_source_state.py`
  - lifecycle ownership -> `src/jobs/state_lifecycle.py`
  - cadence/freshness policy -> `src/jobs/state_incremental.py`
- What becomes easier after this change: targeted pipeline fixes, lower AI token cost, clearer task routing, and less pressure to re-expand root compatibility files

## Why Now

- Current pain: `src/jobs/pipeline.py` mixed CLI, runtime setup, source execution, progress wiring, and finalize coupling; `src/jobs/state.py` mixed source-state persistence, lifecycle logic, browser fallback state, and incremental policy
- Why this is worth doing now: the other high-noise compatibility roots are already thinned, so pipeline/state is the next highest-value backend debloat lane
- Why this should stay narrow: fetch reports, output feeds, source-state rows, lifecycle behavior, and jobs-fetcher compatibility all need to remain stable

## In Scope

- Add a repo-tracked charter for the pipeline/state cleanup lane
- Extract runtime setup into `src/jobs/pipeline_run_setup.py`
- Extract source execution flow into `src/jobs/pipeline_execution_flow.py`
- Keep `src/jobs/pipeline.py` as the stable package entrypoint with `default_source_loaders(...)`, `run_pipeline(...)`, `parse_args(...)`, and `main(...)`
- Extract source-state logic into `src/jobs/state_source_state.py`
- Extract lifecycle logic into `src/jobs/state_lifecycle.py`
- Keep `src/jobs/state.py` as the stable compatibility surface
- Update routing docs and suite-contract guardrails for the new ownership map

## Out of Scope

- CLI flag changes
- Output JSON/CSV/light JSON schema changes
- Loader naming or default loader mix changes
- Browser-fallback queue semantics changes
- Frontend contract changes in `frontend/jobs/domain.js`

## Stability Impact

- Runtime behavior touched: pipeline setup ordering, source execution orchestration, late-stage report assembly, source-state persistence, lifecycle stamping
- Persisted state touched: source-state and lifecycle-state implementation only, behavior-preserving
- Packaging or desktop behavior touched: no
- Compatibility concern: `src.jobs.pipeline` stays the stable entrypoint, `src.jobs.state` stays the stable helper surface, and `src.jobs_fetcher` callers must not need new targets
- Rollback trigger: any regression in fetcher compatibility, output preservation on empty runs, lifecycle missing/archive behavior, incremental freshness skips, circuit-breaker exclusions, or browser-fallback queue semantics

## AI Accessibility Impact

- Source-of-truth files after refactor:
  - `src/jobs/pipeline.py`
  - `src/jobs/pipeline_run_setup.py`
  - `src/jobs/pipeline_execution_flow.py`
  - `src/jobs/pipeline_finalize.py`
  - `src/jobs/state.py`
  - `src/jobs/state_source_state.py`
  - `src/jobs/state_lifecycle.py`
  - `src/jobs/state_incremental.py`
- Expected search path for future edits:
  - pipeline entry/CLI -> `pipeline.py`
  - runtime setup/progress wiring -> `pipeline_run_setup.py`
  - source execution flow -> `pipeline_execution_flow.py`
  - late-stage output/report assembly -> `pipeline_finalize.py`
  - source-state persistence, circuit breakers, browser fallback -> `state_source_state.py`
  - lifecycle missing/archive behavior -> `state_lifecycle.py`
  - cadence/freshness decisions -> `state_incremental.py`
- Docs or registry to update:
  - `docs/AI_ASSISTANT_GUIDE.md`
  - `docs/architecture-ai-map.md`
  - `docs/INDEX.md`
  - `docs/scraping-pipeline.md`
  - Serena `routing_and_boundaries`
- Transitional seam being kept temporarily:
  - `src/jobs.pipeline` and `src.jobs.state` remain stable compatibility surfaces even though implementation ownership moved to helper leaves

## Implementation Shape

- Modules to shrink, split, or simplify:
  - `src/jobs/pipeline.py`
  - `src/jobs/state.py`
- Interfaces or contracts to formalize:
  - pipeline root orchestration boundary
  - jobs state compatibility surface
- Existing abstractions to reuse:
  - `pipeline_runtime.py`
  - `pipeline_loader_selection.py`
  - `pipeline_stage_source_execution.py`
  - `state_incremental.py`
- New abstraction to avoid unless proven necessary:
  - another composition root
  - root-to-root imports from the new helper leaves

## Verification

- Cheapest syntax/check step:
  - `python -m py_compile src/jobs/pipeline.py src/jobs/pipeline_run_setup.py src/jobs/pipeline_execution_flow.py src/jobs/pipeline_finalize.py src/jobs/state.py src/jobs/state_source_state.py src/jobs/state_lifecycle.py`
- Cheapest focused test step:
  - pipeline/fetcher compatibility pytest slices plus browser-fallback and structured-migration state tests
- Broader verification required only if:
  - docs guardrails drift or static adapter compatibility changes while the state helpers move

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

Keep `src/jobs.pipeline` and `src.jobs.state` as stable surfaces even though the ownership moved behind them. The point of the pass is less noise and less AI drift, not a public API redesign.

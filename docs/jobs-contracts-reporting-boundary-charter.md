# Jobs Contracts and Reporting Boundary Charter

## Goal

Keep `src/jobs/common/contracts.py` and `src/jobs/reporting.py` as stable import surfaces while moving payload normalization, summary generation, queue builders, and breakdown/social helpers into focused leaves. The payoff is less search noise, less AI drift, and smaller roots on the canonical pipeline payload path without changing report, task-state, browser-fallback, or parser-regression behavior.

## Target Boundary

- Primary subsystem: jobs contracts and reporting
- Entry file(s): `src/jobs/common/contracts.py`, `src/jobs/reporting.py`
- Ownership boundary being clarified: stable compatibility roots vs helper-owned payload normalization and reporting logic
- What becomes easier after this change: future payload/report edits start in the owning leaves instead of rediscovering two large mixed-responsibility modules

## Why Now

- Current pain: the canonical contract/reporting surfaces still carry most of the remaining jobs-pipeline line count and duplicate shaping logic
- Why this is worth doing now: the pipeline/state roots are already thinner, so this is the next highest-value lane for token waste and routing drift
- Why this should stay narrow: report shapes, task-state payloads, queue semantics, and fetcher compatibility all need to remain unchanged

## In Scope

- Move runtime payload normalization into `src/jobs/common/contracts_runtime.py`
- Move source-report normalization into `src/jobs/common/contracts_source_reports.py`
- Move task-state normalization into `src/jobs/common/contracts_task_state.py`
- Move fetch-report assembly into `src/jobs/common/contracts_fetch_report.py`
- Move reporting breakdown, queue, summary, and social helpers into `src/jobs/reporting_{breakdowns,queues,summary,social}.py`
- Update canonical docs and guardrails for the new ownership split

## Out of Scope

- Schema redesign in `src/core/schemas.py` or `src/core/contracts.py`
- Frontend `frontend/jobs/domain.js` cleanup
- Pipeline behavior changes, loader changes, or queue policy changes
- New fetcher facade work

## Stability Impact

- Runtime behavior touched: payload/report assembly only
- Persisted state touched: none
- Packaging or desktop behavior touched: none
- Compatibility concern: `src.jobs.common.contracts`, `src.jobs.reporting`, and `src.jobs_fetcher` must keep their current public names and semantics
- Rollback trigger: any payload shape drift in fetch reports, task state, source reports, browser-fallback queues, parser-regression queues, or social review payloads

## AI Accessibility Impact

- Source-of-truth file after refactor: `src/jobs/common/contracts.py` and `src/jobs/reporting.py` stay as stable surfaces, while implementation ownership moves to the new contract/reporting leaves
- Expected search path for future edits: contract/reporting leaves first, stable roots second
- Docs or registry to update: `docs/DATA_CONTRACT.md`, `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, `docs/INDEX.md`, `docs/scraping-pipeline.md`, Serena `routing_and_boundaries`
- Any transitional seam being kept temporarily: direct imports from the stable roots stay valid for existing callers and fetcher compatibility exports

## Implementation Shape

- Modules to shrink, split, or simplify: `src/jobs/common/contracts.py`, `src/jobs/reporting.py`
- Interfaces or contracts to formalize: runtime/source-report/task-state/fetch-report ownership and breakdown/queue/social-report ownership
- Existing abstractions to reuse: `src.shared.live_task`, `src.jobs.common.taxonomy`, `src.scrapers.domain_profiles`, current pipeline finalize/runtime call sites
- New abstraction to avoid unless proven necessary: another compatibility facade or broad “shared utils” barrel

## Verification

- Cheapest syntax/check step: targeted `py_compile` or narrow pytest runs against reporting/contract call sites
- Cheapest focused test step: fetcher/reporting pytest slices plus `tests/jobs_static/`
- Broader verification required only if: docs routing or compatibility exports drift while the roots are thinned

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

Keep the stable roots importable and boring. The goal is to reduce token waste and duplicated logic, not to make callers learn new module paths.

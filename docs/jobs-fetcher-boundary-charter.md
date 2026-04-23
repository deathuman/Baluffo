# Jobs Fetcher Boundary Charter

## Goal

Keep `src/jobs_fetcher.py` as a stable CLI and test patch surface while moving lazy export bookkeeping and root-backed runtime wrappers into focused helper modules under `src/jobs/`. The payoff is a smaller search surface for humans and AI without changing fetcher behavior, loader naming, admin launch wiring, or static adapter compatibility.

## Target Boundary

- Primary subsystem: jobs fetcher compatibility facade
- Entry file(s): `src/jobs_fetcher.py`
- Ownership boundary being clarified: root CLI/patch surface vs helper-owned lazy exports and runtime wrappers
- What becomes easier after this change: future fetcher behavior work starts in `src/jobs/*` leaves instead of re-expanding the root facade

## Why Now

- Current pain: the stable facade still owned a large inline export table and several compatibility wrappers, which created noise for search, review, and AI routing
- Why this is worth doing now: the repo already thinned the other major compatibility roots, so `src/jobs_fetcher.py` was the next obvious backend surface
- Why this should stay narrow: fetcher outputs, source registry shape, admin launch behavior, and adapter behavior all need to remain unchanged

## In Scope

- Move lazy compatibility export bookkeeping into `src/jobs/fetcher_compat_exports.py`
- Move root-backed wrapper seams into `src/jobs/fetcher_compat_runtime.py`
- Restore lazy compatibility access for `canonicalize_job`, `canonicalize_job_with_reason`, `canonicalize_google_sheets_rows`, and `deduplicate_jobs`
- Remove test-only alias repatching drift caused by the missing root exports
- Update routing docs and suite-contract guardrails for the new helper ownership

## Out of Scope

- New pipeline behavior or output-schema changes
- Source registry or report payload changes
- Admin launch/task payload changes
- Static adapter behavior changes

## Stability Impact

- Runtime behavior touched: facade wiring only
- Persisted state touched: none
- Packaging or desktop behavior touched: none
- Compatibility concern: `src.jobs_fetcher` remains both a CLI surface and a monkeypatch-safe test surface
- Rollback trigger: any change in fetcher CLI behavior, root monkeypatch targets, loader naming, or jobs-static compatibility

## AI Accessibility Impact

- Source-of-truth file after refactor: `src/jobs_fetcher.py` for root compatibility, `src/jobs/fetcher_compat_exports.py` for lazy export routing, and `src/jobs/fetcher_compat_runtime.py` for root-backed wrappers
- Expected search path for future edits: `src/jobs/*` leaves first, fetcher helper modules second, root facade last
- Docs or registry to update: `docs/AI_ASSISTANT_GUIDE.md`, `docs/architecture-ai-map.md`, `docs/INDEX.md`, `docs/adapter-plugin-inventory.md`, Serena `routing_and_boundaries`
- Any transitional seam being kept temporarily: root-backed monkeypatch seams for `httpx`, `urlopen`, `STUDIO_SOURCE_REGISTRY`, `SOURCE_DIAGNOSTICS`, and wrapper delegation through `src.jobs_fetcher`

## Implementation Shape

- Modules to shrink, split, or simplify: `src/jobs_fetcher.py`
- Interfaces or contracts to formalize: lazy compatibility exports and root-backed wrapper seams
- Existing abstractions to reuse: package-owned pipeline, transport, registry, and adapter leaves
- New abstraction to avoid unless proven necessary: another composition root or eager symbol barrel

## Verification

- Cheapest syntax/check step: `python -m py_compile src/jobs_fetcher.py src/jobs/fetcher_compat_exports.py src/jobs/fetcher_compat_runtime.py`
- Cheapest focused test step: fetcher facade + jobs-static pytest slices
- Broader verification required only if: admin launch wiring or docs contract checks drift while the root surface changes

## Acceptance Criteria

- Boundary is clearer than before
- No new cross-subsystem dependency leak
- No product-facing behavior regression
- Docs/source-of-truth are updated if edit location changed
- Future AI/human editor can find the right file in 1-2 searches

## Notes

Keep `src.jobs_fetcher` as the stable CLI and monkeypatch surface. Restoring the canonicalize and dedup lazy exports is intentional compatibility work, not a return to eager root re-export sprawl.

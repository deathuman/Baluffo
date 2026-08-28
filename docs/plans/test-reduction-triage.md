# Test Reduction Triage

> - **Status:** Parked closeout baseline — the May 2026 reduction campaign closed 2026-05-31; revive only for a new coverage-backed sweep using the recorded prerequisites
> - **Use this when:** checking the completed May 2026 test-reduction campaign, explaining retained tests, or deciding whether a new coverage-backed sweep is warranted
> - **Canonical for:** completed reduction slices, retained-test rationale, and prerequisites for future merge/delete work
> - **Not canonical for:** verification command ownership or product/runtime contracts
> - **Then inspect:** [`../testing.md`](../testing.md), the candidate test file, and the owning source or contract doc
> - **Last updated:** 2026-08-28 (status review — campaign closed; parked baseline)

This triage records the safe-reduction path from the May 2026 broad test sweep.
The high-confidence reduction campaign is closed out as of 2026-05-31. Coverage
data is a signal only: delete or merge a test only when the asserted behavior is
duplicated, obsolete, or belongs in a static guardrail rather than the runtime
test lane.

Future reductions should start with a fresh coverage-context or structural
inventory. Do not continue deleting from this record alone; the remaining tests
default to retained unless a new sweep proves a concrete duplicate or obsolete
contract.

## Baseline Evidence

- Initial Python collection: `3338` pytest cases across `337` files.
- Closeout Python inventory: `3229` collected pytest node ids across `336` files.
- Closeout Python verification: the broad Python lane last passed with `3090 passed, 139 deselected`.
- Closeout line-budget enforcement: explicit test-file baselines were ratcheted down to the post-reduction line counts so deleted or merged bloat cannot silently regrow.
- Follow-up pure contract grouping: Python URL/taxonomy/log/provider-detail variants were reduced from `173` to `95` targeted pytest cases, and frontend Admin/Jobs progress/location variants were reduced from `65` to `48` top-level `test(...)` calls while preserving case assertions.
- Follow-up verification: after the pure contract grouping, `npm run test:py` passed with `3012 passed, 139 deselected`; `npm run test:frontend:unit` and `npm run lint:repo-guardrails` also passed.
- Python coverage-context sweep: full suite passed with coverage contexts over `src`, `scripts`, and `tools`.
- Initial frontend unit sweep: `117` Node unit files and `571` static `test(...)` calls ran successfully under per-file V8 coverage.
- Closeout frontend static inventory: `117` unit files and `564` top-level `test(...)` calls.
- Frontend smoke and packaged desktop scripts were inventoried but are release/runtime gates; do not judge them by unit coverage.

## Removed Immediately

These tests were deletion-grade because they asserted imports, archived notes, or one-off retired implementation state rather than current behavior:

- `tests/test_pipeline_execution.py::test_pipeline_execution_module_loads`
- `tests/test_jobs_package.py::test_jobs_fetcher_internal_shims_are_not_contract_surfaces`
- `tests/test_jobs_fetcher_providers.py::test_default_registry_no_longer_seeds_stale_ashby_personio_or_placeholder_greenhouse_rows`
- `tests/source_discovery/test_config_and_helpers.py::test_source_discovery_public_barrel_no_longer_uses_module_forwarding`

## Completed Python Reduction Slices

1. **Move source-text policy checks out of pytest.**
   Completed on 2026-05-31: the jobs package/private-boundary, broad `src.jobs.common` import, retired `legacy_runners`, retired `tests.jobs_fetcher_helpers`, and migrated owning-import checks now live in `tools/repo_health/suite_contract_policy.py` and run through `npm run lint:repo-guardrails`.

2. **Merge city/location noise coverage by layer.**
   Completed on 2026-05-31: `tests/test_jobs_fetcher_city_noise_quality.py` was removed after preserving representative canonicalization assertions in `tests/test_jobs_fetcher_quality.py`. The exhaustive parser/sanitizer corpus remains in `tests/jobs/adapters/parsers/test_location.py` and `tests/fixtures/city_regression_corpus.json`.

3. **Consolidate source-discovery directory/audit fallback tests.**
   Completed on 2026-05-31: directory-index failure paths, web-page seed/web scenarios, web-search audit HTTP recovery/no-recovery paths, and GameProg/GamesMap directory candidate scenarios were collapsed into parametrized tests while preserving recovery, provenance, cache, and success assertions. Cache/signature/browser-recovery audit tests and parser-level directory fixtures are intentionally retained.

4. **Consolidate dedup/provider-static audit files.**
   Dedup micro-slice completed on 2026-05-31: the two sparse Stellar static-source variants in `tests/test_jobs_fetcher_quality.py` were collapsed into one parametrized test while preserving the rendering-engineer and technical-artist cases. Source-report normalization micro-slice completed on 2026-05-31: the adjacent structured-details and site-changed provider URL preservation checks in `tests/test_jobs_fetcher_providers.py` were merged into one contract test. Static-probe/linked-static micro-slice completed on 2026-05-31: seven same-seam static probe cases were grouped into two loop-driven tests and the two linked-static identity cases were grouped into one scenario test. Provider-coverage next-action micro-slice completed on 2026-05-31: positive debug-validation and exhausted-validation fixture variants were grouped into two loop-driven tests while preserving action, blocker, diagnostic, and command assertions. Remaining soak-report tests are retained unless a future slice proves a concrete duplicate.

## Retained Python Boundaries

- Remaining soak-report tests are retained unless a future slice proves a concrete duplicate; current coverage-gap, staging, advisory, and warning-gate tests protect distinct persisted report output and route-facing contracts.
- Pipeline, discovery, source-sync, and packaged-adjacent tests often cover cross-component contracts. Do not delete them just because they are slow or share covered lines; first prove that a narrower seam test plus an existing smoke/release gate covers the same failure mode.
- Bridge conflict-adjudication, packaged-runtime, source-sync, and release-adjacent tests default to keep because their value is compatibility and integration failure detection, not unique line coverage.

## Frontend JS Triage Classification

### Remove or merge candidates

Completed on 2026-05-31: the zero-unique `jobs-html.test.mjs` snapshot-style checks over static HTML/CSS and retired visual structure were removed. Future frontend cleanup should not re-triage those deleted checks unless a current UI contract doc is created for the exact invariant.

Remaining frontend reductions should start from a fresh manual and coverage-context triage rather than static snapshot deletion alone.

### Keep candidates despite zero unique file-level coverage

Keep tests that assert persisted data compatibility, payload compatibility, bridge/runtime sequencing, or user-facing behavior even when they share all executed source lines with other files:

- `tests/frontend/unit/local-data-tracking.test.mjs`
- `tests/frontend/unit/local-data-runtime-contract.test.mjs`
- `tests/frontend/unit/saved-runtime-controllers.test.mjs`
- `tests/frontend/unit/jobs-feed-startup.test.mjs`
- `tests/frontend/unit/jobs-feed-bootstrap-confirm.test.mjs`
- `tests/frontend/unit/task-run-view-model.test.mjs`
- `tests/frontend/unit/admin-ops-controller.test.mjs`
- `tests/frontend/unit/admin-registry-controller.test.mjs`
- `tests/frontend/unit/admin-source-policy-review-controller.test.mjs`
- `tests/frontend/unit/live-task-observability.test.mjs`

### Move or keep as tooling policy

These tests exercise repo tooling or harness behavior rather than frontend product UI. Keep them if the tool remains supported; otherwise move policy checks to the owning tool guardrail:

- `tests/frontend/unit/mcp-playwright-server.test.mjs`
- `tests/frontend/unit/playwright-smoke-runtime.test.mjs`
- `tests/frontend/unit/perf-counters.test.mjs`
- `tests/frontend/unit/perf-marks.test.mjs`
- `tests/frontend/unit/startup-metrics-effects.test.mjs`

## Safety Rules For Future Reduction

- Do not remove compatibility tests for persisted local data, bridge payloads, release manifests, packaged desktop flows, or source-sync formats without a replacement contract test.
- Prefer merging duplicated parametrized cases over deleting edge-case data.
- If a test only scans source text for architecture policy, migrate it to repo guardrails or delete it; do not keep expanding pytest for repository policy.
- If a test protects a retired behavior, delete it unless the code still accepts that behavior as an intentional compatibility surface.
- Before any new reduction campaign, refresh collection counts and coverage-context evidence. Treat this closeout as a baseline, not an evergreen deletion queue.

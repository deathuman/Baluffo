# Jobs Adapter Mass-Refactoring Plan

## Status

Active plan. This is separate from the source-discovery deletion-first cleanup, which is now closed for behavior-preserving migration work. This plan targets `src/jobs/adapters/` and only touches `src/source_discovery/` when a jobs adapter change needs an evidence handoff.

## Goal

Reduce `src/jobs/adapters/` to thin entrypoints plus source-specific parsers. Shared runners should own fetch, retry, fallback, probe, dedupe, reporting, progress, and audit lifecycle. Each slice must be deletion-first and net LOC-negative unless it adds test scaffolding, source coverage, or a required evidence harness.

## Protected Surfaces

- Saved jobs and local user data contracts.
- Bridge/API contracts for UI/runtime invocation.
- Queue, pending review, tombstone, static suppression, and auto-approval behavior.
- Frontend payload shapes for discovery/fetch reports.
- Public plugin registration contracts, especially `can_handle(...)` and `run(...)` signatures during migration.
- Persisted public job text, location, and report fields.

## Hard Gates

- Every production refactor phase must be net LOC-negative unless adding new source coverage.
- No new C901 offenders.
- Existing C901 offenders touched by a phase must decrease in score or be explicitly left untouched with a reason.
- No new helper unless the same slice deletes or substantially thins adapter-owned code.
- Adapters should not own fetch, retry, recovery, probe, dedupe, report, progress, or audit lifecycle after migration.
- Yield validation must compare before/after job results on representative sources using valid current source identifiers. If yield drops for a protected representative source, the phase is not complete until the drop is understood and accepted.

## Validation Sweep Baseline

The current code sweep supports the plan direction with these corrections:

- `src/jobs/adapters/provider_api.py` already has `_dispatch_provider_api(...)`, but BambooHR and Workday still bypass it through direct `_provider_structured_listing` calls even though both are registered provider plugins.
- `tests/jobs/adapters/plugins/static/` is absent, so static plugin refactors need fixture coverage before broad changes.
- `scripts/compare_yield` does not exist; yield comparison needs either a small read-only helper or documented manual comparison of pipeline output artifacts.
- Current pipeline flags are `--only-sources`, `--output-dir`, `--force-refresh-all`, and `--quiet`, not the older `--sources` / `--output` shape.
- Static shard names such as `static_studio_pages_a_i` are not valid current `--only-sources` entries. Static source identifiers are generated as `static_source::...` entries, so the yield gate must use generated source IDs or add a read-only source-list helper.
- `python -m src.jobs.pipeline` may emit a cosmetic import-order `RuntimeWarning`; this is not a yield regression by itself.
- `cmd /c npm run lint:precommit` works in the project validation path, but some Windows shells can block direct npm scripts through execution policy; use the documented ruff fallback only when the npm guardrail cannot be invoked.
- `src/jobs/adapters/static_listing.py` is about 611 lines with `process_static_source` at C901 52.
- `src/jobs/adapters/static_listing_flow.py` is about 630 lines with `_extract_listing_candidates` at C901 24 and `_run_plugin_fast_path` at C901 23.
- `src/jobs/adapters/static_detail.py` has `run_detail_traversal` at C901 27.
- `src/jobs/adapters/static_detail_heuristics.py` has `_infer_detail_page_fields` at C901 30, `process_detail_html` at C901 26, `choose_detail_traversal_mode` at C901 12, and `source_detail_limit_for` at C901 11.
- `src/jobs/adapters/location_rules.py` has `classify_city_garbage` at C901 42 and `_looks_like_location_name` at C901 11.

## Execution Sequence

| Order | Phase | Priority | Expected Net LOC | Risk | Result |
| --- | --- | --- | --- | --- | --- |
| 1 | Collapse provider API dispatch | P0 | about 280 deleted after alias removal | Very low | One dispatcher path for all provider API plugins. |
| 2 | Add static plugin fixture scaffolding and yield harness | P1 | about 200 added | Low | Safe static plugin refactors and valid source-yield measurement. |
| 3 | Tiered static plugin runner | P1 | 800-1,200 deleted | Medium | Standard/custom static plugins stop owning repeated fetch/fallback/meta flow. |
| 4 | Reduce `static_listing_flow.py` complexity before merge | P1 | small deletion or neutral | Low-medium | `_extract_listing_candidates` and `_run_plugin_fast_path` become merge-safe. |
| 5 | Merge `static_listing_flow.py` into `static_listing.py` | P1 | about 200 deleted | Medium | Removes artificial flow split without creating an unmanageable combined file. |
| 6 | Merge `DetailTraversalRunner` lifecycle into static runner | P2 | about 200 deleted | Medium | Detail traversal lifecycle moves into the unified static runner. |
| 7 | Collapse static fetch/listing/detail orchestration | P2 | 400-500 deleted | Medium-high | `process_static_source(...)` becomes a thin runner entrypoint. |
| 8 | Reduce `static_detail_heuristics.py` C901 offenders | P2 | near neutral | Low | Detail heuristic complexity drops below the current baseline. |
| 9 | Reduce `location_rules.py` C901 offenders | P2 | near neutral | Low | Location garbage classification becomes table/handler-driven. |
| Deferred | Delete dead plugins/sources | P3 | 1,000-2,500 deleted | Low with evidence | Removes repeatedly zero-yield source code after fresh fetch evidence. |
| Skipped | Source-discovery provenance refactor | Skipped | 0 | n/a | Source-discovery behavior-preserving cleanup is closed; future source-discovery work should be evidence-backed behavior change. |

## Phase 1: Collapse Provider API Dispatch

Priority: P0.

Files:

- `src/jobs/adapters/provider_api.py`
- `src/jobs/adapters/plugins/provider_api/register.py` only if registration assumptions are wrong
- `tests/test_jobs_fetcher_providers.py`

Problem:

- Most provider API wrappers already use `_dispatch_provider_api(...)`.
- BambooHR and Workday still call `_provider_structured_listing.run_bamboohr_sources(...)` and `_provider_structured_listing.run_workday_sources(...)` directly.
- BambooHR and Workday are registered provider plugins, so the direct calls are legacy boilerplate.

Implementation:

- Route BambooHR and Workday through `_dispatch_provider_api(...)` like the other provider API adapters.
- Keep old `run_*_source(...)` public names as one-line compatibility wrappers for one release cycle if existing fetcher call sites still use them.
- Do not change plugin `can_handle(...)` or `run(...)` signatures.
- Do not change provider job row fields or persisted job text.

Validation:

```powershell
python -m pytest -q tests/test_jobs_fetcher_providers.py
python -m pytest -q tests/jobs/adapters
cmd /c npm run lint:precommit
```

Fallback only if npm execution policy blocks the guardrail command:

```powershell
python -m ruff check src tests scripts
python -m ruff format --check src tests scripts
```

Acceptance:

- BambooHR and Workday use the same dispatcher path as the rest of provider API.
- Existing provider tests pass.
- Net production LOC is negative or alias-only neutral with a documented follow-up deletion.

## Phase 2: Add Static Plugin Fixture Scaffolding And Yield Harness

Priority: P1. This phase is allowed to add test/helper LOC because it protects larger deletion phases.

Files:

- `tests/jobs/adapters/plugins/static/test_standard_plugins.py`
- `tests/jobs/adapters/plugins/static/__init__.py`
- Optional read-only helper under `scripts/` or `tests/jobs/adapters/` for listing representative current source IDs and comparing yield artifacts

Problem:

- Static plugin modules have no dedicated test directory.
- Refactoring static plugin orchestration without fixture coverage risks silent job-yield regressions.
- The previous representative yield command used invalid static shard names. Current pipeline `--only-sources` selectors must use generated source IDs such as `static_source::...`, not internal shard loader names like `static_studio_pages_a_i`.

Implementation:

- Add fixture-based tests for three to four representative static plugins without live network access.
- Mock `fetch_text`, parser output, and Playwright fallback.
- Add or document a read-only way to select valid representative static source IDs from current loader output before running yield gates.
- Add a small comparison helper only if it is read-only and directly supports before/after yield validation.

Recommended fixture coverage:

- Successful parse returns rows with `adapter`, `studio`, and `source` fields populated.
- Fetch exception stores `_staticPluginMeta` with blocked/challenge classification and browser fallback recommendation.
- JS-shell HTML stores blocked/challenge metadata with a shell diagnostic hint.
- Confirmed no-openings marker stores empty-confirmed metadata.
- Empty parse without a no-openings marker stores the existing empty/diagnostic classification.

Tier caveat:

- Activision and Supercell are not Tier 1 fixtures unless the runner explicitly supports their canonical listing paths and detail-link fallback. Treat them as Tier 2/custom in the refactor plan.
- Remedy is not a Tier 1 fixture because it calls Jobylon extraction directly.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters/plugins/static/ -v
cmd /c npm run lint:precommit
```

Acceptance:

- Tests pass before Phase 3 begins.
- The plan has an executable yield-gate method using valid source identifiers or a documented provider-only interim gate plus fixture tests.

## Phase 3: Tiered Static Plugin Runner

Priority: P1. Start only after Phase 2 coverage exists and passes.

Files:

- `src/jobs/adapters/plugins/static/_runner.py`
- Standard static plugin modules
- Custom static plugin modules only where deletion is clear

Problem:

Static plugin duplication is real but not uniform. A single universal runner would either miss behavior or grow too abstract. Use tiering instead.

Tier 1: simple static plugins

- `climax`
- `embark`
- `globalstep`
- `naconstudiomilan`
- `cdprojektred`
- `example_com`
- `example_org`

Tier 2: custom static plugins

- `activision`, because it has canonical listing path resolution and a `/job/` regex fallback.
- `supercell`, because it has probable-detail-link fallback through domain profiles.
- `blizzard`
- `littlechicken`
- `frontier`
- `milestone`
- `remedy`, because it calls Jobylon extraction directly.

Leave complex multi-flow plugins alone unless deletion evidence is clear:

- `sheet_studios`
- `ncsoft`
- `nintendo_csod`
- `kojima`
- `riot`
- `larian`
- `amanotes`

Implementation:

- Add `SimpleStaticPlugin` only for the Tier 1 shape.
- Add `StaticFetchContext` or equivalent only if Tier 2 migrations delete more code than they add.
- Keep plugin parser functions local.
- Keep `can_handle(...)` and `run(...)` signatures stable.
- Delete duplicated exception, JS-shell, parse-empty, fallback, and row-tagging code only where tests prove identical behavior.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters/plugins/static/ -v
python -m pytest -q tests/jobs/adapters
cmd /c npm run lint:precommit
```

Acceptance:

- Tier 1 plugins are thin specs plus parser functions.
- Tier 2 migrations are deletion-positive; otherwise they remain local.
- Complex plugins are not forced into the runner.

## Phase 4: Reduce `static_listing_flow.py` Complexity Before Merge

Priority: P1.

Files:

- `src/jobs/adapters/static_listing_flow.py`
- Relevant static adapter tests

Problem:

`static_listing_flow.py` is about 630 lines and already has two C901 offenders: `_extract_listing_candidates` at 24 and `_run_plugin_fast_path` at 23. Merging it directly into `static_listing.py`, which is about 611 lines with `process_static_source` at 52, would consolidate files but make the combined module harder to reason about.

Implementation:

- Split `_run_plugin_fast_path(...)` into explicit helpers for source selection, plugin invocation, metadata normalization, and result classification.
- Split `_extract_listing_candidates(...)` into parser dispatch, candidate normalization, and rejection/diagnostic handling.
- Preserve root monkeypatch seams until callers/tests are moved by the merge phase.
- Do not add new lifecycle helpers unless they delete flow-owned code in the same slice.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters -k static
cmd /c npm run lint:precommit
```

Acceptance:

- `_run_plugin_fast_path(...)` and `_extract_listing_candidates(...)` decrease below their current C901 scores.
- Net production LOC is negative or neutral with a clear reduction in complexity.
- Phase 5 can merge a simpler flow module instead of importing two large C901 functions.

## Phase 5: Merge `static_listing_flow.py` Into `static_listing.py`

Priority: P1. Do this only after Phase 4.

Files:

- `src/jobs/adapters/static_listing.py`
- `src/jobs/adapters/static_listing_flow.py`

Problem:

`static_listing.py` currently re-exports flow symbols and mutates `static_listing_flow.root`, while `static_listing_flow.py` has a composition-root lookup seam. This is artificial coupling.

Implementation:

- Move remaining flow logic into `static_listing.py` after Phase 4 has reduced flow complexity.
- Remove `static_listing_flow.py` once imports are updated.
- Replace `_root_module()` indirection only where tests and callers no longer need the old seam.
- Preserve `process_static_source(...)` as the public entrypoint.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters -k static
cmd /c npm run lint:precommit
```

Acceptance:

- `static_listing_flow.py` is deleted.
- Combined `static_listing.py` does not introduce new C901 offenders beyond known targets.
- Net production LOC is negative.

## Phase 6: Merge Detail Traversal Lifecycle

Priority: P2.

Files:

- `src/jobs/adapters/static_detail.py`
- `src/jobs/adapters/static_listing.py`

Problem:

`static_detail.py` owns detail fetch batching, progress emission, heartbeat behavior, and a second `_root_module()` seam. These are lifecycle concerns that belong in the static runner.

Implementation:

- Move detail traversal lifecycle into a runner object or cohesive private section in `static_listing.py`.
- Keep detail parsing and heuristic code in focused modules.
- Delete `static_detail.py` only once all references are gone.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters -k static
cmd /c npm run lint:precommit
```

Acceptance:

- `run_detail_traversal(...)` no longer exists as an adapter-owned lifecycle island.
- Net production LOC is negative.
- Detail traversal behavior and progress payloads are compatible.

## Phase 7: Collapse Static Fetch/Listing/Detail Orchestration

Priority: P2. Do this only after Phase 3 and Phase 6 are stable.

File:

- `src/jobs/adapters/static_listing.py`

Problem:

`process_static_source(...)` is currently the largest jobs-adapter C901 offender at 52. The static fetch lifecycle still spans listing, plugin fast path, detail traversal, budget exhaustion, and progress callbacks.

Implementation:

- Introduce a cohesive `StaticFetchRunner` only if it deletes existing orchestration and lowers complexity in the same slice.
- Keep `process_static_source(...)` as a thin public entrypoint.
- Preserve progress, heartbeat, skip/revalidation, plugin fast path, listing fetch, candidate extraction, and detail traversal behavior.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters -k static
cmd /c npm run lint:precommit
```

Acceptance:

- `process_static_source(...)` C901 trends down substantially.
- Production LOC is negative.
- No static yield regression on representative sources.

## Phase 8: Reduce `static_detail_heuristics.py` C901 Offenders

Priority: P2.

File:

- `src/jobs/adapters/static_detail_heuristics.py`

Current offenders:

- `_infer_detail_page_fields`: C901 30.
- `process_detail_html`: C901 26.
- `choose_detail_traversal_mode`: C901 12.
- `source_detail_limit_for`: C901 11.

Implementation:

- Prioritize `_infer_detail_page_fields(...)`, the highest remaining detail heuristic complexity target.
- Extract language/source-specific inference handlers into small private functions.
- Convert traversal mode and detail-limit threshold decisions into table-driven rules.
- Split `process_detail_html(...)` into row construction, inferred-field fallback, and rejection classification helpers.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters
python -m ruff check --select C901 src/jobs/adapters/static_detail_heuristics.py
```

Acceptance:

- Each touched C901 score drops below the current baseline.
- No public job text/location behavior changes unless covered by explicit parser tests.

## Phase 9: Reduce `location_rules.py` C901 Offenders

Priority: P2.

File:

- `src/jobs/adapters/location_rules.py`

Current offenders:

- `classify_city_garbage`: C901 42.
- `_looks_like_location_name`: C901 11.

Implementation:

- Split `classify_city_garbage(...)` into focused classifiers for chrome labels, technical noise, prose bleed, role categories, and location-like false positives.
- Convert large conditional clusters into ordered rule tables where behavior remains readable.
- Reduce `_looks_like_location_name(...)` with named predicates or table-driven exceptions if touched.
- Preserve public location normalization output.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters/parsers/test_location.py
python -m ruff check --select C901 src/jobs/adapters/location_rules.py
```

Acceptance:

- C901 scores decrease.
- Existing location parser tests pass.
- Persisted/user-facing location text contracts are preserved.

## Deferred Phase: Delete Dead Plugins/Sources

Priority: P3, evidence-backed only.

Why deferred:

- Deleting source code has the largest possible LOC impact, but stale evidence is not enough.
- A source should not be deleted solely because an old snapshot shows zero yield.

Fresh fetch-yield evidence method:

1. Choose an isolated output root under `_out/`, for example `_out/jobs-adapter-dead-source-evidence-YYYYMMDD`.
2. Run the current jobs pipeline with forced refresh on representative provider and static sources using valid source IDs.
3. Capture source-level kept counts, fetch failures, blocked/challenge classifications, empty-confirmed classifications, and browser fallback recommendations.
4. Repeat once to distinguish repeat zero-yield from transient fetch noise.
5. Delete or quarantine only sources that repeatedly produce zero kept jobs and no useful diagnostic/recovery value.

Example shape after valid source IDs are selected:

```powershell
python -m src.jobs.pipeline --only-sources greenhouse_boards,<valid_static_source_id_1>,<valid_static_source_id_2> --output-dir _out/jobs-adapter-yield-before --force-refresh-all --quiet
python -m src.jobs.pipeline --only-sources greenhouse_boards,<valid_static_source_id_1>,<valid_static_source_id_2> --output-dir _out/jobs-adapter-yield-after --force-refresh-all --quiet
```

Note: the `python -m src.jobs.pipeline` invocation can print a cosmetic import-order `RuntimeWarning`; do not treat that warning alone as a failure.

## Corrected Yield Gate

Do not use `static_studio_pages_a_i` or `static_studio_pages_j_r` as `--only-sources` selectors in the current pipeline gate. They are internal loader names, not current generated static source IDs.

Until the Phase 2 source-list helper exists, use this minimum executable gate for provider work and rely on static fixture tests for static-only refactors:

```powershell
python -m src.jobs.pipeline --only-sources greenhouse_boards --output-dir _out/jobs-adapter-yield-provider-before --force-refresh-all --quiet
python -m src.jobs.pipeline --only-sources greenhouse_boards --output-dir _out/jobs-adapter-yield-provider-after --force-refresh-all --quiet
```

For static phases, the gate must first select valid generated static source IDs from current loader output or a checked fixture list. Once selected, run before/after with the same IDs and compare kept counts, failure classifications, and static metadata buckets. If any kept count drops, the phase is not complete until the drop is explained and accepted.

## Pre-Flight Checklist For Execution

- Run `python -m pytest -q tests/test_jobs_fetcher_providers.py` before Phase 1.
- Add static plugin fixtures before Phase 3.
- Add or document valid static source ID selection before static yield gates are required.
- Split `static_listing_flow.py` complexity before merging it into `static_listing.py`.
- Use `cmd /c npm run lint:precommit` as the normal guardrail. Use direct `python -m ruff ...` checks only if npm script execution is blocked by the host shell.
- Do not start dead-source deletion until a fresh fetch-yield evidence snapshot exists.
# Jobs Fetcher Aggressive Simplification Plan

> - **Status:** Active
> - **Use this when:** choosing the next jobs fetcher simplification, adapter lifecycle consolidation, or high-risk deletion/refactor slice
> - **Canonical for:** aggressive jobs fetcher simplification goals, current objective assessment, next refactor sequence, and validation gates
> - **Not canonical for:** saved-job/local-user data contracts, bridge API payload contracts, or source-discovery behavior
> - **Then inspect:** [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and the touched source files
> - **Last updated:** 2026-04-29

## Objective

The jobs fetcher should prioritize finding active job openings reliably and quickly enough for repeated refreshes. It does not need to preserve long-standing internal fetch/discovery compatibility layers when those layers make the implementation harder to delete, reason about, or improve.

Hard preservation boundary:

- Preserve saved jobs, local user data, and user-entered job tracking information.
- Preserve only the bridge/frontend payload fields that are actively required by the app.
- Allow internal jobs fetcher imports, adapter boundaries, plugin shapes, package-shape tests, and historical compatibility shims to break when a slice replaces them with simpler current behavior.

Primary engineering goals:

- Make advanced behavior the normal path: deduplication, TTL/cache decisions, bounded concurrency, source-state updates, web page identification, redirect-aware fetches, and Playwright fallback should be owned once and reused by source execution paths.
- Reduce duplication by deleting repeated lifecycle implementations, not by adding many tiny helpers that preserve the same complexity in more places.
- Reduce broad C901 pressure and production LOC together. Complexity extraction is not enough unless it deletes concepts, branches, or files.

## Current Assessment

The completed jobs adapter mass-refactor made real but narrow progress:

- Provider dispatch was unified.
- A simple static plugin runner exists.
- `static_listing_flow.py` and `static_detail.py` were deleted.
- Some static detail and location-rule complexity was reduced.
- Evidence-backed dead-source deletion now exists with tombstones and dated snapshots.

The broader objective is not complete:

- `src/jobs` is still about 134 Python files and 27,753 lines.
- `src/jobs/adapters` is still about 75 Python files and 16,542 lines.
- Since the jobs adapter refactor started, production `src/jobs` is roughly net-flat, not meaningfully smaller.
- `python -m ruff check --select C901 src/jobs` still reports 63 complexity offenders.
- Several compatibility surfaces are still preserved by docs/tests even though the current product goal allows breaking internal fetcher compatibility.

The completed roadmap was removed from active docs after commit `7e62dac` completed the evidence-backed dead-source deletion slice. Use git history for historical provenance; keep this plan as the active direction.

## Evaluated Refactor Suggestions

A later "unified edition" refactor proposal was reviewed against the current repo and should not be implemented verbatim. Its useful direction is retained below, but these claims were stale or misleading:

- `src/jobs` is currently about 138 Python files and 30,586 lines, not about 27,935 lines.
- `python -m ruff check --select C901 src/jobs` currently reports 63 offenders, not a cleanable per-slice pass gate.
- `scripts/complexity_baseline.json` now tracks jobs C901 allowances; touched hotspots should leave that baseline when a slice brings them below the threshold.
- `static_listing_flow.py` and `static_detail.py` are already deleted; any plan entries targeting `_extract_listing_candidates` or `run_detail_traversal` are historical.
- `static_listing.py::process_static_source` and `location_rules.py::classify_city_garbage` are no longer current broad C901 offenders.
- Bridge route C901 offenders such as `handle_get` and `handle_post` are outside this jobs fetcher simplification scope unless the product scope is explicitly expanded.

Valuable low-risk candidate slices from that review:

- Deduplicate small coercion helpers where this does not create new coupling: static `_as_dict` copies and contracts `_float_or_zero`.
- Centralize static fetch exception classification so `StaticSourceContext.record_static_fetch_failure(...)` and static plugin error handling share one taxonomy path.
- Reuse `update_source_detail_taxonomy(...)` from the Scrapy static path instead of keeping a parallel `_update_taxonomy_fields(...)` implementation.
- Delete repeated static listing progress and budget guard boilerplate only where a small method replaces multiple blocks and reduces net lines.
- Audit `_fetch_listing_job_async(...)` before refactoring it: delete it if unused; otherwise share only the browser-fallback branch that is truly duplicated.

Valuable medium-risk candidates:

- Consolidate state, reporting, and contracts leaf modules only after the compatibility-policy reset updates docs and package-shape tests. These are not current public product contracts; they are internal guardrails that can be rewritten if the replacement is simpler.
- Break `sys.modules[__name__]` and root-protocol indirection as standalone slices with targeted package/startup tests.

Current jobs C901 priority list:

- `pipeline_source_results.py::execute_loader` at 33.
- `canonicalize.py::canonicalize_job_with_reason` at 32.
- `dedup.py::deduplicate_jobs` at 28 and `dedup.py::merge_records` at 24.
- `adapters/parsers/location.py::parse_generic_location_fields` at 26 and `text_utils.py::invalid_location_reason` at 26.
- Static plugin runners and renderers, especially `littlechicken`, `sheet_studios`, `supercell`, `_rendered_cards`, `activision`, `kojima`, and `larian`.

## Active Simplification Strategy

### 1. Reset compatibility policy

Update docs and package-shape tests so only saved-job/local-user contracts and actively used bridge/frontend payloads are protected. Internal jobs fetcher shims are deletion candidates, especially:

- `pipeline_runtime.py`
- `pipeline_execution_flow.py`
- `state.py` / `state_source_state.py`
- `static_helpers.py`
- `reporting.py` and `common/contracts.py` facade layers
- root monkeypatch and `sys.modules[__name__]` compatibility seams in jobs pipeline/static adapter code

Acceptance:

- The docs no longer call these internal jobs fetcher modules stable compatibility surfaces unless a concrete runtime caller still needs them.
- Tests that only enforce historical module shape are rewritten or deleted.
- Any surviving shim has a named current purpose and owner.

### 2. Build one source execution lifecycle

Replace repeated provider/static/social/community execution lifecycles with one current source execution engine. The engine owns:

- TTL/cache decision and cache report fields.
- Fetch/retry/error classification.
- Bounded concurrency and per-source timing.
- Browser fallback policy and cooldown guard.
- Source-state update and circuit-breaker accounting.
- Queue/report row assembly for fetch outcomes.

Adapters/plugins should declare only source-specific behavior:

- source identity/type
- URL/provider configuration
- parser/extractor callable
- optional browser fallback policy
- optional source-specific normalization or evidence extraction

Acceptance:

- Provider API, static listing, JSON feed, social, and community paths do not each implement their own lifecycle/report/cache branches.
- Advanced behavior is used by default instead of being reachable only through particular adapters.
- The source execution engine replaces more code than it adds in every migration slice after the initial scaffolding.

### 3. Collapse static plugin code into specs where behavior is generic

Continue beyond the initial `_runner.py` work:

- Convert simple fetch/parse/static-card plugins into declarative registrations.
- Keep custom plugins only when they own real source-specific extraction logic.
- Delete plugin modules that become pure declarations.
- Keep evidence-backed source/plugin deletion as a separate gate for removing real source coverage.

Acceptance:

- Simple static plugin modules disappear or shrink to parser-only code.
- Browser fallback and fetch classification are centralized.
- No plugin keeps a custom `run(...)` only to repeat fetch/cache/fallback/report boilerplate.

### 4. Attack the largest complexity hotspots directly

Prioritize functions that dominate current C901 and concept count:

- `pipeline_source_results.py::execute_loader` at C901 33.
- `canonicalize.py::canonicalize_job_with_reason` at C901 32.
- `dedup.py::deduplicate_jobs` at C901 28 and `merge_records` at C901 24.
- Static plugin `run(...)` offenders, especially `littlechicken`, `sheet_studios`, `supercell`, `activision`, `kojima`, and `larian`.

Acceptance:

- Broad `python -m ruff check --select C901 src/jobs` offender count trends down each milestone.
- Touched hotspots either leave the C901 baseline or get a clearly lower score in `scripts/complexity_baseline.json`.
- Refactors reduce source concepts, not just move branches into many one-off helpers.

## Execution Sequence

### Phase 0: Current-fact refresh and no-structure dedupe

Status: completed in commits `018e281` and `8cd9954`.

- Refresh the current metrics in this plan when they drift: jobs LOC, jobs file count, broad jobs C901 offender count, and adapter C901 offender count.
- Implement only no-structure cleanup in this phase: `_as_dict` / `_float_or_zero` dedupe, static fetch error classification reuse, Scrapy taxonomy reuse, and repeated static listing progress/budget boilerplate deletion.
- Do not delete compatibility surfaces, move modules, or change plugin signatures in this phase.
- Skip constants-only rewrites unless they delete duplicated branches or reduce typo-prone taxonomy handling in the same commit.

Validation:

```powershell
python -m pytest -q tests/jobs_static tests/jobs/adapters tests/test_jobs_fetcher.py tests/test_jobs_fetcher_providers.py tests/test_jobs_fetcher_quality.py
python -m ruff check --select C901 <touched jobs files>
cmd /c npm run lint:precommit
```

### Phase 1: Docs and guardrail reset

Status: active policy reset. Current milestone metrics after Phase 0B:

- Jobs Python files: 138.
- Jobs Python lines: 27,892.
- Broad `src/jobs` C901 offenders: 69.
- Adapter C901 offenders: 46.

- Update `docs/architecture-ai-map.md`, `docs/scraping-pipeline.md`, and tests that describe jobs fetcher internals as compatibility surfaces.
- Make the new boundary explicit: saved/local user data is protected; internal fetcher shims are not.
- Establish milestone metrics: jobs LOC, jobs file count, broad C901 offender count, adapter C901 offender count.

Validation:

```powershell
cmd /c npm run lint:precommit
python -m ruff check --select C901 src/jobs --output-format concise
```

The broad C901 command is informational in this phase; record the offender count rather than treating it as a pass gate.

### Phase 2: Delete internal shims and root indirection

Status: completed in commits `67d46fb`, `2d39c77`, `501db44`, `68c809e`, and `d7bbcaa`.

- Delete or collapse jobs fetcher shims whose only purpose is historical import compatibility.
- Remove `sys.modules[__name__]` root indirection where direct parameters or direct imports are now simpler.
- Update package-shape tests to assert the new simpler boundaries instead of the old facades.

Validation:

```powershell
python -m pytest -q tests/test_jobs_package.py tests/test_pipeline_runtime.py tests/test_jobs_fetcher_pipeline.py tests/test_pipeline_execution.py tests/test_pipeline_stage_source_execution.py
python -m ruff check --select C901 <touched jobs files>
cmd /c npm run lint:precommit
```

### Phase 3: Source execution engine migration

Status: active. The first JSON-feed slice converted the branch-heavy provider plugin factory into declarative specs and removed JSON-feed C901 baseline allowances. The structured-provider slice removed the no-op revalidate branch and split the repeated listing/detail lifecycle enough to leave the C901 baseline. The provider-runner lifecycle slice now shares provider cache skip/revalidate report handling across JSON feed, Greenhouse, Teamtailor, and HTML-board runners and removes those runner C901 allowances. Current milestone metrics:

- Jobs Python files: 134.
- Jobs Python lines: 27,887.
- Broad `src/jobs` C901 offenders: 64.
- Adapter C901 offenders: 41.

- Introduce or consolidate one execution engine only where the same slice migrates at least one existing source family and deletes repeated lifecycle code.
- Migrate provider JSON feed and structured provider runners first because they repeat TTL/cache/report patterns and have high C901.
- Migrate static listing/plugin execution only after the engine supports browser fallback, fetch classification, and source-state updates.
- Migrate social/community paths last unless their code becomes trivially compatible earlier.

Validation:

```powershell
python -m pytest -q tests/test_jobs_fetcher_providers.py tests/jobs/adapters tests/test_jobs_fetcher_pipeline.py
python -m ruff check --select C901 <touched jobs files>
cmd /c npm run lint:precommit
```

### Phase 4: Static plugin declarative collapse

Status: active. The first safe slice added shared exact-identity and simple-runner factories, then collapsed boilerplate in parser-local static plugins without deleting source coverage. Current milestone metrics:

- Jobs Python files: 134.
- Jobs Python lines: 27,753.
- Broad `src/jobs` C901 offenders: 64.
- Adapter C901 offenders: 41.

- Audit registered static plugins and classify each as declarative, parser-only, or custom.
- Convert declarative plugins to data registrations handled by the shared runner/engine.
- Delete modules that no longer own behavior.
- Keep custom plugins only when they perform source-specific extraction that cannot be expressed as a small spec.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters/plugins/static tests/jobs_static tests/test_jobs_fetcher.py
python scripts/jobs_yield_gate.py list-static-sources --limit 20
python -m ruff check --select C901 src/jobs/adapters/plugins/static src/jobs/adapters/static_listing.py
cmd /c npm run lint:precommit
```

### Phase 5: Hotspot reductions and broad C901 ratchet

Status: active. `contracts_source_reports.py::normalize_source_report_row` now delegates cohesive field groups and has left the C901 baseline. Current milestone metrics:

- Jobs Python files: 134.
- Jobs Python lines: 27,753.
- Broad `src/jobs` C901 offenders: 63.
- Adapter C901 offenders: 41.

- Refactor the largest remaining hotspots after lifecycle deletion has removed duplicated branches.
- Update `scripts/complexity_baseline.json` only when scores decrease or offenders disappear.
- Start requiring `python -m ruff check --select C901 src/jobs` only after the broad offender count is low enough to make it practical.

Validation:

```powershell
python -m ruff check --select C901 src/jobs --output-format concise
python -m pytest -q tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py tests/jobs/adapters
cmd /c npm run lint:precommit
```

## Metrics To Track Per Commit

Each implementation slice should record:

- Production jobs LOC before/after.
- Jobs Python file count before/after.
- Broad `src/jobs` C901 offender count before/after.
- Adapter C901 offender count before/after.
- Whether the slice deleted a compatibility surface, repeated lifecycle branch, plugin module, or source row.

Do not accept a large helper-creation slice unless it immediately deletes repeated lifecycle code or unlocks a named deletion in the next commit.

## Validation Rules

Default gate for all code slices:

```powershell
python -m pytest -q <targeted tests for touched area>
python -m ruff check --select C901 <touched jobs files>
cmd /c npm run lint:precommit
```

Use broad C901 as a metric until the baseline is small enough to become a hard gate:

```powershell
python -m ruff check --select C901 src/jobs --output-format concise
```

Never use `--no-verify`.

## High-Risk Decisions To Confirm Before Implementation

Ask before starting these, but do not hide them:

- Removing bridge/frontend report fields that appear unused.
- Replacing plugin `can_handle(...)` / `run(...)` signatures for surviving plugins.
- Deleting source rows without fresh evidence.
- Changing saved-job/local-user storage or migration behavior.
- Making broad C901 a hard pass gate before the existing offender count is reduced.

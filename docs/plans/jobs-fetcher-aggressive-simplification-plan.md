# Jobs Fetcher Deletion-First Simplification Plan

> - **Status:** Active
> - **Use this when:** choosing the next jobs fetcher simplification, adapter lifecycle deletion, source-family evidence run, or high-risk fetcher refactor
> - **Canonical for:** deletion-first jobs fetcher goals, current objective assessment, protected boundaries, next refactor sequence, and validation gates
> - **Not canonical for:** saved-job/local-user data contracts, bridge endpoint contracts, frontend payload ownership, or source-discovery behavior
> - **Then inspect:** [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and the touched source files
> - **Last updated:** 2026-04-30

## Objective

The jobs fetcher should find active job openings reliably and quickly enough for repeated refreshes. It does not need to preserve long-standing internal fetch/discovery compatibility layers when those layers make the implementation harder to delete, reason about, or improve.

Protected product surfaces:

- Preserve saved jobs, local user data, and user-entered job tracking information.
- Current UI/runtime invocation paths and bridge/frontend payload fields actively consumed by the app.
- Queue, pending review, tombstone, static suppression, and auto-approval behavior unless a future behavior slice explicitly changes and tests those boundaries.

Internal jobs fetcher shims are deletion candidates when current product behavior remains covered. Internal jobs fetcher imports, adapter boundaries, plugin shapes, package-shape tests, and historical compatibility shims should not be preserved only because they existed historically.

Primary engineering goals:

- Make advanced behavior the normal path: deduplication, TTL/cache decisions, bounded concurrency, source-state updates, web page identification, redirect-aware fetches, and Playwright fallback should be owned once and reused by source execution paths.
- Reduce duplication by deleting repeated lifecycle implementations, not by adding many tiny helpers that preserve the same complexity in more places.
- Reduce production LOC and broad C901 pressure together. Complexity extraction alone is not enough unless it deletes concepts, branches, files, or source rows.

## Current Assessment

The original jobs adapter mass-refactor objective is only partially met.

What improved:

- Source discovery is now much closer to the deletion-first goal, and `src/source_discovery` is currently C901-clean.
- Provider dispatch was unified.
- `static_listing_flow.py`, `static_detail.py`, `static_helpers.py`, and several static/plugin/facade surfaces were deleted.
- Static detail, location-rule, static listing, provider, and plugin complexity were reduced in targeted places.
- Evidence-backed dead-source deletion now exists with tombstones and dated snapshots.
- The first source-family evidence snapshot is available at [`jobs-source-family-evidence-2026-04-30.md`](../snapshots/jobs-source-family-evidence-2026-04-30.md).

What is still not done:

- The old `docs/plans/jobs-adapter-mass-refactoring-plan.md` is historical and no longer exists. This file is the active jobs fetcher simplification tracker.
- `src/jobs` is still about 132 tracked Python files and 31,272 tracked Python lines.
- `src/jobs/adapters` is still about 73 tracked Python files and 18,269 tracked Python lines.
- `python -m ruff check --select C901 src/jobs` currently reports 38 offenders.
- `python -m ruff check --select C901 src/jobs/adapters` currently reports 23 offenders.
- Jobs adapters still own too much lifecycle, especially social, community, static Scrapy, static plugins, parser/report shims, and fetcher compatibility facades.

The conclusion is blunt but useful: the repo has better shared mechanics now, but the final lean objective is not closed. Future work must delete code, delete modules, remove lifecycle branches, or produce evidence for source deletion.

## Active Simplification Strategy

### 1. Refresh facts before each milestone

Record these metrics before and after each implementation slice:

- `src/jobs` tracked Python file count.
- `src/jobs` tracked Python LOC.
- `src/jobs/adapters` tracked Python file count.
- `src/jobs/adapters` tracked Python LOC.
- Broad `src/jobs` C901 offender count.
- Adapter C901 offender count.

Use repeatable tracked-file counts, not shell glob estimates:

```powershell
python -m ruff check --select C901 src/jobs --output-format concise
python -m ruff check --select C901 src/jobs/adapters --output-format concise
```

### 2. Reset internal compatibility boundaries

Aggressively reduce internal compatibility shims while preserving current product surfaces.

Investigate first:

- `src/jobs/fetcher_compat_exports.py`
- `src/jobs/fetcher_compat_runtime.py`
- `pipeline_runtime.py`
- `src/jobs/state.py`
- `state_source_state.py`
- `src/jobs/pipeline_execution_flow.py`
- `static_helpers.py`
- `reporting.py`
- `common/contracts.py`
- tests that enforce old package shape instead of current runtime behavior

High-risk gate: before removing broad `src/jobs_fetcher.py` compatibility re-exports, stop and confirm. That can break external/import-style tests even when current app behavior is preserved.

Acceptance:

- Docs and tests no longer describe internal jobs fetcher shims as stable product contracts.
- Any surviving shim has a named current purpose and owner.
- Removed shims are replaced by direct current runtime imports, not new compatibility facades.

### 3. Build evidence before deleting source families

Run representative isolated fetch evidence before deleting registered sources or default loaders.

Use `_out/` only. Do not mutate tracked `data/`.

Classify source families and sources as:

- `keep`: reliable yield or needed product coverage.
- `merge`: useful yield but duplicated lifecycle.
- `delete`: repeated zero-yield, unsupported, stale, or redundant.
- `defer`: inconclusive because of network, browser, timeout, or anti-bot blockers.

Priority evidence targets:

- `scrapy_static`
- social sources
- community sources
- low-yield static plugins
- remaining static custom plugins

High-risk gate: before deleting active source rows or default loaders, stop with the evidence table and ask for approval.

### 4. Collapse remaining lifecycle duplication by family

Implement only deletion-positive migrations.

Recommended order:

1. `static_scrapy`: delete if evidence says dead/redundant, otherwise merge into the current static execution lifecycle so it no longer owns report/cache/error lifecycle separately.
2. `social`: remove duplicate cache/progress/error/source-report lifecycle, or delete low-yield social paths if evidence supports it.
3. `provider_personio`: fold remaining provider-specific lifecycle into the provider runner path.
4. `community`: keep Google Sheets local only if it remains genuinely source-specific; otherwise migrate fetch/report/cache lifecycle into the shared execution path.

Acceptance:

- Each migration reduces LOC or deletes a module.
- No adapter owns fetch, retry, cache, report, or progress lifecycle unless it is proven source-specific.
- Touched C901 offenders leave the baseline or have lower recorded scores.

### 5. Collapse static plugins into declarations where possible

Continue static plugin simplification with file deletion as the gate.

Targets:

- Convert simple parser-only plugins into registry declarations.
- Delete plugin modules that become pure declarations.
- Keep custom modules only when they own real source-specific parsing or fallback behavior.

Do not add a new shared plugin framework unless the same slice deletes more plugin code than it adds.

### 6. Ratchet C901 after lifecycle deletion

After deletion passes, reduce remaining hotspots in priority order:

- `static_scrapy`
- social/community runners
- static plugin parsers and rendered-card extraction
- shared taxonomy/registry helpers
- `pipeline_finalize`, `state_incremental`, and source-state update flow

Acceptance:

- Broad `src/jobs` C901 offender count trends down each milestone.
- Touched hotspots either disappear or have lower recorded scores.
- Avoid C901-only extraction if it increases LOC without deleting concepts.

## Execution Sequence

### Phase 1: Plan refresh and boundary reset

Status: active.

- Keep this plan aligned with measured repo facts.
- Update architecture/scraping/plugin docs and package-shape tests where they still protect historical internal fetcher surfaces.
- Stop treating old compatibility shims as permanent architecture.

Validation:

```powershell
cmd /c npm run lint:precommit
python -m ruff check --select C901 src/jobs --output-format concise
```

The C901 command is informational until the broad offender count is low enough to become a hard gate.

### Phase 2: Evidence-backed source-family decisions

Status: first snapshot captured on 2026-04-30.

- Use [`jobs-source-family-evidence-2026-04-30.md`](../snapshots/jobs-source-family-evidence-2026-04-30.md) as the starting point for `scrapy_static`, social, community, and representative static source decisions.
- Add narrower follow-up snapshots when the evidence is channel-specific or source-specific.
- Ask before deleting source rows, default loaders, or registered source families.

Validation:

```powershell
python scripts/jobs_yield_gate.py list-static-sources --limit 20
python -m src.jobs.pipeline --only-sources <valid-source-ids> --output-dir _out/<slice>/before --force-refresh-all --ignore-circuit-breaker --quiet
python -m src.jobs.pipeline --only-sources <same-source-ids> --output-dir _out/<slice>/after --force-refresh-all --ignore-circuit-breaker --quiet
python scripts/jobs_yield_gate.py compare _out/<slice>/before _out/<slice>/after --allow-drops
cmd /c npm run lint:precommit
```

### Phase 3: Static Scrapy lifecycle decision

Status: retained and C901-cleaned on 2026-04-30; registry-wide deletion remains separate.

- Keep `scrapy_static_sources` as a supported fallback runtime path even when current evidence has no enabled rows.
- Preserve the default loader, `run_scrapy_static_source(...)`, browser fallback queue behavior, compatibility exports, and frontend progress IDs.
- Thin only duplicated report/error/progress plumbing where the slice stays LOC-negative or reduces `static_scrapy` C901.
- Exception accepted on 2026-04-30: `static_scrapy.py` and the related browser-queue registry path were made C901-clean with a small production LOC increase because no safe fallback-lane deletion was available without widening into compatibility/source-shape removal.
- Keep Scrapy child-process orchestration local unless a future shared runner can replace more code than it adds.

Validation:

```powershell
python -m pytest -q tests/jobs_static tests/jobs/adapters
python -m ruff check --select C901 src/jobs/adapters/static_scrapy.py src/jobs/adapters/static_listing.py
cmd /c npm run lint:precommit
```

### Phase 4: Social and community lifecycle simplification

Status: social lifecycle thinning, Personio provider lifecycle cleanup, and Google Sheets community review completed on 2026-04-30.

- Collapse repeated social cache/progress/error handling, or delete low-yield social paths with evidence.
- Revisit community Google Sheets only if another community path shares the same lifecycle or evidence supports deletion/merge.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters tests/test_jobs_fetcher.py tests/test_jobs_fetcher_quality.py
python -m ruff check --select C901 src/jobs/adapters/social.py src/jobs/adapters/community
cmd /c npm run lint:precommit
```

### Phase 5: Static plugin declaration collapse

Status: custom static plugin parser and rendered-card extraction C901 cleanup completed on 2026-04-30.

- Convert remaining simple plugin modules into declarations.
- Delete modules that no longer own real source-specific logic.
- Keep complex custom plugins local until evidence supports deletion or a smaller lifecycle merge.

Validation:

```powershell
python -m pytest -q tests/jobs/adapters/plugins/static tests/jobs_static tests/test_jobs_fetcher.py
python -m ruff check --select C901 src/jobs/adapters/plugins/static
cmd /c npm run lint:precommit
```

### Phase 6: Broad C901 ratchet

Status: adapter parser C901 cleanup completed on 2026-04-30; non-adapter jobs C901 remains.

- Reduce remaining C901 hotspots only after lifecycle deletion has removed duplicated branches.
- Adapter parser hotspots were decomposed locally after lifecycle cleanup, with parser signatures and row shapes preserved.
- Update `scripts/complexity_baseline.json` only when scores decrease or offenders disappear.

Validation:

```powershell
python -m ruff check --select C901 src/jobs --output-format concise
python -m pytest -q tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py tests/jobs/adapters
cmd /c npm run lint:precommit
```

## Validation Rules

Default gate for docs-only slices:

```powershell
cmd /c npm run lint:precommit
```

Default gate for code slices:

```powershell
python -m pytest -q <targeted tests for touched area>
python -m ruff check --select C901 <touched jobs files>
cmd /c npm run lint:precommit
```

Use broad C901 as a metric until the offender count is low enough to become a hard gate:

```powershell
python -m ruff check --select C901 src/jobs --output-format concise
```

Never use `--no-verify`.

## High-Risk Decisions To Confirm Before Implementation

Ask before starting these, but do not hide them:

- Removing broad `src/jobs_fetcher.py` compatibility re-exports.
- Removing bridge/frontend report fields that appear unused.
- Replacing plugin `can_handle(...)` / `run(...)` signatures for surviving plugins.
- Deleting active source rows, default loaders, or registered source families.
- Changing saved-job/local-user storage or migration behavior.
- Making broad C901 a hard pass gate before the existing offender count is reduced.

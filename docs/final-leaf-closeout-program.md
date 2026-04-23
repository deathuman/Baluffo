# Final Leaf-Monolith Closeout Program

## Goal

Finish the long cleanup campaign with one compatibility-first closeout program aimed at the largest remaining leaf monoliths, not the already-thinned public roots. The stop condition is deliberate:

- public contracts stay stable
- docs remain the source of truth
- guardrails prevent root or route regressions
- the repo ends with an explicit deferred list instead of an open-ended "next cleanup" drift

## Scope Rules

- No new dependencies
- No CLI, schema, or route-signature changes
- No widening of direct cross-slice imports
- Every wave ends with docs and guardrails updated before the next wave is considered complete

## Wave Checklist

| Wave | Scope | Status |
|------|-------|--------|
| 1 | Desktop runtime leaf closeout | Complete |
| 2 | Jobs execution and source-state leaf closeout | Complete |
| 3 | Bridge POST routing and Jobs domain closeout | Complete |
| 4 | End-state freeze and intentional stop list | Complete |

## Hotspot Snapshot

### Refactored roots

| Module | Before | After | Notes |
|--------|--------|-------|-------|
| `src/ship/desktop_app/launcher.py` | 853 | 115 | Stable package-private orchestration root over `launcher_{flow,diagnostics,recovery}.py` |
| `src/ship/desktop_app/startup.py` | 701 | 34 | Stable compatibility surface over `startup_{ready,watchdog}.py` |
| `src/jobs/pipeline_stage_source_execution.py` | 807 | 143 | Stable execution-stage root over `pipeline_source_{loop,results,progress}.py` |
| `src/jobs/pipeline_runtime.py` | 723 | 41 | Stable runtime compatibility root over `pipeline_runtime_{writers,summary}.py` |
| `src/jobs/state_source_state.py` | 705 | 184 | Stable source-state compatibility root over `state_source_{records,browser,migration}.py` |
| `src/bridge/routes/post_routes.py` | 658 | 26 | Stable POST registration root over route-family leaves |
| `frontend/jobs/domain.js` | 561 | 26 | Stable Jobs domain export surface over `domain/{query,feed,view}.js` |

### New owning leaves

| Family | New owners |
|--------|------------|
| Desktop runtime | `launcher_flow.py`, `launcher_diagnostics.py`, `launcher_recovery.py`, `startup_ready.py`, `startup_watchdog.py` |
| Jobs execution/runtime | `pipeline_source_loop.py`, `pipeline_source_results.py`, `pipeline_source_progress.py`, `pipeline_runtime_writers.py`, `pipeline_runtime_summary.py` |
| Jobs source-state | `state_source_records.py`, `state_source_browser.py`, `state_source_migration.py` |
| Bridge POST routes | `post_routes_admin.py`, `post_routes_local_data.py`, `post_routes_update.py` |
| Jobs domain | `frontend/jobs/domain/query.js`, `frontend/jobs/domain/feed.js`, `frontend/jobs/domain/view.js` |

## Wave 1: Desktop Runtime Leaf Closeout

### Acceptance criteria

- `src.ship.desktop_app` remains the only stable import and monkeypatch surface used by desktop tests and packaged smoke
- startup, reclaim, bridge-ready, browser-watch, and startup-probe behavior remain unchanged
- desktop runtime docs route future edits to the new launcher/startup leaves
- suite guardrails keep `launcher.py` and `startup.py` thin

### Result

- `launcher.py` now holds orchestration entrypoints and root-backed seams only
- startup trace, diagnostics, stale-session recovery, and failure-cleanup logic moved into dedicated leaves
- `startup.py` now re-exports readiness/watchdog behavior from narrow leaves without changing public behavior

## Wave 2: Jobs Execution and Source-State Leaf Closeout

### Acceptance criteria

- `src.jobs.pipeline` and `src.jobs.state` stay unchanged as stable package surfaces
- jobs output, report, task-state, source-state, browser-fallback, and parser-regression semantics remain unchanged
- jobs helper leaves do not import compatibility roots
- docs route future edits to the new execution/runtime/source-state owners

### Result

- per-loader execution orchestration, result accumulation, and progress stamping moved out of `pipeline_stage_source_execution.py`
- writer/preservation logic and runtime summary assembly moved out of `pipeline_runtime.py`
- source-state records, browser helpers, and structured-migration helpers moved out of `state_source_state.py`
- root-backed seams were preserved where tests patch the stage/runtime roots directly

## Wave 3: Bridge POST Routing and Jobs Domain Closeout

### Acceptance criteria

- POST route signatures and payloads remain unchanged
- Jobs-page domain exports and runtime behavior remain unchanged
- `post_routes.py` stays a registration-only surface
- `frontend/jobs/domain.js` stays a thin export surface without cross-slice widening

### Result

- admin, local-data, and update POST handlers now live in dedicated route-family leaves
- `post_routes.py` only dispatches and keeps the stable route-registration surface
- Jobs query, feed, and view logic moved behind `frontend/jobs/domain.js`
- Jobs-page callers continue importing the same domain root

## Wave 4: End-State Freeze

### Acceptance criteria

- this tracker records the real before/after footprint
- verification history is captured in one place
- the cleanup ends with an intentional deferred list rather than an implied next default lane

### Current state

- tracker and docs are updated
- full wave-by-wave verification and aggregate completion runs are green
- the deferred-module stop list is now explicit

## Documentation Routing Updated In This Program

- `docs/AI_ASSISTANT_GUIDE.md`
- `docs/architecture-ai-map.md`
- `docs/INDEX.md`
- `docs/startup-probe-architecture.md`
- `docs/scraping-pipeline.md`
- `docs/admin-bridge-api.md`
- `docs/runtime-first-cleanup-handoff.md`
- Adjacent review completed: `docs/testing.md` remains accurate and did not need changes

## Verification Matrix

### Wave 1 commands

```text
python -m pytest tests/desktop_app/ -q
python -m pytest tests/packaged_desktop/ -q
python -m pytest tests/test_suite_contract.py tests/test_release_docs.py tests/test_workflow_entrypoints.py -q
```

### Wave 2 commands

```text
python -m pytest tests/test_jobs_fetcher.py tests/test_jobs_fetcher_pipeline.py tests/test_jobs_pipeline_guard.py tests/test_jobs_package.py -q
python -m pytest tests/test_browser_fallback.py tests/test_structured_migration_state.py -q
python -m pytest tests/jobs_static/ -q
python -m pytest tests/test_suite_contract.py tests/test_release_docs.py -q
```

### Wave 3 commands

```text
python -m pytest tests/bridge/test_routes_post.py tests/admin/ -q
node --test tests/frontend/unit/jobs-domain.test.mjs tests/frontend/unit/jobs-source-metadata.test.mjs tests/frontend/unit/jobs-pipeline.test.mjs tests/frontend/unit/structure-cleanup.test.mjs
python -m pytest tests/test_suite_contract.py tests/test_release_docs.py -q
```

### Program completion commands

```text
npm run test:py
cmd /c npm run test:frontend:unit
```

## Verification History

- Pre-wave guardrails:
  - `python -m pytest tests/test_suite_contract.py tests/test_jobs_package.py tests/test_release_docs.py -q` -> `86 passed`
  - `node --test tests/frontend/unit/structure-cleanup.test.mjs` -> `23 passed`
- Wave 1:
  - `python -m pytest tests/desktop_app/ -q` -> `94 passed`
  - `python -m pytest tests/packaged_desktop/ -q` -> `71 passed`
  - `python -m pytest tests/test_suite_contract.py tests/test_release_docs.py tests/test_workflow_entrypoints.py -q` -> `73 passed`
- Wave 2:
  - `python -m pytest tests/test_jobs_fetcher.py tests/test_jobs_fetcher_pipeline.py tests/test_jobs_pipeline_guard.py tests/test_jobs_package.py -q` -> `79 passed`
  - `python -m pytest tests/test_browser_fallback.py tests/test_structured_migration_state.py -q` -> `5 passed`
  - `python -m pytest tests/jobs_static/ -q` -> `118 passed`
  - `python -m pytest tests/test_suite_contract.py tests/test_release_docs.py -q` -> `64 passed`
- Wave 3:
  - `python -m pytest tests/bridge/test_routes_post.py tests/admin/ -q` -> `149 passed`
  - `node --test tests/frontend/unit/jobs-domain.test.mjs tests/frontend/unit/jobs-source-metadata.test.mjs tests/frontend/unit/jobs-pipeline.test.mjs tests/frontend/unit/structure-cleanup.test.mjs` -> `55 passed`
  - `python -m pytest tests/test_suite_contract.py tests/test_release_docs.py -q` -> `64 passed`
- Program completion:
  - `cmd /c npm run test:py` -> `1569 passed, 98 deselected`
  - `cmd /c npm run test:frontend:unit` -> passed

## Intentionally Deferred Specialized Owners

These modules are acceptable to leave as-is for now. They are specialized owners, not the next cleanup defaults.

| Module | Current lines | Rationale |
|--------|---------------|-----------|
| `src/source_registry.py` | 667 | Specialized source policy/registry owner with broad runtime reach |
| `src/ship/update_manager.py` | 676 | Release-critical capability owner; avoid reopening without behavior work |
| `src/jobs/adapters/social_parsers.py` | 860 | Specialized parser family with broad format coverage |
| `src/source_discovery/core.py` | 548 | Discovery core policy owner; not a good final closeout lane without behavior work |
| `src/ship/packaged_smoke/runtime.py` | 792 | Specialized packaged-smoke runtime owner tied to release-critical flows |

## Closeout Standard

The cleanup program is considered complete when:

1. the verification matrix is green
2. docs point future edits to the new owners
3. guardrails enforce thin roots and no-root-import helper boundaries
4. the deferred list is explicit enough that future work reopens these areas only for real behavior reasons

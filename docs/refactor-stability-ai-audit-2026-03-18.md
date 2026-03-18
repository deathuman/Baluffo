# Refactor Stability + AI Accessibility Audit

Date: 2026-03-18

## Summary

The refactor wave has materially improved the repo for both stability and AI-assisted work, especially in test organization, explicit data contracts, frontend slice boundaries, and backend module extraction. The codebase is in a better state than before, but it is not yet in a fully "settled" modular shape.

The strongest pattern in the current local repo is this:

- boundaries are clearer
- guardrails are much stronger
- docs for AI navigation are much better
- a few high-value compatibility seams and large orchestrator files still behave like residual god objects

The best next move is not another broad refactor. It is a tightening pass that finishes a handful of incomplete migrations, removes docs/HUD ambiguity, and prevents new compatibility seams from spreading.

Since this audit was first written, the first tightening wave has been completed locally:

- bridge runtime state moved into `src/bridge/server/runtime_state.py`
- run-history reconciliation moved behind `src/bridge/run_history_api.py`
- admin runtime tests now distinguish entrypoint fixture usage more clearly
- HUD manifest artifacts now expose `py_tests_status` / `node_tests_status` in addition to compatibility booleans
- contributor docs now call out the remaining transitional seams explicitly
- frontend runtime size is now guarded against further growth
- the first `frontend/jobs/app/runtime.js` extraction has started by moving country/region filter metadata and helpers into `frontend/jobs/app/countries.js`
- `frontend/jobs/app/runtime.js` was narrowed further by moving source preset/data wrapper logic into `frontend/jobs/app/sources.js`
- `frontend/saved/app/runtime.js` was narrowed by moving filter/sort state logic into `frontend/saved/app/view-state.js`
- `src/jobs/pipeline.py` now delegates bootstrap, loader selection, and runtime/task-state concerns to package-private helper modules
- `src/jobs/adapters/static.py` now delegates report/config/fetch/link/detail internals to `src/jobs/adapters/static_helpers.py`
- `src/jobs/common/__init__.py` now declares a curated compatibility surface explicitly, while package-internal hotspots (`pipeline.py`, `transport.py`, `adapters/__init__.py`) now prefer direct submodule imports over the broad barrel
- `src/admin_bridge.py` now delegates registry auto-sync persistence/start logic to `src/bridge/registry_sync_flow.py`, and the stale local sync-normalization helper block has been removed
- sync task worker logic is now shared in `src/bridge/sync_task_flow.py` instead of being duplicated across `admin_bridge.py` and `SyncService`
- `build_bridge_api(...)` now relies on `SyncService` for sync-status wiring instead of entrypoint lambda/wrapper glue
- `BridgeApi` now defaults registry identity/url helpers from `src.source_registry`, reducing reliance on entrypoint stub behavior for registry POST routes
- `RegistryService` now exposes those identity/url helpers too, and `BridgeApi` prefers the typed registry service when present

## Audit Baseline

- Baseline branch: `main`
- Head commit at audit start: `6e65980` (`Fix portable desktop packaging and refactor jobs runtime modules`)
- Remote alignment: local `main` and `origin/main` point to the same commit
- Local dirty state now includes the completed bridge/HUD follow-up changes plus generated data artifacts and untracked local directories:
  - modified code/docs/tests: `src/`, `scripts/`, `tests/`, `docs/`
  - modified generated outputs: `data/*.json`, `data/*.csv`, `data/*.jsonl`
  - untracked: `.cursor/`, `data/local-user-data/`
- There is now local refactor work pending in the working tree, and it matches the roadmap items below rather than unrelated broad churn

## Evidence Collected

### Workspace and docs

- `_out/LATEST_MANIFEST.json` reports a successful build on 2026-03-17, but `py_tests_ok` and `node_tests_ok` are both `false`
- `docs/architecture-ai-map.md`, `docs/testing.md`, `docs/DATA_CONTRACT.md`, and `frontend/shared/ui/selectors.js` are all present and actively maintained
- contributor-facing maps exist for both architecture and frontend slice ownership
- local code now supports explicit HUD test-lane status fields so future manifests can distinguish `not_run` from `failed`

### Recent evolution on `main`

Most recent commits show a fast, coherent architectural direction rather than random churn:

- 2026-03-18: packaging fix plus jobs runtime refactor
- 2026-03-18: pipeline provenance and test fixes
- 2026-03-17: source discovery modularization
- 2026-03-17: adapter plugin rollout
- 2026-03-16: bridge extraction, Pydantic integration, static plugins
- 2026-03-16: AI-native refactor, shared utils/contracts/state hub/UI components
- 2026-03-16: pytest consolidation and AI-efficiency docs
- 2026-03-15: `data-ui` refactor and hard-coded data contracts

### Churn hotspots since 2026-03-01

High churn is concentrated in the right places for a repo mid-refactor:

- `src/`
- `frontend/`
- `tests/`
- `scripts/`
- `docs/`

The important code hotspots are:

- `src/admin_bridge.py`
- `src/jobs/pipeline.py`
- `src/jobs/adapters/static.py`
- `frontend/jobs/app/runtime.js`
- `frontend/saved/app/runtime.js`

This is healthy churn if the next step is narrowing and tightening. It becomes unhealthy only if more broad extraction starts before these hotspots are simplified or locked down.

### Targeted verification run

The current local repo is more stable than the manifest implies.

- `python -m pytest tests/test_suite_contract.py tests/test_no_new_runtime_facade_usage.py tests/test_jobs_package.py -q` -> 12 passed
- `python -m pytest tests/admin/test_admin_bridge_ops_runtime.py tests/bridge/test_sync_service.py -q` -> 22 passed
- `python -m pytest tests/test_desktop_app.py -q` -> 31 passed
- `python -m pytest tests/test_jobs_fetcher.py -q` -> 78 passed
- `python -m pytest tests/test_source_discovery.py -q` -> 38 passed
- `node --check frontend/jobs/app.js frontend/saved/app.js frontend/admin/app.js` -> passed
- `python -m pytest tests/bridge/test_routes_smoke.py tests/admin/test_admin_bridge_ops_runtime.py tests/test_suite_contract.py tests/test_orchestrator_manifest.py tests/bridge/test_sync_service.py -q` -> 32 passed
- `node tests/frontend/unit/all.test.mjs` -> 123 passed
- `python -m pytest tests/test_jobs_package.py tests/test_suite_contract.py tests/test_no_new_runtime_facade_usage.py tests/test_jobs_fetcher.py -q` -> 93 passed

## Subsystem Decision Table

| Subsystem | Stability | AI Access | Recommended action | Notes |
| --- | --- | --- | --- | --- |
| Frontend slice architecture | 5/5 | 5/5 | Keep as-is | Both large runtime entrypoints are now under guardrail and slimmer, with non-boot concerns moving into slice-local helpers. |
| Jobs pipeline and adapters | 4/5 | 5/5 | Tighten | `pipeline.py` and `static.py` are materially narrower, and the remaining internal package hotspots now avoid the broad `jobs/common` barrel. |
| Admin bridge | 4/5 | 4/5 | Tighten | Runtime state, run-history, registry auto-sync flow, and sync task worker flow are now extracted and guarded, but `admin_bridge.py` remains a large composition root with more wiring to trim over time. |
| Source discovery | 4/5 | 4/5 | Keep as-is | Package split, schema validation, and focused tests look healthy. |
| Desktop/runtime packaging | 3/5 | 2/5 | Tighten | Runtime tests pass, but docs have path drift and this area is still high-churn. |
| Test organization and guardrails | 5/5 | 5/5 | Keep as-is | Pytest consolidation, structure tests, facade guardrails, bridge state guardrails, and runtime budget guardrails are strong improvements. |
| AI-facing contracts/docs | 5/5 | 5/5 | Keep as-is | HUD contract ambiguity and contributor-facing transitional seam drift have been tightened in code and docs. |

## What Improved

### 1. Frontend modularity is now discoverable

The frontend now gives AI coders a usable path:

- clear page entrypoints
- runtime helper directories per slice
- shared UI selector registry
- contributor maps that explain where edits belong
- structure tests that prevent cross-slice drift

This is a major improvement over a repo shape where the correct edit location had to be guessed.

### 2. Tests are significantly better organized

The suite now has:

- pytest-only Python tests
- targeted fixture locations
- admin-specific fixtures
- frontend unit manifest discipline
- explicit guardrails around architectural backsliding

This is one of the strongest positive changes in the current repo.

### 3. Contracts are clearer and more explicit

The repo now has a real AI-friendly contract layer:

- `docs/DATA_CONTRACT.md`
- Pydantic validation in `src/core/`
- `data-ui` registry discipline
- workspace HUD manifest
- contributor maps and architecture docs

That gives both humans and AI agents better source-of-truth anchors.

### 4. Backend extraction was directionally correct

The bridge and jobs pipeline refactors clearly moved in the right direction:

- services extracted from the bridge
- plugin families introduced
- shared utils and contracts centralized
- jobs pipeline promoted into a package-owned entrypoint

The issue is not that the extraction happened. The issue is that a few compatibility and orchestration seams are still too large to call the migration complete.

### 5. Backend hotspot files now have clearer internal seams

The latest narrowing wave improved AI and human navigation in the jobs backend:

- `src/jobs/pipeline.py` now reads more like an orchestration root
- path/bootstrap, loader selection, and task/progress concerns live behind named helper modules
- `src/jobs/adapters/static.py` keeps its public loader surface while delegating dense internals
- package tests now guard the new helper boundaries explicitly

## Ranked Findings

### High

#### 1. `src/jobs/common` still carries compatibility-heavy surface area

Evidence:

- `_runtime.facade()` still exists as a compatibility boundary
- `src/jobs/common/__init__.py` is still large and re-export heavy
- the package surface is safer than before, but compatibility affordances are still broad enough to attract new coupling

Impact:

- modularization gains are real, but dependency clarity still depends on keeping the curated surface small
- legacy convenience imports still exist for facade compatibility, so regrowth has to stay guarded

Decision: tightening wave in progress; keep capping compatibility spread

#### 2. `admin_bridge.py` is still a residual composition hotspot

Evidence:

- `src/admin_bridge.py` remains one of the largest live Python files in the repo
- the extracted `BridgeApi` exists, but the entrypoint still holds substantial wiring and compatibility responsibility
- service-level and route-level tests are in better shape, but some admin tests still rely on entrypoint monkeypatch seams by design

Impact:

- stability risk is lower than at audit start, but edits in this file still have a larger than ideal blast radius
- AI coders still need more context than they should for some admin bridge changes

Decision: tightening wave in progress; continue narrowing, not rollback

### Medium

#### 3. Desktop/runtime packaging remains a high-churn boundary

Evidence:

- packaging/runtime paths have changed repeatedly in recent history
- this is still a high-risk area for user-facing regressions
- docs are better aligned now, but this subsystem remains more fragile than ordinary app code

Impact:

- runtime and packaged-app behavior still deserve stricter verification than normal feature edits

Decision: tighten only with explicit verification

#### 4. Test organization improved, but naming/ownership can still be tightened

Evidence:

- tests are split across `tests/admin/`, `tests/bridge/`, root `tests/`, and frontend subtrees
- this is workable and mostly documented
- it is still slightly inconsistent for first-time contributors

Impact:

- low runtime risk
- moderate AI-navigation friction

Decision: keep as-is for now, then normalize only if it can be done without broad movement

### Low

#### 6. Working tree "pending files" still include generated outputs that should not steer architecture decisions

Evidence:

- `git diff --stat` shows only generated data artifacts changing
- no local code file is pending in the working tree

Impact:

- these files should not influence architectural decisions
- future audits should explicitly separate runtime output churn from code churn

Decision: defer from code refactor review; ignore unless the task is data pipeline validation

## Pending Local File Triage

Current local pending files should be treated as runtime artifacts, not architectural proposals.

| Pending file group | Type | Default action | Reason |
| --- | --- | --- | --- |
| `data/jobs-unified.*` | generated output | defer from refactor review | reflects pipeline runs, not code architecture |
| `data/jobs-fetch-*.json` | generated runtime/report state | defer from refactor review | operational evidence only |
| `data/source-discovery-*.json` | generated discovery state | defer from refactor review | useful as telemetry, not pending code |
| `.cursor/` | local tooling state | ignore | not part of repo architecture |
| `data/local-user-data/` | local runtime state | ignore | user/runtime data, not code |

## Recommended Roadmap

### Immediate

- Completed in the local follow-up wave:
  - kept architecture map paths in sync with the real repo
  - made manifest test state explicitly distinguish `not_run` from `failed`
  - documented active compatibility seams in contributor-facing docs
  - added an explicit no-new bridge module-global patch-surface guardrail
  - added a frontend runtime size guardrail
  - replaced the remaining package-internal broad `src.jobs.common` imports in `pipeline.py`, `transport.py`, and `adapters/__init__.py`
  - made `src/jobs/common/__init__.py` declare its curated compatibility surface explicitly
  - moved registry auto-sync persistence/start flow behind `src/bridge/registry_sync_flow.py`
  - removed stale sync-normalization helper leftovers from `src/admin_bridge.py`
- Current immediate targets:
  - keep reducing legacy convenience re-exports in `src/jobs/common` only when repo-internal callers no longer need them
  - keep `src/admin_bridge.py` on a composition-root diet by moving only real business seams out of the entrypoint
  - only revisit frontend runtime splits if new behavior starts regrowing the entrypoints

### Next

- Simplify jobs compatibility surfaces:
  - reduce remaining legacy re-export density in `src/jobs/common`
  - keep `_runtime.facade()` usage capped to the current compatibility boundary
- Continue bridge tightening only if it stays narrow:
  - move more handler/wiring behavior behind typed bridge modules or `BridgeApi`
  - reduce direct global monkeypatch dependence in admin tests where practical
- Add backend structural guardrails only where they protect the new helper boundaries from regrowth

### Later

- Normalize remaining test ownership conventions if it can be done surgically
- Add a compact machine-readable "edit map" for key subsystem entrypoints if contributor churn remains high
- Revisit optional refactors only when repetition justifies them:
  - generic static fallback plugin
  - state-to-DOM helper abstraction

## Default Decisions For Follow-up Refactors

- Prefer narrow tightening over another repo-wide architectural pass
- Do not move files simply for symmetry unless the move also reduces edit ambiguity
- Keep compatibility wrappers only when they protect tests or stable public behavior
- When a compatibility seam remains, guard it with a test and document it as temporary
- Require every new refactor to name:
  - target boundary
  - expected stability gain
  - expected AI-accessibility gain
  - cheapest verification path
  - rollback trigger

## Recommended First Follow-up Tickets

1. Reduce compatibility-heavy surface area in `src/jobs/common`.
2. Continue shrinking `admin_bridge.py` only through narrow composition-root extractions.
3. Add small guardrails around the new backend helper boundaries if they start drifting.
4. Normalize remaining test ownership only if it can stay surgical.
5. Revisit frontend/runtime tightening only if new churn regrows the entrypoints.

# Refactor Stability + AI Accessibility Audit

Date: 2026-03-18

## Summary

The refactor wave has materially improved the repo for both stability and AI-assisted work, especially in test organization, explicit data contracts, frontend slice boundaries, and backend module extraction. The codebase is in a better state than before, and the local tightening follow-up has now reduced some of the most obvious residual compatibility/orchestration hotspots, but it is not yet in a fully "settled" modular shape.

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

The latest local tightening pass also completed the next highest-value steps:

- more repo-internal jobs callers now import from `src.jobs.common.config`, `social`, `sources`, or `url` directly instead of treating `src.jobs.common` as the default package surface
- provider-api registration now imports `GREENHOUSE_JOBS_URL_TEMPLATE` from `src/jobs/common/config.py`, so the canonical definition no longer lives only in the compatibility barrel
- jobs diagnostics helpers now live in `src/jobs/common/diagnostics.py`, and registry default/provider-redundancy data now live in `src/jobs/common/registry_defaults.py`
- the remaining package-internal barrel-heavy callsites in `registry.py`, `adapters/social.py`, `adapters/static.py`, `adapters/static_scrapy.py`, `adapters/__init__.py`, and `adapters/community/__init__.py` now import from direct submodules or package-owned surfaces
- `src/bridge/source_check_api.py` now owns the source-check orchestration flow, and `src/admin_bridge.py` delegates `normalize_manual_static_studio_fields(...)` and `trigger_source_check(...)` into that bridge module
- `src/bridge/task_launch_api.py` now owns subprocess launch and fetcher-arg assembly for fetch/discovery task starts, and `src/admin_bridge.py` keeps only thin wrappers for `run_background_script(...)` and `build_fetcher_args_from_payload(...)`
- `src/bridge/ops_api.py` now owns fetch-report filtering, run-history reconciliation assembly, ops-health dependency assembly, and fetcher-metrics assembly, and `src/admin_bridge.py` now keeps thin wrappers for those route-facing helpers too
- `BridgeApi` now exposes typed route-facing callables for `append_startup_metric`, `persist_state_and_auto_sync`, `add_manual_source`, and `trigger_source_check`
- `src/jobs/common/__init__.py` has now pruned several stale compatibility re-exports, so the curated facade is smaller than in the first tightening pass
- jobs package guardrails and suite-contract checks now cover the narrower compatibility, task-launch, and ops-orchestration expectations explicitly

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

- `_out/LATEST_MANIFEST.json` now reports a successful build on 2026-03-18 at `17:25:22`, but the compatibility booleans `py_tests_ok` and `node_tests_ok` are still both `false`
- `docs/architecture-ai-map.md`, `docs/testing.md`, `docs/DATA_CONTRACT.md`, and `frontend/shared/ui/selectors.js` are all present and actively maintained
- contributor-facing maps exist for both architecture and frontend slice ownership
- local code now supports explicit HUD test-lane status fields so future manifests can distinguish `not_run` from `failed`
- the manifest is now slightly stale relative to the local verification state captured below

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
- `py -3 -m pytest tests/test_jobs_package.py tests/test_no_new_runtime_facade_usage.py tests/test_suite_contract.py -q` -> passed
- `py -3 -m pytest tests/test_jobs_fetcher.py -q` -> passed
- `py -3 -m pytest tests/admin/ -q` -> passed
- `py -3 -m py_compile src/admin_bridge.py src/bridge/api.py src/bridge/source_check_api.py src/bridge/task_launch_api.py src/jobs/common/diagnostics.py src/jobs/common/registry_defaults.py src/jobs/common/__init__.py src/jobs/common/config.py src/jobs/common/registry.py src/jobs/registry.py src/jobs/adapters/__init__.py src/jobs/adapters/community/__init__.py src/jobs/adapters/social.py src/jobs/adapters/static.py src/jobs/adapters/static_scrapy.py` -> passed
- `py -3.13` with a repo-local tempdir shim reran `tests/bridge/test_routes_smoke.py tests/bridge/test_sync_service.py -q` -> 8 passed
- `py -3 -m py_compile src/admin_bridge.py src/bridge/ops_api.py src/jobs/common/__init__.py` -> passed
- direct `py -3 -m pytest tests/bridge/test_routes_smoke.py tests/bridge/test_sync_service.py -q` still fails in this shell because Windows temp-root creation/cleanup hits sandbox permission errors under `%LOCALAPPDATA%\\Temp`
- `py -3.13` with the repo-local tempdir shim reran `tests/bridge/test_routes_smoke.py tests/bridge/test_sync_service.py -q` after the latest `ops_api` extraction -> 8 passed

The bridge rerun matters because the earlier failure mode was environmental, not architectural:

- Python `tempfile` / pytest temp-root creation under this sandbox can produce inaccessible Windows temp directories
- the failing pattern was tied to the tempdir/cleanup path and Windows permission handling, not bridge route or sync assertions
- when pytest temp roots were forced into a repo-local writable directory created with plain `mkdir()` semantics, both previously blocked bridge files passed cleanly

## Subsystem Decision Table

| Subsystem | Stability | AI Access | Recommended action | Notes |
| --- | --- | --- | --- | --- |
| Frontend slice architecture | 5/5 | 5/5 | Keep as-is | Both large runtime entrypoints are now under guardrail and slimmer, with non-boot concerns moving into slice-local helpers. |
| Jobs pipeline and adapters | 4/5 | 5/5 | Tighten | `pipeline.py` and `static.py` are materially narrower, package-internal callers now use direct `jobs.common` submodules, and the remaining risk is mostly the still-large compatibility facade implementation rather than broad internal import misuse. |
| Admin bridge | 4/5 | 4/5 | Tighten | Runtime state, run-history, registry auto-sync flow, sync task worker flow, source-check orchestration, task-launch assembly, and ops/report orchestration are now extracted and guarded, but `admin_bridge.py` remains a large composition root with more wiring to trim over time. |
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
- diagnostics and registry-default data now have dedicated `jobs.common` submodules instead of living only in the compatibility facade
- package tests now guard the new helper boundaries explicitly

### 6. Bridge route-facing seams are now more explicit

The latest bridge follow-up improved entrypoint clarity without over-splitting the system:

- source-check flow now has a real bridge module home in `src/bridge/source_check_api.py`
- fetch/discovery task launch flow now has a real bridge module home in `src/bridge/task_launch_api.py`
- ops/report orchestration now has a real bridge module home in `src/bridge/ops_api.py`
- `admin_bridge.py` keeps stable wrappers where useful, but those wrappers now delegate immediately for source-check behavior
- `admin_bridge.py` now also keeps stable wrappers for task launch/arg building while delegating the business logic immediately
- `admin_bridge.py` now also keeps stable wrappers for failed-source filtering, run-history reconciliation, ops-health payload assembly, and fetcher-metrics assembly
- `BridgeApi` now carries explicit typed callables for startup metrics, state persistence/auto-sync, manual-source addition, and source checks
- suite-contract tests now protect those route-facing expectations directly, including the newer task-launch and ops/report seams

## Ranked Findings

### High

#### 1. `src/jobs/common` still carries compatibility-heavy implementation surface area

Evidence:

- `_runtime.facade()` still exists as a compatibility boundary
- `src/jobs/common/__init__.py` is still large even after diagnostics/default-data moved out, more package-internal callers moved to direct submodules, and stale curated exports were pruned
- the package surface is safer than before, but compatibility affordances are still broad enough to attract new coupling

Impact:

- modularization gains are real, repo-internal imports are better aligned now, and the curated export list is smaller, but dependency clarity still depends on keeping the facade implementation from regrowing
- legacy convenience imports still exist for facade compatibility, so regrowth has to stay guarded

Decision: tightening wave in progress; keep capping compatibility spread

#### 2. `admin_bridge.py` is still a residual composition hotspot

Evidence:

- `src/admin_bridge.py` remains one of the largest live Python files in the repo
- the extracted `BridgeApi` exists, and source-check, task-launch, and ops/report orchestration now moved out too, but the entrypoint still holds substantial wiring and compatibility responsibility
- service-level and route-level tests are in better shape, but some admin tests still rely on entrypoint monkeypatch seams by design

Impact:

- stability risk is lower than at audit start, and source-check behavior is less entangled, but edits in this file still have a larger than ideal blast radius
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
  - moved more jobs package internals off the broad barrel in `canonicalize.py`, `parsers.py`, `reporting.py`, `state.py`, and plugin/provider registration paths
  - moved the canonical `GREENHOUSE_JOBS_URL_TEMPLATE` definition into `src/jobs/common/config.py` while preserving compatibility imports
  - moved diagnostics helpers into `src/jobs/common/diagnostics.py`
  - moved default registry rows and provider-redundancy rules into `src/jobs/common/registry_defaults.py`
  - moved the last package-internal barrel-heavy jobs callsites onto direct submodules or package-owned surfaces
  - moved source-check orchestration behind `src/bridge/source_check_api.py`
  - moved fetch/discovery task-launch assembly behind `src/bridge/task_launch_api.py`
  - moved ops/report orchestration behind `src/bridge/ops_api.py`
  - pruned stale compatibility exports from `src/jobs/common/__init__.py` while preserving `src.jobs_fetcher` and compat-test behavior
  - wired `append_startup_metric`, `persist_state_and_auto_sync`, `add_manual_source`, and `trigger_source_check` through `BridgeApi`
  - added structure tests that guard the narrowed `jobs.common` compatibility list and the new ops/report delegation seam
  - verified the previously blocked bridge route/sync tests by rerunning them with a repo-local pytest tempdir shim that avoids the sandbox's broken Windows temp permissions
- Current immediate targets:
  - keep `src/jobs/common/__init__.py` from regrowing now that the curated export list has been pruned
  - keep `src/admin_bridge.py` on a composition-root diet by targeting only the remaining wiring/helper clusters that still combine real behavior
  - add only narrowly scoped backend guardrails if a new seam appears without coverage
  - only revisit frontend runtime splits if new behavior starts regrowing the entrypoints

### Next

- Simplify jobs compatibility surfaces:
  - keep reducing legacy implementation density inside `src/jobs/common` only when a change materially clarifies ownership
  - keep the newer `diagnostics` and `registry_defaults` modules as the owning homes instead of letting data/helpers drift back into the facade
  - keep `_runtime.facade()` usage capped to the current compatibility boundary
- Continue bridge tightening only if it stays narrow:
  - move more handler/wiring behavior behind typed bridge modules or `BridgeApi`
  - prefer remaining high-signal seams such as residual sync-config wrapper clusters or route-local wiring, not request-dispatch or bootstrap churn
  - reduce direct global monkeypatch dependence in admin tests where practical
- Add backend structural guardrails only where they protect new helper boundaries from regrowth

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

1. Keep `src/jobs/common` compatibility pruning surgical and only remove more exports when a concrete compat caller disappears.
2. Continue shrinking `admin_bridge.py` only through one narrow remaining wiring seam at a time.
3. Add the smallest missing structure tests only when a new extracted seam needs protection.
4. Normalize remaining test ownership only if it can stay surgical.
5. Revisit frontend/runtime tightening only if new churn regrows the entrypoints.

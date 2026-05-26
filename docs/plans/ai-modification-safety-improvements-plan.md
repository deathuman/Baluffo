# AI Modification Safety Improvements Plan

> - **Status:** Active plan, advisory-only
> - **Use this when:** reducing AI edit risk around compatibility roots, bridge route contracts, large report builders, task lifecycle/storage flows, or packaged desktop release surfaces
> - **Canonical for:** proposed sequencing for making high-risk code easier for AI and human maintainers to modify safely
> - **Not canonical for:** current endpoint payloads, data schemas, runtime behavior, release requirements, or subsystem ownership
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../storage-contract.md`](../storage-contract.md), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-13

## Summary

Baluffo already has strong docs-first routing, compatibility-surface warnings, and targeted verification lanes. The remaining AI modification risk is concentrated in places where the code is correct but difficult to reason about safely:

```text
thin compatibility roots
+ handwritten route strings
+ dict-shaped payload contracts
+ large report/policy builders
+ long-running task lifecycle and storage authority flows
+ packaged desktop/update side effects
```

The goal of this plan is to make those boundaries easier to identify, type, search, and verify without changing product behavior and without adding dependencies.

## Current Repo Check

The audit found these recurring high-risk patterns.

### Compatibility Roots And Monkeypatch Surfaces

Files such as [`../../src/admin_bridge.py`](../../src/admin_bridge.py), [`../../src/jobs_fetcher.py`](../../src/jobs_fetcher.py), [`../../src/source_sync.py`](../../src/source_sync.py), [`../../src/source_discovery.py`](../../src/source_discovery.py), [`../../src/local_data_store.py`](../../src/local_data_store.py), [`../../src/packaged_desktop_smoke.py`](../../src/packaged_desktop_smoke.py), and [`../../src/ship/desktop_updater.py`](../../src/ship/desktop_updater.py) intentionally expose stable names while implementation lives in leaf modules.

Risk: an AI agent may treat the root file as the owning implementation, inline behavior into it, delete a patch seam, or import it from a narrow helper and create circular or compatibility failures.

Current good signals:

- [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md) and [`../architecture-ai-map.md`](../architecture-ai-map.md) already identify many thin surfaces.
- [`../../src/jobs/common/__init__.py`](../../src/jobs/common/__init__.py) clearly states that it is a package marker only.

Remaining gap: the warning is mostly prose. The root files themselves do not consistently expose a compact "edit here instead" header or a machine-checkable boundary list.

### Bridge Routes And Frontend Payload Builders

Bridge route contracts span Python route handlers and vanilla JS callers:

- GET routes: [`../../src/bridge/routes/get_routes.py`](../../src/bridge/routes/get_routes.py)
- POST route family: [`../../src/bridge/routes/post_routes.py`](../../src/bridge/routes/post_routes.py), [`../../src/bridge/routes/post_routes_admin.py`](../../src/bridge/routes/post_routes_admin.py), [`../../src/bridge/routes/post_routes_local_data.py`](../../src/bridge/routes/post_routes_local_data.py), [`../../src/bridge/routes/post_routes_update.py`](../../src/bridge/routes/post_routes_update.py)
- Frontend API client: [`../../frontend/shared/api-client.js`](../../frontend/shared/api-client.js)
- Admin fetch task caller: [`../../frontend/admin/app/fetcher.js`](../../frontend/admin/app/fetcher.js)
- Jobs pipeline status caller: [`../../frontend/jobs/app/runtime/pipeline-controller.js`](../../frontend/jobs/app/runtime/pipeline-controller.js)
- Desktop local-data client: [`../../frontend/shared/local-data/desktop/api.js`](../../frontend/shared/local-data/desktop/api.js)

Risk: route path, request shape, response shape, busy-state, task-start, and log-polling behavior can drift independently. Some local-data frontend calls use relative route fragments under a prefixed base URL, so literal full-route searches can miss them.

Current good signals:

- [`../admin-bridge-api.md`](../admin-bridge-api.md) documents endpoint intent.
- `frontend/shared/api-client.js` centralizes many bridge fetch semantics.

Remaining gap: route names and payload types are not executable enough. There is no small, shared inventory that maps route path -> Python handler -> frontend caller -> verification lane.

### Task Launch, Lifecycle, And Storage Authority

[`../../src/bridge/task_launch_api.py`](../../src/bridge/task_launch_api.py) owns several concerns in one class:

- fetch preset payload parsing and CLI argument construction
- background fetch process launch
- active-run response behavior
- fetch lifecycle heartbeat and terminal closeout
- source-run SQLite mirroring and rollback
- jobs-feed SQLite mirroring and compatibility exports
- fetch-report archive and compaction

Risk: small changes can accidentally affect task liveness, current-run projection, report closeout, storage rollback, or frontend busy-state behavior.

Current good signals:

- [`../storage-contract.md`](../storage-contract.md) documents authority modes, rollback, WAL discipline, and export behavior.
- [`../admin-bridge-api.md`](../admin-bridge-api.md) documents task-state and task-live expectations.

Remaining gap: code boundaries are still named around implementation details rather than the compatibility decisions future editors must preserve.

### Large Evidence And Policy Builders

The largest high-risk report/policy files are:

- [`../../src/jobs/reporting_dedup_evidence.py`](../../src/jobs/reporting_dedup_evidence.py)
- [`../../src/bridge/registry_conflicts.py`](../../src/bridge/registry_conflicts.py)
- [`../../frontend/admin/render/ops-summary.js`](../../frontend/admin/render/ops-summary.js)

Risk: these files combine policy classification, diagnostic sampling, output shaping, and UI presentation. Most payloads are `dict[str, Any]`, `Mapping[str, Any]`, or generic JS objects, so renaming fields or changing semantics is easy to do without a type-level signal.

Current good signals:

- [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) documents many payload fields.
- [`../../src/core/schemas.py`](../../src/core/schemas.py) validates core job, saved-job, local-data, backup, and manifest rows.
- [`../../frontend/shared/types.js`](../../frontend/shared/types.js) documents core frontend job and saved-job shapes.

Remaining gap: dedup evidence, registry conflict rows, live task payloads, and Admin/Ops render inputs are not yet covered by small typed contracts.

### Source Discovery And Static Adapter Flows

High-risk discovery and static adapter flow files include:

- [`../../src/source_discovery/gamedevmap_active_dry_run.py`](../../src/source_discovery/gamedevmap_active_dry_run.py)
- [`../../src/source_discovery/web_search_candidates.py`](../../src/source_discovery/web_search_candidates.py)
- [`../../src/jobs/adapters/static_listing.py`](../../src/jobs/adapters/static_listing.py)

Risk: these modules coordinate staged audits, recovery artifacts, browser/HTTP fallback, probe evidence, candidate generation, and source-row shaping. AI agents can change a helper that looks local but affects source-policy evidence or discovery yield.

Current good signals:

- [`../scraping-pipeline.md`](../scraping-pipeline.md) and [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md) route subsystem ownership.
- Static plugin migration work is already tracked separately in [`static-plugin-simple-runner-migration-plan.md`](static-plugin-simple-runner-migration-plan.md).

Remaining gap: stage-result shapes are mostly dict-shaped, and the long flow files do not consistently advertise which helpers are pure classifiers, mutating artifact writers, or external-network/runtime stages.

### Packaged Desktop And Updater Surfaces

High-risk packaged/release surfaces include:

- [`../../src/ship/desktop_app/_windows.py`](../../src/ship/desktop_app/_windows.py)
- [`../../src/ship/desktop_updater_install.py`](../../src/ship/desktop_updater_install.py)
- [`../../src/packaged_desktop_smoke.py`](../../src/packaged_desktop_smoke.py)
- [`../RELEASE.md`](../RELEASE.md)
- [`../testing.md`](../testing.md)

Risk: these files touch process ownership, Windows APIs, rollback snapshots, update handoff, relaunch verification, and release-critical smoke paths.

Current good signals:

- [`../testing.md`](../testing.md) lists the packaged rehearsal lanes.
- [`../RELEASE.md`](../RELEASE.md) documents update and rollback requirements.

Remaining gap: risky code paths are protected by tests and docs, but the code can still look like ordinary helper logic to an AI agent unless the local names and comments make side effects obvious.

## Goals

1. Make high-risk edit boundaries obvious from file names, local headers, and index routing.
2. Reduce route and payload drift between Python bridge code, frontend callers, and docs.
3. Add lightweight typing to dict-shaped contracts where it most improves edit safety.
4. Split the largest policy/report builders into named ownership slices without changing output shape.
5. Make long-running task lifecycle and storage authority changes easier to verify in narrow lanes.
6. Preserve current product behavior, public payload keys, persisted data, and compatibility exports.
7. Keep improvements dependency-free and compatible with the existing Python and vanilla JS stack.

## Non-Goals

- Do not add new Python or Node dependencies.
- Do not convert the frontend to TypeScript, React, Vite, or another framework.
- Do not remove compatibility roots unless a separate compatibility plan covers every caller and test patch seam.
- Do not change endpoint paths, payload field names, or persisted data shapes as part of a structural cleanup.
- Do not broaden this into source-policy behavior changes, dedup policy changes, storage migration work, or packaged release work.
- Do not use automated formatting or broad refactors that obscure behavior-preserving intent.

## Implementation Plan

### 1. Add Local AI Boundary Markers

Completed 2026-05-26 for the first safety slice. The highest-risk
compatibility roots and GET/POST route surfaces now carry compact local
AI-boundary headers. Keep future marker maintenance concise and focused on
ownership, caller searches, and the narrowest verification lane.

Add compact headers to the highest-risk compatibility roots and route surfaces:

- [`../../src/admin_bridge.py`](../../src/admin_bridge.py)
- [`../../src/jobs_fetcher.py`](../../src/jobs_fetcher.py)
- [`../../src/source_sync.py`](../../src/source_sync.py)
- [`../../src/source_discovery.py`](../../src/source_discovery.py)
- [`../../src/local_data_store.py`](../../src/local_data_store.py)
- [`../../src/packaged_desktop_smoke.py`](../../src/packaged_desktop_smoke.py)
- [`../../src/ship/desktop_updater.py`](../../src/ship/desktop_updater.py)
- [`../../src/bridge/routes/get_routes.py`](../../src/bridge/routes/get_routes.py)
- [`../../src/bridge/routes/post_routes.py`](../../src/bridge/routes/post_routes.py)

Each marker should state:

- what the file is allowed to own
- where new implementation logic should go instead
- what callers or frontend payload builders to search before changing signatures
- the narrowest relevant verification lane

Keep the marker short. It should prevent wrong edits, not duplicate the architecture map.

### 2. Maintain The Route Contract Inventory

Completed 2026-05-13. The dependency-free route inventory now lives in
`tools/repo_health/bridge_route_inventory.py`, and `npm run lint:repo-guardrails`
runs the `routes` group to catch handler/inventory/API-doc drift. Keep this
section as maintenance guidance for future route edits, not as new-build work.

The inventory should map:

- HTTP method
- route path
- Python handler module
- primary frontend caller or caller family
- contract doc owner
- focused verification lane

Current focus:

- `/tasks/run-fetcher`
- `/tasks/run-jobs-pipeline`
- `/tasks/run-jobs-pipeline-status`
- `/ops/task-state`
- `/ops/task-live/<taskType>`
- `/ops/fetch-report`
- `/ops/fetch-report/sources`
- `/registry/conflicts`
- `/registry/conflicts/auto-demote-safe`
- `/registry/conflicts/check-sources`
- `/desktop-local-data/*`
- `/sync/*`

Keep exceptions explicit for low-value diagnostics: internal routes should carry
an inventory rationale rather than being forced into the public API reference.

### 3. Add Lightweight Typed Payload Contracts

Started 2026-05-26 for the first safety slice. Shared live-task/task-state
Python `TypedDict` contracts and frontend JSDoc typedefs now cover the first
bridge/frontend boundaries. Continue this section incrementally; do not convert
the advisory contracts into runtime validation unless a narrower behavior plan
requires it.

Continued 2026-05-26 with advisory dedup evidence and audit-gate Python
contracts plus matching Admin/Ops JSDoc contracts. The full dedup builder split
remains pending.

Add narrow typing where it reduces accidental field drift. Prefer `TypedDict`, dataclasses, existing Pydantic schemas, and JSDoc typedefs.

Candidate Python contracts:

- fetch task start request and response
- current task-state route row
- live task payload, event, work item, and progress shapes
- dedup audit gate detail rows
- provider/static disagreement rows
- registry conflict card and safe-automation result rows
- source-policy recommendation/review rows where Admin/Ops renders them

Candidate frontend contracts:

- Admin/Ops fetch report metrics input
- dedup evidence and audit gate input
- registry conflict card input
- task-live/task-state input
- desktop-local-data route payloads

Use the new types at boundary helper signatures first. Do not attempt to type every internal helper in the same slice.

### 4. Split Dedup Evidence Ownership

Refactor [`../../src/jobs/reporting_dedup_evidence.py`](../../src/jobs/reporting_dedup_evidence.py) behind the existing public entrypoints:

- keep `build_dedup_evidence(...)` stable
- keep `build_dedup_audit_gate(...)` stable
- extract provider/static disagreement helpers
- extract Google Sheets role-bucket audit helpers
- extract merge-example and non-primary merge gate helpers
- extract final audit-gate detail assembly helpers

Acceptance guard: the final `dedupEvidence` payload remains byte-contract compatible except for intentionally documented additive fields.

### 5. Split Registry Conflict Ownership

Refactor [`../../src/bridge/registry_conflicts.py`](../../src/bridge/registry_conflicts.py) behind the existing public entrypoints:

- keep `derive_registry_conflict_queue(...)` stable
- keep `load_registry_conflicts_payload(...)` stable
- keep `apply_registry_conflict_safe_demotions(...)` stable
- extract conflict row normalization and family-card comparison
- extract safe automation eligibility analysis
- extract safe automation mutation/apply helpers
- extract report/audit payload assembly

Acceptance guard: `/registry/conflicts` and `/registry/conflicts/auto-demote-safe` keep their existing route payload shape and reversible behavior.

### 6. Narrow Task Launch Responsibilities

Refactor [`../../src/bridge/task_launch_api.py`](../../src/bridge/task_launch_api.py) only after route and payload contracts are clearer.

Candidate extraction boundaries:

- `FetcherLaunchOptions` or equivalent typed parse result for `/tasks/run-fetcher`
- fetch CLI argument builder
- fetch lifecycle watcher
- source-run mirror/rollback helper
- jobs-feed mirror/export helper
- fetch-report archive/compact helper

Acceptance guard: task-start, already-running attach, busy-state, `/ops/task-state`, `/ops/task-live/fetch`, log polling, terminal closeout, storage rollback, and compatibility exports are verified together.

### 7. Partition Admin Ops Rendering

Split [`../../frontend/admin/render/ops-summary.js`](../../frontend/admin/render/ops-summary.js) by display ownership while preserving current exported functions:

- dedup evidence and audit gate rendering
- provider/static disagreement rendering
- source-policy rendering
- fetch metrics rendering
- task lane rendering

Add JSDoc imports from shared typedefs so field names are easier to discover before changing backend payloads.

Acceptance guard: frontend unit tests cover representative payloads for each partition, including missing optional fields.

### 8. Label Discovery And Static Adapter Flow Stages

For discovery and static adapter files, clarify stage boundaries before deeper refactors:

- classify pure row/candidate inference helpers
- classify artifact read/write helpers
- classify runtime/network/probe helpers
- classify mutation or registry-facing helpers

Then extract only where this lowers risk:

- stage result dataclasses for repeated dict payloads
- small modules for browser recovery artifact handling
- small modules for page outcome/candidate assembly
- plugin/static listing flow helpers that align with [`static-plugin-simple-runner-migration-plan.md`](static-plugin-simple-runner-migration-plan.md)

Acceptance guard: discovery yield, source-policy report shape, and static adapter behavior stay unchanged unless a separate behavior plan says otherwise.

### 9. Add Packaged Desktop Risk Labels And Verification Hooks

For packaged desktop and updater code, prefer explicit side-effect names over generic helper names:

- process ownership
- rollback snapshot
- install mutation
- relaunch verification
- stale runtime reclaim
- startup readiness proof

Add local comments only where Windows API or updater side effects are non-obvious.

Acceptance guard: changes to these files use the relevant packaged rehearsal lane from [`../testing.md`](../testing.md).

## Suggested Sequencing

1. Route contract inventory and local AI boundary markers.
2. Typed payload contracts for task-live/task-state and `/tasks/run-fetcher`.
3. Dedup evidence split.
4. Registry conflict split.
5. Task launch extraction.
6. Admin Ops render partition.
7. Discovery/static stage labels and limited extraction.
8. Packaged desktop local labels and side-effect naming cleanup.

This order makes later refactors safer by first improving searchability and route/payload visibility.

## Verification Matrix

| Change area | Minimum verification |
|---|---|
| Docs-only update to this plan | No code tests required; inspect links and index entry |
| AI boundary comments only | `npm run lint:repo-guardrails` if comments affect guarded files |
| Route inventory or route contract tests | focused route inventory test plus `python -m pytest tests/admin/ -q` when bridge routes move |
| `/tasks/run-fetcher` payload or launch parsing | `python -m pytest tests/admin/test_admin_bridge_task_launch.py -q` and nearest frontend unit test |
| Task-state/task-live payload typing | `python -m pytest tests/admin/ -q` plus `npm run test:frontend:unit` when frontend view models change |
| Dedup evidence split | focused dedup tests under `tests/test_jobs_dedup_*.py` and fetch report normalization tests |
| Registry conflict split | focused registry conflict/Admin route tests under `tests/admin/` |
| Admin Ops rendering split | `npm run test:frontend:unit` |
| Static adapter/discovery flow stage extraction | nearest `tests/jobs_static/` or `tests/source_discovery/` slice |
| Packaged desktop/updater naming or flow changes | matching packaged rehearsal lane from [`../testing.md`](../testing.md) |

For compatibility root changes, run `npm run test:refactor:changed`.

## Acceptance Criteria

- Future AI/human maintainers can identify the owning file for a high-risk edit in one or two searches.
- Route changes have an obvious frontend caller list and focused verification lane.
- New high-risk payload fields are represented in docs and at least one typed boundary.
- Large report builders are smaller without changing public payload shape.
- Compatibility roots remain thin and documented.
- No new dependencies are introduced.
- No persisted data contract, public job text, route path, release behavior, or compatibility export changes unintentionally.

## Open Questions

- Whether route inventory should live as a Markdown table, a small Python data module, a JS data module, or both. Prefer the lowest-maintenance option that can be checked by tests.
- Whether `frontend/shared/types.js` should remain the single JSDoc typedef file or split into route-specific typedef modules.
- Whether typed contract work should extend the existing Pydantic schemas or stay as `TypedDict` for diagnostic payloads that are intentionally lenient.

## Notes

This plan is a safety and maintainability plan. It should not be used as permission to change behavior in dedup, registry automation, discovery, storage authority, or packaged update flows. Behavior changes need their own narrower plan or task.

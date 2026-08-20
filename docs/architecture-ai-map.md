# Baluffo Architecture AI Map

> - **Status:** Active
> - **Use this when:** locating the correct subsystem, choosing edit boundaries, or mapping task-to-files
> - **Canonical for:** system boundaries, task routing, compatibility-surface detail, and the expanded verification matrix
> - **Not canonical for:** endpoint payloads or data schema details
> - **Then inspect:** the minimal source files listed in the task table, plus the matching contract doc if shape changes are involved
> - **Last updated:** 2026-08-20 (reporting_dedup_evidence 1,133→12 + state_lifecycle 1,121→38 splits: 12/12 and 45/49+4 seam)
>
> Start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) first. Retired boundary-charter detail now lives in git history; this map is the current routing source.
> For any file described below as a stable thin surface, compatibility surface, or monkeypatch surface, preserve the root-level exported names that tests or leaf modules patch through that root unless the matching contract tests and docs are updated in the same change.

---

## 1) System boundary map

```text
jobs.html / saved.html / admin.html
  -> styles/base.css + styles/components.css + styles/<page>.css
  -> frontend/{jobs|saved|admin}/index.js
  -> frontend/{jobs|saved|admin}/app.js
  -> frontend/{jobs|saved|admin}/app/runtime.js
      -> page modules (app/*.js + actions/services/render/domain/data-source)
      -> runtime leaves (frontend/{jobs|saved|admin}/app/runtime/*.js + frontend/saved/app/admin-bridge-state.js)
      -> shared (frontend/shared/*, selectors.js, api-client.js, state-hub.js)

src/dev_admin_supervisor.py (Baluffo launcher)
  -> starts site + bridge + owned browser session
  -> tears down on session exit

src/admin_bridge.py (stable thin entrypoint / wiring-only composition root)
  -> src/bridge/ (services: sync, registry, discovery, pipeline, routes)
  -> src/bridge/admin_entrypoint_{runtime,services,registry_api,task_runtime}.py

src/container_gateway.py (container public same-origin gateway)
  -> serves startup static assets + /app/ready + pipeline status/abort control plane
  -> proxies non-critical/full API routes to internal bridge

src/container_server.py (container internal bridge worker)
  -> src/bridge/server/handler.py + src/bridge/server/static_files.py
  -> src/runtime_seed.py
  -> same bridge route surface with container-only desktop route suppression

src/jobs_fetcher.py (stable thin CLI facade)
  -> src/jobs/ (pipeline, adapters, dedup)
  -> src/jobs/fetcher_compat_{exports,runtime}.py
src/jobs/adapters/static.py (stable static adapter surface)
  -> src/jobs/adapters/static_{runtime,listing,detail,sources}.py
  -> src/jobs/adapters/static_{runtime_support,detail_heuristics}.py
  -> plugins/static/*
src/source_discovery.py (stable thin CLI entrypoint)
  -> src/source_discovery/ (package)

src/ship/desktop_app/ (desktop runtime package)
  -> launcher.py stays the package-private orchestration surface
  -> launcher_{flow,diagnostics,recovery}.py own launch flow, diagnostics, and recovery helpers
  -> startup.py stays the package-private readiness/watchdog surface
  -> startup_{ready,watchdog}.py own readiness, handoff, heartbeat, and watchdog flow
  -> browser.py / session.py / _windows.py / _linux.py / config.py own focused helpers
  -> __init__.py dispatches _windows or _linux via _COMPAT_MODULES based on os.name

src/packaged_desktop_smoke.py (stable packaged smoke entrypoint / monkeypatch surface)
  -> src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals}.py
  -> src/ship/packaged_smoke/rehearsal_{sync,update,browser}.py

src/ship/desktop_update.py (stable updater surface)
  -> src/ship/desktop_update_{shared,state,service}.py
src/ship/desktop_updater.py (stable updater helper executable / monkeypatch surface)
  -> src/ship/desktop_updater_{ui,release,install}.py
```

---

## 2) CLI Scripts

| Script | Purpose |
|--------|---------|
| `src/jobs_fetcher.py` | Build unified jobs feed |
| `src/source_discovery.py` | Discover candidate sources (delegates to package) |
| `src/dev_admin_supervisor.py` | Baluffo launcher (site + bridge + browser) |
| `src/admin_bridge.py` | Bridge-only entry (expert/manual mode, wiring only) |
| `src/container_gateway.py` | Container public same-origin control/static gateway |
| `src/container_server.py` | Container internal bridge worker |
| `src/jobs/pipeline.py` | Stable pipeline entry flow over `pipeline_{run_setup,execution_flow,finalize}.py` |
| `src/source_discovery/` | Discovery package modules |
| `src/packaged_desktop_smoke.py` | Packaged smoke CLI and rehearsal entry flow |
| `scripts/build_ship_bundle.py` | Create ship bundle |
| `scripts/build_portable_exe.py` | Create portable EXE |

---

## 3) Task -> minimal files

| Task | Start here | Then only if needed |
|------|------------|---------------------|
| Jobs filter/search | `frontend/jobs/app/filters.js`, `frontend/jobs/app/runtime/query.js` | `frontend/jobs/app/runtime.js` only for page-entry wiring/export changes |
| Jobs page runtime wiring | `frontend/jobs/app/feed.js`, `frontend/jobs/app/runtime/{composition,boot,page-flow,events,feed-controller,list-view,pipeline-controller,startup-preview,auth-controller}.js` | `frontend/jobs/app/runtime.js` only when the stable page-entry root must change |
| Jobs desktop-update UI | `frontend/jobs/app/desktop-update-{model,dom,controller}.js` | `frontend/jobs/app/desktop-update.js` only when the stable export surface must change |
| Jobs feed refresh | `frontend/jobs/app/feed.js` | `frontend/jobs/services.js` |
| Saved page runtime wiring | `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes,activity-controller,attachments-controller,custom-job-controller,render-controller,auth-controller}.js`, `frontend/saved/app/admin-bridge-state.js` | `frontend/saved/app/runtime.js` only when the stable page-entry/export root must change |
| Saved notes | `frontend/saved/app/notes.js` | `frontend/saved/services.js` |
| Saved attachments | `frontend/saved/app/attachments.js` | `frontend/saved/services.js` |
| Admin runtime wiring | `frontend/admin/app/runtime/{composition,overview,events,state,view,effects,actions}.js` | `frontend/admin/app/runtime.js` only for page-entry wiring/export changes |
| Admin registry | `frontend/admin/app/registry/{ui,load,mutations}.js` | `frontend/admin/app/registry.js` only for stable controller/export changes |
| Admin ops | `frontend/admin/app/ops/{format,task-state,health,bridge-status}.js`, `frontend/admin/app/{auth,fetcher,discovery,sync}.js` | `frontend/admin/app/ops.js` only for stable controller/export changes |
| Admin/Umbrel active-run route slowness or stale progress | `src/bridge/active_task_snapshot.py`, `src/bridge/ops_api.py`, `src/container_gateway.py`, `src/jobs/pipeline_runtime_writers.py`, `src/bridge/{discovery_service,sync_task_flow}.py` | Full history/detail projection modules only after active summary routes and the hot snapshot path are ruled out |
| Bridge API | `src/bridge/*.py`, `src/bridge/routes/get_*.py` | `src/bridge/routes/{get_routes,post_routes}.py` only for public delegator/registration compatibility |
| Admin bridge entrypoint/runtime wiring | `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py` | `src/admin_bridge.py` only for root-surface compatibility work |
| Container / Umbrel runtime | `src/container_gateway.py`, `src/container_entrypoint.py`, `src/container_server.py`, `src/bridge/server/{handler,static_files}.py`, `src/bridge/container_mode.py`, `src/runtime_seed.py`, `Dockerfile`, `deathuman-baluffo/*` | `src/admin_bridge.py` only for shared BridgeApi assembly compatibility work |
| Discovery behavior | `src/source_discovery/orchestrator.py`, `orchestrator_{runtime,generation,probe,finalize}.py`, `runtime_metrics.py`, `stage_control.py`, `reporting_{progress,candidates,backlog}.py`, `gamesmap_{cache,parsing,candidates}.py`, `web_search_{fetch,extract,candidates}.py` | `src/source_discovery.py` only for CLI compatibility, and `gamesmap.py`, `reporting.py`, or `web_search.py` only for stable import-surface compatibility work |
| Bridge sync | `src/bridge/sync_service.py`, `src/source_sync_{config,runtime,snapshot,crypto}.py` | `src/source_sync.py` only for root-surface compatibility work, plus `src/bridge/sync_state.py` |
| Bridge registry | `src/bridge/registry_service.py`, `src/source_registry_{identity,io,state,canonicalize,policy,auto_approval}.py` (io is a thin coordinator over `src/source_registry_io_{paths,load,journal,save}.py` — keep the DATA_DIR root-injection seam in the coordinator; the load leaf owns reading incl. the array-read chain and journal delta parsing, the journal leaf is write/maintenance side), `src/bridge/registry_tombstones.py`, `src/bridge/registry_conflicts*.py` (automation is a thin coordinator over `registry_conflicts_automation_{triage,eligibility,provider,static}.py` — triage owns the bucket tables, eligibility owns shared blockers, provider/static own the per-family analyzers; `registry_conflict_adjudication.py` is a thin coordinator over `registry_conflict_adjudication_{core,progress,probe,decide,run}.py` — core owns row/url helpers, progress owns the payload builders + `_AdjudicationProgress`, probe owns parsing/scoring, decide owns classification/selection, run owns the orchestrator + overlay; the verbatim `__all__` tail lives in the run leaf for fidelity-checker reasons; `registry_conflicts_row.py` is a thin coordinator over `registry_conflicts_row_{core,identity,path,source_state,adjudication,audit}.py` — the coordinator keeps the 23 `SAFE_AUTO_*`/`RESOLVED_PENDING_DEMOTION_REASONS` constants and the 73-name re-export surface (importers include the automation-family modules and `registry_conflicts.py`, unchanged; F401 per-file-ignored in `ruff.toml` like `admin_bridge.py`), core owns row accessors + evidence scoring + `_row_urls` (moved there to break the core↔identity cycle), identity owns url/alias/job-identity keys, path owns listing-path heuristics, source_state owns the fetch-report merge, adjudication owns family/board derivation, audit owns row assembly + diff comparison) | `src/source_registry.py` only for compatibility-surface changes |
| Jobs pipeline / fetcher behavior | `src/jobs/pipeline.py`, `src/jobs/pipeline_{run_setup,execution_flow,finalize}.py`, `src/jobs/pipeline_{runtime_writers,runtime_summary}.py`, `src/jobs/pipeline_stage_source_execution.py`, `src/jobs/pipeline_source_{loop,results,progress}.py`, `src/jobs/pipeline_timing.py`, `src/jobs/state_{source_state,lifecycle,incremental}.py`, `src/jobs/state_source_{records,browser,migration}.py`, `src/jobs/availability_{validator,schedule}.py`, `src/jobs/common/contracts_{runtime,source_reports,task_state,fetch_report}.py`, `src/jobs/reporting_{summary,queues,breakdowns,social}.py`, `src/jobs/fetcher_compat_{exports,runtime}.py`, other `src/jobs/*` leaf modules | `src/jobs_fetcher.py` only for CLI/user-facing fetcher entry changes. Internal jobs-fetcher facades and package-shape shims are simplification candidates, not permanent compatibility surfaces. |
| Job availability checks and Saved attention | `src/bridge/job_availability_service.py`, `src/local_data_store_availability.py`, `src/bridge/routes/{get_pipeline_tasks,get_local_data,post_routes_local_data}.py`, `frontend/{jobs,saved}/**`, `frontend/shared/local-data/desktop/api.js` | Keep direct-link enforcement shadow-only until the documented promotion gate passes; do not import the local-data composition root into Jobs helpers. |
| Jobs domain helpers | `frontend/jobs/domain/{query,feed,view}.js` | `frontend/jobs/domain.js` only for stable export-surface changes |
| Static adapter behavior | `src/jobs/adapters/static_{runtime,listing,detail,sources}.py`, `src/jobs/adapters/static_{runtime_support,detail_heuristics}.py` | `src/jobs/adapters/static.py` for the current adapter entrypoint. The old `static_helpers.py` facade was deleted; use direct leaf imports. |
| Local-data backend store | `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py` | `src/local_data_store.py` only for root-surface compatibility work |
| Desktop/container bridge local-data runtime | `frontend/shared/local-data/{runtime-context,app-client,desktop-client}.js`, `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`, `frontend/shared/config/admin-config.js`, `frontend/shared/api-client.js` | `frontend/shared/local-data/desktop-client.js` only for root bootstrap or `window.JobAppLocalData` wiring |
| Local-data page wiring | `frontend/<page>/services.js` | `frontend/local-data/services.js` only when the shared local-data API changes |
| Desktop runtime | `src/ship/desktop_app/{launcher_flow,launcher_diagnostics,launcher_recovery,startup_ready,startup_watchdog}.py` | `src/ship/desktop_app/{launcher,startup,browser,session,_windows,config}.py`, `src/ship/runtime_launcher.py` |
| Desktop updater helper executable | `src/ship/desktop_updater_{ui,release,install}.py` | `src/ship/desktop_updater.py` only for helper CLI/root patch-surface compatibility work |
| Packaged smoke / updater | `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`, `src/ship/desktop_update_{shared,state,service}.py` | `src/packaged_desktop_smoke.py` and `src/ship/desktop_update.py` only for CLI/public-surface compatibility work |
| UI selectors | `frontend/shared/ui/selectors.js` | - |

---

## 4) Frontend topology

**Jobs page:** `frontend/jobs/app.js` -> `runtime.js` -> `app/feed.js`, `app/filters.js`, `app/cache.js`, `app/pipeline.js`, `app/sources.js`, `domain/{query,feed,view}.js`, `runtime/{events,feed-controller,list-view,pipeline-controller,query,startup-preview,auth-controller}.js`
  -> desktop update UI stays rooted at `frontend/jobs/app/desktop-update.js` over `frontend/jobs/app/desktop-update-{model,dom,controller}.js`

**Saved page:** `frontend/saved/app.js` -> `runtime.js` -> `runtime/{state,events,auth-controller,activity-controller,attachments-controller,custom-job-controller,render-controller}.js`, `app/{notes,attachments,activity}.js`, `app/admin-bridge-state.js`, `app/view-state.js`

**Admin page:** `frontend/admin/app.js` -> `runtime.js` -> `runtime/{composition,overview,events,state,view,effects,actions}.js`, `app/{auth,fetcher,discovery,sync}.js`, `app/registry/{ui,load,mutations}.js`, `app/ops/{format,task-state,health,bridge-status}.js`
  -> render exports stay stable through `frontend/admin/render.js` -> `frontend/admin/render/ops.js`
  -> ops renderer ownership lives in `frontend/admin/render/{ops-summary,ops-history,ops-shared}.js`

**Shared:** `frontend/shared/state-hub.js` (cross-module state), `frontend/shared/api-client.js` (bridge HTTP), `frontend/shared/config/admin-config.js` (frontend-safe runtime config), `frontend/shared/local-data/runtime-context.js` (browser/desktop/container mode resolution), `frontend/shared/local-data/desktop-client.js` (stable bridge-local runtime root over `desktop/{api,lifecycle,navigation,state}.js`), `frontend/shared/local-data/browser-client.js` (browser-local runtime)

**Styles:** `styles/base.css` owns tokens and page foundations, `styles/components.css` owns shared UI primitives, and `styles/{jobs,saved,admin}.css` own page-specific polish. Change shared styling in the shared layer first; only touch page CSS when the selector is clearly page-owned.

---

## 5) Backend topology

**Bridge services (`src/bridge/`):**
- `sync_service.py`, `sync_state.py` - sync operations
- `registry_service.py` - canonical active/pending/rejected state, tombstone filtering, local persistence
- `registry_tombstones.py` - local delete ledger and restore helpers
- `discovery_service.py` - thin discovery-task coordinator: `DiscoveryService` mixin composition, `__init__` DI, `__all__` re-exports; behavior lives in the `discovery_service_{core,config,launch,lifecycle,registry,watch}.py` mixins + `discovery_service_core.py` `DiscoveryServiceState` shared base (construction types + cross-mixin stubs)
- `pipeline_service.py` - thin jobs-pipeline service coordinator (constants, `__init__` DI, type re-exports); behavior lives in the `pipeline_service_{control,lifecycle,children,status,stages}.py` mixins + `pipeline_service_types.py` shared state
- `task_launch_api.py` - thin task-launch coordinator: `TaskLaunchApi` composition, `__init__` DI, background-script launch primitives (`run_background_script`, fetcher args), and the public `start_{jobs_bootstrap,fetcher}_task` entry points; behavior lives in the `task_launch_api_{contexts,smoke,bootstrap}.py` mixins + `task_launch_api_state.py` shared types (BOOTSTRAP_* constants, dataclasses, cross-mixin stubs)
- `task_lifecycle.py` - thin persisted admin task lifecycle coordinator: `TaskLifecycleService` mixin composition, `__init__` DI, `__all__` re-exports (status constants); storage/parity + shared state live in `task_lifecycle_core.py` (`TaskLifecycleState` base with cross-mixin stubs), the `_compact_*` chain in `task_lifecycle_compact.py`, route-row projection in `task_lifecycle_rows.py`, run lifecycle in `task_lifecycle_runs.py`, legacy reconciliation in `task_lifecycle_legacy.py`
- `job_availability_service.py` - bounded direct-link checks, shadow/enforced projections, and sweep orchestration
- `routes/get_routes.py`, `routes/post_routes.py` - public GET/POST delegators only; route-owned behavior lives in narrow `get_*.py` and `post_routes_*` leaves
- `routes/get_{admin_bootstrap,admin_ops_tab_counts,app,discovery,fetch_report,local_data,ops_diagnostics,ops_status,pipeline_tasks,registry,registry_conflicts,source_policy,sync}.py` - GET route-family ownership behind the thin public delegator
- `routes/post_routes_{admin,local_data,update}.py` - POST route-family ownership behind the thin registration root
- `container_mode.py` - container-only desktop route suppression helpers
- `server/handler.py`, `server/static_files.py` - shared HTTP handler and container static/runtime-data serving
- `ops_api.py` - thin coordinator (45 lines): `OpsApi` composes five mixin leaves (`ops_api_{core,reports,live,health,task_state}.py`) over `ops_history_projection.py`, `ops_task_live.py`, `ops_task_{fetch_live,discovery_live,projection}.py`, and `ops_live_payload.py`; `OpsPaths`/`OpsDeps` live in the core leaf and are re-exported here
- `active_task_snapshot.py` - hot active-run summary artifact for bounded `/ops/task-state?view=summary` and `/ops/task-live/<task>?view=summary` during fetch/discovery/sync
- `source_check_api.py` - source probe/check helpers

**Still in `admin_bridge.py`:** bridge startup entrypoint, one-line `build_bridge_api(...)` wrapper, stable compatibility exports, and root-surface monkeypatch compatibility. Service lifetime now lives in `BridgeServices`, and former admin entrypoint helper modules receive root explicitly instead of owning module-level `root`.

**Admin bridge entrypoint helpers:**
- `admin_entrypoint_runtime.py` - bridge log emission, runtime-path rebinding, startup metrics, owner-session lifecycle helpers
- `admin_entrypoint_services.py` - cached sync/registry/discovery/pipeline/updater/task-launch/ops builders
- `admin_entrypoint_api.py` - `BridgeApi` bootstrap/dependency assembly behind the stable root wrapper
- `admin_registry_api.py` - manual-source add/update flow, source-check glue, registry-state persistence helpers
- `admin_task_runtime.py` - sync/task runtime helpers, report waits, fetch-task launch/runtime glue

**Jobs package (`src/jobs/`):**
- `pipeline.py`, `src/jobs_fetcher.py` - current fetcher entrypoints; preserve CLI/user-facing task launch behavior unless the bridge/frontend caller is updated in the same slice.
- `pipeline_run_setup.py`, `pipeline_execution_flow.py`, `pipeline_stage_source_execution.py`, `pipeline_source_{loop,results,progress}.py`, `pipeline_runtime_{writers,summary}.py`, `pipeline_timing.py`, `pipeline_finalize.py` - current pipeline implementation. `pipeline_execution_flow.py` is now a 142-line execution helper with real source-orchestration logic, not a historical shim or default simplification target. Other internals can be collapsed or rewired when the replacement is simpler and tests cover the same source execution behavior.
- `pipeline_finalize.py` - thin coordinator: the `_finalization_phase` conductor, sector gating, `finalize_pipeline_run`, and the compat re-export surface (test-pinned private helper names + monkeypatch targets). Implementation leaves: `finalize_locations.py` (location guardrails), `finalize_lifecycle.py` (timing/memory/lifecycle), `finalize_reports.py` (reports/social/failed-report writers), `finalize_output.py` (output shaping/policy export), `finalize_availability.py` (availability artifacts + feed-reconciliation decorator). Leaves resolve test-patched names (`now_iso`, `write_atomic_if_changed`, etc.) through the coordinator module at call time so the root monkeypatch seam is preserved.
- `state.py` is a removable source-state facade over `state_source_state.py`, `state_lifecycle.py`, and `state_incremental.py`; `state_source_state.py` is the current source-state implementation. Preserve persisted source-state meaning, not historical module boundaries.
- `common/contracts_{runtime,source_reports,task_state,fetch_report}.py` - current fetch/report payload normalization. Preserve active report fields used by the app.
- `reporting_{summary,queues,breakdowns,social}.py` - current report assembly. Preserve active report shapes.
- `fetcher_compat_runtime.py` - root-backed wrapper seams; the lazy compatibility export table lives in `src/jobs_fetcher.py` (merged from `fetcher_compat_exports.py`)
- Existing jobs root injection appears in `fetcher_compat_runtime.py` and `pipeline_source_{loop,progress,results}.py`; `jobs_fetcher.py` and `pipeline_stage_source_execution.py` bind those roots. Treat this as compatibility debt, not a pattern to expand.
- `adapters/` - static, provider_api, social fetchers
- `canonicalize.py` - thin coordinator: `canonicalize_job`, `canonicalize_job_with_reason`, `canonicalize_google_sheets_rows`, `CanonicalNormalizer`, audit entry points, and the compat re-export surface. Implementation leaves: `canonicalize_google_sheets.py` (thin coordinator re-exporting the Google Sheets leaves), `canonicalize_google_sheets_slug.py` (slug/title-token helpers), `canonicalize_google_sheets_category.py` (category-label detection), `canonicalize_google_sheets_link.py` (link-employer evidence), `canonicalize_google_sheets_provider.py` (provider title lookup + category-link status resolver), `canonicalize_google_sheets_title.py` (title repair pipeline), `canonicalize_locations.py` (location/sector audit + canonical job build), `canonicalize_redirects.py` (sheet redirect resolution + row pipeline)
- `dedup.py` - thin coordinator: `deduplicate_jobs`, `CanonicalDeduplicator`, `_sort_enrich_and_number`, and the public scoring re-export surface. Implementation leaves: `dedup_identity.py` (constants, identity predicates, scoring, source-class classification), `dedup_preferences.py` (title/company merge preferences), `dedup_state.py` (source bundle + location accumulation), `dedup_record_merge.py` (record merge orchestration + gracklehq enrichment), `dedup_targeting.py` (merge target selection + dedup indexes), `dedup_gate.py` (gate tiers, reason accounting, merge samples)
- `state_lifecycle.py` - thin coordinator: `apply_job_lifecycle_state`, `normalize_job_lifecycle_payload`, `read/write_job_lifecycle_state`, `read/write_job_lifecycle_archive_state`, `lifecycle_counts`, `lifecycle_state_fingerprint`, `build_availability_history_payload`, `build_lifecycle_source_evidence` (38 lines; 45/49 byte-identical + 4 seam diffs via `_sl.normalize_job_lifecycle_payload`). Implementation leaves: `state_lifecycle_identity.py` (120: aliases + `_lifecycle_alias_index`/`_index_lifecycle_entry_aliases`/`_resolve_lifecycle_key`/`availability_id_for_job`), `state_lifecycle_availability.py` (236: `_normalize_availability_*` + `build_lifecycle_source_evidence`/`build_availability_history_payload`), `state_lifecycle_normalization.py` (257: payload normalization + `lifecycle_state_fingerprint`/`read/write` via `_sl.` seam), `state_lifecycle_transitions.py` (291: `_lifecycle_entry_from_active_job`/`_apply_missing_lifecycle_entry`/`apply_direct_availability_evidence`), `state_lifecycle_orchestration.py` (321: `apply_job_lifecycle_state` + `_apply_active`/`_initialize_carried`/`_apply_missing`/`_prune_archived`/`_project_available`)
- `reporting_dedup_evidence.py` - thin coordinator: `build_dedup_evidence`, `build_dedup_audit_gate` (12 lines; 12/12 byte-identical). Implementation leaves: `common/dedup_evidence_review_queue.py` (285: `_job_summary`/`_recommended_review_action`/`_suspected_cause`), `common/dedup_evidence_audit_gate_payload.py` (302: `build_dedup_audit_gate`), `common/dedup_evidence_bundle_report.py` (596: `build_dedup_evidence`); shared pure leaves remain siblings: `dedup_evidence_bundle.py`/`audit_gate.py`/`google_sheets.py`/`merge.py`/`provider_static.py` + `contracts_dedup_evidence.py`
- `common/` - leaf helpers (`config`, `contracts`, `heuristics`, `parsing`, etc.); `common/__init__.py` is compatibility-only

**Discovery package (`src/source_discovery/`):**
- `orchestrator.py` - public run flow and test patch surface over `orchestrator_{runtime,generation,probe,finalize}.py`
- `runtime_metrics.py`, `stage_control.py` - runtime bookkeeping and stage toggles
- `reporting.py` - stable discovery reporting compatibility surface
- `reporting_{progress,candidates,backlog}.py` - progress/taskProgress assembly, candidate-stream staging/merge, and M5 backlog ownership
- `gamesmap.py` - stable Gamesmap compatibility surface
- `gamesmap_{cache,parsing,candidates}.py` - Gamesmap cache, parsing, and candidate generation ownership
- Existing discovery root injection appears in `orchestrator_{generation,probe,finalize}.py` and `gamesmap_candidates.py`; `orchestrator.py` and `gamesmap.py` bind those roots. Prefer explicit imports for new cross-module references.
- `web_search.py` - stable discovery web-search compatibility surface
- `web_search_{fetch,extract,candidates}.py` - web-search fetch, extraction, and candidate inference ownership
- `gamedevmap.py` - stable GameDevMap adapter compatibility surface; routes default discovery through the active-source audit/recovery engine
- `gamedevmap_active_dry_run.py` - GameDevMap resumable audit/recovery engine shared by default discovery, dry-run audit CLI, lost-recovery comparison, and explicit browser recovery
- `gameprog.py`, `sheet_directory.py` - other domain generators

**Sync helpers:**
- `source_sync.py` - stable thin compatibility and test patch surface
- `source_sync_config.py` - packaged config resolution, validation, and raw GitHub request setup
- `source_sync_runtime.py` - runtime state, auth manager, rate limiting, DPAPI, and JSON request flow
- `source_sync_snapshot.py` - snapshot normalization, transition backfill, merge ranking, and remote read/write helpers
- `source_sync_crypto.py` - private-key encryption, PEM/ASN.1 parsing, and JWT signing helpers
- `adapters/static.py` - current static adapter entrypoint over `static_runtime.py`, `static_listing.py` (thin coordinator over `static_listing_{common,state,flow,plugin,rows,traversal,runner}.py`), `static_sources.py`, `static_runtime_support.py`, and `static_detail_heuristics.py` (thin coordinator re-exporting `static_detail_heuristics_{config,filter,parse,entry}.py`; the `extract_rendered_card_jobs` alias stays in the coordinator); use direct leaf imports for helper ownership

**Local data:**
- `local_data_store.py` - stable desktop local-data store facade
- `local_data_store_shared.py` - shared file IO, normalization helpers, `LocalDataPaths`, and locking
- `local_data_store_profiles.py` - profile/session ownership and sign-in helpers
- `local_data_store_saved_jobs.py` - saved-job normalization, merge, activity, and mutation helpers
- `local_data_store_attachments.py` - attachment metadata and blob persistence helpers
- `local_data_store_backup.py` - backup import/export plus admin overview/wipe helpers

**Desktop updater helper:**
- `desktop_updater.py` - stable helper executable and test patch surface
- `desktop_updater_ui.py` - helper window, diagnostics, and native error-dialog helpers
- `desktop_updater_release.py` - release lookup, manifest recovery, ZIP re-verification, and failure classification
- `desktop_updater_install.py` - install handoff, rollback snapshot, relaunch verification, and success finalization
- `desktop_update.py`, `desktop_updater.py`, and `update_manager.py` are import-compatible facades. Production runtime/build consumers use leaf modules directly; guardrail inventories block new runtime facade imports or root-dependency drift.

---

## 6) Data model

| File | Purpose |
|------|---------|
| `data/jobs-unified-light.json` | Supported public Jobs feed projection |
| `data/jobs-unified-startup.json` | Bounded boot cache derived from the same published generation |
| private `jobs-unified.json` | Deprecated pipeline/rollback handoff; never publicly served |
| `data/jobs-fetch-report.json` | Last fetch diagnostics |
| `data/defaults/source-registry-active.seed.json` | Tracked approved-source defaults |
| `data/defaults/source-registry-pending.seed.json` | Tracked pending-source defaults |
| `data/source-registry-active.json` | Ignored local approved-source runtime state |
| `data/source-registry-pending.json` | Ignored local pending-source runtime state |
| `data/source-registry-rejected.json` | Rejected sources, local-only |
| `data/source-registry-tombstones.json` | Local-only delete ledger keyed by source identity |
| `data/source-sync.json` | Remote sync snapshot v2 (`active` and `pending` only) |
| `data/source-discovery-report.json` | Last discovery run |
| `data/admin-active-task-snapshot.json` | Hot active-run summary for Admin/Jobs/Umbrel compact task routes |
| `data/local-user-data/profiles.json` | Desktop-local profile registry |
| `data/local-user-data/session.json` | Desktop-local current session |
| `data/local-user-data/users/{uid}/*` | Desktop-local per-user saved jobs, notes, attachments |

---

## 7) Runtime contracts

- **Desktop single-instance:** If healthy session exists, raise error - do not open another window
- **Desktop startup:** start site + bridge -> wait for page URL readiness -> wait for `/ops/health` before steady state
- **Container startup:** seed `/data` only when missing -> start one same-origin UI/API server -> keep desktop lifecycle/updater routes disabled
- **Active task summaries:** while fetch/discovery/sync is active, `/ops/task-state?view=summary` and `/ops/task-live/<task>?view=summary` should read bounded hot snapshots; idle history, full diagnostics, and detail routes stay on authoritative projection/report paths
- **Session/watchdog:** store metadata in `desktop-session.json`, track browser heartbeat, close on idle timeout

---

## 8) Fast verification matrix

| Change area | Fastest verification |
|-------------|----------------------|
| Frontend syntax | `node --check frontend/jobs/app.js` |
| Frontend unit | `npm run test:frontend:unit` |
| Bridge behavior | `python -m pytest tests/admin/ -q` |
| Container / Umbrel runtime | `python -m pytest tests/bridge/test_container_runtime.py -q` plus targeted container frontend unit tests |
| Pipeline/fetcher | `python -m pytest tests/test_jobs_fetcher_*.py -q` |
| Desktop launcher | `python -m pytest tests/desktop_app/ -q` |
| Packaged desktop smoke | `python -m pytest tests/packaged_desktop/ -q` |
| Linux Python tests | `npm run test:py:linux` |
| Linux frontend tests | `PLAYWRIGHT_SYSTEM_CHROMIUM=1 npm run test:frontend:linux` |
| Full verification | `npm run verify` |

See [`testing.md`](testing.md) for more commands.

---

## 9) Thin boundaries (don't move blindly)

- `src/packaged_desktop_smoke.py` - stable packaged smoke entrypoint and test patch surface; keep implementation in `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`
- `src/ship/desktop_update.py` - stable updater surface; keep implementation in `src/ship/desktop_update_{shared,state,service}.py`
- `src/ship/desktop_updater.py` - stable updater helper executable and test patch surface; keep implementation in `src/ship/desktop_updater_{ui,release,install}.py`; helper root injection has been removed from the leaves
- `src/admin_bridge.py` - stable thin entrypoint; add new bridge logic to `src/bridge/*.py` or `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`
- `src/source_discovery.py` - stable thin CLI entrypoint; add discovery logic to `src/source_discovery/*.py`
- `src/jobs_fetcher.py` - current CLI/task facade; preserve user-facing launch behavior, but lazy export routing and root-backed wrapper seams are internal simplification candidates
- `src/jobs/pipeline.py` - current package entrypoint; keep CLI/task launch behavior covered, but internal runtime, source-execution, and report modules may be rewired or collapsed when the replacement is simpler
- `frontend/jobs/app/desktop-update.js` - stable Jobs desktop-update export surface; keep implementation in `frontend/jobs/app/desktop-update-{model,dom,controller}.js`
- `src/jobs/adapters/static.py` - current static adapter entrypoint; generic listing/detail/runtime modules may be collapsed when the replacement is simpler and covered by adapter tests
- `src/jobs/state.py` - removed source-state facade; persisted source-state meaning now lives in the `src/jobs/state_source_state.py` leaf — preserve that meaning, not the old facade/leaf split
- `src/jobs/state_lifecycle.py` - thin lifecycle-state coordinator (38 lines); keep `apply_job_lifecycle_state`/`normalize_job_lifecycle_payload`/`read/write` persistence semantics here and bridge task lifecycle in `src/bridge/task_lifecycle*.py`; implementation lives in `src/jobs/state_lifecycle_{identity:120,availability:236,normalization:257,transitions:291,orchestration:321}.py` (`_sl.normalize_job_lifecycle_payload` call-time seam; 45/49 byte-identical + 4 seam diffs)
- `src/jobs/reporting_dedup_evidence.py` - thin read-only dedup evidence coordinator (12 lines); keep `build_dedup_evidence`/`build_dedup_audit_gate` presentation here and dedup policy in `src/jobs/dedup*.py`; implementation lives in `src/jobs/common/dedup_evidence_{review_queue:285,audit_gate_payload:302,bundle_report:596}.py` (12/12 byte-identical)
- `src/bridge/routes/post_routes.py` - stable POST registration surface; keep route-family logic in `src/bridge/routes/post_routes_{admin,local_data,update}.py`
- `src/bridge/routes/get_routes.py` - stable GET delegator surface; keep GET route-family logic in `src/bridge/routes/get_*.py`
- `frontend/jobs/domain.js` - stable Jobs domain export surface; keep query/feed/view ownership in `frontend/jobs/domain/{query,feed,view}.js`
- `src/source_sync.py` - permanent thin sync integration surface; keep new sync logic in `src/source_sync_{config,runtime,snapshot,crypto}.py`
- `src/local_data_store.py` - stable local-data store surface; keep implementation in `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py`
- `frontend/shared/local-data/desktop-client.js` - stable desktop local-data runtime root; keep implementation in `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`
- `src/jobs/common/__init__.py` - package marker only; prefer `src.jobs.common.<leaf>` or package-submodule imports
- `frontend/local-data/services.js` - transitional local-data boundary; page code should go through slice-local `services.js`

**Leaf modules that are still safe extraction targets**
- `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py`, `src/bridge/ops_live_payload.py`
- `frontend/admin/render/{ops-summary,ops-history,ops-shared}.js`
- `src/jobs/fetcher_compat_{exports,runtime}.py`
- `src/jobs/pipeline_{run_setup,finalize}.py`
- `src/jobs/pipeline_runtime_{writers,summary}.py`
- `src/jobs/pipeline_source_{loop,results,progress}.py`
- `src/jobs/state_{source_state,lifecycle,incremental}.py`
- `src/jobs/state_source_{records,browser,migration}.py`
- `src/bridge/routes/post_routes_{admin,local_data,update}.py`
- `src/ship/desktop_app/launcher_{flow,diagnostics,recovery}.py`
- `src/ship/desktop_app/startup_{ready,watchdog}.py`
- `frontend/jobs/domain/{query,feed,view}.js`
- `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py`
- `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`
- `frontend/saved/app/runtime/*.js`

---

## 10) Key libraries

- **Playwright** - frontend smoke tests
- **PyInstaller** - portable Windows executable
- **Scrapy + Playwright** - scraping fallback

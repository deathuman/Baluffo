# Cross-Cutting Tech Debt Reduction Plan

> - **Status:** Active plan, advisory-only
> - **Use this when:** evaluating or performing cross-cutting tech debt reduction before expanding to new platforms (container, Umbrel, native desktop, headless)
> - **Canonical for:** the June 2026 cross-cutting tech debt inventory: BridgeApi god object, admin_bridge legacy globals, get_routes.py decomposition, bare except Exception, _as_dict/_as_list proliferation, test-time sleep/port coupling, data model and contract drift, desktop/ship update-system complexity, deferred macOS platform gap, shared-layer isolation violations, and CSS/build infrastructure gaps
> - **Not canonical for:** jobs/fetcher-specific refactoring (see [`initial_findings.md`](initial_findings.md)), source-discovery decomposition, adapter plugin internals, or individual component tests
> - **Then inspect:** [`../architecture-ai-map.md`](../architecture-ai-map.md), [`refactor-charter-template.md`](refactor-charter-template.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), bridge service leaf modules, and component-specific test coverage
> - **Last updated:** 2026-06-19 — validated against current source; multiple P0 implementation slices completed; admin bootstrap, admin ops-tab counts, app, registry, registry-conflicts, sync status, pipeline task, discovery, fetch-report, source-policy recommendations, and desktop local-data GET routes extracted from GET routes; updater facade consumer/root-dependency inventories added, production updater facade imports removed, release-builder facade import removed, desktop-app/helper/admin updater imports moved to leaves, packaged update rehearsal manifest constants/helpers moved to pure leaves, and updater leaf root lookups removed through pure desktop update constants/crypto/version/app-version/HTTPS/timestamp/context/network-stdlib/temp-name/state/shared-self/private-state/service-private/release-note/state-local/shared-private/service-state/service-shared/service-release-lookup/service-manifest-validation/service-download-hash/state-request/state-handoff-io/service-session-write/service-status-persistence/service-stdlib/state-launch/shared-stdlib-json/shared-network-path/shared-final-root slices; macOS platform work deferred by product priority; stale footprint counts and unsafe acceptance criteria corrected; _as_dict/_as_list remains P3 (4 callers, contained refactor)

## Summary

A systematic analysis identified twelve cross-cutting tech debt clusters that impede platform expansion. Unlike jobs-pipeline-specific issues (already covered in `initial_findings.md`), these span the bridge API, data contracts, HTTP routing, error handling, desktop/ship platform prep, utility patterns, and test infrastructure — touching every subsystem.

**Quick stats:**

| Area | Severity | Current status | Footprint / remaining work |
|------|----------|----------------|----------------------------|
| BridgeApi god object | P0 | Partial | Current-task default payload builders merged; source-derived field classification guardrail added and hardened for dynamic API lookups. Field deletion/split still open. |
| admin_bridge legacy globals | P0 | Partial | 5-way root injection seam now has explicit coverage. Singleton/service-holder migration still open. |
| get_routes.py monolith | P0 | Done for route-owned behavior | Partial JSON parser, provider-coverage link backfill, registry source table compaction, fetch-report source-run read support, ops diagnostics routes, ops status routes, admin bootstrap route, admin ops-tab counts route, app routes, registry routes, registry-conflicts route, sync status route, pipeline task routes, discovery routes, fetch-report routes, source-policy recommendations route, and desktop local-data GET routes extracted with tests. `handle_get` remains the public delegating entrypoint. |
| Data model drift (CanonicalJob) | P0 | Done for missing-field slice | `CanonicalJobSchema` now preserves `lifecycleEvent`, `lifecycleReason`, `locations`, and `locationSummary`; `DATA_CONTRACT.md` documents locations fields. `id` consistency remains deferred by strategy. |
| Fetch report normalization duplicated | P0 | Partial | Shared-compatible task-progress, socialSummary, timingSummary, and source-row base helpers extracted while preserving bridge/jobs shape differences. Deeper source-row enrichment differences remain open. |
| macOS platform gap | Deferred | Deferred | No `_darwin.py`; current desktop package maps non-Windows to `_linux.py`. Real gap, but not a near-term blocker. |
| Update subsystem over-engineering | P0 | Partial | 17 files across two parallel subsystems, root-injection/re-export facades, and runtime `update_manager` coupling inside `src/ship/`. Import-compatible facades preserved for tests/external compatibility; facade consumer inventory now guards import expansion, root-dependency inventory now guards updater leaf root-binding drift, production updater imports use leaves directly, packaged update rehearsal manifest constants/signing helpers no longer read through the facade, release-notes history and desktop updater tests now use desktop update leaf modules directly, and updater shared/state/service leaves no longer require root-bound `deps.<name>` lookups. Runtime facade reduction remains open. |
| Bare `except Exception` | P0 | Partial | Adapter-audit runner fallback, adapter recovery fallback, two URL parsing fallbacks, and run-history ISO parse/pipeline-status fallbacks narrowed, update POST routes moved onto the shared route boundary, jobs transport request/cleanup/strategy-selection/client-construction/fetch-retry catches narrowed, jobs browser fallback wrapper narrowed, static source registry facade fallback narrowed, bridge server-handler bookkeeping suppressions narrowed, metric-only GET route catches removed, retained bridge-log event writes narrowed to `OSError`, low-risk bridge helper fallbacks narrowed, admin bootstrap best-effort fallback narrowed, source-discovery browser fallback setup narrowed, source-discovery ordered text and directory-index fallbacks narrowed, source-discovery fetch retry, probe fetch, web-search scan fetch, and sheet-directory URL validation fallbacks narrowed, source-discovery auto-approval finalization fallback narrowed, NCSoft, generic, listing, rendered-card, Milestone, and Kojima dynamic-listing static plugin fetch fallbacks and Jobylon parse fallback narrowed, provider API source/error boundaries including Personio narrowed, container gateway process termination narrowed to `OSError`, ship runtime launcher client-disconnect handling narrowed to `OSError`, desktop-app process termination and stale-lock reclaim callback cleanup narrowed to expected process/callback failures, ship update-manager CLI failures narrowed to expected operational errors, static plugin detail/dynamic-fetch fallbacks narrowed, Reddit HTML and gameprog directory fetch fallbacks made explicit, optional-certifi/console-encoding/profile-summary/registry-journal-compaction fallbacks narrowed, source-sync packaged-key decrypt fallback narrowed, source-sync remote timing/idempotency catches narrowed, dev-supervisor POSIX kill and reclaim termination suppressions narrowed, shared HTTP batch import/fetch/progress fallbacks narrowed, desktop update facade optional psutil import, and desktop update manifest optional cryptography import now catch only `ImportError`, source-policy migration source-id, fetch-report detail parser, parser-regression redirect, canonical job-link redirect, source-check fetch, source-check embedded fetch, source-check Playwright runtime fallback, fetcher launch spawn fallback, packaged runtime snapshot fallback, packaged runtime optional status fallback, packaged failure-metrics fetch fallback, packaged update rehearsal, Windows desktop probe, desktop launcher retry, desktop launcher diagnostic, embedded runtime probe, packaged sync rehearsal, desktop update rehearsal, packaged browser rehearsal, job discovery increment measurement, repo-health analyzer, BridgeApi default-field metadata/activity timestamp, ops dashboard fallback boundaries, ops lifecycle stale-repair fallback, source-policy recommendation export, Scrapy runner envelope/import/stdin/static-source/queue fallbacks, structured provider detail/workday fallbacks, social adapter subsource boundaries, static-listing URL/probe/artifact/dynamic/listing-fetch/page-processing fallbacks, pipeline child-boundary wrappers, server response-write socket boundary fallbacks, HTML extractor URL-join fallback, bridge HTTPD startup callback fallback, and HTML external-script fetch fallback narrowed, and source-sync shard pull, SQLite write rollback, update-manager apply rollback, plus desktop updater install rollback cleanup moved to explicit `BaseException` cleanup; BLE001 budget lowered from 129 to 20. Remaining work is annotated HTTP/process/tool boundaries, not unannotated broad catches. |
| `json_io.py` shared-layer violation | P1 | Open | Imports from `src.storage_metrics`, violating "stdlib-only" shared-layer contract. |
| Test time/port coupling | P1 | Open | 23 `time.sleep()`, 39 hardcoded port 8877 references, 81 monkeypatches on admin_bridge internals. |
| `parse_iso` proliferation | P2 | Open | 9 locations, 4 semantic groups; 3 bridge versions skip tzinfo normalization (latent comparison bug). |
| CSS/build pipeline gaps | P2 | Open | 2,632-line `components.css` no build processing; 18 duplicated gradient patterns; inconsistent manual cache-busting. |
| `_as_dict`/`_as_list` proliferation | P3 (demoted) | Open | 42+28 definitions but only 4 callers of the `utils.py` private versions; contained refactor. |

Expected implementation: ~9-13 engineering days across all phases.

### Completed P0 Slice Ledger

Completed on 2026-06-17:

- **BridgeApi default payload dedup:** `_default_current_task_state_payload()` now supports the summary variant without duplicate builders.
- **BridgeApi field classification guardrail:** `tools/repo_health/bridge_api_field_inventory.py` now classifies all 90 `BridgeApi` dataclass fields as runtime/path, service handle, bootstrap-injected, service-wired, route/post-route/helper-used, test-overridden, or default-only evidence; repo guardrails fail on field-count drift or unsafe default-only production references, including dynamic `getattr(api, "...")` lookups.
- **admin_bridge root seam coverage:** tests assert all five injected entrypoint modules point back to `admin_bridge`.
- **get_routes partial JSON extraction:** top-level partial JSON span/decode/prefix helpers moved to `src/shared/partial_json.py` with direct unit tests; `handle_get` remains the public route entrypoint.
- **get_routes provider backfill extraction:** provider-coverage link-backfill loading/enrichment moved to `src/bridge/source_policy_link_backfill.py`; `/source-policy/recommendations` keeps the same response shape.
- **get_routes registry table extraction:** `/registry/sources?view=table` row compaction moved to `src/bridge/registry_source_table.py`; route dispatch, bucket filtering, and pending auto-approval enrichment remain route-owned.
- **get_routes fetch-report source-run extraction:** SQLite source-run read/hydration and rollback diagnostics moved to `src/bridge/routes/get_fetch_report_sources.py`; shared route storage-read metrics moved to `src/bridge/routes/route_storage_metrics.py`; `/ops/fetch-report` and `/ops/fetch-report/sources` keep the same response shapes.
- **get_routes ops diagnostics extraction:** `/ops/fetcher-metrics`, `/ops/perf-counters`, `/ops/performance-profile`, `/ops/storage-metrics`, `/ops/storage-health`, `/ops/discovery-audit-artifacts`, `/ops/task-failure-attempts`, and `/ops/fetch-report/sources` dispatch moved to `src/bridge/routes/get_ops_diagnostics.py`; `handle_get` remains the public entrypoint.
- **get_routes ops status extraction:** `/ops/health`, `/ops/dashboard-health`, `/ops/fetch-kpis`, `/ops/history`, `/ops/task-state`, and `/ops/task-live/*` dispatch moved to `src/bridge/routes/get_ops_status.py`; route payloads, timing labels, and status codes stay unchanged.
- **get_routes registry extraction:** `/registry/active`, `/registry/pending`, `/registry/rejected`, `/registry/sources`, and `/registry/summary` dispatch moved to `src/bridge/routes/get_registry.py`.
- **get_routes registry-conflicts extraction:** `/registry/conflicts` full and summary GET dispatch moved to `src/bridge/routes/get_registry_conflicts.py`; summary-cache writes, adjudication overlay, auto-heal payload attachment, and Admin Ops registry-conflicts badge behavior stay unchanged.
- **get_routes discovery extraction:** `/discovery/report`, `/discovery/candidates`, `/discovery/log`, and `/discovery/config` dispatch moved to `src/bridge/routes/get_discovery.py`; shared log chunking and route payload helpers moved to `src/bridge/routes/route_payload_helpers.py`; fetcher log and fetch-report routes remain in `get_routes.py`.
- **get_routes desktop local-data GET extraction:** `/desktop-local-data/session`, `/desktop-local-data/profiles`, `/desktop-local-data/saved-jobs`, `/desktop-local-data/saved-job-keys`, `/desktop-local-data/attachments`, `/desktop-local-data/attachments/content`, `/desktop-local-data/backup/export-file`, `/desktop-local-data/activity`, and `/desktop-local-data/startup-metrics` dispatch moved to `src/bridge/routes/get_local_data.py`; POST local-data routes remain in `post_routes_local_data.py`.
- **get_routes fetch-report extraction:** `/fetcher/log`, `/ops/fetch-report`, and `/ops/fetch-report/sources` dispatch moved to `src/bridge/routes/get_fetch_report.py`; source-row read/hydration remains in `get_fetch_report_sources.py`.
- **get_routes source-policy recommendations extraction:** `/source-policy/recommendations` dispatch moved to `src/bridge/routes/get_source_policy.py`; provider backfill remains in `src/bridge/source_policy_link_backfill.py`; response shape, warnings, review-state merge, and suppression eligibility behavior stay unchanged.
- **get_routes sync status extraction:** `/sync/status` full and summary GET dispatch moved to `src/bridge/routes/get_sync.py`; summary payload shape, unsupported-view errors, best-effort runtime-state fallback, and timing labels stay unchanged.
- **get_routes pipeline task extraction:** `/tasks/jobs-pipeline-schedule` and `/tasks/run-jobs-pipeline-status` GET dispatch moved to `src/bridge/routes/get_pipeline_tasks.py`; payload sources and POST schedule updates stay unchanged.
- **get_routes app route extraction:** `/app/ready` and `/app/update-status` GET dispatch moved to `src/bridge/routes/get_app.py`; readiness payload source, update-status error boundary, and container unavailable behavior stay unchanged.
- **get_routes admin bootstrap extraction:** `/admin/bootstrap` GET dispatch moved to `src/bridge/routes/get_admin_bootstrap.py`; packaged-smoke fail-once guard, timing label, and bounded bootstrap payload source stay unchanged.
- **get_routes admin ops-tab counts extraction:** `/admin/ops-tab-counts` GET dispatch moved to `src/bridge/routes/get_admin_ops_tab_counts.py`; badge keys, bounded summary payload, unsupported-view error, and timing label stay unchanged.
- **CanonicalJob missing-field preservation:** `CanonicalJobSchema` now includes `lifecycleEvent`, `lifecycleReason`, `locations`, and `locationSummary`; schema dump preservation is tested.
- **Fetch-report task progress compatibility:** bridge and jobs use shared task-progress helpers while keeping their existing public count shapes and compatibility differences.
- **Fetch-report social/timing compatibility:** bridge and jobs now share socialSummary and timingSummary normalization helpers while preserving bridge lowercase-adapter/runtime-field compatibility differences and jobs runtime extras.
- **Fetch-report source-row base compatibility:** bridge and jobs now share base source-row normalization for common count/status/health fields while preserving bridge defaults/clamps and jobs enrichment/default behavior.
- **Exception suppression ratchet:** adapter-audit runner fallback, adapter recovery fallback, two URL parsing catches, run-history ISO parse and pipeline-status fallbacks, jobs transport request/cleanup/strategy-selection/client-construction/fetch-retry catches, jobs browser fallback wrapper, static source registry facade fallback, and bridge server-handler bookkeeping suppressions were narrowed from `except Exception`/`suppress(Exception)`; update POST routes now use `src/bridge/routes/error_boundary.py`; registry and fetch-report GET route metric wrappers now default to failed and mark success only after payload/send completion; ops lifecycle-cache metric wrappers now default to failed and mark success only after row loading completes; ship update-manager CLI errors now catch only expected operational failures; source-sync packaged-key decrypt fallback and source-sync remote timing/idempotency catches now handle only expected remote IO/runtime/validation/decryption failures; source-sync shard progress callbacks now suppress only expected sink failures; source-sync shard pull, SQLite write rollback, update-manager apply rollback, and desktop updater install rollback cleanup now use `BaseException` only to cancel/rollback before re-raising; admin bootstrap best-effort fallback, retained bridge-log event writes, source-discovery browser fallback setup, source-discovery ordered text and directory-index fallbacks, source-discovery fetch retry, probe fetch, web-search scan fetch, sheet-directory URL validation fallback, source-discovery auto-approval finalization fallback, NCSoft, generic, listing, rendered-card, Milestone, and Kojima dynamic-listing static plugin fetch fallbacks, Jobylon parse fallback, provider API source/error boundaries including Personio, container gateway process termination, ship runtime launcher client-disconnect handling, desktop-app process termination and stale-lock reclaim callback cleanup, dev-supervisor POSIX kill timeout cleanup and reclaim termination cleanup, registry enrichment, registry journal compaction, static gzip, source-check Playwright import, redirect probes, fetch fallback, embedded fetch fallback, source-check Playwright runtime fallback, fetcher launch spawn fallback, packaged runtime snapshot fallback, packaged runtime optional status fallback, packaged failure-metrics fetch fallback, packaged update rehearsal JSON reads/health poll/taskkill cleanup, Windows elevation/window probe fallbacks, desktop launcher retry fallback, desktop launcher diagnostic rethrow fallback, embedded runtime probe fallback, packaged sync rehearsal fallback, desktop update rehearsal fallback, packaged browser rehearsal fallbacks, job discovery increment measurement fallbacks, repo-health analyzer fallbacks, BridgeApi default-field metadata and activity timestamp fallbacks, ops dashboard fallback boundaries, ops lifecycle stale-repair fallback, bridge HTTPD startup callback fallback, source-policy recommendation export, Scrapy runner envelope/import/stdin/static-source/queue fallbacks, structured provider detail/workday fallbacks, social adapter subsource boundaries, static-listing URL/probe/artifact/dynamic/listing-fetch/page-processing fallbacks, pipeline child-boundary wrappers, server response-write socket boundary fallbacks, HTML extractor URL-join and external-script fetch fallbacks, source-discovery URL resolve probes, gameprog directory fetch fallback, jobs quarantine-date parsing, static plugin detail/dynamic-fetch fallbacks, Reddit HTML parser fallback, optional-certifi, pipeline console-encoding, profile summary rendering, shared HTTP batch optional import/fetch/progress fallbacks, desktop update facade optional psutil import, desktop update manifest optional cryptography import, source-policy migration source-id fallback, fetch-report detail parser fallback, parser-regression redirect fallback, canonical job-link redirect fallback, source-sync packaged-key fallback/cache writes, local-key cache reads, and optional keyring fallback, admin active-task/update-handoff owner-session fallbacks, ops dashboard/fetch KPI summary and live-task evidence fallbacks, sync status summary runtime-state fallback, updater psutil process lookup, updater current-version repair, updater release-check, updater handoff session-root, updater handoff diagnostics, updater download-progress status, updater download-worker, updater download-start, updater helper UI import/theme/progress-stop/mainloop, updater install preflight, updater helper-staging, updater interrupted-install startup recovery, updater data-backup restore, updater rollback snapshot/relaunch, and updater install-handoff write fallbacks now catch only expected input/runtime failures; `tools/repo_health/source_suppression_budget.json` now budgets `BLE001` at 20.
- **Updater facade consumer inventory:** `tools/repo_health/desktop_update_facade_inventory.py` classifies all current `desktop_update.py`/`desktop_updater.py` facade imports and guardrails fail on unclassified consumers, count drift, or new leaf imports outside the compatibility allowlist. Direct import migration remains open.
- **Updater release-builder facade migration:** manifest-only desktop update helpers moved to `src/ship/desktop_update_manifest.py`; `scripts/build_desktop_update_release.py` imports manifest/shared leaf helpers directly, reducing facade inventory from 11 to 10 records while keeping `src.ship.desktop_update` exports compatible.
- **Updater desktop-app facade migration:** `src/ship/desktop_app/__init__.py` now imports desktop update paths and install-state helpers from `desktop_update_shared.py`/`desktop_update_state.py` directly, reducing facade inventory from 10 to 9 records while preserving desktop app compatibility exports.
- **Updater helper facade migration:** `src/ship/desktop_updater.py` no longer imports `src.ship.desktop_update` to bind shared/state roots; updater shared/state leaves own their dependencies directly, reducing facade inventory from 9 to 7 records while keeping `desktop_updater.py` import-compatible.
- **Updater admin facade migration:** `src/admin_bridge.py` now imports `DesktopUpdateService` from `desktop_update_service.py` directly, and `DesktopUpdateService` no longer requires a root facade binding, reducing facade inventory from 7 to 6 records while preserving `desktop_update.py` service re-export compatibility.
- **Updater packaged-smoke facade migration:** `src/packaged_desktop_smoke.py` now builds its stable `desktop_update_mod` compatibility namespace from updater leaf helpers instead of importing `src.ship.desktop_update`, reducing facade inventory from 6 to 5 records; remaining facade imports are test-compat only.
- **Updater release-notes history test facade migration:** `tests/test_desktop_update_release_notes_history.py` now imports desktop update shared/state/service leaves directly, reducing facade inventory from 5 to 4 records while keeping remaining facade tests as explicit compatibility coverage.
- **Updater helper test desktop-update facade migration:** `tests/test_desktop_updater.py` now imports desktop update constants/shared/state leaves directly for update state setup and assertions, reducing facade inventory from 4 to 3 records while retaining its `desktop_updater.py` facade coverage.
- **Updater root-dependency inventory:** `tools/repo_health/desktop_update_root_dependency_inventory.py` classifies all `deps.<name>` root dependencies across desktop update shared/state/service leaves and guardrails fail on dependency/reference count drift or unclassified root-binding usage. Runtime facade migration remains open until dependency roots are split deliberately.
- **Updater packaged rehearsal manifest helper migration:** packaged update rehearsal now uses pure manifest/signing/hash/version/channel/public-key helpers exposed by `src.packaged_desktop_smoke` or its updater helper leaf instead of reading those symbols through `desktop_update_mod`; the facade remains only for root-bound path/session/update-state behavior.
- **Updater pure helper root-binding reduction:** `desktop_update_shared.py` and `desktop_update_state.py` now read `DESKTOP_UPDATE_CHANNEL`, `DESKTOP_UPDATE_SCHEMA_VERSION`, `DESKTOP_UPDATER_VERSION`, and `PUBLIC_KEYS_FILE` from `desktop_update_manifest.py`; remaining pure desktop update constants moved to `desktop_update_constants.py`; manifest crypto key classes, version comparison, app-version lookup, GitHub HTTPS helpers, timestamp helpers, context suppression, network stdlib helpers, temp-name helpers, state-only label/version/hash helpers, private same-module shared helpers, private same-module handoff state helpers, service-private state helpers, private release-note/artifact helpers, state-local validation/default/temp helpers, private shared runtime/path helpers, service-side state helper calls, service-side timestamp/label/public-key helper calls, service-side release lookup helper calls, service-side manifest validation/signature calls, service-side download/hash calls, state-local install-request helper calls, state handoff/status IO helper calls, service-side session/path/write helper calls, service-side status persistence calls, service-side stdlib calls, state launch/handoff helper calls, shared stdlib/JSON helper calls, shared network/path helper calls, and final shared runtime/psutil hooks now come from their owning leaves. Tracked updater root dependencies dropped from 111 to 0 and references from 249 to 0 while keeping `desktop_update.py` import-compatible.

Verification evidence for this slice:

- `npm run test:py:extended` passed: `3616 passed, 1 skipped`
- `npm run lint:repo-guardrails` passed

---

## 1. BridgeApi God Object

### Problem

`src/bridge/api.py:294-527` defines a `BridgeApi` dataclass with **90 annotated callable fields**, all with default stubs. It acts as the central composition layer for the entire bridge — every GET and POST route handler receives the full bag.

**Evidence:**

- 40 module-level stub functions (lines 65-291): `_noop`, `_empty_string`, `_not_started_noarg`, `_not_started_result`, `_abort_not_available`, `_empty_registry_state`, `_invalid_manual_source`, `_default_move_entries`, etc.
- `__post_init__` (lines 471-513) does conditional wiring: checks each field against its dataclass default before overriding from `RegistryService`, `SyncService`, `PipelineService`, `DiscoveryService`
- `_wire_registry_defaults()` (lines 440-469) copies 13 methods from `self.registry` using `getattr` fallbacks
- Two near-identical payload builders: `_default_current_task_state_payload()` and `_default_current_task_state_summary_payload()` differ only by one `"summary": True` key
- Stubs like `_noop_mark_desktop_session_activity`, `_not_implemented_lifecycle`, `_noop_desktop_local_data_store`, `_empty_startup_metrics` — unclear which are actually overridden in production vs. dead defaults

**Why it blocks platform expansion:**

A new platform (headless CLI, alternative container runtime, native desktop variant) must replicate this 90-field contract. There is no documented subset of "required" vs. "optional" fields. The conditional wiring in `__post_init__` is invisible to static analysis.

### Target Boundary

- **Primary subsystem:** `src.bridge.*`
- **Entry file(s):** `src/bridge/api.py`, `src/bridge/ops_api.py` (1,328 lines, 37-field OpsDeps)
- **Ownership boundary being clarified:** BridgeApi should expose a narrow, documented interface contract. Route handlers should receive only the capabilities they need.
- **What becomes easier:** Adding new route handlers, writing platform-specific bridge adapters, testing bridge components in isolation

### In Scope

- Audit all 90 fields: classify as "used in routes," "used in tests," or "dead"
- Remove unreachable/never-overridden stubs
- Merge `_default_current_task_state_payload` / `_default_current_task_state_summary_payload`
- Extract a narrow `RouteContext` or per-route capability set that routes actually need
- Consider splitting BridgeApi into 3-4 role-specific interfaces (RegistryView, SyncView, PipelineView, OpsView)

### Out of Scope

- OpsApi full decomposition (separate charter needed, 1,328 lines)
- OpsDeps 37-field bag simplification (keep for now)
- Full migration off `src.bridge.api` — it works, just needs trimming

### Implementation Shape

1. **Default/stub classification:** Map every BridgeApi field to route-used, service-wired, test-only, or true default. Do not delete a stub from grep evidence alone; many stubs are dataclass default behavior for unsupported routes/platforms. (Guardrail complete; deletion decisions still require field-by-field review.)
2. **Dedup stub payloads:** Merge `_default_current_task_state_payload` and `_default_current_task_state_summary_payload` into one with an optional `summary` parameter.
3. **Name cleanup:** Rename confusing stubs (`_noop_desktop_local_data_store` → `_noop`, `_empty_startup_metrics` → `_empty_json_list`, `_not_started_noarg` → `_not_started_result`).
4. **Route-scoped context:** Add a `RouteContext` frozen dataclass that exposes only the ~20 fields routes actually read. BridgeApi creates it once per request.
5. **Document required vs optional:** Add a comment block or `Protocol` per field category.

### Verification

- `npm run test:py` passes
- No import or name changes in route handler files
- `npm run lint:repo-guardrails` passes (no new cross-subsystem leaks)
- Removed stubs, if any, have field-by-field production override evidence and focused route/test coverage

---

## 2. admin_bridge Legacy Global State + Root Injection Seam

### Problem

`src/admin_bridge.py` (633 lines) is a legacy monolith with:
- **15 mutable singleton variables** with per-service `RLock` (lines 232-245): `_SYNC_SERVICE`, `_REGISTRY_SERVICE`, `_DISCOVERY_SERVICE`, `_PIPELINE_SERVICE`, `_DESKTOP_UPDATE_SERVICE` + companion data-dir variables
- **5-way root injection seam** (lines 276-280): sets `sys.modules[__name__]` onto `admin_entrypoint_api_mod.root`, `admin_entrypoint_runtime_mod.root`, `admin_entrypoint_services_mod.root`, `admin_registry_api_mod.root`, `admin_task_runtime_mod.root`
- **27 path constants** monkeypatched in 54+ test calls in `tests/admin/` + 27 more in `tests/bridge/`
- **Explicit `global` mutation** in `refresh_sync_config()` (line 324): `global SYNC_CONFIG`
- **Zero test coverage** of the root injection seam — no test asserts `.root` is set or points to the correct module

**Why it blocks platform expansion:**

Multi-instance scenarios (multiple bridges, parallel operations) are impossible with module-level singletons. Any new platform that needs a differently-configured bridge must mutate global state, risking test pollution and production races.

### Target Boundary

- **Primary subsystem:** `src/admin_bridge.py` + its 5 injected sub-modules
- **Entry file(s):** `src/admin_bridge.py`, `src/bridge/admin_entrypoint_api.py`, `src/bridge/admin_entrypoint_runtime.py`, `src/bridge/admin_entrypoint_services.py`, `src/bridge/admin_registry_api.py`, `src/bridge/admin_task_runtime.py`
- **Ownership boundary being clarified:** admin_bridge.py becomes a thin CLI entrypoint. All service state moves into explicit service objects owned by `BridgeApi` or `RuntimeConfig`.
- **What becomes easier:** Multi-instance testing, parallel test execution, clean platform-specific entrypoints

### In Scope

- Move `_SYNC_SERVICE`, `_REGISTRY_SERVICE`, `_DISCOVERY_SERVICE`, `_PIPELINE_SERVICE`, `_DESKTOP_UPDATE_SERVICE` singletons into explicit holder objects passed through `BridgeApi` or `RuntimeConfig`
- Replace `global SYNC_CONFIG` mutation with a method on the sync service holder
- Replace root injection seam with explicit import or parameter passing
- Add test coverage for the seam (assert each `*.root` is set before first use)

### Out of Scope

- Inline contents of the 5 injected sub-modules (they have their own responsibilities)
- The jobs-pipeline root injection pattern (covered in `initial_findings.md` §2E)

### Implementation Shape

1. **Service holder dataclass:** Create `BridgeServices` frozen dataclass with optional `SyncService`, `RegistryService`, `DiscoveryService`, `PipelineService`, `DesktopUpdateService` fields. Pass through `BridgeApi` or as a separate parameter.
2. **Deprecate module-level getters:** `_get_sync_service()`, `_get_registry_service()`, etc. become pass-throughs to the holder. Add `@deprecated` warning.
3. **Replace `global SYNC_CONFIG`:** Move cached config into `SyncService` itself.
4. **Replace root injection:** The 5 injected sub-modules receive their dependencies as constructor parameters or explicit `configure()` calls instead of `module.root = ...`.
5. **Add seam test:** Verify each `.root` attribute is set during `build_bridge_api()` or `main()`.

### Verification

- `npm run test:py` passes (including all admin and bridge tests)
- `npm run test:py:extended` passes
- All 54+ admin test monkeypatch calls still work (or are reduced)
- Verifiable: no `global` keyword outside explicit config/singleton holder

---

## 3. get_routes.py Monolith

### Problem

`src/bridge/routes/get_routes.py` is now **75 lines** with **1 public entry point** (`handle_get`). It originally contained 5 distinct subsystems; route-owned behavior has now been extracted:

| Subsystem | Lines | Description |
|-----------|-------|-------------|
| Hand-rolled JSON parser | ~120 | `_skip_json_string`, `_skip_json_value`, `_top_level_json_field_spans`, `_read_json_prefix` — custom partial JSON reading to avoid loading large files |
| File caching | Extracted | caches moved with their route families |
| Provider coverage backfill | Extracted | Moved to `src/bridge/source_policy_link_backfill.py`; route now imports load/enrich helpers |
| Registry table compacting | Extracted | Moved to `src/bridge/registry_source_table.py`; route now imports the compact-row helper |
| Ops health dispatch | Extracted | Moved to `src/bridge/routes/get_ops_status.py`; route now delegates to the handler |
| Admin bootstrap dispatch | Extracted | Moved to `src/bridge/routes/get_admin_bootstrap.py`; smoke fail-once state moved with the route |
| Admin ops-tab counts dispatch | Extracted | Moved to `src/bridge/routes/get_admin_ops_tab_counts.py`; bounded badge summary helpers moved with the route |

Additionally: `handle_get` is now a delegating public entrypoint; no route-owned payload helpers remain in `get_routes.py`.

**Why it blocks platform expansion:**

Every new GET endpoint for a new platform requires navigating this monolith. The caching, JSON parsing, and provider coverage logic are entangled with route dispatch.

### Target Boundary

- **Primary subsystem:** `src.bridge.routes`
- **Entry file(s):** `src/bridge/routes/get_routes.py`
- **Ownership boundary being clarified:** Each GET route family lives in its own module under `src/bridge/routes/get/` or similar.
- **What becomes easier:** Adding GET routes, testing routes in isolation, replacing the hand-rolled JSON parser

### In Scope

- Extract hand-rolled JSON parser into `src/shared/partial_json.py` with tests
- Extract provider coverage link backfill into `src/bridge/source_policy_link_backfill.py` (done)
- Extract registry source table compacting into `src/bridge/registry_source_table.py` (done)
- Extract fetch-report source-run read/hydration support into `src/bridge/routes/get_fetch_report_sources.py` (done)
- Extract ops diagnostics route family into `src/bridge/routes/get_ops_diagnostics.py` (done)
- Extract ops status route family into `src/bridge/routes/get_ops_status.py` (done)
- Extract registry route family into `src/bridge/routes/get_registry.py` (done)
- Extract discovery route family into `src/bridge/routes/get_discovery.py` (done)
- Extract desktop local-data GET route family into `src/bridge/routes/get_local_data.py` (done)
- Extract fetch-report route family into `src/bridge/routes/get_fetch_report.py` (done)
- Extract admin bootstrap route into `src/bridge/routes/get_admin_bootstrap.py` (done)
- Extract admin ops-tab counts route into `src/bridge/routes/get_admin_ops_tab_counts.py` (done)
- Split `handle_get` dispatch into per-domain files (`get_ops.py`, `get_registry.py`, `get_discovery.py`, `get_admin.py`)
- Replace `_as_dict`, `_as_list`, `_clean_text`, `_safe_int` with imports from shared utils
- Remove module-level mutable caches (replace with LRU or remove)

### Out of Scope

- Rewriting the partial JSON parser with `ijson` (desirable but separate decision)
- Full route handler refactoring (behavior-preserving split only)
- Touching `post_routes*.py` files

### Implementation Shape

1. **Extract partial JSON parser** → `src/shared/partial_json.py` (done)
2. **Extract provider coverage link backfill** (functions `_load_provider_coverage_link_backfill` through `_enrich_link_backfill_review_candidates`) → `src/bridge/source_policy_link_backfill.py` (done)
3. **Extract registry table compacting** (`compact_registry_source_table_row`) → `src/bridge/registry_source_table.py` (done)
4. **Split dispatch:** Move route families into dedicated `src/bridge/routes/get_*.py` leaves while keeping `handle_get` as the public delegating entrypoint (done for route-owned behavior).
5. **Replace private helpers** with route helper imports where still needed (done for `get_routes.py`; broader helper cleanup remains P3).
6. **Move caches/helpers with their owning route leaves** so `get_routes.py` no longer owns payload builders (done).

### Verification

- `npm run test:py` passes (especially `tests/bridge/` which covers GET routes)
- Bridge route inventory (`npm run lint:repo-guardrails` or manual `python tools/repo_health/bridge_route_inventory.py`) matches before and after
- No private function from `get_routes.py` has external callers outside bridge routes
- File size drops from 2,667 lines to <800 lines per new file

---

## 4. Bare `except Exception` Proliferation

### Problem

**19 source instances** of `except Exception`/`suppress(Exception)` remain across **13 source files**:

- 28 total `src` suppression comments, including 20 BLE001 suppressions guarded by budget
- 19 source broad-catch sites remain after excluding one non-broad BLE001 suppression
- 0 without any annotation

**Worst concentrations:**

| File | Count | Typical pattern |
|------|-------|-----------------|
| `src/bridge/routes/error_boundary.py` | 3 | Route boundary fallback responses |
| `src/jobs/adapters/social.py` | 3 | Social adapter source isolation |
| `src/jobs/adapters/static_listing.py` | 3 | Adapter source isolation |
| `src/bridge/server/handler.py` | 1 | Server route boundary fallback |
| `src/bridge/pipeline_service.py` | 2 | Pipeline lifecycle boundaries |

**Why it blocks platform expansion:**

Bare `except Exception` masks every class of bug — `KeyError`, `TypeError`, `AttributeError`, `ValueError`, `ImportError` — all become silent continuations or generic fallbacks. When a new platform triggers unexpected code paths, the errors will be invisible.

### Target Boundary

- **Primary subsystem:** All `src/` subpackages
- **Entry file(s):** 99 files total — prioritize by criticality: bridge routes, pipeline service, adapters, desktop update, then shared/scrapers
- **Ownership boundary being clarified:** Each `except` block catches the narrowest exception type that documents why it can be safely ignored.
- **What becomes easier:** Debugging platform-specific issues, adding observability, understanding error contracts

### In Scope

- Replace every `except Exception` with a specific exception type or documented `except BaseException` where actually needed (e.g., cleanup)
- At minimum: add a comment explaining *which* exception is expected and why it's safe
- For adapter code: distinguish between "expected failures from external source" (catch `HTTPError`, `Timeout`, `ConnectionError`) vs. "unexpected bugs" (let them propagate)
- For bridge routes: catch specific HTTP/validation errors, let internal errors propagate to the error boundary

### Out of Scope

- `except Exception` in test files (2 instances, both intentional)
- Adding structured error types to every adapter (separate effort)
- Refactoring the error boundary pattern itself

### Implementation Shape

1. **Triage by risk:**
   - **Low risk:** Bridge `post_routes_update.py` (3 instances, all trivially replaceable with `OSError`)
   - **Medium risk:** Adapters (`static_listing.py`, `transport.py`) — replace with `(HTTPError, Timeout, ConnectionError)`
   - **Higher risk:** Bridge `pipeline_service.py`, `ops_api.py` — needs understanding of what can actually fail
2. **Each replacement:** Verify the specific exception type is actually raised in that code path (grep for `raise` in called functions)
3. **Remaining:** Add a comment documenting the expected exception for any that genuinely need broad catching

### Verification

- `tools/repo_health/source_suppression_budget.json` ratchets down when suppressions are removed; intentional HTTP/process boundaries remain documented
- `npm run test:py` passes
- No behavioral change — the caught exception type is a superset of what can actually occur
- For borderline cases: `npm run test:py:extended` passes

---

## 5. `_as_dict` / `_as_list` Proliferation (Demoted to P3)

**2026-06-16 correction:** Initial analysis overstated severity. The `utils.py` private versions (`_as_list`/`_as_dict`) have only **4 callers** (all in `src/jobs/*`). The canonical public versions in `json_shapes.py` (`as_json_list`/`as_json_object`) already serve 44+ files. This is a contained 5-file refactor, not a 45-file problem. The remaining 40+ duplicates use variants (copy, cast, Mapping+stringify, delegation) that are inconsistent by design and cannot all be blindly replaced.

### Problem

**42 definitions of `_as_dict`** and **28 definitions of `_as_list`** across ~45 unique files, with **5 semantically different variants**:

| Variant | Mechanism | Files | Behavioral Difference |
|---------|-----------|-------|----------------------|
| **Identity** | `return value if isinstance(value, dict/list) else {}/[]` | 15 (dict) / 12 (list) | Returns original reference. Caller mutation affects original. |
| **Copy** | `return dict(value)/list(value) if isinstance(...)` | 18 (dict) / 12 (list) | Returns new reference. Caller mutation is isolated. |
| **Cast** | `return cast(dict[str,Any], value) if ...` | 2 (dict) / 3 (list) | Same as identity, adds typing.cast for type-narrowing. |
| **Mapping+stringify** | `isinstance(value, Mapping) ... str(key): value` | 1 (`report_normalizer.py`) | Accepts any Mapping (not just dict). Stringifies all keys. **Cannot be replaced.** |
| **Delegation** | `return runtime_wait.as_dict(value)` | 1 (packaged_smoke) | Delegates to another module. Must verify identical semantics. |

Canonical shared version lives in `src/shared/utils.py:94-99` and `src/shared/json_shapes.py:9-26` (public variants `as_json_object`/`as_json_list`/`json_object_rows`).

**Constrained scope:** The `utils.py` private trio (`_as_list`/`_as_dict`/`_as_dict_rows`) is imported by only 4 files: `jobs/pipeline_cli.py`, `jobs/pipeline_finalize.py`, `jobs/pipeline_source_results.py`, `jobs/state_source_records.py`. These should migrate to the `json_shapes.py` public equivalents. The remaining 40+ private definitions are inconsistent-by-design and better left alone.

### Target Boundary

- **Primary subsystem:** `src/jobs/` (4 pipeline files) + `src/shared/utils.py` + `src/shared/json_shapes.py`
- **Entry file(s):** 4 pipeline files that import `utils.py`'s `_as_list`/`_as_dict`/`_as_dict_rows`
- **Ownership boundary being clarified:** The `json_shapes.py` public variants are canonical. The `utils.py` private trio is removed.
- **What becomes easier:** One less source of confusion for new modules; 5 files touched, no risk.

### In Scope

- Migrate the 4 pipeline files from `utils.py._as_list`/`_as_dict`/`_as_dict_rows` to `json_shapes.py.as_json_list`/`as_json_object`/`json_object_rows`
- Remove the private trio from `utils.py` (lines 93-103)
- Verify no other `src/jobs/` modules import them (the 4 files are the sole callers)

### Out of Scope

- The 40+ remaining private definitions in other packages (intentional divergence)
- The `report_normalizer.py` Mapping variant (semantically different)
- The delegation variant in `packaged_smoke/runtime.py`

### Implementation Shape

1. **Update 4 callers:** Replace `from src.shared.utils import _as_list, _as_dict` with `from src.shared.json_shapes import as_json_list, as_json_object` (and `json_object_rows` for `_as_dict_rows`)
2. **Remove from utils.py:** Delete lines 93-103 (the `_as_list`/`_as_dict`/`_as_dict_rows` definitions)
3. **Grep for remaining imports:** Confirm no other module imports these names from `utils.py`

### Verification

- `npm run test:py` passes
- Grep for `from src.shared.utils import.*_as_` returns zero results
- 4 pipeline files still produce identical output

---

## 6. Test Time/Port Coupling

### Problem

The test suite has coupling patterns that cause flakiness on loaded CI runners:

- **23 `time.sleep()` calls** across 12 test files — worst: 2.2s and 1.2s in `test_static_source_execution.py`
- **39 hardcoded port 8877 references** — risk of port conflict on parallel CI
- **81 `monkeypatch.setattr` calls** on `admin_bridge.py` internal constants (54 in `tests/admin/`, 27 in `tests/bridge/`)
- **7 `_helpers.py` files** across test subdirectories with overlapping patterns
- **Oldest test file: `test_jobs_fetcher_pipeline.py`** at 2,431 lines

**Why it blocks platform expansion:**

Flaky tests erode confidence during rapid platform iteration. Hardcoded ports prevent parallel test execution. Monkeypatching internals makes refactoring expensive.

### Target Boundary

- **Primary subsystem:** `tests/` (all subdirectories)
- **Entry file(s):** 12 files with `time.sleep()`, all files referencing port 8877, `tests/admin/_helpers.py`
- **Ownership boundary being clarified:** Tests rely on dependency injection and synchronization primitives, not wall-clock time. Ports are dynamically assigned.
- **What becomes easier:** CI reliability, parallel test execution, refactoring confidence

### In Scope

- Replace all `time.sleep()` in tests with `threading.Event` or `queue.Queue` synchronization
- Parameterize hardcoded port 8877 into a conftest fixture that uses `bind(('127.0.0.1', 0))`
- Reduce monkeypatching on `admin_bridge.py` internals — prefer service-level DI

### Out of Scope

- Rewriting `test_jobs_fetcher_pipeline.py` (2,431 lines) — create a separate charter
- Consolidating all 7 `_helpers.py` files (low ROI)
- Adding new integration tests (separate charter)

### Implementation Shape

1. **Replace `time.sleep(N)` with `threading.Event.wait(N)`:** For all 23 instances, replace the sleep with a synchronization primitive. Pattern: `event = threading.Event(); ... event.wait(timeout=N)` instead of `time.sleep(N)`. For concurrent work simulations, use `event.set()` from the mock.
2. **Port parameterization:** Add `bridge_port` fixture to root `conftest.py` that finds a free port via `socket.bind(('127.0.0.1', 0))`. Replace all hardcoded `8877` references with `bridge_port` fixture.
3. **Monkeypatch reduction target for admin tests:** The 27-path `patch_admin_bridge_paths` function in `tests/admin/_helpers.py` is the single highest-leverage target. If BridgeApi or RuntimeConfig carries these paths, tests can configure via the config object instead.

### Verification

- `npm run test:py` passes without flakiness on 3 consecutive CI runs
- `npm run test:py:extended` passes (if applicable)
- No `time.sleep()` calls remain in `tests/` (exceptions: genuinely slow external resource waits)
- No hardcoded `8877` in test files (exceptions: documentation strings, fixture templates)

---

## 7. Data Model & Contract Drift

### 7A. CanonicalJob Has 3 Drifting Representations

#### Problem

`CanonicalJob` — the core data type representing a game industry job — exists in **three separate representations** that have drifted apart:

| Representation | File | Type | Fields |
|---------------|------|------|--------|
| **Schema** | `src/core/schemas.py:14` | `CanonicalJobSchema` (Pydantic `BaseModel`) | 27 fields |
| **Dataclass** | `src/jobs/models.py:66` | `CanonicalJob` (frozen dataclass) | 31 fields |
| **Doc** | `docs/DATA_CONTRACT.md` §1 | Markdown table | 29 fields |

**Fields that differ:**

| Field | Schema | Dataclass | Doc | Risk |
|-------|--------|-----------|-----|------|
| `lifecycleEvent` | **Missing** | `str` | `string` | **Data loss**: Pydantic `model_validate()` silently strips this field (default `extra="ignore"`) |
| `lifecycleReason` | **Missing** | `str` | `string` | Same data loss risk |
| `locations` | **Missing** | `list[dict[str,Any]]` | **Missing** | Dataclass-only; neither validated nor documented |
| `locationSummary` | **Missing** | `str` | **Missing** | Same |
| `id` | `Any\|None` (default `None`) | `Any` (default `""`) | `string`/`number` | Three-way type inconsistency |

The Pydantic schema is the **validation boundary** (used by `validate_canonical_jobs_payload()` in `src/core/contracts.py:17`). The current code path calls `model_validate(row)` for validation only and passes the original row dict through unchanged — so no active data loss today. However, this is a **latent risk**: any future refactoring that uses `model_validate(row).model_dump()` for transformation would silently strip `lifecycleEvent`, `lifecycleReason`, `locations`, and `locationSummary` due to `extra="ignore"`.

#### Target Boundary

- **Primary subsystem:** `src/core/`, `src/jobs/models.py`, `docs/DATA_CONTRACT.md`
- **What becomes easier:** No silent data loss; one authoritative field list

#### In Scope

- Align `CanonicalJobSchema` with the dataclass: add the 4 missing fields
- Or switch all validation to use the dataclass (make it the single source of truth)
- Update `docs/DATA_CONTRACT.md` §1 to reflect the complete field set
- Fix `id` type inconsistency

---

### 7B. Fetch Report Normalization Duplicated

#### Problem

The fetch report (the core output of a jobs pipeline run) is normalized in **two separate functions** that produce **different output shapes**:

| Aspect | Bridge (`report_normalizer.py:300`) | Jobs (`contracts_fetch_report.py:249`) |
|--------|-------------------------------------|---------------------------------------|
| **Uses shared `live_task.py`?** | No — has its own `_normalize_task_progress()` | Yes — `normalize_live_task_payload()`/`build_live_task_contract_fields()` |
| **Task progress counts** (completed) | 5 keys: `resolvedSources`, `sourceCount`, `outputCount`, `failedSources`, `excludedSources` | 9 keys: adds `totalTasks`, `queuedTasks`, `runningTasks`, `completedTasks` |
| **Source rows** | Subset (no `loss`, `stats`, `stageTimingsMs`, HTTP fields, migration fields) | Richer with conditional extra sections |
| **Runtime** | Passthrough `**dict(runtime)` + manual `slowestSources`/`timingSummary` | Explicit field-by-field via `normalize_runtime_payload()` |
| **Extra sections** | `sourceRuns` | `taskType`, `active`, `heartbeatAt`, `contaminationAudit`, `locationQualityAudit`, `lifecycleSummary`, `healthSummary` (8 sections bridge lacks) |

**Evidence of divergence:**
- `socialSummary` sub-objects are near-identical (same 6-channel keys)
- `timingSummary` sub-structures are identical for overlapping keys (`stageTotalsMs`, `adapterTimings`, `slowestAdapters`, `highCostLowYieldSources`)
- Source-row normalization: bridge clamps values to `[0, 1_000_000]`, jobs has no upper bound; bridge defaults `healthScore` to 0, jobs defaults it to 100

#### Target Boundary

- **Primary subsystem:** `src/bridge/report_normalizer.py`, `src/jobs/common/contracts_fetch_report.py`, `src/shared/live_task.py`
- **What becomes easier:** Single code path for fetch report normalization; bridge uses shared `live_task.py` infrastructure

#### In Scope

- Remove bridge's hand-rolled `_normalize_task_progress()` and `_derive_fetch_task_progress()`; delegate to `live_task.py` functions
- Share source-row normalization between bridge and jobs
- Unify `socialSummary` and `timingSummary` construction in a shared helper

---

### 7C. `parse_iso` Proliferation (9 Locations, 4 Semantic Groups)

#### Problem

The canonical `parse_iso()` lives in `src/shared/utils.py:26`. It has been re-implemented in **9 locations** across the codebase, with **4 semantically distinct behaviors**:

| Group | Locations | Z Handling | tzinfo Normalization | Return Type | Risk |
|-------|-----------|------------|---------------------|-------------|------|
| **Canonical** | `shared/utils.py` + 3 delegation wrappers | `endswith("Z")` | Full: `replace(UTC)` + `astimezone(UTC)` | `datetime\|None` | — |
| **Replace-all Z** | `storage/task_runtime.py`, `storage/evidence_archive.py`, `source_discovery/core_scoring.py` | `replace("Z", ...)` — broader match | Full | `datetime\|None` | Low — unlikely to hit false Z match |
| **No tzinfo normalization** | `bridge/task_history.py`, `bridge/sync_service.py`, `bridge/lifecycle_cleanup.py` | `replace("Z", ...)` | **None** — returns naive datetimes | `datetime\|None` (or missing) | **Latent comparison bug** — callers may compare against UTC datetimes |
| **Float return** | `ship/runtime_launcher.py` | `endswith("Z")` | Full (via `.timestamp()`) | `float\|None` | Different contract by design |

The 3 bridge versions (`task_history.py:13`, `sync_service.py:1001`, `lifecycle_cleanup.py:37`) are the most dangerous: they return datetimes with **no UTC normalization**, so comparisons against canonical-parse_iso datetimes can be off by the local UTC offset.

#### Target Boundary

- **Primary subsystem:** `src/bridge/`, `src/storage/`, `src/shared/utils.py`
- **What becomes easier:** One canonical datetime parser; no latent timezone comparison bugs

#### In Scope

- Replace the 3 bridge implementations with the canonical version
- Replace the 2 storage implementations with the canonical version
- Keep the float-return variant (different contract, intentional)
- Keep the delegation wrappers (thin pass-throughs)

---

## 8. Desktop/Ship Platform Gaps

### 8A. macOS Platform Gap (Deferred)

#### Problem

The desktop runtime has **no macOS support**. A new `_darwin.py` platform module is needed, and 9 OS-check conditionals across 6 files all treat macOS incorrectly (as "not Windows, so use Linux conventions").

**Critical gaps:**

| Function | File | Line | Behavior | macOS Impact |
|----------|------|------|----------|-------------|
| `_process_identity_matches` | `session.py` | 135-136 | `if os.name != "nt": return True` — skips all identity verification | No process-path validation on macOS |
| `_resolve_browser_from_registry_app_paths` | `browser.py` | 94-107 | Always returns `""` on non-Windows | No registry-based browser discovery (needs `LaunchServices`/`CFBundleCopyExecutableURL`) |
| `_legacy_update_success_marker_paths` | `startup_watchdog.py` | 183-184 | Returns `None` on non-Windows | Legacy marker migration unsupported |
| `_resolve_desktop_session_root_fallback` | `desktop_update_shared.py` | 323-333 | Uses XDG path (`~/.local/share/`) on non-Windows | Should use `~/Library/Application Support/` on macOS |
| `_windows_create_kill_on_close_job` | `_linux.py` | 303-308 | Emulated with global dict on Linux | Would need `os.setpgid` on macOS |
| Platform module | `desktop_app/_*.py` | — | `_windows.py` and `_linux.py` exist | **No `_darwin.py`** |

**Current source location:** the relevant session/browser/runtime helpers live under `src/ship/desktop_app/`, not `src/bridge/`. `desktop_app/__init__.py` still selects `_linux.py` for every non-Windows platform.

**Status:** deferred. This is real platform debt, but macOS support is not a near-term product priority and should not block the current P0 cleanup pass.

#### Target Boundary

- **Primary subsystem:** `src/ship/desktop_app/`
- **What becomes easier:** macOS desktop support; cleaner platform abstraction

#### Deferred Scope

- Create `_darwin.py` platform module (process management, window enumeration, browser discovery)
- Update `desktop_app/__init__.py` to import `_darwin` on `sys.platform == "darwin"`
- Fix `desktop_update_shared.py:323-333` to use `~/Library/Application Support/` on macOS
- Fix `session.py:135-136` to verify process identity on macOS (via `os.kill(pid, 0)` + proc info)
- Fix `browser.py:94-107` to discover browsers via `CFBundleCopyExecutableURL` on macOS

---

### 8B. Update Subsystem Over-Engineering

#### Problem

The update system has **16 files** split across two parallel subsystems:

| Group | Files | Count |
|-------|-------|-------|
| `update_manager*` | `update_manager.py`, `update_manager_apply.py`, `update_manager_bootstrap.py`, `update_manager_cli.py`, `update_manager_paths.py`, `update_manager_recovery.py`, `update_manager_state.py`, `update_manager_validation.py` | **8** |
| `desktop_update*`/`desktop_updater*` | `desktop_update.py`, `desktop_update_service.py`, `desktop_update_shared.py`, `desktop_update_state.py`, `desktop_updater.py`, `desktop_updater_install.py`, `desktop_updater_release.py`, `desktop_updater_ui.py` | **8** |

**Evidence of over-engineering:**
- `update_manager` has runtime consumers inside `src/ship/` (`runtime_launcher.py`, `desktop_updater_install.py`, `desktop_update_shared.py`) plus script/packaging consumers. The issue is parallel updater architecture and root/re-export facades, not that `update_manager` is build-time-only.
- `desktop_updater.py` (173 lines) is primarily a re-export facade: lines 35-95 are attribute assignments splicing together symbols from 6 sibling modules, plus root-injection seam setup
- `desktop_update.py` (261 lines) is a pure re-export facade: ~80 re-exported names with `__all__`, plus root-injection seam setup, zero executable logic beyond constants
- Both subsystems use the same root-injection pattern (`module.root = sys.modules[__name__]`)
- `desktop_update_shared.py:278-281` lazily imports `update_manager` inside a function body — runtime coupling between the two subsystems

#### Target Boundary

- **Primary subsystem:** `src/ship/` (update subsystem only)
- **What becomes easier:** Single coherent update code path; no root injection

#### In Scope

- Merge the `update_manager*` files into the `desktop_update*` subsystem (or vice versa — they do the same thing)
- Remove root-injection seams; use explicit dependency passing instead
- Keep the pure re-export facades (`desktop_updater.py`, `desktop_update.py`) import-compatible first; migrate direct consumers to leaves before reducing them to smaller wrappers

---

## 9. Shared Utility & Infrastructure Gaps

### 9A. `json_io.py` Shared-Layer Isolation Violation

#### Problem

`src/shared/json_io.py:11` imports from `src.storage_metrics`:
```python
from src.storage_metrics import duration_ms, record_json_write
```

The `shared/__init__.py` (line 1) explicitly states: *"Shared package for cross-cutting utilities (stdlib-only, no jobs/bridge/admin_bridge deps)."*

`src.storage_metrics` is a ~500-line module with JSONL journaling, threading, and file I/O — decidedly not stdlib. This import:
- Couples the shared layer to the storage metrics layer
- Creates a dependency that makes `json_io.py` untestable without the full storage layer
- Makes the `shared/__init__.py` contract a lie

#### Target Boundary

- **Primary subsystem:** `src/shared/json_io.py`, `src/storage_metrics.py`
- **What becomes easier:** Shared layer is truly stdlib-only; `json_io.py` testable in isolation

#### In Scope

- Move the storage-metrics recording calls into the callers of `json_io.py` functions (e.g., `save_json_atomic` in `source_registry_io.py`)
- Or extract a minimal `StorageMetrics` interface that `json_io.py` can depend on without the full implementation

---

### 9B. CSS Build Pipeline & Infrastructure Gaps

#### Problem

The frontend has a JS build pipeline (esbuild) but **zero CSS processing**:

| Gap | Detail | Impact |
|-----|--------|--------|
| No CSS build | `build_container_frontend.mjs` only bundles JS. CSS served verbatim from `styles/`. | No minification, no bundling, no autoprefixing |
| Monolithic `components.css` | 2,632 lines with 18 duplicated `radial-gradient` patterns in 6 variant families | Maintenance burden; 2 exact byte-for-byte duplicates |
| Hardcoded light-mode bug | `components.css:2125` `box-shadow` hardcodes `rgba(187, 134, 252, 0.8)` = dark-theme `--accent`. Light-theme accent (`#6f56f1`) won't match | Visual regression in light mode |
| Inconsistent cache-busting | `saved.css` has no version at all. `base.css`/`components.css`/`theme.js` never versioned. Version numbers differ per page (admin config v=1, jobs config v=4). | Stale CSS served on deploy |
| Theme init duplicated | `index.html:10-26` (inline script) and `theme.js:81-87` (DOMContentLoaded handler) both initialize theme | Maintenance hazard |

#### Target Boundary

- **Primary subsystem:** `styles/`, `scripts/build_container_frontend.mjs`, HTML files
- **What becomes easier:** Reliable cache busting, consistent theming, automated CSS processing

#### In Scope

- Add CSS bundling/minification/hashing to the esbuild build script
- Extract shared gradient utility classes to eliminate the 18 repeated patterns
- Fix the hardcoded box-shadow in `.fetch-progress`
- Auto-generate CSS cache-busting versions from content hashes (same as JS already does)
- Deduplicate theme initialization (keep only the inline script, remove `theme.js` handler or vice versa)

---

## Implementation Sequencing

### Phase 1: Quick Wins (~2 days)

| # | Action | Area | Files | Risk | Dependencies |
|---|--------|------|-------|------|-------------|
| 1 | Fix `parse_iso` in 3 bridge files: replace with `shared/utils.py` canonical version | §7C | 3 files | Low | None |
| 2 | Fix `json_io.py` layer violation: move storage_metrics calls to callers | §9A | 2-3 files | Low | None |
| 3 | Fix hardcoded box-shadow CSS bug + deduplicate theme init | §9B | 2 files | Low | None |
| 4 | Migrate 4 pipeline files from `utils.py._as_list`/`_as_dict` to `json_shapes.py` public variants; remove private trio from `utils.py` | §5 | 5 files | Low | None |
| 5 | Add BridgeApi field classification guardrail before dead-stub removal | §1 | 3 files | Done | Completed 2026-06-17 |
| 5A | Remove dead stub functions from BridgeApi after classification evidence | §1 | 1 file | Low | Phase 1 #5 |
| 6 | Merge `_default_current_task_state_payload` / `_default_current_task_state_summary_payload` | §1 | 1 file | Low | None |
| 7 | Replace `time.sleep(N)` in tests (tiny delays) with `threading.Event` | §6 | ~8 files | Low | None |
| 8 | Parameterize port 8877 with conftest fixture | §6 | ~15 test files | Low | None |

### Phase 2: Medium Effort (~3 days)

| # | Action | Area | Files | Risk | Dependencies |
|---|--------|------|-------|------|-------------|
| 9 | Extract partial JSON parser from get_routes.py → `src/shared/partial_json.py` | §3 | 2 files | Low | None |
| 10 | Extract provider coverage link backfill from get_routes.py | §3 | 2 files | Done | Completed 2026-06-17 |
| 10A | Extract registry source table compaction from get_routes.py | §3 | 2 files | Done | Completed 2026-06-17 |
| 10B | Extract fetch-report source-run read support from get_routes.py | §3 | 2 files | Done | Completed 2026-06-17 |
| 10C | Extract ops diagnostics route family from get_routes.py | §3 | 3 files | Done | Completed 2026-06-17 |
| 10D | Extract ops status route family from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10E | Extract registry route family from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10F | Extract discovery route family from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10G | Extract desktop local-data GET route family from get_routes.py | §3 | 5 files | Done | Completed 2026-06-17 |
| 10H | Extract fetch-report route family from get_routes.py | §3 | 5 files | Done | Completed 2026-06-17 |
| 10I | Extract source-policy recommendations GET route from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10J | Extract registry-conflicts GET route from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10K | Extract sync status GET route from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10L | Extract pipeline task GET routes from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10M | Extract app GET routes from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10N | Extract admin bootstrap GET route from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 10O | Extract admin ops-tab counts GET route from get_routes.py | §3 | 4 files | Done | Completed 2026-06-17 |
| 11 | Replace `except Exception` in low-risk files (post_routes_update, adapters, shared) | §4 | ~15 files | Partial | Adapter-audit runner fallback, adapter recovery fallback cleanup, update POST route, run-history ISO parse and pipeline-status fallbacks, jobs transport request/cleanup/strategy-selection/client-construction/fetch-retry, jobs browser fallback wrapper, static source registry facade fallback, bridge server-handler bookkeeping, metric-only route, retained-log, helper fallback, admin bootstrap best-effort fallback, source-discovery browser fallback setup, source-discovery ordered-text/directory-index cleanup, source-discovery fetch retry/probe/web-search scan cleanup, source-discovery sheet-directory URL validation cleanup, source-discovery auto-approval finalization cleanup, NCSoft/generic/listing/rendered-card/Milestone/Kojima static plugin fetch cleanup, Jobylon parse fallback cleanup, Personio provider source cleanup, provider API source/error boundary cleanup, source-sync packaged-key decrypt fallback, source-sync shard progress callback cleanup, container gateway termination cleanup, ship runtime launcher client-disconnect cleanup, desktop-app process termination and stale-lock callback cleanup, ship update-manager CLI cleanup, optional-certifi, console-encoding, profile-summary, registry journal compaction, source-sync remote timing/idempotency, source-sync shard pull cleanup, SQLite write rollback cleanup, dev-supervisor POSIX kill/reclaim cleanup, shared HTTP batch optional import/fetch/progress cleanup, desktop update facade optional psutil import cleanup, desktop update manifest optional cryptography import cleanup, update-manager apply rollback cleanup, desktop updater install rollback cleanup, source-policy migration source-id fallback cleanup, fetch-report detail parser fallback cleanup, parser-regression redirect fallback cleanup, canonical job-link redirect fallback cleanup, source-check fetch, embedded-fetch, Playwright runtime fallback cleanup, fetcher launch spawn fallback cleanup, packaged runtime snapshot fallback cleanup, packaged runtime optional status fallback cleanup, packaged failure-metrics fetch fallback cleanup, packaged update rehearsal fallback cleanup, Windows desktop probe fallback cleanup, desktop launcher retry fallback cleanup, desktop launcher diagnostic fallback cleanup, bridge HTTPD startup callback cleanup, server response-write socket boundary cleanup, source-policy recommendation export cleanup, Scrapy runner envelope/import/stdin/static-source/queue cleanup, structured provider detail/workday cleanup, social adapter subsource cleanup, and static-listing URL/probe/artifact/dynamic/listing-fetch/page-processing cleanup slices completed |
| 12 | Service holder dataclass for admin_bridge singletons | §2 | 6 files | Medium | None |
| 13 | Align `CanonicalJobSchema` with canonical dataclass: add missing 4 fields (`lifecycleEvent`, `lifecycleReason`, `locations`, `locationSummary`), fix `id` type | §7A | 2-3 files | Medium | None (but verify with integration test) |
| 14 | Start updater facade migration: add direct leaf imports for low-risk consumers while keeping `desktop_updater.py` and `desktop_update.py` compatible | §8B | 2-4 files | Partial | Facade consumer inventory, root-dependency inventory, release-builder direct leaf import, packaged rehearsal manifest helper/constant migration, release-notes history and desktop updater test leaf migrations, and pure desktop update helper root-binding reduction completed; runtime facade migration still open |

### Phase 3: Higher Effort (~4-5 days)

| # | Action | Area | Files | Risk | Dependencies |
|---|--------|------|-------|------|-------------|
| 16 | Split get_routes.py dispatch into per-domain files | §3 | 5-7 files | Medium | Phase 2 #9, #10 |
| 17 | Replace remaining high-risk `except Exception` in pipeline_service, ops_api | §4 | 3 files | Partial | Pipeline child-boundary wrappers completed; remaining pipeline lifecycle boundaries still open |
| 18 | Replace root injection seam with explicit dependency passing | §2 | 6 files | High | Phase 2 #12 |
| 19 | Bridge fetch report normalization: extract shared-compatible task-progress helpers while preserving bridge output shape | §7B | 2-3 files | High | None |
| 20 | Remove module-level caches from get_routes.py | §3 | 1 file | Medium | Phase 3 #16 |

### Phase 4: Deferred

| # | Action | Area | Rationale |
|---|--------|------|-----------|
| 24 | `admin_bridge.py` full migration to service holder | §2 | Requires Phase 2 #12 + Phase 3 #18 to settle |
| 25 | Reduce `patch_admin_bridge_paths` 27-path monkeypatch | §2 | Depends on service holder settling |
| 26 | Replace partial JSON parser with `ijson` | §3 | Separate decision, not blocking |
| 27 | OpsApi full decomposition (1,328 lines) | — | Separate charter needed |
| 28 | Unify fetch report source-row normalization (bridge vs jobs) | §7B | Deeper than progress-type fix; requires shared helper |
| 29 | Unify `socialSummary` / `timingSummary` construction in shared helper | §7B | Deferred until bridge normalization uses `live_task.py` |
| 30 | Add CSS bundling/minification/hashing to esbuild build script | §9B | Not blocking; manual cache-busting works for now |
| 31 | Deduplicate 18 gradient patterns in `components.css` | §9B | Maintenance quality, not platform-blocking |
| 32 | Align `docs/DATA_CONTRACT.md` with final CanonicalJob field set | §7A | Do after Phase 2 #13 settles |
| 33 | `parse_iso` — replace 2 storage implementations with canonical version | §7C | Low risk; can batch with Phase 1 #1 if convenient |
| 34 | `source_discovery/core_scoring.py` parse_iso variant (Z replace-all) | §7C | Low risk |
| 35 | macOS desktop support (`_darwin.py`, LaunchServices browser discovery, macOS session roots) | §8A | Deferred by current product priority |

---

## Files Summary

| Phase | Files Created | Files Modified | Files Deleted |
|-------|---------------|----------------|---------------|
| 1 | 0 | ~38 | 0 |
| 2 | 1 | ~33 | 2 |
| 3 | 3 | ~22 | 0 |
| 4 | 0 | ~4 | 0 |
| **Total** | **~4** | **~96** | **2** |

No compatibility facade deletion is assumed. New files are expected only for extracted leaves such as `partial_json.py`; macOS `_darwin.py` is deferred.

---

## Risk Registry

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| CanonicalJobSchema adds missing fields but pipeline code assumes old narrower schema | Medium | High | Integration test: write a job with lifecycleEvent set, read it back through validate_canonical_jobs_payload, assert field preserved |
| Bridge fetch normalization switch to `live_task.py` changes progress shape for existing consumers | Medium | High | Before/after: capture `_normalize_task_progress` output, diff against `live_task.py` output, verify superset equality |
| Deferred macOS work is accidentally mixed into current P0 cleanup | Medium | Medium | Keep `_darwin.py` and macOS session/browser work out of this pass unless explicitly reprioritized |
| Desktop update facade removal breaks an import outside `src/ship/` | Low | Medium | Grep for imports from `desktop_updater` / `desktop_update` before removing; fix any external references |
| `parse_iso` swap (naive → tz-aware) changes time comparison behavior | Medium | Medium | Before/after: capture return values for same input; verify all callers that compare datetimes still produce same ordering |
| Copy-semantics `_as_dict` callers break when switched to identity | Low | Medium | Now P3 — 4 callers only, already using identity variant. Risk near-zero. |
| `except Exception` replacement misses an edge case | Medium | Medium | Pair each replacement with a grep for what `raise`s in the protected block |
| get_routes.py split misses a route path | Low | High | Run route inventory before/after; compare all paths |
| admin_bridge service holder changes service init order | Low | High | Run full test suite; verify `main()` boot sequence |
| Test port fixture conflicts with existing port-0 bind in test helpers | Low | Low | Use a distinct fixture name; verify no double-bind |

---

## Acceptance Criteria

- All 12 areas show measurable improvement — metrics per area documented in PR description
- No product-facing behavior regression:
  - Identical bridge responses (diff bridge output before/after for a reference session)
  - Same job pipeline output (run a reference job, diff canonical_jobs payload)
  - Unchanged sync behavior (sync a reference source, verify identical results)
  - Fetch report normalization produces same consumer-facing shape (superset equality check)
- CanonicalJob lifecycle/location fields survive write→validate→read round-trip (integration test added)
- `parse_iso` in bridge files returns tz-aware datetimes identical to canonical version for same input
- `npm run test:py` and `npm run test:py:extended` pass
- `npm run lint:repo-guardrails` passes (no new cross-subsystem leak)
- Future platform entrypoint can be added without touching `admin_bridge.py` globals or BridgeApi's 90-field bag
- macOS platform support remains deferred and is not required for this P0 cleanup pass
- Update subsystem: `desktop_updater.py` and `desktop_update.py` remain import-compatible while direct consumers move toward leaf modules; root-injection seams are reduced only with updater tests and packaged rehearsal coverage

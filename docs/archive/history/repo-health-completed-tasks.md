# Repository Health Completed Tasks

> Historical repo-health completion record preserved for archive/reference use. Start with [`../../repo-health-action-tracker.md`](../../repo-health-action-tracker.md) for current priorities before using this record.

This page stores completed items moved out of the active repository-health tracker so [`../../repo-health-action-tracker.md`](../../repo-health-action-tracker.md) can stay focused on work that still needs attention.

## Completed P0 Items

1. **Completed: type the `admin_bridge` composition root and its immediate helper boundary.**
   The enforced mypy scope now includes `src/admin_bridge.py`, `src/bridge/admin_entrypoint_services.py`, and `src/bridge/admin_entrypoint_runtime.py` alongside the prior seven-file bridge/admin leaf scope. The surrounding service/runtime helper aliases now expose typed facade protocols instead of leaking `Any` into the composition root, and the broad audit dropped from `835` to `796` errors.
   **Done when:** complete.

2. **Completed: add a Python dependency lock strategy for reproducible builds.**
   `requirements-lock.txt` is now the canonical Python lock artifact, and CI/release install surfaces consume it instead of floating `requirements.txt`.
   **Done when:** complete.

3. **Completed: stop generated-file newline churn in `data/source-approval-state.json`.**
   `save_json_atomic` now writes newline-terminated JSON, and targeted regression coverage protects the writer behavior used by the approval-state file.
   **Done when:** complete.

4. **Completed: type the static adapter/detail parser cluster.**
   The enforced mypy scope now includes `src/jobs/adapters/html_parsers.py`, `src/jobs/adapters/static_detail_heuristics.py`, and `src/jobs/adapters/static_listing.py`. This pass normalized JSON-LD organization/identifier reads, stage timing payloads, and listing batch payload/profile reads without changing parser output, traversal behavior, cache decisions, or progress payloads. The broad audit dropped from `452` errors in `94` files to `295` errors in `81` files.
   **Done when:** complete.

5. **Completed: resolve GitHub Dependabot high-severity vulnerabilities.**
   The Scrapy remediation updated the direct dependency to the latest released `Scrapy==2.15.0`, raised the `scrapy-playwright` source requirement floor to `>=0.0.46`, and regenerated `requirements-lock.txt`. The remediation was validated with the dependency import/version check, `python -m pip check`, focused Scrapy/runtime tests, refactor and lint gates, and a forced orchestrator build; the remaining `pip-audit` Scrapy advisory had no fixed version and affected an unused Scrapy file-download storage path.
   **Done when:** complete.

6. **Completed: type the static listing/detail runtime cluster.**
   The enforced mypy scope now includes `src/jobs/adapters/static_listing_flow.py`, `src/jobs/adapters/static_detail.py`, `src/jobs/adapters/static.py`, and `src/jobs/adapters/plugins/static/_rendered_cards.py`. This pass preserved the static adapter compatibility roots while normalizing detail payload reads, listing candidate state, and rendered-card primary location typing.
   **Done when:** complete.

7. **Completed: type the jobs source-execution compatibility cluster.**
   The enforced mypy scope now includes `src/jobs/pipeline_source_progress.py`, `src/jobs/pipeline_stage_source_execution.py`, `src/jobs/fetcher_compat_runtime.py`, `src/jobs/pipeline_timing.py`, and `src/jobs/common/contracts_task_state.py`. This pass preserved the patchable source-execution root assignments while normalizing task-state summary/output payloads, runtime timing/detail metrics, and compatibility runtime monkeypatch aliases. The broad audit dropped from `273` errors in `73` files to `260` errors in `68` files.
   **Done when:** complete.

8. **Completed: type the jobs reporting/text utility tail cluster.**
   The enforced mypy scope now includes `src/jobs/reporting_summary.py`, `src/jobs/reporting_social.py`, `src/jobs/location_bucket_manifest.py`, `src/jobs/page_gating.py`, `src/jobs/common/http.py`, and `src/jobs/text_utils.py`. This pass normalized optional report lists, tightened untyped text/HTTP return values, and made manifest/page-gating utility reads locally checkable without changing report payloads, page classification behavior, or persisted job data contracts. The broad audit dropped from `260` errors in `68` files to `253` errors in `62` files.
   **Done when:** complete.

9. **Completed: type the jobs transport/canonicalization tail cluster.**
   The enforced mypy scope now includes `src/jobs/transport.py`, `src/jobs/canonicalize.py`, `src/jobs/adapters/plugins/static/littlechicken.py`, and `src/fetch_incremental_sanity_benchmark.py`. This pass preserved the patchable transport `httpx` alias, removed stale canonicalization ignore noise, normalized Little Chicken detail HTML typing, and typed incremental benchmark loader selection without changing loader order, fetch behavior, parser output, or compatibility surfaces. The broad audit dropped from `253` errors in `62` files to `245` errors in `58` files.
   **Done when:** complete.

10. **Completed: type the source registry/checker and source-sync boundary.**
   The enforced mypy scope now includes `src/source_registry.py`, `src/bridge/source_checker.py`, `src/bridge/source_helpers.py`, `src/bridge/source_check_http.py`, `src/source_sync_crypto.py`, `src/source_sync_runtime.py`, `src/source_sync_config.py`, `src/source_sync_snapshot.py`, and `src/source_sync.py`. This pass normalized registry/checker JSON-shape reads, removed stale source-check ignore noise, typed sync crypto/runtime helpers, declared the sync root auth attributes, and kept the source-sync compatibility root thin. The broad audit dropped from `245` errors in `58` files to `211` errors in `49` files.
   **Done when:** complete.

11. **Completed: type the desktop app launch/session cluster.**
   The enforced mypy scope now includes `src/ship/desktop_app/_windows.py`, `src/ship/desktop_app/launcher_flow.py`, `src/ship/desktop_app/session.py`, `src/ship/desktop_app/browser.py`, and `src/ship/desktop_app/launcher_diagnostics.py`. This pass normalized desktop runtime payload reads, typed browser launch subprocess/environment boundaries, and made Windows process/window metric reads checkable without changing launcher flow, session recovery, browser lifecycle, or stale-runtime reclaim behavior. The broad audit dropped from `211` errors in `49` files to `173` errors in `44` files.
   **Done when:** complete.

12. **Completed: reduce the remaining mypy sweep debt below the current target.**
   The enforced mypy scope now includes the desktop updater boundary, packaged runtime/startup support, release repeatability helper, desktop app startup process helpers, and high-yield source-discovery/local-data cleanup files. This pass also repaired the local active-interpreter vulture installation, kept the desktop app formatter check green, and reduced the broad audit from `173` errors in `44` files to `40` errors in `19` files without changing runtime behavior, updater compatibility roots, packaged smoke contracts, or persisted payload shapes.
   **Done when:** complete.

13. **Completed: finish the broad mypy sweep.**
   Broad `python -m mypy src` now passes with `0` errors across `313` source files, and `mypy.ini` now enforces the full `src/` tree instead of a staged file list. The final pass cleared bridge run-history/runtime routes, source-discovery state-shape and JSON helpers, local saved-job normalization, scraper utility tails, optional HTTP aliases, and concrete string-return helpers without changing runtime behavior or payload contracts.
   **Done when:** complete.

## Completed P1 Items

5. **Completed: continue the mypy staged rollout through bridge live-payload and report JSON helpers.**
   The enforced mypy scope now includes the bridge/admin runtime surface that feeds task status, ops health, and report summaries, plus a shared internal JSON-shape helper. The lane covers `src/shared/json_shapes.py`, `src/shared/live_task.py`, `src/bridge/ops_task_fetch_live.py`, `src/bridge/ops_task_discovery_live.py`, `src/bridge/discovery_service.py`, `src/bridge/sync_service.py`, `src/bridge/sync_task_flow.py`, `src/bridge/ops_health.py`, `src/bridge/routes/post_routes_admin.py`, and `src/fetcher_metrics.py`. The broad audit dropped from `796` errors in `119` files to `677` errors in `110` files.
   **Done when:** complete.

6. **Completed: continue the mypy staged rollout through source/discovery audit JSON lanes.**
   The enforced mypy scope now includes the JSON-heavy source/discovery reporting and audit helpers: `src/source_discovery/reporting_progress.py`, `src/source_discovery/runtime_metrics.py`, `src/pipeline_audit.py`, and `src/source_audit_sweep.py`. This lane reused shared JSON-shape narrowing without changing report payloads, markdown text, or CLI behavior. The broad audit dropped from `677` errors in `110` files to `607` errors in `106` files.
   **Done when:** complete.

7. **Completed: continue the mypy staged rollout through jobs runtime/report contract helpers.**
   The enforced mypy scope now includes `src/jobs/reporting_queues.py`, `src/jobs/common/contracts_runtime.py`, `src/jobs/pipeline_source_loop.py`, and `src/jobs/pipeline_finalize.py`. This pass normalized optional report queues and runtime timing payloads, replaced the source-loop fallback namespace with `PipelineTaskRuntime`, and passed typed social-review rows into the social experiment helpers. The broad audit dropped from `295` errors in `81` files to `273` errors in `73` files.
   **Done when:** complete.

8. **Completed: reduce JS hygiene noise before the next broad frontend refactor.**
   ESLint now reports `0 warnings, 0 errors`, and `knip` reports `0` unused JS exports. This pass removed production discovery/controller warning sources, pruned unused test imports, removed dead loading-render code, and narrowed internal-only frontend/test helper exports without changing runtime or UI behavior.
   **Done when:** complete.

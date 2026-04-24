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

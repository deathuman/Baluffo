# Repository Health Action Tracker

> - **Status:** Active
> - **Use this when:** reviewing repository health, prioritizing maintenance work, or correcting external repo audits
> - **Canonical for:** validated repo-health findings and immediate improvement priorities
> - **Not canonical for:** architecture ownership, contracts, or release procedure
> - **Then inspect:** [`testing.md`](testing.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`RELEASE.md`](RELEASE.md)
> - **Last updated:** 2026-04-24

This page converts an external repository analysis into a repo-native action tracker. The source analysis was reviewed against the current repository state at `f722957`, and only validated claims are carried forward into strengths, gaps, and next steps.

Completed items are archived in [`archive/history/repo-health-completed-tasks.md`](archive/history/repo-health-completed-tasks.md) so this page stays focused on active repository-health work.

## Validation Snapshot

| Metric | Current validated value |
|--------|-------------------------|
| Python files | `313` |
| Frontend JS files | `184` |
| Top-level HTML entry points | `4` (`admin.html`, `index.html`, `jobs.html`, `saved.html`) |
| Python test files | `97` |
| Coverage lane | `1606 passed, 74 deselected`, total coverage `75%` |
| Broad type-check run | `python -m mypy src --no-incremental` -> `607 errors in 106 files (checked 313 source files)` |
| Enforced type-check gate | `python -m mypy --config-file mypy.ini --no-incremental` passes on the staged 24-file scope (`src/python_version_guard.py`, `src/pipeline_io.py`, `src/bridge/report_normalizer.py`, `src/bridge/api.py`, `src/bridge/admin_registry_api.py`, `src/bridge/admin_task_runtime.py`, `src/bridge/ops_live_payload.py`, `src/admin_bridge.py`, `src/bridge/admin_entrypoint_services.py`, `src/bridge/admin_entrypoint_runtime.py`, `src/shared/json_shapes.py`, `src/shared/live_task.py`, `src/bridge/ops_task_fetch_live.py`, `src/bridge/ops_task_discovery_live.py`, `src/bridge/discovery_service.py`, `src/bridge/sync_service.py`, `src/bridge/sync_task_flow.py`, `src/bridge/ops_health.py`, `src/bridge/routes/post_routes_admin.py`, `src/fetcher_metrics.py`, `src/source_discovery/reporting_progress.py`, `src/source_discovery/runtime_metrics.py`, `src/pipeline_audit.py`, `src/source_audit_sweep.py`) |
| ESLint | `137 warnings, 0 errors` |
| `knip` | `20` unused JS exports |
| Python lock file | `requirements-lock.txt` present |
| Node lock file | `package-lock.json` present |
| Dependabot alert signal | GitHub push reported `6` high vulnerabilities on the default branch; local Scrapy remediation updated `Scrapy==2.12.0` to `Scrapy==2.14.2`, and exact alert closure still needs Dependabot dashboard validation after push |

## Confirmed Strengths Worth Protecting

- **Docs/wiki structure:** [`INDEX.md`](INDEX.md), [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) form a clear routing stack and are actively maintained.
- **Thin compatibility-surface discipline:** the repo consistently protects stable roots and shims with explicit contract tests and routing docs instead of letting refactors collapse back into monoliths.
- **Packaging and updater rehearsals:** packaged smoke, updater, sync rehearsal, orphan reclaim, and browser-job flows are covered by dedicated release-oriented verification lanes.
- **Startup and performance instrumentation:** startup probes, timing lanes, and discovery/perf sanity scripts are real maintained systems, not placeholder docs.

## Confirmed Gaps Worth Acting On

### P0

4. **In progress: resolve GitHub Dependabot high-severity vulnerabilities.**
   GitHub reported `6` high vulnerabilities on the default branch during the latest push. The local Scrapy remediation updates the direct dependency from `Scrapy==2.12.0` to `Scrapy==2.14.2` and regenerates `requirements-lock.txt`; the Scrapy-adjacent lock entries (`scrapy-playwright`, `twisted`, `cryptography`, `pyopenssl`, `lxml`, `parsel`, `w3lib`, and `queuelib`) remained stable, and `brotli` is not present in the lock. Validation so far: `python -c "import scrapy; print(scrapy.__version__)"` -> `2.14.2`, `python -m pip check` passed, focused Scrapy/runtime tests passed (`187 passed`), `cmd /c npm run test:refactor:changed` passed, `cmd /c npm run lint:precommit:changed` passed, and `python scripts/orchestrator.py build --force` passed with run `20260424_122828`. `uvx pip-audit -r requirements-lock.txt` reports one residual Scrapy advisory (`PYSEC-2017-83` / `GHSA-h7wm-ph43-c39p` / `CVE-2017-14158`) with no fix version; the affected Scrapy file-download storage path (`FilesPipeline` / `S3FilesStore`) is not used in `src/` or tests.
   **Done when:** Dependabot shows no unresolved high or critical vulnerabilities for the default branch, dependency lock files reflect the approved updates, and relevant Python/Node test gates pass.

### P1

7. **Continue the mypy staged rollout through job runtime/report contract helpers.**
   Repo-wide mypy is still not complete: the broad audit reports `607` errors in `106` files. The next related non-release lane appears to be job runtime/report contract helpers, including `src/jobs/reporting_queues.py`, `src/jobs/pipeline_runtime_summary.py`, `src/jobs/common/contracts_task_state.py`, `src/jobs/common/contracts_source_reports.py`, `src/jobs/common/contracts_runtime.py`, and adjacent `src/source_registry.py` JSON normalization. Defer desktop updater and release-repeatability typing to a separate release-sensitive lane.
   **Done when:** the next cohesive lane is added to `mypy.ini`, focused tests for that lane remain green, and the broad audit count is re-recorded.

8. **Raise coverage in the weakest validated modules.**
   Prioritize `src/source_sync_crypto.py` (`52%`), `src/source_discovery/stage_control.py` (`51%`), `src/source_discovery/probe.py` (`65%`), and `src/source_discovery/url_patches.py` (`71%`).
   **Done when:** each target module has a named test addition and reaches an agreed post-baseline coverage threshold.

9. **Reduce JS hygiene noise before the next broad frontend refactor.**
   Fix or justify the `137` ESLint warnings and `20` `knip` unused exports, starting with the small number of production-file warnings before mass test-import cleanup.
   **Done when:** production-file ESLint warnings are eliminated and the unused-export list is either reduced or documented with explicit keep-alive reasons.

10. **Add static security scanning to CI.**
   Current workflows cover tests, lint, and release packaging, but not Python dependency/security scanning.
   **Done when:** CI runs at least one Python security/dependency scan (`bandit`, `pip-audit`, or equivalent) and documents failure ownership.

11. **Evaluate a complexity gate after the first typing and hygiene pass.**
   Complexity enforcement is worthwhile, but it should not be added before the current typing and warning debt is under control.
   **Done when:** the repo adopts a complexity ceiling with an explicit allowlist or baseline strategy instead of freezing current hotspots.

### P2

12. **Add real CI status badges to `README.md`.**
   The README has product badges today, but no workflow status badges.
   **Done when:** README shows current workflow status badges for the maintained CI lanes.

13. **Evaluate structured logging for support and ops diagnostics.**
   The repo already has strong observability hooks; structured logs would make support bundles and smoke artifacts easier to consume programmatically.
   **Done when:** one agreed logging surface adopts a structured format and demonstrates clear improvement over current ad hoc strings.

## Corrections to the Source Analysis

- `CONTRIBUTING.md` exists and should not be treated as missing.
- `.github/ISSUE_TEMPLATE/` exists and currently includes `bug_report.md` and `feature_request.md`.
- README has static product badges, but not CI status badges.
- `TODO` / `FIXME` / `HACK` count in `src/` plus `frontend/` is currently `0`, not `3`.
- `python -m vulture` does **not** work in the active interpreter, but the repo's pre-commit flow manages vulture separately; this is not the same as a broken repo gate.
- The previous `data/source-approval-state.json` newline-only churn was real, but it is now fixed at the shared writer level rather than hidden from the local checks.
- The type-safety claim still needs nuance: repo-wide mypy debt is large, but the enforced mypy scope now includes the admin bridge composition-root milestone and remains green.
- The original 1-10 score table and overall `7.5/10` rating were not retained here because they are subjective and partially stale relative to the current repo state.

## Not Locally Validated

These claims were not confirmed from checked-in repo state alone and should not drive immediate work without revalidation:

- GitHub labels such as `good first issue`
- External OSS discoverability or contributor conversion
- Exact remote vulnerability dashboard details outside the push-time Dependabot summary
- Any public reputation-style scoring that depends on live GitHub metadata rather than the repository contents

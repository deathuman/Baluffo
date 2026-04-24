# Repository Health Action Tracker

> - **Status:** Active
> - **Use this when:** reviewing repository health, prioritizing maintenance work, or correcting external repo audits
> - **Canonical for:** validated repo-health findings and immediate improvement priorities
> - **Not canonical for:** architecture ownership, contracts, or release procedure
> - **Then inspect:** [`testing.md`](testing.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`RELEASE.md`](RELEASE.md)
> - **Last updated:** 2026-04-24

This page converts an external repository analysis into a repo-native action tracker. The source analysis was reviewed against the current repository state at `f722957`, and only validated claims are carried forward into strengths, gaps, and next steps.

## Validation Snapshot

| Metric | Current validated value |
|--------|-------------------------|
| Python files | `313` |
| Frontend JS files | `184` |
| Top-level HTML entry points | `4` (`admin.html`, `index.html`, `jobs.html`, `saved.html`) |
| Python test files | `97` |
| Coverage lane | `1606 passed, 74 deselected`, total coverage `75%` |
| Broad type-check run | `python -m mypy src` -> `796 errors in 119 files (checked 312 source files)` |
| Enforced type-check gate | `python -m mypy --config-file mypy.ini` passes on the staged ten-file scope (`src/python_version_guard.py`, `src/pipeline_io.py`, `src/bridge/report_normalizer.py`, `src/bridge/api.py`, `src/bridge/admin_registry_api.py`, `src/bridge/admin_task_runtime.py`, `src/bridge/ops_live_payload.py`, `src/admin_bridge.py`, `src/bridge/admin_entrypoint_services.py`, `src/bridge/admin_entrypoint_runtime.py`) |
| ESLint | `137 warnings, 0 errors` |
| `knip` | `20` unused JS exports |
| Python lock file | `requirements-lock.txt` present |
| Node lock file | `package-lock.json` present |

## Confirmed Strengths Worth Protecting

- **Docs/wiki structure:** [`INDEX.md`](INDEX.md), [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) form a clear routing stack and are actively maintained.
- **Thin compatibility-surface discipline:** the repo consistently protects stable roots and shims with explicit contract tests and routing docs instead of letting refactors collapse back into monoliths.
- **Packaging and updater rehearsals:** packaged smoke, updater, sync rehearsal, orphan reclaim, and browser-job flows are covered by dedicated release-oriented verification lanes.
- **Startup and performance instrumentation:** startup probes, timing lanes, and discovery/perf sanity scripts are real maintained systems, not placeholder docs.

## Confirmed Gaps Worth Acting On

### P0

1. **Completed: type the `admin_bridge` composition root and its immediate helper boundary.**
   The enforced mypy scope now includes `src/admin_bridge.py`, `src/bridge/admin_entrypoint_services.py`, and `src/bridge/admin_entrypoint_runtime.py` alongside the prior seven-file bridge/admin leaf scope. The surrounding service/runtime helper aliases now expose typed facade protocols instead of leaking `Any` into the composition root, and the broad audit dropped from `835` to `796` errors.
   **Done when:** complete.

2. **Completed: add a Python dependency lock strategy for reproducible builds.**
   `requirements-lock.txt` is now the canonical Python lock artifact, and CI/release install surfaces consume it instead of floating `requirements.txt`.
   **Done when:** complete.

3. **Completed: stop generated-file newline churn in `data/source-approval-state.json`.**
   `save_json_atomic` now writes newline-terminated JSON, and targeted regression coverage protects the writer behavior used by the approval-state file.
   **Done when:** complete.

### P1

4. **Continue the mypy staged rollout through bridge live-payload and report JSON helpers.**
   The P0 `admin_bridge` composition-root milestone is complete, but the broad audit still reports `796` errors in `119` files. The most related next lane is the bridge/admin runtime surface that feeds task status, ops health, and report summaries: `src/bridge/discovery_service.py`, `src/bridge/ops_task_discovery_live.py`, `src/bridge/ops_task_fetch_live.py`, `src/bridge/routes/post_routes_admin.py`, `src/bridge/sync_service.py`, `src/bridge/ops_health.py`, `src/shared/live_task.py`, and `src/fetcher_metrics.py`. This lane should focus on reusable JSON-shape narrowing and callback protocol fixes rather than one-off casts.
   **Done when:** a cohesive bridge live-payload/report helper subset is added to the enforced mypy scope without regressing the ten-file gate, `tests/admin/` and the live-payload/ops-health tests remain green, and the broad audit drops by a meaningful recorded amount.

5. **Raise coverage in the weakest validated modules.**
   Prioritize `src/source_sync_crypto.py` (`52%`), `src/source_discovery/stage_control.py` (`51%`), `src/source_discovery/probe.py` (`65%`), and `src/source_discovery/url_patches.py` (`71%`).
   **Done when:** each target module has a named test addition and reaches an agreed post-baseline coverage threshold.

6. **Reduce JS hygiene noise before the next broad frontend refactor.**
   Fix or justify the `137` ESLint warnings and `20` `knip` unused exports, starting with the small number of production-file warnings before mass test-import cleanup.
   **Done when:** production-file ESLint warnings are eliminated and the unused-export list is either reduced or documented with explicit keep-alive reasons.

7. **Add static security scanning to CI.**
   Current workflows cover tests, lint, and release packaging, but not Python dependency/security scanning.
   **Done when:** CI runs at least one Python security/dependency scan (`bandit`, `pip-audit`, or equivalent) and documents failure ownership.

8. **Evaluate a complexity gate after the first typing and hygiene pass.**
   Complexity enforcement is worthwhile, but it should not be added before the current typing and warning debt is under control.
   **Done when:** the repo adopts a complexity ceiling with an explicit allowlist or baseline strategy instead of freezing current hotspots.

### P2

9. **Add real CI status badges to `README.md`.**
   The README has product badges today, but no workflow status badges.
   **Done when:** README shows current workflow status badges for the maintained CI lanes.

10. **Evaluate structured logging for support and ops diagnostics.**
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
- Remote vulnerability dashboard state outside the checked-in workflows and config
- Any public reputation-style scoring that depends on live GitHub metadata rather than the repository contents

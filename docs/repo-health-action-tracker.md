# Repository Health Action Tracker

> - **Status:** Active
> - **Use this when:** reviewing repository health, prioritizing maintenance work, or correcting external repo audits
> - **Canonical for:** validated repo-health findings and immediate improvement priorities
> - **Not canonical for:** architecture ownership, contracts, or release procedure
> - **Then inspect:** [`testing.md`](testing.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`RELEASE.md`](RELEASE.md)
> - **Last updated:** 2026-04-24

This page converts an external repository analysis into a repo-native action tracker. The source analysis was reviewed against the current repository state, and only validated claims are carried forward into strengths, gaps, and next steps.

Completed items are archived in [`archive/history/repo-health-completed-tasks.md`](archive/history/repo-health-completed-tasks.md) so this page stays focused on active repository-health work.

## Validation Snapshot

| Metric | Current validated value |
|--------|-------------------------|
| Python files | `313` |
| Frontend JS files | `184` |
| Top-level HTML entry points | `4` (`admin.html`, `index.html`, `jobs.html`, `saved.html`) |
| Python test files | `97` |
| Coverage lane | `1606 passed, 74 deselected`, total coverage `75%` |
| Broad type-check run | `python -m mypy src` -> `480 errors in 99 files (checked 312 source files)` |
| Enforced type-check gate | `python -m mypy --config-file mypy.ini` passes on the staged bridge/admin, source-discovery, audit, and jobs contracts/runtime/loader/plugin scope. |
| ESLint | `137 warnings, 0 errors` |
| `knip` | `20` unused JS exports |
| Python lock file | `requirements-lock.txt` present |
| Node lock file | `package-lock.json` present |
| Dependabot alert signal | GitHub push reported `2` high vulnerabilities on the default branch after the first Scrapy remediation; local Scrapy remediation now targets latest released `Scrapy==2.15.0` and the latest `scrapy-playwright` baseline `>=0.0.46`, and exact alert closure still needs Dependabot dashboard validation after push |

## Confirmed Strengths Worth Protecting

- **Docs/wiki structure:** [`INDEX.md`](INDEX.md), [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) form a clear routing stack and are actively maintained.
- **Thin compatibility-surface discipline:** the repo consistently protects stable roots and shims with explicit contract tests and routing docs instead of letting refactors collapse back into monoliths.
- **Packaging and updater rehearsals:** packaged smoke, updater, sync rehearsal, orphan reclaim, and browser-job flows are covered by dedicated release-oriented verification lanes.
- **Startup and performance instrumentation:** startup probes, timing lanes, and discovery/perf sanity scripts are real maintained systems, not placeholder docs.

## Confirmed Gaps Worth Acting On

### P0

1. **Next staged mypy milestone: type the remaining provider/plugin helper cluster.**
   The jobs adapter/plugin pass landed in two steps. Phase 1 corrected the plugin protocol, normalized taxonomy coercion, and made the provider/social registration seam checkable. Phase 2 normalized the social and `scrapy_static` adapter payload boundaries. That widened the enforced gate from `24` files to `30` files and dropped the broad audit from `557 errors in 105 files` to `480 errors in 99 files`.
   The next coherent jobs target is the remaining provider/plugin helper surface rather than the already-green adapter registration seam: `src/jobs/adapters/plugins/provider_api/json_feed.py`, `src/jobs/adapters/plugins/provider_api/html_board.py`, `src/jobs/adapters/plugins/provider_api/greenhouse_runner.py`, and the direct adapter-helper fallout they still feed (`src/jobs/adapters/social_parsers.py` and any immediate parser/helper seams exposed by that pass).
   **Done when:** the enforced mypy scope expands beyond the current thirty-file gate to cover the provider/plugin helper cluster, and those modules stop leaking `Any` through plugin builders, provider helper parsing, and direct adapter helper fallthrough paths.

2. **In progress: resolve GitHub Dependabot high-severity vulnerabilities.**
   GitHub reported `6` high vulnerabilities on the default branch before the first Scrapy remediation, then `2` high vulnerabilities after `Scrapy==2.14.2` landed. The local Scrapy remediation now updates the direct dependency to latest released `Scrapy==2.15.0`, raises the `scrapy-playwright` source requirement floor to the latest published `>=0.0.46`, and regenerates `requirements-lock.txt`; if Dependabot still requires `>2.15.0`, the remaining alerts are upstream-blocked until a newer Scrapy release exists. The Scrapy-adjacent lock entries (`scrapy-playwright`, `twisted`, `cryptography`, `pyopenssl`, `lxml`, `parsel`, `w3lib`, and `queuelib`) remained stable, and `brotli` is not present in the lock. Validation so far: `python -c "import scrapy, scrapy_playwright; print(scrapy.__version__); print(scrapy_playwright.__version__)"` -> `2.15.0` / `0.0.46`, `python -m pip check` passed, focused Scrapy/runtime tests passed (`187 passed`), `cmd /c npm run test:refactor:changed` passed, `cmd /c npm run lint:precommit:changed` passed, and `python scripts/orchestrator.py build --force` passed with run `20260424_133557`. `uvx pip-audit -r requirements-lock.txt` reports one residual Scrapy advisory (`PYSEC-2017-83` / `GHSA-h7wm-ph43-c39p` / `CVE-2017-14158`) with no fix version; the affected Scrapy file-download storage path (`FilesPipeline` / `S3FilesStore`) is not used in `src/` or tests.
   **Done when:** Dependabot shows no unresolved high or critical vulnerabilities for the default branch, dependency lock files reflect the approved updates, and relevant Python/Node test gates pass.

3. **Completed: add a Python dependency lock strategy for reproducible builds.**
   `requirements-lock.txt` is now the canonical Python lock artifact, and CI/release install surfaces consume it instead of floating `requirements.txt`.
   **Done when:** complete.

4. **Completed: stop generated-file newline churn in `data/source-approval-state.json`.**
   `save_json_atomic` now writes newline-terminated JSON, and targeted regression coverage protects the writer behavior used by the approval-state file.
   **Done when:** complete.

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
- The type-safety claim still needs nuance: repo-wide mypy debt is still large, but the enforced mypy scope now covers the `admin_bridge` composition root, its immediate helper boundary, the bridge live payload pair, and the first jobs contracts/runtime/loader seam, and it remains green.
- The original 1-10 score table and overall `7.5/10` rating were not retained here because they are subjective and partially stale relative to the current repo state.

## Not Locally Validated

These claims were not confirmed from checked-in repo state alone and should not drive immediate work without revalidation:

- GitHub labels such as `good first issue`
- External OSS discoverability or contributor conversion
- Exact remote vulnerability dashboard details outside the push-time Dependabot summary
- Any public reputation-style scoring that depends on live GitHub metadata rather than the repository contents

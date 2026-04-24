# Repository Health Action Tracker

> - **Status:** Active
> - **Use this when:** reviewing repository health, prioritizing maintenance work, or correcting external repo audits
> - **Canonical for:** validated repo-health findings and immediate improvement priorities
> - **Not canonical for:** architecture ownership, contracts, or release procedure
> - **Then inspect:** [`testing.md`](testing.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`RELEASE.md`](RELEASE.md)
> - **Last updated:** 2026-04-25

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
| Broad type-check run | `python -m mypy src` -> `0 errors in 313 source files` |
| Enforced type-check gate | `python -m mypy --config-file mypy.ini` covers the full `src/` tree and passes. |
| ESLint | `0 warnings, 0 errors` |
| `knip` | `0` unused JS exports |
| Python lock file | `requirements-lock.txt` present |
| Node lock file | `package-lock.json` present |

## Confirmed Strengths Worth Protecting

- **Docs/wiki structure:** [`INDEX.md`](INDEX.md), [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) form a clear routing stack and are actively maintained.
- **Thin compatibility-surface discipline:** the repo consistently protects stable roots and shims with explicit contract tests and routing docs instead of letting refactors collapse back into monoliths.
- **Packaging and updater rehearsals:** packaged smoke, updater, sync rehearsal, orphan reclaim, and browser-job flows are covered by dedicated release-oriented verification lanes.
- **Startup and performance instrumentation:** startup probes, timing lanes, and discovery/perf sanity scripts are real maintained systems, not placeholder docs.

## Confirmed Gaps Worth Acting On

### P0

No active P0 repository-health item is open. The previous broad mypy sweep is complete and archived.

### P1

8. **Raise coverage in the weakest validated modules.**
   Prioritize `src/source_sync_crypto.py` (`52%`), `src/source_discovery/stage_control.py` (`51%`), `src/source_discovery/probe.py` (`65%`), and `src/source_discovery/url_patches.py` (`71%`).
   **Done when:** each target module has a named test addition and reaches an agreed post-baseline coverage threshold.

9. **Add static security scanning to CI.**
   Current workflows cover tests, lint, and release packaging, but not Python dependency/security scanning.
   **Done when:** CI runs at least one Python security/dependency scan (`bandit`, `pip-audit`, or equivalent) and documents failure ownership.

10. **Evaluate a complexity gate after the first typing and hygiene pass.**
   Complexity enforcement is worthwhile, but it should not be added before the current typing and warning debt is under control.
   **Done when:** the repo adopts a complexity ceiling with an explicit allowlist or baseline strategy instead of freezing current hotspots.

### P2

11. **Add real CI status badges to `README.md`.**
   The README has product badges today, but no workflow status badges.
   **Done when:** README shows current workflow status badges for the maintained CI lanes.

12. **Evaluate structured logging for support and ops diagnostics.**
   The repo already has strong observability hooks; structured logs would make support bundles and smoke artifacts easier to consume programmatically.
   **Done when:** one agreed logging surface adopts a structured format and demonstrates clear improvement over current ad hoc strings.

## Corrections to the Source Analysis

- `CONTRIBUTING.md` exists and should not be treated as missing.
- `.github/ISSUE_TEMPLATE/` exists and currently includes `bug_report.md` and `feature_request.md`.
- README has static product badges, but not CI status badges.
- `TODO` / `FIXME` / `HACK` count in `src/` plus `frontend/` is currently `0`, not `3`.
- `python -m vulture` now works in the active interpreter after local environment repair; the repo's pre-commit flow still manages its own vulture hook environment separately.
- The previous `data/source-approval-state.json` newline-only churn was real, but it is now fixed at the shared writer level rather than hidden from the local checks.
- The type-safety claim is now materially different from the source analysis: broad `python -m mypy src` is green, and the enforced mypy gate covers the full `src/` tree.
- The original 1-10 score table and overall `7.5/10` rating were not retained here because they are subjective and partially stale relative to the current repo state.

## Not Locally Validated

These claims were not confirmed from checked-in repo state alone and should not drive immediate work without revalidation:

- GitHub labels such as `good first issue`
- External OSS discoverability or contributor conversion
- Exact remote vulnerability dashboard details outside the push-time Dependabot summary
- Any public reputation-style scoring that depends on live GitHub metadata rather than the repository contents

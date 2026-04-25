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
| Coverage lane | `1634 passed, 74 deselected`, total coverage `75%` |
| Broad type-check run | `python -m mypy src` -> `0 errors in 313 source files` |
| Enforced type-check gate | `python -m mypy --config-file mypy.ini` covers the full `src/` tree and passes. |
| ESLint | `0 warnings, 0 errors` |
| `knip` | `0` unused JS exports |
| Python import sorting / unused import check | Enforced by `ruff.toml` (`F` and `I` selected) through `npm run lint:precommit:ci`; `python -m ruff check --select I,F401 src tests` also passes |
| Python source complexity | Enforced by `scripts/check_complexity_baseline.py` through `npm run lint:precommit:ci`; Ruff `C901` uses threshold `10` against the checked-in `src/` baseline |
| Static security scanners | `bandit`, `pip-audit`, `radon`, and `xenon` are not installed locally and are not wired in CI/pre-commit |
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

P1-8 is complete and archived; the remaining items below are the active P1 gaps.

9. **Add static security scanning to CI.**
   Current workflows cover tests, lint, and release packaging, but not Python dependency/security scanning.
   `bandit` and `pip-audit` are not currently installed or wired. Adding them requires explicit dependency approval and should include an allowlist/baseline policy for known non-actionable findings.
   **Done when:** CI runs at least one Python security/dependency scan (`bandit`, `pip-audit`, or equivalent) and documents failure ownership.

### P2

P2-11, P2-12, and P2-14 are complete and archived; the remaining item below is the active P2 gap.

13. **Raise coverage in the remaining weak runtime/security modules.**
   The validated coverage lane still reports `source_sync_runtime.py` at `76%` and `source_discovery/web_search_candidates.py` at `75%`. The previously cited `source_discovery/probe.py` is no longer a weak module; it reports `93%`.
   **Done when:** the remaining named modules reach the agreed module-level target or have a documented reason to stay below it.

## Corrections to the Source Analysis

- `CONTRIBUTING.md` exists and should not be treated as missing.
- `.github/ISSUE_TEMPLATE/` exists and currently includes `bug_report.md` and `feature_request.md`.
- README has static product badges, but not CI status badges.
- `TODO` / `FIXME` / `HACK` count in `src/` plus `frontend/` is currently `0`, not `3`.
- `python -m vulture` now works in the active interpreter after local environment repair; the repo's pre-commit flow still manages its own vulture hook environment separately.
- The previous `data/source-approval-state.json` newline-only churn was real, but it is now fixed at the shared writer level rather than hidden from the local checks.
- The type-safety claim is now materially different from the source analysis: broad `python -m mypy src` is green, and the enforced mypy gate covers the full `src/` tree.
- The submitted "1 ESLint error" claim is stale; current validation found `2` `no-extra-boolean-cast` errors in `frontend/admin/domain/sources.js`.
- The submitted weak-coverage list is partially stale; `source_discovery/probe.py` now reports `93%`, while `source_sync_runtime.py` and `source_discovery/web_search_candidates.py` remain below `80%`.
- The submitted unused-import gate claim is stale; Ruff's default `F` rules cover `F401`, and import sorting (`I`) is selected in `ruff.toml` and enforced by the pre-commit/CI lane.
- The submitted complexity-gate gap is now closed with a Ruff `C901` baseline gate for `src/`, avoiding new complexity-specific dependencies while preventing new or worsened source hotspots.
- The original 1-10 score table and overall `7.5/10` rating were not retained here because they are subjective and partially stale relative to the current repo state.

## Not Locally Validated

These claims were not confirmed from checked-in repo state alone and should not drive immediate work without revalidation:

- GitHub labels such as `good first issue`
- External OSS discoverability or contributor conversion
- Exact remote vulnerability dashboard details outside the push-time Dependabot summary
- Any public reputation-style scoring that depends on live GitHub metadata rather than the repository contents

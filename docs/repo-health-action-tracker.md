# Repository Health Action Tracker

> - **Status:** Active
> - **Use this when:** reviewing repository health, prioritizing maintenance work, or correcting external repo audits
> - **Canonical for:** validated repo-health findings and immediate improvement priorities
> - **Not canonical for:** architecture ownership, contracts, or release procedure
> - **Then inspect:** [`testing.md`](testing.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`RELEASE.md`](RELEASE.md)
> - **Last updated:** 2026-04-25

This page tracks active repository-health work after the broad type, lint, complexity, security, guardrail-migration, and helper-indirection passes. Completed items are archived in [`archive/history/repo-health-completed-tasks.md`](archive/history/repo-health-completed-tasks.md); this active page now focuses on repeated inline test setup plus the few remaining general health gaps.

## Current Validation Snapshot

| Metric | Current validated value |
|--------|-------------------------|
| Source Python files | `315` under `src/` |
| Python test files | `99` |
| Frontend JS files | `183` under `frontend/` |
| Frontend unit test files | `58` under `tests/frontend/unit/` |
| Top-level HTML entry points | `4` (`admin.html`, `index.html`, `jobs.html`, `saved.html`) |
| Repository policy guardrails | `npm run lint:repo-guardrails` via `tools/repo_health/repo_guardrails.py` |
| Named large inline-data / boilerplate targets | `6,399` lines across `4` current files |
| Fixture files | `50` files under `tests/fixtures/` |
| Frontend unit manifest tooling | `scripts/sync_frontend_unit_manifest.mjs` plus `tests/frontend/unit/all.test.mjs` are still active; structural cleanup policy is no longer collected as a frontend unit test |
| Static security scanners | `pip-audit` is wired through `npm run security:python` and the CI lint workflow; `bandit`, `radon`, and `xenon` are not wired in `package.json`, `.pre-commit-config.yaml`, requirements files, scripts, or CI |

The previous full-suite validation remains the last broad quality snapshot: coverage lane `1634 passed, 74 deselected`, total coverage `75%`; broad `python -m mypy src` green; enforced `mypy.ini` gate green; ESLint green; `knip` green; Ruff import and unused-import checks enforced by `ruff.toml`; source complexity enforced by `scripts/check_complexity_baseline.py`.

## Confirmed Strengths Worth Protecting

- **Docs/wiki structure:** [`INDEX.md`](INDEX.md), [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) form a clear routing stack and are actively maintained.
- **Thin compatibility-surface discipline:** stable roots and shims are protected by explicit contract tests and routing docs.
- **Packaging and updater rehearsals:** packaged smoke, updater, sync rehearsal, orphan reclaim, and browser-job flows are covered by dedicated release-oriented verification lanes.
- **Startup and performance instrumentation:** startup probes, timing lanes, and discovery/perf sanity scripts are maintained systems, not placeholder docs.
- **Behavioral test coverage:** the expensive tests mostly protect real bridge, package, pipeline, discovery, and frontend runtime behavior. The remaining bloat problem is concentrated in repeated inline setup and large payloads.

## Active P1 Plan

### P1-D. Extract repeated inline data and setup from the largest behavioral tests

The current largest high-value bloat targets are:

| File | Lines | Debloat focus |
|------|-------|---------------|
| `tests/test_jobs_fetcher_pipeline.py` | `1,966` | Social configs, Reddit/Mastodon payloads, repeated pipeline setup |
| `tests/desktop_app/test_launcher_orchestration.py` | `1,609` | Repeated `DesktopRuntimeConfig(...)`, session dictionaries, broad autouse patching |
| `tests/source_discovery/test_run_discovery_flow.py` | `1,395` | Discovery configs, stage toggles, repeated runtime setup |
| `tests/frontend/unit/admin-fetcher-controller.test.mjs` | `1,429` | Repeated state, refs, calls, toasts, logs, and controller construction |

**Implementation plan:**

1. Move large JSON-like social payloads into `tests/fixtures/payloads/` and load them with small fixture helpers.
2. Add a minimal desktop runtime config helper in `tests/desktop_app/_helpers.py` instead of repeating full config objects.
3. Move discovery stage-control fixtures into `tests/fixtures/discovery/` only when the fixture content is genuinely reused or obscures the assertion.
4. Add frontend factory helpers for admin fetcher controller tests in `tests/frontend/unit/helpers/admin-controller-test-helpers.mjs`.
5. Audit `tests/fixtures/` for orphaned files before adding new payload fixtures.
   A lightweight `repo_guardrails.py` check can require each fixture file to be referenced by at least one test or helper via a path/name string. Keep an allowlist for intentionally external/manual fixtures if any exist.
6. Remove the autouse launcher patch only where tests can name the required patch set without making the file harder to audit.

**Done when:** the named files are materially smaller, assertions remain local and obvious, extracted fixtures hold data rather than reimplementing production behavior, and fixture files do not accumulate without references.

## Active P2 Plan

### P2-A. Raise coverage in the remaining weak runtime/security modules

The last coverage snapshot still named `source_sync_runtime.py` and `source_discovery/web_search_candidates.py` as below `80%`; `source_discovery/probe.py` was no longer weak at `93%`.

**Done when:** the remaining named modules reach the agreed module-level target or have a documented reason to stay below it.

### P2-B. Simplify frontend unit test discovery and merge small adjacent files

The submitted merge list is mostly plausible, but direct Node glob execution should be verified on the active Node version before deleting manifest tooling. The current package script still runs `npm run check:test-manifest && node --test --test-reporter=dot tests/frontend/unit/all.test.mjs`.

**Implementation plan:**

1. Verify a direct Node test discovery command works on the supported runtime before removing the generated manifest.
2. If direct discovery is stable, delete `scripts/sync_frontend_unit_manifest.mjs`, `tests/frontend/unit/all.test.mjs`, and `tests/frontend/unit/manifest-contract.test.mjs` together.
3. Merge only tightly related small files where setup overlap is real:
   `admin-live-task*` with `admin-progress-ui`, `admin-render*`, `jobs-runtime-events/state`, and `saved-phase-time/timeline`.
4. Keep independent runtime-controller tests split when their fixture setup or failure messages become less clear after merging.

**Done when:** adding a frontend unit test no longer requires manifest regeneration, related tiny files are consolidated, and frontend unit failures still point at a focused behavioral area.

## Explicit Non-Goals For Test Debloat

- Do not delete packaged rehearsal tests under `tests/packaged_desktop/`; they are release guarantees.
- Do not weaken bridge route tests under `tests/bridge/test_routes_*.py`; simplify their helpers instead.
- Do not remove `tests/source_discovery/test_coverage_targets.py`; it is fast, behavioral, and still protects the recent coverage push.
- Do not replace production behavior with helper logic in tests. Extract payloads and setup, not parallel implementations.
- Do not add new Python or Node dependencies for this cleanup without explicit approval.

## Not Locally Validated

These claims should not drive immediate work without live revalidation:

- GitHub labels such as `good first issue`
- External OSS discoverability or contributor conversion
- Remote vulnerability dashboard state outside checked-in repo configuration
- Any public reputation-style scoring that depends on live GitHub metadata rather than repository contents

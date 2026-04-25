# Repository Health Action Tracker

> - **Status:** Active
> - **Use this when:** reviewing repository health, prioritizing maintenance work, or correcting external repo audits
> - **Canonical for:** validated repo-health findings and immediate improvement priorities
> - **Not canonical for:** architecture ownership, contracts, or release procedure
> - **Then inspect:** [`testing.md`](testing.md), [`../CONTRIBUTING.md`](../CONTRIBUTING.md), and [`RELEASE.md`](RELEASE.md)
> - **Last updated:** 2026-04-25

This page tracks active repository-health work after the broad type, lint, complexity, security, guardrail-migration, helper-indirection, and test-debloat passes. Completed items are archived in [`archive/history/repo-health-completed-tasks.md`](archive/history/repo-health-completed-tasks.md); this active page now focuses on the few remaining general health gaps.

## Current Validation Snapshot

| Metric | Current validated value |
|--------|-------------------------|
| Source Python files | `315` under `src/` |
| Python test files | `99` |
| Frontend JS files | `183` under `frontend/` |
| Frontend unit test files | `52` under `tests/frontend/unit/` |
| Top-level HTML entry points | `4` (`admin.html`, `index.html`, `jobs.html`, `saved.html`) |
| Repository policy guardrails | `npm run lint:repo-guardrails` via `tools/repo_health/repo_guardrails.py` |
| Fixture reference guardrail | `npm run lint:repo-guardrails` fails unreferenced `tests/fixtures/**` files unless explicitly allowlisted with a reason |
| Fixture files | `50` files under `tests/fixtures/` |
| Frontend unit discovery | `npm run test:frontend:unit` runs Node directly against `tests/frontend/unit/*.test.mjs`; generated frontend unit aggregators are blocked by repo guardrails |
| Static security scanners | `pip-audit` is wired through `npm run security:python` and the CI lint workflow; `bandit`, `radon`, and `xenon` are not wired in `package.json`, `.pre-commit-config.yaml`, requirements files, scripts, or CI |

The previous full-suite validation remains the last broad quality snapshot: coverage lane `1634 passed, 74 deselected`, total coverage `75%`; broad `python -m mypy src` green; enforced `mypy.ini` gate green; ESLint green; `knip` green; Ruff import and unused-import checks enforced by `ruff.toml`; source complexity enforced by `scripts/check_complexity_baseline.py`.

## Confirmed Strengths Worth Protecting

- **Docs/wiki structure:** [`INDEX.md`](INDEX.md), [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) form a clear routing stack and are actively maintained.
- **Thin compatibility-surface discipline:** stable roots and shims are protected by explicit contract tests and routing docs.
- **Packaging and updater rehearsals:** packaged smoke, updater, sync rehearsal, orphan reclaim, and browser-job flows are covered by dedicated release-oriented verification lanes.
- **Startup and performance instrumentation:** startup probes, timing lanes, and discovery/perf sanity scripts are maintained systems, not placeholder docs.
- **Behavioral test coverage:** the expensive tests mostly protect real bridge, package, pipeline, discovery, and frontend runtime behavior. The latest debloat pass moved repeated payload/setup data into narrow test helpers without weakening those behavioral lanes.

## Active P1 Plan

No active P1 repository-health items remain after P1-D. Keep new repo-health findings triaged here before promoting them into P1/P2 work.

## Active P2 Plan

### P2-A. Raise coverage in the remaining weak runtime/security modules

The last coverage snapshot still named `source_sync_runtime.py` and `source_discovery/web_search_candidates.py` as below `80%`; `source_discovery/probe.py` was no longer weak at `93%`.

**Done when:** the remaining named modules reach the agreed module-level target or have a documented reason to stay below it.

No other active P2 repository-health items remain after P2-B. Keep new P2 findings triaged here before promoting them into implementation work.

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

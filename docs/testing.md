# Testing

> **AI usage**
> - **Use this when:** choosing the narrowest verification step, finding relevant test files, or understanding fixture layout
> - **Canonical for:** test commands, targeted test routing, and fixture references
> - **Not canonical for:** runtime architecture or data contracts
> - **Then inspect:** the nearest `tests/` module for the subsystem you changed

This document owns the verification matrix for Baluffo. Keep build, test, and fixture guidance here instead of repeating command tables in routing docs.

## Python tests (pytest)

Run the balanced developer Python lane:

```bash
npm run test:py
```

This wrapper now uses the repo-local pytest temp root under `.tmp/pytest` (with `--basetemp=.tmp/pytest/basetemp`) so Windows temp-root ACL issues do not interfere with the suite. It excludes `slow`, `packaging`, and `release` tests so the default local loop stays focused on day-to-day development.

Run the full Python suite when you need release-level confidence:

```bash
npm run test:py:extended
```

**Direct local filtering:** To reproduce the developer lane directly from pytest:

```bash
python -m pytest tests -q -m "not slow and not packaging and not release" --color=no
```

Slow, packaging, and release tests stay in the extended lane. The timing lane still runs the full suite so performance regressions stay visible.

Run a quick timing sanity check (prints the slowest tests at the end):

```bash
npm run test:py:timing
```

Notes:
- `--durations=25` prints the 25 slowest tests.
- `--durations-min=0.2` only prints tests slower than 0.2s (adjust as needed).

If you want a full per-test breakdown once (noisy):

```bash
python -m pytest tests -q --durations=0 --color=no
```

## Test layout and fixtures

The Python suite is fully pytest (no `unittest.TestCase`). All tests are plain `def test_*` functions.

**Targeted runs:**

| Goal | Command |
|------|---------|
| Developer lane | `npm run test:py` |
| Full suite / release lane | `npm run test:py:extended` |
| Local pre-commit gate | `npm run lint:precommit:changed` |
| Full pre-commit sweep | `npm run lint:precommit:all` |
| CI pre-commit sweep | `npm run lint:precommit:ci` |
| Build ship bundle | `npm run build:ship-bundle` |
| Build portable EXE | `npm run build:portable-exe` |
| Ship bundle leaf builder | `python scripts/build_ship_bundle.py --bundle-version <version>` |
| Portable EXE leaf builder | `python scripts/build_portable_exe.py --bundle-version <version>` |
| Packaged desktop smoke gate | `npm run test:frontend:packaged` |
| Jobs-page no-Admin packaged smoke gate | `npm run test:frontend:packaged:jobs-pipeline` |
| Orchestrated packaged smoke gate | `npm run test:frontend:packaged:orchestrated` |
| Rebuild-backed packaged diagnostic | `npm run probe:desktop:startup:cold` |
| One file | `python -m pytest tests/<path/to/test_*.py> -q` |
| Admin bridge | `python -m pytest tests/admin/ -q` |
| Match developer lane directly | `python -m pytest tests -q -m "not slow and not packaging and not release" --color=no` |

**Shared fixtures (where they are defined):**

| Fixture | Location |
|---------|----------|
| `repo_root`, `codex_tmp_root`, `make_test_root`, `source_sync_test_root` | `tests/conftest.py` |
| `admin_bridge_entrypoint_root` | `tests/admin/conftest.py` |
| `workspace_tmpdir(prefix)` (context manager) | `tests/helpers/temp_paths.py` |

**Temp directory note (Windows sandbox):**

- Prefer repo-local temp fixtures such as `workspace_tmpdir(...)` and `admin_bridge_entrypoint_root` for new tests that write runtime state.
- In this environment, direct pytest temp-root creation under `%LOCALAPPDATA%\\Temp` can hit Windows permission errors during setup/cleanup.
- If a narrow bridge test run fails before assertions with tmpdir/tempfile ACL errors, rerun it with a repo-local `--basetemp` or the existing repo-local tempdir shim rather than treating it as a product regression.

## Packaged artifact ownership

- Direct packaging commands own `dist/` outputs:
  - `npm run build:portable-exe`
  - `python scripts/build_portable_exe.py`
  - `npm run test:frontend:packaged*`
- Orchestrated build and verify commands own `_out/runs/...` and `_out/latest/...`:
  - `npm run build`
  - `npm run verify`
  - `python scripts/orchestrator.py build`
  - `python scripts/orchestrator.py verify`
- Do not expect `build:portable-exe` to refresh `_out/latest`; that mirror only belongs to the orchestrator flow.

## Test ownership rules

- Real shard files must own real tests. Do not hide test functions inside giant imported `_cases.py` containers.
- Shared helpers should stay local to the test family and helper-only. Prefer `_helpers.py`, `conftest.py`, or a focused helper module over a broad test utility barrel.
- Before adding a new guard or smoke test, delete or merge any older test that already protects the same invariant.
- Prefer seam-patched unit checks for selection, normalization, and routing logic. Keep only one intentionally slow smoke test when full execution is the behavior under test.

## Jobs Pipeline Smoke Contract

- `npm run test:frontend:packaged:jobs-pipeline` is no longer just a “pipeline started” check.
- It must prove all of the following in the packaged desktop runtime:
  - the Jobs page renders and the pipeline button becomes busy,
  - the pipeline reports a real `runId`,
  - the tracked run reaches a terminal non-error state,
  - no backend `error` payload is surfaced after startup.
- This lane uses a smoke-only stub-success pipeline mode so it stays deterministic and bounded while still exercising the real `PipelineService` worker path.

## Release/build regression picks

Use the narrowest check that matches the risky path:

- Packaging or portable EXE changes: `python scripts/build_portable_exe.py`
- Bridge route wiring or task-launch signature changes: focused `tests/bridge/...` plus `tests/test_pipeline_execution.py` for worker-path coverage
- Admin task buttons, presets, or busy-state changes: focused frontend unit tests plus the nearest admin bridge payload test
- Contamination or location-quality regressions: targeted fetcher/unit checks around sanitization, canonicalization, or audit helpers

**Test-to-source map:**

| Area | Test path |
|------|-----------|
| Jobs pipeline / jobs_fetcher | `tests/test_jobs_fetcher.py` (integration shim), `tests/test_jobs_fetcher_google_sheets.py`, `tests/test_jobs_fetcher_parsing.py`, `tests/test_jobs_fetcher_providers.py`, `tests/jobs_static/`, `tests/test_jobs_fetcher_pipeline.py`, `tests/test_jobs_fetcher_quality.py` |
| Source discovery | `tests/source_discovery/` |
| Admin bridge (registry, runtime, static fallback, sync) | `tests/admin/test_admin_bridge_ops_*.py` |
| Desktop app / launcher | `tests/test_desktop_app.py` |
| Source sync | `tests/test_source_sync.py` |
| Local data store, backup, config, etc. | `tests/test_local_data_store.py`, `tests/test_desktop_app.py`, and the nearest focused `tests/test_*.py` module for the subsystem |

## Frontend smoke tests (Playwright)

Playwright smoke tests are run by `npm run test:smoke` / `npm run test`.

`playwright.config.js` starts a local web server using `python scripts/serve_static_site.py --directory .`.
Make sure `python` on your machine resolves to Python 3 (not Python 2), otherwise the web server will fail to start.

If needed for local development, set `PLAYWRIGHT_PYTHON=py` (or any Python 3 launcher) to override what Playwright uses.

Playwright artifacts are written under `.tmp/playwright/test-results`, so the repo root does not accumulate a `test-results/` directory.

## Test types in Baluffo

Baluffo uses multiple test layers:

- **Python tests (`pytest`)**: backend, bridge, pipeline, packaging-adjacent logic
- **Frontend unit tests (`node --test`)**: fast JavaScript unit coverage
- **Frontend smoke tests (`Playwright`)**: browser-level behavior checks
- **Full verification (`npm run verify`)**: broader end-to-end confidence, including the CI-parity pre-commit gate

## Coverage

Run Python tests with coverage:

```bash
npm run test:py:cov
```

Equivalent direct command:

```bash
python -m pytest tests -q -m "not slow" --cov=src --cov-report=term-missing --color=no
```

## Which command should I run?

- Small Python logic change: `npm run test:py`
- Before pushing to `main` or preparing a release: `npm run test:py:extended`
- Before merging a broad or risky backend change: `npm run test:py:extended` or `npm run verify`
- JavaScript/frontend unit change: `npm run test:unit`
- Browser or page-flow change: `npm run test:smoke`
- Broad or risky change: `npm run verify`

For the AI bootstrap and task-routing summary, see [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md).

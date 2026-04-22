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

This wrapper now uses the repo-local pytest temp root under `.tmp/pytest` (with `--basetemp=.tmp/pytest/basetemp`) and configures pytest cache under `.tmp/pytest/cache`. The repo disables pytest's cacheprovider by default because this Windows environment can leave unreadable `pytest-cache-files-*` temp directories behind. It excludes `slow`, `packaging`, and `release` tests so the default local loop stays focused on day-to-day development.

Run the full Python suite when you need release-level confidence:

```bash
npm run test:py:extended
```

**Direct local filtering:** To reproduce the developer lane directly from pytest:

```bash
python -m pytest tests -q -m "not slow and not packaging and not release" --color=no --basetemp=.tmp/pytest/basetemp
```

Slow, packaging, and release tests stay in the extended lane. The timing lane still runs the full suite so performance regressions stay visible.

Run a quick timing sanity check (prints the slowest tests at the end):

```bash
npm run test:py:timing
```

This timing lane also uses the repo-local pytest temp root under `.tmp/pytest`, so Windows temp-root ACL issues and pytest cache noise do not push perf triage into `%LOCALAPPDATA%\\Temp`.

Notes:
- `--durations=25` prints the 25 slowest tests.
- `--durations-min=0.2` only prints tests slower than 0.2s (adjust as needed).

If you want a full per-test breakdown once (noisy):

```bash
python -m pytest tests -q --durations=0 --color=no --basetemp=.tmp/pytest/basetemp
```

## Performance checks

For packaged startup timing architecture, ownership boundaries, and report semantics, see [startup-probe-architecture.md](startup-probe-architecture.md). Keep command guidance here and the internal startup-measurement design there.

Use the repo-native perf entrypoints before adding new benchmark tooling:

| Goal | Command | Output location |
|------|---------|-----------------|
| Slowest Python tests | `npm run perf:py:timing` | Console output only |
| Isolated discovery sanity benchmark | `npm run perf:discovery:benchmark` | `_out/perf-sanity-discovery/` |
| Packaged desktop cold startup probe | `npm run perf:startup:cold` | `.tmp/packaged-desktop-smoke/` and `data/packaged-desktop-smoke-report.json` |
| Packaged desktop warm startup probe | `npm run perf:startup:warm` | `.tmp/packaged-desktop-smoke/` and `data/packaged-desktop-smoke-report.json` |

Notes:
- Prefer repo-local artifact roots such as `.tmp/` and `_out/` for new perf workflows; avoid `%LOCALAPPDATA%\\Temp` for benchmark or runtime-state outputs in this Windows-first repo.
- `npm run perf:discovery:benchmark` is the default discovery perf entrypoint because it keeps artifacts under `_out/`; use `python scripts/benchmark_discovery_probe.py` separately when tuning discovery probe concurrency.
- Do not add `pytest-benchmark` or `py-spy` by default here. If dependency approval happens later, benchmark deterministic Python leaf logic first and keep desktop startup analysis on the existing startup-trace pipeline.

## Test layout and fixtures

The Python suite is fully pytest (no `unittest.TestCase`). All tests are plain `def test_*` functions.

**Targeted runs:**

| Goal | Command |
|------|---------|
| Developer lane | `npm run test:py` |
| Full suite / release lane | `npm run test:py:extended` |
| Release preflight | `npm run release:preflight` |
| Local pre-commit gate | `npm run lint:precommit:changed` |
| Full pre-commit sweep | `npm run lint:precommit:all` |
| CI pre-commit sweep | `npm run lint:precommit:ci` |
| Build ship bundle | `npm run build:ship-bundle` |
| Build portable EXE | `npm run build:portable-exe` |
| Ship bundle leaf builder | `python scripts/build_ship_bundle.py --bundle-version <version>` |
| Portable EXE leaf builder | `python scripts/build_portable_exe.py --bundle-version <version>` |
| Python perf timing | `npm run perf:py:timing` |
| Discovery perf sanity | `npm run perf:discovery:benchmark` |
| Packaged startup perf probe (cold/warm) | `npm run perf:startup:cold` / `npm run perf:startup:warm` |
| Packaged desktop smoke gate | `npm run test:frontend:packaged` |
| Packaged sync rehearsal | `npm run test:frontend:packaged:sync-rehearsal` |
| Packaged orphan reclaim rehearsal | `npm run test:frontend:packaged:orphan-reclaim-rehearsal` |
| Packaged browser job rehearsal | `npm run test:frontend:packaged:browser-job-rehearsal` |
| Jobs-page no-Admin packaged smoke gate | `npm run test:frontend:packaged:jobs-pipeline` |
| Packaged desktop updater rehearsal | `npm run test:frontend:packaged:update-rehearsal` |
| Orchestrated packaged smoke gate | `npm run test:frontend:packaged:orchestrated` |
| Rebuild-backed packaged diagnostic | `npm run probe:desktop:startup:cold` |
| One file | `python -m pytest tests/<path/to/test_*.py> -q` |
| Admin bridge | `python -m pytest tests/admin/ -q` |
| Match developer lane directly | `python -m pytest tests -q -m "not slow and not packaging and not release" --color=no --basetemp=.tmp/pytest/basetemp` |

Use `npm run release:preflight` when you are about to push a release commit, move a release tag, or publish release artifacts. It runs the pre-commit gate, the full Python lane, frontend unit tests, and the packaged desktop release lanes in canonical order.

**Shared fixtures (where they are defined):**

| Fixture | Location |
|---------|----------|
| `repo_root`, `codex_tmp_root`, `make_test_root`, `source_sync_test_root` | `tests/conftest.py` |
| `admin_bridge_entrypoint_root` | `tests/admin/conftest.py` |
| `workspace_tmpdir(prefix)` (context manager) | `tests/helpers/temp_paths.py` |

**Temp directory note (Windows sandbox):**

- Prefer repo-local temp fixtures such as `workspace_tmpdir(...)` and `admin_bridge_entrypoint_root` for new tests that write runtime state.
- In this environment, direct pytest temp-root creation under `%LOCALAPPDATA%\\Temp` can hit Windows permission errors during setup/cleanup.
- Keep pytest temp roots under `.tmp/pytest`; the repo disables pytest's cacheprovider by default so unreadable `pytest-cache-files-*` debris does not accumulate in the workspace.
- If a narrow bridge test run fails before assertions with tmpdir/tempfile ACL errors, rerun it with a repo-local `--basetemp` or the existing repo-local tempdir shim rather than treating it as a product regression.

## Packaged artifact ownership

- Direct packaging commands own `dist/` outputs:
  - `npm run build:portable-exe`
  - `python scripts/build_portable_exe.py`
- `npm run test:frontend:packaged*`
- `npm run test:frontend:packaged:sync-rehearsal`
- `npm run test:frontend:packaged:orphan-reclaim-rehearsal`
- `npm run test:frontend:packaged:browser-job-rehearsal`
- `npm run test:frontend:packaged:update-rehearsal`
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

## Desktop Updater Rehearsal Contract

- `npm run test:frontend:packaged:update-rehearsal` is the packaged `N -> N+1` updater gate for the portable desktop runtime.
- It must prove all of the following:
  - the packaged app can surface an available update and hand off install to `BaluffoUpdater.exe`,
  - the helper installs the target portable ZIP and relaunches the target runtime successfully,
  - the target runtime reaches desktop startup readiness and writes the post-install success marker,
  - seeded local profile data, saved jobs, notes, and attachments remain intact across the update.
- Use this lane for updater-helper, desktop session/handoff, release-manifest, and portable runtime mutation changes.

## Packaged Sync Rehearsal Contract

- `npm run test:frontend:packaged:sync-rehearsal` is the packaged GitHub sync portability gate for the portable desktop runtime.
- It must prove all of the following:
  - the shipped `packaging/github-app-sync-config.json` is present and not `keyDerivation: "machine"`,
  - the packaged runtime loads the embedded sync config successfully,
  - packaged GitHub App auth initializes against a local fake GitHub endpoint,
  - `/sync/test` reads a remote snapshot successfully without touching real GitHub.
- Use this lane for packaged sync config, source-sync auth, ship-bundle embedding, and release portability changes.

## Packaged Orphan Reclaim Rehearsal Contract

- `npm run test:frontend:packaged:orphan-reclaim-rehearsal` is the packaged stale-runtime recovery gate for the portable desktop runtime.
- It must prove all of the following:
  - stale packaged `site` and `bridge` children can be seeded with strong attribution in desktop session state,
  - the relaunched packaged app reuses the requested ports instead of silently retrying to fresh ports,
  - startup metrics emit `desktop_stale_runtime_reclaim_started`,
  - startup metrics emit `desktop_stale_runtime_reclaim_result` with `target=bridge` / `outcome=killed` and `target=site` / `outcome=killed`,
  - no `desktop_lock_reclaim_failed` event appears during relaunch.
- Use this lane for packaged desktop supervision, stale-session recovery, startup self-heal, and runtime-port collision fixes.

## Packaged Browser Job Rehearsal Contract

- `npm run test:frontend:packaged:browser-job-rehearsal` is the packaged Chromium supervision gate for the portable desktop runtime.
- It must prove all of the following:
  - the packaged runtime launches in managed Chromium app mode instead of degrading to default-browser or recovery mode,
  - startup metrics emit `desktop_browser_job_attached` and do not emit `desktop_browser_job_attach_failed`,
  - the rehearsal can prove a live browser proof PID from either the still-attached Chromium launcher PID or a live visible-window PID,
  - killing only `Baluffo.exe` causes that proof PID to exit before any generic smoke cleanup runs.
- Use this lane for packaged browser supervision, Chromium app-mode launch changes, and Windows Job Object browser-lifecycle fixes.

## Release/build regression picks

Use the narrowest check that matches the risky path:

- Packaging or portable EXE changes: `python scripts/build_portable_exe.py`
- Packaged sync config, auth portability, or sync release-gate changes: `npm run test:frontend:packaged:sync-rehearsal`
- Packaged desktop supervision, stale-runtime recovery, or launcher self-heal changes: `npm run test:frontend:packaged:orphan-reclaim-rehearsal`
- Packaged Chromium supervision or managed-browser shutdown propagation changes: `npm run test:frontend:packaged:browser-job-rehearsal`
- Packaged updater, desktop handoff, or release-manifest changes: `npm run test:frontend:packaged:update-rehearsal`
- Bridge route wiring or task-launch signature changes: focused `tests/bridge/...` plus `tests/test_pipeline_execution.py` for worker-path coverage
- Admin task buttons, presets, or busy-state changes: focused frontend unit tests plus the nearest admin bridge payload test
- Contamination or location-quality regressions: targeted fetcher/unit checks around sanitization, canonicalization, or audit helpers

**Test-to-source map:**

| Area | Test path |
|------|-----------|
| Jobs pipeline / jobs_fetcher | `tests/test_jobs_fetcher.py` (integration shim), `tests/test_jobs_fetcher_google_sheets.py`, `tests/test_jobs_fetcher_parsing.py`, `tests/test_jobs_fetcher_providers.py`, `tests/jobs_static/`, `tests/test_jobs_fetcher_pipeline.py`, `tests/test_jobs_fetcher_quality.py` |
| Source discovery | `tests/source_discovery/` |
| Admin bridge (registry, runtime, static fallback, sync) | `tests/admin/test_admin_bridge_ops_*.py` |
| Desktop app / launcher | `tests/desktop_app/` |
| Packaged desktop smoke / rehearsal | `tests/packaged_desktop/` |
| Source sync | `tests/test_source_sync.py` |
| Local data store, backup, config, etc. | `tests/test_local_data_store.py`, `tests/desktop_app/`, and the nearest focused `tests/test_*.py` module for the subsystem |

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
- Perf-sensitive backend or packaging change: `npm run perf:py:timing`, then the nearest discovery/startup perf lane if relevant
- Before pushing to `main` or preparing a release: `npm run test:py:extended`
- Before merging a broad or risky backend change: `npm run test:py:extended` or `npm run verify`
- JavaScript/frontend unit change: `npm run test:unit`
- Browser or page-flow change: `npm run test:smoke`
- Broad or risky change: `npm run verify`

For the AI bootstrap and task-routing summary, see [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md).

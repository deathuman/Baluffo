# Testing

> - **Status:** Active
> - **Use this when:** choosing the narrowest verification step, finding relevant test files, or understanding fixture layout
> - **Canonical for:** test commands, targeted test routing, and fixture references
> - **Not canonical for:** runtime architecture or data contracts
> - **Then inspect:** the nearest `tests/` module for the subsystem you changed
> - **Last updated:** 2026-06-01

This document owns the verification matrix for Baluffo. Keep build, test, and fixture guidance here instead of repeating command tables in routing docs.

Python dependency installs that need reproducible runtime parity should use `requirements-lock.txt`. Regenerate it from the human-edited `requirements.txt` with:

```bash
uv pip compile requirements.txt -o requirements-lock.txt
```

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

Run the Linux-compatible suite (skips `@pytest.mark.windows` tests):

```bash
npm run test:py:linux
```

This skips tests using Windows-only APIs (`ctypes.windll`, `winreg`, pefile). Use this on Linux, macOS, or CI. Tests marked `@pytest.mark.windows` are registered in `pytest.ini` and are automatically excluded by this lane.

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
| Complete benchmark report | `npm run perf:complete` | `_out/perf-complete/summary.json` plus the timestamped run directory |
| Complete benchmark with live bridge sample | `python scripts/perf_complete.py --bridge-base-url http://192.168.50.61:8877` | `_out/perf-complete/summary.json` plus a bounded live bridge sample |
| Complete benchmark with multi-timeout live sample | `python scripts/perf_complete.py --bridge-base-url http://192.168.50.61:8877 --bridge-timeouts 3,10,30` | Same as complete benchmark, with live request rows for each timeout |
| Live bridge profile sample only | `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url http://192.168.50.61:8877` | `_out/perf-complete/live/<timestamp>/bridge-profile/live/` |
| Live bridge profile sample with timeout comparison | `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url http://192.168.50.61:8877 --timeouts 3,10,30` | Same live output, with TCP, first-byte, and full-response timing per timeout |
| Startup bridge A/B experiment | `python scripts/perf_startup_bridge_ab.py --pairs 5 --pages jobs,admin` | `_out/perf-complete/startup-bridge-ab/<timestamp>/startup-bridge-ab-summary.json` |
| Slowest Python tests | `npm run perf:py:timing` | Console output only |
| Isolated discovery sanity benchmark | `npm run perf:discovery:benchmark` | `_out/perf-sanity-discovery/` |
| Packaged Jobs cold/warm startup probe | `npm run perf:startup:cold` / `npm run perf:startup:warm` | `.tmp/packaged-desktop-smoke/` and `data/packaged-desktop-smoke-report.json` |
| Packaged Admin cold/warm startup probe | `npm run perf:startup:admin:cold` / `npm run perf:startup:admin:warm` | `.tmp/packaged-desktop-smoke/` and `data/packaged-desktop-smoke-report.json` |

Notes:
- Prefer repo-local artifact roots such as `.tmp/` and `_out/` for new perf workflows; avoid `%LOCALAPPDATA%\\Temp` for benchmark or runtime-state outputs in this Windows-first repo.
- Use `npm run perf:complete` when asked for the most complete benchmark. It aggregates discovery/fetch medians, fetch source/provider-board timing and status/cache buckets, frontend boot traces, Jobs and Admin cold/warm packaged startup, packaged sync push/pull timings, sync push detail stages, sync remote-write timing, bridge route/operation profile snapshots, optimization targets, artifact sizes, best-effort process-tree RAM, and top process-level RAM contributors.
- Use `python scripts/perf_complete.py --bridge-base-url <url>` when route-level timing from a running desktop or Umbrel bridge needs to be correlated with local benchmark evidence. This is read-only and failure is recorded as evidence, not as a benchmark failure. Default `npm run perf:complete` stays local and reproducible.
- Use `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url <url>` for a quick live-only read-only sample without running the packaged startup, frontend, discovery, fetch, and sync benchmarks. Add `--timeouts 3,10,30` when debugging LAN reachability so the output separates TCP connect, first-byte, and full-response timing for each endpoint.
- Use `python scripts/perf_startup_bridge_ab.py --pairs 5 --pages jobs,admin` only when evaluating whether `BALUFFO_STARTUP_PARALLEL_BRIDGE=1` should become the desktop default. Promotion requires complete default and parallel samples for both Jobs and Admin, material cold/warm median improvements, no cold-start regression, and no packaged lifecycle smoke regressions.
- Use `/ops/performance-profile` directly for ad hoc route-level timing checks. It is in-memory and bounded; it reports aggregate route and backend operation timings only, with query strings and dynamic path segments redacted.
- Current safe RAM tuning is scoped to Chromium app-mode startup flags. The packaged sync section remains a full-runtime no-browser rehearsal so its RAM numbers stay comparable with earlier complete benchmark reports.
- `npm run perf:discovery:benchmark` is the default discovery perf entrypoint because it keeps artifacts under `_out/`; use `python scripts/benchmark_discovery_probe.py` separately when tuning discovery probe concurrency.
- Do not add `pytest-benchmark` or `py-spy` by default here. If dependency approval happens later, benchmark deterministic Python leaf logic first and keep desktop startup analysis on the existing startup-trace pipeline.

### Playwright perf traces

Opt-in Playwright performance traces capture boot traces for Jobs, Admin, and Saved pages:

```powershell
npm run test:frontend:perf
```

Output goes to `_out/perf-traces/`:

| Artifact | Description |
|----------|-------------|
| `_out/perf-traces/{pageName}-boot-trace.zip` | Standard Playwright trace archive (open with `playwright show-trace` or trace.playwright.dev) |
| `_out/perf-traces/{pageName}-boot-summary.json` | Navigation timings, paint events, user-timing marks/measures per page |

To attach traces to a performance investigation, run `npm run test:frontend:perf`, collect the `.zip` traces from `_out/perf-traces/`, and open them in Chrome DevTools (Performance tab) or at `https://trace.playwright.dev`. Compare warm-vs-cold boot runs or before-vs-after a suspected performance regression change.

The perf-trace spec lives at `tests/frontend/perf-trace.spec.js` with config at `playwright.perf.config.js`.

## Python dependency security audit

Run the Python dependency vulnerability audit with:

```bash
npm run security:python
```

The audit scans the checked-in `requirements-lock.txt` with `pip-audit`, writes a JSON report to `.tmp/security/pip-audit.json`, and fails on any unallowlisted advisory. CI runs this lane after the pre-commit guardrails in the lint workflow.

Known non-actionable findings must be listed in `tools/security/pip-audit-allowlist.json` with an advisory id, package, reason, owner, and `review_by` date. Expired or malformed allowlist entries are failures. Ownership defaults to the matching code owner; for repository-wide dependency findings, use the default owner from `CODEOWNERS`.

## Secret scanning

`gitleaks` runs through the existing pre-commit lane. Use the normal commands:

```bash
npm run lint:precommit:changed
npm run lint:precommit:ci
```

The repo-specific tuning lives in `.gitleaks.toml`, with filename-aware hook routing in `scripts/gitleaks_precommit.py`. Keep allowlists narrow: allowlist known fake fixtures and documented placeholders, not whole test or docs trees. Normal PR and push CI scans the tracked file list passed by the lint workflow, including its existing `data/` exclusion. A full-history audit is a separate manual incident or rollout task, for example `gitleaks git --config .gitleaks.toml --redact --verbose .`, and should be followed by credential rotation before any history rewrite is considered.

## Pre-push gate

The tracked Git pre-push hook keeps the default local push path narrow and makes the full local CI-equivalent gate explicit.

- Normal push to `main`: runs only `npm run lint:precommit:ci`.
- Optional full local CI gate: set `PRE_PUSH_FULL_CI=1` or run `npm run prepush:full`.
- Optional hook warmup: set `PRE_PUSH_WARM_HOOKS=1` or run `npm run prepush:warm`.
- Optional timing CSV log: set `PRE_PUSH_TIMING_LOG=1`.
- Optional custom timing log path: set `PRE_PUSH_TIMING_LOG_PATH=<path>`.

The hook emits lightweight console timing lines in this format:

```text
[timing] phase=<name> status=<code> elapsed_ms=<n>
```

Measured baseline on the primary Windows development machine:

- Default `main` lint gate: about `31s`
- Full local CI mode: about `109s`
- Warmup path after environments exist: under `1s`

The main source of the “first push timed out” complaint is usually hook environment bootstrap or accidentally using the full local CI path as the default push gate. Warm hooks once after a new clone, Python switch, or cache reset to avoid the first-run pre-commit spike:

```bash
npm run prepush:warm
```

If an external launcher or local automation wraps `git push origin main` with a hard timeout, keep the default hook behavior unchanged and raise only that outer timeout to at least `150s`. No tracked repo-side `git push` wrapper currently enforces a `120s` timeout, so any remaining cap is outside the repository hook source.

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
| Repository policy guardrails | `npm run lint:repo-guardrails` |
| Frontend unit tests | `npm run test:frontend:unit` |
| Python dependency security audit | `npm run security:python` |
| Build ship bundle | `npm run build:ship-bundle` |
| Build portable EXE | `npm run build:portable-exe` |
| Build Linux AppImage | `npm run build:linux` |
| Build container image | `docker build -t ghcr.io/deathuman/baluffo:local .` |
| Build clean archived container context | `python scripts/docker_build_clean_context.py --tag ghcr.io/deathuman/baluffo:local` |
| Prepare shared portable EXE | `npm run build:portable-exe:prepare` |
| Ship bundle leaf builder | `python scripts/build_ship_bundle.py --bundle-version <version>` |
| Portable EXE leaf builder | `python scripts/build_portable_exe.py --bundle-version <version>` |
| Linux AppImage leaf builder | `python scripts/build_portable_linux.py` |
| Python perf timing | `npm run perf:py:timing` |
| Complete perf report | `npm run perf:complete` |
| Discovery perf sanity | `npm run perf:discovery:benchmark` |
| Packaged Jobs startup perf probe (cold/warm) | `npm run perf:startup:cold` / `npm run perf:startup:warm` |
| Packaged Admin startup perf probe (cold/warm) | `npm run perf:startup:admin:cold` / `npm run perf:startup:admin:warm` |
| Packaged desktop smoke gate | `npm run test:frontend:packaged` |
| Packaged sync rehearsal | `npm run test:frontend:packaged:sync-rehearsal` |
| Packaged orphan reclaim rehearsal | `npm run test:frontend:packaged:orphan-reclaim-rehearsal` |
| Packaged browser job rehearsal | `npm run test:frontend:packaged:browser-job-rehearsal` |
| Packaged desktop lifecycle rehearsal | `npm run test:frontend:packaged:desktop-lifecycle-rehearsal` |
| Packaged active-task close rehearsal | `npm run test:frontend:packaged:active-task-close-rehearsal` |
| Packaged task abort and scheduler rehearsal | `npm run test:frontend:packaged:task-abort-schedule-rehearsal` |
| Packaged deterministic Jobs first-run gate | `npm run test:frontend:packaged:first-run` |
| Jobs-page no-Admin packaged smoke gate | `npm run test:frontend:packaged:jobs-pipeline` |
| Admin startup packaged smoke gate | `npm run test:frontend:packaged:admin-startup` |
| Packaged desktop updater rehearsal | `npm run test:frontend:packaged:update-rehearsal` |
| Orchestrated packaged smoke gate | `npm run test:frontend:packaged:orchestrated` |
| Cache-backed packaged Jobs diagnostic | `npm run probe:desktop:startup:cold` |
| Cache-backed packaged Jobs cold-start release gate | `npm run probe:desktop:startup:jobs:cold` |
| Cache-backed packaged Admin diagnostic | `npm run probe:desktop:startup:admin:cold` |
| One file | `python -m pytest tests/<path/to/test_*.py> -q` |
| Admin bridge | `python -m pytest tests/admin/ -q` |
| GameDevMap discovery lane | `python -m pytest -q tests/source_discovery -k gamedevmap`, then `python -m pytest -q tests/source_discovery`, then `npm run lint:precommit` |
| Match developer lane directly | `python -m pytest tests -q -m "not slow and not packaging and not release" --color=no --basetemp=.tmp/pytest/basetemp` |

**Container / Umbrel targeted checks:**

| Goal | Command |
|------|---------|
| Container bridge/static/runtime seeding and CORS policy | `python -m pytest tests/bridge/test_container_runtime.py -q` |
| Container frontend mode and same-origin bridge | `node --test --test-reporter=dot tests/frontend/unit/admin-config.test.mjs tests/frontend/unit/api-client.test.mjs tests/frontend/unit/admin-runtime-bridge-base.test.mjs tests/frontend/unit/container-local-data.test.mjs tests/frontend/unit/desktop-local-data-navigation.test.mjs tests/frontend/unit/local-data-runtime-contract.test.mjs` |
| Container image build | `docker build -t ghcr.io/deathuman/baluffo:local .` |
| Windows/reparse-safe image build fallback | `python scripts/docker_build_clean_context.py --tag ghcr.io/deathuman/baluffo:local` |
| Container sync config packaging | `python -m pytest tests/test_container_packaging.py tests/test_build_container_sync_config.py tests/test_build_sync_app_config.py tests/test_source_sync.py -q` |
| Container smoke | `docker run --rm -p 8877:8080 -v baluffo-data:/data ghcr.io/deathuman/baluffo:local`, then poll `http://127.0.0.1:8877/ops/health` and load `http://127.0.0.1:8877/jobs.html` |

Prefer the normal `docker build .` command. The clean-context helper builds committed `HEAD` from a temporary `git archive` under `.tmp` and cleans it afterward, so it is only a fallback when live Windows workspace context transfer is blocked by reparse points or similar local artifacts.

Official GHCR publishes embed the portable encrypted GitHub App sync config from GitHub Actions BuildKit secrets. Pull request and local builds without those secrets remain valid container smoke targets, but `/sync/status` will report a missing packaged config in those images.

`Build Container` is path-filtered for branch and pull request changes that only touch docs, tests, or repo-process files. A skipped container workflow is expected for those ignored paths; require container publish evidence for runtime-relevant container changes, version/tag publishes, Umbrel metadata changes, or manual `workflow_dispatch` publishes.

Use `npm run release:preflight` when you are about to push a release commit, move a release tag, or publish release artifacts. It runs the pre-commit gate, the full Python lane, frontend unit tests, prepares the shared portable EXE once, and then runs the packaged desktop release lanes in canonical order.

**Shared fixtures (where they are defined):**

| Fixture | Location |
|---------|----------|
| `repo_root`, `codex_tmp_root`, `make_test_root` | `tests/conftest.py` |
| `admin_bridge_entrypoint_root` | `tests/admin/conftest.py`, backed by `tests/admin/_helpers.py` |
| `source_sync_test_root` | `tests/source_sync_helpers.py` |
| `workspace_tmpdir(prefix)`, shared temp-root allocation/cleanup helpers | `tests/helpers/temp_paths.py` |
| Job payload fixture loaders | `tests/helpers/job_fixtures.py`, with reusable data under `tests/fixtures/` |
| Desktop launcher config/session factories | `tests/desktop_app/_helpers.py` |
| Source-discovery local config/runtime helpers | `tests/source_discovery/_helpers.py` |
| Focused GameDevMap source-discovery helpers | `tests/source_discovery/gamedevmap_test_helpers.py` |
| Frontend admin controller factories | `tests/frontend/unit/helpers/admin-controller-test-helpers.mjs` |

**Temp directory note (Windows sandbox):**

- Prefer repo-local temp fixtures such as `workspace_tmpdir(...)`, `make_test_root(...)`, and local family fixtures for new tests that write runtime state.
- In this environment, direct pytest temp-root creation under `%LOCALAPPDATA%\\Temp` can hit Windows permission errors during setup/cleanup.
- Keep pytest temp roots under `.tmp/pytest`; the repo disables pytest's cacheprovider by default so unreadable `pytest-cache-files-*` debris does not accumulate in the workspace.
- If a narrow bridge test run fails before assertions with tmpdir/tempfile ACL errors, rerun it with a repo-local `--basetemp` or the existing repo-local tempdir shim rather than treating it as a product regression.

## Packaged artifact ownership

- Direct packaging commands own `dist/` outputs and refresh the convenience mirror at `_out/latest/build/portable`:
  - `npm run build:portable-exe`
  - `npm run build:portable-exe:prepare`
  - `python scripts/build_portable_exe.py`
- Portable EXE builds are content-addressed under `_out/portable-build-cache`. Use `python scripts/build_portable_exe.py --force`, `npm run build:portable-exe -- --force`, or `npm run build -- --force` only when you intentionally want to bypass the cache.
- Portable builds must stay self-contained but browser-minimal: `_internal/playwright/driver/package/.local-browsers/` may contain only the `chromium_headless_shell-*` directory required by packaged Playwright `browsers.json`.
- `npm run test:frontend:packaged*`
- `npm run test:frontend:packaged:sync-rehearsal`
- `npm run test:frontend:packaged:orphan-reclaim-rehearsal`
- `npm run test:frontend:packaged:browser-job-rehearsal`
- `npm run test:frontend:packaged:desktop-lifecycle-rehearsal`
- `npm run test:frontend:packaged:first-run`
- `npm run test:frontend:packaged:admin-startup`
- `npm run test:frontend:packaged:update-rehearsal`
  - Packaged test lanes reuse the default portable EXE when its build fingerprint is current. Explicit `--exe-path` values are never auto-rebuilt, and `src/packaged_desktop_smoke.py --rebuild` is an opt-in debugging escape hatch.
  - Cold startup probes isolate runtime data/profile state instead of requiring a fresh executable build.
  - Packaged smoke and rehearsal launches isolate both `%APPDATA%` and `%LOCALAPPDATA%` under the lane artifact directory so missing data-root overrides cannot write rehearsal state into real Windows user data.
  - The update rehearsal copies the portable build before injecting rehearsal public keys or removing optional `_internal/psutil*`, so cached and `dist/` portable roots stay pristine while the source-side handoff verifier still passes through the dependency-free Windows PID fallback.
- Orchestrated build and verify commands own `_out/runs/...` and the rest of `_out/latest/...`:
  - `npm run build`
  - `npm run verify`
  - `python scripts/orchestrator.py build`
  - `python scripts/orchestrator.py verify`
- `build:portable-exe -- --skip-latest-mirror` can be used for an isolated `dist/` materialization when the latest mirror should not move.

## Test ownership rules

- Repository policy checks belong in `tools/repo_health/repo_guardrails.py` and run through `npm run lint:repo-guardrails`, not pytest or frontend unit collection.
- Frontend unit tests are discovered directly by Node through `npm run test:frontend:unit`; new files only need to live under `tests/frontend/unit/` and match `*.test.mjs`.
- Do not add generated frontend unit aggregators or manifest-sync scripts.
- Real shard files must own real tests. Do not hide test functions inside giant imported `_cases.py` containers.
- Shared helpers should stay local to the test family and helper-only. Prefer `_helpers.py`, `conftest.py`, or a focused helper module over a broad test utility barrel.
- Do not add root pytest fixtures for single-family setup. Keep source-sync, admin, bridge, jobs, and static-adapter helpers in their nearest test family unless they are truly universal.
- Large fixture files under `tests/fixtures/` must be referenced by at least one test or helper, or explicitly listed with a reason in `tools/repo_health/fixture_reference_allowlist.json`; `npm run lint:repo-guardrails` treats unreferenced fixtures as failures.
- Extract data and repeated setup into helpers, but keep behavioral assertions in the owning test file unless the assertion itself is duplicated across multiple tests.
- Before adding a new guard or smoke test, delete or merge any older test that already protects the same invariant.
- Prefer seam-patched unit checks for selection, normalization, and routing logic. Keep only one intentionally slow smoke test when full execution is the behavior under test.
- Task abort changes should at minimum run the lifecycle, abort service/route, pipeline, focused race, and frontend lanes: `python -m pytest tests/bridge/test_task_lifecycle_service.py tests/bridge/test_task_abort_service.py tests/bridge/test_task_abort_route.py tests/bridge/test_lifecycle_cleanup.py tests/bridge/test_pipeline_service.py tests/bridge/test_discovery_abort_races.py -q` and `npm run test:frontend:unit -- tests/frontend/unit/admin-ops-history-abort-render.test.mjs tests/frontend/unit/jobs-pipeline-controller.test.mjs tests/frontend/unit/jobs-pipeline.test.mjs`.
- Jobs pipeline schedule changes should at minimum run the scheduler, route, Ops health, and Admin Ops frontend lanes: `python -m pytest tests/bridge/test_pipeline_schedule_service.py tests/bridge/test_pipeline_schedule_routes.py tests/admin/test_pipeline_schedule_ops_health.py tests/admin/test_admin_bridge_ops_health.py -q` and `npm run test:frontend:unit -- tests/frontend/unit/admin-ops-summary-render.test.mjs tests/frontend/unit/admin-ops-pipeline-schedule.test.mjs`.

## Jobs First-Run Packaged Smoke Contract

- `npm run test:frontend:packaged:first-run` is the deterministic packaged Jobs first-run gate.
- It launches the packaged runtime on `desktop-probe.html` first, then the node smoke script opens `jobs.html` so the first Jobs page load belongs to the test, not to startup profiling or readiness probing.
- The lane sets `BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE=controlled-heartbeat-success` only for this smoke script. The real Jobs UI starts `/tasks/run-jobs-bootstrap`, the route and lifecycle/report promotion path run, but the bootstrap feed is a deterministic one-row sheet-scoped fixture and does not call live Google Sheets.
- The smoke uses URL-scoped first-run timeout overrides so the backend remains active past the UI timeout boundary in seconds rather than minutes, with fresh task-live heartbeats suppressing timeout UI.
- It must prove all of the following:
  - the isolated runtime data dir starts without row-bearing `jobs-unified*` artifacts,
  - cold Jobs startup shows first-run progress, not a silent empty list or retry UI,
  - no visible `Bridge timed out` or `first-run sheet refresh timed out` text appears while task-live heartbeat is fresh,
  - the UI bootstrap request body is exactly `{ "source": "jobs_first_run" }` and does not include `forceBootstrap`,
  - the bridge bootstrap route starts or reattaches with `smokeMode: "controlled-heartbeat-success"`,
  - task state and fetch report evidence show a running bootstrap before promotion,
  - the deterministic feed promotes and renders,
  - a post-success cold reload does not start a second bootstrap,
  - sheet-limited first-run coverage messaging remains visible after the feed renders,
  - light and dark first-run/local-auth popup screenshots are captured at desktop and mobile widths,
  - computed popup style assertions cover overlay opacity, panel bounds, light surfaces, readable text, controls, inputs, and selects.
- The smoke writes screenshots and `first-run-style-report.json` under the packaged smoke output directory. These are artifacts, not checked-in pixel baselines.

## Jobs Pipeline Smoke Contract

- `npm run test:frontend:packaged:jobs-pipeline` is no longer just a “pipeline started” check.
- It must prove all of the following in the packaged desktop runtime:
  - fresh packages do not include row-bearing jobs artifacts and must not fall back to stale bundled `jobs-unified*` or startup preview rows,
  - a bootstrap-scoped feed keeps full-pipeline limited-coverage messaging visible until a full Jobs pipeline succeeds,
  - the Jobs page renders and the pipeline button becomes busy,
  - the pipeline reports a real `runId`,
  - the tracked run reaches a terminal non-error state,
  - no backend `error` payload is surfaced after startup.
- This lane uses a smoke-only stub-success pipeline mode so it stays deterministic and bounded while still exercising the real `PipelineService` worker path.
- On Windows CI, bridge requests can briefly fail with loopback errors such as `ECONNREFUSED` while the packaged runtime is settling after startup. Bounded retry is acceptable, but a failure after the retry window should be treated as a bridge/runtime failure and diagnosed from the packaged smoke report plus bridge stdout/stderr artifacts.

## Packaged Smoke CI Diagnostics

- When a packaged smoke lane fails in GitHub Actions, download the run artifacts and inspect the scenario report JSON before relying on truncated console logs.
- Failure summaries must be safe for non-UTF-8 Windows consoles. If Playwright output contains Unicode and the Python smoke wrapper raises `UnicodeEncodeError`, fix the diagnostic printer first because it may be hiding the real packaged runtime failure.

## Admin Startup Packaged Smoke Contract

- `npm run test:frontend:packaged:admin-startup` is the packaged Admin first-render gate for the portable desktop runtime.
- It must prove all of the following in packaged mode:
  - `/ops/health` reports `desktopMode: true`,
  - the Admin page records `admin_first_interactive`,
  - the Admin page records `admin_ops_health_first_render`,
  - the bridge badge reaches `Bridge Online`,
  - `#admin-ops-trends` does not remain stuck on `Loading operations health...`,
  - `#admin-source-status` does not remain stuck on `Loading admin overview...`,
  - startup requests use `/ops/task-state?view=summary` and `/registry/conflicts?view=summary`, not the full multi-MiB diagnostic routes.
- Use this lane for Admin startup, ops-summary payloads, desktop local-data overview, and packaged bridge availability changes.

## Desktop Updater Rehearsal Contract

- `npm run test:frontend:packaged:update-rehearsal` is the packaged `N -> N+1` updater gate for the portable desktop runtime.
- It must prove all of the following:
  - the packaged app can surface an available update and hand off install to `BaluffoUpdater.exe`,
  - the install plan carries the resolved `dataDir` so helper state, downloads, rollback metadata, logs, and success markers stay under the external Windows data root,
  - the helper installs the target portable ZIP and relaunches the target runtime successfully,
  - the target runtime reaches desktop startup readiness and writes the post-install success marker,
  - seeded local profile data, saved jobs, notes, and attachments remain intact across the update.
- Use this lane for updater-helper, desktop session/handoff, release-manifest, Windows packaged data-root migration, and portable runtime mutation changes.

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

## Packaged Desktop Lifecycle Rehearsal Contract

- `npm run test:frontend:packaged:desktop-lifecycle-rehearsal` is the packaged desktop-window liveness and shutdown gate for the portable desktop runtime.
- It must prove all of the following:
  - the packaged runtime accepts a short smoke-only `--owner-idle-timeout-s` override while production defaults remain unchanged,
  - a controlled desktop page can lose `/app/desktop-session-lifecycle` POST/beacon traffic while continuing non-health page traffic such as `/ops/task-state`,
  - the launcher and bridge stay alive past the owner idle timeout while that non-health page traffic continues,
  - owner activity advances from non-health page traffic; `/ops/health` polling alone remains excluded,
  - after the controlled page traffic stops, the launcher exits and releases the site/bridge ports,
  - smoke-only CDP attaches to the managed Chromium app-mode page, dispatches the regular page lifecycle close signal, and then sends a Windows main-window close instead of `taskkill`,
  - the launcher, browser proof PID, and site/bridge ports release within the 5s regular-close cleanup target.
- This lane is required for desktop-window liveness, owner idle timeout, lifecycle heartbeat, launcher shutdown, or orphan-process cleanup changes.

## Packaged Active-Task Close Rehearsal Contract

- `npm run test:frontend:packaged:active-task-close-rehearsal` is the packaged confirmed-close gate for active critical task shutdown.
- It must prove all of the following:
  - the packaged runtime launches in managed Chromium app mode with smoke-only CDP enabled,
  - the Jobs UI sees a deterministic active bootstrap task before close,
  - accepting the active-work close confirmation records `desktop_confirmed_active_work_shutdown_requested`,
  - the launcher exits and releases the browser proof PID and site/bridge ports,
  - no `desktop_browser_relaunch_requested` or `desktop_runtime_fatal` event appears.
- This lane is required for active-task beforeunload behavior, confirmed desktop close intent, launcher active-work recovery, or shutdown cleanup changes.

## Packaged Task Abort And Scheduler Rehearsal Contract

- `npm run test:frontend:packaged:task-abort-schedule-rehearsal` is the packaged bridge/runtime smoke gate for task abortion and recurring Jobs pipeline scheduling.
- It must prove all of the following:
  - the packaged bridge can start a deterministic long-heartbeat Jobs bootstrap task,
  - `POST /tasks/abort` accepts a runId-scoped fetch abort and the terminal lifecycle evidence reports `canceled` with `user_abort_requested`,
  - enabling `POST /tasks/jobs-pipeline-schedule` with `{ enabled: true, intervalHours: 1 }` records a `lastTriggerRunId`,
  - the scheduled Jobs pipeline reaches a terminal non-error state in smoke-only `stub-success` mode,
  - disabling the schedule clears pending work.
- This lane is required for packaged abort route, lifecycle terminal evidence, recurring Jobs pipeline scheduler, or scheduler Admin Ops contract changes. It is not a replacement for focused Python and frontend unit coverage.

## Release/build regression picks

Use the narrowest check that matches the risky path:

- Packaging or portable EXE changes: `python scripts/build_portable_exe.py`
- Container or Umbrel deployment changes: `python -m pytest tests/bridge/test_container_runtime.py -q`, targeted frontend unit checks for container mode, and a Docker smoke build/run when Docker is available
- Linux packaging or AppImage changes: `npm run build:linux` then `bash scripts/smoke_test_appimage.sh`
- Packaged sync config, auth portability, or sync release-gate changes: `npm run test:frontend:packaged:sync-rehearsal`
- Packaged desktop supervision, stale-runtime recovery, or launcher self-heal changes: `npm run test:frontend:packaged:orphan-reclaim-rehearsal`
- Packaged Chromium supervision or managed-browser shutdown propagation changes: `npm run test:frontend:packaged:browser-job-rehearsal`
- Desktop-window liveness, owner idle timeout, lifecycle heartbeat, launcher shutdown, or orphan-process cleanup changes: `npm run test:frontend:packaged:desktop-lifecycle-rehearsal`
- Active-task desktop close confirmation or active-work launcher recovery changes: `npm run test:frontend:packaged:active-task-close-rehearsal`
- Packaged task abort route, lifecycle cancel evidence, or recurring Jobs pipeline scheduler changes: `npm run test:frontend:packaged:task-abort-schedule-rehearsal`
- Packaged Jobs first-run, cold empty-state, bootstrap confirm/retry, or popup theme changes: `npm run test:frontend:packaged:first-run`
- Packaged Admin startup, overview, or heavy ops-payload loading changes: `npm run test:frontend:packaged:admin-startup`
- Packaged updater, desktop handoff, Windows packaged data-root migration, or release-manifest changes: `npm run test:frontend:packaged:update-rehearsal`
- Packaged Jobs startup threshold changes: `npm run probe:desktop:startup:jobs:cold`
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
| Container / Umbrel runtime | `tests/bridge/test_container_runtime.py`, `tests/frontend/unit/api-client.test.mjs`, `tests/frontend/unit/container-local-data.test.mjs`, `tests/frontend/unit/desktop-local-data-navigation.test.mjs`, `tests/frontend/unit/local-data-runtime-contract.test.mjs` |
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

## Refactor Guard

Run the path-aware refactor lane when you change compatibility roots, archive/doc routing, or hook workflow wiring:

```bash
npm run test:refactor:changed
```

The lane inspects changed tracked files first. It runs only the matching contract/subsystem tests for narrow compatibility-surface changes, runs `tests/test_release_docs.py` for docs routing changes, and escalates to `npm run test:py:extended` for broad multi-subsystem or packaging/release refactors.

## Which command should I run?

- Small Python logic change: `npm run test:py`
- Compatibility root or monkeypatch-surface refactor (`src/ship/desktop_app/*`, `src/ship/desktop_updater.py`, `src/packaged_desktop_smoke.py`, `src/source_discovery/{gamesmap,reporting,web_search}.py`, `src/source_sync.py`, `src/admin_bridge.py`, `src/jobs_fetcher.py`, `src/jobs/{pipeline,state,reporting}.py`): `npm run test:refactor:changed`
- Docs/archive move or routing cleanup (`docs/`, especially `docs/archive/`, `docs/INDEX.md`, `docs/CHANGELOG.md`): `npm run test:refactor:changed`
- Perf-sensitive backend or packaging change: `npm run perf:py:timing`, then the nearest discovery/startup perf lane if relevant
- Before pushing to `main` or preparing a release: `npm run test:py:extended`
- Before merging a broad or risky backend change: `npm run test:py:extended` or `npm run verify`
- JavaScript/frontend unit change: `npm run test:frontend:unit`
- Browser or page-flow change: `npm run test:smoke`
- Broad or risky change: `npm run verify`

If you switch Python interpreters or recreate the local environment, run `npm run lint:precommit:changed` before starting refactor work. Hook setup also expects `python -m mypy --version` to succeed in the active interpreter.

For the AI bootstrap and task-routing summary, see [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md).

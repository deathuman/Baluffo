# Testing

> - **Status:** Active
> - **Use this when:** choosing the narrowest verification step, finding relevant test files, or understanding fixture layout
> - **Canonical for:** test commands, targeted test routing, and fixture references
> - **Not canonical for:** runtime architecture or data contracts
> - **Then inspect:** the nearest `tests/` module for the subsystem you changed
> - **Last updated:** 2026-08-21

This document owns the verification matrix for Baluffo. Keep build, test, and fixture guidance here instead of repeating command tables in routing docs.

Python dependency installs that need reproducible runtime parity should use `requirements-lock.txt`. Regenerate it from the human-edited `requirements.txt` with:

```bash
uv pip compile requirements.txt -o requirements-lock.txt
```

## Python tests (pytest)

The repo `.venv` is a Linux/WSL venv whose `bin/python` symlink is broken on Windows; run Python tests with the global `python -m pytest` (pytest 9.x is installed there), not `.venv/bin/python`.

Availability-focused development lane:

```bash
python -m pytest tests/test_job_availability_identity.py tests/test_job_availability_identity_quarantine.py tests/test_job_availability_carried_seed.py tests/test_job_availability_validator.py tests/test_job_availability_tombstones.py tests/test_job_availability_schedule.py tests/test_jobs_lifecycle_source_evidence.py tests/test_jobs_lifecycle_output.py tests/test_local_data_store.py tests/test_local_data_store_availability.py tests/bridge/test_job_availability_routes.py tests/bridge/test_job_availability_atomic.py -q
npm run test:frontend:unit
```

This lane covers seed-vs-observation regression, source failure/absence semantics, overdue timing,
direct classifier fixtures and conflicts, stable aliases, history publication, bounded scheduling,
Saved idempotency/terminal behavior/reports, private custom-URL monitoring, backup v4 compatibility,
duplicate/stale direct checks, SQLite/private-full/public-projection reconciliation, post-publication
projection/sweep, atomic mirror interleaving, private canonical-tombstone reopening, fail-closed
missing authority, runtime-gated unavailable-link actions, routes, and frontend attention state.
Export-retirement changes also run task-launch jobs-feed, container/static-server, runtime-launcher,
JSON quality-tool, and Saved lifecycle-overlay tests; public full JSON and CSV requests must be 404.
Identity-repair changes additionally cover source-ID collisions across exact URLs, one-to-one URL
changes, post-assignment collisions between legacy and generated source-backed IDs, URL-less
candidate exclusion from observations/publication, quarantine v1/v2
pruning/truncation/private serving, carried-seed initialization, exact Saved rebinding, terminal
finalization failure projection, parent error propagation, and fetch-report field preservation. The
quick lane includes a 5,000-row collision regression; the slow/release lane includes a 10,003-row
partition regression with a bounded quarantine projection. That fixture started as the 79,525-row
live identity count, but per-row cost is flat (~0.05 ms) once each distinct URL is validated once
per run rather than once per pass, so the extra rows bought ~30 s of wall clock without changing the
partition outcome. Its cost guard is a ratio rather than a deadline: the 10,003-row run is compared
in CPU time against a 5,000-row probe, so 2x the rows must stay under 3x the CPU, and host speed
cancels out of the comparison.

Run the balanced developer Python lane:

```bash
npm run test:py
```

This wrapper now uses the repo-local pytest temp root under `.tmp/pytest` (with `--basetemp=.tmp/pytest/basetemp`) and configures pytest cache under `.tmp/pytest/cache`. The repo disables pytest's cacheprovider by default because this Windows environment can leave unreadable `pytest-cache-files-*` temp directories behind. It excludes `slow`, `packaging`, and `release` tests so the default local loop stays focused on day-to-day development.

Run the full Python suite when you need release-level confidence:

```bash
npm run test:py:extended
```

This lane shards with `pytest-xdist` (`-n 4 --dist=loadfile`, measured 9m15s serial to
2m23s), so every workflow that runs the Python suite installs `pytest-xdist` alongside
`pytest` — including the AppImage lane, which runs the suite before it builds. Files stay whole
on one worker, and two hygiene rules keep the sharded run honest: the stale-temp sweep at
conftest import only removes directories older than 30 minutes (a worker or child pytest
starting mid-run would otherwise delete another worker's live `pytest-tmp-*` directory), and
the child pytest in `tests/test_compat_seam_guard.py` runs from a private temp root, because
pytest collects the intermediate directories of a file argument.

Every session also records which files each worker ran, under
`.tmp/pytest/xdist-assignments/run-<uid>/`: xdist never says which worker took which file, so
without it a cross-file leak (a patch that outlives the file that installed it) can only be
re-chased by re-running the lane and hoping the same pair lands together. `python -m
tests.helpers.xdist_assignment` prints the newest run and, per worker, the single-process pytest
command over exactly those files in order; add `--worker gw2 --execute` to run it, or set
`BALUFFO_XDIST_ASSIGNMENTS_DIR` to move the records. Child pytest sessions a test spawns are
recorded as well, under `nested/` and with the spawning test named, but left out of the listing
because a one-test child process is not a run anyone replays; `--all` shows them.

When a sharded run fails, the session prints, after its summary, the failing tests grouped by the
worker that ran them and that worker's replay command. A failure therefore hands you the command
that reproduces it in one process, instead of a lane to re-run and hope the same files share a
worker again.

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
| Complete benchmark with a Chrome DevTools trace | `python scripts/perf_complete.py --chrome-trace C:\path\to\Trace.json.gz` | Same complete benchmark, with Chrome LCP, slow resource, user timing, and long-task evidence folded into optimization targets |
| Chrome DevTools trace summary only | `python scripts/chrome_trace_summary.py C:\path\to\Trace.json.gz` | Console JSON summary of LCP candidates, slow resources, user timing spans, and long main-thread tasks |
| Lightweight page-load audit | `npm run perf:page-load:audit -- --base-url http://192.168.50.61:8877 --pages admin.html,jobs.html` | Console JSON with DOMContentLoaded, LCP element, slow requests, request failures, and known page error text |
| Live bridge profile sample only | `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url http://192.168.50.61:8877` | `_out/perf-complete/live/<timestamp>/bridge-profile/live/` |
| Live bridge profile sample with timeout comparison | `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url http://192.168.50.61:8877 --timeouts 3,10,30` | Same live output, with TCP, first-byte, and full-response timing per timeout |
| Startup bridge A/B experiment | `python scripts/perf_startup_bridge_ab.py --pairs 5 --pages jobs,admin` | `_out/perf-complete/startup-bridge-ab/<timestamp>/startup-bridge-ab-summary.json` |
| Slowest Python tests | `npm run perf:py:timing` | Console output only |
| Isolated discovery sanity benchmark | `npm run perf:discovery:benchmark` | `_out/perf-sanity-discovery/` |
| Packaged Jobs cold/warm startup probe | `npm run perf:startup:cold` / `npm run perf:startup:warm` | `.tmp/packaged-desktop-smoke/` and `data/packaged-desktop-smoke-report.json` |
| Packaged Admin cold/warm startup probe | `npm run perf:startup:admin:cold` / `npm run perf:startup:admin:warm` | `.tmp/packaged-desktop-smoke/` and `data/packaged-desktop-smoke-report.json` |
| Seed Admin benchmark volume from live `/data` | `python scripts/perf_admin_seed.py --from-volume-path <path>` (pass `data`, not the repo root) | `_out/perf-admin-flows/seed-data/` |
| Run Admin route + flow benchmark against seeded container | `npm run perf:admin:flows -- --image <img> --profile pi4-tight --data-volume <path>` | `_out/perf-admin-flows/<run-token>/` |
| Pipeline per-stage benchmark (RSS + CPU per stage) | `python scripts/perf_pipeline_stages.py --image <img> --data-volume <path> [--preset smoke] [--fresh]` | `_out/perf-pipeline/<run-token>/` with `stages.json`, `samples.ndjson`, `report.md`, `FINDINGS.md` |
| Full pipeline cgroup/process memory profile | `python tools/measurements/pipeline/full_pipeline_memory.py --base-url <url> --container <name> --run-dir <host-run-dir>` (mount the run directory into the container at `/out`) | `<host-run-dir>/cgroup-process.ndjson`, `progress.ndjson`, `phase.json`, and `meta.json` |
| Fetch-lane CPU per thread, then per function | `python tools/measurements/pipeline/thread_cpu.py --container <name> --run-dir <run-dir>`, then `python tools/measurements/pipeline/cpu_attribution.py --run-dir <run-dir> --wall-clock-seconds <s>` | `<run-dir>/thread-cpu.ndjson`, `thread-cpu-summary.json`, `CPU-ATTRIBUTION.md`, and `cpu-attribution.json` |
| UI geometry review against a running site | `npm run review:ui -- --site http://127.0.0.1:8080` | `_out/ui-review/report.json` plus screenshots: pager cell count and left edges per page, the 430 px pager overflow, Admin busy-button width deltas, and Jobs page screenshots in both themes |

Notes:
- `perf_pipeline_stages.py` reads the persisted `stageLedger` from the terminal lifecycle row. Stage rows include top-level stages (`discovery`, `fetch`, ...) and sub-stage observations from each child task's `taskProgress.phaseKey` transitions (`fetch/loading_state`, `discovery/probing_candidates`). Inside fetch's `loading_state` we emit three finer splits (`loading_state/read_source_state`, `loading_state/seed_redirect_cache`, `loading_state/read_lifecycle_state`) so the per-read cost inside the dominant stage is visible without profiling.
- `perf_admin_seed.py --from-volume-path` expects the *data* directory, not a repo root: pass `data` for this checkout, or the host's mounted `/data` directory for a deployed container. Passing the repo root exits with an explicit error instead of silently copying 0 files. Each whitelisted artifact is accepted as either the plain file or its `.gz` sibling, because a container `/data` keeps them flat while this repo's `data/` stores some registry and feed artifacts gzip-on-disk. The summary prints the copied count and names any whitelisted artifacts absent from the source.
- `tools/measurements/pipeline/full_pipeline_memory.py` is dependency-free development instrumentation for a running Linux container. It copies the sampler into the container, triggers the real pipeline request with `{"jobsPageLoadedCount":0}`, samples cgroup v2/v1 memory and `/proc` process RSS/PSS, and correlates samples with live phase progress. Treat cgroup `memory.peak` and `memory.events` as authoritative; Docker `MemUsage` is only a sampled working-set signal. The sampler does not change runtime behavior or add application dependencies.
- GameDevMap recovery-cache scope defaults to `"batch"`: the in-memory recovery-body cache is cleared at each batch boundary while retaining wave-local reuse. Set `gamedevmap.activeAuditRecoveryCacheScope: "run"` only for an explicit compatibility comparison; the default was promoted after two repeated real-path trials showed a 15.41% mean cgroup-peak reduction with stable source-field parity and zero OOM events.
- Prefer repo-local artifact roots such as `.tmp/` and `_out/` for new perf workflows; avoid `%LOCALAPPDATA%\\Temp` for benchmark or runtime-state outputs in this Windows-first repo.
- Use `npm run perf:complete` when asked for the most complete benchmark. It aggregates discovery/fetch medians, fetch source/provider-board timing and status/cache buckets, frontend boot traces, Jobs and Admin cold/warm packaged startup, packaged sync push/pull timings, sync push detail stages, sync remote-write timing, bridge route/operation profile snapshots, optimization targets, artifact sizes, best-effort process-tree RAM, and top process-level RAM contributors.
- When user-visible page load is the concern, include a Chrome DevTools Performance export with `--chrome-trace`. The backend route profile is useful supporting evidence, but Chrome LCP/resource/user-timing rows are the primary signal for page-perceived delays.
- Use `npm run perf:page-load:audit -- --base-url <url>` as a quick browser smoke before or after a trace. It is not a substitute for a Chrome Performance export, but it catches obvious regressions such as slow first requests, Fetcher/Discovery text becoming the LCP element, `Could not load admin overview`, and missing packaged sync config copy.
- `npm run review:ui -- --site <url>` is the manual, read-only geometry driver for the shared UI primitives work in [`plans/shared-ui-primitives-plan.md`](plans/shared-ui-primitives-plan.md). It needs a running site with a loaded jobs feed, drives the served `pagination.js` and `syncAdminBusyUi()` from inside the page, and writes `_out/ui-review/report.json` plus screenshots. It is evidence, not a gate: it does not run in CI, and S1-S4 pass/fail lives in `node --test` unit tests.
- Use `python scripts/perf_complete.py --bridge-base-url <url>` when route-level timing from a running desktop or container bridge needs to be correlated with local benchmark evidence. This is read-only and failure is recorded as evidence, not as a benchmark failure. Default `npm run perf:complete` stays local and reproducible.
- Use `python scripts/perf_bridge_profile_snapshot.py --bridge-base-url <url>` for a quick live-only read-only sample without running the packaged startup, frontend, discovery, fetch, and sync benchmarks. Add `--timeouts 3,10,30` when debugging LAN reachability so the output separates TCP connect, first-byte, and full-response timing for each endpoint.
- Use `python scripts/perf_startup_bridge_ab.py --pairs 5 --pages jobs,admin` only when evaluating whether `BALUFFO_STARTUP_PARALLEL_BRIDGE=1` should become the desktop default. Promotion requires complete default and parallel samples for both Jobs and Admin, material cold/warm median improvements, no cold-start regression, and no packaged lifecycle smoke regressions.
- Use `/ops/performance-profile` directly for ad hoc route-level timing checks. It is in-memory and bounded; it reports aggregate route and backend operation timings only, with query strings and dynamic path segments redacted.
- Current safe RAM tuning is scoped to Chromium app-mode startup flags. The packaged sync section remains a full-runtime no-browser rehearsal so its RAM numbers stay comparable with earlier complete benchmark reports.
- `npm run perf:discovery:benchmark` is the default discovery perf entrypoint because it keeps artifacts under `_out/`; use `python scripts/benchmark_discovery_probe.py` separately when tuning discovery probe concurrency.
- `npm run perf:admin:flows` starts a Docker container with Pi-class CPU/memory caps against a seeded Admin `/data` volume, then samples every Admin-facing GET route (cold + warm), runs a small set of reversible mutations with runtime-DB rollback, and drives composite flows (`admin.open.cold/warm`, `admin.sources`, `admin.conflicts.drill`, `admin.fetcher.trigger`, `admin.sync.ready`) including a 250 ms `/ops/task-live/fetch?view=summary` poll. Output includes `routes.json`, `flows.json`, `report.md`, and `meta.json`. Pair with `python scripts/perf_admin_seed.py --from-volume-path <path>` to refresh the volume from a live deployed host; seed data stays out of git.
- Fetch-lane CPU attribution is opt-in and stdlib-only. `BALUFFO_PROFILE_SAMPLE=<ms>` gates an all-threads sampler inside the fetch child (`src/shared/profile_utils.py`, entered through `src/jobs_fetcher.py`), writing `<data>/perf-profiles/cpu-samples-<runId>.json` every 60 s of sampling, and again at a clean exit — the pipeline kills the fetch child, so treat the periodic dumps as the record and read the report's coverage line before trusting the tail. Unset means one env lookup and no thread, so production runs are untouched — do not set it on a release or deployed host. `thread_cpu.py` samples the same child per thread from outside through `/proc` as an independent check; start it *before* the pipeline POST, because the fetch child does not exist until the run starts.
- Run that attribution against a bench seat with the documented acceptance call — the real `POST /tasks/run-jobs-pipeline` with `{"jobsPageLoadedCount":0}`, and no preset, only-source, worker, browser-fallback, or fetch-size override — and aggregate with `--wall-clock-seconds` set from the run's own elapsed time and `--cpu-quota` matching the seat's CPU cap (default `2.0`). Artifacts land under `_out/perf-runs/<token>/`, with the profile files under the seed volume's `perf-profiles/`. The report ends in a computed shape (`latency-bound`, `single-thread-transport-bottleneck`, `thread-contended-python-work`, or `inconclusive-no-cpu-weighting`), so it is evidence, not a verdict on a change: schedule it or run it on `workflow_dispatch` rather than in per-PR CI.
- The hermetic fetch lane lives in `tools/measurements/corpus/` — record one real run into a content-addressed corpus (`run_fetch.py --mode record`) and replay it with no network (`--mode replay`), then `determinism.py` (3 samples, identical rows, wall-clock spread, plus the two D1 instrumentation ceilings), `ab.py run --pairs 6` (paired alternating arms with a 95% CI and an explicit `inconclusive`; `--resume` rebuilds finished pairs after an interruption instead of re-running them), `explain.py --run <token>` (rank bottlenecks from any past run) and `adjudicate.py` (S0's verdict on the shipped pool claim). Replay arms a socket-level guard where every outbound connect raises: `blockedRemote == 0` is the egress bar, loopback and asyncio's self-pipe are counted separately, and a corpus miss or a parity divergence fails the run rather than producing a plausible report — a drift is accepted as `parity-bound` only when every differing row is attributed to a host the epoch's own conflict report declared (`binding.py`), and refused the moment one differing source is outside that set. Replay measures the engine, not the wire, over the `force_refresh_all` full-fetch workload — treat an absolute replay wall clock as a harness figure, never a production one. `BALUFFO_FETCH_COUNTERS=1` emits the per-source counters the A/B's retry-exposure line reads; it is off by default and aggregated per source, never per request. Do not add `pytest-benchmark` or `py-spy` by default here. If dependency approval happens later, benchmark deterministic Python leaf logic first and keep desktop startup analysis on the existing startup-trace pipeline. Normalisation changes are priced on the same lane without a second tree: `BALUFFO_TEXT_NORMALIZATION=legacy` runs the pre-pass spellings and bypasses the memo caches (`src/jobs/text_utils.py`, `src/jobs/adapters/parsers/location.py`, `src/jobs/adapters/location_rules.py`) as the A/B's control arm, unset is production, and `tests/test_text_normalization_equivalence.py` pins both modes to identical outputs — see [the pass receipt](snapshots/fetch-pipeline-normalization-pass-2026-10-08.md). A candidate change is *priced* before it is committed, too: `tools/measurements/corpus/probes/` holds dev-only ablation payloads that ride one arm through `PYTHONPATH` (`sitecustomize.py` is imported before the driver runs), so `src/` stays byte-identical, both arms get the same path, and only the candidate sets the ablation variable. Each payload writes a per-process marker (calls, builds, mismatches) and raises rather than degrading, because a probe that silently failed to apply would report a confident zero — and its arms' parity line is the end-to-end equivalence check — see [the pricing receipt](snapshots/fetch-pipeline-next-optimisation-price-2026-10-10.md).

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

The audit resolves the checked-in `requirements-lock.txt` for the image's own target — CPython 3.13 on manylinux x86_64, as named in `Dockerfile` — and fails if a pin has no distribution that target can install, so the result describes the shipped image rather than the developer's machine. Those declared pins are then audited with `pip-audit` in `--no-deps --disable-pip` mode, which does no local install and no re-resolution, and writes a JSON report to `.tmp/security/pip-audit.json`; any unallowlisted advisory fails the lane. CI runs this lane after the pre-commit guardrails in the lint workflow.

## JavaScript dependency security audit

Run the npm dependency vulnerability audit with:

```bash
npm run security:js
```

The audit runs `npm audit --json` against the checked-in `package-lock.json` (dev transitives included — the js-yaml/smol-toml/@humanfs advisories all rode tooling chains), writes a JSON report to `.tmp/security/npm-audit.json`, and fails on any advisory at severity moderate or above unless its id is accepted in the expiry-enforced `tools/security/npm-audit-allowlist.json`. The gate fails closed on missing npm or unparsable reports. CI runs it in the lint workflow on every PR and main push, and `npm run release:preflight` verifies it locally before tagging.

The same audit runs locally through the pre-commit gate: a `pip-audit` local hook fires on commit whenever `requirements-lock.txt` or `tools/security/pip-audit-allowlist.json` changes, and on every push via the full-repo gate (which always includes the lock file, so a new advisory against unchanged pins is caught before it reaches CI). If `pip-audit` is not installed locally, the gate fails with install guidance (`python -m pip install pip-audit==2.10.0`).

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

- Normal push to `main` **or a `v*` release tag**: runs `npm run lint:precommit:ci`, then `npm run lint:deadcode:js`, then `npm run test:py:ratchets`. Tag gating matters because a tag publishes the GitHub release, the signed desktop update manifest, and the GHCR image. Feature branches and `refs/tags/rollback-*` are deliberately not gated; `test_pre_push_hook_gates_release_tags` in `tools/repo_health/workflow_policy.py` pins that distinction by executing the hook's real ref-detection block against four ref shapes, so it is part of `repo guardrails: workflow` rather than the pytest suite.

The third step exists because the first two execute no test at all: pre-commit, pip-audit and repo guardrails inspect the tree, and knip inspects JS, so a live-tree pytest scan could only fail after the push, in CI's Tests job. That is how the int-coercion ratchet named three `int(<bare name>)` handlers that let `float("inf")` escape and left `main` red for three pushes while every local gate was green. `scripts/static_ratchet_gate.py` runs only the modules whose assertions read the checked-out tree -- the AST scans, the docs-versus-registry audits and the git-read version policy -- which is 98 tests in about 16 s of pytest (roughly 22 s including interpreter start) rather than the developer lane's four minutes. Modules that drive their guard against a temp fixture belong in the suite, not here, and the three redundant or non-hermetic exclusions (`test_repo_guardrails.py`, `test_rule_list_guardrail.py`, `test_workflow_syntax_policy.py`) are documented in the script.
- Optional full local CI gate: set `PRE_PUSH_FULL_CI=1` or run `npm run prepush:full`. It skips the standalone `test:py:ratchets` step, because `npm run test:py` in that mode already selects the same modules; a failing ratchet still blocks the push, from the full run, and the step prints that it was skipped.
- Optional hook warmup: set `PRE_PUSH_WARM_HOOKS=1` or run `npm run prepush:warm`.
- Optional timing CSV log: set `PRE_PUSH_TIMING_LOG=1`.
- Optional custom timing log path: set `PRE_PUSH_TIMING_LOG_PATH=<path>`.

The hook emits lightweight console timing lines in this format:

```text
[timing] phase=<name> status=<code> elapsed_ms=<n>
```

Measured baseline on the primary Windows development machine, re-measured 2026-09-29 after mypy/eslint/vulture moved to the commit path and `v*` tags became gated:

- Commit gate (`lint:precommit:changed`): about `35s` (was about `24s` before the pre-push hooks joined it; ~+10.5s)
- Default `main` **or `v*` tag** push gate: `115-130s` (`lint:precommit:ci` `89-104s` across the two 2026-10-29 runs, `lint:deadcode:js` about `2s`, and `test:py:ratchets` about `22-24s`)
- Full local CI mode: about `109s`; it skips the standalone ratchet step, whose modules its `test:py` run already contains
- Warmup path after environments exist: under `1s`

The commit gate's guardrail half is filtered by path. A commit touching `src/`, `frontend/`, `tests/`, `scripts/`, `.github/`, `.githooks/`, `docs/` or one of the watched root files runs all sixteen groups (about `31s` measured); a shipped path outside that set (`Dockerfile`, `styles/`, runtime `data/`) runs only the `release` group, which is the container version gate every commit needs. `.githooks/` joined the watched roots because the `workflow` group asserts on the hook scripts, so a hook-only commit used to reach the push gate before any group had judged it.

The main source of the “first push timed out” complaint is usually hook environment bootstrap or accidentally using the full local CI path as the default push gate. Warm hooks once after a new clone, Python switch, or cache reset to avoid the first-run pre-commit spike:

```bash
npm run prepush:warm
```

If an external launcher or local automation wraps `git push origin main` with a hard timeout, keep the default hook behavior unchanged and raise only that outer timeout to at least `180s`. No tracked repo-side `git push` wrapper currently enforces a `120s` timeout, so any remaining cap is outside the repository hook source.

## Test layout and fixtures

The Python suite is fully pytest (no `unittest.TestCase`). All tests are plain `def test_*` functions.

**Targeted runs:**

| Goal | Command |
|------|---------|
| Developer lane | `npm run test:py` |
| Full suite / release lane | `npm run test:py:extended` |
| Release preflight | `npm run release:preflight` |
| Local pre-commit gate | `npm run lint:precommit:changed` |
| Full pre-commit sweep (local and CI; identical command) | `npm run lint:precommit:ci` |
| Repository policy guardrails | `npm run lint:repo-guardrails` |
| Frontend unit tests | `npm run test:frontend:unit` |
| Live-tree static ratchets (pre-push) | `npm run test:py:ratchets` |
| Python dependency security audit | `npm run security:python` |
| JavaScript dependency security audit | `npm run security:js` |
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
| Update-panel divergence gate fault-injection lane | `npm run test:frontend:packaged:update-panel-gate` |
| Packaged sync rehearsal | `npm run test:frontend:packaged:sync-rehearsal` |
| Packaged orphan reclaim rehearsal | `npm run test:frontend:packaged:orphan-reclaim-rehearsal` |
| Packaged browser job rehearsal | `npm run test:frontend:packaged:browser-job-rehearsal` |
| Packaged desktop lifecycle rehearsal | `npm run test:frontend:packaged:desktop-lifecycle-rehearsal` |
| Packaged active-task close rehearsal | `npm run test:frontend:packaged:active-task-close-rehearsal` |
| Packaged task abort and scheduler rehearsal | `npm run test:frontend:packaged:task-abort-schedule-rehearsal` |
| Packaged deterministic Jobs first-run gate | `npm run test:frontend:packaged:first-run` |
| Jobs-page no-Admin packaged smoke gate | `npm run test:frontend:packaged:jobs-pipeline` |
| Admin startup packaged smoke gate | `npm run test:frontend:packaged:admin-startup` |
| Admin active-run packaged smoke gate | `npm run test:frontend:packaged:admin-active-run` |
| Packaged desktop updater rehearsal | `npm run test:frontend:packaged:update-rehearsal` |
| Orchestrated packaged smoke gate | `npm run test:frontend:packaged:orchestrated` |
| Cache-backed packaged Jobs diagnostic | `npm run probe:desktop:startup:cold` |
| Cache-backed packaged Jobs cold-start release gate | `npm run probe:desktop:startup:jobs:cold` |
| Cache-backed packaged Admin diagnostic | `npm run probe:desktop:startup:admin:cold` |
| One file | `python -m pytest tests/<path/to/test_*.py> -q` |
| Admin bridge | `python -m pytest tests/admin/ -q` |
| GameDevMap discovery lane | `python -m pytest -q tests/source_discovery -k gamedevmap`, then `python -m pytest -q tests/source_discovery`, then `npm run lint:precommit` |
| Match developer lane directly | `python -m pytest tests -q -m "not slow and not packaging and not release" --color=no --basetemp=.tmp/pytest/basetemp` |

When a Baluffo task asks for a “fresh build” without saying container, Docker, or GHCR image, treat that as the desktop portable build: `npm run build:portable-exe`.

**Container targeted checks:**

| Goal | Command |
|------|---------|
| Container bridge/static/runtime seeding and CORS policy | `python -m pytest tests/bridge/test_container_runtime.py -q` |
| Container frontend mode and same-origin bridge | `node --test --test-reporter=dot tests/frontend/unit/admin-config.test.mjs tests/frontend/unit/api-client.test.mjs tests/frontend/unit/admin-runtime-bridge-base.test.mjs tests/frontend/unit/container-local-data.test.mjs tests/frontend/unit/desktop-local-data-navigation.test.mjs tests/frontend/unit/local-data-runtime-contract.test.mjs` |
| Container image build | `docker build -t ghcr.io/deathuman/baluffo:local .` |
| Windows/reparse-safe image build fallback | `python scripts/docker_build_clean_context.py --tag ghcr.io/deathuman/baluffo:local` |
| Container sync config packaging | `python -m pytest tests/test_container_packaging.py tests/test_build_container_sync_config.py tests/test_build_sync_app_config.py tests/test_source_sync.py -q` |
| Container smoke | `docker run --rm -p 8877:8080 -v baluffo-data:/data ghcr.io/deathuman/baluffo:local`, then poll `http://127.0.0.1:8877/app/ready`, verify `/tasks/run-jobs-pipeline-status`, and load `http://127.0.0.1:8877/jobs.html`; use `/ops/health` as a heavier diagnostic check |

Prefer the normal `docker build .` command. The clean-context helper builds committed `HEAD` from a temporary `git archive` under `.tmp` and cleans it afterward, so it is only a fallback when live Windows workspace context transfer is blocked by reparse points or similar local artifacts.

Official GHCR publishes embed the portable encrypted GitHub App sync config from GitHub Actions BuildKit secrets. Pull request and local builds without those secrets remain valid container smoke targets, but `/sync/status` will report a missing packaged config in those images.

`Build Container` is path-filtered for branch and pull request changes that only touch docs, tests, or repo-process files. A skipped container workflow is expected for those ignored paths; require container publish evidence for runtime-relevant container changes, version/tag publishes, or manual `workflow_dispatch` publishes.

Use `npm run release:preflight` when you are about to push a release commit, move a release tag, or publish release artifacts. It runs the pre-commit gate, the full Python lane, frontend unit tests, prepares the shared portable EXE once, and then runs the packaged desktop release lanes in canonical order.

**Shared fixtures (where they are defined):**

| Fixture | Location |
|---------|----------|
| `repo_root`, `codex_tmp_root`, `make_test_root` | `tests/conftest.py` |
| `admin_bridge_entrypoint_root` | `tests/admin/conftest.py`, backed by `tests/admin/_helpers.py` |
| `source_sync_test_root` | `tests/source_sync_helpers.py` |
| `workspace_tmpdir(prefix)`, shared temp-root allocation/cleanup helpers | `tests/helpers/temp_paths.py` |
| `ScriptedClock`, `install_time_module`, `EventLog`, `forbid` (structural timing); `cpu_seconds`, `assert_sublinear_growth` (growth ratios) | `tests/helpers/timing.py` |
| Job payload fixture loaders | `tests/helpers/job_fixtures.py`, with reusable data under `tests/fixtures/` |
| Desktop launcher config/session factories | `tests/desktop_app/_helpers.py` |
| Source-discovery local config/runtime helpers | `tests/source_discovery/_helpers.py` |
| Focused GameDevMap source-discovery helpers | `tests/source_discovery/gamedevmap_test_helpers.py` |
| Frontend admin controller factories | `tests/frontend/unit/helpers/admin-controller-test-helpers.mjs` |

**Timing assertions:**

- Do not compare a measured elapsed time to a literal unless the deadline *is* the behaviour under test. `elapsed < N` measures the host: it passes on an idle machine and fails inside a full suite run without saying anything about the code.
- Assert the structure instead. `ScriptedClock` (with `window()` for a measured window) makes durations exact; `install_time_module` scripts `time.perf_counter`/`time.monotonic` inside a module while leaving `time.sleep` real; `EventLog` records admission/completion order so an ordering invariant cannot move with host load; `forbid` fails the test if a path that must be short-circuited is reached at all.
- Where a real bound is the property (an abandoned straggler thread, a client that must close), keep it with at least a 2x margin and say in the docstring why it is temporal, so the next reader does not have to re-derive whether it is a flake risk.
- When the property is proportional cost rather than a deadline, use `assert_sublinear_growth(run, probe_size=..., size=..., tolerance=...)`: `run(n)` builds and runs its own workload of `n` items, the helper takes the smallest of `samples` CPU-time readings on each side (a scheduling pause inflates one reading; host speed cancels between the two) and fails when the larger population costs more than the size ratio times `tolerance`. Give the sizes a wide span -- 4x is a good default -- because linear work reads the ratio itself while work that compares items with items reads its square, so the tolerance sits between them. `cpu_seconds(call)` is the underlying primitive for a bare measurement. Do not hand-roll probe sizes, sample counts or limits: a probe under roughly 62 ms (four 15.6 ms `process_time` ticks on Windows) is refused outright rather than asserted on, and a pair of sizes that sit too close together fails on a quiet host.
- **The gate:** `repo_guardrails.py --group timing` (part of `npm run lint:repo-guardrails`, so it runs at commit) flags any `assert` that compares a `time.perf_counter()`/`monotonic()`-derived value to a literal. A deliberate bound must carry `# timing-ok: <reason>` on the assert or on a comment line directly above it -- the reason is required. The checked-in baseline in `tools/repo_health/timing_assertion_baseline.json` is empty, so every existing bound is declared at the site and any unmarked one is new. Lower bounds (`elapsed >= 0`) are not flagged; a monotonic value only grows, so load cannot make them flake.
- The gate follows the clock, not just the variable name: a value stored in a payload field (`payload["elapsed"] = ...`, `entry.update(elapsed=...)`, `result.elapsed = ...`), collected in a list (`samples.append(...)`), or reduced into a summary (`sum(samples) / len(samples)`, `statistics.mean(samples)`, `max(samples)`) is still flagged where it is compared. A dict key that never held a clock value is not.
- It also crosses helpers defined in the same module: a function returning the duration it measured (or a payload whose `elapsed` key holds it) is summarised as clock-derived when its arguments are, so `elapsed = measure(t0)` asserted in another function, `assert measure(t0) < 1`, and `assert payload()["elapsed"] < 1` are all flagged, including one hop through another helper. A helper whose return does not depend on a clock argument stays clean. The flow stops at an import boundary and at non-literal keys, so a duration returned by a helper in another module still needs the marker or the structural rewrite.

**Temp directory note (Windows sandbox):**

- Prefer repo-local temp fixtures such as `workspace_tmpdir(...)`, `make_test_root(...)`, and local family fixtures for new tests that write runtime state.
- In this environment, direct pytest temp-root creation under `%LOCALAPPDATA%\\Temp` can hit Windows permission errors during setup/cleanup.
- Keep pytest temp roots under `.tmp/pytest`; the repo disables pytest's cacheprovider by default so unreadable `pytest-cache-files-*` debris does not accumulate in the workspace.
- If a narrow bridge test run fails before assertions with tmpdir/tempfile ACL errors, rerun it with a repo-local `--basetemp` or the existing repo-local tempdir shim rather than treating it as a product regression.
- **Only one pytest session may run against this checkout at a time.** This is now **enforced**, not just advised: `tests/conftest.py` takes an exclusive lock at `.tmp/pytest/session.lock` and a second concurrent session exits with code 4 and an explanation naming the holding PID. If you see that message, wait for the first run; if you are certain no run is in flight, delete the lock file. The lock releases on normal exit, and a lock older than six hours is treated as stale and taken over.

  **Mechanism, corrected 2026-10-02.** The earlier explanation here was wrong, and the correction matters because it changes the fix:

  ```
  FileNotFoundError: [Errno 2] No such file or directory:
    '...\.tmp\pytest\pytest-tmp-37b1450302ee4b66aa9bd750143738ec\
     bridge-profile\live\performance-profile.json'
  ```

  `pytest-tmp-<random>` is **not** the `--basetemp`. On pytest 9.1.1 it is pytest's own
  per-session `tmp_path` directory, and it already contains a UUID, so two concurrent
  sessions do **not** collide on its name. What actually removes a live directory is that
  **a pytest session deletes sibling `pytest-tmp-*` directories when it starts** —
  verified by planting `.tmp/pytest/pytest-tmp-SENTINEL123`, running one test, and finding
  the sentinel gone. Two sessions therefore race on the shared `.tmp/pytest` root, and one
  can delete the tree the other is still writing into. The write is a plain
  `Path.write_text`, so the flake is the *harness* tearing a live test's working directory,
  never the test asserting anything wrongly.

  **Three candidate fixes were measured and all were no-ops on pytest 9.1.1**, because
  `tmp_path` no longer derives from any of them:

  | Lever | Result |
  |---|---|
  | `--basetemp=.tmp/pytest/basetemp` (what the npm scripts pin) | ignored; the observed root is the *parent* of the value, so a per-process suffix still resolves to the same shared `.tmp/pytest` |
  | a deeper `--basetemp` (`.../run-<pid>/inner`) | still ignored |
  | `PYTEST_DEBUG_TEMPROOT` | ignored |

  Setting `option.basetemp` from `pytest_configure` was tried and **discarded as a no-op**
  rather than shipped as a fix that does nothing. That is why the answer is a session lock:
  pytest offers no way to move the root, so the collision is prevented instead.

  `test_transient_get_error_retries_with_backoff` was a **different** flake and is now fixed
  (`2e039570`): it monkeypatched `sleep` on the global `time` module, so its assertion
  measured process-wide sleep traffic. See
  [`plans/remaining-work-implementation-plan.md`](plans/remaining-work-implementation-plan.md) §5.

**Discovery audit artifact hygiene:**

- Discovery audits land in repo `data/` unless `activeAuditPath` is pinned: `audit_artifact_path()` (src/source_discovery/audit_config.py) falls back to a hardcoded `Path(__file__).parents[2] / "data"` — ignoring redirected runtime dirs — and `run_directory_audit` always writes the artifact, even on empty/failed scans. `DEFAULT_DISCOVERY_CONFIG` (config.py) pins explicit paths for all four adapters; keep it that way.
- `stage_control.discovery_stage_enabled()` defaults stages to ENABLED when `stageToggles` is absent, so flow tests running `run_discovery` with partial configs run all real stages. Gameprog/gamesmap config resolves via flat `.get` (no default merge), unreachable by `override_discovery_runtime` (tests/helpers/discovery_runtime.py, which pins only sheet/web) — any test passing its own gameprog/gamesmap section must pin `activeAuditPath` itself.
- This `data/` audit pollution already recurred once (Aug 16 fix, Aug 20 reappearance); `gameprog-discovery-audit.json` / `gamesmap-discovery-audit.json` appearing in `data/` after a test run means an unpinned caller snuck in again.

**`data/` directory hygiene (runtime state vs regenerable artifacts):**

- Tracked `data/` files (`git ls-files data/`) are canonical - never delete them in cleanup: `.gitkeep`, `contracts/`, `defaults/`, `adapter-audit-report.md`, `pipeline-audit-report.md`, `release-repeatability-report.md`, `social-sources-config.json`, `source-discovery-config.json`, `source-registry-tombstones.json.gz`. The runtime-owned set below is **untracked** even though it is listed in the next bullet, so it never appears here and must never be re-committed.
- Everything else under `data/` is gitignored (`data/*.json`, `*.csv`, `*.log`, `*.md`, `*.jsonl`, `*.jsonl.gz`, `*.json.gz`, `*.lock`, `*.db`, `local-user-data/`). Among ignored files, only these are **regenerable** one-shot artifacts — the safe cleanup candidates: the five discovery audits (`gameprog-`/`gamesmap-`/`web-search-`/`sheet-directory-discovery-audit.json`, `gamedevmap-active-source-dry-run.json`), `m5-strategic-backlog.json`, `packaged-desktop-smoke-report.json`, `source-policy-recommendations.json`. Real discovery runs rewrite the audit files into `data/` by design (the ops UI reads them there), so their presence alone is not a pollution signal.
- The root pytest session fixture snapshots those five known audit files and fails if a test creates, deletes, or changes one under the repository `data/` directory. Unchanged pre-existing runtime artifacts are allowed; test callers must pin `activeAuditPath` to a repo-local test root.
- Everything else ignored is **live runtime state** the app reads/writes every run — keep it: `baluffo-runtime.db`, `source-registry-*`, `jobs-unified*` feeds, `jobs-fetch-report.json` + `-summary.json`, `jobs-lifecycle-state.json.gz` + archive, `jobs-source-state.json.gz`, `jobs-availability-*`, `jobs-parser-regression-queue.json`, `admin-*` journals, `storage-metrics.jsonl`, `sync-live-task.json`, `sync-timing-history.json`, `source-sync-runtime.json`, `url-patch-manifest.json`, `registry-conflicts-summary.json` + `-full.json` (route caches), `source-approval-state.json`, `social-experiment-review.json`, `runtime/`, `local-user-data/`.

## Packaged artifact ownership

- Direct packaging commands own `dist/` outputs and refresh the convenience mirror at `_out/latest/build/portable`:
  - `npm run build:portable-exe`
  - `npm run build:portable-exe:prepare`
  - `python scripts/build_portable_exe.py`
- Portable EXE builds are content-addressed under `_out/portable-build-cache`; the builder prunes this cache to the five newest entries by default. Use `--cache-retain <n>` or `BALUFFO_PORTABLE_BUILD_CACHE_RETAIN=<n>` when a release/debug run needs a different retention window, and use `python scripts/build_portable_exe.py --force`, `npm run build:portable-exe -- --force`, or `npm run build -- --force` only when you intentionally want to bypass the cache.
- Portable builds must stay self-contained but browser-minimal: `_internal/playwright/driver/package/.local-browsers/` may contain only the `chromium_headless_shell-*` directory required by packaged Playwright `browsers.json`.
- `npm run test:frontend:packaged*`
- `npm run test:frontend:packaged:update-panel-gate`
- `npm run test:frontend:packaged:sync-rehearsal`
- `npm run test:frontend:packaged:orphan-reclaim-rehearsal`
- `npm run test:frontend:packaged:browser-job-rehearsal`
- `npm run test:frontend:packaged:desktop-lifecycle-rehearsal`
- `npm run test:frontend:packaged:first-run`
- `npm run test:frontend:packaged:admin-startup`
- `npm run test:frontend:packaged:admin-active-run`
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
- The `duplication` group is the repo's duplicated-function-body gate. It is stdlib-only (no new dependency) and lives in `tools/repo_health/duplicate_body_policy.py`, keyed on an AST hash of the normalized body so comments do not matter but docstrings do. It flags a body with **>=3 copies averaging >=4 lines** across `src/`, `scripts/`, and `tools/`, ignoring `...`/`pass` stubs and `raise NotImplementedError` declarations. Known groups are recorded in `tools/repo_health/duplicate_bodies_baseline.json` as `{"digest": ..., "max_copies": N}` objects; a bare digest string is rejected, because a count-independent entry would let a baselined body be copied without limit. The gate fails when a baselined body gains another copy, and when a baselined digest no longer describes a qualifying group, so a baseline entry must be deleted as soon as its copies are consolidated. It is **blocking**: `DUP_GATE_ENFORCING` in `repo_guardrails.py` is `True`, so a new duplicate body fails the run. Set that flag back to `False` only as a deliberate, temporary loosening.
- Accepted limits of that gate, so they are not mistaken for bugs: `frontend/` JavaScript is **not** scanned at all (the gate is stdlib-only and parses Python ASTs), and the `MIN_COPIES`/`MIN_AVERAGE_LINES` thresholds mean two-copy duplication and short bodies are invisible by design. It is a floor on the worst duplication, not a complete duplication report.
- Prefer deleting a duplicated body over baselining it. `tools/repo_health/duplicate_bodies_baseline.json` is a ratchet, not a list of accepted debt.
- The `dead-code` group is the repo's dead-branch and leaked-loop-binding gate, stdlib-only in `tools/repo_health/dead_branch_policy.py`. It covers two classes **no other gate can see** — verified against `ruff` (E, F, I, B, UP, and a trial of SIM, RET, PIE, PLR), `vulture --min-confidence=60`, `mypy`, and `eslint`, all of which report nothing for either. (1) `noop-branch`: an `if` whose body and the statement immediately following it return the identical expression, so the condition cannot change the outcome — Python and JavaScript. (2) `leaked-loop-binding`: a name first bound inside a `for` body and read after the loop with nothing rebinding it, so the read sees whichever iteration ran last. Both rules are deliberately narrow: no-op branches require no `elif`/`else` and an identical trailing return, and leaked bindings ignore names shadowed by a lambda or comprehension parameter (that shadowing produced every false positive in the first attempt at the rule) and names bound before the loop, so the ordinary `last_error` accumulator idiom does not fire. `ruff`'s F841 already covers "assigned but never read", so this gate does not duplicate it. Known findings live in `tools/repo_health/dead_branch_baseline.json` as `path:line` strings; the baseline is currently **empty**, because every finding it started with was fixed. The gate fails on a new finding and on a baselined finding that no longer exists. Run `python tools/repo_health/dead_branch_policy.py` to list findings, or `--update` to re-record the baseline.
- Accepted limits of the `dead-code` gate: it is a pattern matcher, not a dataflow analysis. A dead branch whose arms differ by whitespace or an equivalent-but-not-identical expression is invisible, as is a leaked binding read inside a nested closure. It is a floor on two specific defect shapes, not a general dead-code report.
- **`slopo` is not part of the build or CI contract.** It is an optional local similarity scanner (`slopo` on `PATH`, plus the `slopo-review`, `slopo-analyze-one`, and `slopo-analyze-ignore` agent skills). It is deliberately absent from `scripts/toolbelt_check.py` and from every guardrail, so a machine without it loses nothing and no lane depends on it — matching the AGENTS.md rule that toolbelt tools are conveniences, not build/CI requirements. Its state files are local-only: `slopo.db` and `slopo-report/` are in `.gitignore`, and `slopo.ignore.txt` is in `.git/info/exclude` (not committed, because the dismissal hashes embed paths relative to the index root and would be meaningless on another checkout).
- An **empty** `slopo.ignore.txt` — the `#`-comment header with zero hashes — is the correct steady state, not a missing configuration. Each hash dismisses one reviewed cluster, so zero hashes means nothing has been dismissed yet, which is what a clean tree should report. Do not add placeholder hashes to make the file look populated; a stale hash is worse than none, because it suppresses a cluster that has since changed and will re-surface under a new hash anyway.
- `tests/test_shared_utils_coercion.py` holds the behavioural contract for the coercion helpers (including the desktop `_as_int` copies), and `tests/test_shared_utils_coercion_ratchet.py` holds the three static ratchets that keep the defect from spreading. `int(float("inf"))` raises `OverflowError`, which is **not** a `ValueError` subclass, so `except (TypeError, ValueError)` catches `nan` but lets `inf` escape. The name-gated ratchet scans `_INT_HELPER_NAMES` for `int()` without an `OverflowError` handler, and `_KNOWN_OVERFLOW_UNSAFE` is now empty. Because the defect is a property of the *body* and not the name, a second test compares body digests: a guarded helper's body reappearing unguarded under a name the ratchet ignores fails the run. The third is name-independent and ratchets `_UNDECIDABLE_BARE_NAME_INT_SITES`, the set of `int(<bare name>)` sites under a `ValueError`-only handler whose argument cannot be shown statically to be a `str` (a name annotated `int`/`bool`/`str` is excluded). Those entries are **not** claims of bugs — they are sites a static rule cannot decide, and the count must not grow. Prefer routing such a value through `int_or_default` over adding a new entry.
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
- Packaged smoke fails any run whose startup journal has a `desktop_launch_jobs_report_gate` row with `reader: "full-report"` (scenario `startup-jobs-report-gate`). That is the jobs success gate parsing the whole `jobs-fetch-report.json` because its `jobs-fetch-report-summary.json` sidecar was missing, unfinished, or described another run; fix the sidecar rather than the threshold, and note that `--profile-record-only` does not relax this check. The jobs-pipeline lane seeds row artifacts and so also fails with `startup_jobs_report_gate_missing` when no gate row appears, because that row is what makes the reader observable. Any gate journal event also makes the launch halves observable: each process either journals the reader row or a `desktop_launch_jobs_report_gate_skipped` row when it had nothing to read, and the smoke fails with `startup_jobs_report_gate_missing` when only one of them reports (or when the rows carry no `process`), so one process cannot silently stop journaling. The seeded jobs-pipeline lane needs a reader row from both processes rather than a skip, since a skip would mean the fixture never exercised the sidecar path. A launch that journaled neither event never reached the gate; the other startup checks own that failure. A `jobs_row_artifact_quarantine_failed` row fails the run as scenario `jobs-row-artifact-quarantine` (step `jobs-row-artifact-quarantine`, category `jobs_row_artifact_quarantine_failed`), listing every crashed half with its own recorded `error` (one failure, not one per process) instead of treating those halves as missing gate rows; `--profile-record-only` does not relax it either.

## Packaged Smoke CI Diagnostics

- When a packaged smoke lane fails in GitHub Actions, download the run artifacts and inspect the scenario report JSON before relying on truncated console logs.
- Failure summaries must be safe for non-UTF-8 Windows consoles. If Playwright output contains Unicode and the Python smoke wrapper raises `UnicodeEncodeError`, fix the diagnostic printer first because it may be hiding the real packaged runtime failure.

## Update Panel Divergence Gate Contract

- `npm run test:frontend:packaged` fails the Jobs update scenario when the page records `jobs_desktop_update_panel_state_diverged` (the panel open in controller state while it renders hidden, which turns the next toggle click into a close), and when the click-recorder install record is missing, because a zero divergence count without that record is an absence of data rather than a pass.
- `npm run test:frontend:packaged:update-panel-gate` is the anti-rot lane for that gate. It boots the packaged Jobs page, confirms the gate passes on the real page, injects one divergence metric over the same browser to bridge startup-metric route the controller's diagnostics use, and then **requires the gate to reject it** with the event name, reason, and DOM/state payload in the message.
- A green run of `update-panel-gate` is the evidence the release gate still trips; if the gate is narrowed, short-circuited, or the metric event is renamed, that lane fails instead of the release lane passing silently. Never "fix" this lane by removing the injected metric or relaxing the rejection assertion.

## Admin Startup Packaged Smoke Contract

- `npm run test:frontend:packaged:admin-startup` is the packaged Admin first-render gate for the portable desktop runtime.
- It must prove all of the following in packaged mode:
  - `/ops/health` reports `desktopMode: true`,
  - the Admin page records `admin_first_interactive`,
  - the Admin page records `admin_ops_health_first_render`,
  - the bridge badge reaches `Bridge Online`,
  - `#admin-ops-trends` does not remain stuck on `Loading operations health...`,
  - `#admin-source-status` does not remain stuck on `Loading admin overview...`,
  - startup requests use bounded routes, with the compact `/registry/sources?view=table&detail=summary&buckets=pending,active,rejected&includeHiddenPending=0&limitPerBucket=250` source-table request queued before schedule/history fallback and before `/ops/fetch-kpis?view=summary`, `/registry/conflicts?view=summary`, or `/admin/ops-tab-counts?view=summary`; full multi-MiB diagnostics and fixed startup-delay workarounds are not allowed.
- Use this lane for Admin startup, ops-summary payloads, desktop local-data overview, and packaged bridge availability changes.
- For container page-load performance fixes, package/browser startup gates are supporting evidence only. Capture Chrome DevTools Performance traces for Admin cold/warm, Jobs cold/warm, and Jobs-to-Admin navigation before publishing; acceptance is based on Chrome-visible shell/useful-content timing, LCP element, request waterfall, long tasks, and absence of first-load full diagnostics.

## Admin Active-Run Packaged Smoke Contract

- `npm run test:frontend:packaged:admin-active-run` is the packaged Admin active-task gate for the portable desktop runtime.
- It opens `admin.html` in desktop runtime mode, starts the deterministic long-heartbeat fetch smoke task, verifies KPI cards and schedule controls hydrate while active, aborts the task, and checks captured bridge requests for active-run storms or forbidden routes.
- The lane must fail if Admin keeps KPI cards on `Updating while job is running.`, leaves the schedule on `loading schedule...`, shows `Pending Sources` as table-blocked when compact data is available, calls active-run `/ops/storage-health`, calls full `/ops/fetch-report`, fans out to registry summary/source-table routes, or repeatedly hammers `/tasks/run-jobs-pipeline-status` or `/ops/task-state?view=summary`.

### Admin Active-Fetch In-App Browser Proof

- Admin active-fetch recovery work must include a Codex in-app Browser visual proof before release closeout when the user-visible risk is false-empty panels, stale rows, log progress, or heavy-route timeout spam.
- Always create a fresh Browser-managed tab and navigate it explicitly; do not rely on the currently selected tab that the prompt reports.
- Start a local patched bridge-backed runtime with `BALUFFO_PACKAGED_SMOKE_RUNTIME=1`, `BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE=controlled-heartbeat-success`, `BALUFFO_PACKAGED_SMOKE_ADMIN_BOOTSTRAP_FAIL_ONCE=1`, and an isolated `--data-dir`; use `src/dev_admin_supervisor.py --no-browser` on explicit free ports.
- From the Codex Browser runtime, import `scripts/admin_active_fetch_browser_proof.mjs` and run `runAdminActiveFetchBrowserProof({ browser, baseUrl, bridgeBase, expectBootstrapFailOnce: true })`. The helper creates a fresh in-app Browser tab, seeds a local profile and schedule, starts a controlled bootstrap fetch, and writes screenshots plus DOM/console/resource evidence under `_out/admin-active-fetch-browser-proof/`.
- The proof must show Stored Profiles, Pipeline schedule, Source Sync, source-table delayed placeholders, Current Runs, and fetch log/progress are Browser-visible without a reload, and it must not show repeated heavy-route timeout spam while active.
- Active-run route sampling should use finite fixed-attempt probes, not indefinite shell loops. For active-route performance work, sample `/tasks/run-jobs-pipeline-status`, `/ops/task-state?view=summary`, `/ops/task-live/fetch?view=summary`, and `/app/ready`; acceptance should include zero 504s and p95 under the target named in the release plan.
- Full-pipeline fetch-prep smoke must sample from discovery completion through fetch setup at `0s`, `2s`, `15s`, `60s`, and first source execution. Once a fetch child exists, `/ops/task-live/fetch?view=summary` must show a preparation phase such as `loading_state`, `seeding_existing_output`, `selecting_sources`, `applying_exclusions`, or `initializing_runtime`; it must not remain a vague `starting` state for minutes.
- Active fetch smoke should capture `/ops/storage-metrics` before and after the run window. Acceptance requires no active-run `/ops/storage-health` polling, no runaway `jobs-fetch-tasks.json` or summary-sidecar growth, source-execution hot writes bounded to the configured 5s cadence outside phase/terminal transitions, no same-phase full `jobs-fetch-report.json` rewrites, and compact `admin-task-lifecycle.json` rows that omit active source row collections.
- A live deployment's browser smoke only counts after `/ops/health.appVersion` matches the forward patch version; older live evidence is diagnostic only for that recovery.

## Desktop Updater Rehearsal Contract

- `npm run test:frontend:packaged:update-rehearsal` is the packaged `N -> N+1` updater gate for the portable desktop runtime.
- It must prove all of the following:
  - the packaged app can surface an available update and hand off install to `BaluffoUpdater.exe`,
  - the install plan carries the resolved `dataDir` so helper state, downloads, rollback metadata, logs, and success markers stay under the external Windows data root,
  - the helper installs the target portable ZIP and relaunches the target runtime successfully,
  - the target runtime reaches desktop startup readiness and writes the post-install success marker,
  - seeded local profile data, saved jobs, notes, and attachments remain intact across the update.
- Use this lane for updater-helper, desktop session/handoff, release-manifest, Windows packaged data-root migration, and portable runtime mutation changes.

## Packaged AppImage Update Rehearsal Contract

- `npm run test:frontend:packaged:update-rehearsal:appimage` is the packaged `N -> N+1` updater gate for the Linux AppImage. `build-linux` runs it against the AppImage it is about to publish.
- It must prove all of the following:
  - the running AppImage reads `baluffo-desktop-update-manifest-linux.json` and offers the release it names,
  - the download verifies against the manifest digest,
  - the handoff copies `baluffo-updater` out of the read-only mount and closes the app,
  - the helper replaces the launched AppImage file and relaunches it with the recorded launch mode,
  - the relaunched app reports the target version on a ready bridge and records `installState: installed`,
  - the previous build's snapshot is removed, seeded user data survives, and the file on disk hashes to the downloaded build.
- The rehearsal signs its own manifest with a per-run Ed25519 key handed to the app through `BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON`, and serves the release over loopback; it never touches a real release.
- Run it on a Linux host with the AppImage payload: the lane needs `--appimage-extract-and-run`-compatible execution, which is also why CI needs no FUSE.
- Use this lane for AppImage packaging, updater-helper, Linux install-backend, and Linux release-manifest changes.

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
- Container deployment changes: `python -m pytest tests/bridge/test_container_runtime.py -q`, targeted frontend unit checks for container mode, and a Docker smoke build/run when Docker is available
- Container frontend asset changes: `npm run build:container-frontend -- --out-dir .tmp/container-frontend-check`, then verify generated HTML references `/container-assets/assets/*.js` and that the jobs page inlines `early-data.js` ahead of its module script rather than requesting it; desktop/local HTML must still work without `.container-frontend`. The admin page is split: its HTML carries one `modulepreload` for the static chunk and the ops chunk sits unpreloaded under `/container-assets/assets/chunks/`, so a container run must answer both paths (the static route serves nested chunk paths with the same immutable caching). `tests/frontend/unit/container-frontend-bundle.test.mjs` drives the real bundler for the inline hoist, the desktop-only update stub, the hosted composition (no IndexedDB adapter and no close-signal handlers in any page, the bridge client still present), the admin lazy chunk and its preload, the jobs page staying one asset within budget, and the build's refusal to ship a jobs page that lost the hoist tag or a page that reached another runtime's composition; `tests/frontend/unit/desktop-frontend-bundle.test.mjs` asserts the mirror image for the packaged pages (desktop composition, no browser client, close signals intact)
- Desktop ship frontend bundle changes: `node scripts/build_desktop_frontend.mjs --root <dir holding admin.html, jobs.html and saved.html>`, then verify each page loads one asset next to its modules (`frontend/jobs/jobs.<hash>.js`, `frontend/admin/admin.<hash>.js`, `frontend/saved/saved.<hash>.js`), that admin's lazy chunk (`frontend/admin/chunks/admin-<hash>.js`) and jobs' two lazy chunks (the pipeline controller and the desktop update surface, under `frontend/jobs/chunks/`) exist, are not statically imported by their entries, and are the only chunks without a `modulepreload` link, that a ship build records all three bundled pages in `app/version-hashes.json`, and that the build's own guard still passes: each shell (entry plus preloaded chunks) under its `PAGE_BUDGETS` byte budget, and every declared lazy surface inside a lazy chunk rather than the shell (`tests/frontend/unit/desktop-frontend-bundle.test.mjs` asserts both failure modes against the real bundler; also `tests/frontend/unit/admin-ops-lazy-surface.test.mjs`, `tests/frontend/unit/jobs-lazy-surfaces.test.mjs`, `tests/test_desktop_frontend_bundle.py`, `tests/test_build_ship_bundle_frontend_bundle.py`)
- Linux packaging or AppImage changes: `npm run build:linux` then `bash scripts/smoke_test_appimage.sh`
- Packaged sync config, auth portability, or sync release-gate changes: `npm run test:frontend:packaged:sync-rehearsal`
- Packaged desktop supervision, stale-runtime recovery, or launcher self-heal changes: `npm run test:frontend:packaged:orphan-reclaim-rehearsal`
- Packaged Chromium supervision or managed-browser shutdown propagation changes: `npm run test:frontend:packaged:browser-job-rehearsal`
- Desktop-window liveness, owner idle timeout, lifecycle heartbeat, launcher shutdown, or orphan-process cleanup changes: `npm run test:frontend:packaged:desktop-lifecycle-rehearsal`
- Active-task desktop close confirmation or active-work launcher recovery changes: `npm run test:frontend:packaged:active-task-close-rehearsal`
- Packaged task abort route, lifecycle cancel evidence, or recurring Jobs pipeline scheduler changes: `npm run test:frontend:packaged:task-abort-schedule-rehearsal`
- Packaged Jobs first-run, cold empty-state, bootstrap confirm/retry, or popup theme changes: `npm run test:frontend:packaged:first-run`
- Packaged Admin startup, overview, or heavy ops-payload loading changes: `npm run test:frontend:packaged:admin-startup`
- Packaged desktop Admin active-run hydration, schedule controls during active work, KPI placeholders, or abort request-budget changes: `npm run test:frontend:packaged:admin-active-run`
- Packaged updater, desktop handoff, Windows packaged data-root migration, or release-manifest changes: `npm run test:frontend:packaged:update-rehearsal`
- Packaged Jobs update-panel visibility, toggle click path, or desktop startup-metric gating changes: `npm run test:frontend:packaged` followed by `npm run test:frontend:packaged:update-panel-gate`
- Packaged Jobs startup threshold changes: `npm run probe:desktop:startup:jobs:cold`
- Bridge route wiring or task-launch signature changes: focused `tests/bridge/...` plus `tests/test_pipeline_execution_*.py` for worker-path coverage
- Admin task buttons, presets, or busy-state changes: focused frontend unit tests plus the nearest admin bridge payload test
- Contamination or location-quality regressions: targeted fetcher/unit checks around sanitization, canonicalization, or audit helpers

**Test-to-source map:**

| Area | Test path |
|------|-----------|
| Jobs pipeline / jobs_fetcher | `tests/test_jobs_fetcher.py` (integration shim), `tests/test_jobs_fetcher_google_sheets.py`, `tests/test_jobs_fetcher_parsing.py`, `tests/test_jobs_fetcher_provider*.py`, `tests/jobs_static/`, `tests/test_jobs_fetcher_pipeline_*.py`, `tests/test_jobs_fetcher_quality_*.py` |
| Source discovery | `tests/source_discovery/` |
| Admin bridge (registry, runtime, static fallback, sync) | `tests/admin/test_admin_bridge_ops_*.py` |
| Desktop app / launcher | `tests/desktop_app/` |
| Container runtime | `tests/bridge/test_container_runtime.py`, `tests/frontend/unit/api-client.test.mjs`, `tests/frontend/unit/container-local-data.test.mjs`, `tests/frontend/unit/desktop-local-data-navigation.test.mjs`, `tests/frontend/unit/local-data-runtime-contract.test.mjs` |
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

This lane needs the `pytest-cov` plugin, which is **not** pinned in `requirements.txt` /
`requirements-lock.txt` — those carry runtime and shipping tooling only, and adding a test-runner
plugin there would pull it into the runtime pin and the `pip-audit` scope. Install it locally the
same way `pip-audit` is handled above:

```bash
python -m pip install pytest-cov==7.1.0
```

Without it, `--cov=src` fails with `unrecognized arguments`, which reads as a broken test lane
rather than a missing dev dependency. For machine-readable output add
`--cov-report=json:<path>` (and/or `--cov-report=html:<dir>`) so a coverage sweep can be post-processed
without scraping the terminal table. For test-reduction work see
[`test-reduction-policy.md`](test-reduction-policy.md).

## Type Check

The mypy gate is **cross-platform**: `python -m mypy --config-file mypy.ini` must pass on both Windows and Linux (WSL). Each platform's typeshed has mirror-image blind spots — Windows lacks POSIX-only attributes (`fcntl`, `os.killpg`, `pwd`, `os.sysconf`, …), Linux lacks Windows-only ones (`ctypes.windll`, `msvcrt`, …) — so run the command in **both** environments after touching platform-conditional code. `mypy.ini` sets `warn_unused_ignores = True` and the tree carries **zero** suppressions, so any `type: ignore`/`noqa` you add is flagged as unused on one of the two platforms. Both runs must report `Success: no issues found`. mypy is pinned in `requirements-lock.txt`, so a normal `pip install -r requirements-lock.txt` provides it on both platforms. See [`WSL_SETUP.md`](WSL_SETUP.md) (Cross-platform type check) for the exact WSL command; Linux CI also runs the gate via `npm run typecheck:py` in the `build-linux` workflow.

**The local gate runs the `pre-push` hooks too.** `scripts/precommit_gate.py` first invokes the framework with the **default `pre-commit` stage** (`--files …` / `--all-files`, no `--hook-stage`), then runs one `pre_commit run <hook-id> --all-files --hook-stage pre-push` command per entry in its `PRE_PUSH_HOOK_IDS` (`vulture`, `mypy`, `eslint`). Until that second pass existed, the three `pre-push`-only hooks were dead configuration locally, so a green `npm run lint:precommit` said nothing about types — `e696be64` landed two mypy errors on `main` that CI's `Lint` lane caught. `tests/test_precommit_gate.py` pins both the command shape and the hook list against `.pre-commit-config.yaml`, so adding a `stages: [pre-push]` hook without selecting it fails the suite.

Two details worth keeping in mind. `pre-commit run` accepts only a **single** hook id, hence one command per hook. And each of those commands uses `--all-files` rather than the gate's chunked file list because `mypy` and `eslint` both set `pass_filenames: false` and scan the whole repo regardless; passing chunks would multiply the cost by the chunk count. `run_changed` (the commit-time path) runs the file-scoped hooks on the default stage **and then** one `--hook-stage pre-push` command per `PRE_PUSH_HOOK_IDS` entry, so a type or lint error fails the commit instead of surfacing at push time. That is a deliberate change: measured on this tree the three hooks cost 2.5s / 1.0s / 3.1s in isolation but moved the whole commit gate from 24.4s to ~34.9s, so ~+10.5s end to end, because each is a separate `pre-commit` process. Against a ~108s push gate that already ran them, that is cheap enough to close the hole at its source.

Still run `npm run typecheck:py` yourself before pushing Python changes, especially new test files, where `getattr(module, "x", None)` infers as `None` and reads as `"None" not callable`; the gate now catches that class locally, but CI remains the backstop.

`eslint` is enforced by that local gate, which is the same entry point CI and the push hook use: `.github/workflows/lint.yml` runs `npm run lint:precommit:ci`, `.githooks/pre-push` runs the same script, and `release:preflight` runs `lint:precommit` (an alias for it). So a single gate fix propagated to all three surfaces at once; the only bypass is `--no-verify`, which is forbidden. `eslint.config.js` ignores vendored and generated JavaScript (`.venv/**`, `.container-frontend/**`, `_out/**`, `dist/**`, `node_modules/**`, and the agent-tool directories); without those ignores the hook reported 7,118 errors, 7,063 of them from vendored/built files. The tree is at **0 errors** (19 pre-existing `no-unused-vars` warnings), so the hook is a real gate rather than noise — but treat the remaining warnings as a warning budget, not a licence to add more.

`npm run lint:deadcode:js` (knip) now follows that same one-script-per-surface shape, having been the one gate that was CI-only. It is not a `.pre-commit-config.yaml` hook and is not in `PRE_PUSH_HOOK_IDS`, so nothing local could fail a push on it: `Unused exports (6)` sat red on the `Lint` lane for 21 consecutive pushes before `2b2148c8` fixed the six names and `.githooks/pre-push` gained the call. The first of those runs failed an *earlier* step instead (`check_container_shipped_code_version_gate`), and because pre-commit runs before the scan and short-circuits on failure, that masked the knip failure entirely until the version gate cleared on 25/09. Both surfaces now invoke the identical script, so a dead JS export fails before the push rather than after it. Knip is deliberately still **not** part of `release:preflight` — see the local-preflight coverage list in `RELEASE.md`.

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

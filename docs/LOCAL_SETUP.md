# Local Mode Setup

This app runs in local-first mode (no backend required).

## Storage model
- Profiles are stored in `localStorage`.
- Saved jobs and attachment metadata are stored in IndexedDB database `baluffo_jobs_local`.
- Active session profile id is stored in `localStorage` key `baluffo_current_profile_id`.

## Sign-in behavior
- Clicking `Sign in` prompts for a profile name.
- Existing profile names sign into that local profile.
- New names create a profile and sign in immediately.

## Backup and restore
- `Saved Jobs` page includes `Export Backup` and `Import Backup`.
- Backup file format is JSON and profile-scoped.
- Import merges jobs by deterministic `jobKey`.
- Export supports `Include files` toggle:
  - off: notes + attachment metadata only
  - on: includes attachment file contents

## Data contract
Saved job record fields:
- `jobKey`, `title`, `company`, `companyType`, `city`, `country`, `workType`, `contractType`, `jobLink`
- `savedAt`, `updatedAt`
- `applicationStatus` (default `bookmark`)
- `notes` (default empty string)
- `attachmentsCount` (default `0`)

## Administration
- `Admin` page (`admin.html`) shows local profiles and storage usage totals.
- Access is protected by the configured local admin PIN: `1234` by default in `baluffo.config.json`.
- Wiping an account removes profile, saved jobs, notes, and attachments for that user.

## Root config
- Default runtime values now live in `baluffo.config.json`.
- Local machine-only overrides should go in `baluffo.config.local.json`.
- Effective precedence:
  - CLI
  - env
  - `baluffo.config.local.json`
  - `baluffo.config.json`
  - code fallback
- Browser-safe frontend defaults are generated into `frontend-runtime-config.js`.
- If you change `bridge.host`, `bridge.port`, `security.admin_pin_default`, or `security.github_app_enabled_default`,
  run `npm run build:frontend-runtime-config` before testing or packaging browser pages.
- Run `npm run setup:hooks` once per clone to point Git at the tracked repo hook directory in `.githooks/`.

## Future migration note
`local-data-client.js` intentionally keeps a stable abstraction boundary (`window.JobAppLocalData`) so this local implementation can later be swapped to another backend without rewriting page-level UI logic.

## Unified jobs feed generation
- Run `python src/jobs_fetcher.py` to aggregate listings into:
  - `data/jobs-unified.json` (primary feed used by Jobs page modules)
  - `data/jobs-unified.csv` (CSV fallback + inspection)
  - `data/jobs-fetch-report.json` (per-source diagnostics)
- Active source configuration is file-backed:
  - `data/source-registry-active.json` (used by fetcher at runtime)
  - `data/source-registry-pending.json` (awaiting approval)
  - `data/source-registry-rejected.json` (rejected history)
- The fetch runner pulls from:
  - Google Sheets (current curated source + mirror fallback)
  - Remote OK API
  - GamesIndustry HTML
  - Greenhouse, Teamtailor, Lever, SmartRecruiters, Workable, Ashby, Personio, static studio pages
- Social-source loaders are opt-in at the CLI (`--social-enabled`), but bridge-started fetch runs from the Jobs page pipeline action and Admin Fetcher actions enable social by default unless explicitly disabled in the payload.
- If the current run yields zero jobs, the runner keeps the previous `jobs-unified.json` output by default.

## Source discovery and approval
- Run `python src/source_discovery.py` (dynamic mode by default) to discover new candidate sources into:
  - `data/source-discovery-report.json`
  - `data/source-discovery-candidates.json`
  - `data/m5-strategic-backlog.json` (derived M5 review snapshot)
  - `data/source-registry-pending.json` (report-only, no auto-enable)
- Optional flags:
  - `--mode static` to probe only static seed list
  - `--no-web-search` to skip lightweight web search expansion
- Probe concurrency tuning (env vars; defaults benchmarked via `python scripts/benchmark_discovery_probe.py`):
  - `BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL` (default `40`)
  - `BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER` (default `40`)
  - `BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TEAMTAILOR` (default `15`)
  - `BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC` (default `16`)
  For faster runs on reliable networks, try total/provider 50–80 and teamtailor 25–40.
  - Optional: `BALUFFO_SHEET_DIRECTORY_MAX_ROWS` to cap how many game studio sheet rows are considered (no cap by default).
- Run `npm run dev:bridge` or the VS Code task `Launch Baluffo` to start the Baluffo launcher used by `jobs.html` on normal startup:
  - local static site on `http://127.0.0.1:8080`
  - local admin bridge on `http://127.0.0.1:8877`
  - owned browser session teardown for the local workspace
- Expert/manual bridge-only mode is still available with `python src/admin_bridge.py` when you intentionally want the bridge without the Baluffo launcher.
- The bridge exposes localhost admin endpoints used by `admin.html`:
  - `GET /discovery/report`
  - `GET /registry/pending`
  - `GET /registry/active`
  - `GET /sync/status`
  - Local lint and release gates require `pre-commit` and `ruff` in the active Python environment; if they are missing, install them with `python -m pip install pre-commit ruff`.
  - `POST /registry/approve`, `POST /registry/reject`, `POST /registry/rollback`
  - `POST /sync/pull`, `POST /sync/push`
  - `POST /tasks/run-discovery`, `POST /tasks/run-fetcher`
  - `POST /tasks/run-sync-pull`, `POST /tasks/run-sync-push` (preferred for UI task/history tracking)
- Admin task presets:
  - Fetcher: `default`, `incremental`, `retry_failed`, `force_full`, `uncapped`
  - Discovery: `default`, `uncapped`
    - `default` uses the former uncapped-lite discovery behavior
    - `uncapped` is the stronger exploration preset with higher queue caps, while still keeping evidence thresholds, low-evidence probe cap, blocked-domain suppression, and probe concurrency safety
- In the Admin Source Discovery panel, `Auto-approve healthy pending sources after discovery completes` is enabled by default and saved by the bridge as a machine-local preference.
- When that setting is enabled, discovery completion sweeps all currently pending sources and auto-approves only rows that are still healthy and already show jobs (`jobsFound > 0`, or `sampleCount > 0` when `jobsFound` is absent, with no explicit probe/error state). Auto-approval runs before any follow-on discovery auto-sync push check.
- The Admin discovery log now reconnects to already-running discovery tasks on page load/refresh and tails `/discovery/log` live while the task is active.
- If the admin bridge is unavailable, the Admin UI uses a VS Code task fallback only for the standard fetcher run and shows a manual command fallback (`python -m src.jobs_fetcher --social-enabled`). Bridge-only discovery presets such as `uncapped` still require the bridge.

- Optional bridge runtime options:
  - Baluffo launcher:
    - `npm run dev:bridge`
    - VS Code: `Launch Baluffo` / `Stop Baluffo`
  - CLI:
    - `--host`, `--port`, `--data-dir`, `--log-format (human|jsonl)`, `--log-level (info|debug)`, `--quiet-requests`
  - env:
    - `BALUFFO_BRIDGE_HOST`, `BALUFFO_BRIDGE_PORT`, `BALUFFO_DATA_DIR`, `BALUFFO_BRIDGE_LOG_FORMAT`, `BALUFFO_BRIDGE_LOG_LEVEL`
    - `BALUFFO_SYNC_APP_CONFIG_PATH` (optional override for the packaged GitHub App sync config)
  - precedence: `CLI > env > defaults`
  - default file source: `baluffo.config.json` plus optional `baluffo.config.local.json`

### Source discovery package (Python implementation)

- Core modules live under `src/source_discovery/`:
  - `config.py`: paths, global constants, thresholds, default config, studio seeds.
  - `sheet_directory.py`: Google Sheet parsing and sheet-directory candidates.
  - `web_search.py`: HTTP fetch/retry helpers, DuckDuckGo queries, web-search candidates.
  - `provider_patterns.py`: seed-based \"likely provider\" patterns for Greenhouse/Lever/etc.
  - `static_candidates.py`: static-site heuristics (HTML-only jobs pages).
  - `gamesmap.py`: Gamesmap index/detail parsing and Gamesmap candidates.
  - `probe.py`: candidate validation, probe URL selection, job-count parsing.
  - `core.py`: scoring, confidence, queue balancing, fingerprint helpers.
  - `io_runtime.py`: registry IO + endpoint URL helpers.
  - `reporting.py`: logging and candidate-stream merging.
  - `orchestrator.py`: `parse_args`, `run_discovery`, and the end-to-end discovery flow.
- To extend discovery:
  - Add/adjust thresholds or default config in `config.py`.
  - Add new HTML/provider parsing in `web_search.py`, `static_candidates.py`, or `gamesmap.py`.
  - Keep the public surface (`run_discovery`, `discover_gamesmap_candidates`, `probe_candidate`, etc.) stable; tests rely on it.
- Python tests that define the discovery contract:
  - `tests/test_source_discovery.py::test_discovery_report_snapshot_contract`
  - `tests/test_source_discovery.py::test_run_discovery_*`
  - `tests/test_source_discovery.py::test_parse_gamesmap_*` and related helpers.

### GitHub source sync (multi-PC)
- Source sync is now packaged GitHub App based.
- Ship or local package the GitHub App config JSON as `packaging/github-app-sync-config.json`, or override with `BALUFFO_SYNC_APP_CONFIG_PATH`.
- Packaged builds can generate that file automatically when these build-time env vars are set:
  - required: `BALUFFO_SYNC_BUILD_APP_ID`, `BALUFFO_SYNC_BUILD_INSTALLATION_ID`, `BALUFFO_SYNC_BUILD_REPO`
  - required: one of `BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH` or `BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM`
  - optional defaults: `BALUFFO_SYNC_BUILD_BRANCH=main`, `BALUFFO_SYNC_BUILD_PATH=baluffo/source-sync.json`
  - default key protection mode for packaged builds: `BALUFFO_SYNC_BUILD_KEY_DERIVATION=embedded`
- Standard user flow:
  - start the bridge
  - keep Source Sync enabled in Admin
  - startup pull runs automatically
  - use `Pull Sources Sync` / `Push Sources Sync` for manual recovery when needed
- Behavior:
  - bridge does a best-effort pull on startup (non-fatal on failure)
  - admin UI includes manual pull/push actions
  - sync payload includes `active`, `pending`, `rejected`

## Suggested local schedules
- Windows Task Scheduler action:
  - Program/script: `python`
  - Arguments: `src/jobs_fetcher.py`
  - Start in: repository root (`Baluffo`)
- Discovery (daily):
  - Program/script: `python`
  - Arguments: `src/source_discovery.py --mode dynamic`
  - Start in: repository root (`Baluffo`)

## Ship bundle (zip-first)

- Build:
  - `npm run build:ship-bundle`
  - direct Python entrypoint: `python scripts/build_ship_bundle.py --bundle-version 1.2.3`
  - default version source: `src/app_version.py`
- Output:
  - `dist/baluffo-ship`
- Launchers in bundle root:
  - `run-site.ps1`
  - `run-bridge.ps1`
  - `run-all.ps1`
  - `apply-update.ps1`
  - `recover-previous.ps1`
  - `create-support-bundle.ps1`
- Detailed runbook:
  - `docs/RELEASE.md`
- The ship bundle seeds clean runtime files under `data\`; it does not package the repo's local runtime JSON/CSV state.

## Portable executable (Windows)

- Install dependencies:
  - `python -m pip install -r requirements.txt`
- Build:
  - `npm run build:portable-exe -- --bundle-version 1.2.3`
  - direct Python entrypoint: `python scripts/build_portable_exe.py --bundle-version 1.2.3`
  - default version source: `src/app_version.py`
- Output:
  - `dist/baluffo-portable`
  - `dist/baluffo-portable-1.2.3.zip`
- Dedicated desktop entrypoint:
  - `Baluffo.exe`
- Runbook:
  - `docs/RELEASE.md`

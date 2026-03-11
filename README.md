# Baluffo

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Baluffo is a local-first web app for browsing, filtering, saving, and managing game development job listings.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

It combines:
- A multi-source jobs feed pipeline (`scripts/jobs_fetcher.py`)
- A Jobs UI with advanced filters and quick actions (`jobs.html`)
- A Saved Jobs workspace with backup/restore and custom entries (`saved.html`)
- An Admin console for source discovery, approvals, and operations health (`admin.html`)

## Features

- Aggregated jobs feed from multiple sources into:
  - `data/jobs-unified.json`
  - `data/jobs-unified.csv`
  - `data/jobs-fetch-report.json`
- Jobs filtering by work type, country/region, city, sector, profession, and text search
- Region-aware country filter (e.g. Europe, North America, South America, Asia, Africa, Oceania)
- Saved jobs with profile-based local auth
- Custom saved jobs with notes, reminders, and optional attachments
- Backup and restore (JSON or ZIP when attachments are included)
- Admin source lifecycle:
  - run discovery
  - review pending sources
  - approve/reject/restore sources
- Operations visibility:
  - bridge status
  - fetcher/discovery logs
  - run history and ops health
- Theme toggle (light/dark)

## Tech Stack

- Frontend: plain HTML/CSS/JavaScript with native ES modules (no framework)
- Local storage:
  - `localStorage` for profile/session state
  - IndexedDB for saved jobs/attachments metadata
- Data/ops scripts: Python

## Project Structure

```text
.
|- index.html                 # Compatibility redirect to jobs.html
|- jobs.html                  # Canonical jobs app entry
|- saved.html                 # Saved jobs page
|- admin.html                 # Admin page
|- frontend/                  # ES module entrypoints + page architecture layers
|- local-data-client.js       # Local auth/storage provider
|- jobs-state.js              # Shared filter labels/config
|- data/                      # Feed outputs, source registries, reports
|- scripts/
|  |- jobs_fetcher.py         # Build unified jobs feed
|  |- source_discovery.py     # Discover candidate sources
|  `- admin_bridge.py         # Local admin HTTP bridge
`- tests/                     # Python test suite
```

## Architecture Map

- `frontend/*/app.js`: page orchestration (events, state flow, service calls)
- `frontend/*/domain.js`: pure transformation/business rules
- `frontend/*/data-source.js`: async fetch/read envelopes
- `frontend/*/render.js`: HTML/DOM composition
- `frontend/shared/ui` and `frontend/shared/data`: reusable cross-page helpers

## Getting Started

### 1) Requirements

- Python 3.10+ (recommended)
- Node.js (only needed for JS syntax checks in validation)

Windows commands below use `py -3` so the project does not accidentally resolve to Python 2. On non-Windows shells, use `python3` instead.

### 2) Serve the site locally

```powershell
py -3 -m http.server 8080 --directory .
```

Open:
- `http://localhost:8080/jobs.html`
- `http://localhost:8080/index.html` (compatibility redirect)
- `http://localhost:8080/saved.html`
- `http://localhost:8080/admin.html`

### 3) Generate or refresh jobs feed

```powershell
py -3 scripts/jobs_fetcher.py
```

### 4) Run source discovery (optional)

```powershell
py -3 scripts/source_discovery.py --mode dynamic
```

### 5) Run admin bridge (for Admin discovery actions)

```powershell
py -3 scripts/admin_bridge.py
```

Optional runtime overrides:

```powershell
$env:BALUFFO_DATA_DIR = "C:\baluffo\data"
py -3 scripts/admin_bridge.py --host 127.0.0.1 --port 8877 --log-format human --log-level info
```

Runtime config precedence:

- CLI
- env
- `baluffo.config.local.json`
- `baluffo.config.json`
- code fallback

The committed root `baluffo.config.json` is now the default source of truth for bridge, sync, storage, security, and desktop defaults. Use `baluffo.config.local.json` for machine-local overrides that must not be committed.

Browser-safe frontend defaults are generated into `frontend-runtime-config.js`. If you change
`bridge.host`, `bridge.port`, `security.admin_pin_default`, or `security.github_app_enabled_default`,
regenerate that file with:

```powershell
npm run build:frontend-runtime-config
```

Supported env vars:

- `BALUFFO_BRIDGE_HOST`
- `BALUFFO_BRIDGE_PORT`
- `BALUFFO_DATA_DIR`
- `BALUFFO_BRIDGE_LOG_FORMAT`
- `BALUFFO_BRIDGE_LOG_LEVEL`
- `BALUFFO_SYNC_APP_CONFIG_PATH` (optional override for packaged GitHub App config JSON)

CLI/env precedence: `CLI > env > defaults`.

Source sync API (admin bridge):

- `GET /sync/status`
- `POST /sync/pull`
- `POST /sync/push`
- `POST /tasks/run-sync-pull` (preferred for UI/task history)
- `POST /tasks/run-sync-push` (preferred for UI/task history)

Notes:
- `/sync/pull` and `/sync/push` remain supported as direct synchronous APIs.
- Task endpoints start async runs that appear in Ops Run History (`type: sync`).
- Source sync now uses packaged GitHub App credentials plus a local enabled/disabled toggle; end users do not enter PATs.
- Ship and portable builds can auto-generate `packaging/github-app-sync-config.json` when build-time env vars are set:
  `BALUFFO_SYNC_BUILD_APP_ID`, `BALUFFO_SYNC_BUILD_INSTALLATION_ID`, `BALUFFO_SYNC_BUILD_REPO`,
  and one of `BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH` or `BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM`.

Snapshot schema (`source-sync.json`, v1):

- `schemaVersion`, `generatedAt`, `source`
- `active[]`, `pending[]`, `rejected[]`

### 6) Build ship bundle (zip-first)

```powershell
npm run build:ship-bundle
```

Optional version:

```powershell
py -3 scripts/build_ship_bundle.py --bundle-version 1.2.3
```

Bundle output: `dist/baluffo-ship` with launcher scripts:

- `run-site.ps1`
- `run-bridge.ps1`
- `run-all.ps1`
- `apply-update.ps1`
- `recover-previous.ps1`
- `create-support-bundle.ps1`

Release guide: `docs/RELEASE.md`.

## Local Auth and Storage Model

- Sign in is local profile based (name prompt), no remote backend required
- Session key: `baluffo_current_profile_id` (localStorage)
- IndexedDB database: `baluffo_jobs_local`
- Export/import available from Saved Jobs page
- Admin panel is protected by the configured local admin PIN (`1234` by default from `baluffo.config.json`)

## Portable Executable (Windows)

Baluffo can also be wrapped as a portable Windows desktop executable using `pywebview` + `PyInstaller`.

Install desktop build dependencies:

```powershell
py -3.13 -m pip install -r requirements-desktop.txt
```

Build:

```powershell
npm run build:portable-exe -- --bundle-version 1.2.3
```

If `packaging/github-app-sync-config.json` is not already present, the build will generate it from the
same `BALUFFO_SYNC_BUILD_*` env vars used by `scripts/build_ship_bundle.py`.

Desktop defaults like bridge/site ports, title, and WebView2 flags now come from `baluffo.config.json`
unless overridden by CLI or env.

Optional custom icon override:

```powershell
py -3.13 scripts/build_portable_exe.py --bundle-version 1.2.3 --icon C:\path\to\Baluffo.ico
```

The desktop executable build currently targets Python 3.13 on Windows. In this project environment, Python 3.14 fails to install `pywebview` because `pythonnet` wheel build fails; that leads to an EXE that starts but cannot launch the desktop webview.

Output:

- `dist/baluffo-portable`
- `dist/baluffo-portable-1.2.3.zip`

Portable layout:

- `Baluffo.exe`: dedicated desktop app window
- `ship\`: embedded zip-first runtime bundle
- `ship\data\`: portable runtime/user data
- `ship\data\local-user-data\`: desktop-only profiles, saved jobs, notes, activity, and attachment metadata/files

The executable starts the local site and bridge internally, waits for readiness, then opens Baluffo in a dedicated window.
Desktop mode now uses a fixed site origin so theme/quick-filter browser state stays stable, while core user data is stored under `ship\data\local-user-data\` instead of WebView-local IndexedDB/localStorage.

If no custom icon is provided, the build generates and embeds a branded default `.ico`.
On Windows, the packaged app now checks for Microsoft Edge WebView2 Runtime at startup and shows an installer prompt if it is missing.

## Validation

### JavaScript syntax checks

```powershell
node --check local-data-client.js
node --check jobs-parsing-utils.js
node --check saved-zip-utils.js
node --check jobs-state.js
node --check admin-config.js
node --check frontend/jobs/index.js frontend/jobs/app.js
node --check frontend/saved/index.js frontend/saved/app.js
node --check frontend/admin/index.js frontend/admin/app.js
node --check frontend/shared/ui/index.js frontend/shared/data/index.js
```

### Frontend smoke regression (Playwright)

```powershell
npm install
npx playwright install chromium
npm run test:frontend:unit
npm run test:smoke
```

### Python tests

```powershell
npm run test:py
```

### Fetcher performance baseline

Compute current performance metrics (latest run + rolling history median):

```powershell
py -3 scripts/fetcher_metrics.py --data-dir data --window-runs 20
```

### Backup E2E validation runbook

Run deterministic desktop file-store backup validation (isolated profile, no real-user data wipe):

```powershell
py -3 scripts/backup_e2e_validate.py
```

Report output:

- `data/backup-validation-report.json`

What it validates:

- Scenario A: JSON export/import (`includeFiles=false`)
- Scenario B: file-inclusive export/import (`includeFiles=true`) with attachment byte hash checks
- Scenario C: duplicate/malformed import handling with warnings

Success criteria:

- top-level report `ok` is `true`
- scenarios A/B have zero mismatches
- scenario B has no attachment hash mismatches
- script exits with code `0`

Troubleshooting hints:

- if `ok=false`, inspect each scenario `mismatches[]` entry for exact key + before/after values
- verify desktop runtime backup contract is still `schemaVersion: 2`
- re-run after cleaning validator temp data dir: `data/.backup-validation-tmp`

## Data and Registry Files

- Unified outputs:
  - `data/jobs-unified.json`
  - `data/jobs-unified.csv`
  - `data/jobs-fetch-report.json`
- Source lifecycle:
  - `data/source-registry-active.json`
  - `data/source-registry-pending.json`
  - `data/source-registry-rejected.json`
- Discovery artifacts:
  - `data/source-discovery-report.json`
  - `data/source-discovery-candidates.json`

## Notes

- This project is optimized for local/personal operation.
- Third-party source reliability may vary (rate limits, anti-bot, temporary failures).
- Always verify critical job details on the original posting.

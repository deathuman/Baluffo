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

## Documentation

- **Architecture Map:** [docs/architecture-ai-map.md](docs/architecture-ai-map.md) - A scan-first architecture guide for AI-assisted coding.
- **Release Process:** [docs/RELEASE.md](docs/RELEASE.md) - A guide to the release process.
- **Ship Bundle Runbook:** [docs/ship-bundle-runbook.md](docs/ship-bundle-runbook.md) - A runbook for the ship bundle.
- **Portable Executable Runbook:** [docs/portable-executable-runbook.md](docs/portable-executable-runbook.md) - A runbook for the portable executable.
- **Deployment and Update Guide:** [docs/deployment-and-update-guide.md](docs/deployment-and-update-guide.md) - A guide to deployment and updates.
- **Versioning Policy and Release Checklist:** [docs/versioning-policy-and-release-checklist.md](docs/versioning-policy-and-release-checklist.md) - The project's versioning policy and release checklist.
- **Jobs Saved Contributor Map:** [docs/jobs-saved-contributor-map.md](docs/jobs-saved-contributor-map.md) - A map of contributors to the saved jobs feature.
- **Fetcher Runtime Contracts:** [docs/fetcher-runtime-contracts.md](docs/fetcher-runtime-contracts.md) - The runtime contracts for the fetcher.
- **Agents:** [AGENTS.md](AGENTS.md) - Information about the agents.
- **Local Setup:** [LOCAL_SETUP.md](LOCAL_SETUP.md) - Instructions for setting up a local development environment.
- **Ship Package:** [SHIP_PACKAGE.md](SHIP_PACKAGE.md) - Information about the ship package.

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

- Start here for AI-assisted scanning: `docs/architecture-ai-map.md`
- `frontend/*/app.js`: page entrypoint that re-exports `frontend/*/app/runtime.js`
- `frontend/*/app/runtime.js`: orchestration root for page flow, state transitions, and service calls
- `frontend/*/app/*.js`: focused modules for feature logic (`feed`, `filters`, `notes`, `attachments`, `auth`, `ops`, etc.)
- `frontend/*/{render,domain,data-source,services,state-sync,actions}.js`: shared page-layer primitives still composed by runtime
- Desktop runtime launcher and contracts: `scripts/ship/desktop_app.py`

## Getting Started

### 1) Requirements

- Python 3.13.x (required for project build/test workflows)
- Node.js (only needed for JS syntax checks in validation)

Direct shell examples below use `python` and expect that executable to resolve to Python 3.13.x.
The npm build/test entrypoints use the Windows launcher explicitly via `py -3.13`.

### 2) Serve the site locally

```powershell
python -m http.server 8080 --directory .
```

Open:
- `http://localhost:8080/jobs.html`
- `http://localhost:8080/index.html` (compatibility redirect)
- `http://localhost:8080/saved.html`
- `http://localhost:8080/admin.html`

### 3) Generate or refresh jobs feed

```powershell
python scripts/jobs_fetcher.py
```

### 4) Run source discovery (optional)

```powershell
python scripts/source_discovery.py --mode dynamic
```

### 5) Run admin bridge (for Admin discovery actions)

```powershell
python scripts/admin_bridge.py
```

Optional runtime overrides:

```powershell
$env:BALUFFO_DATA_DIR = "C:\baluffo\data"
python scripts/admin_bridge.py --host 127.0.0.1 --port 8877 --log-format human --log-level info
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

Optional version override:

```powershell
python scripts/build_ship_bundle.py --bundle-version 1.2.3

Default build version comes from `scripts/app_version.py`.
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

Baluffo can also be wrapped as a portable Windows desktop executable using `PyInstaller` and the desktop launcher runtime.

Install desktop build dependencies:

```powershell
python -m pip install -r requirements-desktop.txt
```

Build:

```powershell
npm run build:portable-exe -- --bundle-version 1.2.3
```

If `packaging/github-app-sync-config.json` is not already present, the build will generate it from the
same `BALUFFO_SYNC_BUILD_*` env vars used by `scripts/build_ship_bundle.py`.

Desktop defaults like bridge/site ports and title now come from `baluffo.config.json`
unless overridden by CLI or env.

Optional custom icon override:

```powershell
python scripts/build_portable_exe.py --bundle-version 1.2.3 --icon C:\path\to\Baluffo.ico

Without `--bundle-version`, portable packaging uses the shared app version from `scripts/app_version.py`.
```

Build/test workflows are standardized on Python 3.13.x for deterministic local and CI behavior.

Output:

- `dist/baluffo-portable`
- `dist/baluffo-portable-1.2.3.zip`

Portable layout:

- `Baluffo.exe`: dedicated desktop app window
- `ship\`: embedded zip-first runtime bundle
- `ship\data\`: portable runtime/user data
- `ship\data\local-user-data\`: desktop-only profiles, saved jobs, notes, activity, and attachment metadata/files

The executable starts the local site and bridge internally, waits for readiness, then opens Baluffo in a dedicated window.
Desktop mode now uses a fixed site origin so theme/quick-filter browser state stays stable, while core user data is stored under `ship\data\local-user-data\` instead of browser-local IndexedDB/localStorage.

If no custom icon is provided, the build generates and embeds a branded default `.ico`.
Packaged desktop launch prefers Chromium app mode with Chrome/Brave and falls back to the default browser when needed.
Edge app mode is disabled by default because some Windows setups crash in that path; set
`BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE=1` only if you explicitly want to opt back into Edge app mode.

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
npm run test:frontend:packaged
npm run test:smoke
```

`npm run test:frontend:unit` is the canonical local and CI entrypoint for frontend unit coverage.
It first verifies that the generated manifest `tests/frontend/unit/all.test.mjs` matches every
`tests/frontend/unit/*.test.mjs` file, then runs the suite in the existing single-process mode.
If you add, rename, or remove a frontend unit test file, refresh the manifest with:

```powershell
npm run sync:test-manifest
```

`npm run test:frontend:packaged` is the canonical local and CI entrypoint for the packaged desktop
release gate. It runs the packaged smoke runner through `py -3.13`, so it does not depend on the
machine-default `python` executable. The packaged smoke runner now drives Playwright from a
single-process Node script and records temp-dir / node-path / elevation diagnostics in the smoke
report to make Windows `spawn EPERM` failures easier to diagnose.

### Python tests

```powershell
npm run test:py
```

`npm run test:py` is the canonical local and CI entrypoint for Python tests. It uses Python 3.13
and `unittest` discovery over `tests/test_*.py`. Helpers and compatibility shims must stay outside
that filename pattern.


### Fetcher performance baseline

Compute current performance metrics (latest run + rolling history median):

```powershell
python scripts/fetcher_metrics.py --data-dir data --window-runs 20
```

### Backup E2E validation runbook

Run deterministic desktop file-store backup validation (isolated profile, no real-user data wipe):

```powershell
python scripts/backup_e2e_validate.py
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
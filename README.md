# Baluffo

Baluffo is a local-first web app for browsing, filtering, saving, and managing game development job listings.

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
├─ index.html                 # Landing page
├─ jobs.html                  # Jobs page
├─ saved.html                 # Saved jobs page
├─ admin.html                 # Admin page
├─ frontend/                  # ES module entrypoints + page architecture layers
│  └─ home/                   # Landing page module entrypoint + app
├─ local-data-client.js       # Local auth/storage provider
├─ jobs-state.js              # Shared filter labels/config
├─ data/                      # Feed outputs, source registries, reports
├─ scripts/
│  ├─ jobs_fetcher.py         # Build unified jobs feed
│  ├─ source_discovery.py     # Discover candidate sources
│  └─ admin_bridge.py         # Local admin HTTP bridge
└─ tests/                     # Python test suite
```

## Architecture Map

- `frontend/*/app.js`: page orchestration (events, state flow, service calls)
- `frontend/home/app.js`: landing page behavior orchestration
- `frontend/*/domain.js`: pure transformation/business rules
- `frontend/*/data-source.js`: async fetch/read envelopes
- `frontend/*/render.js`: HTML/DOM composition
- `frontend/shared/ui` and `frontend/shared/data`: reusable cross-page helpers

## Getting Started

### 1) Requirements

- Python 3.10+ (recommended)
- Node.js (only needed for JS syntax checks in validation)

### 2) Serve the site locally

```powershell
python -m http.server 8080 --directory .
```

Open:
- `http://localhost:8080/index.html`
- `http://localhost:8080/jobs.html`
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

Supported env vars:

- `BALUFFO_BRIDGE_HOST`
- `BALUFFO_BRIDGE_PORT`
- `BALUFFO_DATA_DIR`
- `BALUFFO_BRIDGE_LOG_FORMAT`
- `BALUFFO_BRIDGE_LOG_LEVEL`

CLI/env precedence: `CLI > env > defaults`.

### 6) Build ship bundle (zip-first)

```powershell
python scripts/build_ship_bundle.py
```

Optional version:

```powershell
python scripts/build_ship_bundle.py --bundle-version 1.2.3
```

Bundle output: `dist/baluffo-ship` with launcher scripts:

- `run-site.ps1`
- `run-bridge.ps1`
- `run-all.ps1`
- `apply-update.ps1`
- `recover-previous.ps1`
- `create-support-bundle.ps1`

Detailed runbook: `docs/ship-bundle-runbook.md`.
Deployment/update procedure: `docs/deployment-and-update-guide.md`.
Versioning policy + release checklist: `docs/versioning-policy-and-release-checklist.md`.

## Local Auth and Storage Model

- Sign in is local profile based (name prompt), no remote backend required
- Session key: `baluffo_current_profile_id` (localStorage)
- IndexedDB database: `baluffo_jobs_local`
- Export/import available from Saved Jobs page
- Admin panel is protected by a local PIN (`1234` by default in current local setup)

## Validation

### JavaScript syntax checks

```powershell
node --check local-data-client.js
node --check jobs-parsing-utils.js
node --check saved-zip-utils.js
node --check jobs-state.js
node --check admin-config.js
node --check frontend/home/index.js frontend/home/app.js
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

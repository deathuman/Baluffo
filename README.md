# Baluffo

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Tests](https://github.com/deathuman/Baluffo/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/deathuman/Baluffo/actions/workflows/test.yml) [![Lint](https://github.com/deathuman/Baluffo/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/deathuman/Baluffo/actions/workflows/lint.yml) [![Build Portable EXE](https://github.com/deathuman/Baluffo/actions/workflows/build-portable-exe.yml/badge.svg)](https://github.com/deathuman/Baluffo/actions/workflows/build-portable-exe.yml) [![Build Linux](https://github.com/deathuman/Baluffo/actions/workflows/build-linux.yml/badge.svg)](https://github.com/deathuman/Baluffo/actions/workflows/build-linux.yml) ![Platform](https://img.shields.io/badge/Platform-Windows%20%2B%20Linux-blue) ![Tech](https://img.shields.io/badge/Tech-Python%2BJS-orange)

## What is Baluffo? 🚀

Baluffo aggregates game development job listings from across the web into one fast, local-first desktop app for Windows and Linux. No cloud account, no required sync, no hassle — just a better way to find your next game dev role.

🟢 **Local-first** &nbsp; 🔒 **No cloud account** &nbsp; 📦 **Windows ZIP/EXE + Linux AppImage** &nbsp; 🌙 **Dark mode** &nbsp; ✨ **Fully vibe coded**

---

## Quick Start

1. **Download** Baluffo from [Releases](https://github.com/deathuman/Baluffo/releases)
   - Windows: `baluffo-portable-<version>.zip`
   - Linux: `Baluffo-<version>-x86_64.AppImage`
2. **Run**
   - Windows: extract the ZIP and double-click `Baluffo.exe`
   - Linux: `chmod +x Baluffo-<version>-x86_64.AppImage`, then `./Baluffo-<version>-x86_64.AppImage`
3. **Browse** — filter by region, work type, or search text, then save jobs you like

That's it. Your data stays on your machine. Windows packaged data lives under `%APPDATA%\Baluffo`; Linux follows XDG paths. See [Local Setup](docs/LOCAL_SETUP.md) and [WSL/Linux Setup](docs/WSL_SETUP.md) for development and platform details.

---

## Screenshots

| Jobs Page | Saved Jobs | Admin |
|:---:|:---:|:---:|
| ![Jobs](docs/screenshots/jobs.png) | ![Saved](docs/screenshots/saved.png) | ![Admin](docs/screenshots/admin.png) |

---

## Features

| 🔍 Smart Filtering | 💾 Save & Track | ⚡ Fast & Local | 🎮 Game Dev Focus |
|---|---|---|---|
| Filter by region, work type, city, sector, profession, and text search | Save jobs with notes, reminders, activity history, and custom entries | Desktop data is local-first and bridge-backed, with browser-mode fallbacks | Curated for game industry roles from studios worldwide |

| 🌍 Region-Aware | 📦 Backup & Restore | 🛠️ Source Management | 🖥️ Desktop Releases |
|---|---|---|---|
| Filter by continents: Europe, North America, South America, Asia, Africa, Oceania | Export to JSON or ZIP, import anytime — your data stays yours | Discover new sources, approve/reject in Admin panel | Windows portable builds, Linux AppImages, and desktop update support |

---

## For Developers 🛠️

<details>
<summary>Click to expand — technical details</summary>

### Tech Stack

- **Frontend:** plain HTML/CSS/JavaScript with native ES modules (no framework)
- **Backend:** Python bridge, jobs pipeline, source discovery, and release tooling
- **Desktop:** Windows portable EXE/ZIP and Linux AppImage built from the ship bundle
- **Storage:** browser-mode localStorage + IndexedDB; desktop-mode bridge-backed file storage

### Quick Development Setup

```powershell
# Serve locally + start admin bridge (recommended)
npm run dev:bridge

# Generate jobs feed
npm run dev:pipeline
```

### AI Tooling

- Serena MCP is the required AI dev tool for Baluffo repo work.
- Basic Memory MCP is the required AI continuity memory for planning, handoff, gotchas, and stale-memory corrections.
- Codex CLI and OpenCode are the first-class client paths.
- Setup guides: [Serena MCP Setup](tools/mcp/SERENA.md), [Basic Memory MCP Setup](tools/mcp/BASIC_MEMORY.md)

### Documentation

- [Docs Index](docs/INDEX.md) — navigation hub for the documentation set
- [AI Assistant Guide](docs/AI_ASSISTANT_GUIDE.md) — AI coding workflow and edit routing
- [Docs Workflow](docs/DOCS_WORKFLOW.md) — documentation ownership, freshness checks, and maintenance rules
- [Architecture Map](docs/architecture-ai-map.md) — subsystem boundaries and file routing
- [Testing Guide](docs/testing.md) — verification commands and fixture guidance
- [WSL/Linux Setup](docs/WSL_SETUP.md) — Linux packaging, AppImage, and WSL workflow notes
- [Admin Bridge API](docs/admin-bridge-api.md) — endpoint reference
- [Release Process](docs/RELEASE.md) — build and release guide
- [Scraping Pipeline](docs/scraping-pipeline.md) — Playwright and Scrapy flow
- [Adapter Plugin Inventory](docs/adapter-plugin-inventory.md) — source loaders and static plugins
- [Data Contracts](docs/DATA_CONTRACT.md) — data structures between Python and JS

### Project Structure

```
.
|- *.html                    # Page entry points (jobs.html, saved.html, admin.html)
|- frontend/                  # ES module frontend code
|  |- shared/                 # Shared UI components, desktop/browser local-data clients, config, state hub
|  |- jobs/                   # Jobs browser page and feed/filter runtime
|  |- saved/                  # Saved jobs page, tracking UI, notes, attachments, backup helpers
|  |- admin/                  # Admin console page
|  |- local-data/             # Browser IndexedDB adapter
|- .tmp/                      # Repo-owned temp roots (pytest, Playwright, packaged smoke probes)
|- src/
|  |- jobs_fetcher.py         # Build unified jobs feed
|  |- source_discovery.py     # CLI entrypoint for source discovery
|  |- admin_bridge.py         # Stable local bridge entrypoint and compatibility exports
|  |- bridge/                 # Bridge services, server, route leaves, api.py, ops_api.py
|  |- source_discovery/       # Source discovery package (orchestrator, probe, web_search, etc.)
|  |- jobs/                   # Job pipeline and adapters
|  |  |- adapters/            # Source adapters (static, provider, social)
|  |  |  |- plugins/          # Adapter plugins (provider_api/, social/, static/)
|  |  |- common/              # Jobs package helpers (config, contracts, heuristics, etc.)
|  |- core/                   # Core schemas and contracts (Pydantic models)
|  |- shared/                 # Shared utilities (regex, utils, exceptions)
|  |- scrapers/               # Scrapy runner and spiders
|  |- ship/                   # Desktop packaging, updater, runtime launcher, platform compat
|  |  |- desktop_app/         # Windows/Linux desktop launcher helpers
|- packaging/                 # Desktop release metadata and Linux AppImage assets
|- scripts/                   # Build/orchestration scripts for Windows and Linux packaging
|- probes/                    # Development/testing probes
|- docs/                      # Documentation
|- data/                      # Repo/source-run jobs feed outputs, source registries, local user data
```

### Running Tests

```powershell
npm run verify        # Full build + test suite
npm run test:py      # Python tests
npm run test:py:linux # Linux packaging/compatibility Python tests
npm run test:frontend:unit  # Frontend unit tests
npm run test:frontend:linux # Linux frontend/package smoke checks
npm run build:portable-exe  # Windows portable desktop build
npm run build:linux         # Linux AppImage build
```

### Configuration

- Default config: `baluffo.config.json`
- Local overrides: `baluffo.config.local.json` (not committed)
- Runtime data override: `BALUFFO_DATA_DIR` or `--data-dir`
- Config precedence: CLI → env → local config → committed config

</details>

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Notes

- This project is optimized for local/personal operation
- Third-party source reliability may vary (rate limits, anti-bot, temporary failures)
- Always verify critical job details on the original posting
- Linux AppImage support expects a desktop Linux/WSL environment with AppImage/FUSE support; see [WSL/Linux Setup](docs/WSL_SETUP.md)
- Source registry sync is transition-aware: local delete is tombstone-backed, and remote sync snapshots only carry `active` and `pending` rows.

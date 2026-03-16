# Baluffo

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) ![Platform](https://img.shields.io/badge/Platform-Windows-blue) ![Tech](https://img.shields.io/badge/Tech-Python%2BJS-orange)

## What is Baluffo? 🚀

Baluffo aggregates game development job listings from across the web into one fast, local-first app. No logins, no cloud, no hassle — just a better way to find your next game dev role.

🟢 **Local-first** &nbsp; 🔒 **No login required** &nbsp; 📦 **Portable** &nbsp; 🌙 **Dark mode** &nbsp; ✨ **Fully vibe coded**

---

## Quick Start

1. **Download** the portable EXE from [Releases](https://github.com/your-repo/baluffo/releases)
2. **Run** — double-click `Baluffo.exe`, it opens automatically in your browser
3. **Browse** — filter by region, work type, or search text, then save jobs you like

That's it. Your data stays on your machine.

---

## Screenshots

| Jobs Page | Saved Jobs | Admin |
|:---:|:---:|:---:|
| ![Jobs](docs/screenshots/jobs.png) | ![Saved](docs/screenshots/saved.png) | ![Admin](docs/screenshots/admin.png) |

---

## Features

| 🔍 Smart Filtering | 💾 Save & Track | ⚡ Fast & Local | 🎮 Game Dev Focus |
|---|---|---|---|
| Filter by region, work type, city, sector, profession, and text search | Save jobs with notes, track application status, add custom entries | All data stored locally — no cloud, no waiting | Curated for game industry roles from studios worldwide |

| 🌍 Region-Aware | 📦 Backup & Restore | 🛠️ Source Management | 🌗 Dark Theme |
|---|---|---|---|
| Filter by continents: Europe, North America, South America, Asia, Africa, Oceania | Export to JSON or ZIP, import anytime — your data stays yours | Discover new sources, approve/reject in Admin panel | Toggle between light and dark modes |

---

## For Developers 🛠️

<details>
<summary>Click to expand — technical details</summary>

### Tech Stack

- **Frontend:** plain HTML/CSS/JavaScript with native ES modules (no framework)
- **Backend:** Python scripts for jobs fetching, source discovery, and admin bridge
- **Storage:** localStorage + IndexedDB (browser mode), file-based (desktop mode)

### Quick Development Setup

```powershell
# Serve locally
python -m http.server 8080 --directory .

# Generate jobs feed
python -m src.jobs_fetcher

# Run admin bridge (for discovery/actions)
python -m src.admin_bridge
```

### Documentation

- [Architecture Map](docs/architecture-ai-map.md) — scan-first guide for AI-assisted coding
- [Admin Bridge API](docs/admin-bridge-api.md) — endpoint reference
- [Local Setup](LOCAL_SETUP.md) — development environment setup
- [Release Process](docs/RELEASE.md) — build and release guide

### Project Structure

```
.
|- jobs.html                  # Jobs browser (main entry)
|- saved.html                 # Saved jobs workspace
|- admin.html                 # Source management console
|- frontend/                  # ES module frontend code
|- data/                      # Jobs feed outputs, source registries
|- src/
|  |- jobs_fetcher.py         # Build unified jobs feed
|  |- source_discovery.py     # Discover candidate sources
|  |- admin_bridge.py         # Local admin HTTP API
|- scripts/                   # Build/orchestration scripts
```

### Running Tests

```powershell
npm run verify        # Full build + test suite
npm run test:py      # Python tests
npm run test:unit    # Frontend unit tests
```

### Configuration

- Default config: `baluffo.config.json`
- Local overrides: `baluffo.config.local.json` (not committed)
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
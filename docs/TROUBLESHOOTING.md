# Troubleshooting Guide

> Common issues and solutions for the Baluffo project.

---

## Table of Contents

1. [Frontend Issues](#1-frontend-issues)
2. [Backend/Bridge Issues](#2-backendbridge-issues)
3. [Pipeline & Fetch Issues](#3-pipeline--fetch-issues)
4. [Discovery Issues](#4-discovery-issues)
5. [Desktop & Packaging Issues](#5-desktop--packaging-issues)
6. [Testing Issues](#6-testing-issues)
7. [Data & Storage Issues](#7-data--storage-issues)

---

## 1. Frontend Issues

### Jobs page is empty / shows no jobs

| Possible Cause | Solution |
|----------------|----------|
| No jobs feed generated | Run `python src/jobs_fetcher.py` to generate `data/jobs-unified.json` |
| Bridge not running | Start the Baluffo launcher with `npm run dev:bridge` or VS Code task `Launch Baluffo` |
| CORS errors in browser | Ensure frontend is served from same origin as bridge |

### Admin page shows "Bridge unavailable"

| Possible Cause | Solution |
|----------------|----------|
| Bridge process not started | Start the Baluffo launcher with `npm run dev:bridge` or VS Code task `Launch Baluffo` |
| Wrong port | Check `baluffo.config.json` for bridge port (default 8877) |
| Port in use | Kill existing process or change port in config |

### Saved jobs not persisting

| Possible Cause | Solution |
|----------------|----------|
| Browser mode: IndexedDB issue | Check browser console for IndexedDB errors |
| Desktop mode: file permissions | Ensure `data/local-user-data/` is writable |
| Guest mode | Sign in if you want seen/saved job state to persist; guest browsing is intentionally non-persistent |

### Desktop sign-in creates an unexpected new profile

| Possible Cause | Solution |
|----------------|----------|
| Existing profile list did not load | Use the desktop sign-in `Retry` path first; only choose `Create new profile` when you intentionally want a new local profile |
| Bridge/local data unavailable | Confirm the bridge is running and `data/local-user-data/` is readable/writable |

### UI elements not responding

| Possible Cause | Solution |
|----------------|----------|
| Busy state active | Wait for current operation (fetch/discovery) to complete |
| JavaScript error | Check browser console for errors |
| data-ui attribute missing | Verify element has `data-ui` attribute per [`selectors.js`](../frontend/shared/ui/selectors.js) |

---

## 2. Backend/Bridge Issues

### Bridge returns 404 for all endpoints

| Possible Cause | Solution |
|----------------|----------|
| Bridge not running | Start with `npm run dev:bridge` |
| Wrong host/port | Default is `http://127.0.0.1:8877` |
| Port conflict | Check if another process is using the port |

### Bridge won't start / port already in use

```powershell
# Find process using port 8877
netstat -ano | findstr :8877

# Kill the process (replace <PID> with actual PID)
taskkill /PID <PID> /F
```

If you intentionally want the bridge without the owned site/browser supervisor, use the expert-only bridge command:

```powershell
python src/admin_bridge.py --host 127.0.0.1 --port 8877
```

### Bridge starts but returns errors

| Error | Solution |
|-------|----------|
| "Module not found" | Ensure `PYTHONPATH` includes repo root |
| "Config not found" | Check `baluffo.config.json` exists |
| Import errors | Run `python -m pip install -r requirements.txt` |

### Sync operations fail

| Possible Cause | Solution |
|----------------|----------|
| GitHub App not configured | Run `python scripts/build_sync_app_config.py` |
| `SSL certificate verification failed` | Install/update the machine trust store, or set `BALUFFO_GITHUB_CA_BUNDLE` to a PEM CA bundle that trusts GitHub and any TLS-inspecting proxy. `BALUFFO_SYNC_CA_BUNDLE` still works for sync-only overrides, and `BALUFFO_DESKTOP_UPDATE_CA_BUNDLE` can scope the override to desktop updates only |
| Invalid credentials | Check `packaging/github-app-sync-config.json` |
| Network issues | Check internet connection |

---

## 3. Pipeline & Fetch Issues

### Jobs fetch returns 0 jobs

| Check | Action |
|-------|--------|
| Source registry | Verify `data/source-registry-active.json` has entries |
| Network access | Test with `python src/jobs_fetcher.py --only-sources google_sheets` |
| Report errors | Check `data/jobs-fetch-report.json` for per-source errors |

### Admin says no successful fetch has run yet

| Check | Action |
|-------|--------|
| First desktop run | This is expected until the first successful fetch; use `Run Jobs Fetcher` from Jobs or Admin |
| Alert cannot be dismissed | `fetch_never_run` is intentionally non-dismissible until a successful fetch clears it |

### Specific source fails

```powershell
# Run single source with verbose output
python src/jobs_fetcher.py --only-sources <source_name> --ignore-circuit-breaker

# Check fetch report for error details
type data\jobs-fetch-report.json | findstr /C:"error"
```

### Circuit breaker blocks sources

| Solution | Command |
|----------|---------|
| Run ignoring circuit breaker | `python src/jobs_fetcher.py --ignore-circuit-breaker` |
| Reset source state | Delete `data/jobs-source-state.json` |

### Social sources (Reddit/X/Mastodon) not fetching

| Check | Action |
|-------|--------|
| Social enabled | Ensure `--social-enabled` flag or config |
| Config file | Check `data/social-sources-config.json` |
| Rate limiting | Wait and retry; social APIs have strict limits |
| Stale bad social rows still in `jobs-unified.json` | Run `python scripts\\jobs_fetcher.py --force-refresh-all --social-enabled` so incremental cache skips do not preserve old Reddit/Mastodon contamination |

---

## 4. Discovery Issues

### Discovery finds no candidates

| Check | Action |
|-------|--------|
| Network access | Verify internet access for DuckDuckGo search |
| Sheet ID configured | Check `GAME_STUDIOS_SHEET_ID` in `src/source_discovery/config.py` |
| Probe concurrency | Increase `BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL` |

### Discovery takes too long

| Solution | Notes |
|----------|-------|
| Use `--no-web-search` | Skip lightweight web search expansion |
| Reduce concurrency | Set lower env vars |
| Run in static mode | `--mode static` only probes seed list |

### Source check fails for specific source

```powershell
# Run source check from admin UI or CLI
python -c "from src.bridge.source_check_api import trigger_source_check; print(trigger_source_check({'url': 'https://example.com'}))"
```

---

## 5. Desktop & Packaging Issues

### Portable EXE won't start

| Check | Action |
|-------|--------|
| Python installed | Portable EXE includes Python, but check version compatibility |
| Admin rights | Try running as administrator |
| Antivirus | Check if antivirus is blocking the exe |

### Desktop app opens but shows blank page

| Check | Action |
|-------|--------|
| Bridge ready | Wait for bridge to finish starting |
| Port conflict | Check if another Baluffo instance is running |
| Log files | Check `data/` for startup logs |

### Desktop updater goes back to `Download` after starting or finishing a download

| Check | Action |
|-------|--------|
| Failed background download | Open the updater panel again and confirm whether it now shows the persisted error with `Try download again` |
| Persisted updater state | Inspect `ship\data\updater\install-state.json` for `downloadState`, `installState`, and `lastError` |
| Helper diagnostics | If install handoff starts, inspect `ship\data\updater\desktop-updater-helper.*.log` and `desktop-updater-helper.diagnostics.jsonl` |
| Bad staged ZIP | Delete only the failed file under `ship\data\updater\downloads\` if it remains after a failed attempt, then retry the download |

### Ship bundle launcher fails

| Check | Action |
|-------|--------|
| Version directory exists | Verify `app/versions/<version>` exists |
| current.txt valid | Check `app/current.txt` points to valid version |
| Permissions | Ensure scripts are not blocked by security policy |

### Build fails

```powershell
# Ensure dependencies installed
python -m pip install -r requirements.txt

# Rebuild frontend config
npm run build:frontend-runtime-config
```

---

## 6. Testing Issues

### Python tests fail with import errors

| Solution | Command |
|----------|---------|
| Set PYTHONPATH | `set PYTHONPATH=%CD%` then run tests |
| Install dev deps | `python -m pip install -r requirements.txt` |

### Playwright smoke tests fail

| Check | Action |
|-------|--------|
| Python 3 default | Ensure `python` resolves to Python 3, not Python 2 |
| Bridge running | Start the Baluffo launcher with `npm run dev:bridge` or VS Code task `Launch Baluffo` before running smoke tests |
| Port available | Ensure port 8080 (web server) and 8877 (bridge) are free |

```powershell
# Override Playwright Python
set PLAYWRIGHT_PYTHON=py
npm run test:smoke
```

### Temp directory errors (Windows)

| Solution | Notes |
|----------|-------|
| Use --basetemp | `python -m pytest --basetemp ./temp_tests` |
| Use repo fixtures | Use `workspace_tmpdir()` fixture from `tests/helpers/temp_paths.py` |

---

## 7. Data & Storage Issues

### Data files corrupted or invalid JSON

| Solution | Notes |
|----------|-------|
| Restore from backup | Check `data/backups/` for previous versions |
| Regenerate | Delete file and re-run relevant command |
| Validate | Use JSON validator to check file syntax |

### Profile data lost after update

| Solution | Notes |
|----------|-------|
| Backup before update | Use Export Backup in Saved Jobs page |
| Check migration | Look for migration plan in `docs/RELEASE.md` |

### Source registry in inconsistent state

| Solution | Notes |
|----------|-------|
| Reset to defaults | Delete registry files and `data/source-registry-tombstones.json`; they regenerate from defaults |
| Manual fix | Edit `data/source-registry-active.json`, `data/source-registry-pending.json`, or `data/source-registry-rejected.json` directly |

### Deleted source reappears after sync or discovery

| Check | Action |
|-------|--------|
| Tombstone ledger | Verify `data/source-registry-tombstones.json` contains the source identity |
| Restore path | Use the explicit restore-deleted route or Admin action before re-adding the source |
| Sync payload | Remember remote sync snapshots only carry `active` and `pending`; deleted rows are local-only tombstones |

---

## Diagnostic Commands

```powershell
# Check bridge health
curl http://127.0.0.1:8877/ops/health

# Check jobs feed
type data\jobs-unified.json | find /C "]"

# Check fetch report summary
python -c "import json; r=json.load(open('data/jobs-fetch-report.json')); print(f'Jobs: {r[\"summary\"][\"keptCount\"]}, Failed: {r[\"summary\"][\"failedSources\"]}')"

# Check discovery report
python -c "import json; r=json.load(open('data/source-discovery-report.json')); print(f'Candidates: {len(r.get(\"candidates\", []))}')"
```

---

## Where to Get Help

1. **Check logs:** `data/` directory contains `jobs-fetch-report.json`, `source-discovery-report.json`
2. **Architecture:** See [`architecture-ai-map.md`](architecture-ai-map.md) for system understanding
3. **Data contracts:** See [`DATA_CONTRACT.md`](DATA_CONTRACT.md) for expected data formats
4. **GitHub issues:** Search existing issues for similar problems

---

*Last updated: 2026-04-18*

# Troubleshooting Guide

> - **Status:** Active
> - **Use this when:** diagnosing a known failure mode in the frontend, bridge, pipeline, discovery, desktop runtime, tests, or local data
> - **Canonical for:** common issue triage steps, quick diagnostics, and known recovery paths
> - **Not canonical for:** subsystem ownership, API contracts, or release policy
> - **Then inspect:** [`architecture-ai-map.md`](architecture-ai-map.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`testing.md`](testing.md), and the owning runtime docs for the affected subsystem
> - **Last updated:** 2026-06-04

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
| Desktop mode: file permissions | Ensure the configured data root is writable (`data/local-user-data/` for repo/source runs, `%APPDATA%\Baluffo\local-user-data\` for Windows packaged desktop) |
| Guest mode | Sign in if you want seen/saved job state to persist; guest browsing is intentionally non-persistent |

### Desktop sign-in creates an unexpected new profile

| Possible Cause | Solution |
|----------------|----------|
| Existing profile list did not load | Use the desktop sign-in `Retry` path first; only choose `Create new profile` when you intentionally want a new local profile |
| Bridge/local data unavailable | Confirm the bridge is running and the configured `local-user-data` directory is readable/writable |

### UI elements not responding

| Possible Cause | Solution |
|----------------|----------|
| Busy state active | Wait for current operation (fetch/discovery) to complete |
| JavaScript error | Check browser console for errors |
| data-ui attribute missing | Verify element has `data-ui` attribute per [`selectors.js`](../frontend/shared/ui/selectors.js) |

### Codex in-app browser cannot visually inspect the app

| Symptom | Solution |
|---------|----------|
| `ERR_CONNECTION_REFUSED` for `http://127.0.0.1:8080/saved.html` | Start the owned site and bridge with `npm run dev:bridge` from the repo root |
| Saved page loads but shows profile restore/sign-in instead of saved rows | Open the desktop URL: `http://127.0.0.1:8080/saved.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1` |
| Bridge-backed page still does not load saved data | Confirm both ports respond with `Test-NetConnection 127.0.0.1 -Port 8080` and `Test-NetConnection 127.0.0.1 -Port 8877` |

Use a bare static server only for static markup/CSS fixtures. For real Saved-page visual QA, use `npm run dev:bridge` so desktop local data comes from the file-backed bridge store under the configured data root.

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

On Linux:

```bash
# Find process using port 8877
lsof -i :8877
# or
fuser 8877/tcp

# Kill the process
kill <PID>
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
| Import errors | Run `python -m pip install -r requirements-lock.txt` |

### Bridge diagnostics are needed for support

| Artifact | Use |
|----------|-----|
| `data/admin-bridge-events.jsonl` | Structured retained bridge lifecycle/task events with bounded retention and token redaction |
| Console output | Human or JSONL stdout controlled by `--log-format` / `BALUFFO_BRIDGE_LOG_FORMAT`; retained artifacts do not change this output |

Use the retained event file when console logs are unavailable or too noisy. The file is written under the configured data directory, so custom `BALUFFO_DATA_DIR` runs keep diagnostics beside the rest of that run's data.

### Sync operations fail

| Possible Cause | Solution |
|----------------|----------|
| GitHub App not configured | Run `python scripts/build_sync_app_config.py` |
| `SSL certificate verification failed` | Install/update the machine trust store, or set `BALUFFO_GITHUB_CA_BUNDLE` to a PEM CA bundle that trusts GitHub and any TLS-inspecting proxy. `BALUFFO_SYNC_CA_BUNDLE` still works for sync-only overrides, and `BALUFFO_DESKTOP_UPDATE_CA_BUNDLE` can scope the override to desktop updates only |
| Desktop update works but sync still hits the SSL error | Rebuild from a version that includes the sync default-`urlopen` TLS fix; older artifacts could still bypass the shared GitHub SSL context on the live sync path |
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

### Jobs pipeline fails with a fetch/discovery wait safety cap

| Check | Action |
|-------|--------|
| Parent status | Check `/tasks/run-jobs-pipeline-status` for `stage`, `error`, `runId`, `baselineOutputCount`, and `finalOutputCount` |
| Active tasks | Check `/ops/task-state?view=summary`; healthy terminal state has `count: 0` |
| Child report | Inspect `jobs-fetch-report.json` or `source-discovery-report.json` under the active data root; a live child should show fresh progress or heartbeat evidence |
| Lifecycle evidence | Inspect retained bridge lifecycle events before concluding the child is dead; stale report artifacts alone are not proof of liveness |
| Umbrel smoke | On the raw-LAN app, also confirm `/ops/health.appVersion`, `/sync/status`, `jobs.html`, and the jobs data feed after the pipeline settles |

If the child is still making progress but the parent failed with an absolute safety-cap error, treat it as parent wait accounting rather than a fetcher parsing failure. If the child lifecycle row is terminal and the matching report is missing or unfinished, the parent should fail or cancel promptly instead of waiting for the absolute cap.

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
| Stale bad social rows still in `jobs-unified.json` | Run `python src/jobs_fetcher.py --force-refresh-all --social-enabled` so incremental cache skips do not preserve old Reddit/Mastodon contamination |

---

## 4. Discovery Issues

### Discovery report and registry counts disagree

| Check | Action |
|-------|--------|
| Latest report | Compare `/discovery/report.runtime.registryFinalization` counts with `/registry/summary` |
| Full registry load | Compare `/registry/sources?buckets=active,pending,rejected&includeHiddenPending=1` counts with the same report |
| Auto-approval | Check `/discovery/report.runtime.autoApproval.enabled`, `status`, and `approvedCount`; completed report-declared approvals are authoritative for repair |
| Safe demotion | Check sync or registry diagnostics for `registryAutoHeal.safeAutomation`; load-time safe demotion should not immediately undo discovery-auto-approved active rows |
| Healthy state | A repaired terminal report has `taskProgress.active == false`, registry summary/source counts matching report finalization, and no active discovery task in `/ops/task-state?view=summary` |

On Umbrel, a mismatch can appear after a completed discovery if auto-approval persistence or load-time registry normalization diverges from the terminal report. Do not manually edit registry files first; use the bridge routes to gather evidence so JSON/gzip/journal and registry authority stay consistent.

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

### Closed desktop window leaves `Baluffo.exe` running

Fixed desktop-window builds shut down the owned launcher, site child, and bridge child shortly after the Baluffo browser window closes when no critical fetch, discovery, pipeline, or sync task is active. A regular user close records `desktop_regular_close_shutdown_requested` and should release launcher/site/bridge resources within the packaged 5s cleanup target. The frontend sends both beacon and keepalive-fetch close signals because Chromium can report a queued beacon and still drop it during shutdown. During unexpected active-work window loss, the launcher may keep the bridge alive temporarily for the existing background recovery path, but a user-confirmed active-work close records `desktop_confirmed_active_work_shutdown_requested` and must exit instead of reopening the browser. `/ops/health` polling alone must not keep the process tree alive.
Approved in-app desktop navigation between Jobs, Saved, and Admin pages must not record `desktop_regular_close_shutdown_requested`; seeing that event during Jobs-to-Admin navigation points to a lifecycle navigation-bypass regression.

If a visible desktop page closes unexpectedly with `admin_bridge_owner_session_exit_requested`, inspect `%APPDATA%\Baluffo\admin-bridge-events.jsonl` for Windows packaged desktop, or the configured data root for dev/custom `--data-dir` runs. Look for non-health page routes such as `/ops/task-state`, `/ops/dashboard-health`, `/tasks/run-jobs-pipeline-status`, or `/app/update-status` between the last lifecycle heartbeat and shutdown. Those page-originated routes are a fallback liveness signal; `/ops/health` remains excluded so the launcher watchdog cannot keep a closed window alive by itself.

For release or shutdown-path changes, run `npm run test:frontend:packaged:desktop-lifecycle-rehearsal`. It covers the false-idle case where lifecycle POST/beacon traffic stops while non-health page traffic continues, then verifies a signal-backed regular window shutdown releases the launcher, browser proof PID, and default desktop ports under the 5s target. The smoke uses CDP to attach to the packaged page, dispatches the page lifecycle close signal, and sends a Windows main-window close instead of `taskkill`. For active fetch/discovery/pipeline/sync close-confirmation changes, also run `npm run test:frontend:packaged:active-task-close-rehearsal`; it verifies the first confirmed active-task close exits without browser relaunch or fatal active-work handling.

Diagnostics:

| Check | Action |
|-------|--------|
| Startup trace | Inspect `%APPDATA%\Baluffo\desktop-startup-metrics.jsonl` for Windows packaged desktop, or the configured data root for dev/custom runs |
| Bridge events | Inspect `%APPDATA%\Baluffo\admin-bridge-events.jsonl`; repeated `/ops/health` entries should not advance the desktop-window owner activity timestamp |
| Session state | Inspect `%LOCALAPPDATA%\Baluffo\desktop-session.json` for stale `sitePid`, `bridgePid`, `sitePort`, and `bridgePort` values |
| Live ports | Check `127.0.0.1:8080` and `127.0.0.1:8877` only if the stuck `Baluffo.exe` children are still present |

### Desktop updater goes back to `Download` after starting or finishing a download

| Check | Action |
|-------|--------|
| Failed background download | Open the updater panel again and confirm whether it now shows the persisted error with `Try download again` |
| Persisted updater state | Inspect `%APPDATA%\Baluffo\updater\install-state.json` for `downloadState`, `installState`, and `lastError` |
| Handoff diagnostics | If the app reports that it could not confirm updater handoff, inspect `%APPDATA%\Baluffo\updater\handoff-diagnostics.json`; it records non-secret verifier predicates such as PID liveness and session match |
| Helper diagnostics | If install handoff starts, inspect `%APPDATA%\Baluffo\updater\desktop-updater-helper.*.log` and `desktop-updater-helper.diagnostics.jsonl` |
| Bad staged ZIP | Delete only the failed file under `%APPDATA%\Baluffo\updater\downloads\` if it remains after a failed attempt, then retry the download |

New Windows packaged installs use `%APPDATA%\Baluffo\updater\post-install-success.json` as the canonical updater success marker. During migration from older source helpers, `ship\data\updater\post-install-success.json` can also appear as a transition-only compatibility marker.

### Portable update from `v0.1.33` reports `install_handoff_unconfirmed`

`v0.1.33` can falsely reject a live Windows launcher during install handoff when the packaged runtime lacks optional `psutil`. A target ZIP cannot repair that already-installed source-side checker. Close Baluffo, extract a fixed portable release `v0.2.1` or newer, keep the old `ship\data\` available for first-launch migration or copy it into `%APPDATA%\Baluffo\`, and start the new `Baluffo.exe`. Do not move or rewrite the published `v0.2.0` release tag to work around this.

### Linux AppImage won't start

| Symptom | Action |
|---------|--------|
| `fuse: failed to exec` or `Cannot mount AppImage` | Install FUSE: `sudo apt install libfuse2` (Ubuntu 24.04+ ships FUSE3 by default, which is not compatible) |
| FUSE unavailable (container/headless CI) | Extract and run directly: `./Baluffo-*.AppImage --appimage-extract-and-run` |
| `GLIBC not found` | Build on the oldest supported glibc distro (CI uses `ubuntu-latest` / Ubuntu 24.04) |
| `Permission denied` | `chmod +x Baluffo-*.AppImage` |

### Linux port already in use

```bash
# Find process using port 8877
lsof -i :8877
# or
fuser 8877/tcp

# Kill the process (replace <PID> with actual PID)
kill <PID>
```

### Linux system Chromium not found

Playwright frontend smoke tests need `chromium-browser` on Ubuntu 26.04 (bundled Chromium is not yet supported):

```bash
sudo apt install chromium-browser
```

Set `PLAYWRIGHT_SYSTEM_CHROMIUM=1` or use `npm run test:frontend:linux` which sets it automatically.

### Linux session paths

| Data | Windows | Linux |
|------|---------|-------|
| Config/data root | `%APPDATA%\Baluffo\` (packaged) | `ship_root/data` by default (`BALUFFO_DATA_DIR` overrides; session/transient uses XDG data) |
| Session/transient | `%LOCALAPPDATA%\Baluffo\` | `~/.local/share/Baluffo/` (XDG data root) |
| Sync key | DPAPI (machine-protected) | System keyring, fallback `~/.config/baluffo/sync.key` (0o600) |

### Ship bundle launcher fails

| Check | Action |
|-------|--------|
| Version directory exists | Verify `app/versions/<version>` exists |
| current.txt valid | Check `app/current.txt` points to valid version |
| Permissions | Ensure scripts are not blocked by security policy |

### Packaged sync private-key validation fails during release build

| Check | Action |
|-------|--------|
| Failure text mentions packaged sync private key | Inspect the failing `scripts/build_ship_bundle.py` path before changing sync runtime behavior |
| Import or helper mismatch | Confirm the build script validates PEM data through `src/source_sync_crypto.py`, not a broad composition-root import |
| Local preflight passed but CI failed | Reproduce with a valid non-secret test PEM path or env value so the secret-backed generation path is actually exercised |
| Coverage gap | Add or inspect focused coverage near `tests/test_build_ship_bundle_import_closure.py` before loosening validation |

### Build fails

```powershell
# Ensure dependencies installed
python -m pip install -r requirements-lock.txt

# Rebuild frontend config
npm run build:frontend-runtime-config
```

---

## 6. Testing Issues

### Python tests fail with import errors

| Solution | Command |
|----------|---------|
| Set PYTHONPATH | `set PYTHONPATH=%CD%` then run tests |
| Install dev deps | `python -m pip install -r requirements-lock.txt` |

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

### Packaged Jobs pipeline smoke gets `ECONNREFUSED`

| Check | Action |
|-------|--------|
| Endpoint | Confirm the refusal is for `/tasks/run-jobs-pipeline-status` on the packaged bridge port |
| Smoke report | Download and inspect `packaged-desktop-smoke-report.json` or the lane-specific report artifact before relying on console output |
| Transient readiness | If retries later succeed, treat it as startup settling; if the retry window expires, inspect bridge stdout/stderr and runtime exit status |
| Product signal | Check whether the backend pipeline reached `stage=error` or emitted a non-empty `error` payload after startup |

### Packaged smoke failure summary raises `UnicodeEncodeError`

| Check | Action |
|-------|--------|
| Console encoding | Treat Windows `cp1252` or other non-UTF-8 console failures as diagnostic failures, not product proof |
| Masked root cause | Fix or use the console-safe failure printer before classifying the Playwright or packaged runtime error |
| Artifacts | Download the packaged smoke report and stdout/stderr files; they usually contain the real failure that console printing hid |

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

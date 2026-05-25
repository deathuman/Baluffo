# Linux Compatibility Plan

> - **Status:** Archived — all 8 phases completed and shipped to `main` (2026-05-25). See git history for implementation detail.

## Summary

Baluffo runs on Windows today. The `src/ship/` (desktop runtime) and `src/source_sync_runtime.py` (sync credential crypto) contain Windows-only `ctypes.windll.*` calls guarded by runtime `os.name == "nt"` checks — these are safe at import time but return stubs/empty results on Linux. The dev workflow (`npm run dev:bridge`, `npm run dev:pipeline`) **already works on Linux** without any changes.

There are no import-time crashes. All Windows-only `ctypes.windll.*` and `ctypes.wintypes` usage is either:
- Guarded by `os.name == "nt"` at module level (safe on import, only defines types/functions)
- Guarded by runtime `if os.name == "nt"` checks (safe at call time, falls back to cross-platform code)

The real remaining gaps are:
- Session root path uses `AppData/Local` fallback instead of XDG Base Directory
- DPAPI credential encryption has no Linux alternative
- No `_linux.py` platform module for native process/tcp-port management via `psutil`
- `WindowsPath` instantiation in `packaged_smoke/build_env.py` crashes on Linux
- No Linux CI or test markers
- No Linux packaging build pipeline (the Windows build uses PyInstaller → EXE; Linux needs PyInstaller → AppImage)
- No Linux release pipeline (CI + GitHub Releases publication)

## Current State

### Already Working on Linux (Ubuntu 26.04, Python 3.14)

- Python venv with deps from `requirements.txt`
- npm `node_modules` installed via `npm ci`
- git hooks configured
- `npm run dev:bridge` — starts bridge + site
- `npm run dev:pipeline` — runs jobs pipeline
- `from src.ship.desktop_app import *` — succeeds
- `from src.ship import desktop_update` — succeeds
- `from src.bridge.desktop_attention import notify_pipeline_completion_attention` — succeeds
- Python tests: **3164 passed, 3 failed, 1 skipped** (without slow/packaging markers: **3029 passed, 0 failed**)

### Import Chain Analysis (No Crashes)

The full import chain `desktop_app/__init__.py` → `desktop_update.py` → `desktop_update_shared.py` **works on Linux**. The `pywin32-ctypes` package also has a `py3-none-any` wheel and installs on Linux without issues. For the exhaustive guard-by-guard audit, see [Import-Time vs Call-Time Safety](#import-time-vs-call-time-safety-corrected) below.

### Test Results

| Suite | Passed | Failed | Notes |
|-------|--------|--------|-------|
| `npm run test:py` (excl. slow/packaging) | 3029 | 0 | 139 deselected |
| `npm run test:py:linux` | 2996 | 0 | 183 deselected |
| `npm run test:py:extended` | 3177 | 0 | 2 skipped |

### Test Failures: 0 (Resolved)

The 3 rehearsal test failures (WindowsPath crash in `build_env.py`) were resolved by:

## Platform-Specific Code Inventory

### 1. `src/ship/desktop_app/_windows.py` (885 lines)

The primary Windows-only module. All functions guarded by `os.name == "nt"` at call time. Contains:
- Process management via `ctypes.windll.kernel32` (OpenProcess, TerminateProcess, GetExitCodeProcess, CloseHandle, WaitForSingleObject, QueryFullProcessImageNameW, GetProcessTimes, GetLastError)
- Job object management via `ctypes.windll.kernel32` (CreateJobObjectW, SetHandleInformation, SetInformationJobObject, AssignProcessToJobObject)
- Window enumeration via `ctypes.windll.user32` (EnumWindows, GetClassNameW, IsWindowVisible, GetWindowTextW) and `ctypes.windll.dwmapi` (DwmGetWindowAttribute)
- TCP port listening via `netstat -ano -p tcp` (Windows-specific)
- `_HANDLE_FLAG_INHERIT`, `_PROCESS_TERMINATE`, `_STILL_ACTIVE`, `_JOB_OBJECT_*` constants

**Linux equivalents needed:**
- Process management → `psutil` or `os.kill(pid, 0)` + `/proc` (already done in `pid_is_running()`)
- Process image path → `/proc/{pid}/exe` (readlink)
- Process start time → `/proc/{pid}/stat` or `psutil`
- Process tree → `psutil.Process(pid).children()`
- Window enumeration → Nothing equivalent (headless). Return stub results.
- TCP port listener PID → `psutil.net_connections()` or `/proc/net/tcp`
- Job objects → Use process groups (`os.setpgid`, `os.killpg`) or `psutil` process tree management

### 2. `src/ship/desktop_update_shared.py` — `_pid_is_running_windows()` (lines 374–396)

Uses `kernel32.OpenProcess`/`GetExitCodeProcess`/`CloseHandle`. Never called on Linux (guarded by `sys.platform == "win32"` at line 410; `psutil` is tried first). The `from ctypes import wintypes` at line 13 is safe — `wintypes` is cross-platform.

### 3. `src/source_sync_runtime.py` — DPAPI Crypto (lines 47–82)

Uses `ctypes.windll.crypt32.CryptProtectData`/`CryptUnprotectData` guarded by `os.name == "nt"`. On Linux falls back to `None` for the crypto functions. Callers need a Linux alternative — `cryptography` Fernet symmetric key encrypted with `keyring` (system keyring) or a file-based key.

### 4. `src/ship/desktop_app/config.py` — `resolve_browser_session_root()` (lines 259–288)

Uses `LOCALAPPDATA` environment variable (Windows). On Linux this returns `""`, falling through to `HOME` + `AppData/Local` (wrong), then to temp dir. **Confirmed:** returns `/home/user/AppData/Local/Baluffo` on Linux. Needs XDG Base Directory (`XDG_DATA_HOME` / `~/.local/share`) fallback.

### 5. `src/ship/desktop_app/browser.py` — `launch_chromium_app()` (lines 258–266)

Uses `subprocess.CREATE_NEW_PROCESS_GROUP` (`os.name == "nt"`) — already guarded, Linux path works with plain `subprocess.Popen`.

### 6. `src/ship/desktop_app/process.py` — `terminate_process()` (line 113)

Uses `process.terminate()` as Linux fallback — cross-platform, safe. Windows path uses `taskkill`.

### 7. `src/ship/desktop_app/launcher_diagnostics.py` — `show_native_message()` (lines 135–142)

Already has `os.name == "nt"` guard. Falls back to `print()` on Linux. Works.

### 8. `src/ship/desktop_app/session.py` — `is_process_alive()` (lines 232–257)

Uses `api.ctypes.windll.kernel32.*` behind `if api.os.name == "nt"` guard. Linux fallback uses `os.kill(pid, 0)`. Safe at call time.

### 9. `src/bridge/desktop_attention.py` (271 lines)

Module-level `from ctypes import wintypes` at line 6 — **safe** (`wintypes` is cross-platform). Runtime functions (`notify_pipeline_completion_attention()`) have `os.name != "nt"` early return. `_user32()` uses `getattr(ctypes, "windll", None)` — returns `None` on Linux.

### 10. `src/ship/packaged_smoke/build_env.py` — `packaged_desktop_local_appdata_root()` (line 254)

**New finding:** When called with a `WindowsPath` argument via the rehearsal test chain, `Path(artifacts_dir).expanduser().resolve()` tries to instantiate `WindowsPath`, which raises `pathlib.UnsupportedOperation` on Linux. The caller `packaged_runtime_env_overrides()` passes `artifacts_dir` from a `WindowsPath`-typed variable.

## Import-Time vs Call-Time Safety (Corrected)

**There are NO import-time crashes on Linux.** All Windows-only code is behind runtime `os.name == "nt"` or `sys.platform == "win32"` guards:

- `desktop_update_shared.py` line 13 (`from ctypes import wintypes`) — safe, `wintypes` is cross-platform
- `_windows.py` lines 315–317 (`from ctypes import wintypes`, `import winreg`) — guarded by `if os.name == "nt"`
- `source_sync_runtime.py` lines 51–82 (`ctypes.windll.crypt32`) — guarded by `if os.name == "nt"`
- `session.py` lines 236–252 (`api.ctypes.windll.kernel32`) — guarded by `if api.os.name == "nt"`
- `launcher_diagnostics.py` lines 137–139 (`api.ctypes.windll.user32`) — guarded by `if api.os.name == "nt"`
- `desktop_updater_ui.py` lines 323–326 (`ctypes.windll.user32.MessageBoxW`) — guarded by `if os_mod.name == "nt"`
- `build_env.py` line 294 (`deps.ctypes.windll.shell32`) — guarded by `if deps.os.name != "nt": return False`
- `admin_entrypoint_runtime.py` lines 230–252 — guarded by `if sys.platform == "win32"` and try/except
- `desktop_attention.py` — `wintypes` import is safe; `_user32()` uses `getattr` with default

This means `dev_admin_supervisor.py` **does** import `desktop_app.browser` and `desktop_app.process` — and it works on Linux.

## Package Dependencies

### Current `requirements.txt`

```
pyinstaller==6.19.0     # Windows-only, PyInstaller EXE bundler
Scrapy==2.16.0          # Cross-platform
Twisted==26.4.0         # Cross-platform
scrapy-playwright~=0.0.46  # Cross-platform
httpx==0.28.1           # Cross-platform
pydantic~=2.13          # Cross-platform
ruff==0.15.9            # Cross-platform, dev dep
```

### Current `requirements-lock.txt` — No Linux blocker

`pywin32-ctypes==0.2.3` (line 111) — transitive dep of `pyinstaller`. Has a `py3-none-any` wheel, installs on Linux without issues. **Not a blocker.**

### New dependencies needed

| Package | Purpose | Platform |
|---------|---------|----------|
| `psutil` | Cross-platform process management (already conditionally imported in `desktop_update.py`, lines 54–58) | Cross-platform |
| `keyring` | System keyring for credential storage (Linux alternative to DPAPI) | Cross-platform |
| `cryptography` | Fernet symmetric encryption (fallback when keyring unavailable) | Already transitive dep via `scrapy` → `pyopenssl` → `cryptography` |

**No new npm dependencies needed.**

## Phased Implementation Plan

> **Completion status:** Phase 0 ✅ | Phase 0a ✅ | Phase 1 ✅ | Phase 2 ✅ | Phase 3 ✅ | Phase 4 ✅ | Phase 5 (merged) | Phase 6 ✅ | Phase 7 ✅ | Phase 8 ✅

### Phase 0: Test Infrastructure & Quick Wins ✅

These changes add `psutil` to requirements, fix the `WindowsPath` crash in build_env, and set up test markers. No behavioral changes on Windows.

| # | Change | Files | Risk |
|---|--------|-------|------|
| 0.1 | **Add `psutil` to `requirements.txt`** and regenerate lockfile | `requirements.txt`, `requirements-lock.txt` | Low — already conditionally imported in `desktop_update.py` |
| 0.2 | **Fix `WindowsPath` crash in `build_env.py`** — the `packaged_desktop_local_appdata_root()` function receives `artifacts_dir` as a `WindowsPath` via the rehearsal chain. Cast `artifacts_dir` to `str` before `Path()` construction to avoid `pathlib.UnsupportedOperation` | `src/ship/packaged_smoke/build_env.py` | Low — pure bugfix, no behavior change |
| 0.3 | **Add `@pytest.mark.windows` markers** — mark tests that require Windows-specific APIs (e.g., `ctypes.windll`, `winreg`, pefile) so they can be skipped on Linux | `tests/` | None |
| 0.4 | **Add `npm run test:py:linux` script** — runs Python tests excluding `windows`-marked tests | `package.json` | None |

**Verification after Phase 0:**
- `npm run test:py` passes on Linux (3164+ passed, no regressions)
- `npm run test:py:linux` runs and reports clean
- `pip install` from lockfile works on Linux
- Rehearsal tests no longer crash on `WindowsPath`

### Phase 0a: Playwright Browser on Linux Developer Workstations ✅

The bundled Playwright Chromium binary **does not support Ubuntu 26.04** yet. Upstream issue [microsoft/playwright#40117](https://github.com/microsoft/playwright/issues/40117) is open, targeted at Playwright v1.61. Until resolved, `npx playwright install chromium` fails with `"Playwright does not support chromium on ubuntu26.04-x64"`.

#### What's Affected (Linux Dev Machine Only)

| Command | Needs Playwright Chromium? | Status |
|---------|---------------------------|--------|
| `npm run test:py` / `test:py:linux` | No | Already works |
| `npm run dev:bridge` / `dev:pipeline` | No | Already works |
| `npm run test:frontend` / `test:smoke` | **Yes** | Fails — `playwright.config.js` uses bundled Chromium |
| `npm run test:frontend:packaged:*` | **Yes** | Fails — `.mjs` scripts use `chromium.launch()` |
| Python `try_fetch_with_playwright()` | No (separate binary) | Already guarded (try/except + circuit breaker) |

**CI is unaffected** — GitHub Actions runs on `ubuntu-latest` (24.04), where Playwright installs and runs correctly.

#### Solution: Use System Chromium Instead of Playwright's Bundled Binary

Ubuntu 26.04 ships `chromium-browser` natively. Playwright 1.58+ supports the `channel: 'chromium'` option, which uses the system-installed Chromium instead of the bundled one.

**Approach:** Add a Linux-aware Playwright config that auto-detects system Chromium availability and falls back gracefully when it's missing.

| # | Change | Files | Risk |
|---|--------|-------|------|
| 0a.1 | **Create `playwright.linux.config.js`** — copy of `playwright.config.js` that uses `channel: 'chromium'` instead of the default bundled Chromium, with a `PLAYWRIGHT_SYSTEM_CHROMIUM` env-var opt-in | `playwright.linux.config.js` (new) | Low — separate config, doesn't affect existing CI |
| 0a.2 | **Guard `.mjs` smoke scripts** — add `channel: 'chromium'` fallback to the `chromium.launch()` calls in `.mjs` smoke scripts when `PACKAGED_SMOKE_SYSTEM_CHROMIUM=1` is set, allowing them to use system Chromium on Linux | `tests/frontend/packaged-desktop-smoke*.mjs` | Low — env-var gated, opt-in |
| 0a.3 | **Resolve system Chromium path** — update `resolve_playwright_chromium_executable()` in `build_env.py` to check `shutil.which("chromium-browser")` as a Linux fallback when Playwright's bundled binary is unavailable | `src/ship/packaged_smoke/build_env.py` | Low — adds fallback after existing resolution logic |
| 0a.4 | **Add `npm run test:frontend:linux` script** — runs `npx playwright test --config=playwright.linux.config.js` | `package.json` | None |

**Verification after Phase 0a:**
- `chromium-browser` installed via apt on Ubuntu 26.04
- `npm run test:frontend:linux` passes (or cleanly reports missing browser)
- `npm run test:frontend` still passes on Windows/macOS (no regression)

#### Alternative: Docker-Based Testing

For developers who prefer not to install system Chromium, a Docker container provides an isolated Ubuntu 24.04 environment:

```bash
docker run --rm -v $(pwd):/work -w /work \
  mcr.microsoft.com/playwright:v1.58-jammy \
  npx playwright test
```

#### Long-Term Fix

When Playwright v1.61 ships with official Ubuntu 26.04 support, upgrade `@playwright/test` in `package.json` and remove the Linux-specific config workarounds.

### Phase 1: Platform Abstraction Layer (`_linux.py`) ✅

Create `src/ship/desktop_app/_linux.py` as a counterpart to `_windows.py`. This is the largest phase.

The compat facade at `src/ship/desktop_app/_compat.py` provides `desktop_api()` → `desktop_app` module. The `__init__.py` dispatches attribute lookups through `_COMPAT_MODULES` via `__getattr__`.

**Dispatch naming strategy:** Call sites throughout the codebase hard-code `_windows_*` prefixed names (e.g., `api._windows_reclaim_stale_runtime_children()`, `api._windows_close_desktop_job()`). These resolve through `__getattr__` → `_COMPAT_MODULES` → `_windows.py`. All called functions already have internal `os.name == "nt"` early-return guards and are safe on Linux (they return `None` / empty / skipped status).

**Approach:** `_linux.py` defines the same `_windows_*` prefixed names with Linux implementations. The `_COMPAT_MODULES` tuple is ordered at import time based on `os.name`:

```python
# _COMPAT_MODULES construction in __init__.py
_SHARED_MODULES = (..., )   # modules without platform-specific names
if os.name == "nt":
    _COMPAT_MODULES = (_windows_module,) + _SHARED_MODULES
else:
    _COMPAT_MODULES = (_linux_module,) + _SHARED_MODULES
```

**Why this works, and why runtime fallthrough doesn't:** `__getattr__` caches every resolved attribute via `globals()[name] = value` (line 91 of `__init__.py`). Once cached, Python finds it in `__dict__` and never calls `__getattr__` again. If we relied on runtime `os.name` guards inside `_linux.py` to "fall through" on Windows, the first call would cache the stub permanently and `_windows.py`'s real implementation would be unreachable. Platform-dependent `_COMPAT_MODULES` ordering at import time avoids the cache trap entirely.

**Key consequences:**
- `_linux.py` functions do NOT need `os.name` guards — the module is only in `_COMPAT_MODULES` on Linux, so it's never dispatched to on Windows
- `_windows.py`'s existing `os.name` guards are kept (they protect call sites that import `_windows.py` directly, outside the compat dispatch)
- Call sites in `launcher_flow.py`, `session.py`, `launcher_recovery.py`, `process.py`, and `browser.py` are unchanged — they still call `api._windows_*()` and get the platform-appropriate implementation

The naming is intentionally kept as `_windows_*` to maintain dispatch-surface compatibility. Once Linux is fully supported, the names can be refactored to platform-agnostic equivalents in a follow-up cleanup PR.

| # | Change | Files | Risk |
|---|--------|-------|------|
| 1.1 | **Create `_linux.py`** — implement Linux equivalents for all functions in `_windows.py` that are called through the compat dispatch | `src/ship/desktop_app/_linux.py` (new) | Medium |
| 1.2 | **Update `__init__.py`** — construct `_COMPAT_MODULES` at import time based on `os.name`. On Linux, `_linux_module` is placed first in the tuple; on Windows, `_windows_module` is placed first. This avoids the `__getattr__` caching trap (line 91 caches the first lookup permanently via `globals()`, so runtime fallthrough can't work). No function-level `os.name` guards needed in `_linux.py` — the module is only in the tuple on Linux | `src/ship/desktop_app/__init__.py` | Medium — dispatch ordering change |
| 1.3 | **Implement process management** — `pids_listening_on_tcp_port()`, `wait_for_process_exit_pid()`, `terminate_process_by_pid()`, `process_image_matches()` using `psutil` and `/proc` | `_linux.py` | Medium |
| 1.4 | **Implement process tree management** — `terminate_process_tree_details_by_pid()`, `try_reclaim_stale_bridge_process()`, `try_reclaim_stale_site_process()`, `reclaim_stale_runtime_children()` using `psutil.Process.children()` and process groups | `_linux.py` | Medium |
| 1.5 | **Implement process-group alternative** — `create_process_group()`, `assign_pid_to_group()`, `close_process_group()` using `os.setpgid()` / `os.killpg()` | `_linux.py` | Medium |
| 1.6 | **Window enumeration → stubs** — `find_baluffo_visible_window()`, `enumerate_visible_desktop_windows()`, `window_is_cloaked()` return empty/None on headless Linux | `_linux.py` | Low — no-op fallback |
| 1.7 | **Implement process identity helpers** — `process_image_path()` → readlink `/proc/{pid}/exe`, `process_start_ts()` → `/proc/{pid}/stat` | `_linux.py` | Low |
| 1.8 | **Update `show_native_message`** — existing `os.name == "nt"` guard handles this; optionally add `notify-send` or zenity on Linux | `launcher_diagnostics.py` | Low |

**Verification after Phase 1:**
- `python -c "from src.ship.desktop_app._linux import *"` succeeds
- All new functions have at least a smoke test
- `npm run test:py` still passes on Windows (no regressions)
- `npm run test:py` passes on Linux
- **Follow-up:** After `_linux.py` is functional, the 3 rehearsal tests in `test_rehearsal_flows.py` that mock `os.name = "nt"` should be refactored (or new variants added) to test the real Linux implementations with `os.name = "posix"` and Linux-appropriate paths

### Phase 2: XDG Base Directory Support ✅

| # | Change | Files | Risk |
|---|--------|-------|------|
| 2.1 | **Update `resolve_browser_session_root()`** — add `XDG_DATA_HOME` / `~/.local/share/Baluffo` fallback before `AppData/Local` when on Linux | `src/ship/desktop_app/config.py` | Low |
| 2.2 | **Update `_resolve_desktop_session_root_fallback()` in `desktop_update_shared.py`** — same XDG fallback for the backup path. Note: the primary code path delegates to `config.resolve_browser_session_root()` via import (line 347–352), which succeeds on Linux. Fixing Phase 2.1 covers the primary path; this is the secondary fallback | `src/ship/desktop_update_shared.py` | Low |
| 2.3 | **Update `_resolve_runtime_path()`** — the `_looks_like_windows_absolute_path()` check at line 364 already handles Linux correctly (returns Path as-is if not a Windows path) | No change needed | None |
| 2.4 | **Update `CHROMIUM_BROWSER_CANDIDATES` in `config.py`** — add Linux binary names: `("google-chrome", "google-chrome-stable"), ("brave-browser", "brave-browser-stable"), ("chromium", "chromium-browser")`. Also support snap paths as a fallback (Chromium is commonly installed via snap on Ubuntu; `shutil.which("chromium")` covers the snap `/snap/bin/chromium` case) | `src/ship/desktop_app/config.py` | Low |
| 2.5 | **`APP_PATH_REGISTRY_SUBKEY`** — Windows-only registry key. `browser.py` falls back to `shutil.which()` when registry lookup fails (line ~108 `resolve_chromium_browser_candidates()`). No change needed. | None | None |

**Verification after Phase 2:**
- Browser session root resolves to `~/.local/share/Baluffo/` on Linux
- `BALUFFO_DESKTOP_SESSION_ROOT` env override still works

### Phase 3: Linux Credential Storage (Sync Crypto) ✅

| # | Change | Files | Risk |
|---|--------|-------|------|
| 3.1 | **Add `keyring` to `requirements.txt`** — for Linux system keyring access (GNOME Keyring / KDE Wallet / Secret Service) | `requirements.txt` | Low |
| 3.2 | **Implement `_encrypt_data_linux()` and `_decrypt_data_linux()` in `source_sync_runtime.py`** — use `cryptography.fernet.Fernet` with a key stored via `keyring` or a fallback file-based key in `~/.config/baluffo/sync.key` | `src/source_sync_runtime.py` | Medium |
| 3.3 | **Update `_encrypt_data()` / `_decrypt_data()` dispatch** — wire the Linux crypto path when `os.name != "nt"` and DPAPI is unavailable | `src/source_sync_runtime.py` | Medium |

**Verification after Phase 3:**
- Credential encrypt/decrypt roundtrip works on Linux
- Windows DPAPI path unchanged

### Phase 4: Linux Shell Launcher Scripts ✅

The Windows ship bundle includes 6 PowerShell launcher scripts copied by `build_ship_bundle.py`. Linux needs bash equivalents of all six for the packaged AppImage.

| # | Change | Files | Risk |
|---|--------|-------|------|
| 4.1 | **Create bash launcher scripts** — `run-bridge.sh`, `run-site.sh`, `run-all.sh`, `apply-update.sh`, `recover-previous.sh`, `create-support-bundle.sh`. Each is a bash equivalent of the existing `.ps1` script of the same base name. `run-all.sh` starts both bridge and site, `apply-update.sh` invokes the update manager, `recover-previous.sh` rolls back to the previous version, `create-support-bundle.sh` collects diagnostics | `scripts/run-bridge.sh`, `scripts/run-site.sh`, `scripts/run-all.sh`, `scripts/apply-update.sh`, `scripts/recover-previous.sh`, `scripts/create-support-bundle.sh` (new) | Low — straightforward bash, no complex logic |
| 4.2 | **Create `scripts/dev_admin_supervisor.sh`** — bash equivalent of the PowerShell dev launcher for running the supervisor in dev mode | `scripts/dev_admin_supervisor.sh` (new) | Low |

**Verification after Phase 4:**
- Each `.sh` script runs from within an extracted AppImage `ship/` directory
- `bash scripts/run-bridge.sh` starts the bridge server
- `bash scripts/run-site.sh` starts the site server
- `bash scripts/run-all.sh` starts both

### Phase 5: CI & Test Infrastructure

> **Merged into Phase 8.** The `build-linux.yml` CI workflow (Phase 8.1) includes `npm run test:py:linux` as a prerequisite step, covering the same territory as the originally planned separate `test-linux.yml`. A standalone test-only CI workflow is not needed — the build CI runs tests before building, which is the correct gate.

### Phase 6: Documentation & Polish

| # | Change | Files | Risk |
|---|--------|-------|------|
| 6.1 | **Update `WSL_SETUP.md`** — add new capabilities (dev workflow now works, XDG paths) | `docs/WSL_SETUP.md` | None |
| 6.2 | **Update `INDEX.md`** — add this plan to the plans index | `docs/INDEX.md` | None |
| 6.3 | **Update `architecture-ai-map.md`** — add `_linux.py` to the `desktop_app` package routing | `docs/architecture-ai-map.md` | None |
| 6.4 | **Update `AI_ASSISTANT_GUIDE.md`** — add verification shortcut for Linux-specific changes | `docs/AI_ASSISTANT_GUIDE.md` | None |
| 6.5 | **Update `AGENTS.md`** — add Linux compatibility rule if needed | `AGENTS.md` | None |

### Phase 7: Linux AppImage Packaging ✅

The objective: produce `Baluffo-{version}-x86_64.AppImage` — a single self-contained file built with PyInstaller `--onedir` and packaged via `appimagetool`. No separate updater binary; the updater logic is folded into the main ELF binary.

**Architecture:**

```
scripts/build_ship_bundle.py    →  ship/           (app bundle, same as Windows)
                                       ↓
PyInstaller --onedir            →  dist/baluffo-linux/
  src/ship/desktop_app/             ├── baluffo          (ELF binary)
  __main__.py                       ├── _internal/       (Python runtime + deps)
                                    └── ship/            (app bundle copy)
                                       ↓
appimagetool                    →  Baluffo-{ver}-x86_64.AppImage
  + packaging/AppRun
  + packaging/baluffo.desktop
  + packaging/baluffo.png
```

**Key differences from the Windows build:**

| Aspect | Windows | Linux (AppImage) |
|--------|---------|-----------------|
| PyInstaller flags | `--windowed --onedir` | `--onedir` (no `--windowed`) |
| Entry point | `desktop_app/__main__.py` | Same, with `_linux.py` from Phase 1 handling runtime |
| Updater | Separate `BaluffoUpdater.exe` (`--onefile`) | Folded into main binary (no separate binary) |
| Output binary | `Baluffo.exe` | `baluffo` (ELF) |
| Icon | `favicon.ico` | `baluffo.png` (converted from `.ico`) |
| Launcher scripts | `.ps1` in `ship/` | `.sh` in `ship/` (from Phase 4) |
| Playwright browser | Bundled `chrome-headless-shell.exe` | Deferred until v1.61; `AppRun` sets `PLAYWRIGHT_SYSTEM_CHROMIUM=1`, relies on system `chromium-browser` |
| Distribution | `.zip` | `.AppImage` (single file, no extraction) |
| CI runner | `windows-2022` | `ubuntu-latest` |
| Default EXE path | `dist/baluffo-portable/Baluffo.exe` | `dist/Baluffo-{version}-x86_64.AppImage` |
| Smoke launcher | `subprocess.Popen([exe_path, ...])` | Same for ELF binary inside AppDir; AppImage smoke test uses `scripts/smoke_test_appimage.sh` |

**Dependencies:** Phases 1, 2, 3, 4 must be complete (needs `_linux.py` for process management, XDG paths for session storage, keyring/Fernet for credential crypto, and `.sh` launcher scripts).

| # | Change | Files | Risk |
|---|--------|-------|------|
| 7.1 | **Create `scripts/build_portable_linux.py`** — builds the ship bundle via `build_ship_bundle.py`, runs PyInstaller with Linux flags (`--onedir`, no `--windowed`, Linux-appropriate hidden imports), then invokes `appimagetool` to produce the `.AppImage`. No separate updater binary — updater logic is folded into the main binary | `scripts/build_portable_linux.py` (new) | Medium — new build script, reuses existing `build_ship_bundle.py` |
| 7.2 | **Create AppImage assets** — `packaging/AppRun` entry script (sets `LD_LIBRARY_PATH`, resolves `$APPDIR`, detects desktop session via `$DISPLAY`/`$WAYLAND_DISPLAY`, launches in service-only mode when headless), `packaging/baluffo.desktop` XDG desktop entry (`Categories=Office;`, `Terminal=false`), `packaging/baluffo.png` icon (converted from `favicon.ico`) | `packaging/AppRun`, `packaging/baluffo.desktop`, `packaging/baluffo.png` (new) | Low — static assets, no code dependencies |
| 7.3 | **Integrate `appimagetool`** — fetch `appimagetool-x86_64.AppImage` during build from GitHub Releases, cache at `_out/appimagetool/`, use it to package the AppDir into the final `.AppImage` | `scripts/build_portable_linux.py` | Low — standard AppImage tooling |
| 7.4 | **Update `build_ship_bundle.py`** — conditionally include bash launcher scripts (`run-bridge.sh`, `run-site.sh`, `run-all.sh`, `apply-update.sh`, `recover-previous.sh`, `create-support-bundle.sh`) from Phase 4 in the ship bundle when targeting Linux | `scripts/build_ship_bundle.py` | Low — additive, gated on platform target |
| 7.5 | **Playwright Chromium strategy** — defer bundling Playwright's Chromium until v1.61. `AppRun` sets `PLAYWRIGHT_SYSTEM_CHROMIUM=1`; packaged smoke tests use system `chromium-browser` via Phase 0a's `channel: 'chromium'` mechanism. On headless (no display), browser tests are skipped gracefully | `packaging/AppRun` | Low — env-var gated, Phase 0a prerequisite |
| 7.6 | **Add `npm run build:linux` script** — single command: `build_ship_bundle.py` → `build_portable_linux.py` → `dist/Baluffo-{version}-x86_64.AppImage` | `package.json` | Low — npm script addition |
| 7.7 | **Add AppImage smoke test** — `scripts/smoke_test_appimage.sh` launches the built AppImage in headless mode, polls bridge + site HTTP endpoints, verifies responses, terminates cleanly. Returns exit code 0 on success | `scripts/smoke_test_appimage.sh` (new) | Low — shell script |
| 7.8 | **Platform-aware default paths in smoke infra** — `packaged_desktop_smoke.py` hardcodes `DEFAULT_EXE_PATH = ROOT / "dist" / "baluffo-portable" / "Baluffo.exe"`. Add `DEFAULT_APPIMAGE_PATH = ROOT / "dist" / f"Baluffo-{version}-x86_64.AppImage"` and make `ensure_portable_exe()`, `run_portable_build()`, and `_exe_path_uses_default_dist()` platform-aware (check `os.name`, use the Windows `.exe` or Linux AppImage path accordingly). Also add a `DEFAULT_ELF_PATH` pointing to the extracted PyInstaller `baluffo` binary for dev smoke testing before AppImage packaging | `src/packaged_desktop_smoke.py`, `src/ship/packaged_smoke/build_env.py` | Medium — touches smoke launch infrastructure |

**Verification after Phase 7:**
- `npm run build:linux` produces `dist/Baluffo-{version}-x86_64.AppImage`
- `bash scripts/smoke_test_appimage.sh` passes (bridge + site respond on HTTP)
- `python -c "from src.packaged_desktop_smoke import DEFAULT_APPIMAGE_PATH; print(DEFAULT_APPIMAGE_PATH)"` resolves correctly on Linux
- AppImage runs on Ubuntu 26.04 (WSL and bare-metal)
- AppImage runs in headless mode on SSH/CI (no display needed, service-only)
- Windows build is not regressed (`npm run build` still produces `Baluffo.exe`)

### Phase 8: Linux Release Pipeline ✅

Unified CI workflow: test, build, smoke, and publish. Absorbs the test CI that was originally a standalone Phase 5; running tests before building is the correct gate and avoids a redundant second workflow.

| # | Change | Files | Risk |
|---|--------|-------|------|
| 8.1 | **Add `.github/workflows/build-linux.yml`** — triggers on tag pushes and `workflow_dispatch`. Full pipeline: checkout → Python 3.14 + Node setup → `pip install -r requirements-lock.txt` → `npm ci` → `npm run lint` + `npm run typecheck:py` → `npm run test:py:linux` → `npm run build:linux` → `bash scripts/smoke_test_appimage.sh` (headless smoke) → upload AppImage as CI artifact → publish `Baluffo-{version}-x86_64.AppImage` to GitHub Releases. Also runs on PRs (test-only, skip publish) | `.github/workflows/build-linux.yml` (new) | Medium — new CI workflow, needs GitHub Release write permissions |
| 8.2 | **Update release orchestrator** — ensure the release workflow triggers both Windows and Linux builds atomically so every tag produces artifacts for both platforms | `.github/workflows/build-linux.yml` (or orchestrator if applicable) | Low — coordination change |
| 8.3 | **GPG sign the AppImage** — during CI release, sign the AppImage with a CI-managed GPG key, produce `Baluffo-{version}-x86_64.AppImage.asc` published alongside the AppImage | `.github/workflows/build-linux.yml` | Low — standard GPG signing |

**Verification after Phase 8:**
- Tag push triggers both Windows and Linux build workflows
- Linux CI passes: lint → typecheck → tests → build → smoke → publish
- `npm run test:py:linux` passes on Linux CI (replaces standalone Phase 5)
- GitHub Release contains both `.zip` (Windows) and `.AppImage` (Linux)
- AppImage has GPG signature file for verification

## Out of Scope

- **X11/Wayland window enumeration** — `enumerate_visible_desktop_windows()` on Linux returns empty. Acceptable because the desktop runtime is primarily headless on Linux (service-only mode without a display). Browser launch from AppImage is supported only when `$DISPLAY` or `$WAYLAND_DISPLAY` is present.
- **Linux native notifications** — `show_native_message()` falls back to `print()`. Adding `notify-send` or `zenity` is optional polish.
- **Playwright browser bundling in AppImage** — See [Phase 0a](#phase-0a-playwright-browser-on-linux-developer-workstations) for the system-Chromium workaround. Upstream Playwright v1.61 will add official Ubuntu 26.04 support, after which bundling can be re-evaluated.
- **deb/rpm/snap packaging** — out of scope for this plan. AppImage covers the primary Linux distribution use case (single file, no root required, runs on any glibc-compatible distro).

## Verification Strategy

### Per-Phase Verification

| Phase | Command | Expected |
|-------|---------|----------|
| 0 | `npm run test:py` | Passes (baseline — already passes) |
| 0 | `npm run test:py:linux` | Passes |
| 0 | `python -c "from src.ship.packaged_smoke.build_env import packaged_desktop_local_appdata_root; print(packaged_desktop_local_appdata_root('/tmp'))"` | Succeeds |
| 0a | `npm run test:frontend:linux` | Passes (requires `chromium-browser` installed via apt) |
| 1 | `python -c "from src.ship.desktop_app._linux import *"` | Succeeds |
| 1 | `npm run test:py` | Same passes + coverage for new `_linux.py` tests |
| 2 | `python -c "from src.ship.desktop_app.config import resolve_browser_session_root; print(resolve_browser_session_root())"` | Returns `~/.local/share/Baluffo` on Linux |
| 3 | `python -c "from src.source_sync_runtime import _encrypt_data; print(_encrypt_data(b'test'))"` | Returns encrypted bytes on Linux |
| 4 | `bash scripts/dev_admin_supervisor.sh --help` | Shows help text |
| 6 | `npm run verify` | Passes |
| 7 | `npm run build:linux` | Produces `dist/Baluffo-{version}-x86_64.AppImage` |
| 7 | `bash scripts/smoke_test_appimage.sh` | Bridge + site respond on HTTP |
| 8 | GitHub Release (tag push) | Release contains both `.zip` (Windows) and `.AppImage` (Linux) |

### Full Verification

Run on both Windows and Linux before claiming the phase is complete:

```bash
# On Windows (no regressions)
npm run test:py:extended

# On Linux (new behavior)
npm run test:py:linux
npm run dev:bridge  # smoke test
```

## Key Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `psutil` API differences across platforms | Low | Medium | Use same public API, test on both platforms |
| `keyring` backend not available (headless SSH) | Medium | Low | Fall back to file-based Fernet key in `~/.config/baluffo/` |
| Process group management differs from Job Objects | Medium | Medium | Test orphan-reclaim rehearsals on Linux |
| `WindowsPath` type leakage from packaged routes | Low | Medium | Fix `build_env.py` to cast `Path` to `str` before reconstruction |
| CI Ubuntu version mismatch (24.04 vs 26.04) | Medium | Low | Use compatible Python APIs |
| AppImage glibc compatibility | Low | Medium | Build on oldest supported glibc (Ubuntu 24.04 CI); AppImage bundles its own Python runtime |
| AppImage requires FUSE on host | Low | Low | Document `sudo apt install fuse` prerequisite; AppImage also supports `--appimage-extract-and-run` without FUSE |
| `appimagetool` download flakiness in CI | Low | Low | Cache tool at `_out/appimagetool/`, only re-download if missing |

## Dependencies to Add

```python
# requirements.txt additions (with user approval per AGENTS.md rule):
psutil     # Cross-platform process management
keyring    # System keyring for credential storage on Linux
# cryptography is already a transitive dep via scrapy → pyopenssl
```

**Build-time tool (not a pip dependency):**
- `appimagetool-x86_64.AppImage` — fetched from GitHub Releases during `build_portable_linux.py`, cached at `_out/appimagetool/`. No new `requirements.txt` entry needed.

## Files Created

| File | Phase | Purpose |
|------|-------|---------|
| `playwright.linux.config.js` | 0a | Linux Playwright config using system Chromium |
| `src/ship/desktop_app/_linux.py` | 1 | Linux counterpart to `_windows.py` |
| `scripts/dev_admin_supervisor.sh` | 4 | Bash launcher equivalent of `.ps1` |
| `scripts/run-bridge.sh` | 4 | Bash launcher: start bridge server |
| `scripts/run-site.sh` | 4 | Bash launcher: start site server |
| `scripts/run-all.sh` | 4 | Bash launcher: start both |
| `scripts/apply-update.sh` | 4 | Bash launcher: apply update |
| `scripts/recover-previous.sh` | 4 | Bash launcher: roll back version |
| `scripts/create-support-bundle.sh` | 4 | Bash launcher: collect diagnostics |
| `scripts/build_portable_linux.py` | 7 | Linux PyInstaller + AppImage build script |
| `packaging/AppRun` | 7 | AppImage entry point script |
| `packaging/baluffo.desktop` | 7 | XDG desktop entry for AppImage |
| `packaging/baluffo.png` | 7 | AppImage icon |
| `scripts/smoke_test_appimage.sh` | 7 | AppImage headless smoke test |
| `.github/workflows/build-linux.yml` | 8 | Linux build + release CI workflow |

## Files Modified

| File | Phase | Change |
|------|-------|--------|
| `requirements.txt` | 0, 3 | Add `psutil`, `keyring` |
| `src/ship/packaged_smoke/build_env.py` | 0, 0a, 7 | Fix `WindowsPath` crash; system Chromium resolution; platform-aware `ensure_portable_exe()` / `_exe_path_uses_default_dist()` |
| `tests/frontend/packaged-desktop-smoke*.mjs` | 0a | Add `channel: 'chromium'` fallback for Linux |
| `package.json` | 0, 0a, 7 | Add `test:py:linux`, `test:frontend:linux`, `build:linux` scripts |
| `src/ship/desktop_app/__init__.py` | 1 | Wire `_linux.py` module into `_COMPAT_MODULES` dispatch |
| `src/ship/desktop_app/config.py` | 2 | XDG fallback, Linux browser candidates |
| `src/source_sync_runtime.py` | 3 | Linux crypto path |
| `scripts/build_ship_bundle.py` | 7 | Conditionally include `.sh` launcher scripts for Linux target |
| `src/packaged_desktop_smoke.py` | 7 | Add `DEFAULT_APPIMAGE_PATH` + `DEFAULT_ELF_PATH`, platform-aware default resolution |
| `docs/WSL_SETUP.md` | 6 | Update with new capabilities |
| `docs/INDEX.md` | 6 | Add this plan |
| `docs/architecture-ai-map.md` | 6 | Add `_linux.py` routing |
| `docs/AI_ASSISTANT_GUIDE.md` | 6 | Add Linux verification shortcuts |

## Acceptance Criteria

- `python -c "from src.ship.desktop_app import *"` succeeds on Linux (already true)
- `npm run test:py:linux` passes on Linux
- `npm run dev:bridge` runs without crash on Linux (already true)
- `npm run dev:pipeline` runs without crash on Linux (already true)
- `resolve_browser_session_root()` returns an XDG-compliant path on Linux
- `packaged_desktop_local_appdata_root()` does not crash on Linux with any `Path` type
- Sync credential encrypt/decrypt roundtrip works on Linux
- All existing Windows behavior is preserved (no regressions in CI)
- No new Python or npm dependencies added without explicit approval
- `npm run build:linux` produces a working `Baluffo-{version}-x86_64.AppImage`
- AppImage runs in headless mode on CI (bridge + site respond on HTTP)
- GitHub Release contains both Windows `.zip` and Linux `.AppImage` with GPG signature

# Linux Compatibility Plan

> - **Status:** Active plan, not yet implemented
> - **Use this when:** porting Baluffo to run natively under Linux (WSL or bare-metal), adding Linux-specific platform abstractions, or fixing Linux import crashes
> - **Canonical for:** the Linux porting target inventory, known crash points, platform-gating strategy, phased implementation sequencing, and verification gaps
> - **Not canonical for:** WSL environment setup (see [`WSL_SETUP.md`](../WSL_SETUP.md)), Windows-only code changes, or current Windows desktop runtime behavior
> - **Then inspect:** [`../architecture-ai-map.md`](../architecture-ai-map.md) for `src/ship/` subsystem boundaries, [`../WSL_SETUP.md`](../WSL_SETUP.md) for dev environment, [`../testing.md`](../testing.md) for test commands, [`../../AGENTS.md`](../../AGENTS.md) for dependency rules
> - **Last updated:** 2026-05-23

## Summary

Baluffo runs on Windows today. The `src/ship/` (desktop runtime) and `src/source_sync_runtime.py` (sync credential crypto) contain Windows-only `ctypes.windll` and `ctypes.wintypes` calls that crash on `import` on Linux. The dev workflow (`npm run dev:bridge`, `npm run dev:pipeline`) already works on Linux from WSL, but a full `pip install` fails because `requirements-lock.txt` depends on `pywin32-ctypes` (a PyInstaller transitive dep) which has no Linux wheel.

This plan phases the work: first unblock the dev workflow (quick wins), then implement the platform abstraction layer for the desktop runtime, then add Linux CI and build targets.

## Current State

### Already Working on Linux (WSL2 / Ubuntu 26.04)

- Python 3.14.4, Node.js 25.8.0, npm, git
- Python venv activated with deps from `requirements.txt`
- npm `node_modules` installed via `npm ci`
- git hooks configured
- Pre-commit, gitleaks, ruff, mypy
- `npm run dev:bridge` — starts bridge + site (import chain avoids crashing modules)
- `npm run dev:pipeline` — runs jobs pipeline (import chain avoids crashing modules)
- Python tests: 1170 passed, 1 expected Windows-path failure

### What Crashes on Linux

| File | Line | Crash | Root Cause |
|------|------|-------|------------|
| `src/ship/desktop_update_shared.py` | 13 | `from ctypes import wintypes` | Module-level `import` — crashes on import before any code runs |
| `src/ship/desktop_update_shared.py` | 378–394 | `ctypes.windll.kernel32.*` calls | Inside `_pid_is_running_windows()` — but this is behind `sys.platform == "win32"` guard at line 410, so safe at call time |
| `src/ship/desktop_update.py` | 6 | `import ctypes as _ctypes` | Not a crash — `ctypes` itself is cross-platform. But this file unconditionally imports `desktop_update_shared` at line 36, which *does* crash |
| `src/ship/desktop_app/__init__.py` | 33–40 | `from src.ship.desktop_update import ...` | This `__init__.py` always imports from `desktop_update`, which imports `desktop_update_shared` — the chain crashes on Linux |
| `src/ship/desktop_app/launcher_diagnostics.py` | 137–139 | `api.ctypes.windll.user32.MessageBoxW(...)` | Inside `show_native_message()` — but there is already a `os.name == "nt"` guard at line 137, so safe at call time |
| `src/ship/desktop_app/session.py` | 237–252 | `api.ctypes.windll.kernel32.*` | Inside `_wait_for_process_exit()` — guarded by `os.name == "nt"` at higher level, but the `import` chain through `__init__.py` is the blocker |
| `src/ship/desktop_app/_windows.py` | (entire file) | `ctypes.windll.*` throughout | This is the Windows-only module — already accessed via `os.name == "nt"` guard at `__init__.py` line 28 |
| `src/source_sync_runtime.py` | 51–82 | `ctypes.windll.crypt32.*` | DPAPI credential encryption — guarded by `os.name == "nt"` at line 51, safe at call time |

### Key Insight: Import-Time vs Call-Time Safety

The only **import-time crash** is `desktop_update_shared.py` line 13 (`from ctypes import wintypes`). All other Windows-only code is behind runtime `os.name == "nt"` or `sys.platform == "win32"` guards. However, because `desktop_app/__init__.py` unconditionally imports from `desktop_update` (line 33–40), which imports `desktop_update_shared`, the entire `desktop_app` package is unimportable on Linux.

This means:
- `npm run dev:bridge` works because `dev_admin_supervisor.py` never imports `desktop_app` or `desktop_update`
- `npm run dev:pipeline` works because the pipeline import graph never touches `src/ship/`
- Any code path that touches `src/ship/desktop_app/` or `src/ship/desktop_update*.py` will crash on Linux

## Platform-Specific Code Inventory

### 1. `src/ship/desktop_app/_windows.py` (885 lines)

The primary Windows-only module. Contains:
- Process management via `ctypes.windll.kernel32` (OpenProcess, TerminateProcess, GetExitCodeProcess, CloseHandle, WaitForSingleObject, QueryFullProcessImageNameW, GetProcessTimes, GetLastError)
- Job object management via `ctypes.windll.kernel32` (CreateJobObjectW, SetHandleInformation, SetInformationJobObject, AssignProcessToJobObject)
- Window enumeration via `ctypes.windll.user32` (EnumWindows, GetClassNameW, IsWindowVisible, GetWindowTextW) and `ctypes.windll.dwmapi` (DwmGetWindowAttribute)
- TCP port listening via `win32` API (`GetExtendedTcpTable`, `GetTcpTable2`)
- `_HANDLE_FLAG_INHERIT`, `PROCESS_QUERY_LIMITED_INFORMATION`, `STILL_ACTIVE`, `JOB_OBJECT_*` constants

**Linux equivalents needed:**
- Process management → `psutil` or `os.kill(pid, 0)` + `/proc`
- Process image path → `/proc/{pid}/exe` (readlink)
- Process start time → `/proc/{pid}/stat` or `psutil`
- Process tree → `psutil.Process(pid).children()`
- Window enumeration → Nothing equivalent (headless). Return stub results.
- TCP port listener PID → `psutil.net_connections()` or `/proc/net/tcp`
- Job objects → Use process groups (`os.setpgid`, `os.killpg`) or `psutil` process tree management

### 2. `src/ship/desktop_update_shared.py` — `_pid_is_running_windows()` (lines 374–396)

Uses `kernel32.OpenProcess`/`GetExitCodeProcess`/`CloseHandle`. On Linux this is never called (guarded by `sys.platform == "win32"` at line 410 and `psutil` is tried first). The only fix needed is guarding the `from ctypes import wintypes` at line 13.

### 3. `src/source_sync_runtime.py` — DPAPI Crypto (lines 47–82)

Uses `ctypes.windll.crypt32.CryptProtectData`/`CryptUnprotectData` guarded by `os.name == "nt"`. On Linux falls back to `None` for the crypto functions. The callers need a Linux alternative — the simplest is a `cryptography` Fernet symmetric key encrypted with `keyring` (system keyring) or a file-based key.

### 4. `src/ship/desktop_app/config.py` — `resolve_browser_session_root()` (lines 259–288)

Uses `LOCALAPPDATA` environment variable (Windows). On Linux this returns `""`, falling through to `HOME` + `AppData/Local` (wrong), then to temp dir. Needs XDG Base Directory (XDG_DATA_HOME / ~/.local/share) fallback.

### 5. `src/ship/desktop_app/browser.py` — `launch_chromium_app()` (lines 258–266)

Uses `subprocess.CREATE_NEW_PROCESS_GROUP` (`os.name == "nt"`) — already guarded, Linux path works with plain `subprocess.Popen`.

### 6. `src/ship/desktop_app/process.py` — `terminate_process()` (line 113)

Uses `process.terminate()` and `process.wait()` — cross-platform, safe.

### 7. `src/ship/desktop_app/launcher_diagnostics.py` — `show_native_message()` (lines 135–142)

Already has `os.name == "nt"` guard. Falls back to `print()` on Linux. Works.

### 8. `src/ship/desktop_app/session.py` — `_wait_for_process_exit()` (lines ~230–260)

Uses `ctypes.windll.kernel32` — guarded by `os.name == "nt"` at a higher level. The path through `_wait_for_process_exit_pid()` in `_windows.py` is called via `api.terminate_process()`. On Linux, `terminate_process` goes through `process.terminate()`, so the Windows-only PID-wait path is never reached.

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

### Current `requirements-lock.txt` — Linux blocker

`pywin32-ctypes==0.2.3` (line 111) — transitive dep of `pyinstaller`. No Linux wheel. Blocks `pip install` on Linux unless `--no-deps` or `--exclude` is used.

### New dependencies needed

| Package | Purpose | Platform |
|---------|---------|----------|
| `psutil` | Cross-platform process management (already conditionally imported in `desktop_update.py`, line 54–58) | Cross-platform |
| `keyring` | System keyring for credential storage (Linux alternative to DPAPI) | Cross-platform |
| `cryptography` | Fernet symmetric encryption (fallback when keyring unavailable) | Already transitive dep via `scrapy` → `pyopenssl` → `cryptography` |

**No new npm dependencies needed.**

## Phased Implementation Plan

### Phase 0: Prerequisites & Quick Wins (unblock dev workflow)

These changes make `pip install` succeed and fix the import-time crash so that the `desktop_app` package is importable on Linux. No behavior change for Windows.

| # | Change | Files | Risk |
|---|--------|-------|------|
| 0.1 | **Fix `desktop_update_shared.py` import guard** — wrap `from ctypes import wintypes` in `if sys.platform == "win32":` with an else branch that creates a placeholder module | `src/ship/desktop_update_shared.py` | Low — Windows code path unchanged |
| 0.2 | **Guard `desktop_update.py` import of `desktop_update_shared`** — the chain `__init__.py` → `desktop_update` → `desktop_update_shared` crashes on import. Move the `from src.ship.desktop_update import ...` in `__init__.py` behind a lazy import or try/except | `src/ship/desktop_app/__init__.py` | Low — lazy import, fallback on Linux |
| 0.3 | **Remove `pywin32-ctypes` from `requirements-lock.txt`** — or regenerate lockfile without it. `pyinstaller` already handles its own deps at build time | `requirements-lock.txt` | Low — Windows CI builds pin their own env |
| 0.4 | **Add `psutil` to `requirements.txt`** and regenerate lockfile | `requirements.txt`, `requirements-lock.txt` | Low — already conditionally imported in existing code |
| 0.5 | **Add `--linux` marker to the expected Windows-path test** to clean up test output | `tests/` | None |

**Verification after Phase 0:**
- `npm run test:py` passes on Linux
- `npm run dev:bridge` starts on Linux
- `npm run dev:pipeline` runs on Linux
- The `desktop_app` package can be imported on Linux without crashing

### Phase 1: Platform Abstraction Layer (`_linux.py`)

Create `src/ship/desktop_app/_linux.py` as a counterpart to `_windows.py`. This is the largest phase.

The compat facade at `src/ship/desktop_app/_compat.py` already provides `desktop_api()` → `desktop_app` module. The `__init__.py` already conditionally imports based on `os.name`:

```python
if os.name == "nt":
    import ctypes
    import ctypes.wintypes
    import winreg
```

The pattern for `_linux.py` is:
- `__init__.py` imports both `_windows` and `_linux` (both can be imported safely on either platform since they only define functions)
- Or: use `_compat.py` to choose which platform module to export

| # | Change | Files | Risk |
|---|--------|-------|------|
| 1.1 | **Create `_linux.py`** — implement Linux equivalents for all public functions in `_windows.py` that are called through the compat dispatch | `src/ship/desktop_app/_linux.py` (new) | Medium |
| 1.2 | **Update `__init__.py`** — add `_linux` to `_COMPAT_MODULES` or choose platform-specific dispatch | `src/ship/desktop_app/__init__.py` | Low |
| 1.3 | **Implement process management** — `_pids_listening_on_tcp_port_linux()`, `_wait_for_process_exit_pid()`, `_linux_terminate_process_by_pid()`, `_linux_process_image_matches()` using `psutil` and `/proc` | `_linux.py` | Medium |
| 1.4 | **Implement process tree management** — `_linux_terminate_process_tree_details_by_pid()`, `_stale_runtime_reclaim_result()`, `_linux_try_reclaim_stale_bridge_process()`, `_linux_try_reclaim_stale_site_process()`, `_linux_reclaim_stale_runtime_children()` using `psutil.Process.children()` and process groups | `_linux.py` | Medium |
| 1.5 | **Implement job-object alternative** — `_linux_create_process_group()`, `_linux_assign_pid_to_group()`, `_linux_close_process_group()` using `os.setpgid()` / `os.killpg()` | `_linux.py` | Medium |
| 1.6 | **Window enumeration → stubs** — `_find_baluffo_visible_window()`, `_enumerate_visible_desktop_windows()`, `_windows_window_is_cloaked()` etc. return empty/None on headless Linux. X11/Wayland support is deferred | `_linux.py` | Low — no-op fallback |
| 1.7 | **Implement process identity helpers** — `get_windows_process_image_path()` → readlink `/proc/{pid}/exe`, `get_windows_process_start_ts()` → `/proc/{pid}/stat` | `_linux.py` | Low |
| 1.8 | **Update `show_native_message`** — the existing `os.name == "nt"` guard already handles this; optionally add `notify-send` or zenity on Linux | `launcher_diagnostics.py` | Low |

**Verification after Phase 1:**
- `python -c "from src.ship.desktop_app._linux import *"` succeeds
- All new functions have at least a smoke test
- `npm run test:py` still passes on Windows (no regressions)

### Phase 2: XDG Base Directory Support

| # | Change | Files | Risk |
|---|--------|-------|------|
| 2.1 | **Update `resolve_browser_session_root()`** — add XDG_DATA_HOME / `~/.local/share/Baluffo` fallback when `LOCALAPPDATA` is not set on Linux | `src/ship/desktop_app/config.py` | Low |
| 2.2 | **Update `resolve_desktop_session_root()` in `desktop_update_shared.py`** — same XDG fallback | `src/ship/desktop_update_shared.py` | Low |
| 2.3 | **Update `_resolve_runtime_path()`** — the `_looks_like_windows_absolute_path()` check at line 364 already handles Linux correctly (returns Path as-is if not a Windows path) | No change needed | None |
| 2.4 | **Update `CHROMIUM_BROWSER_CANDIDATES` in `config.py`** — add Linux binary names: `("google-chrome", "google-chrome-stable"), ("brave-browser", "brave-browser-stable"), ("chromium", "chromium-browser")` | `src/ship/desktop_app/config.py` | Low |
| 2.5 | **`APP_PATH_REGISTRY_SUBKEY`** — Windows-only registry key. The `browser.py` code already falls back to `shutil.which()` when registry lookup fails (line ~259 `if os.name != "nt"`). No change needed. | None | None |

**Verification after Phase 2:**
- Browser session root resolves to `~/.local/share/Baluffo/` on Linux
- `BALUFFO_DESKTOP_SESSION_ROOT` env override still works

### Phase 3: Linux Credential Storage (Sync Crypto)

| # | Change | Files | Risk |
|---|--------|-------|------|
| 3.1 | **Add `keyring` to `requirements.txt`** — for Linux system keyring access (GNOME Keyring / KDE Wallet / Secret Service) | `requirements.txt` | Low |
| 3.2 | **Implement `_encrypt_data_linux()` and `_decrypt_data_linux()` in `source_sync_runtime.py`** — use `cryptography.fernet.Fernet` with a key stored via `keyring` or a fallback file-based key in `~/.config/baluffo/sync.key` | `src/source_sync_runtime.py` | Medium |
| 3.3 | **Update `_encrypt_data()` / `_decrypt_data()` dispatch** — wire the Linux crypto path when `os.name != "nt"` and DPAPI is unavailable | `src/source_sync_runtime.py` | Medium |

**Verification after Phase 3:**
- Credential encrypt/decrypt roundtrip works on Linux
- Windows DPAPI path unchanged

### Phase 4: Linux Shell Launcher Scripts

| # | Change | Files | Risk |
|---|--------|-------|------|
| 4.1 | **Create `scripts/dev_admin_supervisor.sh`** — bash equivalent of the PowerShell launcher | `scripts/dev_admin_supervisor.sh` (new) | Low |
| 4.2 | **Create `scripts/build_portable_exe.sh`** — if Linux PyInstaller support is pursued | Deferred | Low |
| 4.3 | **Add npm scripts** — add `"dev:bridge:linux"` and `"dev:pipeline:linux"` entries to `package.json` if needed | `package.json` | Low |

### Phase 5: CI & Test Infrastructure

| # | Change | Files | Risk |
|---|--------|-------|------|
| 5.1 | **Add Linux test markers** — mark Windows-only tests with `@pytest.mark.windows` so they can be skipped on Linux | `tests/` | Low |
| 5.2 | **Add Linux CI workflow** — add `.github/workflows/test-linux.yml` that runs Python tests on ubuntu-latest (CI already runs on Ubuntu, but only the tests that pass — add explicit Linux-only smoke tests) | `.github/workflows/test-linux.yml` (new) | Low |
| 5.3 | **Add `npm run test:py:linux` script** — runs Python tests excluding `windows`-marked tests | `package.json` | Low |
| 5.4 | **Verify `npm run lint` and `npm run typecheck:py`** pass on Linux | — | Low |

### Phase 6: Documentation & Polish

| # | Change | Files | Risk |
|---|--------|-------|------|
| 6.1 | **Update `WSL_SETUP.md`** — add new capabilities (dev workflow now works, XDG paths) | `docs/WSL_SETUP.md` | None |
| 6.2 | **Update `INDEX.md`** — add this plan to the plans index | `docs/INDEX.md` | None |
| 6.3 | **Update `architecture-ai-map.md`** — add `_linux.py` to the desktop_app package routing | `docs/architecture-ai-map.md` | None |
| 6.4 | **Update `AI_ASSISTANT_GUIDE.md`** — add verification shortcut for Linux-specific changes | `docs/AI_ASSISTANT_GUIDE.md` | None |
| 6.5 | **Update `AGENTS.md`** — add Linux compatibility rule if needed | `AGENTS.md` | None |

## Out of Scope

- **Full Linux desktop build with PyInstaller** — PyInstaller on Linux produces Linux ELF binaries, not Windows EXEs. A Linux AppImage build is a separate project.
- **X11/Wayland window enumeration** — `_enumerate_visible_desktop_windows()` on Linux returns empty. This is acceptable because the desktop runtime is primarily headless on Linux (TUI tool support, not GUI desktop).
- **Linux native notifications** — `show_native_message()` falls back to `print()`. Adding `notify-send` or `zenity` is optional polish.
- **Playwright browser tests on Linux** — Playwright does not support Ubuntu 26.04 yet. Existing CI already runs on `ubuntu-latest` (24.04). WSL users should run browser tests on Windows or rely on CI.
- **Porting `scripts/build_portable_exe.py`** — the PyInstaller build is Windows-only (`pefile` dep, EXE output). A Linux build script would need different packaging tooling (AppImage, deb, snap).

## Verification Strategy

### Per-Phase Verification

| Phase | Command | Expected |
|-------|---------|----------|
| 0 | `python -c "from src.ship.desktop_app import *"` | Succeeds on Linux |
| 0 | `npm run test:py` | Passes (excluding windows-marked tests) |
| 1 | `python -c "from src.ship.desktop_app._linux import resolve_chromium_browser_candidates"` | Succeeds |
| 1 | `npm run test:py` | Same passes + coverage for new _linux.py tests |
| 2 | `python -c "from src.ship.desktop_app.config import resolve_browser_session_root; print(resolve_browser_session_root())"` | Returns `~/.local/share/Baluffo` on Linux |
| 3 | `python -c "from src.source_sync_runtime import _encrypt_data; print(_encrypt_data(b'test'))"` | Returns encrypted bytes on Linux |
| 4 | `bash scripts/dev_admin_supervisor.sh --help` | Shows help text |
| 5 | `npm run test:py:linux` | Passes on Linux CI |
| 6 | `npm run verify` | Passes |

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
| `desktop_update_shared` root injection pattern fragile | Low | Medium | The root module (`desktop_update.py`) already conditionally tries `psutil` and guards platform code |
| CI Ubuntu version mismatch | Medium | Low | CI runs `ubuntu-latest` (24.04). User's WSL runs 26.04. Use compatible Python APIs. |

## Dependencies to Add

```python
# requirements.txt additions (with user approval per AGENTS.md rule):
psutil     # Cross-platform process management
keyring    # System keyring for credential storage on Linux
# cryptography is already a transitive dep via scrapy → pyopenssl
```

## Files Created

| File | Phase | Purpose |
|------|-------|---------|
| `src/ship/desktop_app/_linux.py` | 1 | Linux counterpart to `_windows.py` |
| `scripts/dev_admin_supervisor.sh` | 4 | Bash launcher equivalent of `.ps1` |
| `.github/workflows/test-linux.yml` | 5 | Linux CI workflow (optional) |

## Files Modified

| File | Phase | Change |
|------|-------|--------|
| `src/ship/desktop_update_shared.py` | 0 | Guard `from ctypes import wintypes` with `sys.platform == "win32"` |
| `src/ship/desktop_app/__init__.py` | 0 | Lazy-import desktop_update on Linux |
| `requirements-lock.txt` | 0 | Remove or regenerate without `pywin32-ctypes` |
| `requirements.txt` | 0, 3 | Add `psutil`, `keyring` |
| `src/ship/desktop_app/config.py` | 2, 4 | XDG fallback, Linux browser candidates |
| `src/source_sync_runtime.py` | 3 | Linux crypto path |
| `src/ship/desktop_app/__init__.py` | 1 | Wire `_linux.py` module |
| `package.json` | 4, 5 | Add Linux-specific npm scripts |
| `docs/WSL_SETUP.md` | 6 | Update with new capabilities |
| `docs/INDEX.md` | 6 | Add this plan |
| `docs/architecture-ai-map.md` | 6 | Add `_linux.py` routing |
| `docs/AI_ASSISTANT_GUIDE.md` | 6 | Add Linux verification shortcuts |

## Acceptance Criteria

- `python -c "from src.ship.desktop_app import *"` succeeds on Linux
- `npm run test:py` passes on Linux (excluding known Windows-only tests)
- `npm run dev:bridge` runs without crash on Linux
- `npm run dev:pipeline` runs without crash on Linux
- `resolve_browser_session_root()` returns an XDG-compliant path on Linux
- Sync credential encrypt/decrypt roundtrip works on Linux
- All existing Windows behavior is preserved (no regressions in CI)
- No new Python or npm dependencies added without explicit approval

# WSL Setup

> - **Status:** Active
> - **Use this when:** you need to run Baluffo from Windows Subsystem for Linux 2 (WSL2)
> - **Canonical for:** WSL environment setup, available tooling, and daily workflow
> - **Not canonical for:** local storage model, release sequencing, or the full verification matrix
> - **Then inspect:** [`LOCAL_SETUP.md`](LOCAL_SETUP.md) for local-first commands, [`testing.md`](testing.md) for test lanes, and [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) for AI agent context
> - **Last updated:** 2026-05-25

## Why WSL

Baluffo is a Python + Node.js project. WSL2 gives you a native Linux environment on Windows with proper TTY support, signal handling, and file watching — avoiding the quirks of PowerShell/CMD for TUI tools like Codex or OpenCode.

## What was set up

| Component | Version | Notes |
|-----------|---------|-------|
| **Ubuntu** | 26.04 LTS | WSL2 distro, default user `Andrea` |
| **Python** | 3.14.4 | System-installed via apt |
| **Node.js** | 25.8.0 | Installed via nvm, symlinked to `/usr/local/bin` |
| **npm** | 11.11.0 | Bundled with Node |
| **Git** | 2.53.0 | System-installed |
| **pre-commit** | 4.5.1 | Installed system-wide |
| **gitleaks** | 8.30.1 | Secret scanning |
| **uv** | latest | Python package resolver |
| **Windows Terminal** | latest | Installed via winget on Windows |

### AI coding tools

| Tool | Version | Path |
|------|---------|------|
| **OpenCode** | 1.15.10 | `/usr/local/bin/opencode` |
| **Codex CLI** | 0.133.0 | `/usr/local/bin/codex` |

To start either TUI tool inside the project:

```bash
cd ~/code/Baluffo
opencode
# or
codex
```

### Python virtual environment

Located at `~/code/Baluffo/.venv`.

All project dependencies from `requirements.txt` are installed there.

## Getting started

### 1. Launch WSL

Open **Windows Terminal** and select the **Ubuntu** tab, or run:
```powershell
wsl
```

### 2. Navigate to the project

```bash
cd ~/code/Baluffo
```

> The repo is cloned in WSL's native filesystem (`~/code/Baluffo`) for best performance.
> A Windows-side clone also exists at `/mnt/c/Users/Andrea/Documents/GitHubRepository/Baluffo`
> for Windows-native tooling. Use the WSL path for TUI tools and day-to-day development.

### 3. Activate the Python virtual environment

```bash
source .venv/bin/activate
```

### 4. Verify everything is available

```bash
python --version    # 3.14.4
node --version      # 25.8.0
npm --version       # 11.11.0
git --version       # 2.53.0
```

## Daily commands

All commands run inside WSL after `cd`-ing to the project root.

| Goal | Command |
|------|---------|
| Activate Python venv | `source .venv/bin/activate` |
| Start local launcher | `npm run dev:bridge` |
| Run jobs pipeline | `npm run dev:pipeline` |
| Python tests | `npm run test:py` |
| Frontend smoke tests | `npm run test:smoke` (requires Playwright — see note below) |
| Full build | `npm run build` |
| Full verification | `npm run verify` |
| Python lint | `npm run lint:py` |
| Python type check | `npm run typecheck:py` |
| Frontend lint | `npm run lint:frontend` |
| Update locked Python deps | `uv pip compile requirements.txt -o requirements-lock.txt` |
| Install locked Python deps | `pip install -r requirements-lock.txt` |
| Install npm deps (clean) | `npm ci` |

## Project structure

```
~/code/Baluffo/
├── src/                  # Python backend
├── frontend/             # HTML/CSS/JS frontend
├── tests/                # Python + frontend tests
├── scripts/              # Build, test, and dev scripts
├── docs/                 # Documentation
├── data/                 # Runtime data, defaults, contracts
├── packaging/            # Desktop packaging configs
├── .venv/                # Python virtual environment
└── node_modules/         # npm dependencies
```

## Repository locations

There are two clones of Baluffo — each for its purpose:

| Location | Filesystem | Performance | Use for |
|----------|-----------|-------------|---------|
| `~/code/Baluffo` | WSL native (`ext4`) | ✅ Fast | TUI tools (OpenCode, Codex), daily dev, tests, git operations |
| `/mnt/c/Users/Andrea/.../Baluffo` | Windows NTFS (via DrvFs) | ⚠️ Slower | Windows-native tools, VS Code, PyInstaller builds |

Both clones are kept in sync — push/pull from either.

## Accessing Windows files

WSL mounts your Windows drives under `/mnt/`:

| Path | Windows equivalent |
|------|-------------------|
| `/mnt/c/` | `C:\` |
| `/mnt/d/` | `D:\` |
| `/mnt/c/Users/Andrea/...` | `C:\Users\Andrea\...` |

**Performance note:** file operations on `/mnt/c/` are slower than WSL's native filesystem (`~`). For heavy git operations or builds, consider cloning into `~/code/` instead. You can access those files from Windows via `\\wsl$\Ubuntu\home\Andrea\code\`.

## Path resolution

Windows PATH appending is **disabled** in WSL (`/etc/wsl.conf` has `appendWindowsPath = false`). This prevents conflicts between Windows and Linux tools (e.g. two different Node.js versions). The Linux versions of `node`, `npm`, `python3`, and `git` are symlinked into `/usr/local/bin/` and always resolved first.

To run a Windows executable from WSL, use its full `/mnt/c/...` path or the `.exe` suffix (e.g. `notepad.exe`, `explorer.exe`).

## Configuration files

| File | Location | Purpose |
|------|----------|---------|
| `wsl.conf` | `/etc/wsl.conf` (in WSL) | WSL interop, network, and user defaults |
| `.wslconfig` | `C:\Users\Andrea\.wslconfig` | Global WSL2 resource limits (memory, CPUs) |
| `.venv/` | Project root | Python virtual environment |
| `.nvm/` | `/home/Andrea/.nvm/` | Node version manager |

### `.wslconfig` (Windows host)

Create this file at `C:\Users\Andrea\.wslconfig` to limit WSL2 resource usage:

```ini
[wsl2]
memory=8GB
processors=4
swap=4GB
```

Apply changes: `wsl --shutdown` then restart WSL.

## Linux Compatibility Status

As of May 2026, the Linux compatibility plan (see [`docs/plans/linux-compatibility-plan.md`](plans/linux-compatibility-plan.md)) is fully implemented. Here's what works on Linux:

### Test suite

| Command | Status | Notes |
|---------|--------|-------|
| `npm run test:py:linux` | Passes (2996 tests) | Excludes `@pytest.mark.windows` tests |
| `npm run test:py:extended` | Passes (3177 tests) | Full suite, including Windows-marked tests |
| `npm run test:frontend:linux` | Opt-in | Requires system `chromium-browser`; set `PLAYWRIGHT_SYSTEM_CHROMIUM=1` |
| `npm run dev:bridge` | Works | No changes needed |
| `npm run dev:pipeline` | Works | No changes needed |

### New capabilities

- **Platform abstraction:** `src/ship/desktop_app/_linux.py` provides Linux equivalents of all `_windows.py` functions (process management, stale runtime reclamation, TCP port listening). Platform dispatch is handled at import time in `__init__.py`.
- **XDG paths:** Session root resolves to `~/.local/share/Baluffo/` on Linux. Config uses `~/.config/baluffo/`.
- **Credential storage:** Sync key cache uses `cryptography.fernet.Fernet` with a key stored via system keyring, falling back to `~/.config/baluffo/sync.key` (0o600).
- **Shell launchers:** Seven `.sh` scripts in `src/ship/` mirror the PowerShell `.ps1` launchers: `run-bridge.sh`, `run-site.sh`, `run-all.sh`, `apply-update.sh`, `recover-previous.sh`, `create-support-bundle.sh`, `dev_admin_supervisor.sh`.
- **AppImage build:** `npm run build:linux` produces a self-contained `Baluffo-{version}-x86_64.AppImage` via PyInstaller + appimagetool. CI workflow at `.github/workflows/build-linux.yml`.
- **Playwright workaround:** Ubuntu 26.04 uses system `chromium-browser` via `PLAYWRIGHT_SYSTEM_CHROMIUM=1` and `PACKAGED_SMOKE_SYSTEM_CHROMIUM=1` env vars. Once Playwright v1.61 ships, the bundled Chromium can replace this workaround.

## Known limitations

### Playwright browser tests

Playwright does **not** support Ubuntu 26.04 yet. Frontend smoke tests (`npm run test:smoke`) that require Playwright browsers cannot run inside WSL. Run those natively on Windows or rely on CI.

Workarounds if you need browser tests locally:

- Run `npx playwright test` from PowerShell/CMD on Windows (not WSL)
- The PyInstaller portable EXE build is Windows-only and runs in GitHub CI

### `python` vs `python3`

The `python` command points to `python3` via the `python-is-python3` package. Always use `python3` explicitly if running scripts that depend on the system interpreter; the venv handles this automatically once activated.

### WSL resource usage

WSL2 can consume significant memory over time. If the environment feels slow:

```powershell
# From PowerShell (Windows host)
wsl --shutdown
```

Or limit resources via `.wslconfig` (see above).

## Restarting WSL

| Action | Command |
|--------|---------|
| Restart WSL | `wsl --shutdown` then launch WSL again |
| Restart Ubuntu only | `wsl --terminate Ubuntu` |
| Check WSL status | `wsl -l -v` |
| Check WSL version | `wsl --version` |

## Related docs

- [`LOCAL_SETUP.md`](LOCAL_SETUP.md) — local-first storage model and dev commands
- [`testing.md`](testing.md) — full test matrix (some lanes require Windows)
- [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) — AI agent task routing and edit boundaries
- [`environments.md`](environments.md) — release-path separation and sync transport

# AI Coder Setup

> - **Status:** Active
> - **Use this when:** preparing Windows, WSL, or Linux for AI-assisted Baluffo work
> - **Canonical for:** AI-coder environment checks, local toolbelt setup, and setup drift triage
> - **Not canonical for:** runtime storage contracts, release sequencing, or subsystem implementation behavior
> - **Then inspect:** [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`WSL_SETUP.md`](WSL_SETUP.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), and [`testing.md`](testing.md) as needed
> - **Last updated:** 2026-06-01

This repo keeps AI-coder tooling contributor-local. These tools are not Baluffo runtime, packaging, CI, Python, Node, or pre-commit dependencies.

## First Check

From the repo root:

```bash
python scripts/ai_env_check.py --smoke
```

This reports Python, Node, npm, Serena, lockfiles, local dependency folders, Git hooks, the lean AI toolbelt, Playwright, and obvious WSL path problems.

For machine-readable output:

```bash
python scripts/ai_env_check.py --smoke --json
```

To also check whether Serena is behind the current PyPI release:

```bash
python scripts/ai_env_check.py --smoke --check-updates
```

## Serena MCP

Install or update the required code-intelligence MCP:

```bash
uv tool install --force -p 3.13 "serena-agent==1.6.1"
serena --version
```

Then follow [`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md) for Codex/OpenCode registration and repo-local health checks.

## Toolbelt

Install or repair the default AI-coder toolbelt:

```bash
python scripts/toolbelt_check.py --install
python scripts/toolbelt_check.py --smoke
```

Default tools are `rg`, `fd`, `bat`, `jq`, `yq`, `ast-grep`, and `tokei`.

`mlr` is optional for focused CSV/TSV/JSONL work. Do not use broad repo packers or context generators by default.

## Windows Native

Use the Windows clone for Windows-specific packaging, desktop smoke, and IDE work:

```powershell
cd C:\Users\Andrea\Documents\GitHubRepository\Baluffo
python scripts\ai_env_check.py --smoke
python scripts\toolbelt_check.py --install
npm ci
python -m pip install -r requirements-lock.txt
python scripts\install_git_hooks.py
```

If `winget` installs a tool but direct commands still fail, restart the IDE or shell. The checker also searches known Winget package directories for retained tools.

## WSL / Linux

Prefer the WSL native filesystem clone for Codex/OpenCode TUI work and Linux validation:

```bash
cd ~/code/Baluffo
source .venv/bin/activate
python scripts/ai_env_check.py --smoke
python scripts/toolbelt_check.py --install
npm ci
python -m pip install -r requirements-lock.txt
python scripts/install_git_hooks.py
```

If using `uv`, sync the Python environment from the lockfile:

```bash
uv pip sync requirements-lock.txt
```

Avoid running day-to-day WSL work from `/mnt/c/...`; it is slower and can create avoidable file-watcher and path issues.

## Minimal Daily Preflight

Use this when an AI session starts in an unfamiliar shell or after dependency/tooling changes:

```bash
python scripts/ai_env_check.py --smoke
```

Use `--check-updates` after Serena changes or when setup drift is suspected.

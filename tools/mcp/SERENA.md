# Serena MCP for Baluffo

> Required AI development tooling for this repo. Codex CLI and OpenCode are the two first-class client paths.

## What This Owns

Use this guide when you are setting up Serena for Baluffo repo work.

- Serena is the required AI dev tool for this repo's normal coding workflow.
- This is a contributor-workflow standard only. It is not part of Baluffo runtime, packaging, release, or CI.
- The repo docs stay canonical if Serena memory and repo docs ever diverge.
- `.serena/` is local-only state and must stay untracked.

For Codex browser inspection, use the built-in in-app Browser and Developer mode; do not add Chrome DevTools MCP or `@playwright/mcp` for Baluffo. The repo-local Playwright MCP in [PLAYWRIGHT.md](PLAYWRIGHT.md) is retained only as a deprecated fallback for non-Codex clients.

## Install Serena

Follow Serena's official `uv`-managed install path rather than marketplace installs:

1. Install `uv`: <https://docs.astral.sh/uv/getting-started/installation/>
2. Install Serena:

```powershell
uv tool install --force -p 3.13 "serena-agent"
serena --help
```

If `serena` is not found after install, restart your shell so the `uv` tool path is available.
For direct CLI use on Windows, the `uv`-managed executable is typically available at
`$env:USERPROFILE\.local\bin\serena.exe`.

Update Serena explicitly; normal repo checks should report stale installs, not auto-upgrade them:

```powershell
uv tool install --force -p 3.13 "serena-agent"
serena --version
```

If Windows reports `serena.exe` is locked, stop only running Serena MCP processes, rerun the upgrade,
then restart or reconnect Codex/OpenCode so they open a fresh MCP stdio transport.

### Certified Local Baseline

This setup was validated on Windows with:

- latest `serena-agent` release resolved at setup time
- Python 3.13
- Language-server backend with `python` and `typescript`

Upstream references:
- Serena upstream: <https://github.com/oraios/serena>
- OpenAI Docs MCP / Codex MCP setup: <https://developers.openai.com/learn/docs-mcp>
- Codex CLI getting started: <https://help.openai.com/en/articles/11096431-openai-codex-ci-getting-started>

## First-Class Clients

### Codex CLI

Codex is a first-class client for this repo through a Baluffo-specific profile. The base Codex configuration intentionally has no Serena entry, so Serena does not start in unrelated workspaces.

```powershell
codex --profile baluffo
```

The profile lives at `$env:CODEX_HOME\baluffo.config.toml` and resolves the latest Serena release from `uvx`:

```toml
[mcp_servers.serena]
command = "uvx"
args = ["-p", "3.13", "--from", "serena-agent@latest", "serena", "start-mcp-server", "--context", "ide", "--project-from-cwd"]
```

Do not run `codex mcp add serena` from this repo: that command writes the user-global Codex configuration and would restore unwanted startup in other workspaces. Starting Codex with `--profile baluffo` loads this profile's Serena registration, while the unpinned package reference resolves the latest available Serena release. The profile must contain the `mcp_servers.serena` block above and use `--project-from-cwd`, so Codex and OpenCode both activate the current workspace instead of relying on a stale registered project.

### OpenCode

OpenCode is the other first-class client for this repo.
Baluffo already commits an `opencode.json` reference config that expects Serena on `PATH`.
Use Serena's generic `ide` context here; upstream Serena does not publish an OpenCode-specific context today.

Current repo launch shape:

```json
{
  "mcp": {
    "serena": {
      "type": "local",
      "command": ["uvx", "-p", "3.13", "--from", "serena-agent@latest", "serena", "start-mcp-server", "--context", "ide", "--project-from-cwd"],
      "enabled": true
    }
  }
}
```

Install Serena once, then run OpenCode from the repo root so it can use the committed repo config.
Unlike Codex's registered MCP entry, this committed OpenCode config still expects `serena` on `PATH`.
If OpenCode cannot resolve `serena`, restart your shell/session, add the user-local tool directory to
`PATH`, or override the command in machine-local client configuration. Do not commit absolute
`serena.exe` paths to `opencode.json`; the tracked file stays portable across machines.

## Baluffo Local Project Setup

After installing Serena, create the repo-local project config from the repo root:

```powershell
serena project create --language python --language typescript
serena project health-check
```

This creates `.serena/project.yml` as local-only state for this clone.
Do not commit `.serena/`.

Baluffo should use both `python` and `typescript` in Serena project config.
There is no separate JavaScript Serena language key; use `typescript` for JavaScript and TypeScript files.

Serena's managed TypeScript language-server flow requires Node.js and npm.
Those are already present on this machine, so no extra repo dependency step is needed.

## Setup Health Check

Use this section to verify the current clone and client session after installing Serena.

### Expected Codex Session State

- Serena MCP tools loaded successfully after starting Codex with the `baluffo` profile.
- The Serena project was visible as `Baluffo`, but the session still needed an explicit project activation on first use.
- One-time Serena onboarding had not been completed yet for this clone; after onboarding, `check_onboarding_performed` reported 5 project memories.

### Verification Commands

Use these checks when you want to confirm the local Serena setup is actually working:

```powershell
Get-Content "$env:CODEX_HOME\baluffo.config.toml"
uvx -p 3.13 --from serena-agent serena project health-check
```

Expected working results in this repo:

- The profile contains a live Serena MCP registration using the unpinned `uvx` command.
- `serena project health-check` passes successfully.
- The health check started both configured language servers:
  - Python via Pyright
  - TypeScript via `typescript-language-server`
- Serena reports the version resolved by the current unpinned package reference.
- The repo-local Serena health-check log was written under `.serena/logs/health-checks/`.

### Verified Repo-Local Project State

- `.serena/project.yml` exists and is configured for `python` and `typescript`.
- `.serena/project.local.yml` exists for local-only overrides.
- `.serena/` remains local-only state and must stay untracked.

## Secondary Client Examples

Cursor, Cline, and Windsurf are supported examples, but they are not the repo's primary standard.

### Cursor

```json
{
  "name": "baluffo-serena",
  "command": "serena",
  "args": ["start-mcp-server", "--context", "ide", "--project-from-cwd"],
  "env": {}
}
```

### Cline

```json
{
  "mcpServers": {
    "baluffo-serena": {
      "command": "serena",
      "args": ["start-mcp-server", "--context", "ide", "--project-from-cwd"]
    }
  }
}
```

### Windsurf

Use the same command and args:

```json
{
  "mcpServers": {
    "baluffo-serena": {
      "command": "serena",
      "args": ["start-mcp-server", "--context", "ide", "--project-from-cwd"]
    }
  }
}
```

## Fresh Session Bootstrap (No Guesswork)

When a Codex/OpenCode session is restarted, start Codex with the `baluffo` profile and run OpenCode from the Baluffo repo root so Serena can activate the current project.
Use this exact sequence once per new session:

1. Start Codex with the Baluffo profile so its project-level Serena registration is loaded:

```powershell
codex --profile baluffo
```

2. In the Serena toolset, confirm active project state:

```python
mcp__serena__.get_current_config
```

3. If the response is `Error: No active project...`, activate the repo project explicitly:

```python
mcp__serena__.activate_project project="Baluffo"
```

4. Re-run the config check and verify both required languages are loaded:

```python
mcp__serena__.get_current_config
```

Expected snippet:

- Active project: `Baluffo`
- Programming languages: `typescript, python`

5. Run a one-shot JavaScript symbol check to verify TS tooling:

```python
mcp__serena__.get_symbols_overview relative_path="frontend/admin/actions.js" depth=1
```

Expected: symbol list should include `ADMIN_ACTIONS` and `createAdminDispatcher`.

6. Optional declaration check on a known symbol:

```python
mcp__serena__.find_symbol name_path_pattern="createAdminDispatcher" relative_path="frontend/admin/actions.js" include_body=true
```

If any step fails, continue with the existing health and install checks; avoid repeating ad-hoc discovery in a loop.

### Stale Registered Project Cleanup

If Serena tries to activate an old temporary worktree path instead of the current Baluffo clone,
inspect `$env:USERPROFILE\.serena\serena_config.yml` and remove any stale path under the global
`projects:` list. The only Baluffo project entry should point to:

```text
C:\Users\Andrea\Documents\GitHubRepository\Baluffo
```

## Repo Rules

- Start with [AI_ASSISTANT_GUIDE.md](../../docs/AI_ASSISTANT_GUIDE.md) and [INDEX.md](../../docs/INDEX.md) before relying on Serena memory.
- Use Serena to accelerate symbol-aware navigation, cross-file reasoning, and refactors; use the repo's normal shell/file tools for small direct edits and verification.
- If Serena memory and repo docs disagree, repo docs win.
- Do not commit `.serena/`.

### Usage Boundary

- Use Serena for code intelligence, symbol-aware navigation, cross-file reasoning, and refactor support.
- Do not treat Serena memory as canonical when it conflicts with repo docs or source.
- Use direct file/shell tools for small deterministic edits and verification when that is simpler.
- Do not loop through repeated Serena discovery calls when a direct file path or contract doc already answers the question.

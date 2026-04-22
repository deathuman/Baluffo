# Serena MCP for Baluffo

> Required AI development tooling for this repo. Codex CLI and OpenCode are the two first-class client paths.

## What This Owns

Use this guide when you are setting up Serena for Baluffo repo work.

- Serena is the required AI dev tool for this repo's normal coding workflow.
- This is a contributor-workflow standard only. It is not part of Baluffo runtime, packaging, release, or CI.
- The repo docs stay canonical if Serena memory and repo docs ever diverge.
- `.serena/` is local-only state and must stay untracked.

For the repo's optional browser-driving MCP server, see [PLAYWRIGHT.md](PLAYWRIGHT.md).

## Install Serena

Follow Serena's official `uv`-managed install path rather than marketplace installs:

1. Install `uv`: <https://docs.astral.sh/uv/getting-started/installation/>
2. Install Serena:

```powershell
uv tool install -p 3.13 serena-agent@latest --prerelease=allow
serena --help
```

If `serena` is not found after install, restart your shell so the `uv` tool path is available.
For direct CLI use on Windows, the `uv`-managed executable is typically available at
`$env:USERPROFILE\.local\bin\serena.exe`.

Upstream references:
- Serena upstream: <https://github.com/oraios/serena>
- OpenAI Docs MCP / Codex MCP setup: <https://developers.openai.com/learn/docs-mcp>
- Codex CLI getting started: <https://help.openai.com/en/articles/11096431-openai-codex-ci-getting-started>

## First-Class Clients

### Codex CLI

Codex is a first-class client for this repo. Prefer Codex's MCP command flow instead of hand-editing config files:

```powershell
codex mcp add serena -- serena start-mcp-server --context=codex --project-from-cwd
codex mcp list
codex mcp get serena
```

`codex mcp add` writes the user-global Codex MCP config for you. Keep that config user-local rather than committing a repo-managed Codex config file.
Codex resolves and stores the Serena executable path for you. In the verified Baluffo setup on
2026-04-22, `codex mcp get serena` reported:

- `enabled: true`
- `transport: stdio`
- `command: C:\Users\AMolino\.local\bin\serena.exe`
- `args: start-mcp-server --context=codex --project-from-cwd`
- `env: SERENA_HOME=...`

That means Codex can keep working even if a fresh PowerShell session still does not resolve the bare
`serena` command on `PATH`.

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
      "command": ["serena", "start-mcp-server", "--context", "ide", "--project-from-cwd"],
      "enabled": true
    }
  }
}
```

Install Serena once, then run OpenCode from the repo root so it can use the committed repo config.
Unlike Codex's registered MCP entry, this committed OpenCode config still expects `serena` on `PATH`.
If OpenCode cannot resolve `serena`, restart your shell/session or change the local command to the
explicit `serena.exe` path.

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

## Verified Working Baseline

This repo's Serena setup was re-verified from a fresh Codex session on 2026-04-22.

### Verified Codex Session State

- Serena MCP tools loaded successfully after rebooting the Codex session.
- The Serena project was visible as `Baluffo`, but the session still needed an explicit project activation on first use.
- One-time Serena onboarding had not been completed yet for this clone; after onboarding, `check_onboarding_performed` reported 5 project memories.

### Verified Commands

Use these checks when you want to confirm the local Serena setup is actually working:

```powershell
codex mcp get serena
& "$env:USERPROFILE\.local\bin\serena.exe" project health-check
```

Observed working results in this repo:

- `codex mcp get serena` showed a live stdio MCP registration using `C:\Users\AMolino\.local\bin\serena.exe`.
- `serena.exe project health-check` passed successfully.
- The health check started both configured language servers:
  - Python via Pyright
  - TypeScript via `typescript-language-server`
- Serena reported version `1.1.2` from the active MCP session.
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

## Repo Rules

- Start with [AI_ASSISTANT_GUIDE.md](../../docs/AI_ASSISTANT_GUIDE.md) and [INDEX.md](../../docs/INDEX.md) before relying on Serena memory.
- Use Serena to accelerate symbol-aware navigation, cross-file reasoning, and refactors; use the repo's normal shell/file tools for small direct edits and verification.
- If Serena memory and repo docs disagree, repo docs win.
- Do not commit `.serena/`.

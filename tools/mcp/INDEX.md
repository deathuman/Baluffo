# MCP Tooling Index

> Directory landing page for Baluffo MCP tooling. Use this page to choose the right MCP guide without loading every leaf doc.

## What Lives Here

`tools/mcp/` owns repo MCP tooling docs and MCP server entrypoints used during development. Codex browser inspection uses the built-in in-app Browser and Developer mode from `docs/AI_ASSISTANT_GUIDE.md`, not Chrome DevTools MCP or `@playwright/mcp`.

- Use [SERENA.md](SERENA.md) for the required repo AI tooling standard.
- Use [BASIC_MEMORY.md](BASIC_MEMORY.md) for the required external AI continuity memory (cross-client handoff, project gotchas, current focus).
- Use [PLAYWRIGHT.md](PLAYWRIGHT.md) only for the deprecated local Playwright MCP fallback when a non-Codex client lacks built-in browser control.

## Start Here

| Document | Role | Use it when |
|----------|------|-------------|
| [SERENA.md](SERENA.md) | Required AI tooling | You are setting up Serena for Codex CLI or OpenCode, or checking the repo's Serena-memory rules |
| [BASIC_MEMORY.md](BASIC_MEMORY.md) | Required external AI continuity memory | You need durable cross-client handoff notes, project gotchas, and current focus across Codex/OpenCode/Cline |
| [PLAYWRIGHT.md](PLAYWRIGHT.md) | Deprecated fallback tooling | A non-Codex client lacks built-in browser control and the user explicitly wants the local Playwright MCP fallback |

## Which MCP Tool Should I Use?

| Need | Start here |
|------|------------|
| Standard AI-assisted repo work | [SERENA.md](SERENA.md) |
| Symbol-aware navigation and refactors | [SERENA.md](SERENA.md) |
| Codex browser actions, screenshots, console/network inspection, or page inspection | [`docs/AI_ASSISTANT_GUIDE.md`](../../docs/AI_ASSISTANT_GUIDE.md) and Codex in-app Browser Developer mode |
| Non-Codex browser fallback when explicitly requested | [PLAYWRIGHT.md](PLAYWRIGHT.md) |
| Cross-client AI handoff memory | [BASIC_MEMORY.md](BASIC_MEMORY.md) |
| General repo coding task with no browser interaction | [SERENA.md](SERENA.md) plus [BASIC_MEMORY.md](BASIC_MEMORY.md) when the task creates durable continuity value |

## Freebuff Clients (MCP Client Status)

Freebuff ships two separate clients with different MCP behavior. Verified 2026-08-17 against Freebuff
**CLI** 0.0.149 (npm `freebuff`) and Freebuff **Desktop** 0.0.63 (orchestrator build from 2026-08-15),
including the public [`CodebuffAI/freebuff`](https://github.com/CodebuffAI/freebuff) source. Desktop MCP
behavior re-verified 2026-08-21 (current build reads user-scope `~/.agents/mcp.json`).

### Freebuff CLI — supported native MCP path (use this)

The CLI natively loads `.agents/mcp.json` and merges its servers into the base agent, so the main agent
gets `mcp__<server>__<tool>` tools directly:

- Reads `.agents/mcp.json` from `{cwd}/.agents`, `{cwd}/../.agents`, and `~/.agents` at startup
  (`initializeAgentRegistry()` → `loadMCPConfigSync()`). Later directories override earlier ones.
- Merges every configured server into each agent whose id starts with `base`
  (`loadAgentDefinitions()`), then exposes them as `mcp__<server>__<tool>` tools.
- Also loads custom `.agents/*.ts` agent definitions, auto-adds their ids to base agents'
  `spawnableAgents`, and lists them in the `@` menu.
- This repo's `.agents/mcp.json` registers `serena` and `basic-memory` (same commands as
  `opencode.json`). Verified 2026-08-17 on CLI 0.0.149: the client log
  (`~/.config/manicode/projects/Baluffo/chats/<timestamp>/log.jsonl`) shows
  `[agents] Loaded MCP servers from mcp.json` with `["serena","basic-memory"]`.

Use it with `cd` into the repo and run `freebuff`; MCP servers start lazily on first tool call.

### Freebuff Desktop — current builds read user-scope `~/.agents/mcp.json`

Re-verified 2026-08-21: the current Desktop build's MCP settings surface reads `~/.agents/mcp.json`
("Tools from your MCP servers, available to every agent. Read from `~/.agents/mcp.json` — the same
file the CLI uses"), merging each configured server into the agent's tool list; new servers become
available after adding/editing the file and reloading the app. Only the **user-scope** file is read —
whether the Desktop build also reads repo-scoped `{cwd}/.agents/mcp.json` like the CLI is not yet
verified. This repo works via the user-scope file (created 2026-08-21 with `serena` and `basic-memory`,
same commands as `opencode.json`).

Historical baseline (Desktop 0.0.63, 2026-08-17): the orchestrator did **not** load `.agents/mcp.json`
— `loadMCPConfig` was never called in the installed bundle — and the hosted `codebuff` engine's tool
list came from the cloud-fetched base agent template plus Freebuff's own file/terminal/web tools.
`.freebuff/settings.json` has no MCP surface (only `startupScript`). Engine specifics as of 0.0.63
(re-verify now that the MCP loader is wired in):

- `claude-code` engine: fully locked down. The harness runs the Claude Code SDK with
  `settingSources: ["user"]` and `strictMcpConfig: true`, passing only Freebuff's own `freebuff` MCP
  server, so project `.mcp.json` **and** user-scope `~/.claude.json` MCP servers are both ignored.
- `codex` engine: spawns the installed Codex app-server alongside the **global** `~/.codex/config.toml`;
  `basic-memory` is registered there for this repo. `serena` lives only in the repo-scoped `baluffo`
  profile (`codex --profile baluffo`), so the Codex engine does not inherit it; adding it to the global
  config conflicts with the repo policy of keeping Serena out of unrelated workspaces.
- `codebuff` engine: the only native MCP surface is custom `.agents/*.ts` agent definitions declaring
  `mcpServers` (loaded via `loadLocalAgents`, exposed via `getMCPToolData`), but the Desktop UI does not
  expose custom agents, so it is not usable end-to-end yet.

Practical route for Desktop users: MCP servers work through the user-scope `~/.agents/mcp.json` (add
servers there, then reload). MCP-heavy work can also run in the Freebuff **CLI** (above), or in
**Codex CLI** (`codex --profile baluffo`) / **OpenCode** from the repo root, which load both MCPs per
[SERENA.md](SERENA.md) and [BASIC_MEMORY.md](BASIC_MEMORY.md). Re-verify engine behavior after app
updates.

## Conventions for Future MCP Docs

- Add one named leaf doc per MCP tool.
- Keep this index focused on discovery and classification.
- Keep install, run, config, and client-specific details in the owning leaf doc.
- Register every new MCP leaf doc here when it is added.

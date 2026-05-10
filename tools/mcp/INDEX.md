# MCP Tooling Index

> Directory landing page for Baluffo MCP tooling. Use this page to choose the right MCP guide without loading every leaf doc.

## What Lives Here

`tools/mcp/` owns repo MCP tooling docs and MCP server entrypoints used during development.

- Use [SERENA.md](SERENA.md) for the required repo AI tooling standard.
- Use [PLAYWRIGHT.md](PLAYWRIGHT.md) for the optional browser-driving MCP server.
- Use [BASIC_MEMORY.md](BASIC_MEMORY.md) for the optional external AI continuity memory (cross-client handoff, project gotchas, current focus).

## Start Here

| Document | Role | Use it when |
|----------|------|-------------|
| [SERENA.md](SERENA.md) | Required AI tooling | You are setting up Serena for Codex CLI or OpenCode, or checking the repo's Serena-memory rules |
| [PLAYWRIGHT.md](PLAYWRIGHT.md) | Optional task-specific tooling | You need the local Playwright MCP server for browser interaction or visual verification |
| [BASIC_MEMORY.md](BASIC_MEMORY.md) | Optional external AI continuity memory | You want durable cross-client handoff notes, project gotchas, and current focus across Codex/OpenCode/Cline |

## Which MCP Tool Should I Use?

| Need | Start here |
|------|------------|
| Standard AI-assisted repo work | [SERENA.md](SERENA.md) |
| Symbol-aware navigation and refactors | [SERENA.md](SERENA.md) |
| Browser actions, screenshots, or page inspection | [PLAYWRIGHT.md](PLAYWRIGHT.md) |
| Cross-client AI handoff memory | [BASIC_MEMORY.md](BASIC_MEMORY.md) |
| General repo coding task with no browser interaction | [SERENA.md](SERENA.md) only |

## Conventions for Future MCP Docs

- Add one named leaf doc per MCP tool.
- Keep this index focused on discovery and classification.
- Keep install, run, config, and client-specific details in the owning leaf doc.
- Register every new MCP leaf doc here when it is added.

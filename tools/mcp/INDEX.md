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

## Conventions for Future MCP Docs

- Add one named leaf doc per MCP tool.
- Keep this index focused on discovery and classification.
- Keep install, run, config, and client-specific details in the owning leaf doc.
- Register every new MCP leaf doc here when it is added.

# Playwright MCP Server

This directory contains an optional local MCP server for driving a shared Playwright browser session during development.

It is developer tooling only:
- It is not part of Baluffo startup, packaging, release, or bridge runtime flows.
- It uses the repo's existing `playwright` package.
- If you persist screenshots, prefer writing them under `.tmp/` to avoid repo-root clutter.

## Entry Point

Run the server from the repo root:

```powershell
node tools/mcp/playwright-server.cjs
```

The server speaks MCP over stdio and stays quiet until a client sends JSON-RPC messages.

## Supported Tools

- `navigate`
- `screenshot`
- `click`
- `fill`
- `evaluate`
- `get_html`
- `get_text`
- `wait_for_selector`
- `reset_session`
- `close`

All page-manipulation tools require an active page created by `navigate`. If no page is active yet, the server returns a tool error instead of opening a blank tab.

## Editor Setup

### Cursor

```json
{
  "name": "baluffo-playwright",
  "command": "node",
  "args": ["e:/Baluffo/tools/mcp/playwright-server.cjs"],
  "env": {}
}
```

### Cline

```json
{
  "mcpServers": {
    "baluffo-playwright": {
      "command": "node",
      "args": ["e:/Baluffo/tools/mcp/playwright-server.cjs"]
    }
  }
}
```

### Windsurf

Use the same command and args:

```json
{
  "mcpServers": {
    "baluffo-playwright": {
      "command": "node",
      "args": ["e:/Baluffo/tools/mcp/playwright-server.cjs"]
    }
  }
}
```

## Notes

- `navigate` reuses one active page per server process, so follow-up tools operate on the same page state.
- `reset_session` replaces the browser context and clears the active page.
- `close` shuts down the browser entirely.
- For saved screenshots, prefer paths such as `.tmp/mcp/playwright/home.png`.

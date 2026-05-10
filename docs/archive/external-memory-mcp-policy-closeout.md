# Closeout: External Memory MCP Policy

**Date**: 2026-05-10
**Status**: Completed
**Plan**: `docs/plans/external-memory-mcp-policy-plan.md`

## What Was Done

Implemented a controlled external Memory MCP workflow for AI-coder continuity without making memory a source of truth and without adding Baluffo runtime dependencies.

### Policy (Baluffo repo)
- `AGENTS.md` — appended `## External MCP Memory Policy` (13 rules), disambiguated from Serena project memory
- `tools/mcp/BASIC_MEMORY.md` — 236-line setup guide: prerequisites, install, project registration, first-class clients (Codex CLI, OpenCode), secondary clients (Claude Desktop, Cursor, Cline, VS Code), vault layout, session workflow, permissions, canonical-source rule, promotion path
- `tools/mcp/INDEX.md` — added BASIC_MEMORY.md to "What Lives Here" paragraph, Start Here table, and Which MCP Tool table
- `opencode.json` — registered `basic-memory` MCP server alongside `serena`

### External Vault
- Created private GitHub repo `deathuman/BaluffoMemory` with vault layout per plan
- Installed `basic-memory` v0.20.3 via `uv tool install -p 3.13 basic-memory@latest`
- Registered project `baluffo-memory` pointing to vault directory
- Populated vault with 5 architecture decisions and 7 recurring gotchas about Baluffo
- Verified MCP connectivity after OpenCode restart
- Created session handoff note documenting setup completion

### Verifications Passed
- No runtime, dependency, packaging, release, or CI files modified
- No new Python or Node dependencies added to Baluffo
- Existing docs routing and Serena preflight text untouched
- `AGENTS.md` Repo Guardrails and Docs Routing sections unchanged

## Key Decisions
1. Basic Memory MCP chosen as the external memory tool (over Engram, etc.)
2. Private GitHub repo `BaluffoMemory` cloned alongside Baluffo as the vault
3. Both Codex CLI and OpenCode treated as first-class clients
4. Memory writes require explicit user approval (policy rule)
5. Repo source/docs always win over memory when they conflict

## Where to Look Next
- `tools/mcp/BASIC_MEMORY.md` for setup and client config
- `AGENTS.md` for the 13-rule policy
- `https://github.com/deathuman/BaluffoMemory` for the live vault

# Repo Guardrails

These are the non-negotiable repo rules.

- Do not use destructive Git or file operations unless the user explicitly asks and the target is understood.
- Do not submit changes with `--no-verify`.
- Do not add new Python or Node dependencies without explicit user approval.
- Do not import composition-root modules from narrow helpers, build scripts, or packaging code; prefer leaf modules and direct config/data reads.
- Treat bridge and route signature changes as compatibility work: search both route call sites and frontend payload builders before changing signatures, and verify task-start, busy-state, and log-polling behavior together when launch/completion flows change.
- Treat packaging, installer, release, and tag work as high risk; verify the release-critical path explicitly and never move or recreate release tags unless the user explicitly asks.
- Preserve public job text, locations, and other persisted or user-facing data contracts when changing normalization, adapters, or report payloads.

## Docs Routing

- Start docs discovery at `docs/INDEX.md`.
- Load the smallest authoritative doc set only.
- Default read path for code tasks:
  1. `docs/AI_ASSISTANT_GUIDE.md`
  2. `docs/architecture-ai-map.md` only if you need task-to-files routing or compatibility-surface detail
  3. One relevant contract or workflow doc
- `docs/archive/` holds refactor records and historical notes. Do not load archived docs by default.
- If Serena memory and repo docs diverge, repo docs win and the stale Serena memory should be corrected.
- For doc ownership or maintenance changes, follow `docs/DOCS_WORKFLOW.md`.

## External MCP Memory Policy

This section covers optional memory MCPs such as Basic Memory.
Serena's own project memory is covered in Docs Routing above.

- Repo source, tests, docs, and AGENTS.md are canonical.
- Serena MCP remains the required code-intelligence MCP for repo work.
- External memory MCPs are optional workflow tools for AI-coder continuity only.
- External memory may store durable decisions, recurring gotchas, handoff notes,
  current focus, and stale-memory corrections.
- External memory must not become a source of truth for current implementation
  behavior.
- Do not store current implementation facts unless they include a source path,
  date observed, and reason they matter.
- If external memory conflicts with repo state, ignore memory and update or
  delete the stale note.
- Promote durable, generally useful memory into the owning Baluffo doc when it
  becomes canonical guidance, following docs/DOCS_WORKFLOW.md.
- External AI-coder memory, when used, is maintained in a separate private
  Git-backed Markdown vault such as `BaluffoMemory`.
- Memory writes require explicit user approval before committing.
- Never store secrets, tokens, credentials, private keys, or sensitive data.
- If a secret is accidentally committed to the memory repo, treat the repo as
  compromised and rotate all affected credentials.
- Do not add memory MCPs as Baluffo runtime, Python, Node, packaging, release,
  or CI dependencies.

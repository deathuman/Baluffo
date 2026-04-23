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

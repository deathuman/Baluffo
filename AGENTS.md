# Repo Guardrails

These are the non-negotiable repo rules.

- Do not use destructive Git or file operations unless the user explicitly asks and the target is understood.
- Do not submit changes with `--no-verify`.
- Do not add new Python or Node dependencies without explicit user approval.
- Do not import composition-root modules from narrow helpers, build scripts, or packaging code; prefer leaf modules and direct config/data reads.
- Do not expand root-injection or root monkeypatch seams; existing seams are compatibility-only, and new cross-module references should use explicit imports.
- Treat bridge and route signature changes as compatibility work: search both route call sites and frontend payload builders before changing signatures, and verify task-start, busy-state, and log-polling behavior together when launch/completion flows change.
- Treat packaging, installer, release, and tag work as high risk; verify the release-critical path explicitly and never move or recreate release tags unless the user explicitly asks.
- Preserve public job text, locations, and other persisted or user-facing data contracts when changing normalization, adapters, or report payloads.
- Validate dead-code or boundary-cleanup analyzer findings against actual imports before acting. Known caveat: `tools/repo_health/bin/analyze_refactorability.py` uses word-boundary composition-root matching, so `src.jobs.*` and `src.bridge.*` leaf imports can appear as false positives.
- Before manual dead-code hunting, check the pre-push Vulture hook and `whitelist.py`; the whitelist records known false positives and intentionally retained schema or compatibility symbols.

## Rule Placement

- Add always-loaded rules here only for known recurring or high-risk failures.
- Prefer concise negative constraints over broad positive style advice.
- Do not add one-off preferences or task-specific workflow details to this file.
- Move detailed tool, memory, workflow, or subsystem guidance to the owning doc.

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

This section covers required AI-coder continuity memory such as Basic Memory.
Serena's own project memory is covered in Docs Routing above.

- Repo source, tests, docs, and AGENTS.md are canonical.
- Serena MCP remains the required code-intelligence MCP for repo work.
- Basic Memory is the required external continuity MCP for AI-assisted Baluffo
  planning, handoff, recurring gotchas, current focus, and stale-memory
  corrections.
- For non-trivial Baluffo tasks, check relevant Basic Memory notes at task
  start for continuity and gotchas, then validate anything useful against repo
  source, tests, docs, and AGENTS.md before acting.
- External memory must not become a source of truth for current implementation
  behavior.
- Do not store current implementation facts unless they include a source path,
  date observed, and reason they matter.
- If external memory conflicts with repo state, ignore memory and update or
  delete the stale note.
- Promote durable, generally useful memory into the owning Baluffo doc when it
  becomes canonical guidance, following docs/DOCS_WORKFLOW.md.
- Maintain external AI-coder memory in the separate private Git-backed Markdown
  vault `BaluffoMemory`.
- Keep memory notes current as normal AI task maintenance when they are useful
  for future sessions; no separate memory-specific approval is required.
- At task closeout, explicitly decide whether a current-focus, handoff, gotcha,
  decision, or stale-memory correction should be written or updated. Skip the
  write only when the task produced no durable continuity value.
- Never store secrets, tokens, credentials, private keys, or sensitive data.
- If a secret is accidentally committed to the memory repo, treat the repo as
  compromised and rotate all affected credentials.
- Do not add memory MCPs as Baluffo runtime, Python, Node, packaging, release,
  or CI dependencies.

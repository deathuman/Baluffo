# Repo Guardrails

Always-loaded rules only. Keep detailed workflow in the owning docs.

## Hard Stops

- Do not use destructive Git or file operations unless the user explicitly asks and the target is understood.
- Do not submit changes with `--no-verify`.
- Do not add Python or Node dependencies without explicit user approval.
- Repo source, tests, docs, and `AGENTS.md` are canonical; external memory is continuity only.
- Never store secrets, tokens, credentials, private keys, or sensitive data in repo docs or memory.

## Code Boundaries

- Prefer leaf modules and direct config/data reads; do not import composition-root modules from narrow helpers, build scripts, or packaging code.
- Do not expand root-injection or root monkeypatch seams; existing seams are compatibility-only.
- Treat bridge/route signature changes as compatibility work: check route call sites, frontend payload builders, task-start, busy-state, and log-polling behavior together.
- Treat packaging, installer, release, and tag work as high risk; verify the release-critical path and never move/recreate release tags unless explicitly asked.
- Preserve public job text, locations, and persisted/user-facing data contracts when changing normalization, adapters, or report payloads.
- Validate dead-code or boundary-cleanup analyzer findings against actual imports. Before manual dead-code hunting, check the pre-push Vulture hook and `whitelist.py`.
- Keep `src/ship/desktop_app/_linux.py` and `_windows.py` helpers in sync. When touching desktop_app internals, test both `npm run test:py:linux` and `npm run test:py:extended`.

## Routing

- Start docs discovery at `docs/INDEX.md`; load the smallest authoritative doc set only.
- For AI coding workflow, use `docs/AI_ASSISTANT_GUIDE.md`; load `docs/architecture-ai-map.md` only when file routing or compatibility-surface detail is needed.
- For doc ownership or maintenance changes, follow `docs/DOCS_WORKFLOW.md`.
- Do not load `docs/archive/` by default.

## Memory And Tools

- Serena MCP is required for code-intelligence work; use Basic Memory only for continuity, handoffs, recurring gotchas, current focus, and stale-memory corrections.
- For non-trivial Baluffo tasks, check relevant Basic Memory notes, then validate useful claims against repo source/tests/docs before acting.
- At closeout, update Basic Memory only when the task created durable continuity value. Detailed policy lives in `tools/mcp/BASIC_MEMORY.md`.
- For environment/toolbelt triage, run `python scripts/ai_env_check.py --smoke`; run `python scripts/toolbelt_check.py --install` only when missing tools matter. Toolbelt tools are conveniences, not build/CI requirements.
- Avoid broad repo packers or context generators by default; use targeted search, structured filters, and symbol tools first.

## Testing

- The repo `.venv` is a Linux/WSL venv whose `bin/python` symlink is broken on Windows; run Python tests with the global `python -m pytest`, not `.venv/bin/python`. Verification commands, fixture layout, and test routing live in `docs/testing.md`.
- Test runs must never leave discovery audit artifacts (`gameprog-*`/`gamesmap-*`/`*-discovery-audit.json`) in repo `data/`; if they do, an unpinned caller ran the real stages — see `docs/testing.md` (Discovery audit artifact hygiene).

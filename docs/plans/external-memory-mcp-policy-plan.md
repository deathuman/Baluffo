# External Memory MCP Policy and Setup Plan

> Canonical for the AGENTS.md policy addition, Basic Memory MCP documentation, and BaluffoMemory vault conventions.
>
> Not canonical for Serena setup, runtime dependency decisions, packaging, or CI. See `tools/mcp/SERENA.md` and `docs/RELEASE.md` for those.

## Intent

Add a controlled external Memory MCP workflow for AI-coder continuity without making memory a source of truth and without adding runtime dependencies to Baluffo.

This supports cross-machine AI handoff: durable decisions, gotchas, current focus, and session continuity across any MCP-compatible client (Codex CLI, OpenCode, Cline, etc.).

## Current Status

- `AGENTS.md` has 2 sections: Repo Guardrails (lines 1-11) and Docs Routing (lines 13-23). Line 22 already covers Serena memory divergence ("repo docs win").
- `docs/AI_ASSISTANT_GUIDE.md` has Serena Session Preflight (lines 66-78) and AI Editing Rules (lines 80-89). Line 42 covers the same "repo docs win" principle.
- `tools/mcp/INDEX.md` has 2 entries: SERENA.md (required) and PLAYWRIGHT.md (optional). No external memory MCP is registered.
- `tools/mcp/SERENA.md` has 238 lines covering install, setup, client config, health checks, session bootstrap, and repo rules.
- No Basic Memory, Engram, or external memory MCP reference exists anywhere in the repo.
- BaluffoMemory private repo does not yet exist.
- `docs/plans/` contains 9 existing plans covering benchmarking, storage, dedup, lifecycle, and source-conflict topics.

## Constraints

1. **No runtime changes**: Do not modify `package.json`, `pyproject.toml`, `requirements*.txt`, `src/`, `frontend/`, packaging/release files, or CI files.
2. **No new dependencies**: Do not add Python or Node dependencies to Baluffo.
3. **Preserve existing docs**: Do not change `docs/AI_ASSISTANT_GUIDE.md`, `docs/INDEX.md`, `tools/mcp/SERENA.md`, or the existing `AGENTS.md` guardrails/docs-routing sections.
4. **No secrets**: Memory policy must prohibit secrets, tokens, credentials, and sensitive data.

## Files to Change

| File | Action | Rationale |
|------|--------|-----------|
| `AGENTS.md` | Append `## External MCP Memory Policy` section (13 rules) | Compact policy — 13 rules with preamble disambiguating Serena memory |
| `tools/mcp/INDEX.md` | Add 2 table rows (Start Here + Which MCP Tool) | Discovery entry for the new optional MCP leaf doc |
| `tools/mcp/BASIC_MEMORY.md` | Create new file | Setup, vault layout, session workflow, rules |
| Everything else | No changes | Enforced by constraints above |

## AGENTS.md Policy — 13 Rules

The new section after `## Docs Routing`. Note the preamble disambiguates Serena's
project memory (Docs Routing line 22) from external MCP memory:

```
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
```

## BASIC_MEMORY.md Sections (new file under tools/mcp/)

- Header: "Basic Memory MCP for Baluffo (External / Optional)"
- Status note (optional, external, local-first, not runtime/CI)
- Memory vault layout (recommended BaluffoMemory repo layout)
- What gets committed vs ignored (.gitignore patterns)
- Session start workflow — pull both repos; on first use, clone BaluffoMemory alongside Baluffo (skip memory if clone unavailable). Read AGENTS.md, use Serena for truth, validate memory claims against repo state.
- Session end workflow — AI proposes updates, human approves, commit only durable notes, push.
- Merge-conflict handling — if `git pull` hits a conflict, resolve manually or discard local (`git checkout -- . ; git pull`). Never auto-merge memory files speculatively.
- Permissions — reads free, writes require explicit user approval.
- Canonical source rule — repo wins; update/delete stale notes.
- Promotion path — memory -> Baluffo doc when durable enough, following DOCS_WORKFLOW.md.
- When to skip — one-shot tasks, read-only exploration, no handoff needed.

## INDEX.md Changes

**Start Here table** — add third row:

| Document | Role | Use it when |
|----------|------|-------------|
| SERENA.md | Required AI tooling | ...existing... |
| PLAYWRIGHT.md | Optional task-specific tooling | ...existing... |
| BASIC_MEMORY.md | Optional external AI continuity memory | You want durable cross-client handoff notes, project gotchas, and current focus across Codex/OpenCode/Cline |

**Which MCP Tool table** — add row:

| Need | Start here |
|------|------------|
| ...existing rows... |
| Cross-client AI handoff memory | BASIC_MEMORY.md |

## BaluffoMemory Private Repo (external, created separately)

Recommended layout:

```
BaluffoMemory/
  README.md
  MEMORY_POLICY.md
  baluffo/
    current-focus.md
    decisions/
      2026-05-10-memory-mcp-policy.md
    gotchas/
      repo-truth-vs-memory.md
    handoffs/
      .gitkeep
    stale-memory-corrections.md
  .gitignore
```

`.gitignore` patterns: `*.db`, `*.sqlite*`, `.cache/`, `index/`, `embeddings/`, `node_modules/`, `__pycache__/`, `.env`, `*.local.*`.

## Session Workflow (for AI coders)

**Start:**
1. `git pull` in Baluffo
2. `git pull` in BaluffoMemory (on first use: `git clone <BaluffoMemory-url>` alongside Baluffo; skip memory if clone unavailable)
3. Read `AGENTS.md` and `docs/AI_ASSISTANT_GUIDE.md`
4. Use Serena for repo/code truth
5. Use memory only for handoff/context
6. Validate memory claims against repo state before acting
7. If `git pull` hits a merge conflict, resolve manually (`git checkout -- . ; git pull`); never auto-merge speculatively

**End:**
1. AI proposes memory updates
2. Human approves or edits them
3. Commit only durable notes to BaluffoMemory
4. `git push BaluffoMemory`

## Verification

After implementation:
1. Run `git diff --stat` — confirm exactly 3 files changed (AGENTS.md, INDEX.md, + BASIC_MEMORY.md)
2. Run `rg "Basic Memory|BaluffoMemory|BASIC_MEMORY" src/ frontend/ package.json pyproject.toml requirements*.txt` — confirm zero matches in runtime/dependency files
3. Read `AGENTS.md` — confirm existing 2 sections unchanged, new 3rd section present
4. Read `tools/mcp/INDEX.md` — confirm 2 new rows, existing content unchanged
5. Run `git diff` — confirm no accidental whitespace or content changes to unrelated files

## Acceptance Criteria

- AGENTS.md has a compact `## External MCP Memory Policy` section with all 13 rules, disambiguated from Serena project memory
- tools/mcp/BASIC_MEMORY.md exists and covers setup, vault layout, session workflow, canonical-source rule, and promotion path
- tools/mcp/INDEX.md has discovery entries for BASIC_MEMORY.md
- Zero changes to runtime, dependency, packaging, release, or CI files
- Existing docs routing and Serena preflight text are untouched

# Basic Memory MCP for Baluffo (External / Optional)

> Optional external AI continuity memory. Local-first, not part of Baluffo runtime or CI.

## Status

Basic Memory is an optional, external, local-first MCP tool for durable cross-client AI handoff.
It is never a Baluffo runtime, Python, Node, packaging, release, or CI dependency.

## Prerequisites

- `uv` installed: <https://docs.astral.sh/uv/getting-started/installation/>
- Python 3.13+ (the same 3.13 toolchain used by Serena)
- A private `BaluffoMemory` Git repo cloned alongside the main Baluffo repo (see [Memory Vault Layout](#memory-vault-layout))

Upstream references:
- Basic Memory upstream: <https://github.com/basicmachines-co/basic-memory>
- Documentation: <https://docs.basicmemory.com>

## Install Basic Memory

Follow the same `uv`-managed install path as Serena:

```powershell
uv tool install -p 3.13 basic-memory@latest
basic-memory --help
```

If `basic-memory` or `bm` is not found after install, restart your shell so the `uv` tool path is available.
For direct CLI use on Windows, the `uv`-managed executable is typically available at
`$env:USERPROFILE\.local\bin\basic-memory.exe`.

### Certified Local Baseline

This setup was validated on Windows with:

- `basic-memory` 0.20.3
- Python 3.13
- `sqlite-vec` 0.1.9
- `fastembed` 0.8.0

For deterministic MCP sessions, keep Basic Memory auto-update disabled in
`$env:USERPROFILE\.basic-memory\config.json` and update manually with a health check.

## Register the BaluffoMemory Vault

After cloning `BaluffoMemory` alongside the main Baluffo repo, register it as a Basic Memory project:

```powershell
# Clone the private memory vault (first time only)
git clone https://github.com/<your-org>/BaluffoMemory.git ../BaluffoMemory

# Register as a basic-memory project
basic-memory project add baluffo-memory "../BaluffoMemory" --default

# Verify the project picks up existing notes
basic-memory status --project baluffo-memory
basic-memory reindex --project baluffo-memory --search
```

## First-Class Clients

### Codex CLI

Codex is a first-class client for this repo. Use Codex's MCP command flow instead of hand-editing config files:

```powershell
codex mcp add basic-memory -- "$env:USERPROFILE\.local\bin\basic-memory.exe" mcp --project baluffo-memory
codex mcp list
codex mcp get basic-memory
```

`codex mcp add` writes the user-global Codex MCP config for you. Keep that config user-local rather than committing a repo-managed Codex config file.
A working `codex mcp get basic-memory` registration should report:

- `enabled: true`
- `transport: stdio`
- `command: $env:USERPROFILE\.local\bin\basic-memory.exe` or another user-local basic-memory executable path
- `args: mcp --project baluffo-memory`

The `--project baluffo-memory` lock is intentional. It constrains the MCP server with
`BASIC_MEMORY_MCP_PROJECT`, preventing accidental cross-project reads or writes if another
Basic Memory default project exists.

### OpenCode

OpenCode is the other first-class client for this repo.
Baluffo already commits the `basic-memory` entry in `opencode.json`:

```json
{
  "mcp": {
    "basic-memory": {
      "type": "local",
      "command": ["basic-memory", "mcp", "--project", "baluffo-memory"],
      "enabled": true
    }
  }
}
```

Install Basic Memory once, then run OpenCode from the repo root so it can use the committed repo config.
The committed config expects `basic-memory` on `PATH`.
If OpenCode cannot resolve `basic-memory`, restart your shell/session or change the local command to the
explicit `basic-memory.exe` path.

## Secondary Clients

### Claude Desktop

Edit `claude_desktop_config.json` (Windows: `%APPDATA%\Claude\claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "basic-memory": {
      "command": "uvx",
      "args": ["basic-memory", "mcp", "--project", "baluffo-memory"]
    }
  }
}
```

### Cursor

```json
{
  "name": "baluffo-basic-memory",
  "command": "basic-memory",
        "args": ["mcp", "--project", "baluffo-memory"]
}
```

### Cline

```json
{
  "mcpServers": {
    "baluffo-basic-memory": {
      "command": "basic-memory",
      "args": ["mcp", "--project", "baluffo-memory"]
    }
  }
}
```

### VS Code

Add to User Settings JSON (`Ctrl+Shift+P`, `Preferences: Open User Settings (JSON)`), or to
`.vscode/mcp.json` in the workspace:

```json
{
  "mcp": {
    "servers": {
      "basic-memory": {
        "command": "uvx",
        "args": ["basic-memory", "mcp", "--project", "baluffo-memory"]
      }
    }
  }
}
```

## Memory Vault Layout

The recommended vault is a private Git-backed Markdown repo named `BaluffoMemory`, cloned
alongside the main Baluffo repo:

```
BaluffoMemory/
  README.md
  MEMORY_POLICY.md
  .gitignore
  baluffo/
    current-focus.md
    stale-memory-corrections.md
    decisions/
      Three-Layer Local-First Desktop Architecture.md
      Thin Composition Roots Pattern.md
      data-ui Attribute Selector Convention.md
      Dynamic Redundant-Static Source Suppression.md
      External Memory MCP Policy - Implemented.md
    gotchas/
      Frontend is Vanilla JS, Not React-Vue.md
      Never Import from src-jobs-common-__init__.py.md
      _runtime.facade() Pattern is Retired.md
      CamelCase Data Contracts are Mandatory.md
      Bridge and Route Signature Changes are Compatibility Work.md
      repo-truth-vs-memory.md
    handoffs/
      2026-05-10-setup-complete.md
```

## .gitignore

Commit only Markdown notes. Ignore everything else:

```
*.db
*.sqlite*
.cache/
index/
embeddings/
node_modules/
__pycache__/
.env
*.local.*
```

## Session Start Workflow

1. `git pull` in Baluffo
2. `git pull` in BaluffoMemory (on first use: `git clone <BaluffoMemory-url>` alongside Baluffo; skip memory if clone unavailable)
3. Read `AGENTS.md` and `docs/AI_ASSISTANT_GUIDE.md`
4. Use Serena for repo/code truth
5. Use memory only for handoff/context
6. Validate memory claims against repo state before acting

## Session End Workflow

1. AI proposes memory updates
2. Human approves or edits them
3. Commit only durable notes to BaluffoMemory
4. `git push` BaluffoMemory

## Merge-Conflict Handling

If `git pull` hits a merge conflict, resolve manually or discard local:

```
git checkout -- .
git pull
```

Never auto-merge memory files speculatively.

## Permissions

- Reads are free and automated.
- Writes require explicit user approval before committing.
- Never commit or push memory writes without the user explicitly asking.

## Canonical Source Rule

Repo source, tests, docs, and AGENTS.md are canonical. If external memory conflicts
with repo state, ignore memory and update or delete the stale note.

## Promotion Path

When a memory note becomes durable, generally useful, canonical guidance, promote it
into the owning Baluffo doc following `docs/DOCS_WORKFLOW.md`. Then remove or update
the memory note to link to the canonical doc.

## Vault Maintenance

### Consolidation Checklist

Periodically review the vault to keep it lean and useful:

- Merge overlapping decisions — if two notes cover the same topic, consolidate into the more recent one and link from the older one
- Review gotchas — if a gotcha is no longer relevant (pattern eliminated, convention changed), archive it with a `**Resolved**` date
- Prune stale focus — `current-focus.md` should only track what's active; move completed items to the session handoff note
- Check for orphaned notes — search for notes that no longer link to active code or conventions

### Git Tag Snapshots

Before major refactors or risky changes, snapshot the vault state with a git tag:

```powershell
git tag arch-$(Get-Date -Format 'yyyy-MM-dd') -m "Vault snapshot before [describe change]"
git push origin arch-$(Get-Date -Format 'yyyy-MM-dd')
```

This gives you a restore point — diff against the tag to see what the AI changed, or revert if needed. Tags are lightweight and don't affect normal git operations.

### Health Check

Before a session, quickly verify vault integrity:

```powershell
# In BaluffoMemory directory
git status                    # Any uncommitted drift from last session?
git log --oneline -5          # Recent activity
cmd /c "chcp 65001>nul&& set PYTHONIOENCODING=utf-8&& set NO_COLOR=1&& basic-memory doctor --local"
cmd /c "chcp 65001>nul&& set PYTHONIOENCODING=utf-8&& set NO_COLOR=1&& basic-memory status --project baluffo-memory"
cmd /c "chcp 65001>nul&& set PYTHONIOENCODING=utf-8&& set NO_COLOR=1&& basic-memory reindex --project baluffo-memory --search"
```

Plain `basic-memory doctor` can fail in Windows console hosts because Rich output and the
legacy console renderer disagree. Use the UTF-8/no-color `cmd /c` form above for reliable checks.

If `basic-memory doctor --local` reports inconsistencies, run the search-only reindex command above.

### Semantic Search Status

Text search is the supported reliability baseline for this repo. Semantic/vector search is
locally restored in this Codex IDE by a patch to the installed Basic Memory `0.20.3`
uv tool environment.

The repaired failure was not vault corruption and not a missing `sqlite-vec` package. Basic Memory's
embedding reindex path touched the SQLite `search_vector_embeddings` virtual table before loading
`sqlite_vec` on that connection, and before vector tables were guaranteed initialized in the stale
cleanup path.

Prepared patch artifact: [`basic-memory-sqlite-vec0-reindex.patch`](basic-memory-sqlite-vec0-reindex.patch).
The patch moves stale `vec0` embedding cleanup behind a SQLite repository helper that first ensures
vector tables, then loads `sqlite_vec` on the same session before deleting from
`search_vector_embeddings`.

This is a local tool patch, not an upstream release. Reinstalling or upgrading Basic Memory will
replace the patched files. Before reapplying, confirm `basic-memory --version` is still `0.20.3`
and the installed source still contains `_purge_stale_search_rows()` and
`_ensure_sqlite_vec_loaded()`. If the source shape changed, re-check the fix instead of forcing the
patch.

Recovery helper:

```powershell
# Check whether the local Basic Memory 0.20.3 install still has the vec0 patch.
.\tools\mcp\apply-basic-memory-vec0-patch.ps1

# Reapply after reinstall/upgrade only when the helper reports the patch is missing.
.\tools\mcp\apply-basic-memory-vec0-patch.ps1 -Apply
```

The helper is intentionally check-only by default. `-Apply` backs up the two installed Basic Memory
source files under `C:\tmp\basic-memory-vec0-backup-<timestamp>` before modifying them.

## When to Skip

Skip Basic Memory for:
- One-shot tasks with no recurring value
- Read-only exploration or code review
- Sessions where no handoff continuity is needed
- Tasks where all decisions are already captured in repo docs or commit messages

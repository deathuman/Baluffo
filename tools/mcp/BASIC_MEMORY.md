# Basic Memory MCP for Baluffo (Required Continuity)

> Required external AI continuity memory. Local-first, not part of Baluffo runtime or CI.

## Status

Basic Memory is the required external local-first MCP tool for durable cross-client AI handoff.
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

- `basic-memory` 0.23.0 (re-verified 2026-08-24 after the revision-skew fix; earlier baselines 0.21.5/0.22.1)
- Python 3.13
- Semantic search enabled and vector reindex validated

For deterministic MCP sessions, keep Basic Memory auto-update disabled in
`$env:USERPROFILE\.basic-memory\config.json` and update manually with a health check.

## Register the BaluffoMemory Vault

After cloning `BaluffoMemory` alongside the main Baluffo repo, register it as a Basic Memory project:

```powershell
# Clone the private memory vault (first time only)
git clone https://github.com/deathuman/BaluffoMemory.git ../BaluffoMemory

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
If OpenCode cannot resolve `basic-memory`, restart your shell/session, add the user-local tool directory
to `PATH`, or override the command in machine-local client configuration. Do not commit absolute
`basic-memory.exe` paths to `opencode.json`; the tracked file stays portable across machines.

Verified OpenCode health should show both required MCPs connected:

```powershell
opencode mcp list
# basic-memory connected: basic-memory mcp --project baluffo-memory
# serena connected: serena start-mcp-server --context ide --project-from-cwd
```

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

The vault is a private Git-backed Markdown repo named `BaluffoMemory`, cloned alongside the
main Baluffo repo. Notes live under `baluffo/` or in a small set of root-level topic dirs.
The frontmatter `permalink` is the routing key — never create a filesystem directory named
`baluffo-memory/`; that prefix is the permalink namespace, not a path (a stray nested
`baluffo-memory/` directory created that way was relocated into the normal layout on
2026-08-17).

```
BaluffoMemory/
  README.md
  MEMORY_POLICY.md
  .gitignore
  baluffo/                    # primary namespace; one subdir per topic
    current-focus.md
    stale-memory-corrections.md
    decisions/
    gotchas/
    handoffs/
    p0/                       # P0 ratchets + progress tracker
    provider-discovery/
    releases/
    ...                       # audits/, ci/, cleanup/, deployments/, evidence/,
                              # operations/, performance/, provider-coverage/, umbrel/
  gotchas/                    # root-level cross-cutting gotchas
  releases/                   # root-level release notes
  handoffs/
    jobs/                     # jobs-family handoffs (plans, audits, remediations)
    baluffo/                  # Baluffo release-prep handoffs
  spikes/                     # spike verdicts
```

Both `baluffo/<topic>/...` and the root-level topic dirs above are committed conventions
(verified 2026-08-17). Add each note where its topic family already lives — match the
existing directory, do not create a parallel one.

Permalink convention: `baluffo-memory/` + the note's vault-relative path, with the
filename stem slugified (lowercase, spaces/dashes collapse to `-`):

| Note path | permalink |
|-----------|-----------|
| `baluffo/p0/Baluffo P0 Adapter Recovery Ratchet 2026-06-19.md` | `baluffo-memory/baluffo/p0/baluffo-p0-adapter-recovery-ratchet-2026-06-19` |
| `baluffo/gotchas/<Name>.md` | `baluffo-memory/baluffo/gotchas/<slug>` |
| `baluffo/releases/<Name>.md` | `baluffo-memory/baluffo/releases/<slug>` |
| `gotchas/<Name>.md` | `baluffo-memory/gotchas/<slug>` |
| `releases/<Name>.md` | `baluffo-memory/releases/<slug>` |
| `handoffs/jobs/<Name>.md` | `baluffo-memory/handoffs/jobs/<slug>` |
| `handoffs/baluffo/<Name>.md` | `baluffo-memory/handoffs/baluffo/<slug>` |

Keep the permalink in sync whenever a note is moved or renamed.

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
4. For non-trivial Baluffo tasks, search/read relevant Basic Memory notes for current focus, recent handoffs, recurring gotchas, or stale-memory corrections
5. Use Serena for repo/code truth
6. Use memory only for continuity, handoff, gotchas, current focus, and stale-memory correction
7. Validate memory claims against repo state before acting

## Session End Workflow

1. AI updates useful durable memory as normal task maintenance.
2. Keep notes concise, sourced, and future-session relevant.
3. Explicitly decide whether to update current focus, handoff, gotcha, decision, or stale-memory notes; skip the write only when the task produced no durable continuity value.
4. Commit and push durable BaluffoMemory updates as part of normal closeout when memory changed and the network is available.
5. If memory push fails or network is unavailable, report the pending BaluffoMemory status and the exact command to retry.

### Git-Backed Closeout Checklist

Before pushing BaluffoMemory:

1. Run `git fetch` and inspect `git status --short --branch`, `git log origin/main..HEAD`, and untracked files.
2. Pull or rebase only when there is no conflict; if a conflict appears, stop and inspect the conflicting notes manually.
3. Stage only durable continuity notes. Do not stage accidental nested vault paths such as `baluffo-memory/...` inside the BaluffoMemory repo unless they have been intentionally moved into the normal vault layout.
4. Update stale routing notes such as `baluffo/current-focus.md` when release versions, active work, or next-step guidance changed.
5. Run a targeted secret scan or `git diff --cached` review before committing. Never commit secrets, private keys, token values, local config files, databases, indexes, or cache artifacts.
6. Run `basic-memory status --project baluffo-memory` and `basic-memory reindex --project baluffo-memory --search`.
7. Commit with a message that names the durable event, push `main`, and confirm `git status --short --branch` is clean.

## Merge-Conflict Handling

If `git pull` hits a merge conflict, stop, inspect the conflicting notes, and resolve manually or ask the user how to proceed.
Never auto-merge or discard memory files speculatively.

## Permissions

- Reads are free and automated.
- Writes are normal AI task maintenance when they improve future-session continuity.
- Commit and push memory writes during normal closeout when they improve future-session continuity.
- Do not push memory only when the user explicitly asks to keep memory local, when network access is unavailable, or when the memory repo has unrelated/conflicting changes that need review.

## Canonical Source Rule

Repo source, tests, docs, and AGENTS.md are canonical. If external memory conflicts
with repo state, ignore memory and update or delete the stale note.

## Boundary Rules

- Basic Memory is not source of truth for current implementation behavior.
- Store durable continuity only: handoffs, recurring gotchas, project focus, decisions, and stale-memory corrections.
- Do not store transient failed attempts, unsourced implementation facts, secrets, credentials, or broad summaries that belong in repo docs.
- Any implementation fact stored in memory must include source path, date observed, and why it matters.
- If a memory note becomes canonical guidance, promote it to the owning Baluffo doc and update or remove the note.

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
The plain `status` command can also hang in Git Bash / legacy console hosts; use `--json` for a
reliable machine-readable status (verified 2026-08-17 on basic-memory 0.22.1 with running MCP instances).

If `basic-memory doctor --local` reports inconsistencies, run the search-only reindex command above.

### Semantic Search Status

Text search remains the reliability baseline for this repo, but semantic/vector search is validated
on Windows with upstream Basic Memory `0.21.5+`. Do not apply the old local `0.20.3` `vec0` patch.

The old failure was not vault corruption and not a missing `sqlite-vec` package. Basic Memory
`0.21.5` includes the upstream SQLite vector reindex fix: it loads `sqlite-vec` before dropping
`vec0` virtual tables during full vector reindex.

Windows validation run on 2026-06-01:

```powershell
basic-memory --version  # 0.21.5
basic-memory status --project baluffo-memory
basic-memory doctor
basic-memory reindex --project baluffo-memory --search
basic-memory reindex --project baluffo-memory --embeddings
basic-memory tool search-notes "Baluffo vector sqlite vec0" --project baluffo-memory --hybrid --page-size 5
```

`basic-memory migrate-relations --check-only` was not available in the validated `0.21.5` CLI. If a
future release exposes it, run it before broad relation/index migrations.

If Codex MCP calls report `Transport closed` after stopping or upgrading a running Basic Memory MCP
process, restart the Codex thread/app or re-add the MCP registration so Codex opens a fresh stdio
transport. The CLI validation commands above still verify the local Basic Memory install and search
index.

### Migration Revision Skew (DB newer than pinned tool)

Symptom: every `basic-memory` invocation fails with
`Can't locate revision identified by '<hash>'`, and MCP clients show basic-memory disconnected
(the stdio server dies during startup migrations). Verified 2026-08-24: the uv-tool-pinned install
was 0.22.1 while `memory.db` had been migrated to revision `2d26b287813b` by 0.23.0 — a client
using `uvx basic-memory` (always latest, e.g. the Claude Desktop registration) ran against the
shared `%USERPROFILE%\.basic-memory\memory.db`.

Fix: back up `memory.db`, align the pinned tool with the version that migrated the DB, then run the
health check sequence above:

```powershell
Copy-Item "$env:USERPROFILE\.basic-memory\memory.db" "$env:USERPROFILE\.basic-memory\memory.db.bak-<date>" -Force
uv tool install -p 3.13 basic-memory@latest --force
basic-memory project list   # must not report the revision error
```

Prevention: keep all clients on one delivery channel. Prefer the pinned `basic-memory.exe`
(`uv tool install`) everywhere; avoid `uvx basic-memory` registrations that float to latest while
another install is pinned.

## Required Use

Use Basic Memory for AI-assisted Baluffo planning, handoff, recurring gotchas, current focus, and stale-memory corrections.
If a task produces no durable continuity value, no memory note is required, but Basic Memory remains the configured continuity MCP.

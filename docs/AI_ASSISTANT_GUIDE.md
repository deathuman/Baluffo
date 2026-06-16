# AI Assistant Guide - Baluffo Repository

> - **Status:** Active
> - **Use this when:** starting a code task, choosing edit boundaries, or finding the right subsystem
> - **Canonical for:** task routing, minimal read order, common repo misconceptions, and AI editing rules
> - **Not canonical for:** data contracts, endpoint payloads, or deep subsystem ownership detail
> - **Then inspect:** [`architecture-ai-map.md`](architecture-ai-map.md) for task-to-files routing, plus one matching contract or workflow doc
> - **Last updated:** 2026-06-04

Read this first. Then load only the smallest additional docs needed.

## What this repo is

Baluffo is a local-first game jobs aggregator with four main surfaces:

1. Frontend: plain HTML/CSS/JS ES modules (`jobs.html`, `saved.html`, `admin.html`)
2. Backend: Python for fetching, discovery, sync, and the local HTTP bridge
3. Desktop: Windows packaging/runtime that launches the site and bridge locally
4. Container: one same-origin UI/API service for Umbrel/private LAN deployments

This is not a React/Vite app and not a cloud backend.

## Read order

1. This guide
2. [`architecture-ai-map.md`](architecture-ai-map.md) only when you need task-to-files routing, ownership detail, or compatibility-surface classification
3. [`plans/initial_findings.md`](plans/initial_findings.md) when doing refactoring, consolidation, or dead-code triage work
4. One matching contract or workflow doc:
   - [`DATA_CONTRACT.md`](DATA_CONTRACT.md)
   - [`admin-bridge-api.md`](admin-bridge-api.md)
   - [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md)
   - [`testing.md`](testing.md)
   - [`LOCAL_SETUP.md`](LOCAL_SETUP.md)
5. [`AGENTS.md`](../AGENTS.md) only for repo guardrails and prompt-routing rules

Do not load archive docs by default. Retired cleanup/refactor detail now lives primarily in git history; the active routing docs are the primary source for current ownership.

## Docs-First Boundaries

Baluffo is docs-first, not docs-only. Start with the smallest authoritative doc set, then read code for executable detail, verification, or when the docs do not own the question.

Canonical docs are authoritative only for the surface they declare. Use routing docs for edit location, contract docs for stable payload or API shape, workflow docs for maintenance process, and the codebase for implementation detail outside those declared surfaces.

If Serena memory and repo docs ever diverge, the repo docs stay canonical.

After a long live-hotfix or release patch cycle, pause before continuing direct improvements. Check whether repo docs, Basic Memory, and local skills now encode the new workflow lessons; if the latest public desktop release is behind container/Umbrel-proven `main`, decide whether a desktop rollup release should be planned before more feature work. After a public shared desktop/Umbrel release, update stale `current-focus` memory, push the curated BaluffoMemory repo, and verify local release skills still point agents at the current release closeout checklist.

## Common Wrong Assumptions

| Wrong assumption | Reality |
|------------------|---------|
| Frontend is React/Vue | Vanilla ES modules, no framework |
| `src/admin_bridge.py` is the place for new logic | Prefer `src/bridge/*.py`; `src/admin_bridge.py` stays a thin compatibility surface |
| `src/source_discovery.py` owns discovery implementation | It is a thin CLI surface over `src/source_discovery/*` |
| `src/jobs_fetcher.py` is where new pipeline logic belongs | Treat it as a thin CLI facade; new pipeline logic belongs in `src/jobs/*` |
| Desktop local data uses browser `localStorage` directly | Desktop mode uses the bridge-backed file store under `data/local-user-data/` |
| Container mode is the same as desktop mode | Container mode uses bridge-backed local data but disables desktop lifecycle, updater, host-browser open behavior, and `?desktop=1` navigation params |
| Bridge changes only need backend tests | Verify both Python backend and frontend/runtime callers as needed |
| UI selectors can be guessed | Use `frontend/shared/ui/selectors.js` |
| Endpoint payloads can be assumed | Check [`admin-bridge-api.md`](admin-bridge-api.md) first |
| Dedup/reporting pressure has no known hotspot | The dedup evidence coordinator (`reporting_dedup_evidence.py`) and registry conflict coordinator (`registry_conflicts.py`) were split into leaf modules in 2026-05; public entrypoints remain stable in the coordinator files |

## Verification Shortcuts

| Change area | Fastest check |
|-------------|----------------|
| Frontend syntax/wiring | `node --check frontend/jobs/app.js` |
| Bridge changes | `python -m pytest tests/admin/ -q` |
| Container / Umbrel changes | `python -m pytest tests/bridge/test_container_runtime.py -q` plus targeted frontend unit checks from [`testing.md`](testing.md) |
| Pipeline/fetcher | `python -m pytest tests/test_jobs_fetcher_*.py -q` |
| Jobs helper consolidation | For `_as_list`, `_as_dict`, and `_as_dict_rows`, first verify the jobs copies still share identical list/dict/drop-non-dicts semantics; bridge `_as_dict` helpers are not identical |
| Linux Python tests | `npm run test:py:linux` |
| Linux frontend tests | `npm run test:frontend:linux` |
| Full verification | `npm run verify` |

## Codex In-App Browser Visual QA

Use Codex's built-in Browser plugin and in-app browser for Baluffo visual QA, screenshots, interaction, console/network inspection, and Developer-mode/CDP debugging. Do not configure Chrome DevTools MCP, `@playwright/mcp`, or standalone Playwright automation for Codex browser inspection in this repo.

For visual inspection in the Codex in-app browser, use the bridge-backed desktop runtime instead of a bare static server. From the repo root:

```powershell
npm run dev:bridge
```

Then navigate the in-app browser to the page with the desktop bridge parameters:

```text
http://127.0.0.1:8080/saved.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1
```

If the in-app browser shows `ERR_CONNECTION_REFUSED` for `127.0.0.1:8080`, the site process is not running. If Saved loads but shows profile restore/sign-in instead of saved rows, the admin bridge on `8877` is not running or the page was opened without the desktop query parameters. Confirm both ports before debugging UI code:

```powershell
Test-NetConnection 127.0.0.1 -Port 8080
Test-NetConnection 127.0.0.1 -Port 8877
```

## Repository Navigation Tooling

These are contributor-local helpers for AI-assisted repo work. They are not Baluffo runtime,
packaging, release, Python, Node, CI, or pre-commit dependencies.

Check current toolbelt status when the environment is new, stale, or suspicious:

```bash
python scripts/toolbelt_check.py
```

Run a broader environment readiness check when setup, drift, or IDE behavior is suspect:

```bash
python scripts/ai_env_check.py --smoke
```

Install missing tools:

```bash
python scripts/toolbelt_check.py --install
```

Default to the narrowest deterministic tool:

- Use Serena for symbol-aware code reads/edits inside the repo.
- Use `rg` for text search and `fd` for file discovery before falling back to shell-native search.
- Use `ast-grep` for structural code queries, `jq`/`yq` for structured data, and `bat` for targeted line previews.
- Use `tokei` for a one-command codebase composition overview.
- Avoid broad repo packers, context generators, and token telemetry tools by default; they can increase token waste and drift when a targeted search would answer the question.

| Tool | Default use | Boundary |
|------|-------------|----------|
| Serena | Symbol-aware navigation, references, declarations, diagnostics, and refactor support | Required code-intelligence MCP; keep contributor-local install current; repo docs and source remain canonical |
| `rg` | Fast deterministic text search | Default text-search primitive for agents |
| `fd` | Fast file discovery | Prefer over `find` for agent and human repo navigation |
| `bat` | Focused previews with line numbers and ranges | Use after the relevant file or region is known |
| `jq` | Focused JSON inspection | Prefer before reading full config or data files |
| `yq` | Focused YAML/TOML/XML inspection (jq syntax) | Prefer before reading full config or data files |
| `ast-grep` | Syntax-aware structural search for Python, JS/TS, HTML, JSON, and YAML | Use when code shape matters more than exact text |
| `tokei` | Codebase line-count and composition stats | One-command overview of language/code distribution |
| `git grep` | Git-tracked-file-only search | Use when ignored or untracked files must be excluded |

Optional task-specific helper:

| Tool | Use it when | Boundary |
|------|-------------|----------|
| `mlr` | You need focused CSV/TSV/JSONL filtering, sorting, or column inspection | Install only for tabular-data tasks; do not make it a session-start requirement |

Do not use broad repo packers or context generators such as `repomix`, `gitingest`, or `code2prompt` by default. Do not use token/cost telemetry tools such as `ccusage` or `scc` as a substitute for targeted repo inspection. Use them only when the task explicitly needs a bounded context bundle or usage audit.

Linux (apt-based) toolbelt install:

```bash
sudo apt install -y ripgrep fd-find bat jq yq tokei
mkdir -p ~/.local/bin
ln -sf $(which fdfind) ~/.local/bin/fd
ln -sf $(which batcat) ~/.local/bin/bat
npm install -g @ast-grep/cli
```

Ensure `~/.local/bin` is in your PATH.

macOS (Homebrew) toolbelt install:

```bash
brew install ripgrep fd bat jq yq ast-grep tokei
```

Optional Windows toolbelt install, with package IDs checked through `winget search` on 2026-06-01:

```powershell
winget install -e --id BurntSushi.ripgrep.MSVC
winget install -e --id sharkdp.fd
winget install -e --id sharkdp.bat
winget install -e --id jqlang.jq
winget install -e --id MikeFarah.yq
winget install -e --id ast-grep.ast-grep
winget install -e --id XAMPPRocky.Tokei
```

Verify availability after install, restarting the shell first if a newly installed command is not found:

```bash
rg --version && fd --version && bat --version && jq --version && yq --version
ast-grep --version && tokei --version
python scripts/toolbelt_check.py --smoke
```

Baluffo-specific examples:

```bash
rg -n "desktop-local-data" frontend src tests
fd -e py tests src
ast-grep --lang py --pattern '$OBJ.$METHOD($$$ARGS)' src tests
jq '.scripts | keys' package.json
yq '.repos[].hooks[].id' .pre-commit-config.yaml
bat --style=numbers --line-range 60:110 docs/AI_ASSISTANT_GUIDE.md
git grep -n "desktop-local-data" -- frontend src tests
tokei src/ frontend/ --sort=code
python tools/repo_health/generate_system_map.py --output .tmp/system-map.json
```

Optional tabular-data example when `mlr` is installed:

```bash
mlr --icsv head -n 5 tests/fixtures/gamedevmap_data.csv
```

`tools/repo_health/generate_system_map.py` is an optional broad-orientation helper for AI coders. Use it only when you need a compact generated view of page surfaces, task flows, bridge routes, runtime evidence files, and high-risk areas. Its output is advisory; `AGENTS.md`, this guide, `architecture-ai-map.md`, contract docs, source, and tests remain canonical.

## Serena Session Preflight (for new client sessions)

When starting a new Codex/OpenCode assistant session against this repo:

1. Confirm Serena MCP tools are available to the client.
2. Run Serena `get_current_config`.
3. If it reports no active project, run `activate_project` for `Baluffo`.
4. Re-run `get_current_config` and verify `typescript` + `python` appear in language list.
5. Run a fast JS symbol overview check in one file to ensure tooling is healthy.
6. If setup drift is suspected, run `python scripts/ai_env_check.py --smoke --check-updates`.

This keeps language indexing from being a blocker before touching code.

See [`testing.md`](testing.md) for the full verification matrix.

## AI Editing Rules

- Load minimal context; start narrow.
- Prefer leaf modules over composition roots.
- `src/jobs/common/__init__.py` is a package marker only.
- `_runtime.facade()` is retired and should not be recreated.
- Thin compatibility surfaces include their exported names and monkeypatch/root patch seams, not only their file boundaries or line budgets.
- Update implementation, schemas, tests, and docs together when contracts or workflow move.
- When the task is documentation maintenance or doc ownership, follow [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md).
- Archive notes and git history are supporting context, not default routing sources.

### AI Operating Boundaries

- Use Serena for repo/code navigation and symbol-aware inspection; use Basic Memory only for continuity, handoff, recurring gotchas, current focus, and stale-memory correction.
- At the start of any non-trivial Baluffo task, search/read relevant Basic Memory notes for current focus, recent handoffs, recurring gotchas, or stale-memory corrections before planning edits.
- At closeout, decide whether to update Basic Memory. Update it when the task changes future-session context, creates or resolves a recurring gotcha, changes current focus, records a durable decision, or corrects stale memory.
- Verify memory claims against repo files before acting, and do not let external memory override source, tests, docs, or `AGENTS.md`.
- Do not add a new MCP, tool, or plugin until the failure mode is clear and the existing Serena/Basic Memory setup is insufficient.
- Keep narrow tasks narrow; do not broaden a bug fix into a refactor or cross more than one subsystem boundary without a short edit plan.
- Do not create a parallel workflow unless the old workflow is removed or intentionally preserved as compatibility.
- For implementation changes, run the narrowest relevant verification from this guide or [`testing.md`](testing.md) before claiming completion; if verification cannot run, state why and name the next command.
- Do not claim a behavior is fixed from inspection alone when a relevant test or syntax check exists, and do not hide partial failures behind a broad success summary.

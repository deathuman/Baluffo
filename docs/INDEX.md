# Documentation Index

> **Navigation guide for Baluffo project documentation.** Use this page to find the smallest authoritative document set for your task.
>
> For AI coding tasks, start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) and only add [`architecture-ai-map.md`](architecture-ai-map.md) when you need routing, ownership details, or compatibility-surface classification.
>
> Boundary-charter docs stay in the repo as planning/history records. They are not part of the default AI read path.

---

## Start Here

| Document | Role | Use it when |
|----------|------|-------------|
| [`README.md`](../README.md) | Overview | You need the product summary, top-level structure, and developer-facing quick start |
| [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | **AI entrypoint** | You are an AI coder and need read order, guardrails, common misconceptions, and edit-routing guidance |
| [`architecture-ai-map.md`](architecture-ai-map.md) | **Architecture map** | You need system boundaries, task-to-file routing, runtime guardrails, and verification hints |
| [`../AGENTS.md`](../AGENTS.md) | Workflow guardrails | You need repo-specific editing discipline, validation habits, and operational rules while changing code |

---

## Compatibility Surfaces

Use these as entrypoints or shims only; route new logic to the owning modules they point at.

- `src/packaged_desktop_smoke.py` - stable packaged smoke entrypoint and patch surface; implementation belongs in `src/ship/packaged_smoke/{common,startup_metrics,orchestrator,build_env,runtime,rehearsals,rehearsal_*}.py`
- `src/ship/desktop_update.py` - stable updater surface; implementation belongs in `src/ship/desktop_update_{shared,state,service}.py`
- `src/ship/desktop_updater.py` - stable updater helper executable and patch surface; implementation belongs in `src/ship/desktop_updater_{ui,release,install}.py`
- `src/admin_bridge.py` - stable thin entrypoint for bridge startup and compatibility wrappers
- `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py` - admin bridge runtime/path/session/bootstrap/manual-source/task helpers behind the stable root surface
- `src/source_discovery.py` - stable thin CLI entrypoint delegating to `src/source_discovery/*`
- `src/jobs_fetcher.py` - stable thin CLI facade; new pipeline logic belongs in `src/jobs/*`, while lazy export routing belongs in `src/jobs/fetcher_compat_exports.py` and root-backed wrapper seams belong in `src/jobs/fetcher_compat_runtime.py`
- `src/jobs/adapters/static.py` - stable static adapter surface; implementation belongs in `src/jobs/adapters/static_{runtime,listing,listing_flow,detail,sources}.py` plus `src/jobs/adapters/static_{runtime_support,detail_heuristics}.py`, while `static_helpers.py` stays a compatibility shim
- `src/source_sync.py` - permanent thin sync integration surface; config/runtime/snapshot/crypto logic belongs in `src/source_sync_{config,runtime,snapshot,crypto}.py`
- `src/local_data_store.py` - stable desktop local-data store surface; implementation belongs in `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py`
- `src/jobs/common/__init__.py` - package marker only; import `src.jobs.common.<leaf>` or package-submodule helpers
- `frontend/shared/local-data/desktop-client.js` - stable desktop local-data runtime root; implementation belongs in `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js`
- `frontend/jobs/app/desktop-update.js` - stable Jobs desktop-update export surface; implementation belongs in `frontend/jobs/app/desktop-update-{model,dom,controller}.js`
- `frontend/local-data/services.js` - transitional local-data boundary; page code should go through slice-local `services.js`
- `frontend/admin/render/ops.js` - thin compatibility render surface; ops summary/history rendering belongs in `frontend/admin/render/{ops-summary,ops-history,ops-shared}.js`

Current high-value leaf owners behind those surfaces:
- `src/bridge/ops_history_projection.py`, `src/bridge/ops_task_live.py`, `src/bridge/ops_task_{fetch_live,discovery_live,projection}.py`, `src/bridge/ops_live_payload.py`
- `src/bridge/admin_entrypoint_{runtime,services,api,registry_api,task_runtime}.py`
- `src/source_sync_{config,runtime,snapshot,crypto}.py`
- `src/jobs/fetcher_compat_{exports,runtime}.py`
- `src/jobs/adapters/static_listing_flow.py`
- `src/jobs/state_incremental.py`
- `src/ship/desktop_updater_{ui,release,install}.py`
- `frontend/jobs/app/desktop-update-{model,dom,controller}.js`
- `frontend/saved/app/runtime/*.js`
- `frontend/admin/render/{ops-summary,ops-history,ops-shared}.js`

---

## Canonical Contracts

These documents are the closest thing to source-of-truth references for stable interfaces and runtime expectations.

| Document | Authority | Canonical for |
|----------|-----------|---------------|
| [`DATA_CONTRACT.md`](DATA_CONTRACT.md) | **Canonical contract** | Data structures between pipeline, bridge, frontend, local saved data, and discovery outputs |
| [`admin-bridge-api.md`](admin-bridge-api.md) | **Canonical contract** | Admin Bridge endpoint surface and request/response routing expectations |
| [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md) | **Canonical contract** | Fetcher presets, runtime files, and fetch execution expectations |
| [`game-studios-sheet.md`](game-studios-sheet.md) | Narrow contract | Google Sheet input contract for the game studios directory funnel |

---

## Subsystem Deep Dives

Load these only when your task touches that subsystem.

| Document | Scope | Use it when |
|----------|-------|-------------|
| [`testing.md`](testing.md) | Verification strategy | You need the narrowest relevant tests, fixture layout, or test-to-source map |
| [`startup-probe-architecture.md`](startup-probe-architecture.md) | Startup perf architecture | You are changing packaged startup timing, launcher startup traces, or startup probe policy |
| [`scraping-pipeline.md`](scraping-pipeline.md) | Scraping/browser fallback flow | You are working on adapters, browser queue, Scrapy-Playwright, or extraction flow |
| [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) | Source adapter inventory | You are adding/changing a source family, plugin, or loader path |
| [`LOCAL_SETUP.md`](LOCAL_SETUP.md) | Local runtime/setup | You need browser-local vs desktop-local storage behavior, sign-in semantics, backup/restore, local-data code routing, or the smallest local command set |
| [`../tools/mcp/INDEX.md`](../tools/mcp/INDEX.md) | MCP tooling index | You need the landing page for repo MCP tooling, including required Serena setup and optional Playwright browser tooling |

---

## Operational / Process Docs

Important for maintenance, release, and support workflows, but usually not the first docs an AI should load for coding tasks.

| Document | Scope | Use it when |
|----------|-------|-------------|
| [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) | Documentation maintenance | You are deciding where docs belong, updating docs after code changes, or adding a new documentation page |
| [`../tools/mcp/INDEX.md`](../tools/mcp/INDEX.md) | MCP tooling index | You are choosing which MCP tooling doc to load under `tools/mcp/` |
| [`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md) | AI dev tooling | You are setting up the required Serena MCP workflow for Codex CLI or OpenCode, or checking the repo's Serena-memory rules |
| [`runtime-first-cleanup-handoff.md`](runtime-first-cleanup-handoff.md) | Cleanup handoff | You need historical cleanup sequencing context after checking the canonical routing docs first |
| [`RELEASE.md`](RELEASE.md) | Build and release | You are changing packaging, versioning, release flow, or artifact expectations |
| [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) | Debugging help | You are investigating a known issue or checking common failure modes |
| [`CHANGELOG.md`](CHANGELOG.md) | Historical product change log | You need recent project history or release notes context |
| [`refactor-charter-template.md`](refactor-charter-template.md) | Planning template | You are drafting a structured refactor proposal |

---

## Refactor Charters

Planning/history records for major cleanup passes. Use them only after the canonical AI docs when you need lane-specific compatibility assumptions or acceptance criteria.

| Document | Status | Use it when |
|----------|--------|-------------|
| [`desktop-packaging-boundary-charter.md`](desktop-packaging-boundary-charter.md) | Refactor record | You are changing packaged smoke or desktop updater boundaries and need lane-specific compatibility assumptions |
| [`packaged-smoke-rehearsal-boundary-charter.md`](packaged-smoke-rehearsal-boundary-charter.md) | Refactor record | You are changing the packaged smoke root or rehearsal helper family and need lane-specific compatibility assumptions |
| [`discovery-orchestrator-boundary-charter.md`](discovery-orchestrator-boundary-charter.md) | Refactor record | You are changing discovery orchestration boundaries and need lane-specific compatibility assumptions |
| [`static-adapter-boundary-charter.md`](static-adapter-boundary-charter.md) | Refactor record | You are changing the static adapter boundary and need lane-specific compatibility assumptions |
| [`jobs-fetcher-boundary-charter.md`](jobs-fetcher-boundary-charter.md) | Refactor record | You are changing the jobs fetcher facade boundary and need lane-specific compatibility assumptions |
| [`local-data-boundary-charter.md`](local-data-boundary-charter.md) | Refactor record | You are changing the file-backed local-data store or shared desktop local-data runtime boundaries and need lane-specific compatibility assumptions |
| [`desktop-update-cross-stack-boundary-charter.md`](desktop-update-cross-stack-boundary-charter.md) | Refactor record | You are changing the updater helper executable or Jobs desktop-update boundary and need lane-specific compatibility assumptions |
| [`admin-bridge-boundary-charter.md`](admin-bridge-boundary-charter.md) | Refactor record | You are changing admin bridge startup/runtime boundaries and need lane-specific compatibility assumptions |
| [`admin-ops-live-boundary-charter.md`](admin-ops-live-boundary-charter.md) | Refactor record | You are changing ops live-task payload assembly or admin ops renderer boundaries and need lane-specific compatibility assumptions |
| [`desktop-runtime-refactor-charter.md`](desktop-runtime-refactor-charter.md) | Refactor record | You are changing desktop runtime package boundaries and need lane-specific compatibility assumptions |

---

## Historical / Archive-like Docs

Useful as context, but **not authoritative** for current implementation unless explicitly revalidated.

| Document | Status | Notes |
|----------|--------|-------|
| [`scraping-pipeline-run-notes.md`](scraping-pipeline-run-notes.md) | **Historical** | Snapshot run notes from 2026-03-17; useful for context but should not override current code/contracts |

---

## Quick Routing by Goal

| Your Goal | Start Here | Then Load |
|-----------|------------|-----------|
| Understand the repo quickly | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md) |
| Understand product and top-level layout | [`README.md`](../README.md) | [`architecture-ai-map.md`](architecture-ai-map.md) |
| Change frontend behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then task-specific source files |
| Change jobs page runtime wiring | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `frontend/jobs/app/feed.js`, `frontend/jobs/app/runtime/{composition,boot,page-flow,...}.js`, and `frontend/jobs/app/runtime.js` only if the page entrypoint must change |
| Change saved page runtime wiring | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `frontend/saved/app/runtime/{composition,boot,phase-time,mutations,chrome,notes,...}.js`, `frontend/saved/app/admin-bridge-state.js`, and `frontend/saved/app/runtime.js` only if the page entry/export root must change |
| Change admin page runtime wiring | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `frontend/admin/app/runtime/*.js`, `frontend/admin/app/registry/*.js`, `frontend/admin/app/ops/*.js`, and `frontend/admin/app/runtime.js` only if the page entrypoint must change |
| Change bridge/API behavior | [`architecture-ai-map.md`](architecture-ai-map.md) | [`admin-bridge-api.md`](admin-bridge-api.md) |
| Change discovery behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `src/source_discovery/orchestrator*.py` or the relevant discovery leaf module |
| Change jobs pipeline / fetcher behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `src/jobs/*` leaf modules |
| Change Jobs desktop-update UI | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `frontend/jobs/app/desktop-update-{model,dom,controller}.js` |
| Change updater helper executable | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `src/ship/desktop_updater_{ui,release,install}.py` |
| Change desktop local-data behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), then `src/local_data_store_{shared,profiles,saved_jobs,attachments,backup}.py` or `frontend/shared/local-data/desktop/{api,lifecycle,navigation,state}.js` |
| Change payload/schema shape | [`DATA_CONTRACT.md`](DATA_CONTRACT.md) | `src/core/*`, related tests, task-specific runtime docs |
| Work on scraping/adapters | [`scraping-pipeline.md`](scraping-pipeline.md) | [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) |
| Run the right tests | [`testing.md`](testing.md) | [`architecture-ai-map.md`](architecture-ai-map.md) |
| Debug an issue | [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) | [`architecture-ai-map.md`](architecture-ai-map.md), related contract docs |
| Package or release | [`RELEASE.md`](RELEASE.md) | [`testing.md`](testing.md) |

---

## Recommended AI read sequence

For most coding tasks, load only this minimal set:

1. [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md)
2. [`architecture-ai-map.md`](architecture-ai-map.md) when you need file routing or subsystem boundaries
3. One canonical contract doc relevant to the task
4. [`testing.md`](testing.md) when you need verification commands, fixtures, or test layout
5. One subsystem deep dive only if needed

This keeps context tight and reduces hallucinations from loading too many overlapping docs.

---

## Documentation maintenance rules

Use [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) for the canonical documentation-maintenance process.

When adding or modifying documentation:

1. **Update this index** with the correct category and authority label
2. **Prefer small authoritative docs** over broad overlapping prose or duplicate overview pages
3. **Keep `AI_ASSISTANT_GUIDE.md` aligned** with actual repo structure and existing file paths

---

## Related code areas

- Source code: [`src/`](../src/)
- Frontend: [`frontend/`](../frontend/)
- Tests: [`tests/`](../tests/)
- Scripts: [`scripts/`](../scripts/)
- Runtime data/artifacts: [`data/`](../data/), `_out/`

---

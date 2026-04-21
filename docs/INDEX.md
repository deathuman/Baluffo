# Documentation Index

> **Navigation guide for Baluffo project documentation.** Use this page to find the smallest authoritative document set for your task.
>
> For AI coding tasks, start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) and only add [`architecture-ai-map.md`](architecture-ai-map.md) when you need routing, ownership details, or compatibility-surface classification.

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

- `src/packaged_desktop_smoke.py` - stable packaged smoke entrypoint and patch surface; implementation belongs in `src/ship/packaged_smoke/*`
- `src/ship/desktop_update.py` - stable updater surface; implementation belongs in `src/ship/desktop_update_{shared,state,service}.py`
- `src/admin_bridge.py` - stable thin entrypoint for bridge startup and compatibility wrappers
- `src/source_discovery.py` - stable thin CLI entrypoint delegating to `src/source_discovery/*`
- `src/jobs_fetcher.py` - stable thin CLI facade; new pipeline logic belongs in `src/jobs/*`
- `src/source_sync.py` - permanent thin sync integration surface delegating to `src/source_sync_*`
- `src/jobs/common/__init__.py` - package marker only; import `src.jobs.common.<leaf>` or package-submodule helpers
- `frontend/local-data/services.js` - transitional local-data boundary; page code should go through slice-local `services.js`

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
| [`LOCAL_SETUP.md`](LOCAL_SETUP.md) | Local runtime/setup | You need browser-local vs desktop-local storage behavior, sign-in semantics, backup/restore, or the smallest local command set |

---

## Operational / Process Docs

Important for maintenance, release, and support workflows, but usually not the first docs an AI should load for coding tasks.

| Document | Scope | Use it when |
|----------|-------|-------------|
| [`RELEASE.md`](RELEASE.md) | Build and release | You are changing packaging, versioning, release flow, or artifact expectations |
| [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) | Debugging help | You are investigating a known issue or checking common failure modes |
| [`CHANGELOG.md`](CHANGELOG.md) | Historical product change log | You need recent project history or release notes context |
| [`refactor-charter-template.md`](refactor-charter-template.md) | Planning template | You are drafting a structured refactor proposal |
| [`desktop-packaging-boundary-charter.md`](desktop-packaging-boundary-charter.md) | Active refactor tracker | You are changing packaged smoke or desktop updater boundaries and need the current compatibility assumptions |

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
| Change bridge/API behavior | [`architecture-ai-map.md`](architecture-ai-map.md) | [`admin-bridge-api.md`](admin-bridge-api.md) |
| Change discovery behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `src/source_discovery/*` |
| Change jobs pipeline / fetcher behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then `src/jobs/*` leaf modules |
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

When adding or modifying documentation:

1. **Update this index** with the correct category
2. **Label authority clearly**:
   - AI entrypoint
   - Canonical contract
   - Deep dive
   - Operational
   - Historical
3. **Prefer small authoritative docs over broad overlapping prose**
4. **Cross-link related source-of-truth docs**
5. **Mark historical notes clearly** so they are not mistaken for current guidance
6. **Keep `AI_ASSISTANT_GUIDE.md` aligned** with actual repo structure and existing file paths

---

## Related code areas

- Source code: [`src/`](../src/)
- Frontend: [`frontend/`](../frontend/)
- Tests: [`tests/`](../tests/)
- Scripts: [`scripts/`](../scripts/)
- Runtime data/artifacts: [`data/`](../data/), [`_out/`](../_out/)

---

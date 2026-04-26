# Documentation Index

> - **Status:** Active
> - **Use this when:** choosing the smallest authoritative doc set for a coding, debugging, or maintenance task
> - **Canonical for:** wiki routing, doc discovery, and the default AI read path
> - **Not canonical for:** payload details, route contracts, or subsystem implementation behavior
> - **Then inspect:** [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and one matching contract or workflow doc
> - **Last updated:** 2026-04-26

Use this page as the wiki home. Start here, load the minimum active docs you need, and use git history for old cleanup/refactor details unless the task explicitly needs historical provenance.

For AI coding tasks, start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) and only add [`architecture-ai-map.md`](architecture-ai-map.md) when you need routing, ownership details, or compatibility-surface classification.

Historical archive detail is intentionally trimmed for this personal project. [`archive/README.md`](archive/README.md) records what was retired and where to look next.

## Start Here

| Document | Role | Use it when |
|----------|------|-------------|
| [`README.md`](../README.md) | Overview | You need the product summary, top-level structure, and developer-facing quick start |
| [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | **AI entrypoint** | You are an AI coder and need read order, guardrails, common misconceptions, and edit-routing guidance |
| [`architecture-ai-map.md`](architecture-ai-map.md) | **Architecture map** | You need system boundaries, task-to-file routing, runtime guardrails, and compatibility-surface detail |
| [`../AGENTS.md`](../AGENTS.md) | Workflow guardrails | You need repo-specific editing discipline, validation habits, and the canonical docs read path |

## Compatibility Surfaces

Use compatibility roots as entrypoints or shims only. For the current root-to-leaf routing table, use [`architecture-ai-map.md`](architecture-ai-map.md). This index intentionally does not duplicate that table.

## Canonical Contracts

These documents are the closest thing to source-of-truth references for stable interfaces and runtime expectations.

| Document | Authority | Canonical for |
|----------|-----------|---------------|
| [`DATA_CONTRACT.md`](DATA_CONTRACT.md) | **Canonical contract** | Data structures between pipeline, bridge, frontend, local saved data, and discovery outputs |
| [`admin-bridge-api.md`](admin-bridge-api.md) | **Canonical contract** | Admin Bridge endpoint surface and request/response routing expectations |
| [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md) | **Canonical contract** | Fetcher presets, runtime files, and fetch execution expectations |
| [`game-studios-sheet.md`](game-studios-sheet.md) | Narrow contract | Google Sheet input contract for the game studios directory funnel |

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

## Operational / Process Docs

Important for maintenance, release, and support workflows, but usually not the first docs an AI should load for coding tasks.

| Document | Scope | Use it when |
|----------|-------|-------------|
| [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) | Documentation maintenance | You are deciding where docs belong, updating docs after code changes, or adding a new documentation page |
| [`discovery-fetch-failure-snapshot-2026-04-25.md`](discovery-fetch-failure-snapshot-2026-04-25.md) | Time-bound failure snapshot | You are picking up the discovery/fetch failures observed in the 2026-04-25 fresh build runs |
| [`source-discovery-adapter-follow-ups.md`](source-discovery-adapter-follow-ups.md) | Active follow-up tracker | You are evaluating reusable adapter improvements after the GameDevMap audit migration |
| [`repo-analysis-follow-up-2026-04-25.md`](repo-analysis-follow-up-2026-04-25.md) | Active follow-up tracker | You are picking the next residual follow-up after the completed 2026-04-25 repo-analysis closeout |
| [`../tools/mcp/INDEX.md`](../tools/mcp/INDEX.md) | MCP tooling index | You are choosing which MCP tooling doc to load under `tools/mcp/` |
| [`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md) | AI dev tooling | You are setting up the required Serena MCP workflow for Codex CLI or OpenCode, or checking the repo's Serena-memory rules |
| [`RELEASE.md`](RELEASE.md) | Build and release | You are changing packaging, versioning, release flow, or artifact expectations |
| [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) | Debugging help | You are investigating a known issue or checking common failure modes |
| [`CHANGELOG.md`](CHANGELOG.md) | Historical product change log | You need recent project history or release notes context |
| [`refactor-charter-template.md`](refactor-charter-template.md) | Planning template | You are drafting a structured refactor proposal without loading archived refactor records |

## Archive

| Document | Status | Notes |
|----------|--------|-------|
| [`archive/README.md`](archive/README.md) | Archive index | Short note for retired cleanup/refactor records; use git history for detailed provenance |

## Quick Routing by Goal

| Your Goal | Start Here | Then Load |
|-----------|------------|-----------|
| Understand the repo quickly | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md) only if you need task-to-files routing |
| Change frontend behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then the owning source files |
| Change bridge/API behavior | [`architecture-ai-map.md`](architecture-ai-map.md) | [`admin-bridge-api.md`](admin-bridge-api.md) |
| Change payload/schema shape | [`DATA_CONTRACT.md`](DATA_CONTRACT.md) | related tests and the owning runtime docs |
| Run the right tests | [`testing.md`](testing.md) | [`architecture-ai-map.md`](architecture-ai-map.md) only if you need source ownership |
| Package or release | [`RELEASE.md`](RELEASE.md) | [`testing.md`](testing.md) |
| Update docs | [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) | [`INDEX.md`](INDEX.md), then the owning authoritative doc |

## Recommended AI read sequence

For most coding tasks, load only this minimal set:

1. [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md)
2. [`architecture-ai-map.md`](architecture-ai-map.md) when you need file routing or subsystem boundaries
3. One canonical contract or workflow doc relevant to the task
4. [`testing.md`](testing.md) when you need verification commands, fixtures, or test layout
5. One subsystem deep dive only if needed

This keeps context tight and reduces hallucinations from loading too many overlapping docs.

## Documentation Maintenance Rules

Use [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) for the canonical documentation-maintenance process.

When adding or modifying documentation:

1. Update this index with the correct category and authority label
2. Prefer small authoritative docs over broad overlapping prose or duplicate overview pages
3. Keep `AI_ASSISTANT_GUIDE.md` aligned with actual repo structure and existing file paths
4. Keep archive routing short; use git history instead of long retired planning logs

## Related Code Areas

- Source code: [`src/`](../src/)
- Frontend: [`frontend/`](../frontend/)
- Tests: [`tests/`](../tests/)
- Scripts: [`scripts/`](../scripts/)
- Runtime data/artifacts: [`data/`](../data/), `_out/`

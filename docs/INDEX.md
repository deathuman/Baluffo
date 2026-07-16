# Documentation Index

> - **Status:** Active
> - **Use this when:** choosing the smallest authoritative doc set for a coding, debugging, or maintenance task
> - **Canonical for:** wiki routing, doc discovery, and the default AI read path
> - **Not canonical for:** payload details, route contracts, or subsystem implementation behavior
> - **Then inspect:** [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), and one matching contract or workflow doc
> - **Last updated:** 2026-06-04 (Umbrel raw-LAN release and troubleshooting guidance)

Use this page as the wiki home. Start here, load the minimum active docs you need, and use git history for old cleanup/refactor details unless the task explicitly needs historical provenance.

For AI coding tasks, start with [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) and only add [`architecture-ai-map.md`](architecture-ai-map.md) when you need routing, ownership details, or compatibility-surface classification.

Historical archive detail is intentionally trimmed for this personal project. [`archive/README.md`](archive/README.md) records what was retired and where to look next.

## Start Here

| Document | Role | Use it when |
|----------|------|-------------|
| [`README.md`](../README.md) | Overview | You need the product summary, top-level structure, and developer-facing quick start |
| [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | **AI entrypoint** | You are an AI coder and need read order, guardrails, common misconceptions, and edit-routing guidance |
| [`AI_CODER_SETUP.md`](AI_CODER_SETUP.md) | AI environment setup | You are preparing or checking Windows, WSL, or Linux tooling for AI-assisted work |
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
| [`sync-contract.md`](sync-contract.md) | **Canonical contract** | Source-sync snapshot shape, GitHub API versioning, and release-path notes |
| [`storage-contract.md`](storage-contract.md) | **Canonical contract** | Target runtime storage authority boundaries, SQLite/WAL discipline, compatibility exports, evidence archives, and storage migration safety |
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
| [`plans/reliable-job-availability-plan.md`](plans/reliable-job-availability-plan.md) | Active rollout | You are reviewing availability lifecycle, Saved alerts, shadow classification, promotion, or reconciliation gates |
| [`../tools/mcp/INDEX.md`](../tools/mcp/INDEX.md) | MCP tooling index | You need the landing page for repo MCP tooling, including required Serena, required Basic Memory, and the deprecated Playwright fallback |
| [`WSL_SETUP.md`](WSL_SETUP.md) | WSL development environment | You are running Baluffo from WSL2, need available tooling, or want daily commands for the WSL workflow |

## Operational / Process Docs

Important for maintenance, release, and support workflows, but usually not the first docs an AI should load for coding tasks.

| Document | Scope | Use it when |
|----------|-------|-------------|
| [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) | Documentation maintenance | You are deciding where docs belong, updating docs after code changes, or adding a new documentation page |
| [`source-policy-runbook.md`](source-policy-runbook.md) | Operator runbook | You are running discovery/fetch/soak/Admin migration-link validation or checking provider/static source-policy release readiness |
| [`environments.md`](environments.md) | Release / Environments | You are choosing source-sync writer auth, staging/prod separation, or the repository-side write policy |
| [`plans/task-abort-control-plan.md`](plans/task-abort-control-plan.md) | Active Plan / Task Lifecycle | You are adding or refining abort support for discovery, fetch, first-run bootstrap fetches, or jobs pipeline tasks |
| [`plans/umbrel-raw-lan-deployment-plan.md`](plans/umbrel-raw-lan-deployment-plan.md) | Implemented Deployment / Operations | You are preparing, validating, or operating the private Umbrel raw-LAN deployment, container runtime, GHCR image, app-store manifest, or live smoke work |
| [`plans/registry-summary-desktop-performance-cleanup-report.md`](plans/registry-summary-desktop-performance-cleanup-report.md) | Cleanup Evidence / Performance | You are checking exact registry summary diagnostics, Admin count-basis copy, or the June 2026 desktop startup/page performance benchmark evidence |
| [`plans/test-reduction-triage.md`](plans/test-reduction-triage.md) | Closeout Baseline / Test Reduction | You are checking the completed May 2026 test-reduction campaign, retained-test rationale, or prerequisites for a new coverage-backed sweep |
| [`plans/saved-job-tracker-improvements-plan.md`](plans/saved-job-tracker-improvements-plan.md) | Parked Plan / Deferred Follow-Ups | You are deciding whether to restart Saved Job Tracker work, add v2 CRM-style tracking, or revisit deferred Saved-page list-management behavior |
| [`plans/initial_findings.md`](plans/initial_findings.md) | Plans / Refactoring Inventory | You are doing jobs/fetcher refactoring, consolidation, dead-code triage, or validating analyzer findings from the 2026-05-17 initial refactoring analysis |
| [`plans/dedup-pressure-reduction-plan.md`](plans/dedup-pressure-reduction-plan.md) | Plans / Follow-Ups | You are reducing registry/dedup conflict volume after the sheet role-bucket guard and actionable Dedup badge split |
| [`plans/art-title-repair-quality-gate-plan.md`](plans/art-title-repair-quality-gate-plan.md) | Plans / Follow-Ups | You are fixing Google Sheets title-column parsing, Grackle redirect `Unknown company` repair, misleading exact source-category titles such as `Art`, or the final job-title quality gate |
| [`plans/provider-discovery-coverage-gap-plan.md`](plans/provider-discovery-coverage-gap-plan.md) | Plans / Follow-Ups | You are improving ATS/provider discovery coverage, provider migration staging evidence, or provider coverage gap reporting without adding Apify |
| [`plans/post-0.2.0-desktop-runtime-ram-reduction-plan.md`](plans/post-0.2.0-desktop-runtime-ram-reduction-plan.md) | Plans / Follow-Ups | You are revisiting desktop runtime RAM reduction, packaged startup memory, or static site process consolidation after v0.2.0 |
| [`plans/optional-playwright-browser-download-plan.md`](plans/optional-playwright-browser-download-plan.md) | Plans / Follow-Ups | You are moving Playwright browser binaries out of the portable ZIP or making packaged browser fallback an optional first-start download |
| [`plans/cloakbrowser-enhanced-browser-fallback-ab-test-plan.md`](plans/cloakbrowser-enhanced-browser-fallback-ab-test-plan.md) | Plans / Follow-Ups | You are evaluating CloakBrowser as an optional enhanced browser fallback or measuring blocked/challenged career-page recovery |
| [`snapshots/jobs-dead-source-evidence-2026-04-29.md`](snapshots/jobs-dead-source-evidence-2026-04-29.md) | Snapshots / Evidence | You are auditing the first evidence-backed physical deletion batch for dead jobs sources |
| [`snapshots/jobs-source-family-evidence-2026-04-30.md`](snapshots/jobs-source-family-evidence-2026-04-30.md) | Snapshots / Evidence | You are choosing the next deletion-first jobs fetcher source-family slice |
| [`snapshots/dedup-lifecycle-readiness-closure-2026-05-03.md`](snapshots/dedup-lifecycle-readiness-closure-2026-05-03.md) | Snapshots / Evidence | You are checking why the broad dedup lifecycle readiness plan closed without starting lifecycle UX |
| [`snapshots/dedup-current-run-blocker-triage-closure-2026-05-03.md`](snapshots/dedup-current-run-blocker-triage-closure-2026-05-03.md) | Snapshots / Evidence | You are checking why current-run blocker review closed into Google Sheets role-bucket audit |
| [`snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md`](snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md) | Snapshots / Evidence | You are checking why Google Sheets role-bucket audit closed without starting fresh |
| [`snapshots/dedup-provider-static-location-variant-evidence-2026-05-03.md`](snapshots/dedup-provider-static-location-variant-evidence-2026-05-03.md) | Snapshots / Evidence | You are checking why one carried provider/static location-label variant now warns instead of blocking |
| [`snapshots/dedup-provider-static-provider-identity-evidence-2026-05-03.md`](snapshots/dedup-provider-static-provider-identity-evidence-2026-05-03.md) | Snapshots / Evidence | You are checking why provider/static disagreement blockers dropped to the final carried manual-review set |
| [`snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md`](snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md) | Snapshots / Evidence | You are checking why dedup lifecycle readiness became warning-only after local provider/static review-state |
| [`snapshots/read-only-lifecycle-ux-implementation-2026-05-03.md`](snapshots/read-only-lifecycle-ux-implementation-2026-05-03.md) | Snapshots / Evidence | You are checking what the first read-only lifecycle label/filter slice implemented |
| [`snapshots/source-discovery-yield-evidence-2026-04-29.md`](snapshots/source-discovery-yield-evidence-2026-04-29.md) | Snapshots / Evidence | You are choosing evidence-backed source-discovery behavior changes after the deletion-first adapter cleanup |
| [`snapshots/source-discovery-zero-job-evidence-2026-04-29.md`](snapshots/source-discovery-zero-job-evidence-2026-04-29.md) | Snapshots / Evidence | You are investigating zero-job static candidate pressure before changing source-discovery behavior |
| [`snapshots/source-discovery-fresh-audit-evidence-2026-04-29.md`](snapshots/source-discovery-fresh-audit-evidence-2026-04-29.md) | Snapshots / Evidence | You need the latest fresh source-discovery audit attempt and blocker before behavior tuning |
| [`snapshots/source-discovery-directory-web-evidence-2026-04-29.md`](snapshots/source-discovery-directory-web-evidence-2026-04-29.md) | Snapshots / Evidence | You are choosing evidence-backed source-discovery behavior changes after the deletion-first adapter cleanup |
| [`../tools/mcp/INDEX.md`](../tools/mcp/INDEX.md) | MCP tooling index | You are choosing which MCP tooling doc to load under `tools/mcp/` |
| [`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md) | AI dev tooling | You are setting up the required Serena MCP workflow for Codex CLI or OpenCode, or checking the repo's Serena-memory rules |
| [`../tools/mcp/BASIC_MEMORY.md`](../tools/mcp/BASIC_MEMORY.md) | AI continuity memory | You are setting up or maintaining required Basic Memory continuity for Codex CLI or OpenCode |
| [`RELEASE.md`](RELEASE.md) | Build and release | You are changing packaging, versioning, release flow, or artifact expectations |
| [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) | Debugging help | You are investigating a known issue or checking common failure modes |
| [`CHANGELOG.md`](CHANGELOG.md) | Historical product change log | You need recent project history or release notes context |
| [`plans/refactor-charter-template.md`](plans/refactor-charter-template.md) | Planning template | You are drafting a structured refactor proposal without loading archived refactor records |


## Archive

| Document | Status | Notes |
|----------|--------|-------|
| [`archive/README.md`](archive/README.md) | Archive index | Short note for retired cleanup/refactor records; use git history for detailed provenance |
| [`archive/0.2.0-deferred-desktop-ux-polish-plan.md`](archive/0.2.0-deferred-desktop-ux-polish-plan.md) | Archived closeout | Completed pre-`0.2.0` Jobs, Saved Jobs, and Admin desktop UX polish |
| [`archive/0.2.0-release-readiness-plan.md`](archive/0.2.0-release-readiness-plan.md) | Archived pre-release approval | Pre-release `0.2.0` scope, changelog draft handoff, risk decisions, and remaining release-day gates |
| [`archive/jobs-fetcher-aggressive-simplification-closeout.md`](archive/jobs-fetcher-aggressive-simplification-closeout.md) | Archived closeout | Completed jobs-fetcher broad lifecycle/C901 simplification record |
| [`archive/dedup-lifecycle-readiness-closeout.md`](archive/dedup-lifecycle-readiness-closeout.md) | Archived closeout | Closed broad dedup lifecycle readiness tracker; active work moved through current-run blocker review to Google Sheets role-bucket audit |
| [`archive/dedup-current-run-blocker-review-closeout.md`](archive/dedup-current-run-blocker-review-closeout.md) | Archived closeout | Closed current-run blocker review tracker; active work moved to Google Sheets role-bucket audit |
| [`archive/dedup-google-sheets-role-bucket-audit-closeout.md`](archive/dedup-google-sheets-role-bucket-audit-closeout.md) | Archived closeout | Closed Google Sheets role-bucket audit tracker; active work moved to provider/static disagreement reconciliation |
| [`archive/dedup-provider-static-disagreement-reconciliation-closeout.md`](archive/dedup-provider-static-disagreement-reconciliation-closeout.md) | Archived closeout | Closed provider/static disagreement reconciliation tracker; active work moved to read-only lifecycle UX |
| [`archive/read-only-lifecycle-ux-closeout.md`](archive/read-only-lifecycle-ux-closeout.md) | Archived closeout | Closed first read-only lifecycle UX tracker after implementing conservative labels and filters |
| [`archive/runtime-storage-and-sync-architecture-plan.md`](archive/runtime-storage-and-sync-architecture-plan.md) | Archived plan with closeout | Completed runtime SQLite/WAL storage and sharded source-sync rollout; current behavior is canonical in [`storage-contract.md`](storage-contract.md) and [`sync-contract.md`](sync-contract.md) |
| [`archive/source-sync-production-readiness-closeout.md`](archive/source-sync-production-readiness-closeout.md) | Archived closeout | Closed private BaluffoSync source-sync production-readiness tracker after documenting private-repo operating controls |
| [`archive/task-progress-operational-console-closeout.md`](archive/task-progress-operational-console-closeout.md) | Archived pointer | Historical Admin task/progress console closeout; later lifecycle/progress closeout is archived in [`archive/task-lifecycle-ledger-plan.md`](archive/task-lifecycle-ledger-plan.md) |
| [`archive/task-lifecycle-ledger-plan.md`](archive/task-lifecycle-ledger-plan.md) | Archived closeout | Completed Admin task lifecycle authority, task progress projection, runtime evidence IO, pipeline child ownership, and packaged lifecycle smoke closeout |
| [`archive/bridge-route-inventory-guardrail-plan.md`](archive/bridge-route-inventory-guardrail-plan.md) | Archived plan with closeout | Completed bridge route inventory and repo-health guardrail implementation |
| [`archive/task_lifecycle_ledger_closeout_plan.md`](archive/task_lifecycle_ledger_closeout_plan.md) | Archived superseded pointer | Old underscore-named lifecycle closeout note; use [`archive/task-lifecycle-ledger-plan.md`](archive/task-lifecycle-ledger-plan.md) instead |
| [`archive/static-outlier-source-conflict-decisions.md`](archive/static-outlier-source-conflict-decisions.md) | Archived operator record | Historical Super Lucky and Koei static-outlier source conflict decisions |
| [`archive/static-scope-conflict-dry-run-decisions.md`](archive/static-scope-conflict-dry-run-decisions.md) | Archived operator record | Historical static scope conflict dry-run decisions and Arrowhead apply-safe evidence |
| [`archive/source-discovery-adapter-follow-ups-closeout.md`](archive/source-discovery-adapter-follow-ups-closeout.md) | Archived closeout | Closed source-discovery adapter follow-ups tracker |
| [`archive/external-memory-mcp-policy-plan.md`](archive/external-memory-mcp-policy-plan.md) | Archived plan with closeout | Completed external memory MCP policy and setup implementation |
| [`archive/linux-compatibility-plan.md`](archive/linux-compatibility-plan.md) | Archived plan | Completed Linux compatibility implementation: all 8 phases shipped to `main` on 2026-05-25 |
| [`archive/windows-user-data-migration-plan.md`](archive/windows-user-data-migration-plan.md) | Archived plan | Completed Windows packaged user-data migration; shipped to `main` on 2026-05-25 |
| [`archive/ai-modification-safety-improvements-plan.md`](archive/ai-modification-safety-improvements-plan.md) | Archived plan with closeout | Completed AI modification safety improvements: boundary markers, dedup/conflict/task-launch splits, Admin Ops partition, discovery labels, packaged desktop side-effect labels; deferred §3 typed contracts tracked in closeout |

## Quick Routing by Goal

| Your Goal | Start Here | Then Load |
|-----------|------------|-----------|
| Understand the repo quickly | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md) only if you need task-to-files routing |
| Change frontend behavior | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) | [`architecture-ai-map.md`](architecture-ai-map.md), then the owning source files |
| Change bridge/API behavior | [`architecture-ai-map.md`](architecture-ai-map.md) | [`admin-bridge-api.md`](admin-bridge-api.md) |
| Change payload/schema shape | [`DATA_CONTRACT.md`](DATA_CONTRACT.md) | related tests and the owning runtime docs |
| Configure recurring Jobs pipeline schedule | [`admin-bridge-api.md`](admin-bridge-api.md) | [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`testing.md`](testing.md) |
| Add or refine task abort support | [`plans/task-abort-control-plan.md`](plans/task-abort-control-plan.md) | [`admin-bridge-api.md`](admin-bridge-api.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`storage-contract.md`](storage-contract.md), [`testing.md`](testing.md) |
| Run the right tests | [`testing.md`](testing.md) | [`architecture-ai-map.md`](architecture-ai-map.md) only if you need source ownership |
| Package or release | [`RELEASE.md`](RELEASE.md) | [`testing.md`](testing.md) |
| Update docs | [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md) | [`INDEX.md`](INDEX.md), then the owning authoritative doc |
| Revisit Saved Job Tracker deferred work | [`plans/saved-job-tracker-improvements-plan.md`](plans/saved-job-tracker-improvements-plan.md) | [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`frontend/local-data`](../frontend/local-data/), [`testing.md`](testing.md) |
| Triage refactoring or dead-code candidates | [`plans/initial_findings.md`](plans/initial_findings.md) | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md), [`architecture-ai-map.md`](architecture-ai-map.md), [`testing.md`](testing.md) |
| Validate provider/static source-policy workflow | [`source-policy-runbook.md`](source-policy-runbook.md) | [`scraping-pipeline.md`](scraping-pipeline.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md) |
| Prepare, validate, or operate Umbrel raw-LAN deployment | [`RELEASE.md`](RELEASE.md) | [`plans/umbrel-raw-lan-deployment-plan.md`](plans/umbrel-raw-lan-deployment-plan.md), [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md), [`admin-bridge-api.md`](admin-bridge-api.md), [`storage-contract.md`](storage-contract.md), [`testing.md`](testing.md) |
| Fix Google Sheets category title or redirect company leaks | [`plans/art-title-repair-quality-gate-plan.md`](plans/art-title-repair-quality-gate-plan.md) | [`scraping-pipeline.md`](scraping-pipeline.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`testing.md`](testing.md) |
| Improve provider discovery coverage | [`plans/provider-discovery-coverage-gap-plan.md`](plans/provider-discovery-coverage-gap-plan.md) | [`scraping-pipeline.md`](scraping-pipeline.md), [`source-policy-runbook.md`](source-policy-runbook.md), [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) |
| Revisit desktop RAM reduction | [`plans/post-0.2.0-desktop-runtime-ram-reduction-plan.md`](plans/post-0.2.0-desktop-runtime-ram-reduction-plan.md) | [`startup-probe-architecture.md`](startup-probe-architecture.md), [`architecture-ai-map.md`](architecture-ai-map.md), [`testing.md`](testing.md) |
| Make packaged browser fallback optional | [`plans/optional-playwright-browser-download-plan.md`](plans/optional-playwright-browser-download-plan.md) | [`RELEASE.md`](RELEASE.md), [`testing.md`](testing.md), [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md), [`scraping-pipeline.md`](scraping-pipeline.md) |
| A/B test enhanced browser fallback | [`plans/cloakbrowser-enhanced-browser-fallback-ab-test-plan.md`](plans/cloakbrowser-enhanced-browser-fallback-ab-test-plan.md) | [`scraping-pipeline.md`](scraping-pipeline.md), [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md), [`testing.md`](testing.md) |

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

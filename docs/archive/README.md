# Documentation Archive

> Historical cleanup and refactor records were trimmed on 2026-04-26, legacy active-plan routing was pruned again on 2026-04-30, the completed jobs-fetcher plus dedup readiness closeouts were archived on 2026-05-03, the completed runtime storage/source-sync plan, completed task lifecycle ledger plan, superseded lifecycle note, and completed static decision records were archived on 2026-05-12, the completed bridge route inventory guardrail plan was archived on 2026-05-13, and the pre-`0.2.0` UX/readiness records were archived on 2026-05-15.

This repository now keeps active documentation in `docs/` and relies on git history for detailed cleanup records, old refactor charters, and completed task logs. The archive is intentionally small so the docs remain useful for a personal project.

Latest archives:

- Completed Linux compatibility implementation lives in [`linux-compatibility-plan.md`](linux-compatibility-plan.md). All 8 phases shipped to `main` on 2026-05-25.
- Completed Windows packaged user-data migration lives in [`windows-user-data-migration-plan.md`](windows-user-data-migration-plan.md). Implementation shipped to `main` on 2026-05-25.
- Completed AI modification safety improvements live in [`ai-modification-safety-improvements-plan.md`](ai-modification-safety-improvements-plan.md). All nine sections (boundary markers, route inventory, typed contracts, builder splits, Admin Ops partition, discovery stage labels, packaged desktop side-effect labels) shipped to `main` by 2026-05-27; deferred §3 typed-contract follow-ups are tracked in the closeout.
- Completed cold-run Jobs freshness strategy lives in [`cold-run-jobs-freshness-strategy-plan.md`](cold-run-jobs-freshness-strategy-plan.md). Strip bundled row artifacts, auto-start Google Sheets bootstrap on first cold start, `POST /tasks/run-jobs-bootstrap` bridge route, staged promotion with rollback, `pipeline_never_run` alert, `coverageScope: "bootstrap_sheets"` metadata, startup artifact quarantine, and 17 loophole guardrails — all shipped to `main` by 2026-05-28.
- Completed daily-trust UX strategy lives in [`daily-trust-ux-strategy-plan.md`](daily-trust-ux-strategy-plan.md). Workspace Action Center (4 signals, 30s polling, dismiss with 4h TTL, copy diagnostics), context-aware right inspector (slide-out overlay, 5 entity adapters), honest inline recovery, admin section nav bar, discovery pending count badge, fetcher log error highlighting, saved views + recent views bar, explain-this-state overlays, system map page, and 6 demo fixtures — all shipped to `main` by 2026-05-28. 20 loopholes closed during audit.

Use the active docs first:

- [`../INDEX.md`](../INDEX.md) for routing.
- [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md) for AI read order and edit discipline.
- [`../architecture-ai-map.md`](../architecture-ai-map.md) for current ownership boundaries.

Retired archive categories:

- Boundary/refactor charters for previous root-thinning waves.
- Completed repo-health and repo-analysis implementation logs.
- Time-bound scraping/fetch run notes that no longer describe current behavior.
- April 2026 repo-analysis residual trackers and early source-discovery HTTP-recovery snapshots.
- Completed pre-`0.2.0` desktop UX polish lives in [`0.2.0-deferred-desktop-ux-polish-plan.md`](0.2.0-deferred-desktop-ux-polish-plan.md), and pre-release `0.2.0` approval plus release-day gates live in [`0.2.0-release-readiness-plan.md`](0.2.0-release-readiness-plan.md).
- Completed jobs-fetcher broad lifecycle/C901 simplification closeout lives in [`jobs-fetcher-aggressive-simplification-closeout.md`](jobs-fetcher-aggressive-simplification-closeout.md).
- Closed bridge route inventory guardrail plan lives in [`bridge-route-inventory-guardrail-plan.md`](bridge-route-inventory-guardrail-plan.md).
- Closed dedup lifecycle readiness closeout lives in [`dedup-lifecycle-readiness-closeout.md`](dedup-lifecycle-readiness-closeout.md).
- Closed current-run blocker review closeout lives in [`dedup-current-run-blocker-review-closeout.md`](dedup-current-run-blocker-review-closeout.md); later Google Sheets role-bucket audit closeout lives in [`dedup-google-sheets-role-bucket-audit-closeout.md`](dedup-google-sheets-role-bucket-audit-closeout.md), provider/static reconciliation closeout lives in [`dedup-provider-static-disagreement-reconciliation-closeout.md`](dedup-provider-static-disagreement-reconciliation-closeout.md), first read-only lifecycle UX closeout lives in [`read-only-lifecycle-ux-closeout.md`](read-only-lifecycle-ux-closeout.md), Admin health dashboard closeout lives in [`admin-health-dashboard-console-closeout.md`](admin-health-dashboard-console-closeout.md), Admin task/progress console pointer lives in [`task-progress-operational-console-closeout.md`](task-progress-operational-console-closeout.md), completed task lifecycle ledger closeout lives in [`task-lifecycle-ledger-plan.md`](task-lifecycle-ledger-plan.md), the old superseded underscore lifecycle note lives in [`task_lifecycle_ledger_closeout_plan.md`](task_lifecycle_ledger_closeout_plan.md), completed static decision records live in [`static-outlier-source-conflict-decisions.md`](static-outlier-source-conflict-decisions.md) and [`static-scope-conflict-dry-run-decisions.md`](static-scope-conflict-dry-run-decisions.md), source-sync production-readiness closeout lives in [`source-sync-production-readiness-closeout.md`](source-sync-production-readiness-closeout.md), source-discovery follow-ups closeout lives in [`source-discovery-adapter-follow-ups-closeout.md`](source-discovery-adapter-follow-ups-closeout.md), external memory MCP policy closeout lives in [`external-memory-mcp-policy-plan.md`](external-memory-mcp-policy-plan.md), and completed runtime storage/source-sync rollout history lives in [`runtime-storage-and-sync-architecture-plan.md`](runtime-storage-and-sync-architecture-plan.md).

Current active follow-up trackers live in [`../plans/`](../plans/).

For exact historical detail, inspect git history around the removed file names or the commits that introduced the related behavior.

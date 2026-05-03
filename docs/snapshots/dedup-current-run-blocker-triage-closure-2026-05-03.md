# Dedup Current-Run Blocker Triage Closure Evidence - 2026-05-03

> - **Status:** Closure snapshot
> - **Use this when:** checking why current-run blocker review closed without starting lifecycle UX
> - **Canonical for:** 2026-05-03 current-run dedup blocker triage, blocker buckets, and transferred follow-up ownership
> - **Not canonical for:** dedup payload schema, merge rules, lifecycle label behavior, or Admin route contracts
> - **Then inspect:** [`dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md`](dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md), [`../plans/dedup-provider-static-disagreement-reconciliation-plan.md`](../plans/dedup-provider-static-disagreement-reconciliation-plan.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

## Real-Data Gate Read

Command:

```powershell
python src/jobs_fetcher.py
```

Result:

- `dedupAuditGate.status`: `blocked`
- `dedupAuditGate.lifecycleUxReady`: `false`
- blockers: `current_run_non_primary_merges_need_review`, `provider_static_disagreement_needs_review`, `high_risk_review_queue_causes_need_review`
- warnings: `carried_high_risk_review_queue_causes_present`, `carried_source_bundle_collisions_present`

Current-run merge reason counts:

- primaryUrl: 0
- secondaryKey: 122
- sparseIdentity: 4
- knownMirrorPair: 0
- socialKey: 0
- unknown: 0

High-risk review cause counts:

- spreadsheet_role_bucket_needs_review: 2390
- google_sheets_role_bucket_needs_review: 2036
- parser_or_directory_text_pollution: 429
- non_provider_url_identity_needs_review: 424
- listing_page_bundle: 245
- provider_static_disagreement: 33

Provider/static disagreement gate counts:

- blocked: 20
- currentRunBlocked: 5
- carriedBlocked: 15
- warning: 13

Provider/static classifications:

- title_company_collision: 14
- static_parser_url_variant: 10
- provider_redirect_or_canonical_url: 6
- same_job_different_urls: 3

## Triage Decision

The current-run blocker review plan is closed as **blocked but transferred**.

The gate is readable and the blocker families are now specific. Lifecycle UX is still not ready because high-risk review causes are dominated by Google Sheets role buckets. Current-run non-primary merge blockers remain visible through `secondaryKey` and `sparseIdentity` examples, and provider/static disagreement still blocks, but both are smaller than the Google Sheets role-bucket problem.

Active follow-up now lives in [`../plans/dedup-provider-static-disagreement-reconciliation-plan.md`](../plans/dedup-provider-static-disagreement-reconciliation-plan.md) after the Google Sheets role-bucket audit closure.

## Transferred Ownership

The next owner should start from Google Sheets role-bucket diagnostics, then use `currentRunMergeExamplesByReason.secondaryKey` and `currentRunMergeExamplesByReason.sparseIdentity` to determine whether supporting identity-key blockers are real same-job identity, parser/source identity noise, or role/category bucket pollution.

Read-only lifecycle UX remains deferred until a future real-data run reports:

- `dedupAuditGate.lifecycleUxReady=true`
- `dedupAuditGate.currentRunNonPrimaryMergeCounts.blocking=0`
- `providerStaticDisagreementGateCounts.blocked=0`
- high-risk review queue causes are absent, reviewed, or warning-only

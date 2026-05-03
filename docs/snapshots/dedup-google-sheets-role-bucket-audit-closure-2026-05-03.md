# Dedup Google Sheets Role-Bucket Audit Closure Evidence - 2026-05-03

> - **Status:** Closure snapshot
> - **Use this when:** checking why Google Sheets role-bucket audit closed without starting lifecycle UX
> - **Canonical for:** 2026-05-03 Google Sheets role-bucket audit result, guard-fixed counts, and transferred follow-up ownership
> - **Not canonical for:** dedup payload schema, merge rules, lifecycle label behavior, or Admin route contracts
> - **Then inspect:** [`../archive/dedup-google-sheets-role-bucket-audit-closeout.md`](../archive/dedup-google-sheets-role-bucket-audit-closeout.md), [`dedup-provider-static-reconciliation-closure-2026-05-03.md`](dedup-provider-static-reconciliation-closure-2026-05-03.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

## Real-Data Gate Read

Command:

```powershell
python src/jobs_fetcher.py
```

Result:

- `dedupAuditGate.status`: `blocked`
- `dedupAuditGate.lifecycleUxReady`: `false`
- blockers: `provider_static_disagreement_needs_review`
- warnings: `carried_provider_static_auto_safe_variants_present`, `carried_high_risk_review_queue_causes_present`, `carried_source_bundle_collisions_present`
- `currentRunHighRiskReviewQueueCount`: 0
- `googleSheetsRoleBucketUnresolvedCount`: 0

Google Sheets role-bucket audit:

- `googleSheetsGenericRoleGuardBlockedCount`: 6069
- guard-blocked `secondaryKey`: 1607
- guard-blocked `sparseIdentity`: 4462
- `classificationCounts.fixed_by_generic_role_guard`: 6069
- `classificationCounts.allowed_same_primary_url`: 219
- `classificationCounts.historical_carried_bundle`: 4425
- `classificationCounts.unresolved_current_run_role_bucket`: 0
- `classificationCounts.parser_or_sheet_category_noise`: 0
- `classificationCounts.needs_narrow_dedup_guard`: 0

Provider/static disagreement:

- `providerStaticDisagreementGateCounts.blocked`: 15
- `providerStaticDisagreementGateCounts.warning`: 18
- `providerStaticDisagreementGateCounts.currentRunBlocked`: 0
- `providerStaticDisagreementGateCounts.carriedBlocked`: 15
- `providerStaticDisagreementGateCounts.carriedWarning`: 18
- `providerStaticDisagreementGateCounts.autoSafeWarning`: 18
- `providerStaticDisagreementGateCounts.reviewedSafeWarning`: 0
- `providerStaticDisagreementGateCounts.confirmedBlocking`: 0

Provider/static classifications:

- `same_job_different_urls`: 2
- `provider_redirect_or_canonical_url`: 5
- `static_parser_url_variant`: 13
- `title_company_collision`: 13
- `stale_carried_bundle`: 0
- `needs_manual_review`: 0

## Triage Decision

The Google Sheets role-bucket audit plan is closed as **completed but transferred**.

Google Sheets role-bucket pressure is now explainable: current-run unresolved role buckets are zero, guard-blocked attempts are explicitly counted, same-primary-URL bundles are allowed, and the remaining Google Sheets evidence is carried historical warning pressure.

Later provider/static reconciliation made lifecycle UX ready in warning-only mode. Current active follow-up lives in [`../plans/read-only-lifecycle-ux-plan.md`](../plans/read-only-lifecycle-ux-plan.md).

## Re-Entry Criteria

Read-only lifecycle UX remains deferred until a future real-data run reports:

- `dedupAuditGate.lifecycleUxReady=true`
- `providerStaticDisagreementGateCounts.blocked=0`
- carried provider/static blockers are reviewed, confirmed blocking with a narrow owner, or safely downgraded by existing evidence rules
- current-run high-risk review queue count remains zero or warning-only

# Dedup Provider/Static Location-Variant Evidence - 2026-05-03

> - **Status:** Snapshot
> - **Use this when:** checking why provider/static disagreement blockers dropped without local review-state mutation
> - **Canonical for:** 2026-05-03 carried provider/static location-variant gate evidence
> - **Not canonical for:** payload schemas, dedup merge rules, local review actions, lifecycle labels, or source-policy mutation
> - **Then inspect:** [`dedup-provider-static-reconciliation-closure-2026-05-03.md`](dedup-provider-static-reconciliation-closure-2026-05-03.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

## Command

```powershell
python src/jobs_fetcher.py
```

## Gate Result

- `dedupAuditGate.status=blocked`
- `dedupAuditGate.lifecycleUxReady=false`
- `dedupAuditGate.blockers=["provider_static_disagreement_needs_review"]`
- `dedupAuditGate.warnings=["carried_provider_static_auto_safe_variants_present","carried_high_risk_review_queue_causes_present","carried_source_bundle_collisions_present"]`

## Provider/Static Counts

- `providerStaticDisagreementGateCounts.blocked=14`
- `providerStaticDisagreementGateCounts.warning=19`
- `providerStaticDisagreementGateCounts.currentRunBlocked=0`
- `providerStaticDisagreementGateCounts.carriedBlocked=14`
- `providerStaticDisagreementGateCounts.carriedWarning=19`
- `providerStaticDisagreementGateCounts.autoSafeWarning=18`
- `providerStaticDisagreementGateCounts.confirmedBlocking=0`

## Title/Company Collision Audit

- `providerStaticTitleCompanyCollisionAuditCounts.carried_location_pollution=0`
- `providerStaticTitleCompanyCollisionAuditCounts.carried_location_variant=1`
- `providerStaticTitleCompanyCollisionAuditCounts.possible_real_multi_location_conflict=12`
- `providerStaticTitleCompanyCollisionAuditCounts.not_carried=0`
- `providerStaticTitleCompanyCollisionAuditCounts.unknown=0`

The new `carried_location_variant` row is a carried title/company collision where location labels normalize to the same city and the provider/static sides share strong URL or identifier evidence. It warns instead of blocking, but this does not clear the lifecycle gate because 14 carried provider/static disagreement rows remain unresolved.

## Decision

This snapshot was an intermediate refinement. Later provider/static reconciliation made lifecycle UX ready in warning-only mode, and the first read-only lifecycle UX slice is recorded in [`../archive/read-only-lifecycle-ux-closeout.md`](../archive/read-only-lifecycle-ux-closeout.md).

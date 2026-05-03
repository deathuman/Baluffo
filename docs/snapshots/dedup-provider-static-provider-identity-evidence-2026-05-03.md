# Dedup Provider/Static Provider-Identity Evidence - 2026-05-03

> - **Status:** Snapshot
> - **Use this when:** checking why provider/static disagreement blockers dropped to the final carried manual-review set
> - **Canonical for:** 2026-05-03 provider-identity-backed carried location conflict evidence
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

- `providerStaticDisagreementGateCounts.blocked=3`
- `providerStaticDisagreementGateCounts.warning=30`
- `providerStaticDisagreementGateCounts.currentRunBlocked=0`
- `providerStaticDisagreementGateCounts.carriedBlocked=3`
- `providerStaticDisagreementGateCounts.carriedWarning=30`
- `providerStaticDisagreementGateCounts.autoSafeWarning=18`
- `providerStaticDisagreementGateCounts.confirmedBlocking=0`

## Title/Company Collision Audit

- `providerStaticTitleCompanyCollisionAuditCounts.carried_location_pollution=0`
- `providerStaticTitleCompanyCollisionAuditCounts.carried_location_variant=2`
- `providerStaticTitleCompanyCollisionAuditCounts.carried_provider_identity_location_conflict=10`
- `providerStaticTitleCompanyCollisionAuditCounts.possible_real_multi_location_conflict=1`
- `providerStaticTitleCompanyCollisionAuditCounts.not_carried=0`
- `providerStaticTitleCompanyCollisionAuditCounts.unknown=0`

`carried_provider_identity_location_conflict` is a warning-only carried metadata diagnosis for rows where polluted carried location labels coexist with multiple plausible carried locations, but provider/static sides share provider job identity evidence. This did not mutate local review state and did not change dedup merge behavior.

## Remaining Blockers

- Animoca Brands `Executive Assistant`: same-job/different-URL provider/static disagreement, no shared identifier tokens.
- Wargaming `Global Help Desk Specialist`: same-job/different-URL provider/static disagreement, no shared identifier tokens.
- Wargaming `Render Engineer (Unannounced project)`: title/company collision with `prague` and `prague, czechia`, cross-host provider/static evidence and no shared identifier tokens.

## Decision

This snapshot was an intermediate refinement. Later row-level review-state reconciliation made lifecycle UX ready in warning-only mode; current active follow-up lives in [`../plans/read-only-lifecycle-ux-plan.md`](../plans/read-only-lifecycle-ux-plan.md).

# Dedup Provider/Static Reconciliation Closure - 2026-05-03

> - **Status:** Snapshot
> - **Use this when:** checking the real-data gate result after local provider/static review-state reconciliation
> - **Canonical for:** 2026-05-03 lifecycle UX readiness evidence after provider/static reconciliation
> - **Not canonical for:** payload schemas, dedup merge rules, local review actions, lifecycle labels, or source-policy mutation
> - **Then inspect:** [`../archive/dedup-provider-static-disagreement-reconciliation-closeout.md`](../archive/dedup-provider-static-disagreement-reconciliation-closeout.md), [`../plans/read-only-lifecycle-ux-plan.md`](../plans/read-only-lifecycle-ux-plan.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

## Commands

Local review-state reconciliation used the existing dedup review-state action contract to write `data/dedup-review-state.json`, then the normal fetch path was rerun:

```powershell
python src/jobs_fetcher.py
```

## Gate Result

- `dedupAuditGate.status=warning`
- `dedupAuditGate.lifecycleUxReady=true`
- `dedupAuditGate.blockers=[]`
- `dedupAuditGate.warnings=["carried_provider_static_auto_safe_variants_present","carried_provider_static_reviewed_safe_present","carried_high_risk_review_queue_causes_present","carried_source_bundle_collisions_present"]`

## Provider/Static Counts

- `providerStaticDisagreementGateCounts.blocked=0`
- `providerStaticDisagreementGateCounts.warning=33`
- `providerStaticDisagreementGateCounts.currentRunBlocked=0`
- `providerStaticDisagreementGateCounts.carriedBlocked=0`
- `providerStaticDisagreementGateCounts.carriedWarning=33`
- `providerStaticDisagreementGateCounts.autoSafeWarning=18`
- `providerStaticDisagreementGateCounts.reviewedSafeWarning=3`
- `providerStaticDisagreementGateCounts.confirmedBlocking=0`

## Local Review-State

Three exact carried provider/static rows were marked `reviewed_safe` in the local runtime artifact:

- Animoca Brands `Executive Assistant`
- Wargaming `Global Help Desk Specialist`
- Wargaming `Render Engineer (Unannounced project)`

`data/dedup-review-state.json` remains local runtime evidence and is not a committed contract file. If the artifact is removed or malformed, the bridge/report should surface the read warning and provider/static disagreement can block again until the review-state is restored or rows are re-reviewed.

## Decision

The dedup audit gate is ready enough for the first read-only lifecycle UX slice. The next work must stay label/filter-only and must not change retention, merge rules, source registries, cleanup behavior, source-policy state, or saved-job persistence.

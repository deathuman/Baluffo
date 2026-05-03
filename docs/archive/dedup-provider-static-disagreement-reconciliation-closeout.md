# Dedup Provider/Static Disagreement Reconciliation Closeout

> - **Status:** Closed: lifecycle UX ready after local row-level reconciliation
> - **Use this when:** checking why provider/static disagreement stopped blocking read-only lifecycle UX
> - **Canonical for:** provider/static disagreement reconciliation closeout and handoff to lifecycle UX
> - **Not canonical for:** payload schemas, dedup merge rules, local review actions, lifecycle labels, or source-policy mutation
> - **Then inspect:** [`../snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md`](../snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md), [`../plans/read-only-lifecycle-ux-plan.md`](../plans/read-only-lifecycle-ux-plan.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

The provider/static disagreement reconciliation tracker is closed. Google Sheets role-bucket blockers were already resolved into warnings, and the remaining provider/static blocker set was narrowed to three carried rows. Those three exact rows were recorded as local `reviewed_safe` evidence in `data/dedup-review-state.json`.

## Closure Evidence

Latest real-data fetch after local reconciliation:

- `dedupAuditGate.status=warning`
- `dedupAuditGate.lifecycleUxReady=true`
- `dedupAuditGate.blockers=[]`
- `providerStaticDisagreementGateCounts.blocked=0`
- `providerStaticDisagreementGateCounts.warning=33`
- `providerStaticDisagreementGateCounts.reviewedSafeWarning=3`
- `providerStaticDisagreementGateCounts.confirmedBlocking=0`

## Reviewed Rows

- Animoca Brands `Executive Assistant`: reviewed safe as a carried-only provider/static disagreement with exact title/company/location and provider/static detail URLs.
- Wargaming `Global Help Desk Specialist`: reviewed safe as a carried-only provider/static disagreement with exact title/company/location and provider/static detail URLs.
- Wargaming `Render Engineer (Unannounced project)`: reviewed safe as a carried-only provider/static disagreement with exact title/company, equivalent Prague location labels, and provider/static detail URLs.

## Decision

Lifecycle UX may proceed to the first read-only slice. This closeout does not add lifecycle labels, merge/unmerge controls, source cleanup, registry mutation, source-policy mutation, or dedup merge-rule changes. The next active owner is [`../plans/read-only-lifecycle-ux-plan.md`](../plans/read-only-lifecycle-ux-plan.md).

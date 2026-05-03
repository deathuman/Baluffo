# Read-Only Lifecycle UX Implementation Snapshot - 2026-05-03

> - **Status:** Implemented locally, pending final ship gate
> - **Use this when:** checking what the first lifecycle UX slice exposed after dedup readiness passed
> - **Canonical for:** lifecycle UX implementation evidence for the first read-only labels and filters
> - **Not canonical for:** lifecycle retention policy, dedup merge rules, cleanup, or Saved-job persistence
> - **Then inspect:** [`../archive/read-only-lifecycle-ux-closeout.md`](../archive/read-only-lifecycle-ux-closeout.md), [`../source-policy-runbook.md`](../source-policy-runbook.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-05-03

## Gate Input

The implementation followed the latest provider/static reconciliation closure:

- `dedupAuditGate.status=warning`
- `dedupAuditGate.lifecycleUxReady=true`
- `dedupAuditGate.blockers=[]`
- Remaining provider/static hard blockers were reconciled through local `data/dedup-review-state.json` review-state evidence.

## Implemented UX

Jobs now exposes first-slice lifecycle filters without changing lifecycle behavior:

- `Recently removed` from `status="likely_removed"`.
- `Reappeared` from `lifecycleEvent="reappeared"`.
- `Preserved because source failed` from `lifecycleEvent="preserved"` and `lifecycleReason="source_failed"`.
- `Any` as an explicit all-status listing filter.

The existing `New` Jobs badge remains a user-seen freshness badge, not a pipeline lifecycle event. `Preserved because source skipped` remains operational-only and is not exposed as a first-slice user-facing filter.

Saved continues to render lifecycle labels through the live overlay from current jobs/lifecycle artifacts. Persisted saved-job rows and saved-job schemas were not expanded.

## Non-Changes

- No lifecycle retention changes.
- No merge/unmerge controls.
- No cleanup, hide, reject, tombstone, or delete behavior.
- No registry, source-policy, source-sync, or `REDUNDANT_STATIC_IF_PROVIDER` mutation.
- No Saved-job persistence mutation.

## Focused Validation

Focused frontend coverage passed for:

- Jobs lifecycle filter matching.
- Lifecycle quick-filter values and summaries.
- Jobs HTML filter options.
- URL persistence for lifecycle filter values.
- Existing Jobs/Saved lifecycle badge rendering and Saved live overlay behavior.

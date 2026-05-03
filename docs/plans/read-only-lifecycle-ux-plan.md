# Read-Only Lifecycle UX Plan

> - **Status:** Active next-step tracker
> - **Use this when:** implementing the first user-facing lifecycle labels and filters after dedup gate readiness
> - **Canonical for:** first read-only lifecycle UX scope, preconditions, and forbidden behavior changes
> - **Not canonical for:** dedup merge rules, lifecycle retention policy, source cleanup, source-policy mutation, or saved-job persistence changes
> - **Then inspect:** [`../snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md`](../snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md), [`../source-policy-runbook.md`](../source-policy-runbook.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-05-03

Dedup lifecycle readiness is now warning-only in the latest local real-data fetch after provider/static review-state reconciliation. The next product slice is read-only lifecycle UX, not cleanup or dedup mutation.

## Preconditions

- `dedupAuditGate.lifecycleUxReady=true` on the latest real-data fetch.
- `dedupAuditGate.blockers=[]`.
- Provider/static disagreement blockers are zero or row-level reviewed in `data/dedup-review-state.json`.
- If `data/dedup-review-state.json` is missing or malformed, repair or re-run review-state reconciliation before treating lifecycle UX as ready.

## Scope

Add user-facing read-only labels and filters only:

- `Reappeared`
- `Recently removed`
- `Preserved because source failed`

Keep `New` as the existing user-seen Jobs badge, not a new pipeline lifecycle event. Keep `Preserved because source skipped` operational-only for this first slice.

Saved should read lifecycle labels through a live overlay from current jobs/lifecycle artifacts. Do not mutate persisted saved-job rows for this milestone.

## Forbidden

- No merge/unmerge controls.
- No lifecycle retention changes.
- No source cleanup, hide, reject, tombstone, or delete actions.
- No source registry mutation.
- No source-policy mutation.
- No `REDUNDANT_STATIC_IF_PROVIDER` mutation.
- No persisted saved-job schema expansion for lifecycle labels.

## Validation Standard

Before implementation:

```powershell
python src/jobs_fetcher.py
```

Inspect:

- `dedupEvidence.dedupAuditGate.status`
- `dedupEvidence.dedupAuditGate.lifecycleUxReady`
- `dedupEvidence.dedupAuditGate.blockers`
- `dedupEvidence.providerStaticDisagreementGateCounts`

Implementation validation should include the focused frontend label/filter tests plus:

```powershell
cmd /c npm run lint:precommit
```

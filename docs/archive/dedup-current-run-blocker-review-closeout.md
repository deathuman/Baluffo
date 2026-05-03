# Dedup Current-Run Blocker Review Closeout

> - **Status:** Closed: transferred through Google Sheets audit to provider/static reconciliation
> - **Use this when:** checking why current-run blocker review was retired
> - **Canonical for:** current-run blocker triage closure, transferred identity-audit ownership, and lifecycle UX re-entry criteria
> - **Not canonical for:** persisted payload schemas, bridge route contracts, dedup merge rules, or saved-job storage contracts
> - **Then inspect:** [`../snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md`](../snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md), [`dedup-provider-static-disagreement-reconciliation-closeout.md`](dedup-provider-static-disagreement-reconciliation-closeout.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

The broad dedup lifecycle readiness tracker is closed, and this current-run blocker review tracker is now closed too. Follow-up Google Sheets role-bucket audit later made the dominant role-bucket blocker decisionable, and active ownership moved to provider/static disagreement reconciliation.

## Closeout State

- `dedupAuditGate.status=blocked`.
- `dedupAuditGate.lifecycleUxReady=false`.
- Hard blockers are `current_run_non_primary_merges_need_review`, `provider_static_disagreement_needs_review`, and `high_risk_review_queue_causes_need_review`.
- Current-run non-primary merge blockers remain present at `secondaryKey=122` and `sparseIdentity=4`.
- High-risk review causes are dominated by `spreadsheet_role_bucket_needs_review=2390` and `google_sheets_role_bucket_needs_review=2036`.
- Provider/static disagreement remains blocking with 20 blocked rows, including 5 current-run blockers and 15 carried blockers.

## Closure Outcome

Closed as blocked but transferred.

The gate was decisionable enough to retire this review tracker. Later Google Sheets role-bucket audit and provider/static reconciliation closed, and the first read-only lifecycle UX slice is recorded in [`read-only-lifecycle-ux-closeout.md`](read-only-lifecycle-ux-closeout.md).

## Deferred Work

- Do not add lifecycle labels while `dedupAuditGate.status=blocked`.
- Do not add merge/unmerge controls from this track.
- Do not change dedup merge rules based only on advisory diagnostics.
- Do not perform source cleanup, registry mutation, lifecycle retention changes, or source-policy mutation from this plan.

## Historical Validation Standard

Focused blocker review:

```powershell
python src/jobs_fetcher.py
```

Then inspect:

- `dedupEvidence.dedupAuditGate.status`
- `dedupEvidence.dedupAuditGate.lifecycleUxReady`
- `dedupEvidence.dedupAuditGate.blockers`
- `dedupEvidence.providerStaticDisagreementGateCounts`
- `dedupEvidence.providerStaticDisagreementClassificationCounts`
- `dedupEvidence.reviewQueueCauseCounts`

If gate behavior or review-state behavior changes:

```powershell
python -m pytest -q tests/test_jobs_dedup_audit_gate.py tests/test_jobs_dedup_audit_gate_provider_static.py tests/test_jobs_dedup_provider_static_disagreement_gate.py
cmd /c npm run lint:precommit
```

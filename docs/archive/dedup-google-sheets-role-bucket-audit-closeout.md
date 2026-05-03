# Dedup Google Sheets Role-Bucket Audit Closeout

> - **Status:** Closed: transferred to provider/static disagreement reconciliation
> - **Use this when:** checking why Google Sheets role-bucket audit was retired
> - **Canonical for:** Google Sheets role-bucket blocker closeout, guard-fixed evidence, and lifecycle UX transfer criteria
> - **Not canonical for:** persisted payload schemas, bridge route contracts, dedup merge rules, or saved-job storage contracts
> - **Then inspect:** [`../snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md`](../snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md), [`dedup-provider-static-disagreement-reconciliation-closeout.md`](dedup-provider-static-disagreement-reconciliation-closeout.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

The Google Sheets role-bucket audit tracker is closed. The latest real-data evidence shows the Google Sheets blocker family is decisionable: unresolved current-run Google Sheets role-bucket blockers are zero, while the remaining lifecycle gate blocker is carried provider/static disagreement.

## Closeout State

- `dedupAuditGate.status=blocked`.
- `dedupAuditGate.lifecycleUxReady=false`.
- Hard blockers are now limited to `provider_static_disagreement_needs_review`.
- `currentRunHighRiskReviewQueueCount=0`.
- `googleSheetsRoleBucketUnresolvedCount=0`.
- `googleSheetsGenericRoleGuardBlockedCount=6069`.
- Guard-blocked attempts split into `secondaryKey=1607` and `sparseIdentity=4462`.
- `googleSheetsRoleBucketAudit.classificationCounts.fixed_by_generic_role_guard=6069`.
- `allowed_same_primary_url=219`.
- `historical_carried_bundle=4425`.

## Closure Outcome

Closed as completed but transferred.

The Google Sheets role-bucket blocker is no longer the active lifecycle-readiness owner. It is now explained by existing guard behavior, same-primary-URL allowances, and carried historical evidence. Lifecycle UX remains paused because provider/static disagreement still has unresolved carried blockers.

Later provider/static reconciliation closed; current active follow-up lives in [`../plans/read-only-lifecycle-ux-plan.md`](../plans/read-only-lifecycle-ux-plan.md).

## Deferred Work

- Do not add lifecycle labels while `dedupAuditGate.status=blocked`.
- Do not add merge/unmerge controls from this track.
- Do not add broad Google Sheets review actions.
- Do not loosen Google Sheets, secondary-key, or sparse-identity behavior without source-specific evidence.
- Do not perform source cleanup, registry mutation, lifecycle retention changes, or source-policy mutation from this plan.

## Historical Validation Standard

Focused role-bucket audit:

```powershell
python src/jobs_fetcher.py
```

Then inspect:

- `dedupEvidence.googleSheetsRoleBucketAudit`
- `dedupEvidence.googleSheetsGenericRoleGuardBlockedCount`
- `dedupEvidence.dedupAuditGate.googleSheetsRoleBucketUnresolvedCount`
- `dedupEvidence.dedupAuditGate.blockers`
- `dedupEvidence.providerStaticDisagreementGateCounts`

If gate behavior or evidence shape changes:

```powershell
python -m pytest -q tests/test_jobs_dedup_google_sheets_guard.py tests/test_jobs_dedup_audit_gate.py tests/test_jobs_dedup_evidence_google_sheets.py
cmd /c npm run lint:precommit
```

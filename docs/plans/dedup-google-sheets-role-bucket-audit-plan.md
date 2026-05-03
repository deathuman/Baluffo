# Dedup Google Sheets Role-Bucket Audit Plan

> - **Status:** Active next-step tracker
> - **Use this when:** reducing current-run dedup blockers caused by Google Sheets role buckets and weak identity-key evidence
> - **Canonical for:** Google Sheets role-bucket blocker evidence, current-run identity-key supporting evidence, and lifecycle UX re-entry criteria
> - **Not canonical for:** persisted payload schemas, bridge route contracts, dedup merge rules, or saved-job storage contracts
> - **Then inspect:** [`../snapshots/dedup-current-run-blocker-triage-closure-2026-05-03.md`](../snapshots/dedup-current-run-blocker-triage-closure-2026-05-03.md), [`../source-policy-runbook.md`](../source-policy-runbook.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-05-03

The current-run blocker triage is closed. The dominant remaining lifecycle blocker is broad Google Sheets role-bucket evidence, with smaller current-run `secondaryKey` and `sparseIdentity` merge blockers still visible for supporting audit.

## Current State

- `dedupAuditGate.status=blocked`.
- `dedupAuditGate.lifecycleUxReady=false`.
- Current-run non-primary merge blockers remain present at `secondaryKey=122` and `sparseIdentity=4`.
- High-risk review causes are dominated by `spreadsheet_role_bucket_needs_review=2390` and `google_sheets_role_bucket_needs_review=2036`.
- Provider/static disagreement remains blocking, but it is smaller than the Google Sheets role-bucket blocker set.

## Review Order

1. Audit `reviewQueueCauseCounts.spreadsheet_role_bucket_needs_review` and `reviewQueueCauseCounts.google_sheets_role_bucket_needs_review` before changing dedup behavior.
2. Use `googleSheetsRoleBucketAudit` to separate guard-fixed different-primary-URL rows, same-primary-URL allowed rows, carried historical bundles, parser/category noise, unresolved current-run buckets, and candidates for a narrow future guard.
3. Use `currentRunMergeExamplesByReason.secondaryKey` and `currentRunMergeExamplesByReason.sparseIdentity` to separate supporting identity-key noise from real same-job merges.
4. Identify whether Google Sheets rows are taxonomy buckets, listing/search buckets, parser-normalized role titles, or weak title/company groupings.
5. Keep `knownMirrorPair` non-blocking only for the existing reviewed Guerrilla mirror pattern.
6. Re-run `python src/jobs_fetcher.py` and require `dedupAuditGate.lifecycleUxReady=true` before starting read-only lifecycle UX.

## Deferred Work

- Do not add lifecycle labels while `dedupAuditGate.status=blocked`.
- Do not add merge/unmerge controls from this track.
- Do not loosen Google Sheets, secondary-key, or sparse-identity behavior without source-specific evidence.
- Do not perform source cleanup, registry mutation, lifecycle retention changes, or source-policy mutation from this plan.
- Treat `googleSheetsGenericRoleGuardBlockedCount` and `googleSheetsRoleBucketAudit` as report-only evidence; they do not create broad review-state or Admin action semantics.

## Validation Standard

Focused role-bucket audit:

```powershell
python src/jobs_fetcher.py
```

Then inspect:

- `dedupEvidence.dedupAuditGate.status`
- `dedupEvidence.dedupAuditGate.currentRunNonPrimaryMergeCounts`
- `dedupEvidence.googleSheetsRoleBucketAudit`
- `dedupEvidence.googleSheetsGenericRoleGuardBlockedCount`
- `dedupEvidence.currentRunMergeExamplesByReason.secondaryKey`
- `dedupEvidence.currentRunMergeExamplesByReason.sparseIdentity`
- `dedupEvidence.reviewQueueCauseCounts`
- `dedupEvidence.providerStaticDisagreementGateCounts`

If gate behavior or evidence shape changes:

```powershell
python -m pytest -q tests/test_jobs_dedup_audit_gate.py tests/test_jobs_dedup_evidence_current_run.py tests/test_jobs_dedup_provider_static_disagreement_gate.py
cmd /c npm run lint:precommit
```

# Dedup Provider/Static Disagreement Reconciliation Plan

> - **Status:** Active next-step tracker
> - **Use this when:** reconciling the remaining provider/static disagreement blockers before read-only lifecycle UX
> - **Canonical for:** provider/static disagreement blocker evidence, local review-state reconciliation order, and lifecycle UX re-entry criteria
> - **Not canonical for:** persisted payload schemas, bridge route contracts, dedup merge rules, source-policy cleanup, or lifecycle labels
> - **Then inspect:** [`../snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md`](../snapshots/dedup-google-sheets-role-bucket-audit-closure-2026-05-03.md), [`../source-policy-runbook.md`](../source-policy-runbook.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-05-03

Google Sheets role-bucket audit is closed. The remaining lifecycle-readiness blocker is provider/static disagreement, currently carried rather than current-run.

## Current State

- `dedupAuditGate.status=blocked`.
- `dedupAuditGate.lifecycleUxReady=false`.
- The only hard blocker is `provider_static_disagreement_needs_review`.
- `providerStaticDisagreementGateCounts.blocked=15`.
- `providerStaticDisagreementGateCounts.currentRunBlocked=0`.
- `providerStaticDisagreementGateCounts.carriedBlocked=15`.
- `providerStaticDisagreementGateCounts.warning=18`.
- `googleSheetsRoleBucketUnresolvedCount=0`.

## Review Order

1. Inspect `providerStaticDisagreementExamples` and `providerStaticTitleCompanyCollisionExamples` before changing behavior.
2. Prioritize carried blocked rows by classification: `title_company_collision`, `static_parser_url_variant`, `provider_redirect_or_canonical_url`, then `same_job_different_urls`.
3. Use existing local review-state actions only for rows with understood URL/source evidence:
   - `reviewed_safe` when the exact row should warn, not block
   - `confirmed_blocking` when the exact row remains a real lifecycle blocker
   - `clear_review` when a local decision needs removal
4. Re-run `python src/jobs_fetcher.py` after reconciliation and require `providerStaticDisagreementGateCounts.blocked=0` before starting read-only lifecycle UX.

## Deferred Work

- Do not add lifecycle labels while `dedupAuditGate.status=blocked`.
- Do not add merge/unmerge controls from this track.
- Do not add broad current-run review classifications.
- Do not mutate dedup output, source registries, source-policy state, cleanup state, or `REDUNDANT_STATIC_IF_PROVIDER`.
- Do not mark provider/static rows safe without row-level URL/source evidence.

## Validation Standard

Focused reconciliation:

```powershell
python src/jobs_fetcher.py
```

Then inspect:

- `dedupEvidence.dedupAuditGate.status`
- `dedupEvidence.dedupAuditGate.lifecycleUxReady`
- `dedupEvidence.providerStaticDisagreementGateCounts`
- `dedupEvidence.providerStaticDisagreementClassificationCounts`
- `dedupEvidence.providerStaticDisagreementExamples`
- `dedupEvidence.providerStaticTitleCompanyCollisionExamples`

If review-state or gate behavior changes:

```powershell
python -m pytest -q tests/test_jobs_dedup_audit_gate.py tests/test_jobs_dedup_provider_static_disagreement_gate.py tests/bridge/test_routes_get_dedup_review_state.py tests/admin/test_admin_bridge_dedup_review_actions.py
cmd /c npm run lint:precommit
```

# Dedup Lifecycle Readiness Closeout

> - **Status:** Closed: transferred through current-run and Google Sheets audit to provider/static reconciliation
> - **Use this when:** checking why the broad dedup lifecycle readiness tracker was retired
> - **Canonical for:** broad readiness closure decision, transferred blocker ownership, lifecycle UX entry criteria, and deferred cleanup boundaries
> - **Not canonical for:** persisted payload schemas, bridge route contracts, source adapter behavior, or saved-job storage contracts
> - **Then inspect:** [`../snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md`](../snapshots/dedup-provider-static-reconciliation-closure-2026-05-03.md), [`dedup-provider-static-disagreement-reconciliation-closeout.md`](dedup-provider-static-disagreement-reconciliation-closeout.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

Dedup auditability remains the product-risk gate before read-only lifecycle UX, but this broad readiness tracker is closed. The 2026-05-03 real-data gate read produced a readable blocked result with specific blockers, and follow-up triage moved active ownership through Google Sheets role-bucket audit to provider/static disagreement reconciliation.

## Closeout State

- `dedupAuditGate` is the primary Admin/Ops readiness semaphore.
- Admin renders the gate as a read-only card with status, lifecycle readiness, blockers, warnings, and capped examples.
- Provider/static disagreement review state can be recorded locally in `data/dedup-review-state.json`.
- Source-policy provider/static review and suppression remain conservative: no bulk apply, no delete, no tombstone, and no permanent static cleanup.
- The first lifecycle UX slice remains deferred because the latest real-data gate reported `status=blocked` and `lifecycleUxReady=false`.

## Closure Outcome

Closed as blocked but transferred.

Fresh evidence from `python src/jobs_fetcher.py` on 2026-05-03 showed:

- `dedupAuditGate.status=blocked`
- `dedupAuditGate.lifecycleUxReady=false`
- blockers: `current_run_non_primary_merges_need_review`, `provider_static_disagreement_needs_review`, `high_risk_review_queue_causes_need_review`
- `providerStaticDisagreementGateCounts.blocked=32`
- `providerStaticDisagreementGateCounts.currentRunBlocked=31`
- `providerStaticDisagreementGateCounts.carriedBlocked=1`

The gate was readable and actionable enough to close this broad tracker. Later provider/static reconciliation made the gate warning-only, and the first read-only lifecycle UX slice is recorded in [`read-only-lifecycle-ux-closeout.md`](read-only-lifecycle-ux-closeout.md).

## Deferred Boundaries

- Do not add lifecycle labels while `dedupAuditGate.status=blocked`.
- Do not add merge/unmerge controls from this track.
- Do not change dedup merge rules based only on Admin diagnostics.
- Do not start permanent static cleanup until repeated source-policy and dedup audits are clean or fully understood.

## Historical Validation Standard

Gate-read or docs-only slices:

```powershell
cmd /c npm run lint:precommit
```

Dedup evidence or Admin card slices:

```powershell
python -m pytest -q tests/test_jobs_dedup_audit_gate.py tests/test_jobs_dedup_audit_gate_provider_static.py tests/test_jobs_dedup_provider_static_disagreement_gate.py
node --test tests/frontend/unit/admin-dedup-evidence-render.test.mjs tests/frontend/unit/admin-dedup-provider-static-render.test.mjs tests/frontend/unit/admin-dedup-support-render.test.mjs
cmd /c npm run lint:precommit
```

Real-data verification:

```powershell
python src/jobs_fetcher.py
```

Then inspect:

- `dedupEvidence.dedupAuditGate.status`
- `dedupEvidence.dedupAuditGate.lifecycleUxReady`
- `dedupEvidence.dedupAuditGate.blockers`
- `dedupEvidence.providerStaticDisagreementClassificationCounts`
- `dedupEvidence.reviewQueueCauseCounts`

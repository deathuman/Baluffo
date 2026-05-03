# Dedup Lifecycle Readiness Plan

> - **Status:** Active next-step tracker
> - **Use this when:** deciding whether dedup evidence is ready for read-only lifecycle UX, or choosing the next dedup auditability slice
> - **Canonical for:** dedup audit gate operating order, current blocker review priorities, lifecycle UX entry criteria, and deferred cleanup boundaries
> - **Not canonical for:** persisted payload schemas, bridge route contracts, source adapter behavior, or saved-job storage contracts
> - **Then inspect:** [`../source-policy-runbook.md`](../source-policy-runbook.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), and [`../scraping-pipeline.md`](../scraping-pipeline.md)
> - **Last updated:** 2026-05-03

Dedup auditability is the current product-risk gate before read-only lifecycle UX. Source-policy/provider readiness is mature enough to validate on real data, but lifecycle labels must still wait for `dedupEvidence.dedupAuditGate.lifecycleUxReady=true` and explainable blocker state.

## Current State

- `dedupAuditGate` is the primary Admin/Ops readiness semaphore.
- Admin renders the gate as a read-only card with status, lifecycle readiness, blockers, warnings, and capped examples.
- Provider/static disagreement review state can be recorded locally in `data/dedup-review-state.json`.
- Source-policy provider/static review and suppression remain conservative: no bulk apply, no delete, no tombstone, and no permanent static cleanup.
- The first lifecycle UX slice is still deferred until real-data dedup evidence passes the gate.

## Operating Order

1. Run a real fetch and read `dedupEvidence.dedupAuditGate`.
2. If `status=blocked`, review blockers before adding lifecycle UX.
3. If blockers are provider/static disagreements, use the existing local review actions only: `reviewed_safe`, `confirmed_blocking`, or `clear_review`.
4. If blockers are current-run high-risk merges or non-primary merges, inspect current-run examples and cause counts before changing behavior.
5. If `lifecycleUxReady=true`, proceed only to read-only lifecycle labels and filters.

## Active Follow-Ups

| Priority | Slice | Goal | Acceptance signal |
| --- | --- | --- | --- |
| 1 | Real-data dedup gate read | Record the latest `dedupAuditGate.status`, `lifecycleUxReady`, blockers, warnings, and examples after a fetch. | `_out/` evidence or local decision log explains whether lifecycle UX is blocked, warning-only, or ready. |
| 2 | Provider/static blocker review | Resolve or confirm remaining provider/static disagreement blockers with local review-state actions. | `providerStaticDisagreementGateCounts.blocked=0` or every remaining blocker is `confirmed_blocking` with review evidence. |
| 3 | Current-run high-risk merge review | Inspect current-run non-primary merges and high-risk review queue causes. | Current-run blockers are absent, reviewed, or have a narrow follow-up tied to a specific cause. |
| 4 | Read-only lifecycle UX entry | Add conservative lifecycle labels only after the gate is ready. | `lifecycleUxReady=true` on real data and the first UX slice stays read-only. |

## Deferred Work

- Do not add lifecycle labels while `dedupAuditGate.status=blocked`.
- Do not add merge/unmerge controls from this track.
- Do not change dedup merge rules based only on Admin diagnostics.
- Do not start permanent static cleanup until repeated source-policy and dedup audits are clean or fully understood.

## Validation Standard

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

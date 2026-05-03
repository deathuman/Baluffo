# Dedup Current-Run Blocker Review Plan

> - **Status:** Active next-step tracker
> - **Use this when:** reviewing the current-run dedup blockers that prevent read-only lifecycle UX
> - **Canonical for:** current-run dedup blocker review order, provider/static disagreement follow-up, and lifecycle UX re-entry criteria
> - **Not canonical for:** persisted payload schemas, bridge route contracts, dedup merge rules, or saved-job storage contracts
> - **Then inspect:** [`../snapshots/dedup-lifecycle-readiness-closure-2026-05-03.md`](../snapshots/dedup-lifecycle-readiness-closure-2026-05-03.md), [`../source-policy-runbook.md`](../source-policy-runbook.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-05-03

The broad dedup lifecycle readiness tracker is closed. The latest real-data fetch still blocks lifecycle UX, but the blockers are now specific enough to own through a narrower review plan.

## Current State

- `dedupAuditGate.status=blocked`.
- `dedupAuditGate.lifecycleUxReady=false`.
- Hard blockers are `current_run_non_primary_merges_need_review`, `provider_static_disagreement_needs_review`, and `high_risk_review_queue_causes_need_review`.
- Provider/static disagreement is mostly current-run evidence: 32 blocked rows, including 31 current-run blockers and 1 carried blocker.
- Local `data/dedup-review-state.json` is absent, but review-state reconciliation alone cannot close this gate because current-run and high-risk blockers are also present.

## Review Order

1. Inspect current-run non-primary merge examples and confirm whether they are real merge risk, known mirror behavior, or parser/source identity noise.
2. Review high-risk queue causes, starting with `spreadsheet_role_bucket_needs_review`, `google_sheets_role_bucket_needs_review`, `listing_page_bundle`, `parser_or_directory_text_pollution`, and `non_provider_url_identity_needs_review`.
3. Review provider/static disagreement examples, separating title/company collisions from static URL variants and provider redirects.
4. Use local provider/static review actions only when a row-level disagreement is understood: `reviewed_safe`, `confirmed_blocking`, or `clear_review`.
5. Re-run `python src/jobs_fetcher.py` and require `dedupAuditGate.lifecycleUxReady=true` before starting read-only lifecycle UX.

## Deferred Work

- Do not add lifecycle labels while `dedupAuditGate.status=blocked`.
- Do not add merge/unmerge controls from this track.
- Do not change dedup merge rules based only on advisory diagnostics.
- Do not perform source cleanup, registry mutation, lifecycle retention changes, or source-policy mutation from this plan.

## Validation Standard

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

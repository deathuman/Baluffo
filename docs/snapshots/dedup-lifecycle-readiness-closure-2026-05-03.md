# Dedup Lifecycle Readiness Closure Evidence - 2026-05-03

> - **Status:** Closure snapshot
> - **Use this when:** checking why the broad dedup lifecycle readiness plan was closed without starting lifecycle UX
> - **Canonical for:** 2026-05-03 real-data dedup gate read, closure decision, and transferred blocker ownership
> - **Not canonical for:** dedup payload schema, merge rules, lifecycle label behavior, or Admin route contracts
> - **Then inspect:** [`dedup-current-run-blocker-triage-closure-2026-05-03.md`](dedup-current-run-blocker-triage-closure-2026-05-03.md), [`../plans/dedup-google-sheets-role-bucket-audit-plan.md`](../plans/dedup-google-sheets-role-bucket-audit-plan.md), and [`../source-policy-runbook.md`](../source-policy-runbook.md)
> - **Last updated:** 2026-05-03

## Real-Data Gate Read

Command:

```powershell
python src/jobs_fetcher.py
```

Result:

- `dedupAuditGate.status`: `blocked`
- `dedupAuditGate.lifecycleUxReady`: `false`
- blockers: `current_run_non_primary_merges_need_review`, `provider_static_disagreement_needs_review`, `high_risk_review_queue_causes_need_review`
- warnings: `carried_high_risk_review_queue_causes_present`, `carried_source_bundle_collisions_present`

Provider/static disagreement gate counts:

- blocked: 32
- warning: 0
- currentRunBlocked: 31
- carriedBlocked: 1
- reviewedSafeWarning: 0
- confirmedBlocking: 0

Provider/static classifications:

- same_job_different_urls: 2
- provider_redirect_or_canonical_url: 6
- static_parser_url_variant: 10
- title_company_collision: 14
- stale_carried_bundle: 0
- needs_manual_review: 0

High-risk review cause counts remain broad enough to block lifecycle UX:

- spreadsheet_role_bucket_needs_review: 2389
- google_sheets_role_bucket_needs_review: 2034
- listing_page_bundle: 247
- parser_or_directory_text_pollution: 429
- non_provider_url_identity_needs_review: 418
- provider_static_disagreement: 32

Top gate examples were provider/static disagreement rows, including People Can Fly Studio, Animoca Brands, Bonfire Studios, and Digital Extremes examples.

## Closure Decision

The broad dedup lifecycle readiness plan is closed as **blocked but transferred**.

Lifecycle UX is not ready. The latest gate is readable and specific, but it is blocked by current-run non-primary merges, current-run/high-risk review causes, and provider/static disagreement. Because blockers are now concrete, the remaining work belongs in a narrower current-run blocker review plan rather than the broad readiness tracker.

`data/dedup-review-state.json` was absent locally during this read. That prevents local provider/static review-state evidence from helping with the one carried blocker, but it is not the only blocker: 31 provider/static blockers are current-run rows and the gate also reports current-run non-primary/high-risk blockers.

## Transferred Ownership

Active follow-up now lives in [`../plans/dedup-google-sheets-role-bucket-audit-plan.md`](../plans/dedup-google-sheets-role-bucket-audit-plan.md) after the current-run blocker triage closure.

The first read-only lifecycle UX slice remains deferred until a future real-data run reports:

- `dedupAuditGate.lifecycleUxReady=true`
- `providerStaticDisagreementGateCounts.blocked=0`
- no current-run high-risk merge blockers
- remaining carried collisions are warning-only, reviewed, or otherwise explained

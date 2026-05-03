# Source Policy Operational Runbook

> - **Status:** Active
> - **Use this when:** running the operator workflow for provider/static source-policy validation
> - **Canonical for:** discovery/fetch/soak/Admin migration-link validation steps and release-readiness checks
> - **Not canonical for:** payload schemas, bridge route contracts, loader internals, or suppression thresholds
> - **Then inspect:** [`scraping-pipeline.md`](scraping-pipeline.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`admin-bridge-api.md`](admin-bridge-api.md)
> - **Last updated:** 2026-05-02

This runbook is the operator checklist for the provider/static source-policy workflow. It explains how to gather runtime evidence, review migration link candidates, apply or clear one explicit link, validate provider coverage, and confirm source-sync cleanliness.

## Guardrails

- Apply at most one reviewed migration identity link at a time.
- Medium-confidence candidates require human judgment before applying.
- Do not auto-apply, bulk apply, force suppress, delete, hide, reject, demote, tombstone, or mutate static sources.
- Do not mutate `REDUNDANT_STATIC_IF_PROVIDER`.
- Do not create permanent redundancy rules.
- Do not change loader selection, suppression thresholds, or source-sync semantics during validation.
- Clear only links owned by `admin_provider_link_backfill`.

## Local Runtime Files

Runtime evidence changes local files. Keep these out of commits unless a change intentionally updates checked-in fixtures:

- `data/source-approval-state.json`
- `data/source-registry-active.json`
- `data/source-registry-pending.json`
- `data/jobs-fetch-report.json`
- `data/jobs-source-state.json`
- `data/dedup-review-state.json`
- `data/source-policy-recommendations.json`
- `data/source-policy-review-state.json`
- `_out/`
- `.playwright-mcp/`

Generated reports under `_out/` and runtime artifacts under `data/` are validation evidence, not code changes.

The tracked registry defaults live under `data/defaults/`:

- `data/defaults/source-registry-active.seed.json`
- `data/defaults/source-registry-pending.seed.json`

Runtime active/pending registry files override those seeds locally. Admin actions, discovery approval, migration link apply/clear, and source-sync reconciliation write the runtime files, not the seeds.

## Baseline Evidence

Run discovery, fetch, then the soak report from the repo root:

```bash
python src/source_discovery.py
python src/jobs_fetcher.py
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

Inspect:

- `_out/source-policy-soak-report.json`
- `_out/source-policy-soak-report.md`
- `data/jobs-fetch-report.json`
- `data/source-discovery-report.json`
- `data/source-discovery-candidates.json`

The soak report should stay `ok` or `warning` unless it finds a contract violation such as source-policy review state or recommendations inside `source-sync.json`.

## Review Migration Link Candidates

Open Admin/Ops and use the Source Policy Review panel. The Migration Link Review section is driven by:

```text
sections.providerCoverageLinkBackfill.reviewCandidates
```

The same backfill section may also include `sections.providerCoverageLinkBackfill.blockedCandidates`
when candidate links exist but are not yet reviewable. A non-zero `candidateLinkCount` with an
empty `reviewCandidates` list means the queue is explainable only through the blocked-candidate
surface. The blocked-candidate surface may also include `blockedReasonCounts` and
`disambiguationBlockerCounts` so operators can see both the top-level blocker and the lower-level
evidence split without changing review behavior.

Reviewable provider/static rows require `providerCoverageStatus="validated_provider"` and at least
two consecutive provider successes. Rows without validated provider coverage stay in the blocked
surface, even when they have some source-state history.

Blocked migration-link rows can also surface source-state evidence fields such as `lastStatus`,
`lastKeptCount`, `lastSuccessfulAt`, `lastFetchedAt`, `providerCoverageStatus`,
`providerCoverageConsecutiveSuccesses`, `providerCoverageLatestKeptCount`,
`providerReplacementReadiness`, `evidenceScore`, and `evidenceReasons`. The lower-level blocker
taxonomy distinguishes `no_source_state_history`, `source_state_not_ok`,
`insufficient_provider_success_history`, `multiple_static_candidates_with_equal_history`, and
`static_only_evidence_present`.

For each candidate, review:

- provider and selected static source identity
- confidence and confidence tier
- `whyNotHighConfidence`
- evidence reasons and source-state evidence
- ignored alternatives
- `apiEligible`
- recommended API payload

High-confidence candidates use `recommendedAction="backfill_migration_identity_candidate"` and require exact evidence. Medium-confidence candidates use `recommendedAction="needs_review"` and require a human decision. Do not apply ambiguous, insufficient-evidence, provider-shaped self-link, or missing-payload rows.

## Apply One Link

Use the Admin UI Apply link action for one reviewed, `apiEligible=true` candidate. The UI posts the emitted `recommendedApiPayload` to:

```text
POST /source-policy/migration-link-action
```

The apply action writes only provider-row metadata:

- `migrationSourceIdentity`
- `migrationSourceName`
- `migrationConfidence`
- `migrationReasons`
- `migrationLinkedAt`
- `migrationLinkedBy="admin_provider_link_backfill"`
- `migrationLinkSource`

The static source row remains unchanged. No tombstone, rejected registry, source-sync local review state, or `REDUNDANT_STATIC_IF_PROVIDER` entry is changed.

## Confirm Provider Coverage

After applying one link, run:

```bash
python src/jobs_fetcher.py
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

One successful linked provider fetch should make the provider usable for coverage:

- `providerCoverageStatus` may become `validated_provider`
- `providerCoverageLatestKeptCount` should reflect the provider result
- `providerCoverageConsecutiveSuccesses` may be `1`

One provider success validates coverage but does not suppress the linked static source. Dynamic redundant-static suppression requires repeated successful provider fetches.

Cache or freshness skips, such as `skip_fresh`, do not increment `providerCoverageConsecutiveSuccesses`. This is intentional: only real provider fetch success counts.

## Validate Two Successful Provider Fetches

To validate the repeated-success path without waiting for the normal freshness window, use the existing force-refresh flag. If `suppressionEligibility` shows `loaderNotGeneratedReason=redundant_static_rule_filtered`, include the linked-static validation flag so the linked static row can be generated long enough for normal dynamic suppression to emit its excluded row:

```bash
python src/jobs_fetcher.py --force-refresh-all --include-linked-static-validation
python src/jobs_fetcher.py --force-refresh-all --include-linked-static-validation
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

This validation path does not lower thresholds and does not change default product behavior. Normal fetches still filter redundant static rows before loader generation; `--include-linked-static-validation` only includes ready linked static rows for validation/evidence collection.

Confirm:

- the linked provider has `providerCoverageConsecutiveSuccesses >= 2`
- default fetch selection may exclude the linked static row with `exclusionReason="dynamic_redundant_provider"`
- `staticSuppressionPolicy` shows eligible or suppressed evidence for the pair
- `providerStaticOverlap` and `redundantStaticProposals` begin showing pair evidence when enough data exists

If the provider is `ready_later` with enough consecutive successes but suppression remains `0`, check `sections.suppressionEligibility`. The row's `selectionReason` explains why the linked static source did not produce a `dynamic_redundant_provider` row. Common values include `linked_static_not_in_default_loader_set`, `linked_static_pending_not_default`, `linked_static_hidden_pending`, `linked_static_rejected`, `linked_static_adapter_not_static`, `linked_static_registry_identity_mismatch`, and `linked_static_missing_from_registry`. The linked-static fields (`linkedStaticRegistryBucket`, `linkedStaticRegistryState`, `linkedStaticFoundInSourceRows`, `linkedStaticFoundInSelectedSources`, `expectedStaticLoaderName`, `generatedStaticLoaderName`, `actualSourceRowName`, `loaderNameMatchStatus`, and `loaderNotGeneratedReason`) are visibility diagnostics only; they do not force-select sources. `redundant_static_rule_filtered` means the active static row is loader-compatible but was removed before default loader generation by existing redundant-static rules; rerun validation with `--include-linked-static-validation` when you need to observe the suppression row.

Explicit `--only-sources` selection bypasses dynamic redundant-static suppression. Use that when you deliberately need to run a static source even if the default fetch would skip it.

## Clear A Link

Use the Admin UI Clear link action from Linked Migration Identities. Clear is available only when current registry state confirms:

- `migrationSourceIdentity` is present
- `migrationLinkedBy="admin_provider_link_backfill"`

Clear posts:

```json
{
  "action": "clear_migration_identity_link",
  "providerSourceId": "<providerSourceId>",
  "staticSourceId": "<staticSourceId>"
}
```

It removes only Admin-owned migration metadata from the provider row. It does not modify the static row, rejected registry, tombstones, source-policy review artifacts, source-sync local-only artifacts, or `REDUNDANT_STATIC_IF_PROVIDER`.

## Verify Source-Sync Cleanliness

After apply, fetch, soak, and clear, inspect the soak report source-sync section and quality gates. Source sync must remain registry-only.

Allowed top-level source-sync fields are:

- `schemaVersion`
- `generatedAt`
- `source`
- `active`
- `pending`

Source-policy artifacts must not appear in source-sync:

- `sourcePolicy`
- `sourcePolicyReviewState`
- `sourcePolicyRecommendations`
- `reviewState`
- `manualSuppressionOverride`
- `force_pause`
- `recommendations`
- `redundantStaticProposals`

`migrationSourceIdentity` is normal active/pending provider registry metadata after an explicit Admin action. Source-policy review state, recommendation artifacts, and `force_pause` remain local-only and move only through explicit local backup/import.

## Troubleshooting

| State | Likely meaning | Next action |
|-------|----------------|-------------|
| No review candidates | Discovery/fetch/soak did not find provider/static links with enough evidence for manual review. | Run discovery, fetch, and soak again. Inspect `providerMigrationActivation`, `providerCoverageLinkBackfill.blockerCounts`, `providerCoverageLinkBackfill.blockedCandidates`, `providerCoverageLinkBackfill.disambiguationBlockerCounts`, `providerCoverageLinkBackfill.disambiguationBlockedExamples`, and missing/malformed artifact warnings. |
| Only ambiguous candidates | Multiple static rows match the same provider and no deterministic evidence selects one. | Review `ambiguityGroups`, `candidateStatics`, and ignored alternatives. Do not apply a link until one candidate becomes `apiEligible=true` with clear evidence. |
| Medium-confidence candidate only | Source-state evidence selected one static source, but exact advisory identity is missing. | Review `whyNotHighConfidence`, source-state evidence, and ignored alternatives. Apply only if the evidence is acceptable to a human reviewer. |
| Provider linked but success streak stuck at `1` | The next fetch likely skipped the provider as fresh, so no second real success was recorded. | Use `python src/jobs_fetcher.py --force-refresh-all` for validation, or wait until the provider is no longer fresh. Do not count `skip_fresh` as success. |
| Provider ready but suppression count is `0` | The provider has enough successful fetches, but the linked static source was not selected, is pending/hidden/rejected, has an identity mismatch, is not static-like, is missing from registry rows, or was filtered before default loader generation. | Check `sections.suppressionEligibility.missingLinkedStaticRows[].selectionReason`, `linkedStaticRegistryBucket`, `expectedStaticLoaderName`, `generatedStaticLoaderName`, `loaderNameMatchStatus`, and `loaderNotGeneratedReason`. `redundant_static_rule_filtered` means the active static row is loader-compatible but removed before loader generation by existing redundant-static rules. These diagnostics are visibility-only; do not force-select or mutate registry rows just to create a suppression row. |
| Clear button missing | The link is not owned by `admin_provider_link_backfill`, the static identity does not match the candidate, or Admin has stale data. | Reload Admin/Ops and check Linked Migration Identities. Non-admin-owned links are visible but not clearable. |
| Source-sync warning | `source-sync.json` may contain local source-policy payloads or unexpected top-level keys. | Treat as a release blocker. Inspect the soak report quality gates and keep source-policy review/recommendation artifacts out of sync. |
| Static-only evidence detected | The static source has evidence not covered by the provider, or overlap history is insufficient. | Do not suppress or clean up the static source. Review `providerStaticOverlap`, `staticSuppressionPolicy`, and source-policy recommendations before any further action. |

## Operational Soak & Decision Log

During a real-data soak period, keep a local decision log under `_out/` or in private notes. The log is operational evidence, not a repo artifact, and should not be committed by default.

Copy this template for each run:

```markdown
## Source-policy operational run

Date:
Discovery completed:
Fetch completed:
Soak status:
Failed gates:
Warning gates:

Review candidates:
- Provider:
- Static:
- Confidence:
- Reason:
- Decision: apply / skip / wait / investigate
- Why:

Applied links:
- Provider:
- Static:
- Result:
- Clear tested:

Provider coverage:
- Validated providers:
- Linked provider kept count:
- Consecutive successes:

Suppression:
- dynamic_redundant_provider rows:
- providerStaticOverlap pairs:
- redundantStaticProposals:

Source-sync:
- Clean: yes/no

Runtime files changed:
- list

Follow-up:
```

Use this rubric when recording decisions:

- Apply at most one manually reviewed `apiEligible=true` migration link at a time.
- Skip provider-shaped or provider-to-provider self-link candidates.
- Wait when candidates are only ambiguous, insufficient-evidence, or missing a payload.
- Investigate warning gates or source failure buckets that repeat across runs.
- Never treat source-policy warnings as permission to delete, hide, reject, demote, tombstone, or clean up sources.

Only consider conservative static-cleanup proposals after at least 3 clean or fully understood real-data soak runs, no failed source-sync gates, at least one stable provider/static pair with repeated safe evidence, no static-only evidence for that pair, dynamic suppression has been observed or its absence is explained, apply plus clear/reversal have both been tested, and the registry seed/runtime split is respected.

## Ordered Roadmap Gates

Use this order after source-policy validation is complete:

1. Operational evidence gate
2. Conservative static cleanup policy
3. Dedup auditability closure
4. Read-only lifecycle UX in Jobs/Saved

The operational evidence gate must pass before source cleanup becomes actionable. Record at least 3 clean or fully understood source-policy soak runs and at least 1-2 post-dedup-guard audits. The pass conditions are:

- no failed source-sync gates
- runtime registry writes go to ignored runtime files, not `data/defaults/*.seed.json`
- current-run dedup merges are absent or explainable
- high-risk dedup review queue causes are documented
- provider/static cleanup candidates have repeated safe evidence

The first conservative cleanup action, if separately implemented, must be reversible:

```text
active static
-> hidden/redundant pending
```

Cleanup proposals are advisory until an explicit Admin action exists. The action must require operator confirmation and must explain provider/static pair evidence, repeated provider success, suppression or suppression-eligibility evidence, overlap/audit evidence, absence of static-only evidence, and source-sync cleanliness. It must not delete, tombstone, reject, mutate tracked seeds, or mutate `REDUNDANT_STATIC_IF_PROVIDER`.

Dedup auditability is the next product-risk gate before lifecycle UX. Treat the current `dedupEvidence` diagnostics as the primary review surface: current-run `mergedCount`, current-vs-carried `sourceBundleCollisionCount`, review queue counts, suspected causes, identity quality, non-provider provenance, Google Sheets role-bucket diagnostics, provider/static disagreement examples/classifications, `providerStaticDisagreementGateCounts`, dedicated provider/static title-company collision examples, and `dedupAuditGate`. Do not add lifecycle labels until `dedupAuditGate.lifecycleUxReady=true` in real-data evidence, current-run merges are explainable, provider/static disagreement is low or reviewed with URL/source evidence, Google Sheets generic role/category merges are blocked for new runs, and carried source-bundle collisions are clearly historical, reconciled, or low-risk.

Admin/Ops may record local dedup review state in `data/dedup-review-state.json` for provider/static disagreement rows surfaced through `dedupEvidence`. Use only the explicit local actions:

- `reviewed_safe` when the carried disagreement is understood and should warn, not block
- `confirmed_blocking` when the disagreement remains a real lifecycle blocker
- `clear_review` to restore default gate behavior

These actions are local audit evidence only. They do not rewrite `jobs-unified.json`, do not change dedup merge rules, do not create lifecycle labels, and do not permit source cleanup, registry edits, or source-policy mutation. Lifecycle UX remains paused until unresolved provider/static disagreement count reaches zero.
Ops health and Admin reporting also expose a read-only dedup review-state status block with the artifact path, any missing/malformed read warning, the reviewed pair count, the `reviewed_safe` count, the `confirmed_blocking` count, and the remaining unresolved blocking count so operators can tell when the blocker is really unresolved versus just unreadable local state.

When `providerStaticDisagreementClassificationCounts.title_company_collision` is nonzero, review `providerStaticTitleCompanyCollisionExamples` before treating the gate as ready. Those rows are capped separately from general provider/static disagreements and include provider/static URLs, source IDs, shared identifier tokens, locations, and `collisionReviewHint`. They are advisory evidence only; they do not permit merge/unmerge actions, source cleanup, registry edits, or lifecycle labels by themselves.

If carried title/company collision rows expose `carriedLocationPollutionAudit=carried_location_pollution`, treat them as carried metadata warnings, not proof of a real provider/static multi-location conflict. Those rows usually reflect polluted carried `locations` data such as a role token being stored as a city. The audit remains read-only: it does not rewrite `jobs-unified.json`, does not change dedup merge rules, and does not by itself clear the lifecycle gate if unresolved provider/static conflicts remain.

The first lifecycle UX slice must be read-only. It may show conservative labels such as `New`, `Reappeared`, `Recently removed`, and `Preserved because source failed`, but only when lifecycle/source-health evidence and dedup confidence support the label. Keep `New` as the existing user-seen Jobs badge, not a pipeline lifecycle event. Do not expose `Preserved because source skipped` in the first user-facing slice; keep that operational-only until there is a clearer product reason to surface it. Saved should read lifecycle labels through a live overlay from current jobs/lifecycle artifacts, not by mutating persisted saved-job rows. Do not change retention policy, add merge/unmerge controls, perform source cleanup, or mutate registries in that first slice.

## Conservative Static Cleanup Proposals

The soak report may emit `sections.conservativeStaticCleanupProposals` after repeated safe evidence. This is still reporting only. A proposal is not an action and must not be treated as permission to hide, reject, tombstone, delete, or permanently suppress a source.

Proposal rows use:

- `recommendedAction="move_static_to_hidden_pending"`
- `destructiveActionAllowed=false`
- `requiresExplicitAdminAction=true`

Proposal freshness is part of readiness. Read the additive `proposalGeneratedAt`,
`proposalReportRunId`, `proposalFreshnessStatus`, `proposalFreshnessAgeSeconds`, and
`proposalReadinessHash` fields before deciding whether a row is still actionable. Rows may also
carry `proposalReadiness`, `proposalReadinessReason`, and `proposalReadinessEvidence` so stale rows
stay visible but are not mistaken for fresh action candidates.

`proposalCount=0` is not failure by itself. Inspect `staleCount`, `blockedCount`,
`blockedReasonCounts`, `blockedCandidates`, and the capped `proposalReadyExamples` /
`blockedExamples` samples to see why proposal-ready rows are still absent or no longer actionable.
This section stays report-only until a later milestone adds a separate explicit reversible Admin
action.

The intended first future action, if separately implemented and approved, is reversible:

```text
active static
-> hidden/redundant pending
```

Do not mutate `data/defaults/source-registry-*.seed.json`. Any future cleanup action must write only ignored runtime registry files.

## Source-Policy Release Readiness

Before treating a provider/static source-policy change as release-ready, run the focused regression pack from the repo root:

```bash
python -m pytest -q tests/test_jobs_provider_coverage.py tests/test_jobs_dynamic_static_suppression.py tests/test_jobs_static_suppression_policy.py
python -m pytest -q tests/test_source_policy_soak_report.py tests/test_source_policy_soak_report_backfill.py tests/test_source_policy_soak_report_suppression_selection.py
python -m pytest -q tests/admin tests/source_discovery
cmd /c npm run test:frontend:unit
python -m ruff check --select C901 src/jobs --output-format concise
python -m ruff check --select C901 src/jobs/adapters --output-format concise
cmd /c npm run lint:precommit
```

For optional real-data validation, regenerate runtime evidence and the soak report:

```bash
python src/source_discovery.py
python src/jobs_fetcher.py
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

For validation-only dynamic suppression evidence after applying one reviewed link, use:

```bash
python src/jobs_fetcher.py --force-refresh-all --include-linked-static-validation
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

Release-ready source-policy evidence means:

1. Discovery and fetch complete and write current runtime artifacts.
2. The soak report has no failed gates; expected warning gates are acceptable when understood.
3. Source-sync cleanliness passes, and `source-sync.json` is absent or contains only `schemaVersion`, `generatedAt`, `source`, `active`, and `pending`.
4. Source-policy review state, recommendation artifacts, overrides, and proposal payloads are not present in source-sync.
5. Admin review candidates are inspected manually, and at most one chosen candidate is applied through Admin.
6. One successful linked provider fetch validates coverage but does not imply static suppression.
7. Two real provider successes are observed before expecting `dynamic_redundant_provider`; cache/freshness skips do not count.
8. `--include-linked-static-validation` is used only for validation/evidence collection and does not change normal default fetch behavior.
9. Static rows remain unchanged unless explicitly edited as part of a separate approved source-maintenance task.
10. Clear works for Admin-owned links when reversal is needed.
11. Runtime artifacts stay local and uncommitted.

# Source Policy Operational Runbook

> - **Status:** Active
> - **Use this when:** running the operator workflow for provider/static source-policy validation
> - **Canonical for:** discovery/fetch/soak/Admin migration-link validation steps and release-readiness checks
> - **Not canonical for:** payload schemas, bridge route contracts, loader internals, or suppression thresholds
> - **Then inspect:** [`scraping-pipeline.md`](scraping-pipeline.md), [`DATA_CONTRACT.md`](DATA_CONTRACT.md), [`admin-bridge-api.md`](admin-bridge-api.md)
> - **Last updated:** 2026-05-01

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
- `.playwright-mcp/`

Generated reports under `_out/` and runtime artifacts under `data/` are validation evidence, not code changes.

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
| No review candidates | Discovery/fetch/soak did not find provider/static links with enough evidence for manual review. | Run discovery, fetch, and soak again. Inspect `providerMigrationActivation`, `providerCoverageLinkBackfill.blockerCounts`, and missing/malformed artifact warnings. |
| Only ambiguous candidates | Multiple static rows match the same provider and no deterministic evidence selects one. | Review `ambiguityGroups`, `candidateStatics`, and ignored alternatives. Do not apply a link until one candidate becomes `apiEligible=true` with clear evidence. |
| Medium-confidence candidate only | Source-state evidence selected one static source, but exact advisory identity is missing. | Review `whyNotHighConfidence`, source-state evidence, and ignored alternatives. Apply only if the evidence is acceptable to a human reviewer. |
| Provider linked but success streak stuck at `1` | The next fetch likely skipped the provider as fresh, so no second real success was recorded. | Use `python src/jobs_fetcher.py --force-refresh-all` for validation, or wait until the provider is no longer fresh. Do not count `skip_fresh` as success. |
| Provider ready but suppression count is `0` | The provider has enough successful fetches, but the linked static source was not selected, is pending/hidden/rejected, has an identity mismatch, is not static-like, is missing from registry rows, or was filtered before default loader generation. | Check `sections.suppressionEligibility.missingLinkedStaticRows[].selectionReason`, `linkedStaticRegistryBucket`, `expectedStaticLoaderName`, `generatedStaticLoaderName`, `loaderNameMatchStatus`, and `loaderNotGeneratedReason`. `redundant_static_rule_filtered` means the active static row is loader-compatible but removed before loader generation by existing redundant-static rules. These diagnostics are visibility-only; do not force-select or mutate registry rows just to create a suppression row. |
| Clear button missing | The link is not owned by `admin_provider_link_backfill`, the static identity does not match the candidate, or Admin has stale data. | Reload Admin/Ops and check Linked Migration Identities. Non-admin-owned links are visible but not clearable. |
| Source-sync warning | `source-sync.json` may contain local source-policy payloads or unexpected top-level keys. | Treat as a release blocker. Inspect the soak report quality gates and keep source-policy review/recommendation artifacts out of sync. |
| Static-only evidence detected | The static source has evidence not covered by the provider, or overlap history is insufficient. | Do not suppress or clean up the static source. Review `providerStaticOverlap`, `staticSuppressionPolicy`, and source-policy recommendations before any further action. |

## Release Readiness Checklist

Before treating a provider/static source-policy run as release-ready:

1. Discovery completes and writes current discovery artifacts.
2. Fetch completes and `taskProgress` is completed.
3. Soak report status is `ok`, or only expected warning gates remain.
4. Source-sync cleanliness gates pass.
5. Admin review candidates are inspected manually.
6. At most one chosen candidate is applied through Admin.
7. Provider coverage validates the linked provider.
8. Two real provider successes are observed before expecting `dynamic_redundant_provider`.
9. Static row remains unchanged.
10. Clear works for the Admin-owned link if reversal is needed.
11. Runtime data changes remain local and uncommitted.

# Provider Discovery Coverage Gap Plan

> - **Status:** Archived — closed as evidence-saturated, advisory-only; next-step coverage strategy lives in [`plans/jobs-coverage-improvement-plan.md`](../plans/jobs-coverage-improvement-plan.md)
> - **Use this when:** improving ATS/provider discovery coverage, provider migration staging evidence, or Admin/Ops visibility without adding Apify or another crawler runtime
> - **Canonical for:** next-step provider discovery coverage strategy and provider coverage gap report requirements
> - **Not canonical for:** provider adapter runtime behavior, report payload contracts, source registry policy, or source cleanup authority
> - **Then inspect:** [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../source-policy-runbook.md`](../source-policy-runbook.md), [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-08-28 (archived from `docs/plans/` after closure)

## Summary

Baluffo already has the crawler and provider-fetch foundations that an Apify runtime would duplicate: provider API loaders, provider plugin dispatch, JSON-feed providers, HTML-board providers, Scrapy static fallback, discovery audits, provider inference, provider migration advisory, provider coverage validation, dynamic static suppression, and source-policy soak reporting.

The missing layer is not a new generic crawler. The useful work is:

```text
coverage audit
-> unsupported ATS classification
-> stronger provider inference
-> staged provider validation visibility
-> Admin-operable provider/static cleanup evidence
```

Keep this work local-first, advisory, and non-destructive. Do not add Apify as a runtime dependency.

## Current Repo Check

Current docs and code show these important boundaries:

- `docs/adapter-plugin-inventory.md` lists fetcher loaders and provider plugins for Greenhouse, Teamtailor, Lever, SmartRecruiters, Workable, Recruitee, Pinpoint, Ashby, BambooHR, Breezy, JazzHR, Oracle HCM, Personio, Workday, and `scrapy_static_sources`.
- `src/source_discovery/config.py` currently exposes discovery `SUPPORTED_PROVIDERS` for Greenhouse, Lever, SmartRecruiters, Workable, Teamtailor, Ashby, BambooHR, Breezy, JazzHR, Oracle HCM, Workday, Recruitee, Pinpoint, and Personio.
- `src/source_discovery/provider_inference.py` builds provider candidate rows for those supported discovery providers, including safe Workday `*.myworkdayjobs.com` listing URLs with non-root paths, safe Breezy/JazzHR board URLs, and Oracle HCM Candidate Experience jobs pages on `oraclecloud.com`.
- `src/source_discovery/provider_migration_advisory.py` recognizes safe supported candidates as migration/provider-staging evidence, keeps unsafe Oracle HCM evidence unsupported, and classifies Jobvite, iCIMS, SuccessFactors, Cornerstone/CSOD, Homerun, and HRMOS as unsupported-provider evidence.
- Current checked-in `data/source-discovery-candidates.json` may still contain provider-shaped URLs represented as static rows until discovery is rerun. The first slice added Workday/Oracle HCM classification and reporting logic without rewriting runtime data artifacts.
- Static/plugin coverage already includes partial special cases such as HRMOS and Jobvite-like static handling, but that is not the same as discovery/provider migration coverage.
- The source-policy soak report now includes additive read-only `sections.providerCoverageGaps` in JSON and Markdown. `docs/DATA_CONTRACT.md` is canonical for that report shape, and `docs/scraping-pipeline.md` is the operator-facing summary.

Conclusion: the plan should extend detection and reporting around the existing provider/static workflow, not introduce another scraping stack.

## Goals

1. Make provider discovery coverage measurable from current artifacts.
2. Classify unsupported or weakly handled ATS families with enough evidence for future adapter decisions.
3. Promote safe supported-provider candidates into the existing pending/staged review flow.
4. Expose provider coverage gaps directly in the source-policy soak report and Admin/Ops.
5. Preserve source registry, source-sync, static suppression, and migration-link semantics.

## Non-Goals

- Do not add Apify or any external scrape service to normal runtime fetch.
- Do not add new Python or Node dependencies.
- Do not add a new generic crawler, broad static scraper, or generic browser runtime path.
- Do not auto-promote, hide, reject, delete, tombstone, demote, or permanently suppress sources.
- Do not mutate `REDUNDANT_STATIC_IF_PROVIDER` as part of reporting.
- Do not add unsupported ATS fetch adapters inside this plan unless a later, narrower implementation plan approves one provider family.

## Implementation Plan

1. **Artifact coverage audit**
   - Read current discovery artifacts such as `data/source-discovery-candidates.json`, `data/source-discovery-report.json`, directory/web-search audit artifacts when present, active/pending registry rows, and the latest fetch/source-state evidence.
   - Produce a read-only coverage audit that groups discovered URLs by recognized provider, unsupported ATS family, static/generic adapter, active/pending registry state, and latest provider fetch state.
   - Record examples and counts, not source mutations.

2. **ATS family detection taxonomy**
   - Extend provider evidence detection so unsupported or weakly handled families are classified deliberately: Oracle HCM, iCIMS, SuccessFactors, Jobvite, Cornerstone/CSOD, Homerun, HRMOS, and Workday variants.
   - Keep a clear split between `supported_provider_candidate`, `unsupported_provider_detected`, and `provider_detected_needs_probe`.
   - Add focused tests for host/path recognition and blocker classification.

3. **Supported-provider inference improvements**
   - Add row builders only where the existing fetcher already supports the provider and the URL can be normalized safely.
   - Prioritize Workday variants first because the provider loader exists and current artifacts show `*.myworkdayjobs.com` static candidates.
   - Re-check Greenhouse `job-boards.greenhouse.io` / `boards.greenhouse.io` normalization and Ashby board/API URL choices so current successes remain stable.
   - Preserve existing candidate row shapes and evidence vocabulary.

4. **Provider coverage gap report**
   - Add a read-only `providerCoverageGaps` section to the source-policy soak report, or expose equivalent data through Admin/Ops if that is the smaller surface.
   - Required buckets:
     - `unsupportedProviderDetected`
     - `providerDetectedNeedsProbe`
     - `stagedProviderNotFetched`
     - `fetchedButNotValidated`
     - `validatedProviderMissingMigrationSourceIdentity`
     - `staticStillActiveDespiteValidatedProvider`
   - Include counts, blocker reasons, source/provider identities, current adapter, registry bucket/state, latest fetch status, kept count, provider coverage status, consecutive success count, and capped examples.

5. **Staged-provider validation visibility**
   - Connect staged/pending provider candidates to latest fetch evidence without changing approval rules.
   - Make it obvious when a candidate has never fetched, fetched with zero kept rows, fetched once but lacks repeated validation, or is validated but not linked to a static source.
   - Keep cache/freshness skips from counting as validation successes.

6. **Admin/Ops review path**
   - Surface the gap buckets as review evidence beside existing provider migration activation and provider coverage link backfill diagnostics.
   - Reuse existing explicit Admin actions when possible, especially reviewed migration identity apply/clear.
   - Do not add bulk actions, force suppress, auto apply, cleanup, or destructive source-state changes.

7. **Optional external benchmark**
   - Treat external scrape services as occasional benchmark inputs only, never runtime fetch dependencies.
   - If a benchmark artifact is imported, compare it against Baluffo discovery to answer which ATS families and studios Baluffo missed.
   - Use benchmark deltas to choose future provider-family work, not to fetch jobs in normal runs.

## Report Semantics

The gap report is advisory evidence only:

- It may recommend investigation, provider inference work, probe retry, provider validation, or manual migration-link review.
- It must not apply registry changes or source cleanup.
- It must not change loader selection, suppression thresholds, source-sync output, or runtime fetch behavior.
- It must distinguish active, pending, hidden pending, rejected, duplicate, and generated/runtime-only rows.
- It must explain why a row is blocked from the next step, not just that it is blocked.

## Suggested Verification

Use focused tests first:

```bash
python -m pytest -q tests/source_discovery/test_provider_inference.py tests/source_discovery/test_provider_migration_advisory.py
python -m pytest -q tests/test_source_policy_soak_report.py tests/test_source_policy_soak_report_provider_activation.py
python -m pytest -q tests/test_jobs_provider_coverage.py tests/test_jobs_dynamic_static_suppression.py tests/test_jobs_static_suppression_policy.py
```

For Admin/Ops UI changes, also run the relevant frontend unit checks:

```bash
cmd /c npm run test:frontend:unit
cmd /c npm run lint:precommit
```

For optional real-data validation:

```bash
python src/source_discovery.py
python src/jobs_fetcher.py
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

Runtime artifacts under `data/` and `_out/` are evidence, not default commit targets.

## Acceptance Criteria

- Current discovery artifacts can be summarized by provider coverage gap bucket.
- Unsupported ATS families are named explicitly instead of disappearing into generic static/no-evidence rows.
- Supported provider detections can stage provider candidates through the existing pending/review path.
- Workday provider-shaped URLs are no longer treated as generic static when a safe provider row can be built.
- Staged, fetched, validated, linked, and static-suppression-ready states are visible without digging through multiple artifacts.
- No Apify runtime, new dependency, generic scraper, source cleanup, source-sync mutation, or static suppression semantic change is introduced.

## First Slice (Implemented 2026-05-19)

The smallest implementation that proves the layer is complete:

1. Added read-only detection/classification for Workday and Oracle HCM examples already present in current discovery artifacts.
2. Added tests showing Workday is a supported-provider inference candidate only when the row shape is safe, while Oracle HCM was initially classified as unsupported/provider gap evidence.
3. Added source-policy soak-report `providerCoverageGaps` buckets with counts, blocker reasons, provider/static evidence, and capped examples in JSON and Markdown.
4. Deferred Admin UI changes until the JSON/Markdown report is stable.

Verification completed on 2026-05-19:

```bash
python -m pytest -q tests/source_discovery/test_provider_inference.py tests/source_discovery/test_provider_migration_advisory.py tests/source_discovery/test_web_search_candidates.py
python -m pytest -q tests/test_source_policy_soak_report.py tests/test_source_policy_soak_report_provider_activation.py
python -m pytest -q tests/source_discovery tests/test_source_policy_soak_report.py tests/test_source_policy_soak_report_provider_activation.py tests/test_source_policy_soak_report_backfill.py tests/test_source_policy_soak_report_backfill_enrichment.py tests/test_source_policy_soak_report_cleanup_proposals.py tests/test_source_policy_soak_report_linked_static_identity.py tests/test_source_policy_soak_report_suppression_selection.py
git diff --check
```

Results: 50 focused source-discovery tests passed, 29 focused soak-report tests passed, the broader 502-test source-discovery/soak-report sweep passed, and `git diff --check` passed.

Remaining follow-ups are staged-provider validation outcomes that produce real positive kept-job evidence, a narrow unsupported-family adapter decision if one family becomes worth implementing, and the Admin/Ops review surface. Those should be planned as separate slices.

## Oracle HCM Decision Slice (Implemented 2026-05-19)

Oracle HCM now has a narrow supported-provider path only for Candidate Experience jobs pages on `oraclecloud.com`.

Implemented scope:

1. Added discovery/provider-migration rows with `adapter="oracle_hcm"`, `listing_url`, `base_url`, and normalized `site_path` for safe `/hcmUI/CandidateExperience/.../sites/.../jobs` URLs.
2. Added an `oracle_hcm_sources` provider plugin and fixture-backed Oracle CE requisition parser using the documented `recruitingCEJobRequisitions` endpoint path.
3. Preserved iCIMS, SuccessFactors, Cornerstone/CSOD, Homerun, HRMOS, and Jobvite as unsupported advisory-only families.
4. Kept unsafe Oracle CE URLs unsupported and made auth-gated or empty Oracle fetch evidence non-promotable rather than another provider fetch/debug loop.

Runtime validation should still treat Oracle CE 401/403 or empty tenant responses as a useful decision outcome: the family is structurally supported, but those tenant examples are not promotable unless public requisition payloads return real kept jobs.

## Validated-Count Reassessment Slice (Implemented 2026-05-22)

Current provider coverage work should stay focused on candidates that can raise validated-provider count. Broad discovery, unsupported-family adapters, and migration-link cleanup are lower priority until fresh evidence shows new solvable coverage.

Fresh targeted refresh completed on 2026-05-22:

```powershell
python src/jobs_fetcher.py --only-sources workday_sources,bamboohr_sources,breezy_sources,oracle_hcm_sources --force-refresh-all --include-pending-provider-migration --quiet
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

Resulting state:

- Source-policy soak status remained `warning`; `providerCoverageNextAction.action` remained `none`.
- Provider coverage stayed at `18/26` validated, with `6` needs-review and `2` failed.
- Gap counts stayed at `fetchedButNotValidated=9`, `validatedProviderMissingMigrationSourceIdentity=9`, and all unsupported/probe/not-fetched/static-still-active buckets at `0`.
- Validation diagnostics stayed at `zeroKeptFetched=7`, `fetchError=2`, `validated=17`, `notFetched=0`, `missingDetailEvidence=0`.

Direct probes of the remaining blockers did not show public postings missed by adapters:

- TiMi Workday CXS endpoint returned HTTP 200 with `total=0` and `jobPostings=[]`.
- Wolcen, Reforged, Expression, Eleventh Hour, and Beamdog BambooHR `/careers/list` endpoints returned HTTP 200 with `meta.totalCount=0` and empty `result`.
- Lemon Sky BambooHR `/careers/list` returned HTTP 401, matching the adapter failure class.
- IllFonic Breezy board returned HTML but no public `/p/` posting links.
- Glass Egg Oracle HCM requisition endpoint returned HTTP 200 with `count=0`, `hasMore=false`, and empty `items`.

Conclusion: no validated-count code change is justified from the current remaining 8 blockers. Keep those candidates pending/failed with non-promotable evidence until fresh public postings appear. The next meaningful provider-coverage improvement is either reviewed migration-link cleanup for already validated providers or a fresh discovery run to produce a new candidate set.

## Fresh Candidate Wave Slice (Implemented 2026-05-22)

Fresh discovery was run against normal runtime artifacts to look for a new supported-provider candidate wave:

```powershell
python src/source_discovery.py --preset uncapped --top 0 --timeout 12 --gameprog-enabled --gamedevmap-enabled
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
```

Baseline pending provider-migration rows before discovery were captured in `_out/provider-fresh-wave-baseline.json`: `26` rows split as `13` BambooHR, `8` Workday, `4` Breezy, and `1` Oracle HCM.

Fresh discovery results:

- Discovery generated `996` candidates, `452` survived dedupe, `252` validated, and `133` were auto-approved through the existing discovery policy.
- Discovery final registry counts were `active=2063`, `pending=194`, `rejected=0`.
- Provider migration review found `252` candidates but `0` stageable/staged provider-migration candidates.
- Pending provider-migration rows remained unchanged: `26` current rows, `0` new IDs, `0` removed IDs.
- Provider-migration staging blockers were dominated by existing coverage rather than new solvable validation work: `duplicate_active=161`, `duplicate_pending=19`, `adapter_mismatch=74`, `existing_provider=4`, `insufficient_evidence=83`, and `non_stageable_action=91`.

Final soak after discovery remained `warning` with provider validation unchanged:

- `validated_provider=18`, `needs_review=6`, `failed_provider=2`, total provider candidates `26`.
- Gap buckets remained `fetchedButNotValidated=9`, `validatedProviderMissingMigrationSourceIdentity=9`, and all unsupported/probe/not-fetched/static-still-active buckets at `0`.
- No targeted provider validation fetch was run for new candidates because discovery staged no new pending provider-migration rows.
- `providerCoverageNextAction.action` changed to `review_one_migration_link`, with `4` API-eligible medium-confidence review candidates from existing link-backfill evidence.

Conclusion: this fresh candidate wave improved general discovery/source coverage through normal auto-approval, but it did not create new provider-migration validation work or raise the `18/26` provider validated count. The next meaningful provider-coverage task is reviewed migration-link cleanup for the existing API-eligible candidates, not more parser work on the saturated pending set.

## Closeout Reassessment (Closed 2026-05-22)

This plan is closed as evidence-saturated. Current evidence shows no meaningful provider discovery coverage work remains in this plan without fresh external state, new provider candidates, or a separate source-policy cleanup scope.

Final closeout state:

- Source-policy soak status remains `warning`, but provider discovery coverage is no longer the active blocker.
- Provider validation counts are `validated_provider=20`, `needs_review=6`, and `failed_provider=2`.
- Provider coverage gap counts are `fetchedButNotValidated=9`, `validatedProviderMissingMigrationSourceIdentity=9`, and `staticStillActiveDespiteValidatedProvider=2`.
- Unsupported/probe/not-fetched buckets are all `0`.
- Link-backfill has `0` API-eligible review actions.
- `providerCoverageNextAction.action` is `resolve_link_ambiguity`, not provider parser/debug work or human migration-link approval.

Remaining blocker classification:

- `fetchedButNotValidated` rows are non-promotable until fresh public postings appear; recent targeted probes showed zero public postings or auth/empty-board outcomes rather than parser misses.
- `validatedProviderMissingMigrationSourceIdentity` rows have no safe bulk link path; future work should only apply reviewed registry-backed static matches.
- `staticStillActiveDespiteValidatedProvider` is source-policy/static suppression cleanup, not provider discovery coverage.
- `provider_shaped_self_link` and `ambiguous_static_match` are link-backfill diagnostics, not new provider coverage work.

Future reopen triggers:

- Fresh discovery stages new supported-provider migration candidates.
- A repeated unsupported-provider family appears with enough evidence to justify a narrow adapter plan.
- A registry-backed API-eligible migration-link candidate appears.
- Fresh public postings appear for a currently non-promotable pending provider row.

Out of scope for this closeout:

- No API, schema, source registry, suppression, or fetcher contract changes.
- No static row edits, source cleanup, bulk migration-link actions, new adapter, new crawler, new dependency, or source-sync mutation.
- Runtime artifacts under `data/` and `_out/` remain evidence only and are not commit targets by default.

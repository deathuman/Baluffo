# Provider Discovery Coverage Gap Plan

> - **Status:** Active plan, advisory-only
> - **Use this when:** improving ATS/provider discovery coverage, provider migration staging evidence, or Admin/Ops visibility without adding Apify or another crawler runtime
> - **Canonical for:** next-step provider discovery coverage strategy and provider coverage gap report requirements
> - **Not canonical for:** provider adapter runtime behavior, report payload contracts, source registry policy, or source cleanup authority
> - **Then inspect:** [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../source-policy-runbook.md`](../source-policy-runbook.md), [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-05-19

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

- `docs/adapter-plugin-inventory.md` lists fetcher loaders and provider plugins for Greenhouse, Teamtailor, Lever, SmartRecruiters, Workable, Recruitee, Pinpoint, Ashby, BambooHR, Breezy, JazzHR, Personio, Workday, and `scrapy_static_sources`.
- `src/source_discovery/config.py` currently exposes discovery `SUPPORTED_PROVIDERS` for Greenhouse, Lever, SmartRecruiters, Workable, Teamtailor, Ashby, BambooHR, Workday, Recruitee, Pinpoint, and Personio.
- `src/source_discovery/provider_inference.py` builds provider candidate rows for those supported discovery providers, including safe Workday `*.myworkdayjobs.com` listing URLs with non-root paths. It does not currently build Breezy, JazzHR, iCIMS, SuccessFactors, Jobvite, Cornerstone/CSOD, Homerun, HRMOS, or Oracle HCM fetch candidates.
- `src/source_discovery/provider_migration_advisory.py` recognizes safe Workday candidates as supported migration/provider-staging evidence, and classifies Jobvite and Oracle HCM as unsupported-provider evidence. Breezy and JazzHR remain recognized migration families, but discovery row building for them is still future work.
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
2. Added tests showing Workday is a supported-provider inference candidate only when the row shape is safe, while Oracle HCM is classified as unsupported/provider gap evidence.
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

Remaining follow-ups are the broader ATS taxonomy, staged-provider validation visibility refinements, and Admin/Ops review surface. Those should be planned as separate slices.

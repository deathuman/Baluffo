---
name: baluffo-provider-coverage-triage
description: Triage Baluffo provider discovery coverage, source-policy soak reports, staged provider candidates, provider migration links, Workday or Breezy or BambooHR or ATS validation, source replacement readiness, and source-policy warning buckets. Use when resuming provider coverage work from artifacts, docs, or Basic Memory handoffs.
---

# Baluffo Provider Coverage Triage

## Overview

Use this skill to resume provider/source-policy coverage work from evidence instead of guessing from source names. Repo docs and artifacts are canonical; Basic Memory is continuity only.

## Workflow

1. Load the narrow source set.
   - Read `AGENTS.md`, `docs/INDEX.md`, `docs/AI_ASSISTANT_GUIDE.md`, `docs/source-policy-runbook.md`, `docs/scraping-pipeline.md`, `docs/adapter-plugin-inventory.md`, and `docs/testing.md`.
   - Read relevant Basic Memory handoffs for provider coverage, especially Workday, Breezy, and the latest provider discovery coverage handoff.
   - Inspect current `_out/source-policy-soak-report.json`, `data/jobs-fetch-report.json`, and `data/source-discovery-report.json` when present before making claims.

2. Classify the current blocker.
   - For live Umbrel diagnostics, start with `/ops/task-failure-attempts` before changing discovery/provider behavior.
   - Separate expected fetch exclusions, discovery dedupe skips, queue-filtered rows, permanent DNS misses, and 404/410 generated-path negatives from actionable parser, provider, TLS, 403/5xx, timeout, or validation failures.
   - Use soak buckets such as `fetchedButNotValidated`, `validatedProviderMissingMigrationSourceIdentity`, `zeroKeptFetched`, `fetchError`, and `providerCoverageNextAction`.
   - Separate parser bugs, genuinely empty boards, auth/401 boards, migration-link gaps, and unsupported provider gaps.
   - Do not start unsupported-family adapter work unless current evidence shows unsupported or probe-needed buckets.

3. Pick one bounded slice.
   - Prefer a single provider family, a single diagnostics bucket, or a single migration-link anomaly.
   - Validate with targeted fetches such as `--only-sources <loader>` plus the documented pending-provider or linked-static flags.
   - Confirm loader names and route signatures from source/tests instead of inventing names.

4. Implement only evidence-backed changes.
   - Parser fixes should preserve existing output shape and add focused fixtures.
   - Migration links should be explicit and justified by source identity evidence; do not bulk-link static rows.
   - When a static source becomes provider-backed, preserve homepage-vs-careers replacement policy and update tests from old static ids to canonical provider ids only after seed/source-registry evidence confirms the migration.
   - Keep source registry, provider metadata, and source-sync contracts stable unless the task explicitly changes them.

5. Regenerate evidence and close out.
   - Rerun the targeted fetch twice when consecutive-success behavior matters.
   - Regenerate the soak report with `scripts/source_policy_soak_report.py --data-dir data --out-dir _out`.
   - Run focused tests for provider migration, soak report, and affected parser code.
   - Update Basic Memory with final bucket counts, command results, changed source paths, and the next priority.

## Guardrails

- Preserve public source identities, locations, and persisted source-policy contracts.
- Treat live network output as unstable; use it as evidence, not as a brittle test oracle.
- Do not edit archived docs as the source of truth for current behavior.
- If a full validation run times out, capture partial evidence and switch to the smallest targeted command.

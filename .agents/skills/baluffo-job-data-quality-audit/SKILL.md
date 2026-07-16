---
name: baluffo-job-data-quality-audit
description: Audit Baluffo job data quality, invalid cities, missing or Unknown locations, no-openings classification, generic or non-job titles, provider title hydration, Remote OK cleanup, Google Sheets cleanup, job sanitization plans, canonical drop reasons, and job quality reports. Use when changing or validating job normalization, parser filtering, source reports, or public job data contracts.
---

# Baluffo Job Data Quality Audit

## Overview

Use this skill for evidence-backed cleanup of Baluffo job listings and source reports. The goal is to remove bad rows without silently dropping plausible game jobs or breaking public data contracts.

## Workflow

1. Ground in contracts and artifacts.
   - Read `AGENTS.md`, `docs/INDEX.md`, `docs/AI_ASSISTANT_GUIDE.md`, `docs/DATA_CONTRACT.md`, `docs/scraping-pipeline.md`, `docs/testing.md`, and any named plan under `docs/plans/`.
   - Read relevant Basic Memory handoffs for job sanitization or invalid-city/no-openings work.
   - Inspect current artifacts such as `data/jobs-unified.json`, `data/jobs-fetch-report.json`, `_out/*job*`, or the user-provided audit report when present.

2. Identify the data-quality surface.
   - Separate parser-stage filtering, canonicalization, source-report taxonomy, UI presentation, and audit/reporting logic.
   - Preserve missing-country placeholders such as `Unknown` unless the canonical contract says they are invalid.
   - Treat no-openings detection as visible-text classification, not raw HTML substring matching.

3. Look for loopholes before editing.
   - Test proposed drops against plausible real openings, category rows, static non-job pages, provider detail pages, search-empty pages, and pages with real anchors plus no-match text.
   - Do not drop likely-live jobs only because title extraction failed; repair exact category/static-container titles, prove they are containers/dead pages, or fail a quality gate.
   - Keep raw static extraction permissive when possible; let caller flows or finalization decide whether category/container rows are provisional or shippable.
   - Do not classify a detail page as a listing container only because it has apply/detail anchors when it also has a concrete job-like title.
   - Confirm whether source-level rows need loss counts, child-detail evidence, or canonical drop reasons before changing classification.
   - Do not add strict sector gates or broad employer/domain drops without explicit product approval.

4. Implement the smallest safe change.
   - Prefer shared helpers for repeated parsing or classification logic.
   - Keep output schemas and persisted contracts stable unless the task explicitly changes them.
   - Add focused regression tests for the exact false positive and false negative cases found.
   - Update `docs/DATA_CONTRACT.md` or the owning plan only when contract behavior changes.

5. Validate and close out.
   - Run focused job/provider/pipeline tests first, then broader Python tests if shared taxonomy or contracts changed.
   - After parser or data-quality changes, run the artifact quality gate against fresh output. For static/provider parsing changes, prefer bounded live validation before a full all-source run.
   - Add performance coverage for dedup/finalization changes that touch sourceBundle, sourceBundleCount, locations, or merge sidecar state.
   - If broad tests fail, diagnose the implementation root cause before editing tests.
   - If full live validation stalls, capture pipeline watchdog evidence and continue with targeted artifacts instead of waiting indefinitely.
   - Record durable gotchas or handoff state in Basic Memory when the cleanup changes future audit strategy.

## Guardrails

- Do not rewrite user-facing job locations, titles, or persisted contracts casually.
- Do not treat current live feed contents as stable acceptance criteria.
- Do not drop ambiguous game-adjacent rows without explicit non-game evidence.
- Avoid repeatedly copying growing sourceBundle or locations payloads during dedup/finalization; keep authoritative counts and test large merges.
- If the same test failure repeats twice, stop retrying and inspect docs, source, tests, and artifacts.

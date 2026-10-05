---
name: baluffo-coverage-delivery
description: Close the Baluffo catalogue coverage gap with honest delivery measurement, board registration, discovery queue draining, curated coverage boards, fetch-run kept counts, false-green metrics, board identity on host and tenant, Workday or Ashby or Greenhouse or adapter dispatch diagnosis, and coverage audits against the Games Jobs Index. Use when measuring, registering, or verifying catalogue coverage, diagnosing why registered boards do not deliver openings, or planning the next coverage slice.
---

# Baluffo Coverage Delivery

## Overview

Use this skill to close catalogue coverage gaps without repeating the mistakes
this workflow was written from: registration was measured as delivery (a live
run collected 0 of 6,929 "delivered" openings), a "missing" Workday adapter
existed and worked, and cross-tenant identity matches suppressed 64 boards and
1,257 openings. Coverage work here is never-ending; this workflow is the
default, not optional.

## Workflow

1. Ground in the plan and artifacts.
   - Read `docs/plans/catalogue-coverage-gap-plan.md` — especially *Where this stands*, *Correction on record*, and *Standing verification* — plus `AGENTS.md` and `docs/AI_ASSISTANT_GUIDE.md`.
   - Inspect `data/source-discovery-report.json`, `data/jobs-fetch-report.json`, and `_out/coverage/*` before making claims.
   - Note the generation date of any local DB or registry you reason from; a stale `data/baluffo-runtime.db` produced three wrong conclusions in one session.

2. Measure before claiming.
   - A board is **delivered** only when a fetch run keeps a non-zero count of its openings. Registry presence, adapter presence, and probe job-counts are not delivery.
   - Run the offline half of `tools/coverage_drain.py` first (isolated `--data-dir`); add `--verify-collected` for the real fetch — tens of minutes, local-only.
   - Exit code 3 (`EXIT_NOT_COLLECTED`) means boards registered but nothing collected: a preflight failure, not a report to read optimistically.

3. Run the sanity-check battery before any diagnosis. Wrong diagnoses are the worst offender here.
   - **Missing adapter?** Prove absence in `DEFAULT_SOURCE_LOADER_NAMES`, `registry_entries(<adapter>)`, and `adapterTimings` in the fetch report — all three, not one. Workday "missing" was present in all three.
   - **Never conclude from a zero.** The discovery probe is positive-only; a zero means nothing at all. Absence requires the rendered path or a real fetch, with a known-good control board of known expected count in the same run.
   - **Identity is host + tenant**, resolved through `tools/coverage_board_identity.py` and compared case-insensitively — the registry lowercases path segments. Never key on the id string or the studio label; cross-tenant "duplicate" matches suppressed 64 curated boards.
   - Distinguish harness assumptions from board failures: nine delivered defects all had the shape "the harness was looking somewhere the openings were not."

4. Take one bounded slice.
   - One defect or board family per commit; record the slice in the plan doc before and after.
   - Do not expand scope mid-slice; a newly found gap gets a plan entry, not a detour.

5. Implement, validate, close out.
   - Tests assert invariants, never a tool's current output; never pin a "delivered" column without kept counts. Assert pagination ceilings against the API's own reported `total`, never a literal.
   - `npm run test:py` and `npm run lint:repo-guardrails` before committing; `git status data/` must show no discovery-audit artifacts.
   - Isolate every discovery and fetch run (`BALUFFO_DATA_DIR` / `--data-dir` / `--output-dir`); omitting them writes stub state into live `data/`.
   - Update the plan doc's standing sections and leave a Basic Memory handoff with final counts, commands run, and the next priority.

## Guardrails

- Preserve public job text, locations, and persisted data contracts.
- Treat live network output as evidence, not as a brittle test oracle.
- Do not retire a dead board row for visibility fixes unless the plan says so.
- If a full validation run stalls, capture partial evidence and switch to the smallest targeted command.

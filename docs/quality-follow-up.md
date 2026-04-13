# Quality Follow-up

> **Status:** Active follow-up tracker
> **Last updated:** 2026-04-13
> **Current focus:** Provider parity, remaining static-source triage, and social experiment disposition
> **North Star:** Keep improving source quality and coverage using current evidence, not stale milestone sequencing.

---

## Current Follow-up Work

This document replaces the older milestone roadmap. It keeps only the unresolved, still-live quality work that remains relevant to the current codebase.

### 1. Provider parity follow-up

Provider families must be judged by full-run behavior, not by audit-only health.

Current focus:
- verify workable, personio, breezy, and jazzhr against full-run results
- keep adapter audit parity checks aligned with actual fetch output
- treat "healthy in audit, empty in full run" as unresolved until the live path is proven

Useful checks:
- `python -m pytest tests/test_jobs_fetcher_providers.py -q`
- `python -m pytest tests/test_jobs_fetcher_pipeline.py -q`
- `npm run test:py`

### 2. Static-source triage follow-up

The taxonomy and health-scoring work landed, but remaining high-waste static sources still need ongoing triage.

Current focus:
- fix, quarantine, or intentionally suppress the worst remaining static sources
- keep zero-kept and failure classifications meaningful in reports
- prefer targeted source cleanup over broad new static expansion

Useful checks:
- `python -m pytest tests/jobs_static -q`
- `python -m pytest tests/test_jobs_fetcher_quality.py -q`
- `npm run test:py`

### 3. Social experiment disposition

The social experiment scaffolding is in place. What remains is the operational decision: prove incremental value or explicitly deprioritize it.

Current focus:
- review whether social sources add unique useful jobs versus official-board ingestion
- confirm whether the current pilot should stay enabled, be narrowed further, or be deprioritized
- keep reporting and bridge visibility aligned with the actual experiment status

Useful checks:
- `python -m pytest tests/test_jobs_fetcher_pipeline.py -q`
- `python -m pytest tests/admin/test_admin_bridge_ops_runtime.py -q`
- `npm run test:py`

---

## Verification Lanes

- `npm run test:py` is the balanced developer lane.
- `npm run test:py:extended` is the full Python lane.
- `npm run test:py:timing` is the full-suite timing lane.

Use `docs/testing.md` as the canonical verification matrix.

---

## Related Docs

- [`testing.md`](testing.md) — current verification matrix and suite routing
- [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) — adapter/plugin ownership
- [`scraping-pipeline.md`](scraping-pipeline.md) — scraping and browser fallback flow
- [`DATA_CONTRACT.md`](DATA_CONTRACT.md) — stable runtime/data shapes

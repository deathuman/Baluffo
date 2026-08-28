# Dedup Pressure Reduction Plan

> - **Status:** Stale — validate-then-decide (reviewed 2026-08-28): confirm whether the monitor debt still exists against a fresh fetch report; if not, archive this plan; otherwise fold the remainder into [`jobs-coverage-improvement-plan.md`](jobs-coverage-improvement-plan.md)
> - **Use this when:** reducing dedup gate pressure without chasing individual static source failures
> - **Canonical for:** next-step dedup pressure strategy and latest measured evidence
> - **Not canonical for:** data payload contracts or source registry policy
> - **Then inspect:** [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and the latest fresh `data/jobs-fetch-report.json` from `npm run dev:pipeline`
> - **Last updated:** 2026-08-28 (status review — needs fresh-run validation before further work)

## Summary

The plan has pivoted away from fixing one static source family at a time. That approach exposed an endless sequence of weak static/parser/non-provider samples without changing the underlying gate pressure.

Current strategy: keep diagnostics intact, but gate lifecycle readiness only on high-confidence identity risk. Weak non-provider/static/parser identity pressure is monitor debt, not a lifecycle blocker. Trusted provider/static/provider-vs-provider weak-key collisions are avoided when primary URLs or provider IDs differ, preserving separate output rows instead of forcing risky non-primary merges.

## Fresh Evidence

Latest validation:

```powershell
python -m pytest -q tests/test_jobs_dedup_evidence_current_run.py tests/test_jobs_dedup_confidence_gate.py tests/test_dedup_pressure_report.py tests/test_pipeline_storage_gzip.py
npm run dev:pipeline
python tools/measurements/pipeline/dedup_pressure_report.py --fetch-report data/jobs-fetch-report.json --json --limit 10
npm run lint:precommit:changed
```

Measured fetch window: `2026-05-11T16:09:27.672442+00:00` to `2026-05-11T16:32:53.256271+00:00`.

Result:

- Gate is `warning`, with `lifecycleUxReady=true` and no blockers.
- Current-run non-primary merge gate now separates `blocking=0` from `monitor=3714`.
- Blocking non-primary merge reasons are empty; monitor-only non-primary merges are `secondaryKey=3395` and `sparseIdentity=319`.
- Current-run review queue remains monitor-only: `blocking=0`, `monitor=2665`.
- Provider/static gate has `0` blocked disagreements and `13` warning-only auto-safe variants.
- Provider/static review rows remain visible as diagnostics (`provider_static_disagreement=13`) but no longer duplicate-block through the generic high-risk review queue.
- Google Sheets role-bucket pressure remains visible as monitor debt: `1389` unresolved, `1143` review causes, `17047` guard-blocked rows.
- `dedup_pressure_report.py` now reconfigures stdout to UTF-8, so Windows JSON output no longer needs `PYTHONIOENCODING=utf-8`.
- Blocking examples are now exposed through `currentRunBlockingMergeExamplesByReason`, so capped monitor samples can no longer hide the trusted blocker families.
- Trusted weak-key collisions are now handled by preserving separate rows when primary URLs or provider IDs differ. This covers provider/provider rows, provider/static rows, and SmartRecruiters title/location aliases with distinct provider IDs. The GrackleHQ redirect alias remains a monitor-only exception.
- Windows `WinError 5` output replace during full pipeline finalize is mitigated by retry/backoff on normal atomic output writes.

Conclusion: the confidence gate achieved the intended strategic shift and the lifecycle gate is unblocked. Weak families such as cross-board static Teamtailor rows, On5 reply URLs, YC auth URLs, GameJobs search buckets, and weak Google Sheets role buckets remain visible as monitor debt, but they no longer drive lifecycle readiness blockers. Future work should not chase monitor debt unless a separate output-quality plan chooses a bounded slice.

## Key Strategy

- Treat provider-backed, social/trusted, provider/static disagreement, and other strong identity risks as blocking.
- Treat static, Google Sheets, directory/search/listing URLs, parser pollution, title/company-only bundles, and untrusted non-provider IDs as monitor-only unless stronger identity evidence is present.
- Record aggregate gate-tier counts in dedup stats so capped examples cannot hide true blocker volume.
- Preserve separate trusted rows instead of merging by weak secondary/sparse identity when primary URLs or provider IDs differ.
- Keep all monitor diagnostics in reports; do not delete jobs or silently suppress weak evidence.
- Do not continue source-by-source cleanup unless a later plan chooses a specific output-quality slice separate from lifecycle readiness.

## Next Priority Steps

1. **Hold the gate steady**
   - Treat the current state as the stop point: lifecycle gate is unblocked and monitor debt remains visible.
   - Do not chase monitor-only samples automatically.
   - If blockers reappear, inspect `currentRunBlockingMergeExamplesByReason` first and prefer preserving separate trusted rows over adding broad aliases.

2. **Keep provider/static as monitored diagnostics**
   - Current fresh run has `0` blocked provider/static disagreements and `13` warning-only variants.
   - Do not reopen provider/static work unless a future fresh run introduces a blocked dedicated provider/static gate row.

3. **Keep weak-noise cleanup separate**
   - Static/parser cleanup is now output-quality work, not lifecycle readiness work.
   - If pursued, choose one bounded family and do not use gate closure as the acceptance criterion.

4. **Maintain report clarity**
   - Keep `dedup_pressure_report.py` showing both blocking pressure and monitor debt.
   - Future plan updates must include the latest fetch window, blocking counts, monitor counts, and next decision point.

## Test Plan

- Focused confidence-gate tests:
  - static/static `secondary_key` and `sparse_identity` merges are monitor-only.
  - distinct provider/provider, provider/static, and SmartRecruiters alias rows do not merge on weak non-primary identity.
  - provider-backed non-primary merges with missing primary URLs remain blocking.
  - weak Google Sheets review summaries are monitor-only.
  - legacy reports without tier counts fall back safely.
- Pressure report tests:
  - blocking and monitor non-primary counts are reported separately.
  - blocking and monitor review queue causes are reported separately.
- Validation sequence before shipping:
  - `python -m pytest -q tests/test_jobs_dedup_evidence_current_run.py tests/test_jobs_dedup_confidence_gate.py tests/test_dedup_pressure_report.py tests/test_pipeline_storage_gzip.py`
  - `npm run dev:pipeline`
  - `python tools/measurements/pipeline/dedup_pressure_report.py --fetch-report data/jobs-fetch-report.json --json --limit 10`
  - `npm run lint:precommit:changed`

## Assumptions

- Lifecycle readiness should block only on high-confidence identity risks.
- Weak non-provider/static/parser pressure is accepted monitor debt.
- Registry state, tombstones, provider adapters, and source-policy automation remain unchanged by this strategy.

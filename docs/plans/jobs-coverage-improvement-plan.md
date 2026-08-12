# Jobs Coverage Improvement Plan

> - **Status:** Active follow-up plan (draft for review)
> - **Use this when:** improving jobs feed coverage — recovering zero-kept static sources, closing provider coverage gaps, promoting staged providers, or reducing sheet dominance
> - **Canonical for:** coverage-improvement prioritization and evidence thresholds; not canonical for adapter internals or source-policy approval authority
> - **Then inspect:** `docs/source-policy-runbook.md`, `docs/adapter-plugin-inventory.md`, `docs/scraping-pipeline.md`, `docs/plans/provider-discovery-coverage-gap-plan.md`, `docs/plans/browser-fallback-pool-plan.md`
> - **Evidence basis:** 2026-07-17 full-run artifacts (`data/jobs-source-state.json.gz`, `data/jobs-fetch-report-summary.json`, `data/registry-conflicts-summary.json`, `_out/source-policy-soak-report.json`), audit snapshot `docs/snapshots/jobs-entry-validation-audit-2026-08-12.md`
> - **Last updated:** 2026-08-12

## Coverage Baseline (2026-07-17 run, 40,586 rows)

| Lever | Count | Impact |
|---|---|---|
| Feed composition | sheets 78% / static 14% / provider 7.4% | provider+static recovery rebalances dependence on 3 sheets |
| Zero-kept static sources (last run) | **2,428 of 4,479** (54%) | biggest single recoverable surface |
| `site_changed` static + had jobs before | **269** | true parser regressions (page changed, adapter stale) |
| `site_changed` static + never had jobs | 733 | dead or never-parsed candidates |
| `needs_review` static + never had jobs | 807 | likely parser misses / unsupported layouts |
| anti-bot / JS-required static zero-kept | **109** (23 browser-eligible) | browser-fallback recovery (pool shipped 2026-08-11) |
| Provider adapters with zero yield | oracle 2/2, personio 5/7, ashby 6/11, bamboo 8/15, pinpoint 4/7, workday 2/9 | provider validation/triage targets |
| Provider coverage gaps (soak report) | **24** (9 fetched-not-validated, 15 validated-missing-migration-identity) | validation link work, no code |
| Pending registry rows | **163** (132 static, 31 provider: bamboo 13, workday 8, breezy 4…) | staged-but-never-promoted candidates |
| Browser fallback queue | 130 (timeout 91, anti-bot 20, blocked 14, parse_error 3, js 2) | matches anti-bot/js surface above |
| Static high-cost low-yield | e.g. lionhearts 80.9 s/0 kept, redemptiongames 72.6 s/0 | cleanup + budget wins |

## Principles

- **Recover before deleting.** A zero-kept source is a parser miss, a dead page, or an ATS migration — classify each with evidence before dropping (source-policy runbook authority).
- **Provider first.** Supported-provider adapters (bamboo/workday/ashby/personio/oracle…) already exist; validating + staging them yields structured rows that dedup cleanly and are 99.7% Game sector — better coverage per unit effort than static crawling.
- **No new dependencies, no Apify, no generic crawler** (per provider-discovery-coverage-gap-plan).
- **Evidence-tracked losses.** All drops flow through existing `canonicalDropReasons` / registry buckets; no silent removal.

## Work Tracks (priority order)

### Track 1 — Static zero-kept triage (evidence-first, source-policy runbook)

1. **Parser regression recovery (269 `site_changed` + had jobs before).**
   - Pull the 269 source list with old/new fingerprints and last error.
   - Sample 20–30 live pages; classify: adapter layout change (fix parser or plugin), ATS migration (reclassify to provider adapter), dead (drop via dead-source evidence).
   - Reuse `data/jobs-parser-regression-queue.json` (398 entries) and `scripts/source_audit_sweep.py`.
2. **Anti-bot / JS-required recovery (109 sources, 23 browser-eligible).**
   - Re-run the browser-eligible subset through the new browser fallback pool (`BALUFFO_BROWSER_POOL` default on; `browser_fallback_pool.py`).
   - Measure recovery rate vs the 130-entry fallback queue before scaling eligibility.
3. **Never-yield static cleanup (733 + 807 never-had-jobs).**
   - Batch evidence sweep like `docs/snapshots/jobs-dead-source-evidence-2026-04-29.md`; separate dead pages from unsupported layouts.
   - Unsupported layouts worth a plugin get a narrow static plugin (per adapter inventory); the rest are dead-source candidates.

### Track 2 — Provider coverage closure (no-code validation + staging)

1. **Soak report refresh first** (current report is stale: 2026-05-22).
   - `python scripts/provider_migration_staging_refresh.py --data-dir data --out-dir _out --apply-pending` then `python scripts/source_policy_soak_report.py --data-dir data --out-dir _out`.
   - Re-derive the 24 gap rows against current artifacts.
2. **Close the 15 validated-provider-missing-migration-identity rows** via the Admin migration-link workflow (one link at a time, runbook §Guardrails).
3. **Triage the 9 fetched-but-not-validated rows** (e.g. Wolcen bamboo) — probe/retry via `--include-pending-provider-migration` fetch, then validation.
4. **Promote healthy staged providers from pending** (31 provider rows) only with repeated validation evidence (`providerCoverageConsecutiveSuccesses`, `validated_provider` status).

### Track 3 — Provider adapter zero-yield triage (code, bounded)

- Oracle HCM 2/2 zero, personio 5/7 zero, ashby 6/11 zero: bounded live validation on the failing subset; classify as feed-shape change, auth wall, or genuinely empty.
- Each fix is a leaf-adapter change with focused tests + bounded live run (per data-quality skill §5).
- Workday/bamboo already healthy (7/9, 7/15) — lower priority.

### Track 4 — Sheet dominance rebalance (follows Track 2)

- Only after provider/static recovery: re-evaluate whether the default `google_sheets` 78% share is intentional (community sheet includes Tech rows — product call, ties into `non-game-employer-evidence-2026-08-12.md`).

## Suggested Verification

```bash
# Track 1 evidence sweep
python scripts/source_audit_sweep.py --help   # follow runbook usage
# Track 2 refresh
python scripts/provider_migration_staging_refresh.py --data-dir data --out-dir _out --apply-pending
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
# Bounded fetch of staged providers
python src/jobs_fetcher.py --only-sources <staged> --include-pending-provider-migration --ignore-circuit-breaker
# Gates after any code change
python scripts/precommit_gate.py --mode changed
```

## Open Questions for Review

1. Track 1 scale: sample 20–30 first, or run the full 269-regression sweep? (Recommend: sample first, then sweep.)
2. Dead-source batch: proceed like the 2026-04-29 physical deletion batch, or keep to registry demotion? (Recommend: registry demotion first.)
3. Browser-eligible recovery is capped at 23 now — expand after measuring the pool recovery rate?
4. Track 2 link apply is one-at-a-time by runbook; confirm the 15-link backlog is worth the manual pass.

## Applied 2026-08-12 (operator-approved, Track 2)

Evidence snapshot: `docs/snapshots/provider-coverage-closure-2026-08-12.md`.

- **Migration links applied (2, high-confidence 0.95):** Xsolla → `lever:account:xsolla`; CD PROJEKT RED → `smartrecruiters:company_id:cdprojektred`. `alreadyLinkedCount` 3 → 5; reviewCandidates now 0.
- **Provider rows approved active (9, repeated validation evidence):** bamboo `activategames`, `blazinggriffin`, `catface`, `flyingbark`, `relicentertainment`, `streamlinestudios`; breezy `fugo-games`, `flowplay-llc`, `warhorsestudios`. Active 2268 → 2277.
- **Dead provider rows rejected (3):** `bamboohr:lemonskystudios` (board redirects to BambooHR marketing page) provider + static rows, and `oracle_hcm:glass-egg` (DNS dead). Rejected 0 → 3.
- **Triage findings:** Beamdog / Eleventh Hour / Expression bamboo boards return `[]` (genuinely empty — correct behavior); IllFonic breezy `/json` returns `[]` (empty); reforged/wolcen bamboo have 1 job each (recovery on refresh); workday SSL failures (Aristocrat/Intel/SciPlay/Light & Wonder) are upstream expired certs — transient; Glass Egg static `glassegg.com/careers/` is live (kept — only the oracle host is dead).
- **Validated provider count** 20 → 15 after refresh (workday SSL + empty boards reclassified); next action `resolve_link_ambiguity` (Ubisoft, 6 static candidates, 0.65 confidence — blocked below the 0.75 apply threshold by design).

## Out of Scope

- Apify / external crawlers; new Python/Node deps; broad `google_sheets` removal; parser rewrites beyond leaf fixes; any auto-promote/suppress/delete behavior.

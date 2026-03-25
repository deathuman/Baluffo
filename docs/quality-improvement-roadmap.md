# Baluffo Quality Improvement Roadmap

> **Status:** Active — Q2 2026  
> **Last updated:** 2026-03-25  
> **North Star:** Increase useful live coverage without letting fetch cost, failure rate, or source noise scale faster than output quality.

---

## Baseline Capture Protocol

Before Milestone 1 starts, record a frozen baseline snapshot from the latest successful scheduled run and discovery run.

### Required Baseline Snapshot
- Fetch report summary:
  - **37,289** canonicalized input rows
  - **33,988** final output rows
  - **3,301** dedup merges
  - **566** completed tasks
  - **567** resolved sources
- Discovery report summary:
  - **173** probed candidates
  - **89** queued candidates
  - **21** deferred candidates
- Quality guardrails:
  - contamination audit: no regression from current baseline
  - location audit: no regression from current baseline

### Baseline Rules
- Store the snapshot in a roadmap log or machine-readable baseline file before changing logic.
- All KPI comparisons in this roadmap must be measured against that frozen baseline, not against later moving reports.

---

## AI Implementation Guardrails

- Do not change the shape of `data/source-discovery-report.json` or `data/source-discovery-candidates.json` unless the task explicitly includes a contract-change step.
- Prefer additive fields over renames or removals.
- Do not change stable `src/source_discovery` APIs unless the task explicitly calls for a contract or interface update.
- Any schema-affecting change must update documentation and tests in the same commit.
- Preserve backward compatibility for scheduled fetches and admin reporting.
- Any migration from static/custom to structured must keep rollback behavior until 3 consecutive healthy runs are observed.

---

## Reference Baseline Snapshot

From latest fetch report (2026-03-23):
- **37,289** canonicalized input rows → **33,988** final output rows
- **3,301** dedup merges, **566** completed tasks, **567** resolved sources

From latest discovery report (2026-03-23):
- **173** probed candidates, **89** queued candidates, **21** deferred
- Queued candidates entirely from `sheet_directory` stage

### Critical Pain Points

| Area | Metric | Issue |
|------|--------|-------|
| Static adapter | 541 sources, 1,496,789 ms, 443 errors, **463 zero-kept** | 85% failure/zero-yield rate |
| Provider drift | workable: 289,481 ms, 0 kept; personio: 3 fetched, 0 kept; breezy/jazzhr: 1 source, 0 kept | "Healthy" in audit, broken in full run |
| Discovery backlog | 89 queued, 21 deferred | Promotion pipeline not formalized |

---

## Success Metrics (Q2 Headline KPIs)

| KPI | Target |
|-----|--------|
| static errorCount | Down ≥35% |
| static zeroKeptCount | Down ≥30% |
| queued discovery backlog | Reduced ≥50% |
| New structured adapters/families | ≥2 live |
| Contamination/location audits | No regression |

---

## Milestone 1 — Source Health and Static Hardening

**Window:** Week 1–2  
**Owner:** Data ingestion maintainer  
**Support:** QA / test automation

### Why This Comes First

Static adapter in latest run:
- 541 sources, 1,496,789 ms consumed
- 443 errors logged
- 463 zero-kept sources (85% waste)

This is where the biggest immediate waste lives.

### Deliverables

| # | Deliverable | Description |
|---|-------------|-------------|
| 1.1 | Normalized source outcome taxonomy | Add failure buckets: `site_changed`, `no_openings`, `anti_bot_or_challenge`, `js_required`, `parser_empty`, `timeout`, `seed_invalid` |
| 1.2 | Automatic source health scoring | Rolling run history scoring (error rate, zero-kept rate, latency) |
| 1.3 | Quarantine rules | Auto-quarantine repeat failures (≥3 consecutive) and repeat zero-kept (≥3 consecutive) |
| 1.4 | Zero-kept classifier | Distinguish "legit empty" (no jobs) from "broken extraction" (failed to parse) |
| 1.5 | Regression report | Daily/weekly: top failing domains, top slow domains, top zero-kept domains, source families regressing vs previous run |

### Implementation Notes

- Add taxonomy handling to source reporting/classification in:
  - `src/jobs/adapters/static_scrapy.py`
  - `src/jobs/adapters/provider_api.py`
- Health scoring can leverage existing run history in `data/jobs-fetch-report.json`
- Quarantine rules may use registry lifecycle metadata or add a new `health_status` field

### KPIs

- Static error count reduced by 20% after first cleanup pass
- Static zero-kept count reduced by 15% without lowering total output
- Median static runtime reduced by 10%

### Exit Criteria

- [x] Every failed source lands in a meaningful failure bucket
- [ ] Top 25 worst static sources fixed, quarantined, or intentionally suppressed
- [x] Run report makes "why a source failed" obvious without manual log spelunking

### Completion Status ✅

**Implemented and verified — 2026-03-25.**

Full-run artifact (`data/jobs-fetch-report.json`, commit `1482d98`, 645 sources) confirms:
- `failureBucket` present on all error sources (e.g. `"unknown"`, propagated from taxonomy module)
- `zeroKeptClassification` present on all zero-kept sources (e.g. `"needs_review"`)
- `healthSummary` section present in report with `topFailingDomains`, `topZeroKeptDomains`, `topSlowDomains`, `quarantinedSources`
- `healthScore` and `consecutiveZeroKept` visible in `data/jobs-source-state.json`
- All 619 Python tests pass

**Remaining open item:** Top 25 worst static sources have not yet been individually triaged/fixed — that cleanup work continues in or around M2.

---

## Milestone 2 — Provider Drift Fixes

**Window:** Week 2–3  
**Owner:** Provider adapter maintainer  
**Support:** QA

### Why This Comes Next

Provider adapters appear "healthy" in audit but fail in full runs.

**Updated baseline from 2026-03-25 full run (645 sources, commit `1482d98`):**

| Adapter | Sources | Full-Run Status | keptCount | durationMs |
|---------|---------|-----------------|-----------|------------|
| workable | aggregated under `workable_sources` | error / zero-kept | 0 | ~289,000 ms |
| personio | aggregated under `personio_sources` | not registered in `register.py` | — | — |
| breezy | aggregated under `breezy_sources` | zero-kept | 0 | varies |
| jazzhr | aggregated under `jazzhr_sources` | zero-kept | 0 | varies |

**Key structural finding:** `personio` has a parser in `provider_parsers.py` but is **not registered** in `plugins/provider_api/register.py` and is therefore never executed in a real pipeline run.

Meanwhile structured adapters (greenhouse, lever, ashby, smartrecruiters, recruitee, pinpoint) produce clean non-zero output.

### Deliverables

| # | Deliverable | Description |
|---|-------------|-------------|
| 2.1 | Full-run parity checks | Compare adapter audit output vs full fetch output; flag divergences |
| 2.2 | Seed revalidation | Expand and revalidate seeds for workable, personio, breezy, jazzhr |
| 2.3 | Adapter smoke tests | Live fixture snapshots for each provider adapter |
| 2.4 | Automatic downgrade path | Priority: structured → listing-only fallback → static fallback (only when justified) |

### Implementation Notes

- Provider adapters live in:
  - `src/jobs/adapters/provider_api.py`
  - `src/jobs/adapters/plugins/provider_api/`
- Extend audit logic in `src/adapter_audit.py` to capture full-run parity metrics
- Consider adding an `adapter_health` computed field that combines audit + full-run signals

### KPIs

- workable returns at least one healthy non-zero source in full run
- personio yields non-zero kept count in full run
- breezy and jazzhr each have ≥3 validated seeds or are explicitly deprioritized

### Exit Criteria

- [ ] No adapter considered "healthy" based only on audit output
- [ ] Provider status judged from both targeted audit and full-run reality

---

## Milestone 3 — Discovery Promotion Pipeline

**Window:** Week 3–5  
**Owner:** Discovery maintainer  
**Support:** Data ingestion maintainer

### Why This Comes Third

Discovery produces a backlog but promotion is ad hoc:
- 89 queued candidates (all from `sheet_directory`)
- 21 deferred (`domain_cap`)
- Strong examples waiting: PlayVS, Niantic, Argus, Virtuos, Yostar, BambooHR-shaped Wolcen entry

### Deliverables

| # | Deliverable | Description |
|---|-------------|-------------|
| 3.1 | Candidate state machine | Explicit states: discovered → probed → validated → approved → live → quarantined |
| 3.2 | Candidate ranking | Rank by ATS confidence, studio priority, region priority, expected job yield, and uniqueness vs existing coverage |
| 3.3 | Batch-approval tooling | High-confidence structured candidates (greenhouse, lever, ashby) can be bulk-promoted |
| 3.4 | Domain-cap review flow | Deferred candidates periodically revisited, not forgotten |

### Implementation Notes

- Discovery package lives in `src/source_discovery/`
- Candidate data lives in `data/source-discovery-candidates.json`
- If adding fields like `state` or `rank_score`, follow the contract in `docs/DATA_CONTRACT.md`
- Prefer additive schema changes only

### KPIs

- Reduce queued candidates by 50%
- Promote ≥25 high-confidence candidates to active/live
- False-promotion rate <10%

### Exit Criteria

- [ ] Candidate promotion no longer requires ad hoc manual digging
- [ ] Queue sorted by likely yield, not just discovery recency

---

## Milestone 4 — Add Next Structured Adapters

**Window:** Week 5–7  
**Owner:** Provider adapter maintainer  
**Support:** Discovery maintainer

### Why This Comes Fourth

Discovery backlog already contains evidence:
- BambooHR-style source for Wolcen
- Workday-style URLs (TiMi's `wd1.myworkdayjobs` path)

Priority: BambooHR → Workday → any additional provider family repeatedly appearing in backlog

### Deliverables

| # | Deliverable | Description |
|---|-------------|-------------|
| 4.1 | BambooHR adapter | Listing extraction, detail extraction, canonical URL handling, smoke tests |
| 4.2 | Workday adapter | Pagination support, location normalization, de-dup safety against static fallback |
| 4.3 | Structured migration rules | Auto-recognize BambooHR/Workday sources and prefer structured over static |
| 4.4 | Smoke tests | Live fixture snapshots for both adapters |
| 4.5 | Shadow-mode migration | Run structured migration in shadow mode before full activation |

### Implementation Notes

- Follow existing provider adapter patterns in `src/jobs/adapters/plugins/provider_api/`
- Register adapters in `src/jobs/adapters/plugins/provider_api/register.py`
- Use existing adapters such as greenhouse, lever, and ashby as templates
- Migration safety rules:
  - structured source first runs in shadow mode
  - do not remove static/custom source immediately
  - static/custom source is only demoted after 3 consecutive healthy structured runs
  - rollback if kept count falls to zero or duplicate rate breaches threshold

### KPIs

- ≥10 live sources migrated from static/custom to structured adapters
- Structured-source share of total kept output increases by at least 10 percentage points versus the baseline run used to open M4
- Duplicate rate for migrated sources does not rise by more than 1 percentage point versus the pre-migration baseline, measured from canonical job IDs / dedup merge stats in the fetch report

### Exit Criteria

- [ ] Both adapters run successfully for at least 3 consecutive scheduled fetches, and each has at least 1 live source with non-zero kept output.
- [ ] At least one previously static source is measurably cleaner and faster after migration

### Definition of "Measurably Cleaner and Faster"

For this milestone, "measurably cleaner and faster" means:
- lower median runtime over 3 consecutive scheduled runs, and
- fewer extraction/classification failures over the same 3 runs

---

## Milestone 5 — Strategic Coverage Expansion

**Window:** Week 7–10  
**Owner:** Coverage / source curator  
**Support:** Discovery maintainer

### Why This Comes Fifth

Remaining work should be targeted, not broad. Current fetch shows EA and Nintendo already present as active static sources.

### Deliverables

| # | Deliverable | Description |
|---|-------------|-------------|
| 5.1 | Curated priority list | High-value custom career sites not yet covered |
| 5.2 | Target classification | Categorize as: `structured adapter likely`, `custom scraper needed`, `low ROI / defer` |
| 5.3 | Asia-targeted focus | Large publishers not well-covered, especially Asian-market targets where discovery shows evidence |
| 5.4 | Source inclusion rubric | Every new source has documented inclusion criteria |

### KPIs

- Add 10–15 high-value studios with non-zero output
- ≥30% of newly added sources are headquartered outside Europe, as defined by studio HQ in source registry metadata
- <20% of new sources enter long-term zero-kept bucket

### Definition of "Long-Term Zero-Kept"

A source enters the long-term zero-kept bucket if it records `keptCount == 0` for 3 consecutive scheduled runs after activation.

### Exit Criteria

- [ ] Every newly added source includes one documented justification: discovered candidate, known ATS match, publisher-priority target, or proven community demand
- [ ] Every added source has a recorded classification: structured-adapter likely / custom scraper / low ROI
- [ ] Every added source has first-run outcome recorded after activation

---

## Milestone 6 — Social/Community as Measured Experiment

**Window:** Week 10–12  
**Owner:** Community ingestion maintainer

### Why This Comes Last

Social had 3 sources, 3 kept, 2 zero-kept in latest run. Current config: Reddit disabled, X disabled, Mastodon enabled.

### Deliverables

| # | Deliverable | Description |
|---|-------------|-------------|
| 6.1 | Reddit pilot | Enable with small curated subreddit set |
| 6.2 | Unique-value measurement | Track jobs found only via social and jobs not already on official boards |
| 6.3 | Mastodon quality report | Measure whether it adds unique records vs noise |
| 6.4 | Defer larger channels | LinkedIn/Discord work deferred until core pipeline is healthier |

### KPIs

- At least 10% of kept social jobs are unique versus official-board ingestion during the pilot window
- Duplicate rate from social <70%
- False-positive rate <5% on a manually reviewed sample of 50 social rows

### Exit Criteria

- [ ] Social proves incremental value or is explicitly deprioritized

### Definition of "Explicitly Deprioritized"

A channel is explicitly deprioritized when:
- it is marked out of scope for the current quarter, and
- a short rationale is recorded in roadmap notes or source registry metadata

---

## Execution Order (Recommended)

For highest payoff, execute in this order:

1. **Static hardening** (M1) — Biggest immediate waste reduction
2. **Provider drift fixes** (M2) — Fix "healthy but broken" adapters
3. **Discovery promotion pipeline** (M3) — Convert backlog to coverage
4. **BambooHR + Workday** (M4) — Formalize discovered opportunities
5. **Targeted expansion** (M5) — Evidence-driven coverage growth
6. **Social experiments** (M6) — Validate or deprioritize

---

## Commit Scope Rules

- One deliverable per commit, or at most two tightly related deliverables
- Do not mix schema changes, adapter changes, and social/community changes in the same commit
- Any contract-changing work must be isolated in its own commit
- Every commit must include:
  - the code change
  - related tests
  - a short before/after metric note in the commit message or roadmap log
- If a migration is risky, land it in separate commits:
  - detection
  - tests
  - shadow mode
  - activation
  - cleanup

---

## Issue Breakdown (Optional GitHub Labels)

| Milestone | Issue Label | Example Issues |
|-----------|-------------|----------------|
| M1 | `pipeline-health` | Add source outcome taxonomy, add source health score, quarantine repeat failures, create top-failures dashboard, add zero-kept classifier |
| M2 | `provider-parity` | Workable live-run fix, Personio live-run fix, Breezy seed validation, JazzHR seed validation, audit-vs-full-run diff report |
| M3 | `discovery-promotion` | Candidate state machine, candidate ranking, batch approval tooling, domain cap review report, promotion metrics dashboard |
| M4 | `new-adapters` | BambooHR adapter, BambooHR tests, Workday adapter, Workday tests, structured migration rules |
| M5 | `coverage-expansion` | Curated publisher backlog, Asia-targeted source set, custom scraper pack, source inclusion rubric |
| M6 | `social-experiment` | Reddit pilot, unique-source measurement, Mastodon quality report |

---

## Verification Commands

### Preferred Commands

| Area | Command |
|------|---------|
| Full verification | `npm run verify` |
| Python tests | `npm run test:py` |
| Bridge tests | `python -m pytest tests/admin/ -q` |
| Source discovery | `python -m pytest tests/test_source_discovery.py -q` |
| Jobs pipeline | `python -m pytest tests/test_jobs_fetcher.py -q` |

### Portable Fallback Commands

Use these if the execution environment does not support the npm wrappers cleanly:

| Area | Command |
|------|---------|
| All Python tests | `python -m pytest tests -q` |
| Bridge tests | `python -m pytest tests/admin/ -q` |
| Source discovery | `python -m pytest tests/test_source_discovery.py -q` |
| Jobs pipeline | `python -m pytest tests/test_jobs_fetcher.py -q` |

---

## Related Documentation

- [`docs/scraping-pipeline.md`](docs/scraping-pipeline.md) — Scraping/browser fallback flow
- [`docs/adapter-plugin-inventory.md`](docs/adapter-plugin-inventory.md) — Source adapter inventory
- [`docs/DATA_CONTRACT.md`](docs/DATA_CONTRACT.md) — Source discovery contract (§7)
- [`docs/architecture-ai-map.md`](docs/architecture-ai-map.md) — Task routing and runtime contracts
- [`docs/testing.md`](docs/testing.md) — Test layout and targeted runs
- [`docs/milestone-2-plan.md`](docs/milestone-2-plan.md) — Detailed M2 implementation plan

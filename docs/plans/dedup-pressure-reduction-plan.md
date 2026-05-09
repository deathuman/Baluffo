# Dedup Pressure Reduction Plan

> - **Status:** Active plan
> - **Use this when:** reducing registry/dedup conflict volume after the sheet role-bucket guard and actionable badge split
> - **Canonical for:** next-step dedup pressure reduction priorities
> - **Not canonical for:** data payload contracts or current runtime evidence
> - **Then inspect:** [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and the latest fresh `data/jobs-fetch-report.json` generated after the relevant build
> - **Last updated:** 2026-05-09

## Summary

The previous dedup work separated actionable blockers from monitor-only diagnostics and blocked weak Google Sheets role-bucket merges. The next reductions should focus on preventing bad or duplicate sources from entering the registry and preventing inflated static job counts from defeating better provider sources.

Success means the next full pipeline shows fewer active conflicts, fewer provider/static blockers, fewer duplicate static URL variants, and a smaller actionable Dedup tab badge without hiding monitor diagnostics.

## Priority Steps

1. **Normalize static source URLs before conflict creation**
   - Strip fragments such as `#opening`, collapse `/index.html`, normalize trailing slashes, and canonicalize known careers host aliases before registry identity/conflict grouping.
   - Keep exact same concrete job-detail URLs mergeable; do not collapse unrelated career pages across different hosts.
   - Expected effect: remove same-source conflicts caused only by URL variants.

2. **Make provider/static auto-demotion decisive**
   - When a trusted provider and static source compete for the same company, automatically prefer the provider if it has equal or greater verified live jobs.
   - Demote static sources with no reliable live evidence, no job-detail URLs, or only weak listing-page evidence.
   - Keep unresolved cases reviewable when static has more verified live jobs or provider evidence is missing/stale.

3. **Fix static job-count inflation**
   - Tighten static extraction so counts come only from visible, current job rows/cards or verified job-detail links.
   - Reject inactive sections, stale embedded JSON/JSON-LD, duplicate cards, hidden templates, unrelated page navigation, and homepage text as job evidence.
   - Add regression cases for known failures such as Azra-style inflated static counts.
   - Preserve stored registry `jobsFound`, but prevent weak static counts from beating provider counts when `lastReliableJobsFound` or live adjudication says otherwise.

4. **Reject homepage and non-career static sources earlier**
   - Block discovery/promotion of homepage URLs unless the page exposes a careers/jobs route or provider-backed evidence.
   - Prefer promoting the discovered careers/jobs URL over the homepage when both exist.
   - Expected effect: reduce pending-vs-active and active static URL variant conflicts.

5. **Persist cleanup decisions**
   - After safe URL-variant or provider/static decisions, write demotion/rejection state so the same conflict does not recur on the next full pipeline.
   - Keep manual-review paths for cases that fail the safe-demotion predicates.

6. **Add a fresh post-run dedup pressure report**
   - After a new build/full pipeline, summarize blocking dedupe, monitor diagnostics, provider/static blockers, static URL variants, pending duplicates, same-normalized-URL duplicates, and top suspected causes.
   - Use this report to choose the next slice instead of using stale artifacts.

7. **Prefer completed live source-check evidence in conflict cards**
   - When conflict source checks cover every row in a family, use their live job counts for winner selection and safe automation.
   - Show `registryJobsFound` and `liveJobsFound` separately so stale registry counts remain visible without silently driving decisions.
   - Fall back to registry counts if source-check evidence is missing, partial, running, or failed.

8. **Make conflict source checks visibly progressive**
   - While `Check conflicting sources` is running, persist compact `heartbeatAt`, `taskProgress`, and `progress` diagnostics to the existing adjudication artifact.
   - Keep running payloads diagnostic-only with `families: []` so partial probes cannot influence winners or automation.
   - Render source/family counters, current target, and stale-heartbeat warnings in Admin/Ops without introducing a separate task route.

## Test Plan

- Add focused backend tests for static source URL canonicalization: fragment stripping, `/index.html` collapse, trailing slash normalization, and non-equivalent host preservation.
- Add registry/conflict tests proving same normalized static URLs do not create duplicate active conflicts.
- Add provider/static adjudication tests for provider wins on equal-or-greater verified jobs, static remains reviewable when it has stronger verified evidence, and zero-evidence static loses.
- Add static parser/count tests for stale hidden content, duplicate cards, homepage text, and known inflated-count examples.
- Add Admin/Ops or report tests for the fresh post-run pressure summary once its payload shape is introduced.
- Run the relevant focused Python/frontend tests plus `npm run lint:precommit`.

## Assumptions

- Existing report artifacts are not rewritten manually; count changes are validated only after a fresh pipeline run.
- Provider/static conflict cleanup remains separate from Google Sheets role-bucket merge guarding.
- Static sources can still win when they have stronger verified live job evidence than the provider.
- Monitor diagnostics remain visible, but only actionable blockers drive the Dedup tab badge and readiness pressure.

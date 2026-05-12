# Static Outlier Source Conflict Decisions

> - **Status:** Archived operator decision record
> - **Use this when:** reviewing historical Super Lucky and Koei static-outlier source conflict decisions
> - **Canonical for:** historical operator intent only
> - **Not canonical for:** runtime behavior, registry edits, source suppression, timeout tuning, persisted-job behavior, or source-output behavior
> - **Then inspect:** [`../source-policy-runbook.md`](../source-policy-runbook.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and current source-policy reports if fresh behavior matters
> - **Last updated:** 2026-05-12

This record documents operator review intent only. It does not authorize runtime behavior changes, registry edits, source suppression, timeout tuning, persisted-job changes, or source-output behavior changes until a separate implementation change is made.

## Summary

The latest focused static-outliers benchmark makes the first conflicts clear enough to stop adding diagnostics artifacts and resolve source-policy ambiguity directly. Super Lucky and Koei both keep output while showing cross-host source identity evidence, so they must be treated as explicit policy/scope decisions before Maliyo timeout work.

Evidence source:

- `_out/perf-sanity-fetch-static-outliers/source-decision-matrix.md`
- Benchmark command: `cmd /c npm run perf:fetch:static-outliers`
- Latest run status: passed

## Super Lucky

- Source: `static_source::static:listing_url:https://www.superluckycasino.com`
- Decision type: `explicit_source_policy`
- Current listing host: `superluckycasino.com`
- Off-listing hosts: `stillfront.com`
- Kept output: `27`
- Duration: `24095ms`
- Kept-output host breakdown: `stillfront.com=4`, `stillfrontgroup.teamtailor.com=1`, `twinharbour.teamtailor.com=1`, `goodgamestudios.teamtailor.com=1`
- Risk: kept-output source with persisted/output-contract impact; behavior changes require explicit source-policy implementation.
- Recommended decision: `split_source`
- Decision status: `proposed`
- Required next implementation: `none in this pass`

Rationale:

Super Lucky starts from `superluckycasino.com`, redirects/scope-expands into `stillfront.com`, and emits kept output from Stillfront and Teamtailor child hosts. This is not a safe mechanical timeout fix. The current behavior should be preserved until a separate source-policy implementation decides how to represent the parent/child source split.

Implementation sketch:

- Prefer narrowing the Super Lucky static row to its own listing host instead of creating a new Stillfront source.
- The default registry already has `Stillfront (Sheet)` as `static:listing_url:https://www.stillfront.com/en/career/join-the-team/`.
- The default registry already has `Stillfront (Teamtailor)` as `teamtailor:listing_url:https://stillfrontgroup.teamtailor.com/jobs`.
- Keep the Super Lucky registry `id` unchanged so the source identity remains stable while removing cross-host Stillfront pages from that row.
- Treat any resulting Super Lucky output drop as expected only after explicit implementation review, because current persisted output includes Stillfront/Teamtailor jobs under the Super Lucky source.

Implementation status:

- Default seed narrowed in `data/defaults/source-registry-active.seed.json`.
- Local runtime active registry was checked after the seed update; neither `data/source-registry-active.json` nor `data/source-registry-active.json.gz` existed, so no workspace-local Super Lucky row needed patching.

## Koei

- Source: `static_source::static:listing_url:https://koeitecmo.vn`
- Decision type: `explicit_source_scope`
- Current listing host: `koeitecmo.vn`
- Off-listing hosts: `careerviet.vn`
- Kept output: `7`
- Duration: `25027ms`
- Kept-output host breakdown: `koeitecmo.vn=5`, `vieclamit.vn=1`
- Risk: kept-output source with source-scope and timeout pressure; behavior changes require explicit source-scope implementation.
- Recommended decision: `split_source`
- Decision status: `proposed`
- Required next implementation: `none in this pass`

Rationale:

Koei keeps output from the primary listing host and a second kept-output host while the registry also contains `careerviet.vn` cross-host pages. The timeout evidence is real, but the scope conflict should be resolved before any timeout-budget or extraction behavior change.

## Not Blocking This Decision Pass

- Netflix: `follow_up_review`; zero-kept `needs_review`, not a kept-output conflict.
- Maliyo: `slow_productive_static`; kept output is on `maliyo.com=7` and `requiresExplicitDecision=false`, making it the cleanest later timeout diagnostics candidate after policy/scope conflicts are resolved.
- Atvis: not part of the latest explicit-decision matrix; keep as follow-up source-policy review if fresh evidence reintroduces it as a kept-output conflict.

## Next Implementation Boundary

The next code-bearing implementation should choose one explicit conflict and make a separate, reviewed source-policy/source-scope change. Until then, preserve current fetch behavior and source output.

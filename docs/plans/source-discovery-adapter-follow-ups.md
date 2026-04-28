# Deletion-First Source Discovery Reset

> - **Status:** Active deletion-first roadmap
> - **Use this when:** planning source-discovery simplification, adapter deletion, active-source yield work, or discovery behavior changes
> - **Canonical for:** source-discovery reset goals, protected surfaces, removable internals, complexity gates, and migration sequence
> - **Not canonical for:** saved-job/local-user data contracts, bridge endpoint contracts, or current persisted discovery payload details
> - **Then inspect:** [`scraping-pipeline.md`](../scraping-pipeline.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and the owning source-discovery modules
> - **Last updated:** 2026-04-28

This tracker replaces the previous helper-first follow-up roadmap. The previous work created real shared primitives, but it did not achieve the lean adapter objective. The new product goal is simpler: find active job sources and real openings in acceptable time, while protecting saved jobs, local user data, and current UI/runtime invocation paths.

Source-discovery internals are now considered removable unless they demonstrably improve current active-source yield or preserve a protected runtime surface. Historical behavior, long-term rollback flags, adapter-local orchestration, and compatibility wrappers should not survive by default.

## Current Verdict

| Area | Verdict | Readout |
| --- | --- | --- |
| Shared logic adoption | Partially achieved | Shared fetch, audit, page outcome, recovery, probe, browser, queue, and reporting primitives exist and are used by multiple adapters. |
| Thin adapters | In progress | Gameprog's public discovery entrypoint now delegates to the audit path and no longer owns the legacy cache branch. Gamesmap, Sheet-directory, Web-derived discovery, and GameDevMap still own substantial orchestration, wrappers, summaries, config glue, and artifact/report behavior. |
| Complexity/LOC reduction | Not convincingly achieved | Complexity moved into shared helpers and callback surfaces, while many adapter modules remain large and several source-discovery C901 offenders remain in the baseline. |
| Next direction | Deletion-first reset | Stop adding helpers as the success metric. Migrate adapters toward specs, delete legacy paths, and require net simplification. |

## Protected Surfaces

These are protected unless a future plan explicitly changes the product contract and test coverage:

- Saved jobs and local user data.
- Current frontend/local storage behavior for saved/local user sections.
- Current UI/runtime invocation paths that start discovery and fetch flows.
- Bridge/API contracts needed by the current UI/runtime.
- Queue, pending review, tombstone, static suppression, and admin auto-approval behavior when candidates enter the current product flow.

## Removable Surfaces

These are not sacred and should be removed or collapsed when tests prove current active-source behavior remains acceptable:

- Legacy direct discovery paths that duplicate the shared audit/runner path.
- Long-term rollback flags such as permanent `activeAuditEnabled=false` alternate paths.
- Adapter-owned fetch, retry, recovery, probe, dedupe, report, audit, and progress lifecycle code.
- Compatibility wrappers that exist only because older discovery internals existed.
- Historical report/cache/artifact details that are not consumed by the current UI/runtime or active maintenance workflow.

## Target Adapter Shape

Adapters should become source specs plus parsers, not mini-orchestrators.

An adapter may own:

- Source id, display name, and stage labels.
- Entry URLs, API endpoints, or source-specific seed queries.
- Parser for source-specific index, detail, CSV, JSON, or category formats.
- Evidence metadata and source-specific row fields.
- Optional limits that are truly source-specific.

Shared runners should own:

- Fetch, retry, cache reuse, and timing.
- HTTP recovery and browser recovery.
- Provider inference and static fallback generation.
- Probe dispatch and probe result classification.
- Candidate dedupe and queue/pending movement.
- Report summary, audit/progress writing, and artifact lifecycle.

A healthy adapter should read more like:

```text
SourceSpec(
    id="gameprog",
    display_name="GameProg",
    entrypoints=[...],
    parse_index=parse_gameprog_teams_json,
    evidence=SourceEvidence(...),
    limits=SourceLimits(...),
)
```

## Hard Gates For Future Source-Discovery Refactors

- No new helper unless the same slice deletes or substantially thins adapter-owned code.
- Every source-discovery refactor should be net LOC-negative unless it adds new source coverage.
- No adapter implementation should own fetch, recovery, probe, dedupe, report, or audit lifecycle after migration.
- No new source-discovery C901 offenders.
- Existing source-discovery C901 offenders must trend down by count, complexity score, or line footprint.
- Rollback paths must be temporary, named, tested, and include removal criteria.
- Behavior changes are allowed inside discovery/fetch internals when protected surfaces remain tested.

## Current C901 Baseline

From `scripts/complexity_baseline.json`, current source-discovery C901 offenders include:

| Function | Score |
| --- | ---: |
| `src/source_discovery/reporting_backlog.py::build_m5_strategic_backlog` | 32 |
| `src/source_discovery/orchestrator_generation.py::prepare_probe_inputs` | 30 |
| `src/source_discovery/gamesmap_candidates.py::discover_gamesmap_candidates` | 23 |
| `src/source_discovery/web_search_candidates.py::_provider_candidate` | 23 |
| `src/source_discovery/gamesmap_parsing.py::parse_gamesmap_detail_page` | 20 |
| `src/source_discovery/orchestrator_probe.py::probe_and_recover` | 20 |
| `src/source_discovery/probe.py::parse_probe_count` | 18 |
| `src/source_discovery/sheet_directory.py::discover_game_studio_sheet_candidates` | 17 |
| `src/source_discovery/gamedevmap.py::discover_gamedevmap_candidates` | 16 |
| `src/source_discovery/provider_patterns.py::build_pattern_candidates` | 16 |
| `src/source_discovery/probe.py::fallback_probe_urls` | 15 |
| `src/source_discovery/gamesmap_candidates.py::gamesmap_matches_category` | 14 |
| `src/source_discovery/probe.py::async_probe_candidate` | 13 |
| `src/source_discovery/provider_patterns.py::provider_reinforcement_score` | 13 |
| `src/source_discovery/core_scoring.py::compute_candidate_rank` | 12 |
| `src/source_discovery/gamesmap_parsing.py::_extract_json_array` | 11 |
| `src/source_discovery/probe.py::probe_candidate` | 11 |

Parser complexity can remain temporarily when it is genuinely source-format complexity. Orchestration complexity should not.

## Migration Sequence

### 1. Reset Roadmap And Baseline Metrics

**Status:** Current slice.

Replace helper-first roadmap language with this deletion-first charter, protected/removable surfaces, C901 baseline, target adapter shape, and hard gates.

Projected result: future work has a clear simplification standard instead of rewarding abstraction without adapter thinning.

### 2. Gameprog Proof Point: Delete Legacy Discovery Branch

**Status:** Completed first code slice.

Gameprog now treats the directory audit path as the public discovery path when the adapter is enabled. The old `activeAuditEnabled=false` legacy cache branch was removed from `discover_gameprog_candidates(...)`; fresh audit artifacts are the cache boundary.

Projected result: the first deletion-first proof point is net LOC-negative, and `src/source_discovery/gameprog.py::discover_gameprog_candidates` no longer appears in the C901 baseline.

Remaining removal criteria:

- Keep `activeAuditEnabled=false` accepted as harmless legacy config input for now, but do not route Gameprog through a separate legacy implementation.
- Remove or rename the Gameprog `activeAuditEnabled` config field from docs/defaults in a later config cleanup once other adapters no longer use the same rollback convention.

### 3. Create Source-Spec Runner Contract

Define a generic source-spec runner that accepts source metadata, entrypoints, parser callbacks, evidence metadata, and source limits.

Projected result: adapters can move from orchestration modules to spec modules without losing source-specific parser/evidence behavior.

Acceptance criteria:

- Contract is introduced with one adopter in the same slice.
- The adopter is net LOC-negative.
- No new source-discovery C901 offender is added.
- Protected UI/runtime and saved/local-user surfaces are untouched.

### 4. Migrate One Low-Risk Adapter Spec-First

Recommended next candidate: Sheet-directory.

Gameprog proved the deletion gate by removing its legacy public discovery branch. Sheet-directory is now attractive because CSV parsing and evidence are clear, but the current sheet path has more candidate/recovery wrinkles.

Projected result: one adapter stops owning fetch/recovery/probe/dedupe/report/audit lifecycle and becomes the proof point for deletion-first migration.

Acceptance criteria:

- Adapter implementation is net LOC-negative.
- Legacy direct path is deleted or explicitly temporary with removal criteria.
- Adapter-specific parser and evidence semantics remain local.
- Targeted adapter tests and `tests/source_discovery` pass.

### 5. Remove Legacy Fallback Path For Migrated Adapter

Delete permanent rollback paths after the spec-runner path is validated.

Projected result: the codebase loses a second path instead of carrying both old and new discovery systems indefinitely.

Acceptance criteria:

- No runtime/UI caller depends on the deleted path.
- Tests prove current discovery output remains acceptable for active-source behavior.
- Config docs no longer imply rollback flags are long-term architecture.

### 6. Thin Test Scaffolding After Deletion Proofs

After a deletion slice lands and the new behavior is protected, consolidate repeated adapter test setup without weakening the guardrails.

For Gameprog specifically, later cleanup should reduce repeated audit-path, TTL, payload, and legacy-flag setup in the focused tests. Keep explicit assertions that the legacy Gameprog cache/direct branch is gone and fresh audit artifacts are the cache boundary.

Projected result: tests become leaner after behavior is locked, instead of blocking the initial deletion proof with premature test abstraction.

Acceptance criteria:

- Test changes are readability/LOC improvements only.
- No production behavior changes in the same slice.
- Existing guardrails still prove deleted legacy paths stay deleted.

### 7. Repeat By Yield Priority

Suggested order after the first proof point:

- Gamesmap, because it still has large adapter-owned scan/category/orchestration surface.
- Web-derived discovery, because it has high behavioral complexity and C901 risk.
- GameDevMap, because it has the largest artifact/recovery surface and should wait until the spec-runner contract is proven elsewhere.

Projected result: adapters converge toward specs, shared runners own lifecycle, and source-discovery complexity trends down instead of sideways.

## Validation Standard

Documentation-only reset slices:

```powershell
cmd /c npm run lint:precommit
```

Code migration slices:

```powershell
python -m pytest -q tests/source_discovery
cmd /c npm run lint:precommit
```

Add targeted adapter tests before the full source-discovery suite when a specific adapter is migrated.

## Decision Rules

- If a path does not improve current active-source/job discovery and is not a protected surface, prefer deletion.
- If a helper does not delete or substantially thin adapter code in the same slice, do not add it.
- If preserving old behavior blocks simplification, preserve only the current product behavior and test that boundary.
- If an adapter needs source-specific semantics, keep those semantics local but move lifecycle out.
- If a rollback flag is still needed, document why, who uses it, and exactly when it can be removed.

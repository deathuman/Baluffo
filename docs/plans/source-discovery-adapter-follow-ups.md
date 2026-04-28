# Deletion-First Source Discovery Reset

> - **Status:** Active deletion-first roadmap
> - **Use this when:** planning source-discovery simplification, adapter deletion, active-source yield work, or discovery behavior changes
> - **Canonical for:** source-discovery reset goals, protected surfaces, removable internals, complexity gates, and migration sequence
> - **Not canonical for:** saved-job/local-user data contracts, bridge endpoint contracts, or current persisted discovery payload details
> - **Then inspect:** [`scraping-pipeline.md`](../scraping-pipeline.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and the owning source-discovery modules
> - **Last updated:** 2026-04-28

This tracker covers only source-discovery adapters and orchestration. Job-fetcher adapters such as static, provider API, community, and social belong to [`adapter-plugin-inventory.md`](../adapter-plugin-inventory.md) and fetch pipeline docs.

The product goal is simple: find active job sources and real openings in acceptable time while protecting saved jobs, local user data, and current UI/runtime invocation paths. Source-discovery internals are removable unless they improve current active-source yield or preserve a protected runtime surface.

## Current Adapter State

| Area | Current state | Next pressure |
| --- | --- | --- |
| Shared audit runner | [`DirectoryAuditRunSpec`](../../src/source_discovery/directory_audit.py) and `run_directory_audit_spec` already exist; the old shared direct-scan rollback wrapper has been deleted. | Keep the remaining work focused on thin adapter-owned lifecycle around the existing runner; do not plan as if the first audit runner still needs to be created. |
| Gameprog | Migrated proof point. [`discover_gameprog_candidates`](../../src/source_discovery/gameprog.py) always returns rows from the audit artifact when enabled; `activeAuditEnabled=false` is accepted as legacy input but no longer routes to the old cache branch. | Keep as the deletion proof and later remove/rename the stale config field when other adapters no longer need the rollback convention. |
| Gamesmap | Public discovery now returns rows from the shared directory audit artifact; `activeAuditEnabled=false` is harmless legacy input. Large scan/category/parser surfaces remain. | Reduce orchestration complexity before parser-only complexity. |
| Sheet-directory | Public discovery now returns rows from the shared directory audit artifact; `activeAuditEnabled=false` is harmless legacy input. It still owns CSV parsing, recovery, summary, and scan glue. | Later cleanup can split source metadata/evidence from scan/recovery plumbing after higher-value rollback paths are deleted. |
| Web-derived discovery | Uses `DirectoryAuditRunSpec` in [`web_search_candidates.py`](../../src/source_discovery/web_search_candidates.py), but still owns seed-careers/web-search scan, recovery, browser-recovery, and report complexity. | Keep after one simpler adapter proof because behavior is broader and failure modes are higher. |
| GameDevMap | Uses a separate active-source audit engine through [`gamedevmap.py`](../../src/source_discovery/gamedevmap.py) and `gamedevmap_active_dry_run.py`; artifact/recovery behavior is larger than the directory-audit adapters. | Keep last until the source-discovery reset pattern is proven elsewhere. |
| Stage wiring | [`orchestrator_generation.py`](../../src/source_discovery/orchestrator_generation.py) owns stage invocation and compatibility with current discovery flows. | Treat route changes as compatibility work and preserve task-start, busy-state, queue, pending review, and report behavior. |

## Protected Surfaces

- Saved jobs and local user data.
- Current frontend/local storage behavior for saved/local user sections.
- Current UI/runtime invocation paths that start discovery and fetch flows.
- Bridge/API contracts needed by the current UI/runtime.
- Queue, pending review, tombstone, static suppression, and admin auto-approval behavior when candidates enter the current product flow.

## Removable Surfaces

- Legacy direct discovery paths that duplicate the shared audit path.
- Permanent rollback branches such as long-term `activeAuditEnabled=false` alternate paths.
- Adapter-owned fetch, retry, recovery, probe, dedupe, report, audit, and progress lifecycle code.
- Compatibility wrappers that exist only because older discovery internals existed.
- Historical report/cache/artifact details that are not consumed by the current UI/runtime or active maintenance workflow.

## Target Adapter Shape

Adapters should become source metadata plus parsers/evidence code. Shared runners should own fetch, retry, cache reuse, HTTP/browser recovery, probe classification, candidate dedupe, queue/pending movement, report summaries, progress writing, and artifact lifecycle.

Keep source-specific semantics local: source id, display name, stage labels, entry URLs, API endpoints, seed queries, parser callbacks, evidence metadata, row fields, and truly source-specific limits.

## Hard Gates

- No new helper unless the same slice deletes or substantially thins adapter-owned code.
- Each source-discovery refactor should be net LOC-negative unless it adds new source coverage.
- No adapter should own fetch, recovery, probe, dedupe, report, or audit lifecycle after migration.
- No new source-discovery C901 offenders.
- Existing source-discovery C901 offenders must trend down by count, score, or line footprint.
- Rollback paths must be temporary, named, tested, and include removal criteria.
- Behavior changes are allowed inside discovery/fetch internals when protected surfaces remain tested.

## Current C901 Baseline

From [`scripts/complexity_baseline.json`](../../scripts/complexity_baseline.json), current source-discovery C901 offenders include:

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
| `src/source_discovery/sheet_directory.py::parse_game_studio_sheet_csv` | 16 |
| `src/source_discovery/probe.py::fallback_probe_urls` | 15 |
| `src/source_discovery/gamesmap_candidates.py::gamesmap_matches_category` | 14 |
| `src/source_discovery/probe.py::async_probe_candidate` | 13 |
| `src/source_discovery/provider_patterns.py::provider_reinforcement_score` | 13 |
| `src/source_discovery/gamesmap_parsing.py::_parse_gamesmap_index_entries_with_diagnostics` | 13 |
| `src/source_discovery/core_scoring.py::compute_candidate_rank` | 12 |
| `src/source_discovery/gamesmap_parsing.py::_extract_json_array` | 11 |
| `src/source_discovery/probe.py::probe_candidate` | 11 |

Parser complexity can remain temporarily when it is genuinely source-format complexity. Orchestration complexity should not.

## Migration Sequence

### 1. Gameprog Proof Point: Preserve The Deleted Legacy Branch

Gameprog is the reference deletion slice. Keep `activeAuditEnabled=false` harmless for now, but do not restore the separate legacy cache/direct implementation.

Acceptance criteria:

- `discover_gameprog_candidates(...)` continues to return audit-artifact rows when the adapter is enabled.
- The old Gameprog cache path is not recreated.
- Later config cleanup removes or renames the stale Gameprog flag after shared rollback convention cleanup.

### 2. Sheet-directory Thinning

Sheet-directory public discovery now routes through the existing directory-audit runner. Further work should move it toward source metadata plus CSV parser/evidence code without adding another runner layer.

Acceptance criteria:

- Public discovery returns audit-artifact rows when the stage is enabled.
- The legacy direct-scan rollback path is not restored.
- CSV parsing and evidence semantics remain local.
- Targeted Sheet-directory tests and `tests/source_discovery` pass.

### 3. Gamesmap Thinning

Gamesmap public discovery now routes through the existing directory-audit runner. Further work should reduce adapter-owned scan/category orchestration before touching source-format parser complexity.

Acceptance criteria:

- Public discovery returns audit-artifact rows when the adapter is enabled.
- The legacy cache/direct rollback path is not restored.
- Gamesmap audit rows remain acceptable for current discovery behavior.
- C901 pressure in `gamesmap_candidates.py` trends down.

### 4. Web-derived Discovery Thinning

Apply the proven pattern to seed-careers and web-search after a simpler adapter has landed.

Acceptance criteria:

- Seed-careers and web-search continue feeding candidates through the current queue, pending review, tombstone, and auto-approval path.
- Browser-recovery artifact behavior remains explicit and tested.
- Web-search C901/report complexity trends down.

### 5. GameDevMap Reset

Handle GameDevMap after the directory-audit adapters because it uses a larger active-source audit and recovery engine.

Acceptance criteria:

- Default discovery, dry-run audit, lost-recovery comparison, and explicit browser recovery keep their current invocation surfaces.
- Artifact compatibility changes are documented and tested when needed.
- Adapter-owned lifecycle decreases without weakening active-source yield.

### 6. Test Scaffolding Cleanup

After deletion slices land, consolidate repeated adapter test setup without weakening guardrails.

Acceptance criteria:

- Test changes are readability/LOC improvements only.
- No production behavior changes in the same slice.
- Existing guardrails still prove deleted paths stay deleted.

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

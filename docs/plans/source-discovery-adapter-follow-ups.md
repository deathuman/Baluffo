# Deletion-First Source Discovery Reset

> - **Status:** Active deletion-first roadmap
> - **Use this when:** planning source-discovery simplification, adapter deletion, active-source yield work, or discovery behavior changes
> - **Canonical for:** source-discovery reset goals, protected surfaces, removable internals, complexity gates, and migration sequence
> - **Not canonical for:** saved-job/local-user data contracts, bridge endpoint contracts, or current persisted discovery payload details
> - **Then inspect:** [`scraping-pipeline.md`](../scraping-pipeline.md), [`DATA_CONTRACT.md`](../DATA_CONTRACT.md), and the owning source-discovery modules
> - **Last updated:** 2026-04-29

This tracker covers only source-discovery adapters and orchestration. Job-fetcher adapters such as static, provider API, community, and social belong to [`adapter-plugin-inventory.md`](../adapter-plugin-inventory.md) and fetch pipeline docs.

The product goal is simple: find active job sources and real openings in acceptable time while protecting saved jobs, local user data, and current UI/runtime invocation paths. Source-discovery internals are removable unless they improve current active-source yield or preserve a protected runtime surface.

## Current Adapter State

| Area | Current state | Next pressure |
| --- | --- | --- |
| Shared audit runner | [`DirectoryAuditRunSpec`](../../src/source_discovery/directory_audit.py) and `run_directory_audit_spec` already exist; the old shared direct-scan rollback wrapper has been deleted. | Keep the remaining work focused on thin adapter-owned lifecycle around the existing runner; do not plan as if the first audit runner still needs to be created. |
| Gameprog | Migrated proof point. [`discover_gameprog_candidates`](../../src/source_discovery/gameprog.py) always returns rows from the audit artifact when enabled; `activeAuditEnabled=false` is accepted as legacy input but no longer routes to the old cache branch. | Keep as the deletion proof and later remove/rename the stale config field when other adapters no longer need the rollback convention. |
| Gamesmap | Public discovery now returns rows from the shared directory audit artifact; `activeAuditEnabled=false` is harmless legacy input. Large scan/category/parser surfaces remain. | Reduce orchestration complexity before parser-only complexity. |
| Sheet-directory | Public discovery now returns rows from the shared directory audit artifact; `activeAuditEnabled=false` is harmless legacy input. It still owns CSV parsing, recovery, summary, and scan glue. | Later cleanup can split source metadata/evidence from scan/recovery plumbing after higher-value rollback paths are deleted. |
| Web-derived discovery | Seed-careers and web-search runtime paths now return shared audit-artifact rows; `activeAuditEnabled=false` is harmless legacy input. Browser-recovery artifact loading, fetch analysis, probe-result validation, summary update, and persistence have been split into narrower helpers. | Shared browser-recovery save/merge remains deferred because web-search and GameDevMap persistence semantics are not equivalent. |
| GameDevMap | Uses the separate active-source audit engine through [`gamedevmap.py`](../../src/source_discovery/gamedevmap.py) and `gamedevmap_active_dry_run.py`; `activeAuditEnabled=false` is harmless legacy input and no longer routes to the old cache/direct CSV scan. Artifact/cache lifecycle, active-audit batch lifecycle, default legacy `cachePath`, and local artifact helper cleanup are complete. | Next pressure is legacy TTL/default cleanup and making the harmless audit flag cache-neutral without removing config compatibility. |
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

From [`scripts/complexity_baseline.json`](../../scripts/complexity_baseline.json), current source-discovery C901 offenders are cleared. The next pressure is lifecycle ownership rather than cyclomatic-complexity suppression.

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

Seed-careers and web-search now use the shared audit artifact as the runtime path when their stages are enabled. Further work should shrink report, browser-recovery, and scan orchestration complexity without changing queue semantics.

Acceptance criteria:

- `webSearch.activeAuditEnabled=false` remains harmless legacy input and is no longer a runtime direct-scan selector.
- Seed-careers and web-search continue feeding candidates through the current queue, pending review, tombstone, and auto-approval path.
- Browser-recovery artifact behavior remains explicit and tested.
- Browser-recovery artifact loading/defaulting, fetch-failure recording, success-page analysis, probe-result validation, summary update, and persistence are now split into narrower helpers.
- Web-search C901/report complexity follow-ups remain deferred.
- Deferred browser-recovery cleanup: web-search and GameDevMap still need source-specific callbacks for rendered-page analysis, artifact bucket names, and prevalidated queue caps. Do not add a shared save/merge wrapper until it can delete more than callback plumbing.

### 5. GameDevMap Reset

GameDevMap now always uses the active-source audit path when enabled. Next slices should reduce active-source lifecycle ownership without changing default discovery, explicit dry-run maintenance, lost-recovery comparison, or browser recovery.

Acceptance criteria:

- Default discovery, dry-run audit, lost-recovery comparison, and explicit browser recovery keep their current invocation surfaces.
- `gamedevmap.activeAuditEnabled=false` remains harmless legacy input and is no longer a direct CSV/cache rollback selector.
- Artifact compatibility changes are documented and tested when needed.
- GameDevMap artifact/timing save/finalize wrappers now call shared `active_audit_runtime.py` helpers directly; keep the GameDevMap report summary and cache signature local until artifact compatibility is explicitly reviewed.
- GameDevMap batch-loop artifact merging, summary increments, timing, probe-result application, and progress writes now run through `active_audit_runtime.py`; the adapter keeps source-specific row preparation, provenance, homepage analysis, recovery paths, and rejection factories local.
- GameDevMap local `_as_list`, `_as_dict`, and `_safe_int` now delegate to shared active-audit helpers while preserving copy/default compatibility covered by targeted report tests.
- Deferred active-audit API cleanup: the shared batch strategy still needs adapter-provided wiring for GameDevMap-specific labels and artifact bucket names; consider a named factory only if another active-source adapter needs the same lifecycle shape.
- GameDevMap no longer ships default legacy `cachePath` or `cacheTtlMinutes`; external `cacheTtlMinutes` remains accepted as a temporary fallback for `activeAuditTtlMinutes` while legacy config compatibility is reviewed.
- Deferred web-derived lifecycle cleanup: a shared browser-recovery save/merge wrapper is not currently equivalent. Web-search persists direct directory-audit summary counts, while GameDevMap persists through active-audit completed URL summarization.

### 6. Test Scaffolding Cleanup

After deletion slices land, consolidate repeated adapter test setup without weakening guardrails.

Acceptance criteria:

- Test changes are readability/LOC improvements only.
- No production behavior changes in the same slice.
- Existing guardrails still prove deleted paths stay deleted.
- Shared empty directory-audit fixture setup lives in `tests/source_discovery/_helpers.py`.
- Shared web-search browser-recovery artifact setup lives in `tests/source_discovery/_helpers.py`.

### 7. Deferred Cleanup Sequence

Remaining same-goal cleanup should land as small compatibility-preserving slices:

- Remove any remaining GameDevMap test/doc wording that treats legacy `cachePath` as a meaningful discovery input.
- Continue removing local GameDevMap helper wrappers only when copy/default compatibility can be preserved without weakening report/artifact behavior.
- Browser-recovery artifact save/merge sharing remains deferred until web-search and GameDevMap persistence semantics converge enough to delete real lifecycle code instead of adding callback-only indirection.
- Keep `activeAuditEnabled=false` accepted as harmless legacy input until all source-discovery config compatibility is reviewed together.

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

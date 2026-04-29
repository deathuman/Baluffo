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
| Shared audit runner | [`DirectoryAuditRunSpec`](../../src/source_discovery/directory_audit.py) and `run_directory_audit_spec` already exist; current runtime rollback branches, web direct-scan exports, and the now-unused generic direct-scan helper have been deleted. | Keep the remaining work focused on thin adapter-owned lifecycle around the existing runner. |
| Gameprog | Completed deletion proof. [`discover_gameprog_candidates`](../../src/source_discovery/gameprog.py) always returns rows from the audit artifact when enabled; legacy `activeAuditEnabled`, adapter `cachePath`, and legacy `cacheTtlMinutes` support is removed. | Keep as baseline guardrail; do not recreate legacy cache/direct branches. |
| Gamesmap | Public discovery returns rows from the shared directory audit artifact; legacy `activeAuditEnabled`, adapter `cachePath`, and legacy `cacheTtlMinutes` support is removed. Config/index-collection wrapper thinning is complete. Large scan/category/parser surfaces remain. | Defer larger scan/category cleanup until it can delete lifecycle code without hiding Gamesmap category and parser semantics. |
| Sheet-directory | Public discovery returns rows from the shared directory audit artifact; legacy `activeAuditEnabled` support is removed. Config/recovery request wrapper thinning is complete. It still owns CSV parsing, Sheet evidence, summary, and scan glue. | Defer larger scan/recovery plumbing cleanup until it can delete adapter lifecycle code without hiding Sheet-specific evidence semantics. |
| Web-derived discovery | Enabled seed-careers and web-search runtime stages return shared audit-artifact rows; legacy `activeAuditEnabled` support is removed. Standalone direct-scan exports are retired. Pure web browser-recovery request, diagnostic, summary, row-selection, and persistence-update wrappers are thinned. | Shared browser-recovery save/merge remains deferred because web-search and GameDevMap persistence semantics are not equivalent. |
| GameDevMap | Uses the separate active-source audit engine through [`gamedevmap.py`](../../src/source_discovery/gamedevmap.py) and `gamedevmap_active_dry_run.py`. Artifact/cache lifecycle, active-audit batch lifecycle, default legacy `cachePath` / `cacheTtlMinutes`, local artifact helper cleanup, remaining `activeAuditEnabled` acceptance, and external `cacheTtlMinutes` fallback cleanup are complete. | Keep `activeAuditPath` and `activeAuditTtlMinutes` as the supported artifact controls; defer only larger active-audit lifecycle API cleanup. |
| Stage wiring | [`orchestrator_generation.py`](../../src/source_discovery/orchestrator_generation.py) owns stage invocation and compatibility with current discovery flows. | Treat route changes as compatibility work and preserve task-start, busy-state, queue, pending review, and report behavior. |

## Protected Surfaces

- Saved jobs and local user data.
- Current frontend/local storage behavior for saved/local user sections.
- Current UI/runtime invocation paths that start discovery and fetch flows.
- Bridge/API contracts needed by the current UI/runtime.
- Queue, pending review, tombstone, static suppression, and admin auto-approval behavior when candidates enter the current product flow.

## Removable Surfaces

- Legacy direct discovery paths that duplicate the shared audit path.
- Legacy source-discovery config compatibility for `activeAuditEnabled`, adapter-owned `cachePath`, and legacy `cacheTtlMinutes`.
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

## Completed Baseline

- Legacy `activeAuditEnabled`, adapter-owned `cachePath`, and legacy `cacheTtlMinutes` are removed from source-discovery runtime and tests. Remaining references in docs describe that removal.
- Enabled Gameprog, Gamesmap, Sheet-directory, Web-derived, and GameDevMap runtime paths use audit-artifact rows.
- Sheet-directory config/recovery request wrapper thinning is complete; CSV parsing and Sheet evidence semantics remain local.
- Gamesmap config/index-collection wrapper thinning is complete; category matching, provenance, detail/index parsing, and homepage selection semantics remain local.
- Web-derived direct scanner exports and the unused generic direct-scan helper are retired; enabled seed-careers and web-search stages use the audit-artifact runtime path.
- Web-derived browser-recovery wrapper thinning is complete for request creation, shared diagnostics, summary counting, candidate row selection, and artifact summary persistence; rendered-page analysis and queue-cap semantics remain local.
- Modern artifact controls remain: `activeAuditPath`, `activeAuditTtlMinutes`, `activeAuditRecoveryEnabled`, `activeAuditRecoveryUrlLimit`, and browser-recovery settings.
- From [`scripts/complexity_baseline.json`](../../scripts/complexity_baseline.json), current source-discovery C901 offenders are cleared. The next pressure is lifecycle ownership rather than cyclomatic-complexity suppression.

Parser complexity can remain temporarily when it is genuinely source-format complexity. Orchestration complexity should not.

## Remaining Migration Sequence

### 1. Web-Derived Browser-Recovery Save/Merge Review

Review browser-recovery save/merge and report complexity without changing queue semantics. Enabled runtime stages already use the shared audit artifact, standalone direct-scan exports are retired, and pure local wrapper thinning is complete.

Acceptance criteria:

- Seed-careers and web-search continue feeding candidates through the current queue, pending review, tombstone, and auto-approval path.
- Browser-recovery artifact behavior remains explicit and tested.
- Browser-recovery artifact loading/defaulting, fetch-failure recording, success-page analysis, probe-result validation, summary update, and persistence stay explicit unless a future pass can delete more source-specific plumbing than it adds.
- Web-search C901/report complexity follow-ups remain deferred.
- Deferred browser-recovery cleanup: web-search and GameDevMap still need source-specific callbacks for rendered-page analysis, artifact bucket names, and prevalidated queue caps. Do not add a shared save/merge wrapper until it can delete more than callback plumbing.

### 2. GameDevMap Reset

Reduce active-source lifecycle ownership without changing default discovery, explicit dry-run maintenance, lost-recovery comparison, or browser recovery.

Acceptance criteria:

- Default discovery, dry-run audit, lost-recovery comparison, and explicit browser recovery keep their current invocation surfaces.
- Enabled GameDevMap discovery always uses the active-source audit path.
- Artifact compatibility changes are documented and tested when needed.
- GameDevMap artifact/timing save/finalize wrappers now call shared `active_audit_runtime.py` helpers directly; keep the GameDevMap report summary and cache signature local until artifact compatibility is explicitly reviewed.
- GameDevMap batch-loop artifact merging, summary increments, timing, probe-result application, and progress writes now run through `active_audit_runtime.py`; the adapter keeps source-specific row preparation, provenance, homepage analysis, recovery paths, and rejection factories local.
- GameDevMap local `_as_list`, `_as_dict`, and `_safe_int` now delegate to shared active-audit helpers while preserving copy/default compatibility covered by targeted report tests.
- Deferred active-audit API cleanup: the shared batch strategy still needs adapter-provided wiring for GameDevMap-specific labels and artifact bucket names; consider a named factory only if another active-source adapter needs the same lifecycle shape.
- GameDevMap no longer ships or accepts legacy `cachePath`, legacy `cacheTtlMinutes`, or `activeAuditEnabled` as source-discovery behavior controls.
- Deferred web-derived lifecycle cleanup: a shared browser-recovery save/merge wrapper is not currently equivalent. Web-search persists direct directory-audit summary counts, while GameDevMap persists through active-audit completed URL summarization.

### 3. Test Scaffolding Cleanup

Consolidate repeated adapter test setup without weakening guardrails. Shared browser-recovery artifact setup and minimal directory-audit result helpers already exist in `tests/source_discovery/_helpers.py`; broader empty audit fixture consolidation remains.

Acceptance criteria:

- Test changes are readability/LOC improvements only.
- No production behavior changes in the same slice.
- Existing guardrails still prove deleted paths stay deleted.
- Shared empty directory-audit fixture setup lives in `tests/source_discovery/_helpers.py`.
- Shared web-search browser-recovery artifact setup lives in `tests/source_discovery/_helpers.py`.

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

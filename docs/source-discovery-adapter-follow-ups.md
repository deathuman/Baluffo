# Source Discovery Adapter Follow-Ups

> - **Status:** Active
> - **Use this when:** planning source-discovery adapter reuse, deduplication, and yield improvements
> - **Canonical for:** follow-up opportunities only; not current implementation commitments
> - **Not canonical for:** current discovery behavior, data contracts, or verification commands
> - **Then inspect:** [`scraping-pipeline.md`](scraping-pipeline.md), [`architecture-ai-map.md`](architecture-ai-map.md), and the owning adapter modules
> - **Last updated:** 2026-04-27

The discovery adapters now have enough shared infrastructure to stop treating reuse as opportunistic cleanup. The next work should aggressively migrate adapters onto shared primitives, delete duplicate inline logic, and document the rare cases where source-specific behavior must stay local.

## Operating Principle

- Default to shared primitives. Keep logic local only when source-specific semantics materially differ, and name the exception.
- A new helper is incomplete unless it removes or thins adapter-owned code in the same slice, or records the exact adopter that will be migrated next.
- Shared helpers should expose stable public-internal APIs and strategy callbacks. Do not migrate adapters by importing another module's private `_helper` implementation.
- Better implementations should be lifted into shared modules and backfilled across adapters, with tests proving behavior and rollback paths.
- Audit artifacts remain operational ledgers. They must not bypass queueing, pending review, tombstones, static suppression, or admin auto-approval.

## Reusable Platform Already Built

- **Audit and reporting:** `audit_ledger`, `audit_config`, `audit_report_summary`, and `directory_audit` own freshness, signatures, timing totals, failure aggregation, artifact size stamping, audit execution, report summaries, and scan-row extraction.
- **Fetch, cache, and scan seams:** `directory_cache`, `directory_fetch_jobs`, `directory_fetch`, shared directory adapter wrappers, and scan-stage orchestration own common cache shape, fetch-job shape, cache-compatible discovery wrappers, and provider/static/failure stage plumbing.
- **Candidate, probe, and queue mechanics:** `candidate_collections`, `probe_runtime`, and `prevalidated_queue_policy` own provider/static row normalization, append-with-dedupe, bounded async probing, rendered static shortcuts, and internal queue-cap override fields.
- **Page and recovery classification:** `page_outcomes`, `page_diagnostics`, `recovery_url_planner`, `directory_page_recovery`, and `provider_inference_filters` own provider-first page classification, explicit/static fallback ordering, JS-shell diagnostics, same-site recovery planning, HTTP-only recovery fetches, and bad provider inference filtering.
- **Browser recovery:** `browser_recovery` owns processed-key selection, bounded browser fetch, rendered-result probe filtering, probe dispatch, and recovery state/sample bookkeeping. GameDevMap and web-derived discovery now share the runtime while keeping artifact merge semantics local.
- **Default audit adoption:** Gameprog, Gamesmap, sheet-directory, seed-careers, and web-search use default audit paths when their stages are enabled. GameDevMap uses its audit/recovery path as the canonical adapter behavior while keeping the active-source artifact schema local.

## Current Duplication Inventory

### P0: Gameprog/Gamesmap Twins

These are the lowest-risk, highest-certainty consolidation targets because the implementations are structurally parallel but still live in separate adapter modules.

- `gameprog.py::build_gameprog_static_candidate` and `gamesmap_candidates.py::build_gamesmap_static_candidate` duplicate static careers/website-only row construction with adapter-specific evidence fields. Extract a shared directory static candidate builder or strategy-backed `page_outcomes` callback.
- `gameprog.py::_apply_gameprog_static_page_provenance`, `gamesmap_candidates.py::_apply_gamesmap_provider_provenance`, and `gamesmap_candidates.py::_apply_gamesmap_static_provenance` duplicate provenance tagging. Extract a shared directory provenance callback contract.
- `gameprog.py::_empty_gameprog_scan_result` and `gamesmap_candidates.py::_empty_gamesmap_scan_result` duplicate empty audit scan templates. Move the template into `directory_audit`.
- `gameprog.py::_gameprog_scan` and `gamesmap_candidates.py::_gamesmap_scan` duplicate the same scan control-flow shape: fetch directory entries, cap/select rows, build fetch jobs, analyze pages, recover misses, merge candidates/failures, and emit summary/progress fields. Consolidate the common skeleton while keeping parser and provenance callbacks local.

### P1: GameDevMap Still Reimplements Shared Primitives

GameDevMap created much of the better logic but still has the largest local active-audit surface. It should be decomposed into reusable strategy contracts instead of remaining a special case.

- `gamedevmap_active_dry_run.py::_looks_like_js_shell` overlaps `page_diagnostics.looks_like_js_shell`.
- `gamedevmap_active_dry_run.py::_no_careers_reason_detail` overlaps `directory_page_recovery.no_candidate_reason_detail`.
- `gamedevmap_active_dry_run.py::_provider_candidates_from_html_text` should use a shared provider inference helper rather than local regex extraction.
- `gamedevmap_active_dry_run.py::_append_analyzed_candidates` and `_static_candidate_from_analysis` overlap `page_outcomes.classify_fetched_page`.
- `gamedevmap_active_dry_run.py::_recovery_job` overlaps `directory_fetch_jobs.build_directory_fetch_job`.
- `gamedevmap_active_dry_run.py::_queue_no_careers_recovery`, `_dedupe_recovery_jobs`, `_fetch_recovery_jobs`, and `_apply_recovery_results` overlap `directory_page_recovery` mechanics. Expose stable public-internal recovery APIs before migrating; do not import private helper functions directly.
- `gamedevmap_active_dry_run.py::_filter_bad_provider_inferences` overlaps `provider_inference_filters.split_bad_provider_inferences`, with GameDevMap-specific rejection row formatting still local.
- `gamedevmap_active_dry_run.py::_apply_probe_results` overlaps `probe_runtime` probe-result filtering and validated candidate evidence helpers, with active/zero/rejected bucket assignment still local.
- `gamedevmap_active_dry_run.py::_analyze_browser_recovery_fetches` and `_merge_browser_recovery_artifact_updates` overlap the web-derived browser recovery pipeline. Extract shared browser recovery analysis and merge contracts while preserving GameDevMap's artifact fields.
- `gamedevmap_active_dry_run.py::_default_browser_fetcher` is duplicated in web-derived discovery and should move to a shared browser fetch helper.
- `gamedevmap.py` legacy homepage fetch job construction should use `directory_fetch_jobs` where the job contract matches.

### P1: Web-Derived Browser Recovery Duplication

The runtime is shared, but web-derived discovery still owns analysis and merge code that mirrors GameDevMap.

- `web_search_candidates.py::_provider_candidate_base` and `_provider_candidate` hardcode provider URL parsing that overlaps `infer_web_candidate` and candidate-collapsing helpers. Migrate provider inference and competing-candidate collapsing to shared helpers.
- `web_search_candidates.py::_append_page_analysis_outcome` builds local `FetchedPageContext` callbacks before calling `classify_fetched_page`. Supply those callbacks through a shared page-outcome strategy contract instead.
- `web_search_candidates.py::_web_browser_recovery_candidate` should move to a shared browser-recovery row factory or use the directory recovery candidate factory when the row contract matches.
- `web_search_candidates.py::_analyze_web_browser_recovery_fetches` should migrate to a shared analysis contract that accepts adapter-specific page-analysis and candidate-marking callbacks.
- `web_search_candidates.py::_merge_web_browser_recovery_updates` should migrate to a shared merge contract for positive probe results, zero-job diagnostics, fetch failures, and processed-key state.
- `web_search_candidates.py::_default_browser_fetcher` should be replaced by the same shared browser fetch helper used by GameDevMap.

### P1: Sheet-Directory Manual Implementations

Sheet-directory now has a default audit path, but several small pieces remain manually assembled.

- `sheet_directory.py::_fetch_sheet_csv` implements multi-URL text fetch retry. This needs a shared multi-source text fetch helper, not `fetch_directory_pages`, because the contract is CSV/text selection rather than HTML page fetching.
- `sheet_directory.py::_append_sheet_entry_candidate` manually builds static candidates from sheet rows. Extract a shared static row builder where source-directory evidence and openings-flag evidence can be supplied by callbacks.
- `sheet_directory.py::_empty_sheet_scan_result` duplicates the empty scan template pattern and should use `directory_audit`.
- `sheet_directory.py::_sheet_directory_scan` should be thinned toward a shared scan skeleton once CSV fetch, entry selection, candidate building, and summary callbacks are isolated.

### P2: Prevalidated Queue Policy Adoption

- GameDevMap uses shared prevalidated queue-cap overrides for validated static sources.
- Web-derived browser recovery sets `prevalidatedDiscovery=True` for `jobsFound > 0` recovered candidates but does not apply `apply_prevalidated_queue_overrides`.
- Extending queue-cap overrides to web-derived or future probe-validated adapters is a behavior change, not a pure refactor. It should be tested against adapter/domain deferral behavior, pending/active promotion, tombstones, static suppression, and disabled auto-approval.

## Prioritized Reuse Roadmap

### P0: Consolidate Gameprog/Gamesmap Templates First

- Extract shared empty scan-result templates, directory static candidate construction, and provenance callback contracts.
- Thin `_gameprog_scan` and `_gamesmap_scan` to parser-specific entry selection plus shared scan orchestration callbacks.
- Keep directory-specific evidence fields local through callback data, not duplicated control flow.

### P1a: Backfill Existing Helpers Across All Adapters

- Replace remaining inline fetch-job, cache, candidate collection, probe batch, report summary, scan-row extraction, page outcome, and browser fetch helper code with existing shared modules.
- Every slice should name the exact adapter functions removed or reduced.
- If an existing helper is too private or too narrow, first expose a stable public-internal API, then migrate the adapter.

### P1b: Extract GameDevMap's Active-Audit Pipeline

- Split GameDevMap's active-source logic into strategy callbacks for page analysis, recovery planning/fetching, bad-provider rejection, probe classification, browser recovery analysis, and artifact merge.
- Preserve GameDevMap's artifact shape, `jobsFound > 0` active-candidate requirement, zero-job buckets, rejection rows, rerun modes, and browser-recovery CLI behavior.
- After extraction, use the shared contracts as the default implementation for future adapters that need active-source audits.

### P1c: Standardize Adapter Strategy Contracts

- Define shared strategy shapes for directory provenance, static fallback rows, no-candidate diagnostics, recovery requests, browser-recovery candidate rows, and audit merge results.
- Prefer callback-driven shared flow over adapter-owned branching.
- Keep strategy contracts narrow enough that source-specific evidence fields stay explicit and testable.

### P2: Expand HTTP Recovery

- Apply shared HTTP-only recovery to sheet-directory rows and web-derived seed pages when a source URL or homepage is available and the page produces no provider/static candidate.
- Document true semantic exceptions, such as rows that contain only a direct careers URL with no recoverable company homepage.
- Keep browser rendering opt-in and separate from default HTTP recovery.

### P2: Promote Browser-Recovery Eligibility

- Move browser-recovery candidate row construction and summary counting into a shared contract.
- Adopt it for GameDevMap, web-derived discovery, and any directory adapter that emits HTTP-only JS-shell or browser-recoverable fetch failures.
- Keep rendered recovery commands explicit and artifact-only until a separate plan changes default behavior.

### P2: Adopt Prevalidated Queue Overrides Deliberately

- Migrate every adapter that emits probe-validated `jobsFound > 0` candidates to the shared prevalidated queue policy, or document why normal queue caps should still apply.
- Treat each adoption as behavior-changing and test queued/deferred output, active/pending movement, disabled auto-approval, and internal override-field stripping.

### P3: Evidence-Led Tuning And Legacy Cleanup

- Use audit summaries to rank adapters by no-candidate count, JS-shell count, browser-recoverable fetch failures, profile-host misses, timeout/429 rate, zero-job probes, and recovered-candidate yield.
- Remove or quarantine legacy direct paths only after shared defaults are proven equivalent and rollback tests exist.
- Prefer deleting adapter wrappers that simply delegate to shared helpers over adding another compatibility layer.

## Lean-ness Targets

- No duplicate `_default_browser_fetcher`.
- No duplicate empty scan-result constructors.
- No adapter-local multi-URL retry once a shared text-fetch helper exists.
- No adapter-local provider -> explicit careers -> generic static ordering when `page_outcomes` can own it.
- No bounded probe batch outside `probe_runtime`.
- No adapter-owned provider/static append-and-dedupe when `candidate_collections` can own it.
- No audit artifact freshness, timing, size, or failure aggregation outside `audit_ledger` or `directory_audit`.
- Future refactors must state which duplicated functions are deleted, thinned, or migrated.

## Verification Standard

- Documentation-only roadmap updates should run `npm run lint:precommit`.
- Code dedupe slices should run a targeted adapter lane first, then `python -m pytest -q tests/source_discovery`, then `npm run lint:precommit`.
- Behavior-changing migrations, including queue-cap override adoption or new recovery coverage, must include before/after tests for output rows, deferrals, registry movement, report metadata, and rollback paths.

## Guardrails

- Do not bypass the normal discovery queue, pending registry, tombstone, static suppression, or auto-approval flow.
- Preserve public data contracts, cache shapes, artifact shapes, CLI flags, report fields, and rollback paths unless a future plan explicitly changes them.
- Treat audit artifacts as operational ledgers and report metadata, not active-source registries.
- Prefer targeted tests around the adopting adapter before adding broad utility tests.

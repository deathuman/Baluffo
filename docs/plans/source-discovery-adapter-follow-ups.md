# Source Discovery Adapter Follow-Ups

> - **Status:** Active
> - **Use this when:** planning source-discovery adapter reuse, deduplication, and yield improvements
> - **Canonical for:** follow-up opportunities only; not current implementation commitments
> - **Not canonical for:** current discovery behavior, data contracts, or verification commands
> - **Then inspect:** [`scraping-pipeline.md`](../scraping-pipeline.md), [`architecture-ai-map.md`](../architecture-ai-map.md), and the owning adapter modules
> - **Last updated:** 2026-04-28

The discovery adapters now have enough shared infrastructure to stop treating reuse as opportunistic cleanup. The next work should aggressively migrate adapters onto shared primitives, delete duplicate inline logic, and document the rare cases where source-specific behavior must stay local.

## Operating Principle

- Default to shared primitives. Keep logic local only when source-specific semantics materially differ, and name the exception.
- A new helper is incomplete unless it removes or thins adapter-owned code in the same slice, or records the exact adopter that will be migrated next.
- Shared helpers should expose stable public-internal APIs and strategy callbacks. Do not migrate adapters by importing another module's private `_helper` implementation.
- Better implementations should be lifted into shared modules and backfilled across adapters, with tests proving behavior and rollback paths.
- Audit artifacts remain operational ledgers. They must not bypass queueing, pending review, tombstones, static suppression, or admin auto-approval.

## Reusable Platform Already Built

- **Audit and reporting:** `audit_ledger`, `audit_config`, `audit_report_summary`, and `directory_audit` own freshness, signatures, timing totals, failure aggregation, artifact size stamping, audit execution, report summaries, scan-row extraction, and `DirectoryAuditRunSpec` runner assembly.
- **Fetch, cache, and scan seams:** `directory_cache`, `directory_fetch_jobs`, `directory_fetch`, `directory_adapter_templates`, shared directory adapter wrappers, and scan-stage orchestration own common cache shape, fetch-job shape, static/provenance row templates, cache-compatible discovery wrappers, and provider/static/failure stage plumbing.
- **Candidate, probe, and queue mechanics:** `candidate_collections`, `probe_runtime`, and `prevalidated_queue_policy` own provider/static row normalization, append-with-dedupe, bounded async probing, rendered static shortcuts, and internal queue-cap override fields.
- **Page and recovery classification:** `page_outcomes`, `page_diagnostics`, `recovery_url_planner`, `directory_page_recovery`, and `provider_inference_filters` own provider-first page classification, explicit/static fallback ordering, JS-shell diagnostics, same-site recovery planning, HTTP-only recovery fetches, and bad provider inference filtering.
- **Browser recovery:** `browser_recovery` owns default browser fetch fallback, processed-key selection, bounded browser fetch, rendered-result probe filtering, probe dispatch, and recovery state/sample bookkeeping. GameDevMap and web-derived discovery now share the runtime while keeping artifact merge semantics local.
- **Default audit adoption:** Gameprog, Gamesmap, sheet-directory, seed-careers, and web-search use default audit paths when their stages are enabled. GameDevMap uses its audit/recovery path as the canonical adapter behavior while keeping the active-source artifact schema local.

## Current Duplication Inventory

### P0: Gameprog/Gamesmap Twins

These are the lowest-risk, highest-certainty consolidation targets because the implementations are structurally parallel but still live in separate adapter modules.

- Completed first slice: `gameprog.py::build_gameprog_static_candidate` and `gamesmap_candidates.py::build_gamesmap_static_candidate` now use shared directory static row templates.
- Completed first slice: `gameprog.py::_apply_gameprog_static_page_provenance`, `gamesmap_candidates.py::_apply_gamesmap_provider_provenance`, and `gamesmap_candidates.py::_apply_gamesmap_static_provenance` now use shared provenance enrichment templates.
- Completed first slice: `gameprog.py::_empty_gameprog_scan_result` and `gamesmap_candidates.py::_empty_gamesmap_scan_result` were replaced by direct shared empty scan-result template calls.
- Completed second slice: `gameprog.py::_gameprog_scan` and `gamesmap_candidates.py::_gamesmap_scan` now share the post-selection website fetch, page analysis, recovery, fallback, dedupe, summary, and progress skeleton. Parser/index/category selection remains adapter-owned.
- Completed recovery-contract slice: Gameprog/Gamesmap website scans now use shared recovery summary/application contracts for fallback suppression, recovered rows, browser candidates, and timing merge.
- Completed scan-setup slice: Gameprog/Gamesmap now use a shared website-scan setup wrapper for common fetch-concurrency resolution, recovery-budget handoff, and `run_directory_website_scan(...)` call assembly.
- Completed entry-selection slice: Gameprog/Gamesmap now share parsed-entry empty handling, selection callback timing, selection log handoff, selected summary merge, and website-scan dispatch.
- Completed recovery-budget slice: `gameprog.activeAuditRecoveryUrlLimit` and `gamesmap.activeAuditRecoveryUrlLimit` default to `6`, fall back to `6` for invalid/non-positive values, and participate in audit signatures.

### P1: GameDevMap Still Reimplements Shared Primitives

GameDevMap created much of the better logic but still has the largest local active-audit surface. It should be decomposed into reusable strategy contracts instead of remaining a special case.

- Completed diagnostics slice: `gamedevmap_active_dry_run.py::_looks_like_js_shell` and `_no_careers_reason_detail` now delegate to shared page diagnostics while preserving GameDevMap reason buckets.
- Completed provider/page-outcome slice: `gamedevmap_active_dry_run.py::_provider_candidates_from_html_text` now uses shared provider HTML inference instead of local provider URL extraction.
- Completed provider/page-outcome slice: `gamedevmap_active_dry_run.py::_append_analyzed_candidates` now uses `page_outcomes.classify_fetched_page`; `_static_candidate_from_analysis` was removed.
- Completed page-strategy slice: GameDevMap, Gameprog, Gamesmap, sheet-directory recovery, and web-derived page analysis now use `PageOutcomeStrategy` wiring for provider/static/recovery callbacks.
- Completed recovery-fetch slice: `gamedevmap_active_dry_run.py::_recovery_job`, `_dedupe_recovery_jobs`, `_requests_from_recovery_result`, and `_recovery_cache_result` now delegate to stable `directory_page_recovery` recovery fetch/fanout/cache helpers; `_fetch_recovery_jobs` was pruned in favor of direct shared helper calls.
- Completed recovery-planning slice: `gamedevmap_active_dry_run.py::_queue_no_careers_recovery` now uses shared recovery URL/job wave planning while keeping GameDevMap provider extraction and browser row creation local.
- Completed recovery-result slice: `gamedevmap_active_dry_run.py::_apply_recovery_results` now uses shared recovery-result application for fanout iteration, failure routing, fetched counts, grouped-state threading, and finalization callbacks.
- Completed homepage-runtime slice: `gamedevmap_active_dry_run.py::_extract_candidates_from_homepages` now uses shared active homepage batch runtime for direct provider inference, homepage fetch result routing, no-candidate recovery queueing, browser candidate collection, and fetched counts.
- Completed recovery-fetch slice: `gamedevmap_active_dry_run.py::_filter_bad_provider_inferences` now uses `provider_inference_filters.split_bad_provider_inferences`, with GameDevMap-specific rejection row formatting still local.
- Completed probe-classification slice: `gamedevmap_active_dry_run.py::_apply_probe_results` now uses shared `probe_runtime` probe-result classification and validated candidate evidence helpers, with active/zero/rejected artifact bucket writes still local.
- Completed artifact-state slice: GameDevMap active audit now uses shared active-audit helpers for unique candidate merge, source-identity bucket merge, rejection/failure append, batch timing, and summary count extraction while keeping GameDevMap bucket names local.
- Completed lifecycle slice: GameDevMap active audit artifact initialization, resume/reset refresh, progress finalization, timestamp stamping, and atomic save now use shared active-audit lifecycle helpers.
- Completed rerun-state slice: GameDevMap rerun row selection, rejection pruning, rejection identity indexing, recovered-active mapping, and lost-recovery comparison iteration now use shared active-audit state helpers.
- Completed cache-wrapper slice: GameDevMap active audit cache reuse, signature/freshness checks, rerun/reset bypass, and refresh dispatch now use a shared active-audit cache wrapper.
- Completed candidate-export slice: GameDevMap validated active candidates now use shared active-audit export filtering, validation metadata stamping, provider/static split, static transform, and dedupe helpers.
- Completed batch-runtime slice: GameDevMap active audit one-batch execution now uses shared sequencing for homepage fetch, recovery waves, candidate merge/probe dispatch, timing, progress, and completed-URL updates.
- Completed run-loop slice: GameDevMap active audit outer loop now uses shared batch selection, cursor progress, max-batch stopping, completion writes, and per-batch write sequencing.
- Completed strategy-contract slice: GameDevMap active audit now wires shared batch and loop runtime through compact strategy dataclasses instead of long ad hoc callback lists.
- Completed browser-analysis slice: `gamedevmap_active_dry_run.py::_analyze_browser_recovery_fetches` now uses shared rendered-fetch analysis.
- Completed browser-merge slice: `gamedevmap_active_dry_run.py::_merge_browser_recovery_artifact_updates` and web-derived browser recovery now share merge orchestration, while adapter-specific artifact writes remain local.
- Completed diagnostics slice: `gamedevmap_active_dry_run.py` now calls the shared browser recovery fetch fallback directly; the private `_default_browser_fetcher` wrapper was pruned.
- Completed wrapper-pruning slice: GameDevMap now calls shared merge, recovery-fetch, browser-fetch, and audit freshness/signature helpers directly where private wrappers added no compatibility value.
- Completed recovery-planning slice: `gamedevmap.py` legacy homepage fetch job construction now uses `directory_fetch_jobs`.

### P1: Web-Derived Browser Recovery Duplication

The runtime is shared, but web-derived discovery still owns analysis and merge code that mirrors GameDevMap.

- Completed provider-inference slice: `web_search_candidates.py::_provider_candidate_base`, `_provider_candidate`, and provider adapter detection now live in shared `provider_inference`, while `infer_web_candidate(...)` remains the public compatibility wrapper.
- Completed provider/page-outcome slice: `web_search_candidates.py::_append_page_analysis_outcome` now uses shared static page-outcome callback builders instead of local callback boilerplate.
- Completed browser-row slice: `web_search_candidates.py::_web_browser_recovery_candidate`, GameDevMap JS-shell browser rows, and directory page recovery browser rows now share one browser-recovery row factory.
- Completed browser-analysis slice: `web_search_candidates.py::_analyze_web_browser_recovery_fetches` now uses shared rendered-fetch analysis with adapter-specific page-analysis and candidate-marking callbacks.
- Completed merge-policy slice: `web_search_candidates.py::_merge_web_browser_recovery_updates` and GameDevMap browser-recovery merge state now share probe-result filtering, active counting, and runtime state helpers. Web-derived positive recovered candidates now adopt shared prevalidated queue overrides.
- Completed recovery-contract slice: `web_search_candidates.py::_run_web_http_recovery` now uses shared recovery application/timing-remap helpers, and browser recovery reason summaries are counted by `browser_recovery.browser_recovery_summary`.
- Completed page-stage slice: seed-careers and web-search scanners now share page-job fetch, page-result routing, HTTP recovery, browser summary, candidate dedupe, timing, and completed-URL mechanics.
- Completed diagnostics slice: `web_search_candidates.py` now calls the shared browser recovery fetch fallback directly; the private `_default_browser_fetcher` wrapper was pruned.
- Completed default-recovery slice: web-derived audits run shared HTTP-only same-site recovery by default; `webSearch.activeAuditRecoveryEnabled=false` remains the rollback.
- Completed recovery-budget slice: `webSearch.activeAuditRecoveryUrlLimit` defaults to `6`, falls back to `6` for invalid/non-positive values, and participates in audit signatures.
- Evidence snapshot: [`source-discovery-http-recovery-evidence-2026-04-27.md`](../snapshots/source-discovery-http-recovery-evidence-2026-04-27.md) supports default-enabling both sheet-directory and web-derived HTTP recovery.

### P1: Directory Audit Runner Assembly

- Completed audit-spec slice: Gameprog, Gamesmap, sheet-directory, and web-derived audit runners now build `DirectoryAuditRunSpec` objects and call shared audit assembly instead of repeating long `run_directory_audit(...)` argument lists.
- Remaining local audit code should be source-specific scan setup, signatures, runtime metadata values, and rollback seams.

### P1: Sheet-Directory Manual Implementations

Sheet-directory now has a default audit path. Its low-level fetch and row templates are shared; the remaining duplication is scan orchestration.

- Completed sheet-foundation slice: `sheet_directory.py::_fetch_sheet_csv` now delegates to a shared multi-source text fetch helper.
- Completed sheet-foundation slice: `sheet_directory.py::_append_sheet_entry_candidate` now uses a shared provider-or-static directory entry builder while preserving sheet evidence and provider-first ordering.
- Completed sheet-foundation slice: `sheet_directory.py::_empty_sheet_scan_result` now uses a shared minimal empty scan-result payload helper.
- Completed sheet-index slice: `sheet_directory.py::_sheet_directory_scan` now uses shared direct-entry index scan runtime for parse/failure/candidate/dedupe/progress mechanics while keeping sheet-specific selection and summaries local.
- Completed recovery-contract slice: `sheet_directory.py::_apply_sheet_directory_recovery` now delegates fallback suppression, recovered row merge, browser candidate passthrough, summary merge, and timing merge to shared recovery application helpers.
- Completed default-recovery slice: sheet-directory audits run shared HTTP-only same-site recovery by default; `sheetDirectory.activeAuditRecoveryEnabled=false` remains the rollback.
- Completed recovery-budget slice: `sheetDirectory.activeAuditRecoveryUrlLimit` defaults to `6`, falls back to `6` for invalid/non-positive values, and participates in audit signatures.

### P2: Prevalidated Queue Policy Adoption

- GameDevMap uses shared prevalidated queue-cap overrides for validated static sources.
- Completed merge-policy slice: web-derived browser recovery now applies `apply_prevalidated_queue_overrides` to `jobsFound > 0` recovered candidates.
- Extending queue-cap overrides to future probe-validated adapters is a behavior change, not a pure refactor. It should be tested against adapter/domain deferral behavior, pending/active promotion, tombstones, static suppression, and disabled auto-approval.

## Prioritized Reuse Roadmap

### P0: Continue Gameprog/Gamesmap Scan Skeleton Thinning

- Further thin `_gameprog_scan` and `_gamesmap_scan` only where parser/index/category setup can use shared callbacks without hiding adapter-specific evidence semantics.
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

- Page-outcome callback wiring now uses `PageOutcomeStrategy`; remaining strategy work should target broader scan/recovery assembly, not provider/static classification callback plumbing.
- Define shared strategy shapes for directory provenance, static fallback rows, no-candidate diagnostics, recovery requests, browser-recovery candidate rows, and audit merge results.
- Prefer callback-driven shared flow over adapter-owned branching.
- Keep strategy contracts narrow enough that source-specific evidence fields stay explicit and testable.

### P2: Tune Default HTTP Recovery

- Use sheet-directory and web-derived recovery audit evidence to tune recovery URL limits, skip rules, profile-host handling, and timing budgets. The first shared knob is `activeAuditRecoveryUrlLimit`, defaulting to `6` for Gameprog, Gamesmap, sheet-directory, and web-search.
- Document true semantic exceptions, such as rows that contain only a direct careers URL with no recoverable company homepage.
- Keep browser rendering opt-in and separate from default HTTP recovery.

### P2: Promote Browser-Recovery Eligibility

- Browser-recovery candidate row construction and reason summary counting are shared.
- Adopt it for GameDevMap, web-derived discovery, and any directory adapter that emits HTTP-only JS-shell or browser-recoverable fetch failures.
- Keep rendered recovery commands explicit and artifact-only until a separate plan changes default behavior.

### P2: Adopt Prevalidated Queue Overrides Deliberately

- GameDevMap and web-derived browser recovery now use the shared prevalidated queue policy for probe-validated `jobsFound > 0` candidates. Migrate future probe-validated adapters to the same policy, or document why normal queue caps should still apply.
- Treat each adoption as behavior-changing and test queued/deferred output, active/pending movement, disabled auto-approval, and internal override-field stripping.

### P3: Evidence-Led Tuning And Legacy Cleanup

- Use audit summaries to rank adapters by no-candidate count, JS-shell count, browser-recoverable fetch failures, profile-host misses, timeout/429 rate, zero-job probes, and recovered-candidate yield.
- Remove or quarantine legacy direct paths only after shared defaults are proven equivalent and rollback tests exist.
- Prefer deleting adapter wrappers that simply delegate to shared helpers over adding another compatibility layer.

## Lean-ness Targets

- No duplicate `_default_browser_fetcher`.
- No duplicate empty scan-result constructors.
- No adapter-local multi-URL retry once `multi_source_text` can own the selection contract.
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

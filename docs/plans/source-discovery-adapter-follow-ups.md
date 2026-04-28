# Source Discovery Adapter Follow-Ups

> - **Status:** Active roadmap, mostly completed infrastructure extraction
> - **Use this when:** planning source-discovery adapter reuse, deduplication, and yield improvements
> - **Canonical for:** follow-up opportunities, migration status, and refactor guardrails
> - **Not canonical for:** current discovery behavior, data contracts, or verification commands
> - **Then inspect:** [`scraping-pipeline.md`](../scraping-pipeline.md), [`architecture-ai-map.md`](../architecture-ai-map.md), and the owning adapter modules
> - **Last updated:** 2026-04-28

The discovery adapter reuse effort has moved from "build the shared primitives" to "finish the remaining safe migrations without changing runtime behavior." Most high-duplication paths now have shared contracts. The remaining work is split between small safe refactors, strategy-contract cleanup, and deliberately behavior-changing tuning that should not be mixed into cleanup slices.

## Current Status Dashboard

| Area | Status | Readout |
| --- | --- | --- |
| P0 Gameprog/Gamesmap twins | Complete for behavior-preserving extraction | Shared row templates, provenance, website scan skeleton, recovery contracts, scan setup, entry selection, recovery budgets, and Gamesmap index collection are in place. Remaining parser/index/category differences are source semantics, not shared skeleton backlog. |
| P1a helper backfill | Complete for the current exhaustive sweep | Current mechanical helper backfills are shared across fetch/cache/probe/page/recovery/browser/audit runner paths. Future helper ideas should be filed as new scoped slices, not as open-ended P1a debt. |
| P1b GameDevMap active-audit extraction | Largely complete | Lifecycle, cache, rerun state, homepage runtime, batch runtime, loop runtime, recovery contracts, probe classification, artifact updates, strategy assembly, and browser-recovery assembly are shared. Remaining work should focus on semantic strategy shape cleanup, not another broad extraction. |
| P1c strategy contract normalization | In progress | `PageOutcomeStrategy`, active-audit strategies, browser-recovery assembly, and shared recovery request/result payload contracts exist. Broader contracts for provenance and audit merge result shapes are still future cleanup. |
| P2 recovery/browser/queue behavior | Mostly future work | HTTP recovery is shared and defaulted where already proven. Browser eligibility expansion and queue override adoption remain behavior-changing and need explicit before/after coverage. |
| P3 evidence-led tuning and legacy cleanup | Future work | Use audit evidence to tune recovery budgets, skip rules, browser coverage, and old wrapper deletion after shared behavior is proven. |

## What This Means Operationally

- The big scary duplication problem is mostly behind us: GameDevMap is no longer a one-off active-audit island.
- The document should now guide smaller, safer slices instead of inviting another open-ended extraction marathon.
- Future commits should avoid adding more "Completed slice" bullets to the roadmap sections. Put history under **Completed Migration Log** and keep **Remaining Roadmap** focused on work that is still actionable.
- Behavior-preserving refactors and behavior-changing discovery improvements should stay separate. Queue, pending, tombstone, suppression, auto-approval, browser eligibility, and recovery coverage changes need their own explicit plans.

## Operating Principles

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
- **Active-audit runtime:** `active_audit_runtime` owns active-audit lifecycle, cache reuse, rerun selection, homepage batch sequencing, recovery application, probe/artifact bucket helpers, batch and loop runtimes, and strategy assembly.
- **Browser recovery:** `browser_recovery` owns default browser fetch fallback, processed-key selection, bounded browser fetch, rendered-result probe filtering, probe dispatch, recovery state/sample bookkeeping, and shared browser-recovery run/merge assembly.
- **Default audit adoption:** Gameprog, Gamesmap, sheet-directory, seed-careers, and web-search use default audit paths when their stages are enabled. GameDevMap uses its audit/recovery path as the canonical adapter behavior while keeping the active-source artifact schema local.

## Remaining Roadmap

### P1b: Finish GameDevMap Active-Audit Cleanup

**Type:** Safe refactor if behavior-preserving; high risk if artifact semantics change.

- Treat the major extraction as complete. Future GameDevMap work should be targeted cleanup around remaining local callback bodies.
- Preserve GameDevMap's artifact shape, `jobsFound > 0` active-candidate requirement, zero-job buckets, rejection rows, rerun modes, browser-recovery CLI behavior, cache signatures, and log text.
- Only extract further when the adapter-specific semantics remain explicit through callbacks such as rejection formatting, candidate IDs, provenance/evidence fields, and artifact writes.

### P1c: Standardize Adapter Strategy Contracts

**Type:** Safe refactor when callback-only; potentially behavior-changing if output shapes move.

- Page-outcome callback wiring now uses `PageOutcomeStrategy`; remaining strategy work should target broader scan/recovery assembly, not provider/static classification callback plumbing.
- Define shared strategy shapes for directory provenance, static fallback rows, no-candidate diagnostics, recovery requests, browser-recovery candidate rows, and audit merge results.
- Prefer callback-driven shared flow over adapter-owned branching.
- Keep strategy contracts narrow enough that source-specific evidence fields stay explicit and testable.

### P2: Tune Default HTTP Recovery

**Type:** Behavior-changing.

- Use sheet-directory and web-derived recovery audit evidence to tune recovery URL limits, skip rules, profile-host handling, and timing budgets.
- The first shared knob is `activeAuditRecoveryUrlLimit`, defaulting to `6` for Gameprog, Gamesmap, sheet-directory, and web-search.
- Document true semantic exceptions, such as rows that contain only a direct careers URL with no recoverable company homepage.
- Keep browser rendering opt-in and separate from default HTTP recovery.
- Evidence snapshot: [`source-discovery-http-recovery-evidence-2026-04-27.md`](../snapshots/source-discovery-http-recovery-evidence-2026-04-27.md) supports default-enabling both sheet-directory and web-derived HTTP recovery.

### P2: Promote Browser-Recovery Eligibility

**Type:** Behavior-changing.

- Browser-recovery candidate row construction, reason summary counting, run/probe assembly, and merge-state updates are shared.
- Adopt rendered browser recovery for any additional directory adapter only when it emits HTTP-only JS-shell or browser-recoverable fetch failures and has before/after artifact coverage.
- Keep rendered recovery commands explicit and artifact-only until a separate plan changes default behavior.

### P2: Adopt Prevalidated Queue Overrides Deliberately

**Type:** Behavior-changing.

- GameDevMap and web-derived browser recovery now use the shared prevalidated queue policy for probe-validated `jobsFound > 0` candidates.
- Migrate future probe-validated adapters to the same policy only with tests for queued/deferred output, active/pending movement, disabled auto-approval, static suppression, tombstones, and internal override-field stripping.
- If normal queue caps should still apply for an adapter, document that as an intentional exception.

### P3: Evidence-Led Tuning And Legacy Cleanup

**Type:** Evidence/tuning backlog.

- Use audit summaries to rank adapters by no-candidate count, JS-shell count, browser-recoverable fetch failures, profile-host misses, timeout/429 rate, zero-job probes, and recovered-candidate yield.
- Remove or quarantine legacy direct paths only after shared defaults are proven equivalent and rollback tests exist.
- Prefer deleting adapter wrappers that simply delegate to shared helpers over adding another compatibility layer.

## Completed Migration Log

### P0: Gameprog/Gamesmap Twins

- Completed first slice: `gameprog.py::build_gameprog_static_candidate` and `gamesmap_candidates.py::build_gamesmap_static_candidate` now use shared directory static row templates.
- Completed first slice: `gameprog.py::_apply_gameprog_static_page_provenance`, `gamesmap_candidates.py::_apply_gamesmap_provider_provenance`, and `gamesmap_candidates.py::_apply_gamesmap_static_provenance` now use shared provenance enrichment templates.
- Completed first slice: `gameprog.py::_empty_gameprog_scan_result` and `gamesmap_candidates.py::_empty_gamesmap_scan_result` were replaced by direct shared empty scan-result template calls.
- Completed second slice: `gameprog.py::_gameprog_scan` and `gamesmap_candidates.py::_gamesmap_scan` now share the post-selection website fetch, page analysis, recovery, fallback, dedupe, summary, and progress skeleton. Parser/index/category selection remains adapter-owned.
- Completed recovery-contract slice: Gameprog/Gamesmap website scans now use shared recovery summary/application contracts for fallback suppression, recovered rows, browser candidates, and timing merge.
- Completed scan-setup slice: Gameprog/Gamesmap now use a shared website-scan setup wrapper for common fetch-concurrency resolution, recovery-budget handoff, and `run_directory_website_scan(...)` call assembly.
- Completed entry-selection slice: Gameprog/Gamesmap now share parsed-entry empty handling, selection callback timing, selection log handoff, selected summary merge, and website-scan dispatch.
- Completed recovery-budget slice: `gameprog.activeAuditRecoveryUrlLimit` and `gamesmap.activeAuditRecoveryUrlLimit` default to `6`, fall back to `6` for invalid/non-positive values, and participate in audit signatures.
- Completed index-collection slice: Gamesmap index fetch, parse failure routing, `detailUrl` dedupe, `maxDetailPages` capping, and unresolved reference aggregation now use a shared directory index collection helper. Gameprog remains adapter-local because its upstream fetch is JSON-specific.
- Completed P0 closure slice: Gameprog/Gamesmap scan skeletons were rechecked after shared entry-selection and website-scan adoption; the remaining parser/index/category differences are intentional source semantics rather than shared control-flow backlog.

### P1: GameDevMap Active Audit

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
- Completed active-batch contract slice: GameDevMap active audit now uses shared helpers for batch artifact candidate/browser merges, failure recording, rejected-row appends, summary increments, probe classification application, and recovery fetch-result application while keeping GameDevMap evidence and bucket names local.
- Completed strategy-assembly slice: GameDevMap active audit now builds batch and loop strategy dataclasses through shared active-audit assembly helpers, keeping source-specific fetch, analysis, artifact write, and log callbacks local.
- Completed browser-analysis slice: `gamedevmap_active_dry_run.py::_analyze_browser_recovery_fetches` now uses shared rendered-fetch analysis.
- Completed browser-merge slice: `gamedevmap_active_dry_run.py::_merge_browser_recovery_artifact_updates` and web-derived browser recovery now share merge orchestration, while adapter-specific artifact writes remain local.
- Completed browser-assembly slice: GameDevMap and web-derived browser recovery now share processed-key selection, browser fetch/probe dispatch, merge-state counting, and recovery-state updates while preserving adapter-specific artifact/evidence merges.
- Completed diagnostics slice: `gamedevmap_active_dry_run.py` now calls the shared browser recovery fetch fallback directly; the private `_default_browser_fetcher` wrapper was pruned.
- Completed wrapper-pruning slice: GameDevMap now calls shared merge, recovery-fetch, browser-fetch, and audit freshness/signature helpers directly where private wrappers added no compatibility value.
- Completed recovery-planning slice: `gamedevmap.py` legacy homepage fetch job construction now uses `directory_fetch_jobs`.

### P1: Web-Derived Browser Recovery And Page Stages

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

### P1: Directory Audit Runner Assembly

- Completed audit-spec slice: Gameprog, Gamesmap, sheet-directory, and web-derived audit runners now build `DirectoryAuditRunSpec` objects and call shared audit assembly instead of repeating long `run_directory_audit(...)` argument lists.
- Remaining local audit code should be source-specific scan setup, signatures, runtime metadata values, and rollback seams.
- Completed cache/config backfill slice: Gameprog, GameDevMap legacy discovery, and Gamesmap cache wrappers now share directory cache path, TTL, explicit-cache, load, and write helpers while preserving filenames, signatures, and custom-fetcher cache behavior.
- Completed P1a closure slice: The current behavior-preserving helper sweep is complete; remaining roadmap entries are semantic strategy cleanup, behavior-changing recovery/browser/queue changes, or evidence-led tuning rather than P1a helper debt.
- Completed recovery-contract shape slice: Gameprog and Gamesmap now share directory recovery request construction, page-outcome scan payloads, and recovery-result strategy assembly while keeping provider/static evidence callbacks local.

### P1: Sheet-Directory Manual Implementations

- Completed sheet-foundation slice: `sheet_directory.py::_fetch_sheet_csv` now delegates to a shared multi-source text fetch helper.
- Completed sheet-foundation slice: `sheet_directory.py::_append_sheet_entry_candidate` now uses a shared provider-or-static directory entry builder while preserving sheet evidence and provider-first ordering.
- Completed sheet-foundation slice: `sheet_directory.py::_empty_sheet_scan_result` now uses a shared minimal empty scan-result payload helper.
- Completed sheet-index slice: `sheet_directory.py::_sheet_directory_scan` now uses shared direct-entry index scan runtime for parse/failure/candidate/dedupe/progress mechanics while keeping sheet-specific selection and summaries local.
- Completed recovery-contract slice: `sheet_directory.py::_apply_sheet_directory_recovery` now delegates fallback suppression, recovered row merge, browser candidate passthrough, summary merge, and timing merge to shared recovery application helpers.
- Completed default-recovery slice: sheet-directory audits run shared HTTP-only same-site recovery by default; `sheetDirectory.activeAuditRecoveryEnabled=false` remains the rollback.
- Completed recovery-budget slice: `sheetDirectory.activeAuditRecoveryUrlLimit` defaults to `6`, falls back to `6` for invalid/non-positive values, and participates in audit signatures.

### P2: Queue Policy Adoption Already Landed

- GameDevMap uses shared prevalidated queue-cap overrides for validated static sources.
- Completed merge-policy slice: web-derived browser recovery now applies `apply_prevalidated_queue_overrides` to `jobsFound > 0` recovered candidates.

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

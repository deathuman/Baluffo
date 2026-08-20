# Initial Refactoring Findings Plan

> - **Status:** Active plan, advisory-only
> - **Use this when:** doing jobs/fetcher refactoring, consolidation, dead-code triage, or validating refactorability analyzer findings from the 2026-05-17 analysis
> - **Canonical for:** the 2026-05-17 initial refactoring target inventory, known analyzer false positives, and suggested sequencing for behavior-preserving cleanup
> - **Not canonical for:** current runtime contracts, payload shapes, source registry policy, bridge route contracts, or implementation state after any later refactor lands
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`refactor-charter-template.md`](refactor-charter-template.md), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-08-19 — largest-files list and hotspot ranking refreshed from the 2026-08-19 `analyze_refactorability.py` run; items 5–7, 10, 13, 15 marked done this session; §10 gained the when-to-split decision rule (size + active-subsystem + low-seam-risk)

## Summary

This plan records the initial 7-pass refactoring analysis completed on 2026-05-17. It is a routing and prioritization aid, not blanket approval to remove code. Use it to choose narrow refactor slices, then revalidate each target against current source, tests, and the relevant contract docs before editing.

Highest-value jobs/fetcher targets:

- Remove two thin facades: `src/jobs/state.py` and `src/jobs/parsers.py` — **done**: both facades were removed in `f7376c87` (Phase 1); consumers now import leaf modules directly. The 1A/1B sections below remain as the historical inventory of that removal.
- Merge two compatibility helper modules into their owning facade: `src/jobs/fetcher_compat_exports.py` and `src/jobs/fetcher_compat_runtime.py`.
- Consolidate small duplicated jobs JSON-shape helpers only where semantics are identical.
- Treat jobs root injection as compatibility debt and avoid expanding it.
- Split `src/jobs/reporting_dedup_evidence.py` behind its existing public builders. **Done** (2026-08-19).

**Current (2026-08-19) top remaining targets** — see §3F for the full refreshed ranking: `src/source_discovery/gamedevmap_active_dry_run.py` (2,087) is now the largest file in `src/`; the top jobs-side split targets are `src/jobs/state_lifecycle.py` (1,121) and `src/jobs/reporting_dedup_evidence.py` (1,133) (`dedup.py`, `canonicalize_google_sheets.py`, `static_listing.py`, `source_registry_io.py`, and `static_detail_heuristics.py` were split 2026-08-19 and are no longer hotspots).

## Current Repo Check

The snapshot below was validated against the repo state on 2026-05-17 before this plan was added. Treat counts and consumer lists as starting evidence for a refactor charter, not as a substitute for a fresh `rg`/Serena check immediately before editing.

**Refreshed 2026-08-19** (current `wc -l`; direct `.py` files per directory — nested subdirectories excluded, so the four `src/jobs` rows stay disjoint).

## Codebase Snapshot

| Area | Files | Δ vs 2026-05-17 | Large Files (>500 lines) |
|------|-------|-----------------|--------------------------|
| `src/jobs/` (core) | 65 | +24 | 15 — top: `reporting_dedup_evidence.py:1,133`, `state_lifecycle.py:1,121` |
| `src/jobs/common/` | 38 | +7 | 7 — top: `dedup_evidence_bundle.py:798` |
| `src/jobs/adapters/` | 27 | +10 | 7 — top: `provider_structured_listing.py:860`, `location_rules.py:856` (+ `static_listing_{runner,traversal}.py` and the 4 `static_detail_heuristics_{config,filter,parse,entry}.py` leaves from the 2026-08-19 split) |
| `src/jobs/adapters/plugins/` | 5 | −35 | 0 direct — large files live under `plugins/static/` (`_rendered_cards.py:990`), `plugins/social/` |
| `src/bridge/` | 120 | +69 | 19 — top: `sync_service.py:1,019` |
| `src/source_discovery/` | 66 | +4 | 16 — top: `gamedevmap_active_dry_run.py:2,087` |
| `src/ship/` | 26 | −24 | 6 direct — top: `runtime_launcher.py:1,103`; `packaged_smoke/rehearsal_browser.py:1,341` is nested |
| `src/shared/` | 16 | +5 | 2 — `fetch_report_normalization.py:1,167`, `process_memory.py:545` |
| `src/storage/` | 7 | 0 | 4 — top: `task_runtime.py:703` |
| `src/scrapers/` | 5 | −4 | 0 |
| `scripts/` | 51 | +12 | 9 — top: `source_policy_soak_report.py:5,021` |

Large-file counts moved as the coordinator+leaves splits landed: `dedup.py` (1,804→286) and `canonicalize_google_sheets.py` (1,607→45) dropped out of `src/jobs` core, while `src/bridge` grew by the `task_launch_api.py` (2,441) and `pipeline_service.py` split leaves. See §3F for the repo-wide hotspot ranking this snapshot now matches.

**Corrected line counts** (previous estimates confused bytes with lines):

| Previously Reported | Actual Lines |
|-------------------|--------------|
| `reporting_dedup_evidence.py` ~152k | **3,641** |
| `pipeline_finalize.py` ~34k | **830** |
| `pipeline_source_results.py` ~25k | **721** |
| `pipeline.py` ~14k | **358** |
| `dedup.py` ~48k | **1,319** |
| `canonicalize.py` ~28k | **831** |

Only 2 files exceed 1,000 lines in src/jobs. The codebase is well-decomposed.

**Refreshed 2026-08-19** (current `wc -l`):

| File | 2026-05-17 | Today |
|------|-----------|-------|
| `src/jobs/dedup.py` | 1,319 | **286** (coordinator; split 2026-08-19 — no longer a hotspot) |
| `src/jobs/canonicalize_google_sheets.py` | n/a (extracted 2026-08-17 from `canonicalize.py`) | **45** (coordinator; split 2026-08-19 into 5 leaves: slug, category, link, provider, title — no longer a hotspot) |
| `src/jobs/adapters/static_listing.py` | 1,645 | **52** (coordinator; split 2026-08-19 into 7 leaves — `runner` 726 and `traversal` 510 remain >500) |
| `src/source_registry_io.py` | ~n/a | **146** (coordinator; split 2026-08-19 into paths/load/journal/save leaves — `journal` 603 remains >500) |
| `src/bridge/ops_api.py` | ~1,474 | **45** (coordinator; split 2026-08-19 into 5 mixin leaves — `core` 469, `health` 433, `task_state` 463, `live` 155, `reports` 72; no longer a hotspot) |
| `src/jobs/reporting_dedup_evidence.py` | 3,641 | **1,133** |
| `src/jobs/state_lifecycle.py` | ~n/a | **1,121** |
| `src/jobs/pipeline_source_results.py` | 721 | **807** |
| `src/jobs/canonicalize.py` | 831 | **150** (thin coordinator post-split) |

**2 files now exceed 1,000 lines in `src/jobs` core** (reporting_dedup_evidence, state_lifecycle). `dedup.py`, `canonicalize_google_sheets.py`, and `static_listing.py` dropped out after their 2026-08-19 splits (`static_listing.py` is now a 52-line coordinator; its `runner`/`traversal` leaves are 726/510). The largest files under `src/jobs` are now `reporting_dedup_evidence.py` (1,133) and `state_lifecycle.py` (1,121); the largest `src/jobs/adapters/` files are `provider_structured_listing.py` (860) and `location_rules.py` (856) — see §3F.

---

## 1. Dead Layers (Can Be Removed)

### 1A. `src/jobs/state.py` (72 lines) — Facade, 7 consumers (loophole audit 2026-05-29: plan originally listed 2)

**What it does**: Re-exports 27 symbols and `BROWSER_FALLBACK_STATE_KEY` from `state_source_state.py`, `state_lifecycle.py`, `state_incremental.py`, `browser_fallback.py`.

**Consumers** (all verified 2026-05-29):
| Consumer | Import Pattern | Fix target |
|----------|---------------|------------|
| `src/source_discovery/orchestrator.py:21` | `from src.jobs.state import read_source_state` | `state_source_state` |
| `src/jobs/fetcher_compat_exports.py:13` | `from src.jobs import state as state_mod` (re-exports `should_skip_source_by_*`) | `state_incremental` |
| `src/jobs/adapters/static_listing.py:45` | `from src.jobs.state import should_skip_static_source_for_structured_migration` | `state_source_state` |
| `src/jobs/adapters/static_runtime.py:11` | `from src.jobs.state import get_incremental_cache_decision` | `state_incremental` |
| `src/jobs/adapters/provider_structured_listing.py:19` | `from src.jobs.state import get_incremental_cache_decision` | `state_incremental` |
| `src/jobs/adapters/social.py:21` | `from src.jobs.state import get_incremental_cache_decision` | `state_incremental` |
| `src/jobs/adapters/plugins/provider_api/lifecycle.py:8` | `from src.jobs.state import get_incremental_cache_decision` | `state_incremental` |

**Test consumers** (also need fixing):
| Test file | Import Pattern | Fix target |
|-----------|---------------|------------|
| `tests/test_pipeline_stage_source_execution.py:13` | `from src.jobs.state import BROWSER_FALLBACK_STATE_KEY` | `browser_fallback` |
| `tests/test_jobs_package.py:234` | Tests that `state.py` is a facade (reads file, asserts no function defs) | Remove or rewrite test (has `if not target.exists(): return` guard so deletion auto-skips) |

**Bypassed by** (5 modules that import state sub-modules directly, not via state.py):
- `src/jobs/pipeline_cli.py` → `state_source_state`, `state_incremental`
- `src/jobs/pipeline_execution_flow.py` → `state_source_state`
- `src/jobs/pipeline_finalize.py` → `state_source_state`, `state_lifecycle`
- `src/jobs/pipeline_run_setup.py` → `state_source_state`, `state_lifecycle`, `state_incremental`
- `src/bridge/task_launch_api.py` → `state_lifecycle`

**Verdict**: **REMOVE**. Changes needed (8 files touched):
1. Replace `from src.jobs.state import read_source_state` → `from src.jobs.state_source_state import read_source_state` in `source_discovery/orchestrator.py`
2. Update `fetcher_compat_exports.py`: replace `from src.jobs import state as state_mod` with `import src.jobs.state_incremental as state_incremental_mod`, then update one usage block at line 109-117 (`state_mod` → `state_incremental_mod`)
3. Replace `from src.jobs.state import should_skip_static_source_for_structured_migration` → `from src.jobs.state_source_state import should_skip_static_source_for_structured_migration` in `adapters/static_listing.py`
4-7. Replace `from src.jobs.state import get_incremental_cache_decision` → `from src.jobs.state_incremental import get_incremental_cache_decision` in 4 files: `static_runtime.py`, `provider_structured_listing.py`, `social.py`, `lifecycle.py`
8. Replace `from src.jobs.state import BROWSER_FALLBACK_STATE_KEY` → `from src.jobs.browser_fallback import BROWSER_FALLBACK_STATE_KEY` in `tests/test_pipeline_stage_source_execution.py`
9. Remove `test_state_module_uses_package_private_helper_boundaries` from `tests/test_jobs_package.py` (auto-skips when file absent)

### 1B. `src/jobs/parsers.py` (74 lines) — Shim facade, 3 source consumers + 1 test consumer (loophole audit 2026-05-29: plan originally listed 2)

**What it does**: Re-exports ~28 parser functions from `adapters/html_parsers`, `adapters/provider_parsers`, `adapters/social_parsers`, and `adapters/community`. Also provides `parse_jobpostings_from_html` (3-line pass-through) and `parse_remote_ok_payload` (2-line wrapper adding `looks_like_game_job` arg) thin wrappers.

**Consumers** (all verified 2026-05-29):
| Consumer | Import Pattern | Fix |
|----------|---------------|-----|
| `src/source_discovery/probe.py:19` | `from src.jobs.parsers import parse_jobpostings_from_html` | Import from `html_parsers` directly |
| `src/admin_bridge.py:89` | `from src.jobs.parsers import parse_jobpostings_from_html as _parse_jobpostings_from_html` (used at line 162) | Import from `html_parsers` directly |
| `src/jobs/fetcher_compat_exports.py:10` | `from src.jobs import parsers as parsers_mod` — powers 28 exports in `COMPAT_MODULE_EXPORTS` | **High effort** — rewrite compat table to import from 5 individual source modules |

**Test consumers**:
| Test file | Import Pattern | Fix |
|-----------|---------------|-----|
| `tests/bridge/test_source_checker.py:6` | `from src.jobs.parsers import parse_jobpostings_from_html` | Import from `html_parsers` directly |

**Bypassed by**:
- `src/jobs/page_gating.py` → imports directly from `adapters/html_parsers`
- `src/jobs/adapters/community/__init__.py:168` → already imports `parse_remote_ok_payload` directly from `common.parsing`

**Verdict**: **REMOVE** with caveats. Complexity analysis completed 2026-05-29 — the compat table rewrite is **mechanical, not risky**. Verified mapping of all 29 exports to canonical source modules:

| Count | Source Module | Exports |
|------:|--------------|---------|
| 4 | `adapters.html_parsers` | parse_gamesindustry_html, parse_wellfound_html, parse_teamtailor_listing_links, parse_jobpostings_from_html |
| 5 | `adapters.community` | parse_gamejobs_html, parse_workwithindies_html, parse_8bitplay_html, parse_gracklehq_html, parse_google_sheets_csv |
| 14 | `adapters.provider_parsers` | parse_greenhouse_jobs_payload, parse_lever_jobs_payload, parse_oracle_hcm_requisitions_payload, parse_smartrecruiters_jobs_payload, parse_workable_jobs_payload, parse_recruitee_jobs_payload, parse_pinpoint_jobs_payload, parse_epic_games_jobs_payload, parse_personio_feed_xml, parse_ashby_jobs_from_html, parse_breezy_jobs_html, parse_jazzhr_jobs_html, parse_bamboohr_jobs_html, parse_workday_jobs_html |
| 5 | `adapters.social_parsers` | parse_reddit_json_payload, parse_reddit_rss_payload, parse_x_payload, parse_x_rss_payload, parse_mastodon_payload |
| 1 | inline wrapper | parse_remote_ok_payload (2-line wrapper injecting `looks_like_game_job`) |

All 27 module-level re-exports verified to exist in their canonical source modules. `parse_jobpostings_from_html` signatures confirmed identical (pass-through, can point directly to `html_parsers.parse_jobpostings_from_html`). `parse_remote_ok_payload` wrapper injects `looks_like_game_job` kwarg — needs a thin `_parse_remote_ok_payload_compat` function in `fetcher_compat_exports.py`.

**Additional loophole discovered 2026-05-29: `src/jobs/__init__.py`** imports `parsers` and `state` at lines 8 and 11, and lists them in `__all__` at lines 20 and 23. Both must be removed to prevent `ImportError` after file deletion.

**Additional test consumers that import `state` through the package `__init__.py`** (not via `state.py` directly):
| Test file | Current import | Fix |
|-----------|---------------|-----|
| `tests/test_structured_migration_state.py:3` | `from src.jobs import state as jobs_state` | Import from `state_source_state` directly |
| `tests/test_browser_fallback.py:3` | `from src.jobs import state as jobs_state` | Import from `state_source_state` directly |
| `tests/test_jobs_fetcher_pipeline.py` (4 occurrences) | `from src.jobs import state as state_pkg` | Import from `state_incremental` directly (all 4 use only `get_incremental_cache_decision`) |

Changes needed:
1. Rewrite `fetcher_compat_exports.py`: remove `from src.jobs import parsers as parsers_mod` (line 10); add imports for `html_parsers`, `provider_parsers`, `social_parsers`, `game_detection.looks_like_game_job`, `common.parsing.parse_remote_ok_payload`; replace single 29-entry `_module_attr_exports(parsers_mod, ...)` block with 4 targeted `_module_attr_exports(...)` blocks + 1 inline wrapper; merge the 5 community parser names into the existing `community_mod` block
2. Update `src/jobs/__init__.py`: remove `parsers` and `state` from the import tuple and `__all__`
3. Update 2 direct source consumers (`probe.py`, `admin_bridge.py`) and 1 test consumer (`test_source_checker.py`) to import from `html_parsers` directly
4. Update 3 test files that import `state` through the package to import from leaf modules directly

### 1C. `src/jobs/fetcher_compat_exports.py` (223 lines) — Internal-only dispatch table

**What it does**: Defines `COMPAT_MODULE_EXPORTS` dict mapping ~100+ symbol names to `(module, attr_name)` tuples. Used by `jobs_fetcher.py` `__getattr__`.

**Consumers**: ONLY `src/jobs_fetcher.py`.

**Verdict**: **MERGE INTO `jobs_fetcher.py`**. Neither dead nor removable, but can be consolidated. The export table is an implementation detail of the compat dispatch mechanism. Moving it into `jobs_fetcher.py` eliminates one file with no functional change.

### 1D. `src/jobs/fetcher_compat_runtime.py` (68 lines) — Internal-only runtime wrappers

**What it does**: Provides 5 wrapper functions (`run_pipeline`, `run_scrapy_static_source`, `registry_entries`, `build_redirect_resolver`, `maybe_fetch_kojima_job_listing_html`) that monkey-patch root-module attributes at runtime.

**Consumers**: ONLY `src/jobs_fetcher.py`.

**Verdict**: **MERGE INTO `jobs_fetcher.py`**. These wrappers are the `jobs_fetcher` module's own implementation.

---

## 2. Duplicated Logic

### 2A. Type-coercion helpers `_as_list`/`_as_dict`/`_as_dict_rows` (4 modules in `src/jobs/`)

Tiny guard functions duplicated across files (verified 2026-05-29 — plan originally claimed `pipeline_finalize.py` had `_as_dict`; it only has `_as_list`):

| Helper | Modules | Lines Each | Verdict |
|--------|---------|-----------|---------|
| `_as_list` | `pipeline_cli.py`, `pipeline_finalize.py`, `pipeline_source_results.py`, `state_source_records.py` | 2 | **EXTRACT** to shared utility |
| `_as_dict` | `pipeline_cli.py`, `pipeline_source_results.py`, `state_source_records.py` | 2 | **EXTRACT** to shared utility |
| `_as_dict_rows` | `pipeline_source_results.py`, `state_source_records.py` | 2 | **EXTRACT** to shared utility |

All are identical across modules:
```python
def _as_list(value): return value if isinstance(value, list) else []
def _as_dict(value): return value if isinstance(value, dict) else {}
def _as_dict_rows(value): return [item for item in _as_list(value) if isinstance(item, dict)]
```

**Verdict**: Move into `src/shared/utils.py` or `src/shared/json_shapes.py`. Total savings: ~10 lines duplicated x 4 modules = ~40 lines removed.

### 2B. `build_excluded_source_report` (2 modules)

- `pipeline_loader_selection.py` — takes `source_report_meta` as explicit parameter
- `state_source_records.py` — uses module-level `SOURCE_REPORT_META` directly + adds `exclusionReason` and `durationMs` fields

Body is otherwise identical. The `state_source_records.py` version is more complete (more fields). The `pipeline_loader_selection.py` version is more portable (accepts meta as param).

**Verdict**: **CONSOLIDATE**. Callers of `pipeline_loader_selection.build_excluded_source_report` can import from `state_source_records` instead, or extract a shared helper. ~20 lines duplication.

### 2C. `normalizers.py` `_clean_text`/`_norm_text` (private duplicates of `text_utils.py`)

- `normalizers.py` line 38-43 defines `_clean_text` and `_norm_text` which are identical to `clean_text` and `norm_text` in `text_utils.py`
- **Cannot import from text_utils.py** due to circular import: `text_utils.py` imports `COUNTRY_NAME_TO_CODE` and `normalize_country` from `normalizers.py`
- **Verdict**: **KEEP as-is**. This is a deliberate circular-import workaround, not accidental duplication. Not worth breaking the dependency cycle.

### 2D. `state_incremental.py` — Duplicate `consecutiveFailures` guard

- Both `should_skip_source_by_ttl` (line 65-66) and `should_skip_source_by_cadence` (line 120-121) check:
  ```python
  if int(entry.get("consecutiveFailures") or 0) > 0:
  ```
- **Verdict**: **Minor duplication** within the same file. Extract into a shared guard helper if touched, but low priority (~15 lines change).

### 2E. Root Injection Pattern (8 modules across 2 subsystems)

**Jobs pipeline** (4 modules, 3 injection points):

| Module | Pattern |
|--------|---------|
| `fetcher_compat_runtime.py` | `root: Any \| None = None` + `_root_mod()` with RuntimeError |
| `pipeline_source_loop.py` | `root: _RootLike \| None = None` + `_root_module()` with direct import fallback |
| `pipeline_source_progress.py` | `root: Any \| None = None` + `_require_root()` |
| `pipeline_source_results.py` | `root: _PipelineSourceResultsRoot \| None = None` + `_require_root()` |

Three distinct root-setting sites (jobs only):
1. `jobs_fetcher.py` → sets `fetcher_compat_runtime.root`
2. `pipeline_stage_source_execution.py` → sets root on 3 pipeline_source_* modules
3. `pipeline_source_loop.py` → fallback imports `jobs_fetcher` directly if root is None

**Source discovery** (4 modules, 2 injection points):
| Module | Pattern |
|--------|---------|
| `orchestrator_finalize.py` | `root: Any \| None = None` + `_require_root()` |
| `orchestrator_generation.py` | `root: Any \| None = None` + `_require_root()` |
| `orchestrator_probe.py` | `root: Any \| None = None` + `_require_root()` |
| `gamesmap_candidates.py` | `root: ModuleType \| None = None` |

Two root-setting sites (source_discovery):
1. `orchestrator.py` → sets root on orchestrator_generation, orchestrator_probe, orchestrator_finalize
2. `gamesmap.py` → sets root on gamesmap_candidates

**Verdict (jobs only)**: **HIGH VALUE REFACTOR**. Unify to single injection point (`jobs_fetcher.py`). This eliminates replicated root-access code in 4 modules and removes the confusing 3-way injection. The source_discovery pattern is a separate subsystem concern.

### 2F. `pipeline_source_loop.py` `_root_module()` — Duplicates fetcher_compat_runtime dispatch

- `pipeline_source_loop.py` (`_root_module()`) has its own fallback: `from src import jobs_fetcher as jobs_fetcher_pkg`
- This mirrors what `fetcher_compat_runtime._root_mod()` does (but compat_runtime raises RuntimeError instead of falling back)
- **Verdict**: **FIX AS PART OF ROOT INJECTION UNIFICATION (2E)**. Remove the fallback and rely on the single injection point.

---

## 3. Consolidation Candidates

### 3A. `reporting_dedup_evidence.py` (3,641 lines at analysis time — **1,133 today**, item 11 done)

Internal structure:
| Section | Lines | Helpers |
|---------|-------|---------|
| Bundle shape analysis + identity quality + Google Sheets audit | 88-1,157 | ~60 |
| Review queue logic | 1,158-1,385 | ~15 |
| Provider-static disagreement | 1,386-2,084 | ~25 |
| Audit gate building | 2,085-2,855 | ~20 |
| `build_dedup_audit_gate()` | 2,856-3,125 | 269-line public function |
| `build_dedup_evidence()` | 3,126-3,641 | 515-line public function |

**Verdict**: **SPLIT INTO 5 SUB-MODULES** under `reporting_dedup_evidence/`:
- `bundle_shapes.py` (~1,070 lines, 60 helpers)
- `identity_quality.py` (~230 lines, 15 helpers)
- `provider_static.py` (~700 lines, 25 helpers)
- `review_queue.py` (~200 lines, 15 helpers)
- `audit_gate.py` (~1,270 lines, 20 helpers + 2 public functions)

### 3B. Bridge: `registry_conflicts.py` (3,599 lines), `task_launch_api.py` (2,377 lines)

These are in the bridge subsystem. Not part of the jobs pipeline refactoring, but worth noting as decomposition targets for a future bridge-focused pass.

**2026-08-19 refresh**: both are now done — `registry_conflicts.py` decomposed 2026-05-26 into `registry_conflicts_{row,automation,demotions,summary}.py` (coordinator 489 lines; `automation` 1,544 was split 2026-08-19 into a 252-line coordinator + 4 leaves; `row` 1,401 → 1,313 was split the same session into a 139-line coordinator + 6 leaves (`registry_conflicts_row_{core,identity,path,source_state,adjudication,audit}.py`, all < 245); `adjudication` 1,128 was split the same session into a 98-line coordinator + 5 leaves (`registry_conflict_adjudication_{core,progress,probe,decide,run}.py`, all < 330) — the largest active bridge file is now `sync_service.py` (1,019)); `task_launch_api.py` split this session (item 15) into a 685-line coordinator + `task_launch_api_{state,contexts,smoke,bootstrap}.py`.

### 3C. Adapter: `static_listing.py` (1,645 lines), `static_detail_heuristics.py` (907 lines)

Static adapter modules. These are large but are adapter implementations with clear scope. Decomposition deferrable.

**2026-08-19 refresh**: `static_listing.py` grew to **2,237 lines — the largest file in all of `src/`** (+36% since the plan) and was **split the same day** into a 52-line coordinator + 7 leaves (common, state, flow, plugin, rows, traversal, runner — the latter two at 726/510). `static_detail_heuristics.py` grew to **1,153** (largest file under `src/jobs/adapters/`) and was **split the same day** into a 33-line coordinator + 4 leaves (`static_detail_heuristics_{config,filter,parse,entry}.py`, all < 490; the `extract_rendered_card_jobs` alias stays in the coordinator; 11-name re-export surface via `ruff.toml` per-file-ignore; the two `parse_jobpostings_from_html` monkeypatch seams in `test_detail_fallback.py` retargeted to the entry leaf where `process_detail_html` resolves it).

### 3D. `pipeline_run_setup.py` (508 lines) — Near threshold, but extracted helpers would need to be shared

**2026-08-19 refresh**: grew to **712 lines** — item 12's revisit trigger fired (scanner score 55); evaluated as a single-function complexity problem (`prepare_pipeline_run` is 468 lines) and Option A internal-stage extraction was proposed, not yet implemented.

### 3E. `__init__.py` files

| File | Purpose | Lines |
|------|---------|-------|
| `src/jobs/__init__.py` | Public package surface (re-exports 9 sub-modules) | 25 |
| `src/jobs/common/__init__.py` | Warning docstring only | 5 |
| `src/bridge/__init__.py` | Package surface | 54 |
| `src/jobs/adapters/__init__.py` | Adapter registry + source loader orchestration | ~250 |

The `src/jobs/adapters/__init__.py` (250 lines) contains `default_source_loaders()` and `EXTRACTED_ADAPTERS` — this is non-trivial logic, not just a facade. **Keep as-is**.

### 3F. Current Hotspot Ranking (refreshed 2026-08-19)

From the current `analyze_refactorability.py` run: **109 files ≥ 500 lines**. The tool's top-10 is now **size-primary** (line count descending, score only a tiebreak — changed 2026-08-19), so the largest pure-size files surface instead of being hidden behind runtime-named files. The table below is the tool's exact top-10 output; scoring is `size: 25/50` + `runtime-hotspot: 30` (name-match on runtime/app/domain/orchestrator/bridge/pipeline/main).

**Tool top-10 (size-primary):**

| # | File | Lines | Score |
|---|------|-------|-------|
| 1 | `src/source_discovery/gamedevmap_active_dry_run.py` | 2,087 | 50 |
| 2 | `src/source_sync_shard.py` | 1,525 | 50 |
| 3 | `src/source_sync_snapshot.py` | 1,345 | 50 |
| 4 | `src/ship/packaged_smoke/rehearsal_browser.py` | 1,341 | 50 |
| 5 | `src/source_discovery/active_audit_runtime.py` | 1,337 | 80 |
| 6 | `src/source_discovery/web_search_candidates.py` | 1,263 | 50 |
| 7 | `src/fetch_incremental_sanity_benchmark.py` | 1,171 | 50 |
| 8 | `src/shared/fetch_report_normalization.py` | 1,167 | 50 |
| 9 | `src/jobs/reporting_dedup_evidence.py` | 1,133 | 50 |
| 10 | `src/jobs/state_lifecycle.py` | 1,121 | 50 |

**Full tier breakdown** (same scoring, all tiers):

**Score 80 — high-risk size + runtime name (3 files):**

| File | Lines |
|------|-------|
| `src/source_discovery/active_audit_runtime.py` | 1,337 |
| `src/ship/runtime_launcher.py` | 1,103 |
| `src/source_discovery/orchestrator_generation.py` | 1,031 |

All three are in sensitive/separate-scope subsystems (`src/source_discovery/`, `src/ship/`) — the plan's existing deferral stance applies; the jobs-side runtime-name files dropped out of the 80 tier as the coordinators shrank.

**Score 55 — oversized + runtime name (16 files, all listed):**

| File | Lines |
|------|-------|
| `src/jobs/pipeline_source_results.py` | 807 |
| `src/ship/packaged_smoke/orchestrator.py` | 759 |
| `src/source_sync_runtime.py` | 719 |
| `src/jobs/pipeline_run_setup.py` | 712 (item 12) |
| `src/jobs/pipeline_finalize.py` | 703 (coordinator) |
| `src/storage/task_runtime.py` | 703 |
| `src/source_registry_auto_approval.py` | 644 |
| `src/jobs/pipeline_runtime_writers.py` | 643 |
| `src/admin_bridge.py` | 626 |
| `src/bridge/pipeline_service_control.py` | 568 |
| `src/storage/source_registry_runtime.py` | 558 |
| `src/jobs/pipeline_runtime_summary.py` | 554 |
| `src/pipeline_audit.py` | 537 |
| `src/source_discovery/orchestrator_probe.py` | 511 |
| `src/storage/job_runtime.py` | 505 |
| `src/bridge/pipeline_service_stages.py` | 503 |

**Score 50 — high-risk size only (17 files):** the pure-size tier. These are the actual largest files in the repo and the natural next split targets:

| File | Lines | Notes |
|------|-------|-------|
| `src/source_discovery/gamedevmap_active_dry_run.py` | 2,087 | **Largest file in `src/` now** |
| `src/source_sync_shard.py` | 1,525 | Separate scope |
| `src/source_sync_snapshot.py` | 1,345 | Separate scope |
| `src/ship/packaged_smoke/rehearsal_browser.py` | 1,341 | Ship |
| `src/source_discovery/web_search_candidates.py` | 1,263 | Separate scope |
| `src/fetch_incremental_sanity_benchmark.py` | 1,171 | |
| `src/shared/fetch_report_normalization.py` | 1,167 | Shared |
| `src/jobs/reporting_dedup_evidence.py` | 1,133 | Item 11 done; size is now split leaves |
| `src/jobs/state_lifecycle.py` | 1,121 | |
| `src/container_gateway.py` | 1,084 | |
| `src/bridge/sync_service.py` | 1,019 | Bridge |
| `src/source_discovery/directory_page_recovery.py` | 1,017 | Separate scope |
| `src/jobs/adapters/plugins/static/_rendered_cards.py` | 990 | Plugins are keep-as-is per guardrails |
| `src/bridge/registry_service.py` | 963 | Bridge |
| `src/jobs/adapters/parsers/json_payloads.py` | 913 | Adapter |
| `src/jobs/adapters/parsers/location.py` | 910 | Adapter |
| `src/ship/desktop_app/_windows.py` | 900 | Ship (paired with `_linux.py` 718) |

**Jobs-side takeaway**: `dedup.py` (1,804), `canonicalize_google_sheets.py` (1,607), `static_listing.py` (2,237), and `static_detail_heuristics.py` (1,153) were split 2026-08-19 into coordinators + leaves and are no longer hotspots; the top remaining jobs-side targets are `reporting_dedup_evidence.py` (1,133, item 11's split leaves) and `state_lifecycle.py` (1,121) — neither flagged for monkeypatch or route-contract surface.

**Bridge-side**: `registry_conflicts_automation.py` (1,544) was split the same day into a 252-line coordinator + 4 leaves (triage/eligibility/provider/static, all < 500) — no longer a hotspot — and `registry_conflicts_row.py` trimmed to 1,313 (dead duplicated `TRIAGE_BUCKETS`/`REVIEW_QUEUES` constants removed). `ops_api.py` (1,474) was split the same day into a 45-line coordinator + 5 mixin leaves (`ops_api_{core,reports,live,health,task_state}.py`, all < 500) using the `task_launch_api.py` mixin pattern — no longer a hotspot. `discovery_service.py` (1,162) was split the same day into a 34-line coordinator + 6 mixin leaves (`discovery_service_{core,config,launch,lifecycle,registry,watch}.py`, all < 330; cross-mixin surface stubbed in the core leaf's `DiscoveryServiceState`) — no longer a hotspot. `task_lifecycle.py` (1,153) was split the same day into a 62-line coordinator + 5 leaves (`task_lifecycle_{core,compact,rows,runs,legacy}.py`, all < 350; the `_compact_*`/`_legacy_*` helper chains moved to helper leaves, the class to mixin leaves via `--class-methods`, cross-mixin surface stubbed in the core leaf's `TaskLifecycleState`) — no longer a hotspot. `registry_conflict_adjudication.py` (1,128) was split the same session into a 98-line coordinator + 5 leaves (`registry_conflict_adjudication_{core,progress,probe,decide,run}.py`, all < 330; function-leaf pattern, `_AdjudicationProgress` moved whole into the progress leaf, the verbatim `__all__` tail folded into the run leaf per the fidelity checker's unit rules, coordinator's own `__all__` sits before its first matched unit) — no longer a hotspot. `registry_conflicts_row.py` (1,313) was split the same session into a 139-line coordinator + 6 leaves (`registry_conflicts_row_{core,identity,path,source_state,adjudication,audit}.py`, all < 245; the coordinator keeps the 23 `SAFE_AUTO_*`/`RESOLVED_PENDING_DEMOTION_REASONS` constants and the full 73-name re-export surface via `ruff.toml` per-file-ignore, mirroring the `admin_bridge.py` re-export hub; `_row_urls` moved into the core leaf to break the core↔identity cycle; the fidelity original was reconstructed as HEAD-minus-the-dead-`TRIAGE_BUCKETS`/`REVIEW_QUEUES`-blocks, validated 87/87 against the leaves before the full 110/110 run) — no longer a hotspot; the largest active bridge file is now `sync_service.py` (1,019).

**Split verification**: any split of the files above must follow the fidelity-checker standard in §10 (`verify_split_fidelity.py` pre/post + the five-point checklist) instead of ad-hoc snapshot hashing.

---

## 4. Boundary Violations (False Positives in Tool Output)

The 4 scripts flagged by `tools/repo_health/bin/analyze_refactorability.py` import leaf modules, not composition roots. The tool's regex `^from\s+src\.jobs\b` over-matches due to `\b` matching between 's' (word char) and '.' (non-word char):

| Script | Actual Import | Tool Verdict | Reality |
|--------|---------------|-------------|---------|
| `scripts/audit_diff.py` | `src.jobs.text_utils` | Boundary violation | **False positive** — leaf import |
| `scripts/jobs_yield_gate.py` | `src.jobs.adapters.api`, `src.jobs.adapters.static_sources` | Boundary violation | **False positive** — leaf imports |
| `scripts/location_unknown_country_manifest.py` | `src.jobs.location_bucket_manifest` | Boundary violation | **False positive** — leaf import |
| `scripts/reset_admin_task_lifecycle.py` | `src.bridge.lifecycle_cleanup` | Boundary violation | **False positive** — leaf import |

**Root cause**: Analyzer at line 185 uses regex `^from\s+{re.escape(root)}\b|^import\s+{re.escape(root)}\b` — the `\b` word boundary matches between a word char (`s` in `jobs`) and a non-word char (`.`) on both the `from` and `import` sides. For root `src.jobs`, this becomes `^from\s+src\.jobs\b|^import\s+src\.jobs\b`, which falsely matches:
- `from src.jobs.text_utils import X` (false positive: `\b` matches `s.` boundary)
- `import src.jobs.text_utils` (false positive: `\b` matches `s.` boundary)

**Corrected regex** (fixes both sides):
```
^from\s+src\.jobs\s+import\b|^import\s+src\.jobs\s*$
```
This matches only bare package imports (`from src.jobs import X`, `import src.jobs`) while rejecting leaf imports (`from src.jobs.text_utils import X`, `import src.jobs.text_utils`).

---

## 5. Other Non-Issues

| Potential Issue | Verdict | Reason |
|----------------|---------|--------|
| `normalizers.py` private `_clean_text`/`_norm_text` | **Not duplication** | Circular import workaround |
| `state_incremental.py` consecutiveFailures guard | **Minor, not worth standalone refactor** | Same file, 15 lines |
| `parsers.py` → `source_discovery/probe.py` import | **Can be fixed if parsers.py is removed** | 1 of 3 source consumers |
| `parsers.py` → `admin_bridge.py` import | **Can be fixed if parsers.py is removed** | 2nd of 3 source consumers |
| `state.py` → 5 adapter imports (`static_listing`, `static_runtime`, `provider_structured_listing`, `social`, `lifecycle`) | **Can be fixed if state.py is removed** | Missed by original analysis; found in 2026-05-29 loophole audit |
| `state.py` → `source_discovery/orchestrator.py` import | **Can be fixed if state.py is removed** | 1 of 7 source consumers |
| `src/jobs/adapters/plugins/static/*.py` (28 files) | **Keep** | Company-specific scrapers |
| `src/source_discovery/` (62 files) | **Not in scope** | Separate subsystem |
| `src/ship/` (50 files) | **Not in scope** | Desktop app + update system |
| `src/storage/` (7 files) | **Not in scope** | Storage layer |
| `src/scrapers/` (9 files) | **Not in scope** | Scrapy spider project |
| `common/` modules (31 files) | **All used** | None are dead |

---

## 6. Integrated Action Plan

### Phase 1: Quick Wins (~4h total, revised upward from ~2h after 2026-05-29 loophole audit)

| # | Action | Risk | Files Changed | Lines | Effort | Status |
|---|--------|------|---------------|-------|--------|--------|
| 1 | **Remove `state.py` facade** (72 lines), update 7 source consumers + 2 test consumers + `__init__.py` cleanup + 3 package-import test files | Low | 12 | ~150 | ~1.5h | ✅ Done — `state.py` no longer exists |
| 2 | **Remove `parsers.py` shim** (74 lines), rewrite `fetcher_compat_exports.py` compat table (29 entries → 4 targeted blocks + 1 inline wrapper), update 2 direct source consumers + 1 test consumer + `__init__.py` cleanup | Low | 6 | ~130 | ~2h | ✅ Done — shim removed; `adapters/parsers/` is a package (`json_payloads.py`, `location.py`) |
| 3 | **Fix the `analyze_refactorability.py` regex** — change `^from\s+src\.jobs\b\|^import\s+src\.jobs\b` to `^from\s+src\.jobs\s+import\b\|^import\s+src\.jobs\s*$` to eliminate 4 false positives on both `from` and `import` sides | Low | 1 | ~1 | ~10min | ✅ Done — scanner now uses the proposed `^from\s+{root}\s+import\b\|^import\s+{root}\s*$` form; 0 boundary violations |
| 4 | **Fix `_clean_text`/`_norm_text` false alarm** — add doc comment to normalizers.py explaining circular import workaround | None | 1 | ~4 | ~5min | ✅ Done — circular-import comment added above the duplicates in `src/jobs/normalizers.py` |
| 5 | **Extract `_as_list`/`_as_dict`/`_as_dict_rows`** to `src/shared/utils.py` — remove 4/3/2 copies across modules (pipeline_finalize.py only has `_as_list`, not `_as_dict` as originally claimed) | Low | 5 | ~40 | ~30min | ✅ Done (2026-08-19) — canonical helpers already existed in `src/shared/json_shapes.py`; 21 files + 6 consumers consolidated onto them |

### Phase 2: Medium Effort (30-60 min each)

| # | Action | Risk | Files Changed | Lines | Status |
|---|--------|------|---------------|-------|--------|
| 6 | **Consolidate `build_excluded_source_report`** — unify 2 versions (pipeline_loader_selection.py and state_source_records.py) | Low | 2 | ~20 | ✅ Done (2026-08-19) — single helper in `state_source_records.py` with the `static_source::` fallback folded in |
| 7 | **Merge `fetcher_compat_exports.py` into `jobs_fetcher.py`** (223 lines moved, no logic change) | Low | 2 | ~0 delta | ✅ Done (2026-08-19) — merged as the in-facade `_COMPAT_MODULE_EXPORTS` table (116 entries); facade guardrail pins updated |
| 8 | **Merge `fetcher_compat_runtime.py` into `jobs_fetcher.py`** (68 lines moved, no logic change) | Low | 2 | ~0 delta | Open — still a separate module; carries root-monkeypatch seams + facade-budget tradeoff |
| 9 | **Unify root injection** — eliminate Points B (pipeline_stage_source_execution) and C (pipeline_source_loop fallback); all 4 root-dependent modules point to jobs_fetcher | Medium | 5 | ~100 | Open — root injection still in `pipeline_source_{loop,progress,results}.py` |
| 10 | **Extract shared guard in `state_incremental.py`** — both should_skip functions share `consecutiveFailures` check | Low | 1 | ~15 | ✅ Done (2026-08-19) — `_has_consecutive_failures` helper extracted |

### Phase 3: Larger Effort (1-2 hours each)

| # | Action | Risk | Description | Status |
|---|--------|------|-------------|--------|
| 11 | **Split `reporting_dedup_evidence.py`** (3,641 lines) into 5 sub-modules | Medium | Extract bundle_shapes, identity_quality, provider_static, review_queue, audit_gate | ✅ Done — 1,133 lines today; no longer a hotspot |
| 12 | **Review `pipeline_run_setup.py`** (508 lines) for helper extraction | Low | Currently manageable, revisit if it grows | ⚠️ Revisit triggered (2026-08-19) — grew to **712 lines**, crossed the 500-line warning threshold (scanner score 55) |
| 13 | **Investigate 16 unreferenced scripts** (~7,600 lines total) | Low | Archive or document `source_policy_soak_report.py` (3,711 lines) and others | ✅ Done (2026-08-19) — re-audited: 12 of 16 are live/documented; 4 one-off tools archived to `scripts/archive/` (see `scripts/ARCHIVED_SCRIPTS.md`) |

### Phase 4: Deferred

| # | Action | Scope | Notes | Status |
|---|--------|-------|-------|--------|
| 14 | Decompose `registry_conflicts.py` (3,599 lines) | Bridge | Bridge-specific, out of jobs scope | ✅ Done (2026-05-26) — split into `registry_conflicts_{row,automation,demotions,summary}.py`; coordinator is 489 lines |
| 15 | Decompose `task_launch_api.py` (2,377 lines) | Bridge | Bridge-specific, out of jobs scope | ✅ Done (2026-08-19) — split into 685-line coordinator + `task_launch_api_{state,contexts,smoke,bootstrap}.py`; bridge call sites + monkeypatch seams preserved |
| 16 | Simplify `jobs_fetcher.py` dynamic dispatch for 100+ symbols | Jobs | Could use direct imports instead of __getattr__ | Open — compat table is now in-facade with **116 entries** (post item 7); dynamic dispatch is guardrail-pinned as the compat surface |
| 17 | Evaluate `static_listing.py` (1,645 lines) decomposition | Adapters | Not urgent, well-scoped | ✅ Done (2026-08-19) — split 2,237 → 52-line coordinator + 7 leaves |

## 7. Potentially Dead Scripts (16 scripts, ~7,600 lines)

These scripts are NOT referenced in package.json, GitHub Actions CI, or imported by any module:

| Script | Lines | What it does |
|--------|-------|-------------|
| `source_policy_soak_report.py` | **3,711** | Source policy/runtime evidence soak report |
| `jobs_yield_gate.py` | 575 | CLI for dead-source candidates/registry/decisions |
| `backup_e2e_validate.py` | 519 | Desktop backup end-to-end validation |
| `audit_diff.py` | 264 | Diff audit |
| `repro_discovery_spawn.py` | 259 | Reproduce discovery spawn |
| `benchmark_discovery_probe.py` | 215 | Discovery probe benchmark (not in CI) |
| `check_complexity_baseline.py` | 195 | Complexity baseline check |
| `generate_report.py` | 213 | Standalone report generation |
| `game_studios_sheet_funnel.py` | 144 | Game studios sheet funnel |
| `refresh_url_patches.py` | 76 | URL patch refresh |
| `audit_json_artifacts.py` | 76 | JSON artifact audit |
| `location_unknown_country_manifest.py` | 71 | Unknown country location manifest |
| `gitleaks_precommit.py` | 45 | Gitleaks pre-commit helper |
| `serve_static_site.py` | 36 | Static site server |
| `reset_admin_task_lifecycle.py` | 35 | Reset admin task lifecycle |
| `source_audit_sweep.py` | 17 | Source audit sweep |

These may be invoked manually by developers or through the bridge task system. `source_policy_soak_report.py` (3,711 lines) is the most suspicious — very large and no current reference path.

**Verdict**: Investigate whether these can be archived. If all are developer tools, consider an `ARCHIVED_SCRIPTS.md` or move to a `scripts/archive/` directory.

**Re-audited 2026-08-19 (item 13)**: the "unreferenced" claim was largely stale. 12 of the 16 have tests, CI/config references, or runbook documentation and are kept; the 4 one-off tools with no reference path (`repro_discovery_spawn.py`, `generate_report.py`, `game_studios_sheet_funnel.py`, `refresh_url_patches.py`) were moved to `scripts/archive/` with recovery instructions in `scripts/ARCHIVED_SCRIPTS.md`.

## 8. Bridge Large Files (Separate Scope)

These are in the bridge subsystem — not part of jobs pipeline but worth noting. **All 20 files > 500 lines** (live `wc -l`, 2026-08-19; 19 direct + `routes/post_routes_admin.py` nested):

| File | Lines | Role |
|------|-------|------|
| `sync_service.py` | 1,019 | Sync service |
| `registry_service.py` | 963 | Registry service |
| `admin_entrypoint_services.py` | 879 | Admin entrypoint services |
| `ops_health.py` | 868 | Ops health panel services |
| `task_launch_api_bootstrap.py` | 808 | Task-launch bootstrap leaf (history/staging) |
| `run_history_api.py` | 751 | Run history API |
| `task_launch_api_smoke.py` | 722 | Task-launch packaged-smoke leaf |
| `routes/post_routes_admin.py` | 718 | Admin POST routes (nested in `routes/`) |
| `lifecycle_cleanup.py` | 703 | Lifecycle cleanup |
| `task_launch_api.py` | 685 | Coordinator (split 2026-08-19; was 2,441) |
| `job_availability_service.py` | 635 | Job availability service |
| `ops_task_fetch_live.py` | 618 | Ops task fetch live |
| `api.py` | 601 | Bridge API |
| `source_probe_evidence.py` | 595 | Source probe evidence |
| `active_task_snapshot.py` | 575 | Active task snapshot |
| `task_abort_service.py` | 575 | Task abort service |
| `pipeline_service_control.py` | 568 | Pipeline service control (leaf) |
| `report_normalizer.py` | 519 | Report normalizer |
| `task_launch_jobs_feed.py` | 516 | Task-launch jobs feed leaf |
| `pipeline_service_stages.py` | 503 | Pipeline service stages (leaf) |

*(Table refreshed 2026-08-19 — `registry_conflicts.py` 3,599 and `task_launch_api.py` 2,377 no longer exist as monoliths; `registry_conflicts_automation.py` (1,544) is now a 252-line coordinator + 4 leaves (all < 500), `registry_conflicts_row.py` (1,313) is now a 139-line coordinator + 6 leaves (all < 245), `ops_api.py` (1,474) split into a 45-line coordinator + 5 mixin leaves (all < 500), and `registry_conflict_adjudication.py` (1,128) split into a 98-line coordinator + 5 leaves (`registry_conflict_adjudication_{core,progress,probe,decide,run}.py`, all < 330); the split leaves above are the current large bridge files. `pipeline_service.py` itself is now a 156-line coordinator, below the cutoff.)*

## 9. source_discovery Large Files (Separate Scope)

**All 16 files > 500 lines** (live `wc -l`, 2026-08-19):

| File | Lines | Role |
|------|-------|------|
| `gamedevmap_active_dry_run.py` | 2,087 | Game dev map active dry run |
| `active_audit_runtime.py` | 1,337 | Active audit runtime (hotspot score 80) |
| `web_search_candidates.py` | 1,263 | Web search candidates |
| `orchestrator_generation.py` | 1,031 | Orchestrator generation (hotspot score 80) |
| `directory_page_recovery.py` | 1,017 | Directory page recovery |
| `provider_migration_advisory.py` | 829 | Provider migration advisory |
| `probe.py` | 715 | Source probe |
| `sheet_directory.py` | 704 | Sheet directory |
| `gamesmap_candidates.py` | 686 | Gamesmap candidates |
| `gamesmap_parsing.py` | 639 | Gamesmap parsing |
| `reporting_backlog.py` | 616 | Reporting backlog |
| `gameprog.py` | 599 | Gameprog discovery |
| `config.py` | 590 | Source discovery config |
| `browser_recovery.py` | 585 | Browser recovery |
| `directory_adapter_templates.py` | 513 | Directory adapter templates |
| `orchestrator_probe.py` | 511 | Orchestrator probe |

*(Table refreshed 2026-08-19 — all 16 files listed, matching the snapshot's source_discovery large-file count.)*

### NOT Recommended
- Removing fetcher_compat_* modules (essential for compatibility dispatch)
- Decomposing `canonicalize.py` (150 lines coordinator, reasonable)
- Decomposing `pipeline_finalize.py` (703-line coordinator, well-organized)
- Decomposing `pipeline_source_results.py` (807 lines, well-organized)
- Touching `src/source_discovery/`, `src/ship/`, `src/storage/`, `src/scrapers/` (separate subsystems)

---

## 10. Split Verification Standard (fidelity checker)

**When to split (decision rule).** Line count alone is never a split trigger. A coordinator+leaves split is justified only when **all three** conditions hold; otherwise leave the file whole and keep it on the §3F ranking:

1. **Size threshold** — the file exceeds **1,000 lines** (the plan's split bar). The ≥500-line "Large Files" tables in §3F/§8/§9 and the `analyze_refactorability.py` listing are inventories, not split triggers: well-organized 500–1,000-line files (e.g. `pipeline_finalize.py` 703, `pipeline_source_results.py` 807) are explicitly **not recommended** to decompose.
2. **Active subsystem** — the file lives in a subsystem this plan is actively refactoring: `src/jobs/**` (core + adapters) and the bridge files §3F has already committed to splitting. Separate-scope areas — `src/source_discovery/`, `src/ship/`, `src/storage/`, `src/scrapers/`, `scripts/`, and `src/jobs/adapters/plugins/` (keep-as-is per guardrails) — stay out of the split bar unless a separate charter opens them.
3. **Low seam risk** — the split is cheap on the compatibility surface: no route/bridge-contract surface, no coordinator-owned root-injection seam, and only a small, stable monkeypatch/import surface. Check this before starting, not after: enumerate importers, exported names, and test-patched names, and apply the compatibility-surface rule in [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md). A file with many seams (re-export hubs, root-injection seams, route payload builders, broad test-patching) is not a split target no matter how large — size alone must never drive a risky split.

When only size holds, do **not** split: record the file as size-only/deferred and stop. When all three hold, proceed to the fidelity-checker verification below.

Since the 2026-08-19 coordinator+leaves splits, every split is verified against the byte-exact original with the repo tool `tools/repo_health/bin/verify_split_fidelity.py` — **not** ad-hoc snapshot hashing:

```bash
# pre-split: confirm the working tree matches git HEAD
python tools/repo_health/bin/verify_split_fidelity.py src/bridge/foo.py

# post-split: every original def/class/constant must survive unchanged across leaves
python tools/repo_health/bin/verify_split_fidelity.py src/bridge/foo.py \
    --leaves src/bridge/foo.py src/bridge/foo_alpha.py src/bridge/foo_beta.py
```

- Reads the original from `git show HEAD:<original>`; pass `--snapshot FILE` when the original is untracked working-tree state (e.g. a module produced by an earlier uncommitted split). `--manifest-out` writes a JSON report for CI regression pinning.
- Fingerprints every top-level unit (def/class/constant assignment) and requires each to exist **exactly once** across the leaves with identical byte content (exit 0); reports MISSING / DIFF / AMBIGUOUS / EXTRA (EXTRA is informational).
- Validated on eleven real splits: `registry_conflicts_automation.py` (53/53), `canonicalize_google_sheets.py` (69/69, **untracked original via `--snapshot`**), `dedup.py` (97/97, zero seams), `ops_api.py` (**25/25 units + 37/37 class methods**, the first mixin-leaf split), `discovery_service.py` (**4/4 units + 37/37 class methods**, the second mixin-leaf split, zero seams), `task_lifecycle.py` (**30/30 units + 32/32 class methods**, the first hybrid split — module-level helper chains to helper leaves + class to mixin leaves, zero seams), `registry_conflict_adjudication.py` (**52/52 units**, function-leaf split; the verbatim `__all__` tail folds into the run leaf's `overlay_adjudication` unit per the checker's unit regex, so the run leaf carries it byte-identically and the coordinator's own `__all__` sits before its first matched unit; two test seams retargeted: `derive_registry_conflict_queue` patch → run leaf, `_probe_row` patch → decide leaf, `probe_source_evidence`/`try_fetch_with_playwright` patches + `_parse_jobs`/`_probe_row` imports → probe leaf), `registry_conflicts_row.py` (**110/110 units**, six-leaf split; the original was the uncommitted trimmed working-tree file, reconstructed as HEAD-minus-the-dead-`TRIAGE_BUCKETS`/`REVIEW_QUEUES`-blocks and validated 87/87 against the leaves before the full `--snapshot` run; zero seam migrations — the 73-name re-export surface and `ruff.toml` per-file-ignore keep all 9 importers unchanged; `_row_urls` moved into the core leaf to break the core↔identity cycle), `static_detail_heuristics.py` (**39/39 units**, four-leaf split into `static_detail_heuristics_{config,filter,parse,entry}.py`; the `extract_rendered_card_jobs` assignment stays in the coordinator and the other 10 names re-export via `ruff.toml` per-file-ignore; two `parse_jobpostings_from_html` monkeypatch seams retargeted to the entry leaf) — all `ALL BYTE-IDENTICAL`; `static_listing.py` (59/65 byte-identical + 6 units differing only by the documented `_sl.` call-time seam imports; the traversal-leaf `process_detail_html` seam was later removed by importing the heuristics leaf directly, moving that unit to byte-identical) and `source_registry_io.py` (91/106 + 15 `_srio.` seam units, incl. the `DATA_DIR` root-injection seam) — every non-identical unit is a seam rewrite, verified by strip-normalization, no content drift.
- Tool fixes found by running it on the above (2026-08-19): `_git_show_head` now decodes `git show` output as UTF-8 (was locale/cp1252 on Windows → false DIFFs on non-ASCII originals like `static_listing.py`'s 177 non-ASCII bytes); top-level decorators are now unit boundaries owned by the following def/class (a `@dataclass` line was leaking into the preceding unit's slice → spurious DIFF); the summary line's byte-identical count is now honest (owned minus diffed, denominator = ref units). New `--class-methods CLASS` mode fingerprints the composed class's methods individually across mixin leaves (for class-based splits), skipping `raise NotImplementedError`/`...` stubs that mixin state bases use for mypy typing, and the method pass now correctly flips the exit code. All guarded by `tests/test_repo_health_split_fidelity.py` (20 tests, incl. decorator, UTF-8, class-methods, and trailing-`__all__`-fold cases).

Fidelity alone is necessary but not sufficient — every split also verifies:

1. **Re-export surface**: coordinator attrs must `is` the leaf objects (identity check at import time).
2. **Seams**: monkeypatch-visible names keep call-time `_mod.` resolution through the coordinator; root-injection seams (e.g. the `DATA_DIR` rebind) stay coordinator-owned.
3. **Complexity baseline**: re-key `scripts/complexity_baseline.json` `path::symbol` entries whose functions moved to a leaf (the gate fails on un-baselined findings).
4. **Ship-bundle closure**: `test_build_ship_bundle_import_closure.py` pins top-level `src/*.py`; add new top-level leaves there (bridge/`src/**` files are auto-included via `rglob`).
5. **Full suite + full-project mypy + repo guardrails + precommit gate**, then refresh §3F and the Codebase Snapshot tables when a hotspot is split.

---

## Findings Summary

| Category | Count | Actionable |
|----------|-------|-----------|
| Truly dead files (facades) | **2** | `state.py` (7 consumers, originally miscounted as 2), `parsers.py` (4 consumers, originally miscounted as 2) |
| Merge candidates | **2** | `fetcher_compat_exports.py`, `fetcher_compat_runtime.py` |
| Duplicated type-coercion helpers | **3 functions x 4/3/2 files** | `_as_list`, `_as_dict`, `_as_dict_rows` (pipeline_finalize.py only has `_as_list`, not `_as_dict` as originally claimed) |
| Duplicated report builder | **2 files** | `build_excluded_source_report` |
| Root injection duplication (jobs only) | **4 modules, 3 injection points** | Unify to single point |
| Large files to split | **0 primary now** | `reporting_dedup_evidence.py` (done), `dedup.py` (done 2026-08-19), `canonicalize_google_sheets.py` (done), `static_listing.py` (done), `source_registry_io.py` (done), `static_detail_heuristics.py` (done) — remaining >1,000: `state_lifecycle.py` (1,121), see §3F |
| Potentially dead scripts | **16 scripts (~7,600 lines)** | Investigate `source_policy_soak_report.py` (3,711 lines) |
| Tool false positives | **4** | `analyze_refactorability.py` regex over-match on both `from` and `import` sides |

**Total savings from Phases 1-3**: ~6 files removed/merged, ~1,140 lines of facades/shim/duplicated helpers eliminated, 4 modules consolidated to single root injection point, ~7,600 lines of potentially dead scripts identified.

**Phase 1 effort revised upward** (2026-05-29 loophole audit): ~2h → **~4h**. Consumer counts were undercounted by 5 (state.py) and 2 (parsers.py). The parsers.py `fetcher_compat_exports.py` compat table rewrite is a mechanical 29-entry mapping across 4 source modules plus 1 inline wrapper — verified all symbols exist in canonical source modules, signatures confirmed identical, no risk of circular imports. Two additional loopholes closed: `src/jobs/__init__.py` imports `parsers`/`state` (must remove), and 3 test files import `state` through the package (must update to leaf imports). Phase 1 post-audit confidence: **99%** — item #2 (parsers compat table) promoted from 95% to 99% after verifying all 29 symbol-to-module mappings and closing __init__.py loophole.

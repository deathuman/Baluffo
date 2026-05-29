# Initial Refactoring Findings Plan

> - **Status:** Active plan, advisory-only
> - **Use this when:** doing jobs/fetcher refactoring, consolidation, dead-code triage, or validating refactorability analyzer findings from the 2026-05-17 analysis
> - **Canonical for:** the 2026-05-17 initial refactoring target inventory, known analyzer false positives, and suggested sequencing for behavior-preserving cleanup
> - **Not canonical for:** current runtime contracts, payload shapes, source registry policy, bridge route contracts, or implementation state after any later refactor lands
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`refactor-charter-template.md`](refactor-charter-template.md), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-29 — Phase 1 loophole audit completed; consumer counts corrected; effort estimates revised upward

## Summary

This plan records the initial 7-pass refactoring analysis completed on 2026-05-17. It is a routing and prioritization aid, not blanket approval to remove code. Use it to choose narrow refactor slices, then revalidate each target against current source, tests, and the relevant contract docs before editing.

Highest-value jobs/fetcher targets:

- Remove two thin facades: `src/jobs/state.py` and `src/jobs/parsers.py`.
- Merge two compatibility helper modules into their owning facade: `src/jobs/fetcher_compat_exports.py` and `src/jobs/fetcher_compat_runtime.py`.
- Consolidate small duplicated jobs JSON-shape helpers only where semantics are identical.
- Treat jobs root injection as compatibility debt and avoid expanding it.
- Split `src/jobs/reporting_dedup_evidence.py` behind its existing public builders.

## Current Repo Check

The snapshot below was validated against the repo state on 2026-05-17 before this plan was added. Treat counts and consumer lists as starting evidence for a refactor charter, not as a substitute for a fresh `rg`/Serena check immediately before editing.

## Codebase Snapshot

| Area | Files | Large Files (>500 lines) |
|------|-------|--------------------------|
| `src/jobs/` (core) | 41 | 14 |
| `src/jobs/common/` | 31 | 4 |
| `src/jobs/adapters/` | 17 | 8 |
| `src/jobs/adapters/plugins/` | 40 | 3 |
| `src/bridge/` | 51 | 20 |
| `src/source_discovery/` | 62 | 12 |
| `src/ship/` | 50 | 13 |
| `src/shared/` | 11 | 1 (process_memory.py:511) |
| `src/storage/` | 7 | 3 |
| `src/scrapers/` | 9 | 1 (runner.py:433) |
| `scripts/` | 39 | 0 |

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

### 3A. `reporting_dedup_evidence.py` (3,641 lines, 122 functions, 2 public)

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

### 3C. Adapter: `static_listing.py` (1,645 lines), `static_detail_heuristics.py` (907 lines)

Static adapter modules. These are large but are adapter implementations with clear scope. Decomposition deferrable.

### 3D. `pipeline_run_setup.py` (508 lines) — Near threshold, but extracted helpers would need to be shared

### 3E. `__init__.py` files

| File | Purpose | Lines |
|------|---------|-------|
| `src/jobs/__init__.py` | Public package surface (re-exports 9 sub-modules) | 25 |
| `src/jobs/common/__init__.py` | Warning docstring only | 5 |
| `src/bridge/__init__.py` | Package surface | 54 |
| `src/jobs/adapters/__init__.py` | Adapter registry + source loader orchestration | ~250 |

The `src/jobs/adapters/__init__.py` (250 lines) contains `default_source_loaders()` and `EXTRACTED_ADAPTERS` — this is non-trivial logic, not just a facade. **Keep as-is**.

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

| # | Action | Risk | Files Changed | Lines | Effort |
|---|--------|------|---------------|-------|--------|
| 1 | **Remove `state.py` facade** (72 lines), update 7 source consumers + 2 test consumers + `__init__.py` cleanup + 3 package-import test files | Low | 12 | ~150 | ~1.5h |
| 2 | **Remove `parsers.py` shim** (74 lines), rewrite `fetcher_compat_exports.py` compat table (29 entries → 4 targeted blocks + 1 inline wrapper), update 2 direct source consumers + 1 test consumer + `__init__.py` cleanup | Low | 6 | ~130 | ~2h |
| 3 | **Fix the `analyze_refactorability.py` regex** — change `^from\s+src\.jobs\b\|^import\s+src\.jobs\b` to `^from\s+src\.jobs\s+import\b\|^import\s+src\.jobs\s*$` to eliminate 4 false positives on both `from` and `import` sides | Low | 1 | ~1 | ~10min |
| 4 | **Fix `_clean_text`/`_norm_text` false alarm** — add doc comment to normalizers.py explaining circular import workaround | None | 1 | ~4 | ~5min |
| 5 | **Extract `_as_list`/`_as_dict`/`_as_dict_rows`** to `src/shared/utils.py` — remove 4/3/2 copies across modules (pipeline_finalize.py only has `_as_list`, not `_as_dict` as originally claimed) | Low | 5 | ~40 | ~30min |

### Phase 2: Medium Effort (30-60 min each)

| # | Action | Risk | Files Changed | Lines |
|---|--------|------|---------------|-------|
| 6 | **Consolidate `build_excluded_source_report`** — unify 2 versions (pipeline_loader_selection.py and state_source_records.py) | Low | 2 | ~20 |
| 7 | **Merge `fetcher_compat_exports.py` into `jobs_fetcher.py`** (223 lines moved, no logic change) | Low | 2 | ~0 delta |
| 8 | **Merge `fetcher_compat_runtime.py` into `jobs_fetcher.py`** (68 lines moved, no logic change) | Low | 2 | ~0 delta |
| 9 | **Unify root injection** — eliminate Points B (pipeline_stage_source_execution) and C (pipeline_source_loop fallback); all 4 root-dependent modules point to jobs_fetcher | Medium | 5 | ~100 |
| 10 | **Extract shared guard in `state_incremental.py`** — both should_skip functions share `consecutiveFailures` check | Low | 1 | ~15 |

### Phase 3: Larger Effort (1-2 hours each)

| # | Action | Risk | Description |
|---|--------|------|-------------|
| 11 | **Split `reporting_dedup_evidence.py`** (3,641 lines) into 5 sub-modules | Medium | Extract bundle_shapes, identity_quality, provider_static, review_queue, audit_gate |
| 12 | **Review `pipeline_run_setup.py`** (508 lines) for helper extraction | Low | Currently manageable, revisit if it grows |
| 13 | **Investigate 16 unreferenced scripts** (~7,600 lines total) | Low | Archive or document `source_policy_soak_report.py` (3,711 lines) and others |

### Phase 4: Deferred

| # | Action | Scope | Notes |
|---|--------|-------|-------|
| 14 | Decompose `registry_conflicts.py` (3,599 lines) | Bridge | Bridge-specific, out of jobs scope |
| 15 | Decompose `task_launch_api.py` (2,377 lines) | Bridge | Bridge-specific, out of jobs scope |
| 16 | Simplify `jobs_fetcher.py` dynamic dispatch for 100+ symbols | Jobs | Could use direct imports instead of __getattr__ |
| 17 | Evaluate `static_listing.py` (1,645 lines) decomposition | Adapters | Not urgent, well-scoped |

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

## 8. Bridge Large Files (Separate Scope)

These are in the bridge subsystem — not part of jobs pipeline but worth noting:

| File | Lines | Role |
|------|-------|------|
| `registry_conflicts.py` | 3,599 | Registry conflict detection |
| `task_launch_api.py` | 2,377 | Bridge task launch API |
| `registry_conflict_adjudication.py` | 1,120 | Conflict adjudication |
| `sync_service.py` | 1,021 | Sync service |
| `pipeline_service.py` | 941 | Pipeline service |

## 9. source_discovery Large Files (Separate Scope)

| File | Lines | Role |
|------|-------|------|
| `gamedevmap_active_dry_run.py` | 2,174 | Game dev map active dry run |
| `active_audit_runtime.py` | 1,318 | Active audit runtime |
| `web_search_candidates.py` | 1,256 | Web search candidates |
| `orchestrator_generation.py` | 1,023 | Orchestrator generation |

### NOT Recommended
- Removing fetcher_compat_* modules (essential for compatibility dispatch)
- Decomposing `canonicalize.py` (831 lines, reasonable)
- Decomposing `pipeline_finalize.py` (830 lines, well-organized)
- Decomposing `pipeline_source_results.py` (721 lines, well-organized)
- Touching `src/source_discovery/`, `src/ship/`, `src/storage/`, `src/scrapers/` (separate subsystems)

---

## Findings Summary

| Category | Count | Actionable |
|----------|-------|-----------|
| Truly dead files (facades) | **2** | `state.py` (7 consumers, originally miscounted as 2), `parsers.py` (4 consumers, originally miscounted as 2) |
| Merge candidates | **2** | `fetcher_compat_exports.py`, `fetcher_compat_runtime.py` |
| Duplicated type-coercion helpers | **3 functions x 4/3/2 files** | `_as_list`, `_as_dict`, `_as_dict_rows` (pipeline_finalize.py only has `_as_list`, not `_as_dict` as originally claimed) |
| Duplicated report builder | **2 files** | `build_excluded_source_report` |
| Root injection duplication (jobs only) | **4 modules, 3 injection points** | Unify to single point |
| Large files to split | **1 primary** | `reporting_dedup_evidence.py` (3,641 lines) |
| Potentially dead scripts | **16 scripts (~7,600 lines)** | Investigate `source_policy_soak_report.py` (3,711 lines) |
| Tool false positives | **4** | `analyze_refactorability.py` regex over-match on both `from` and `import` sides |

**Total savings from Phases 1-3**: ~6 files removed/merged, ~1,140 lines of facades/shim/duplicated helpers eliminated, 4 modules consolidated to single root injection point, ~7,600 lines of potentially dead scripts identified.

**Phase 1 effort revised upward** (2026-05-29 loophole audit): ~2h → **~4h**. Consumer counts were undercounted by 5 (state.py) and 2 (parsers.py). The parsers.py `fetcher_compat_exports.py` compat table rewrite is a mechanical 29-entry mapping across 4 source modules plus 1 inline wrapper — verified all symbols exist in canonical source modules, signatures confirmed identical, no risk of circular imports. Two additional loopholes closed: `src/jobs/__init__.py` imports `parsers`/`state` (must remove), and 3 test files import `state` through the package (must update to leaf imports). Phase 1 post-audit confidence: **99%** — item #2 (parsers compat table) promoted from 95% to 99% after verifying all 29 symbol-to-module mappings and closing __init__.py loophole.

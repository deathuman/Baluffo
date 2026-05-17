# Initial Refactoring Findings Plan

> - **Status:** Active plan, advisory-only
> - **Use this when:** doing jobs/fetcher refactoring, consolidation, dead-code triage, or validating refactorability analyzer findings from the 2026-05-17 analysis
> - **Canonical for:** the 2026-05-17 initial refactoring target inventory, known analyzer false positives, and suggested sequencing for behavior-preserving cleanup
> - **Not canonical for:** current runtime contracts, payload shapes, source registry policy, bridge route contracts, or implementation state after any later refactor lands
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`refactor-charter-template.md`](refactor-charter-template.md), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-17

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

### 1A. `src/jobs/state.py` (72 lines) — Facade, 2 consumers

**What it does**: Re-exports 27 symbols from `state_source_state.py`, `state_lifecycle.py`, `state_incremental.py`.

**Consumers**:
| Consumer | Import Pattern |
|----------|---------------|
| `src/source_discovery/orchestrator.py` | `from src.jobs.state import read_source_state` (1 real symbol) |
| `src/jobs/fetcher_compat_exports.py` | `from src.jobs import state as state_mod` (for compat dispatch table) |

**Bypassed by** (5 modules that import state sub-modules directly, not via state.py):
- `src/jobs/pipeline_cli.py` → `state_source_state`, `state_incremental`
- `src/jobs/pipeline_execution_flow.py` → `state_source_state`
- `src/jobs/pipeline_finalize.py` → `state_source_state`, `state_lifecycle`
- `src/jobs/pipeline_run_setup.py` → `state_source_state`, `state_lifecycle`, `state_incremental`
- `src/bridge/task_launch_api.py` → `state_lifecycle`

**Verdict**: **REMOVE**. Two changes needed:
1. Replace `from src.jobs.state import read_source_state` in `source_discovery/orchestrator.py` with `from src.jobs.state_source_state import read_source_state`
2. Update `fetcher_compat_exports.py` to reference `state_source_state_mod`, `state_lifecycle_mod`, `state_incremental_mod` directly instead of via `state_mod`

### 1B. `src/jobs/parsers.py` (74 lines) — Shim facade, 2 consumers

**What it does**: Re-exports ~25 parser functions from `adapters/html_parsers`, `adapters/provider_parsers`, `adapters/social_parsers`, and `adapters/community`. Also provides `parse_jobpostings_from_html` and `parse_remote_ok_payload` thin wrappers.

**Consumers**:
| Consumer | Import Pattern |
|----------|---------------|
| `src/source_discovery/probe.py` | `from src.jobs.parsers import parse_jobpostings_from_html` |
| `src/jobs/fetcher_compat_exports.py` | Uses `parsers_mod` for compat dispatch |

**Bypassed by**:
- `src/jobs/page_gating.py` → imports directly from `adapters/html_parsers`

**Verdict**: **REMOVE** with caveats. The `parse_jobpostings_from_html` function is a thin wrapper around `_html_parsers.parse_jobpostings_from_html` — can be imported directly. The `parse_remote_ok_payload` wrapper adds `looks_like_game_job` arg and would need to move to `adapters/community/__init__.py` where `run_remote_ok_source` lives. Two changes needed:
1. Move `parse_remote_ok_payload` wrapper into `adapters/community/__init__.py`
2. Update `fetcher_compat_exports.py` to reference modules directly

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

### 2A. Type-coercion helpers `_as_list`/`_as_dict`/`_as_dict_rows` (4 modules)

Tiny guard functions duplicated across files:

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

**Root cause**: Analyzer regex `^from\s+src\.jobs\b` — the `\b` word boundary doesn't work as intended between a word char and a dot. The fix would be to tighten the regex to `^from\s+src\.jobs\s+import` (bare package import) or use `^from\s+src\.jobs(?:\s|\.)` to distinguish `from src.jobs import X` (root import) from `from src.jobs.text_utils import X` (leaf import).

---

## 5. Other Non-Issues

| Potential Issue | Verdict | Reason |
|----------------|---------|--------|
| `normalizers.py` private `_clean_text`/`_norm_text` | **Not duplication** | Circular import workaround |
| `state_incremental.py` consecutiveFailures guard | **Minor, not worth standalone refactor** | Same file, 15 lines |
| `parsers.py` → `source_discovery/probe.py` import | **Can be fixed if parsers.py is removed** | Single consumer |
| `state.py` → `source_discovery/orchestrator.py` import | **Can be fixed if state.py is removed** | Single consumer |
| `src/jobs/adapters/plugins/static/*.py` (28 files) | **Keep** | Company-specific scrapers |
| `src/source_discovery/` (62 files) | **Not in scope** | Separate subsystem |
| `src/ship/` (50 files) | **Not in scope** | Desktop app + update system |
| `src/storage/` (7 files) | **Not in scope** | Storage layer |
| `src/scrapers/` (9 files) | **Not in scope** | Scrapy spider project |
| `common/` modules (31 files) | **All used** | None are dead |

---

## 6. Integrated Action Plan

### Phase 1: Trivial Quick Wins (< 30 min each)

| # | Action | Risk | Files Changed | Lines |
|---|--------|------|---------------|-------|
| 1 | **Remove `state.py` facade** (72 lines), update 2 consumers | Low | 3 | ~80 |
| 2 | **Remove `parsers.py` shim** (74 lines), inline wrapper into `adapters/community/__init__.py`, update 2 consumers | Low | 4 | ~90 |
| 3 | **Fix the `analyze_refactorability.py` regex** — change `^from\s+src\.jobs\b` to `^from\s+src\.jobs\s+import` to eliminate 4 false positives | Low | 1 | ~1 |
| 4 | **Fix `_clean_text`/`_norm_text` false alarm** — add doc comment to normalizers.py explaining circular import workaround | None | 1 | ~4 |
| 5 | **Extract `_as_list`/`_as_dict`/`_as_dict_rows`** to `src/shared/utils.py` — remove 4/3/2 copies across modules | Low | 5 | ~40 |

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
| Truly dead files (facades) | **2** | `state.py`, `parsers.py` |
| Merge candidates | **2** | `fetcher_compat_exports.py`, `fetcher_compat_runtime.py` |
| Duplicated type-coercion helpers | **3 functions x 4/3/2 files** | `_as_list`, `_as_dict`, `_as_dict_rows` |
| Duplicated report builder | **2 files** | `build_excluded_source_report` |
| Root injection duplication (jobs only) | **4 modules, 3 injection points** | Unify to single point |
| Large files to split | **1 primary** | `reporting_dedup_evidence.py` (3,641 lines) |
| Potentially dead scripts | **16 scripts (~7,600 lines)** | Investigate `source_policy_soak_report.py` (3,711 lines) |
| Tool false positives | **4** | `analyze_refactorability.py` regex over-match |

**Total savings from Phases 1-3**: ~6 files removed/merged, ~1,140 lines of facades/shim/duplicated helpers eliminated, 4 modules consolidated to single root injection point, ~7,600 lines of potentially dead scripts identified.

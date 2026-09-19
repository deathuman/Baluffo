# Codebase Simplification Program

Active plan. Tracks the LOC-reduction, de-duplication, and legibility program for
Baluffo, with the measured baseline, the guardrail constraints that bound it, and
the per-workstream evidence.

## Goal

Cut tracked source lines dramatically, break up god files, unify reusable
helpers, remove branch-chain routing, and raise the interpretability of how the
codebase connects. Reduce bloat without deleting behavior or coverage.

## Measured Baseline (2026-09-18)

`python tools/repo_health/loc_budget.py --report` — tracked files only, `docs/`
and `data/` excluded.

| Area | Lines | Share |
|---|---:|---:|
| tests | 206,693 | 43.6% |
| src | 172,495 | 36.4% |
| frontend | 45,694 | 9.7% |
| scripts | 23,245 | 4.9% |
| tools | 13,094 | 2.8% |
| styles | 9,896 | 2.1% |
| `<root>` | 1,858 | 0.4% |
| probes | 758 | 0.2% |
| **Total** | **473,733** | 100% |

## Wave 1 Result (measured, landed)

Three workstreams landed as three independently revertable commits. All 14
guardrail groups pass; the full suite reports 5,410 passed with 6 failures that
are a pre-existing missing-Playwright-Chromium-binary environment issue in
`tests/test_browser_fallback_pool.py`, unrelated to these changes.

| Workstream | Commit | Measured |
|---|---|---:|
| W1 shared helper unification | `ab9d3481` | **−207** |
| W6 scripts / tools | `9b5c1508` | **+1,391** |
| W5 test consolidation | `df485e83` | **−444** |
| **Net** | | **+740** |

The plan projected −29,000…−42,500 for these three. They delivered **+740**. Three
projections were wrong, each in the same direction, and the reasons are worth
recording because they invalidate the remaining estimates too:

1. **Duplication is far smaller than the scan suggested.** W1's projection of
   −2,000…−3,500 assumed 1,154 duplicated lines were removable. The real number
   is 207, because a duplicated 2-line helper that becomes a 1-line import saves
   one line — and the shared module that hosts it costs lines back. The 34-copy
   `_as_dict` group is worth 34 lines, not 68.
2. **Splitting a god file ADDS lines.** W6's projection was −4,000…−7,000;
   it measured **+1,391**. Each leaf needs its own import preamble, module
   docstring, and compatibility `__all__` list. Splitting
   `source_policy_soak_report.py` (5,015 → 1,767 + 6 leaves) cost **+919**;
   `perf_complete.py` (2,516 → 749 + 7 leaves) cost **+423**. God-file
   decomposition is a legibility purchase paid for in lines, not a reduction.
3. **`git ls-files` does not see untracked files.** New modules are invisible to
   `loc_budget.py` until staged, so every "before/after" measured on a dirty tree
   overstates the reduction. W5 first reported −872 and was really −444; W6 first
   reported −5,015 and was really +1,342. **Stage before measuring.**

## The 30% Arithmetic Problem

30% of 473,733 is **142,120 lines**. Product code — src + frontend + scripts +
tools + styles — is **264,384 lines** in total. Deleting *every line of scripts,
tools, and styles* reaches only 9.7%.

There is no evidence-backed path to 30% that does not delete tests wholesale.
Bottom-up, the safe workstreams below sum to roughly **−10,000 to −17,000**
(2%–4%), with the test corpus dominating any larger number. This plan therefore
optimizes for *maximum safe reduction*, reports the measured delta per
workstream, and treats 30% as a stated target that the evidence does not
currently support rather than a number to be reached by deleting coverage.

Two facts drive that conclusion and are recorded here so the estimate is not
re-litigated:

- **The test corpus is not duplicated.** Whole-file AST signatures normalized
  over constants, names, and attributes produce **zero** duplicate groups across
  719 Python and 216 `.mjs` test files. Literal-exact duplicate test bodies
  total 6 groups / 46 lines; literal-normalized duplicates total 84 groups /
  2,389 lines; duplicate helper functions total 216 groups / 2,116 lines.
- **Inline fixture literals are the largest single mass in tests.** 276 literal
  nodes of ≥30 lines hold 14,372 lines, and 1,148 nodes of ≥15 lines hold 31,758
  lines. Sharing them is bounded by how much of each literal is genuinely
  shared, which is much less than its size.

## Guardrail Constraints

Every workstream runs against these. They are enforced, not advisory.

| Constraint | Owner | Consequence |
|---|---|---|
| `loc_budget.py` — no area grows, no reduction goes un-ratcheted | coordinator | Every reduction must ratchet `loc_baseline.json` in the same change |
| `duplicate_body_policy.py` — fails on stale baseline entries | coordinator | Each consolidated pattern must be pruned from `duplicate_bodies_baseline.json` in the same change |
| `check_line_budget` — tests max 400 (800 integration), max-only ratchet | coordinator | Shrinking is free; growing is not; shrinking must ratchet down same-change |
| `complexity_baseline.json` — fails only on new/worsened hotspots | coordinator | Shrinking is free |
| `suite_contract_policy.py` — line ceilings and top-level function-name caps | coordinator | Roots may not exceed ceilings; consolidation means deliberately deleting structural assertions |
| `frontend_structure_guardrails.mjs` — per-slice shapes and budgets | coordinator | Same |
| `bridge_route_inventory.py` — 91 routes (42 GET, 49 POST) | coordinator | The route surface is a compatibility contract |
| `source_suppression_budget.json` — max 10 comments, one `BLE001` site | coordinator | No new suppressions |

**The guardrails actively mandate the current decomposition.** Consolidation
therefore means deliberately deleting structural assertions from
`suite_contract_policy.py` and `frontend_structure_guardrails.mjs`. Each removal
must record what protection is lost. This is the program's main risk.

## Workstreams

| ID | Scope | Expected | Status |
|---|---|---:|---|
| W0 | Tracked-source LOC ratchet gate | — | **landed** `c0459a62` |
| W1 | Shared helper unification (`src/shared/`) | −2,000…−3,500 | **landed** `ab9d3481` — measured **−207** |
| W6 | scripts / tools | −4,000…−7,000 | **landed** `9b5c1508` — measured **+1,391** |
| W5 | Test consolidation | −23,000…−32,000 | **landed** `df485e83` — measured **−444** |
| W2 | God-file decomposition (152 src files >400 LOC) | ±0 (legibility) | re-estimated **+0.5…+1.5%** (costs lines) |
| W2b | `AI boundary` banner collapse | −900 | **declined** — see below |
| W3 | Route de-chaining | −500…−1,500 | pending |
| W7 | Frontend decomposition | −3,000…−5,000 | re-estimated **costs lines** (same as W6) |
| W4 | Re-export shim + Protocol consolidation | −1,500…−3,000 | re-estimated **≤ −2,068** hard ceiling |
| W8 | Static plugin table-ization | −800…−1,200 | pending |

### Why the remaining estimates were revised down

Every projection above was built from the same flawed assumption: that a
duplicated or verbose construct converts its full line count into savings. It
does not. Measured ceilings for what is left:

- **W2 / W7 (god-file splits) cost lines.** W6 is the direct evidence: two of the
  largest god files in the repo, split with byte-fidelity proof, cost +1,342.
  The remaining 152 `src` files over 400 LOC will behave the same way. These are
  worth doing for legibility; they are not a reduction lever and must not be
  counted as one.
- **W4 (shims) has a hard ceiling of 2,068 lines** — the total size of all 30
  pure re-export shims ≥20 lines. Deleting every one of them (0.44% of the
  repo) would break the compatibility surfaces `suite_contract_policy.py`
  asserts, so the realistic yield is a fraction of that.
- **W2b is declined.** The 1,238 banner lines across 328 files are **1,107
  distinct** `(kind, text)` pairs — they are bespoke per-file navigation hints,
  not repeated boilerplate. Collapsing them to one line per file saves 910 lines
  (0.19%) and destroys the routing information that makes the codebase
  navigable. That is the opposite of the program's stated goal.
- **W3 and W8 are the only untested levers** whose shape (branch chains →
  declarative tables, N files → 1 table) genuinely converts many lines into few.
  W8's static plugin family is 49 files / 6,635 lines, so its ceiling is real but
  bounded at ~1,000.

### Revised program ceiling

Adding the measured W1 result to the revised ceilings for what remains gives a
realistic total of **−4,000 to −7,000 lines (0.8%–1.5%)**, with the *only*
substantial further reduction available being deletion of the test corpus or of
the compatibility surfaces the guardrails exist to protect.


## Ownership

**Coordinator owns:** architecture decisions, `repo_guardrails.py` wiring, every
baseline edit (`loc_baseline.json`, `duplicate_bodies_baseline.json`,
`complexity_baseline.json`, `test_line_budget_baseline.json`), cross-workstream
integration, doc updates, and final acceptance.

**Workers own:** one bounded goal each, inside an explicit writable scope.
Workers never delegate, never touch baselines or guardrail wiring, and return
immediately once their assigned validation passes.

## Verification Protocol

1. `python tools/repo_health/loc_budget.py --report` before and after — the delta
   is the deliverable.
2. `python tools/repo_health/repo_guardrails.py` — the full gate.
3. Focused tests for the touched area.
4. `python -m ruff check` and `python -m ruff format --check` on changed files.
5. `python tools/repo_health/bin/verify_split_fidelity.py` for every module split.

## Rollback

One commit per workstream, so `git revert <sha>` isolates any regression. Each
workstream's baseline edits live in its own commit and revert with it.

## Out of Scope

- Deleting or weakening behavior or coverage to reach a line count.
- Changing the public route surface (91 routes).
- Packaging, installer, release-tag work.
- Adding Python or Node dependencies.
- Rewriting `docs/archive/`.
- Committing slopo or classifier artifacts (both gitignored).

## Evidence Appendix

### Exact duplicate function bodies (`duplicate_body_policy.py`, 5 patterns)

| Digest | Copies | Avg lines | Name |
|---|---:|---:|---|
| `bc79e9c515f1` | 4 | 5.0 | `_safe_float` |
| `6326d1193860` | 3 | 6.0 | `as_json` |
| `74812c63ce9c` | 3 | 5.0 | `_as_float` |
| `77490ab82fbd` | 3 | 5.0 | `_path_size` |
| `81362ab4777d` | 3 | 4.0 | `_require_root` |

A wider scan (≥2 copies, ≥2 lines, AST-normalized) finds 115 groups / 1,154
lines of potential saving. Largest: `_as_dict` ×34, `_clean_text` ×27,
`_as_list` ×18.

### God files

`src` has **152 files over 400 LOC**. Largest: `source_discovery/gamedevmap_active_dry_run.py`
2,087 (51 defs); `source_sync_shard.py` 1,525; `source_sync_snapshot.py` 1,345;
`ship/packaged_smoke/rehearsal_browser.py` 1,341;
`source_discovery/active_audit_runtime.py` 1,327 (12 classes);
`web_search_candidates.py` 1,263; `fetch_incremental_sanity_benchmark.py` 1,171;
`shared/fetch_report_normalization.py` 1,154; `ship/runtime_launcher.py` 1,103;
`container_gateway.py` 1,095. In `scripts`:
`source_policy_soak_report.py` 4,689 (141 defs), `perf_complete.py` 2,402.

Frontend: `admin/app/ops/health.js` 1,727 (4 functions — one giant factory),
`admin/render/ops-summary.js` 1,156, `admin/render/registry-conflicts.js` 1,113,
`jobs/app/feed.js` 1,021.

### Classifier triage (864 slopo clusters)

`bulk-classify` over all 864 clusters returned 420 "intentional parallel
structure", 258 "coincidental or trivial similarity", 186 "real duplication
worth consolidating". Only 23 scored ≥0.8; 72 fell in 0.5–0.8. Cluster-017
(16 units) was labelled "worth consolidating" at confidence **0.02**.

**Verdict: the classifier is a shortlist generator only; the label column is
near-noise and confidence is the signal. Source decides.** It cannot be repo
infrastructure — it is an external network dependency and adding one is
forbidden.

### Re-export shims

23 candidates totalling 1,028 LOC, led by
`jobs/common/registry_defaults.py` (224), `source_registry_io.py` (149),
`ship/update_manager.py` (103), `jobs/adapters/provider_parsers.py` (67),
`source_discovery/core.py` (63), `bridge/__init__.py` (52).

### Protocols

74 `Protocol` classes across 40 files.

### Tests

719 `.py` (156,867 lines) and 216 `.mjs` (46,307 lines). 5,405 pytest cases;
893 `node:test` cases. Per-file boilerplate is ~13,700 lines (8.8%).
197 `.mjs` files repeat `import test from "node:test"`; 209 repeat
`import assert from "node:assert/strict"`.

Ten test files scan source text to assert structure that `repo_health`
guardrails already enforce (3,094 lines), led by `test_runtime_launcher.py`
(1,056), `test_repo_health_split_fidelity.py` (380), `test_precommit_gate.py`
(356), `test_shared_utils_coercion_ratchet.py` (338).

### CSS

`styles/` is 9,896 lines (`admin.css` 2,944, `components.css` 2,265,
`saved.css` 2,046, `jobs.css` 1,026, `base.css` 294). Only 4 duplicate
selectors — a weak lever.

### Static adapter plugins

`src/jobs/adapters/plugins/static/` is 49 files / 6,635 lines: 27 `_SPEC`
constants, 27 `_parse_html` functions, 25 `run = simple_static_run(_SPEC, _parse_html)`
assignments, 12 files ≤30 LOC.

### `AI boundary` banners

1,240 lines across 327 files in `src` (plus 1 in `tools`). Convention only — no
guardrail or test references them, so collapsing to one ownership line per file
is safe.

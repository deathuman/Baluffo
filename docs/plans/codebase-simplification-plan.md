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
| W1b structural clones | `ebac6401` | **−902** |
| W3 route de-chaining | `02a827af` | **−8** |
| W8 static plugin table | `02a827af` | **−89** |
| W7 ops health split | `02a827af` | **+414** |
| W2a/W2b/W7b god files | `065e0712` | **+2,090** |
| W2a remainder | `50b5fa6b` | **+573** |
| **Net** | | **+2,818** |

### W1b: why the first scan missed most of the duplication

W1 matched function bodies by **AST-normalized exact equality**. That finds
`_as_dict` in file A matching `_as_dict` in file B, but it cannot see *the same
logic with different identifiers and constants* — which is precisely the shape
provider adapters, rehearsal runners, and status renderers take.

Re-scanning with **alpha-renaming** (every identifier → `_V`, every constant →
`_S`/`_N`, attributes and argument names normalized) found **277 groups /
3,369 lines in `src` — 3.3× the exact-match figure.** The same blind spot applied
to `frontend/*.js` and `tests/*.mjs`, which the first pass never scanned for
function duplication at all.

W1b consolidated the substantive subset (median body ≥8 lines):

| Area | Groups | Measured |
|---|---:|---:|
| `src` | 18 | **−176** |
| `frontend` | 19 | **−11** |
| `tests/*.mjs` | 27 | **−715** |
| **Total** | **64** | **−902** |

The wins came from **parameterized factories and tables**, not from moving code:
`_make_sources_runner` replaced 14 near-identical `run_*_sources_source`
wrappers; `_make_rehearsal_runner` replaced 6; a shared 27-line browser-fallback
token list replaced two byte-identical copies. The frontend's −11 is honest: two
new shared modules cost 179 lines of JSDoc'd helpers against 190 lines of
recovered duplication, so the real win there was 17 byte-identical merges, not
a line count.

Two detector traps worth recording, both caught before they caused damage:

- **Over-normalizing string literals is a false-positive machine.** Normalizing
  `"..."`/`'...'`/`` `...` `` to a single token collapsed *every* HTML template
  literal in the frontend into one fake 13-member "clone group" worth 868
  apparent lines. Identifier-only normalization showed those templates are
  genuinely different. The naive number would have sent a worker chasing 13
  non-clones.
- **A 2-line duplicate is worth 1 line, not 2.** It becomes a 1-line import. So
  the 1,297 lines of 2–3-line groups are mostly illusory, and the 83 groups with
  median ≥8 lines were the only ones worth dispatching.

## Wave 2 and 3: god-file decomposition (legibility, not reduction)

Decomposition **costs** lines, measured three times over:

| Workstream | Coordinator | Leaves | Net |
|---|---:|---:|---:|
| W6 `scripts/` (earlier) | −3,749 | +5,140 | **+1,391** |
| W7 `health.js` | −938 | +1,352 | **+414** |
| W2a `gamedevmap_active_dry_run.py` | −737 | +1,045 | **+308** |
| W2b shard + snapshot + normalization | −2,841 | +3,504 | **+663** |
| W7b four frontend files | −3,645 | +4,754 | **+1,109** |

Every leaf needs its own import preamble, module docstring with the 4-line AI
boundary banner, and compatibility `__all__`. That is roughly **+25–35% of the
extracted body**. Decomposition is therefore a **legibility purchase**: it buys
navigability and blast-radius reduction, and it *raises* the LOC line. Do not
schedule it as a reduction lever.

### The god *function* is the real god file

W7 split `frontend/admin/app/ops/health.js` 1,726 → 788 lines and the remaining
788 turned out to be **one 672-line factory function**. The same pattern then
repeated on four more files:

| File | Total | Dominant single function |
|---|---:|---|
| `frontend/admin/app/registry/load.js` | 978 | `createRegistryLoadController` **891 lines** |
| `frontend/jobs/app/feed.js` | 1,020 | `initJobsFeed` **415 lines** |
| `frontend/admin/app/ops/health.js` | 788 | `createOpsHealthController` **672 lines** |

Extracting top-level helpers does nothing for these, because the file *is* one
function. Reducing them means changing how inner closures receive `refs`/`state`/
`deps` — an architecture decision, not a mechanical split. W7b did reduce all four
(ops-summary 1,155→142, registry-conflicts 1,108→102, feed 1,020→229, load
978→143) by extracting coherent groups of inner functions, but the factory bodies
that remain are still the largest units in those files.

### Verification that actually catches split defects

`tools/repo_health/bin/verify_split_fidelity.py` fingerprints every top-level unit
of the original and asserts each appears exactly once, byte-identical, across the
leaves. Measured on Wave 3:

| Split | Result |
|---|---|
| `gamedevmap_active_dry_run.py` | **58/58 byte-identical** |
| `source_sync_shard.py` | **59/59 byte-identical** |
| `fetch_report_normalization.py` | **45/45 byte-identical** |
| `source_sync_snapshot.py` | 41/45 — 4 reported `AMBIGUOUS` |

**The 41/45 is a false positive, verified rather than assumed.** The 4 units
(`read_remote_snapshot`, `write_remote_snapshot`, `pull_and_merge_sources`,
`push_sources_snapshot`) exist twice: the real body in a leaf, plus a
**lazy-import delegator** in the coordinator. `inspect.signature` is identical on
all four pairs, and patching the leaf proved the coordinator forwards. The tool's
own docstring states it does not model "seam observability, monkeypatch
compatibility, or re-export surfaces" — a delegator is exactly that gap. Do not
"fix" the 41/45 by deleting the delegators; the lazy import is deliberate so the
coordinator is fully initialized before any leaf body runs, in either import order.

### Two real defects the splits introduced, both caught by tests

1. **Monkeypatch seams break when a function moves.** `test_gamedevmap_active_dry_run.py`
   patches `fetch_directory_pages` on the *coordinator*; an early emit placed its
   readers in a leaf, so the patch silently missed. The seam-forced units were
   pinned back to the coordinator. **When splitting, every monkeypatched global's
   readers must stay in the module the test patches.** The same rule will apply to
   `active_audit_runtime.run_active_audit_batch`
   (`test_active_audit_runtime_batch.py:405`).
2. **New top-level `src/*.py` files must be registered in the ship bundle.**
   `scripts/ship_bundle_manifest.py` enforces that every top-level `src/` file is
   in `APP_RUNTIME_SCRIPTS` or `NON_SHIPPING_TOP_LEVEL_MODULES`. Ten new top-level
   leaves broke 7 bundle tests. Subdirectory leaves need no entry —
   `build_ship_bundle.py` copies `APP_RUNTIME_SCRIPT_DIRS` wholesale — so only the
   top-level ones matter.

### Where the god-file backlog now stands

| Measure | Before Wave 2/3 | After |
|---|---:|---:|
| `src` files >600 lines | 71 / 60,441 lines | **67 / 53,768 lines** |
| Largest `src` file | 2,087 | **1,350** |
| Frontend god files split | 0 | **5** (`health`, `ops-summary`, `registry-conflicts`, `feed`, `registry/load`) |

**46 `src` files over 700 lines remain, totalling 40,217 lines.** The five worst
are now all in the 1,300–1,350 band rather than one 2,087 outlier:
`gamedevmap_active_dry_run.py` (1,350), `rehearsal_browser.py` (1,342),
`fetch_incremental_sanity_benchmark.py` (1,171), `runtime_launcher.py` (1,103),
`container_gateway.py` (1,095).

Frontend still has 10 files over 500 lines, led by
`frontend/jobs/app/runtime/pipeline-controller.js` (910) and
`frontend/admin/render/ops-summary-dedup.js` (903).

Continuing at the measured rate would cost roughly **+25–35% of each extracted
body** — so finishing the remaining 46 files would *add* on the order of 10,000
lines. That is the trade: **legibility is bought with LOC, and cannot be bought
with it.**

### Where duplication now stands
| Area | Before W1b | After W1b |
|---|---:|---:|
| `src` (all alpha-renamed groups) | 277 / 3,369 | **269 / 2,928** |
| `src` (median ≥8 lines) | 83 / 1,608 | **73 / 1,131** |
| `tests/*.mjs` | 41 / 948 | **5 / 104** |
| `frontend/*.js` | 56 / 510 | 17 byte-identical merged |

`tests/*.mjs` is effectively exhausted (89% cleared). `src` still holds 73
groups at median ≥8 lines / 1,131 lines — a legitimate follow-up, but note the
measured yield rate: 18 groups recovered 176 lines, so the remaining 1,131 lines
of *potential* is worth roughly **−300 to −500** in practice, not 1,131.


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
| W3 | Route de-chaining | −500…−1,500 | **landed** `02a827af` — measured **−8** |
| W7 | Frontend decomposition | −3,000…−5,000 | re-estimated **costs lines** (same as W6) |
| W4 | Re-export shim + Protocol consolidation | −1,500…−3,000 | re-estimated **≤ −2,068** hard ceiling |
| W8 | Static plugin table-ization | −800…−1,200 | **landed** `02a827af` — measured **−89** |

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
- **W3 and W8 were the two levers** whose shape (branch chains → declarative
  tables, N files → 1 table) genuinely converts many lines into few. Both landed
  in `02a827af` and both **underdelivered against projection by an order of
  magnitude**: W3 measured −8 against a −500…−1,500 estimate, W8 measured −89
  against −800…−1,200. W8's static plugin family is 49 files / 6,635 lines, so its
  ceiling was real but bounded — and the 14 plugins that carried a branch chain
  were already the ones worth converting; the other 35 had nothing to table-ize.
  W3's routes were similarly already thin: de-chaining 90 lines of dispatch into
  tables recovered 8, because the branch chains the projection counted had mostly
  been de-chained in earlier work.

### Revised program ceiling

Adding the measured W1 and W1b results to the revised ceilings for what remains
gives a realistic total of **−4,000 to −7,000 lines (0.8%–1.5%)** beyond what has
landed, with the *only* substantial further reduction available being deletion of
the test corpus or of the compatibility surfaces the guardrails exist to protect.

Landed so far: **−162 net** across W1, W5, W6, and W1b.

Post-closeout follow-up (clone consolidation round): an exact-body structural
scan of `src/` found 21 byte-identical pairs (235 nominal lines) after W1b.
Worker-driven extraction merged the genuine clusters; the registry 5-helper
group was rejected because its shared leaf nets **−4** (banner overhead
erodes the win) and the social pair is cycle-blocked (`social.py` imports
`register.py`). Landed net: **−76** (`db4f4db7` tip measurement moved
`src` 173,519 → 173,443).


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

## Wave 4 Result (measured, landed)

Wave 4 changed the objective. Waves 1–3 established that reduction and
legibility pull against each other; Wave 4 spent its budget on the thing the
audit turned up instead: **pre-existing defects, and the absence of any gate
that could see them.**

| Unit | Commit | Measured |
|---|---|---:|
| Dead branches, stale loop var, dead-code gate | `5852e219` | +698 |
| Test fixture consolidation, eslint promotion | `ae774644` | −251 |

The +698 is almost entirely the new policy module and the tests that prove the
fixes; the actual defect removals were ~60 lines. **That is the honest trade:
correctness cost lines.** The dead-code gate cannot be smaller than the
detector, and the detector is only trustworthy because it was validated against
a known positive.

### What the dead-code scan found

The scan was built to catch two specific defects and instead found **ten dead
branches** across `src`, `tools`, and `frontend`, plus the stale loop variable
and one orphaned helper. All ten share one shape: an `if` whose body returns the
byte-identical expression as the statement following it, so the condition cannot
change the outcome. None were introduced by the earlier waves.

Verified before writing the detector that **no existing gate could see either
class**: `ruff` (E, F, I, B, UP, and a trial of SIM, RET, PIE, PLR), `vulture
--min-confidence=60`, `mypy`, and `eslint` all reported nothing.

The detector is validated against the real bug as a **positive control** — a
gate that cannot fire is worse than no gate, and this was the specific failure
mode of two earlier attempts at the loop-variable check (one reported 145 false
positives, the next reported 0 while missing the known bug).

### The real bug

`push_changed_shards` read `output` — a stale binding from the earlier
`as_completed` loop — instead of the current `shard_output`. The last-completed
shard's `remoteRequests` diagnostic rows were appended once per shard. No test
covered the multi-shard aggregator; only the single-shard helper.

**The first version of the regression test would not have caught it.** The
natural assertion — `len(remoteRequests) == sum of per-shard counts` — passes
against the buggy code, because every shard contributes the same number of rows
either way. The discriminating assertion is the per-path multiset. This is
recorded because it generalizes: a count assertion is not a content assertion.

### A note on the clone scan's false merges

The alpha-renamed normalizer maps every string literal to `_S` and every number
to `_N`, so bodies that differ **only in fixture values** hash identically. Most
of the raw test-duplication candidates were therefore false merges: ten distinct
`ok_loader` fixtures (Nebula Games / Orion Labs / Circuit Studio / …) collapsed
into one hash group. Merging them would have silently changed what each test
asserts. They were skipped and reported instead.

Only the byte-identical groups were consolidated. Assertion coverage was
verified as *asserts × parametrize multiplicity* — 41→41, 66→70, 78→78, 44→44 —
because raw assert-line counts fall when tests move into a shared body, which
looks like weakening but is not.

### The eslint gate hole

`no-unused-vars` was `"warn"`, and the pre-push hook runs `npx eslint .`, which
**exits 0 on warnings**. Unused variables could therefore never fail any gate.
Clearing 18 findings (13 in `tests`, 5 in `scripts`) let the rule go to `error`
across frontend, tests, and scripts at once. A probe confirms an unused variable
now exits 1.

One trap worth recording: promoting the rule in the block that matched
`frontend` + `tests` **also matched `scripts/**/*.mjs`**, which carried 5
findings outside the worker's scope. A carve-out block would have left the hole
open and cost 10 lines; closing it properly cost 5 deletions.

## Program Outcome

**The program did not reduce the codebase. It ended +3,265 lines (+0.69%).**

| Lever kind | Measured |
|---|---:|
| Reduction (helper unification, clone consolidation, dead code) | −1,804 |
| Legibility (god-file splits, de-chaining, new gates) | +5,069 |
| **Net** | **+3,265** |

Check: `473,733 (W0) + 3,265 = 476,998 (HEAD)`, which is the measured total.

The 30% goal is **closed as unreachable**, for the reason recorded in *The 30%
Arithmetic Problem*: it requires ~142,000 lines, and canonical `data/` alone is
1,000,391 tracked lines (65.5% of the repo) and cannot be deleted. No
combination of real levers reaches it.

What the program did deliver, measured:

- **Duplication:** the exact-body gate plus two clone-consolidation waves
  removed ~1,800 lines of genuine duplication across `src`, `frontend`, and
  tests.
- **Legibility:** the god-file backlog fell from the starting set to 46 `src`
  files over 700 lines, with the largest four frontend god files reduced
  1,155→142, 1,108→102, 1,020→229, and 978→143.
- **Correctness:** 11 dead branches removed, one real diagnostics bug fixed, and
  two new gate classes where none existed.
- **Honest cost:** decomposition adds 25–35% of each extracted body (import
  preamble, boundary banner, compatibility `__all__`). Measured seven times.
  Decomposition is a legibility purchase, never a reduction lever.

### Open question carried forward — resolved

`startBootstrapWithConfirmation` in `frontend/jobs/app/feed-first-run-flow.js`
had a guard that could never change its outcome; it has been removed
behaviour-identically. The open question was whether the guard was *intended*
to do something on the no-evidence path — emit a metric, or refuse to silently
accept an unconfirmed start — in which case the plain `return payload` was the
bug.

Resolution: **vestigial, confirmed** — the guard was never a functioning
check. `git log -L` shows it was introduced already in no-op form by
`023131df` ("Fix v0.2.1 jobs startup regressions"):

```js
const payload = await startJobsBootstrap({ timeoutMs: bootstrapStartTimeoutMs });
if (bootstrapStartHasRunningEvidence(payload)) return payload;
return payload;
```

Both branches return `payload`, so the guard could never change control flow.
An exhaustive 8/8 truth-table over the three evidence flags (`started`,
`alreadyRunning`, `alreadyCompleted`) shows the guard's condition is exactly
the complement of the caller's existing handling one frame up in
`startBootstrapAndLoad` (`alreadyCompleted` early-return at L133, throw on
`!started && !alreadyRunning` at L142-144). The real check had already
migrated to the caller; the metric intent is likewise covered by the
`requested`/`uncertain`/`confirmed` funnel that only ever fires on the confirm
path. The `bootstrapStartHasRunningEvidence` predicate still lives in
`feed-report-probe.js` because the sibling retry guard at L251 (which gates an
actual `emitMetric`) still needs it.

### Pre-existing blockers — fixed after closeout

Both conditions found during closeout were real pre-existing defects, and both
are now fixed in a follow-up commit. Neither was introduced by this program.

**6 `mypy` errors**, all signature-contract mismatches rather than logic bugs:

- `src/ship/packaged_smoke/rehearsals.py` — the generic runner returned the
  dynamic `getattr(module, scenario)(...)` result as `Any`; wrapped in `cast`.
- `src/bridge/sync_service.py` (×2) — `_write_shadow_surface` declared its
  `write` callback as returning `None`, but both call sites pass store methods
  returning `dict[str, Any]`. The callback's return value is never consumed
  (sole call site is `write(runtime_store, payload)`), so the annotation was the
  wrong side of the contract; widened to `object`.
- `src/jobs/adapters/community/__init__.py` (×2) — `_run_multi_url_source`
  called `parse_html(text, base_url=url)` while declaring
  `Callable[[str, str], list[RawJob]]`. The declared type described a
  positional-second-argument call the body never makes. Replaced with a
  `_HtmlParser` Protocol whose keyword-only `base_url` matches the actual call
  and is satisfied by both parser shapes in the package.
- `src/jobs/adapters/provider_api.py` — `_make_sources_runner` defined two
  `runner` variants with different signatures, which `mypy` rejects. Collapsed
  to one signature that always advertises `try_playwright` and forwards it only
  for accepting adapters. Behaviour-preserving: `_accepted_loader_kwargs`
  (`src/jobs/pipeline_source_results.py:89`) already filtered kwargs against the
  loader's own signature before the call, so a non-accepting adapter never
  received the seam and still does not. Verified directly: omitting
  `try_playwright` is byte-identical to passing `None`, a non-accepting adapter
  still drops a real callable, and an accepting adapter still forwards it.

**`test_browser_fallback_pool.py` failures** from a Playwright version mismatch.
`@playwright/test` is pinned `1.63.0` in both `package.json` and
`package-lock.json` (and `1.63.0` is the current published release); only
`node_modules` held a stale `1.58.2` from an interrupted install. Reinstalling
reconciled it with no manifest change, and the browser cache was completed for
revision `1243`. Both Playwright installs are now consistent:

| Install | Version | Drives |
|---|---|---|
| Node `@playwright/test` | 1.63.0 | `tests/frontend/smoke.spec.js`, `perf-trace.spec.js` |
| Python `playwright` | 1.63.0 | `tests/test_browser_fallback_pool.py` |

Verified: headless smoke 10/10, headed smoke 10/10, perf 3/3, and
`test_browser_fallback_pool.py` 11 passed / 1 skipped — so that suite no longer
needs excluding from the acceptance run.

Note that `requirements-lock.txt:136` pins `playwright==1.58.0` for the
`scrapy-playwright` path. That is a separate, tracked Python requirement and was
not changed; the `1.63.0` install above is the untracked global one the browser
pool test imports.


## Wave 4 Verification

```
python -m pytest tests/ -q                                  # 5417 passed, 2 skipped
node --test --test-reporter=tap "tests/frontend/unit/*.test.mjs"   # 900/900
python tools/repo_health/repo_guardrails.py                 # all 15 groups
python -m ruff check src/ tests/ scripts/ tools/            # clean
python -m vulture src/ whitelist.py --min-confidence=60     # clean
python -m mypy --config-file mypy.ini                       # clean (0 errors)
npx eslint . --ignore-pattern "_out/**" ...                 # 0 problems
npx knip                                                    # clean
npm run lint:precommit:ci                                   # exit 0
```

The full suite now runs **unfiltered** — `test_browser_fallback_pool.py` is
included (11 passed, 1 skipped) since the Playwright reconciliation above. The
previously recorded figure of `5406 passed, 1 skipped` came from an acceptance
run that had to exclude that file.

Both new tests were confirmed to **fail against the unfixed code** before the
fix landed. The dead-code gate was confirmed to **fail when the bug is
reintroduced** and pass when it is not.

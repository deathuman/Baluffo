# Repo Structure & Test Audit Handoff

> - **Status:** Active - 2026-09-28 verification sweep, 2026-09-29 independent re-measurement and **lane-run** pass, **Q6 landed** 2026-09-29, **Q2 + Q7 landed 2026-09-30** (`f33d783e`/`7051947e`/`ccaa1603` and `0c3e0372`), and **Q4 + Q5 landed 2026-10-01** (`3f33e089`, browser-verified). **Q3 remains.** Figures re-anchored to the post-Q7 tree. **Q4/Q5 are landed but deliberately UNRELEASED** — they ride with the next version bump; see *Q4 and Q5: landed, unreleased*. Q2 and Q7 rows below now carry what actually landed, including two places where this doc's own prediction was wrong.
> - **Use this when:** a LOC-reduction, dead-code, or "merge these families" report arrives and you need to know whether it is real, or when you want to execute or extend the verified remaining reduction queue without re-measuring the repo from zero
> - **Canonical for:** the 2026-09-28 corrected structure/test audit as re-measured on 2026-09-29, the refuted-claim list, and the ranked implementation queue derived from it
> - **Not canonical for:** test delete/merge safety rules and retained-test boundaries (see [`test-reduction-triage.md`](test-reduction-triage.md)), the Wave 1–4 program record and its measured yield rates (see [`codebase-simplification-plan.md`](codebase-simplification-plan.md)), or verification command ownership (see [`../testing.md`](../testing.md))
> - **Then inspect:** [`codebase-simplification-plan.md`](codebase-simplification-plan.md), [`test-reduction-triage.md`](test-reduction-triage.md), [`../testing.md`](../testing.md)
> - **Last updated:** 2026-10-01

## Why This Plan Exists

An externally produced audit on 2026-09-28 claimed roughly **15,000 removable LOC** across a
"registry service family", "inventory meta-tools", unreachable `scripts/`, a `src/frontend/styles.css`
shim, and a large test-corpus dedup target. Every load-bearing claim was measured against the tracked
tree. **The report is not usable as written**: several of its files do not exist, its line counts are
off by 3–4× on the files that do exist, and its test-similarity metric (normalized line-set Jaccard)
flags near-identical fixtures and unrelated files alike.

This document is the handoff: what was actually measured, what is refuted, the small queue that
survives verification, and how to reproduce every number. It continues the closed
[`codebase-simplification-plan.md`](codebase-simplification-plan.md) program rather than restarting it,
and it does not relax any rule in [`test-reduction-triage.md`](test-reduction-triage.md).

Three follow-up investigations were first delegated to subagents; their dispatched runs mostly failed with
`INFERENCE_CAP_ERROR` (free-model daily limit), so the lead re-ran the analysis inline. Two teammates did
land substantial raw output before failing, and it is now part of the record — with corrections:
`_out/ws-tests/findings.md` (its numbers are sound and its `splitlines()` method is the one to quote; it
supersedes the lead's cruder tally), and `_out/ws-families/` (its merge arithmetic and five-tool prototype
are load-bearing; its `inventory_common` column is wrong). `_out/ws-scripts/` was written from scratch by
the lead after that teammate produced nothing. All claims across the three `findings.md` files come from
**static analysis, AST parsing, token matching, and read-only guardrail runs**. The 2026-09-29 lane-run pass
then *did* execute things, deliberately and in isolation: `pytest --collect-only` (with `BALUFFO_DATA_DIR`
pointed at `_out/`, and `git status --porcelain data/` confirmed empty afterwards), the full `node --test`
frontend unit lane, and the never-run guardrail policy functions called in-process. Those results are
labelled as lane-run evidence below, and they are what closed Open Questions 2 and 3. **Q6 then went further
and changed the tree:** it deleted the two un-wireable orphan checks, registered the third, and re-ran the
guardrail groups (`_out/ws-verify/oq2_v3.py` is the recount that scoped it).

## Corrected Baseline (2026-09-29)

`python tools/repo_health/loc_budget.py --report` (tracked files; `docs/` and `data/` excluded):

| Area | 2026-09-18 (program start) | 2026-09-29 (this change) | Δ |
|---|---:|---:|---:|
| tests | 206,693 | 212,683 | +5,990 |
| src | 172,495 | 175,392 | +2,897 |
| frontend | 45,694 | 47,786 | +2,092 |
| scripts | 23,245 | 24,776 | +1,531 |
| tools | 13,094 | 15,547 | +2,453 |
| styles | 9,896 | 10,182 | +286 |
| `<root>` | 1,858 | 1,871 | +13 |
| probes | 758 | 758 | 0 |
| **Total** | **473,733** | **488,995** | **+15,262** |

The 2026-09-29 column is today's `--report` output verbatim, taken **after** the Q6 deletion, and every area
reports `delta +0` against `loc_baseline.json` — the committed baseline is in sync with the tree, so the
report can be read as current without re-baselining. **Re-anchor before quoting it:** the tree has moved six
times since this audit started. `2ee26c03` → `ebaff931` (v0.3.0 + two hook/CI fixes) took the total from
488,761 to 489,028; `3c2c7e8f`, `d94ba61d`, `27535f72` and `8ede5716` took it to **489,066**; the Q6 change
(`03bb67c3`) brings it back down to **488,995** (`tools` 15,618 → 15,547, and `loc_baseline.json` re-ratcheted in the
same commit). A doc that cites an absolute LOC total without naming the commit is citing a number that has
already expired. (The 2026-09-18 column is the closed programme's own starting table,
`codebase-simplification-plan.md` lines 20-25.)

The programme closed at **476,998**, and its own arithmetic checks: `473,733 (W0) + 3,265 (net) = 476,998`
(`codebase-simplification-plan.md` line 538). Today's 488,995 is **+11,997** above that close — ordinary
feature growth with re-ratcheted baselines, not drift — but it does mean **no reduction programme is
currently in flight**, which is why an unfalsified "−15k" report looked plausible for a moment.

Test corpus, measured 2026-09-29 at `8ede5716` and re-confirmed after Q6 (`read_bytes().splitlines()` +
regex over `git ls-files`,
method stated because the earlier numbers here were inverted): **4,736** `def test_` functions in the **756**
tracked `tests/**/*.py` — **4,678** at module level plus **58** indented inside test classes — and **966**
`test(` call sites in the **235** tracked `tests/**/*.mjs` (222 of which live in `tests/frontend/unit`).

**Collected counts, now actually measured rather than inherited** (lane-run, 2026-09-29):

| Measure | Command | Result |
|---|---|---:|
| Python, full suite | `python -m pytest tests -q --collect-only` | **5,655** collected |
| Python, developer lane | same + `-m "not slow and not packaging and not release"` | **5,460** collected, 195 deselected |
| JS unit lane | `node --test --test-reporter=tap tests/frontend/unit/*.test.mjs` (211 files) | **969 tests, 969 pass, 0 fail** |

Collected exceeds static because parametrization expands (`+919` over `def test_`); JS moves only `+3`
because subtests are rare. These are *collection/executed* counts on this Windows checkout, not a coverage
claim — the coverage-verified figure stays **5,450 collected across 685 files**
([`test-reduction-triage.md`](test-reduction-triage.md), 2026-09-27), which is a different measure over a
narrower file set. Do not hand-sum the two languages into one "unit" figure — an earlier revision of this
doc did exactly that and reported "5,687 units (4,787 `node:test`, 900 pytest)", which has the split
backwards: Python is ~5× the JS corpus in test-function count.

`data/` is out of bounds and stays out of bounds — but not for the reason this doc stated. **Tracked**
`data/` is **12 files / 140,042 lines = 20.9%** of the repo's **669,050** tracked lines (2,341 files), not
"1,000,391 tracked lines (65.5% of the repo)". That inherited figure (it is line 542 of the closed
programme's *Program Outcome*) is not reproducible today under either definition: counting the working tree
including gitignored job-report JSON gives 147 files / 2,297,681 lines. Cite the tracked number, or
re-measure before repeating the 65%.

## Refuted Claims

| Report claim | Measured reality | How to re-check |
|---|---|---|
| Registry service family: 5 shim files, 2,161 LOC, 1,554 recoverable | Those 5 files **do not exist**; the real surface is **14 tracked paths** matching `source_registry` under `src/` — 12 `src/source_registry*.py` (3,680 lines) plus `src/storage/source_registry_runtime.py` and `src/storage/migrations/008_source_registry_runtime.sql`, **4,286 lines** together; `src/source_registry.py` itself is 173 lines. *This doc's own "four survivors" row was also wrong:* `source_registry_seed.py`, `source_registry_service.py` and `source_registry_deletion.py` are not in the tree | `git ls-files src \| Select-String source_registry` |
| Inventory meta-tools: 5 files including `inventory_v2.py`, 2,256 LOC, 80% duplicated | `inventory_v2.py` **does not exist**. Real set: **9 files / 2,464 LOC** (includes `bridge_route_inventory.py` 1,063, `bridge_api_field_inventory.py` 318, `suppression_inventory.py` 86, `inventory_common.py` 77) — of which the **analyser** subset is **5 files / 920 LOC**. They are *not* all near-clones: difflib over the 10 pairs spans **0.284–0.789 with exactly one pair above 0.60** (`desktop_update_facade_inventory.py` ↔ `update_manager_facade_inventory.py`, 0.789); ws4's normalised-line method ranks that pair at 0.817 and puts only 7 pairs above 0.40. `inventory_common.py` (77 lines) has **6 importers** — the five analysers *plus* `bridge_api_field_inventory.py` | `git ls-files tools/repo_health \| Select-String inventory` |
| `scripts/` holds unreachable files; 45,892 LOC | 24,760 LOC (`loc_budget.py --report`) across **81 tracked files** (73 of them `.py`); token-reachability over all tracked Python/JS/MD/JSON/config finds **zero unreachable files** (each is named by a workflow, `package.json` script, `npm run` target, test, or doc) | *Reproduction Kit* §3 |
| Three named "truly orphan" heavies: `source_policy_soak_report_sections.py` (1,447), `perf_complete_profiles.py` (754), `perf_complete_bench.py` (535) = **2,736 deletable lines** | **All three are imported by live siblings**; deleting them breaks `scripts/` on the next import: `scripts/source_policy_soak_report.py:140`, `scripts/source_policy_soak_report_links.py:28`, `scripts/perf_complete.py:56`, `scripts/perf_complete_summary.py:20`, `scripts/perf_complete.py:26`. They are intra-package modules, not orphans — and the two chains are covered by **14 test files / 4,562 lines** of meta-tests (`test_source_policy_soak_report*.py`: 9 files / 3,453 lines; `test_perf_complete*.py`: 5 files / 1,109 lines), an earlier revision of this row having guessed "~2,778 lines in 5 files" | `git grep -n source_policy_soak_report_sections -- . ":(exclude)scripts/source_policy_soak_report_sections.py"` ; `git ls-files tests \| Select-String "soak_report\|perf_complete"` |
| `src/frontend/styles.css` is dead | That path does not exist — **`src/frontend/` has no tracked files at all**. The stylesheets are at root **`styles/`: 5 files, 10,182 lines**, and the report's line citations were right: `admin.html:8-10`, `index.html:27-29`, `jobs.html:8-10`, `saved.html:8-10` each carry `<link rel="stylesheet" href="styles/…">`. Reachability is 100%; only the *path* in the claim is wrong | `git ls-files styles` ; `git grep -n "stylesheet" -- "*.html"` |
| Large near-clone population in the test corpus | Across the **222** files in `tests/frontend/unit` (44,096 lines) exactly **2** pairs exceed 0.60 normalized-sequence similarity, and they share a file: `admin-ops-audit-artifacts-controller.test.mjs` ↔ `admin-ops-dedup-lists-controller.test.mjs` (0.695) and `admin-ops-dedup-lists-controller.test.mjs` ↔ `admin-ops-run-diagnostics-controller.test.mjs` (0.623) — one file sitting between two neighbours, not a clone population. The `tests/test_jobs_fetcher*` cluster (38 files / 9,546 lines) shows **zero** pairs above 0.55. A strict AST clone scan of all Python finds **38 groups / 676 redundant lines**, of which `tests/*.py` is 29 groups / **532 lines**, mostly duplicated local fixtures — not 15k. (An earlier revision of this row attributed the two 0.60 pairs to "198 vs 382 lines" files — those are the Python inventory tools, a different corpus) | `_out/audit_verify/ws4.out.txt`, `_out/audit_verify/ws10.out.txt` |
| 15,000 LOC removable | The verified queue **still open (Q3 only) sums to about −54** — roughly **0.01%** of the 488,995-line tree. Already landed from this queue: **−71** (Q6), **−677 headroom** (Q1), **−137** (Q2, re-measured), **−199** (Q4+Q5, landed 2026-10-01), **−27** (Q3 so far), and Q7's additive gate. Note the open figure *fell* when Q3 started: its largest cluster proved to be shape-similarity rather than copy-paste. Its largest original item was found by *building a prototype*, not by the report's similarity metric. Four- and five-figure targets are unsupported by anything in the tree | *Implementation Queue* |

## Corrections To The External Audit (and To This Doc's First Pass)

The first pass of this audit accepted four of the report's framings that a later pass measured wrong. They
are kept here rather than quietly rewritten, because the mistakes are the same class this doc exists to
catch — plausible numbers with no command behind them — and because a handoff that hides its own corrections
is no better than the unsourced report it refutes.

### Correction 1 — CSS is at the repo root, and *this doc's own correction was the error*

The external report put the stylesheets at `src/frontend/styles.css`. That path does not exist:
**`git ls-files src/frontend` returns nothing at all** — there is no `src/frontend/` in the tree. But the
report's *figures* were right: root `styles/` holds **5 tracked files / 10,182 lines**, linked straight from
the root entrypoints (`admin.html:8-10`, `index.html:27-29`, `jobs.html:8-10`, `saved.html:8-10`).

A later revision of this doc then "corrected" that into something worse — it asserted 3 files / 1,919 lines,
that `admin.html` and `jobs.html` "do not exist", and that the real linkers were `src/frontend/html/*`,
`picker_history.html` and `options.html`. **Every one of those claims is false and none of them is
reproducible from any command in this tree:**

| Claim made by a revision of this doc | Actual, re-measured | Command |
|---|---|---|
| `styles/` has 3 tracked files | **5**: `admin.css` 3,714 · `base.css` 310 · `components.css` 2,627 · `jobs.css` 1,172 · `saved.css` 2,359 | `git ls-files styles` |
| CSS totals 1,919 lines | **10,182** | `python -c "…splitlines()…"` over `git ls-files styles` |
| `admin.html` / `jobs.html` do not exist | Both exist, at the repo root, and are CSS entrypoints | `git ls-files *.html` |
| Linkers are `src/frontend/html/index.html`, `picker_history.html`, `options.html` | Root `index.html:27-29` etc. **No `src/frontend/html/`, no `picker_history.html`, no `options.html` exists** | `git grep -n "stylesheet" -- "*.html"` |

The likely mechanism is visible in the session record: the `git ls-files styles` step ran in a batch *behind*
a command that had already exited non-zero, so its output was never seen and the result was inferred. That
yields the rule this doc now applies to every table cell:

> **Do not quote a number from a command whose exit status you did not check.** In a batched run, one failed
> step makes every statement sourced from that batch unverified — not just the failed step's own output.

### Correction 2 — "185 src modules" overstates entanglement, and merging is a no-op

The real roster is **9 families / 97 files / 32,038 lines** (`merge2_out.txt` per-family totals:
`source_sync` 17 files / 5,769 ln, `task_launch` 11 / 4,654, `registry_conflicts` 15 / 4,568,
`source_registry` 13 / 4,233, `dedup_evidence` 8 / 3,979 … `fetch_report_normalization` 7 / 1,341 — they sum
to 32,038, not the 32,638 an earlier revision of this section carried). The claim that they are joined by
private cross-imports fails structurally, and the tidy "exactly one" figure this section used to carry does
not survive checking: `_out/ws-families/findings.md` line 16 attributes it to a `ws8.py`, but the surviving
`_out/audit_verify/ws8.py` performs the facade-inventory member diff, not a private-import count, and a
regex over tracked `src/**/*.py` finds **67** files importing `_`-prefixed names from sibling modules. The
defensible claim is the structural one from `fam_out.txt`: every family is **one hub importing its leaves**
(facade re-export) plus hub→leaf private plumbing — e.g. `source_sync.py` → `_config/_crypto/_runtime/_snapshot`
with one module-import each — not a mesh where any leaf can be reached from any other. Cross-sibling
*behavioural* clones are tiny: the surviving artifact
`_out/audit_verify/ws10.out.txt` measures **`src/**/*.py`: 9 clone groups / 144 redundant lines, all 9
cross-file**, i.e. 0.08% of the 175,392-line `src` area. (`findings.md` line 22 states "11 groups / 87 lines,
4 / 40 cross-file" for the same scope and cites a `ws10.py` that does not exist in `_out/ws-families/`; treat
the artifact, not the prose.) Merging all nine and deduplicating every docstring and module
import still yields only **1,026 lines (3.2%)** — 1.4% for `local_data_store`, 8.3% at best for
`active_audit` — while requiring a rebuild of a 5,769-line `source_sync.py`. The closed programme *spent*
**+2,663** lines on this split (`W2a/W2b/W7b` god files **+2,090** at `065e0712`, plus the `W2a` remainder
**+573** at `50b5fa6b` — *not* "W5", which was test consolidation at **−444**), so merging reverses a
decision on the record. Family merging is a **Non-Goal**, not an item.

### Correction 3 — the Q2 seam is five tools, not two

The first pass scoped the facade-inventory clone *pair* at −120…−150. Measuring all five analyzers at once
found a uniform section shape (shim 94 / spec 132 / row 22 / helper 162 / collect 91 / check 182 / main 75 /
guard 10 = **920 lines**), similarity peaking at 0.789 with exactly one pair above 0.60. The shared base is
not missing — `inventory_common.py` is already imported by all five tools (and by
`bridge_api_field_inventory.py`) — what is missing is a shared **spec**: the common module carries helpers
while each tool re-declares the same section bodies.
A prototype driving all five from one spec core came to **362 lines with byte-parity PASS**
(`_out/ws-families/proto_tool.py`). Realistic yield **−380…−620**, and the blast radius is verified smaller
than feared: the module names appear only in their own file plus `tools/repo_health/repo_guardrails.py` —
nothing in `.github/`, `docs/`, `scripts/`, or `package.json`, so the docs link-check gate cannot break.
Renaming is not free, though: the five stems are cross-cited by each other's test modules, so
`desktop_update_root_dependency_inventory` touches **7 files / 2,373 lines** (all five test modules, the tool
itself and `repo_guardrails.py`), while `desktop_updater_root_dependency_inventory` touches only **3** —
measured by `_out/ws-verify/claims.py` §C, and the asymmetry is itself the argument for renaming the
`updater` twin rather than merging the pair.

### Correction 4 — the CSS dead-code method is 91% false positives, and the survivors are mostly dead too

Searching for the *literal* class name only is how the report generated its dead-selector list. Run against
the tracked tree (`_out/ws-scripts/ws15.py`; corpus = 2,126 tracked `.py/.js/.mjs/.cjs/.html/.json` files,
22.9 MB), that method flags 23 selectors — of which 21 (91%) are cleared by dynamic class composition
(`admin-dedup-audit-gate-detail-${type}`, `action-center-status-${meta.tone}`, `inspector-severity-${…}`).
The filter that catches them is cheap: treat a selector as live if any dash-boundary prefix of it appears
inside a string literal. **That filter was the doc's own weak point, and Q4 has now gone past it** — see
*Q4* for the per-selector verdicts, which move the answer from "2 dead" to "**18 unreachable selectors /
236 rule lines**".

Two counting facts settle arguments a future reader will otherwise have again:

- **712, not 718.** `ws15.py` tokenises the *raw* stylesheet, so a class name written inside a `/* … */`
  comment counts as a selector. That is where its extra six names come from — `.admin-ops-*` is a prose
  cross-reference at `styles/admin.css:1892`, not a rule. Strip comments (the `_out/ws-verify/claims.py` and
  `_out/ws-verify/oq3.py` method) and both parsers agree: **712 distinct class selectors**, zero set
  difference.
- **"1,312 selector occurrences" is a rule-head count, not a selector count.** `ws14.py`'s regex lands one hit
  per `… {` head, so a comma list (`a, b, c {`) counts once and compound classes count once: **1,396 rule
  heads → 1,312 head matches**. Splitting heads on commas and counting each class token gives **2,188**.
  Both are correct under their own convention; say which you used.

An earlier revision of this section said "28 flagged / 14 false positives / 7 dead / 39 lines" and listed
selectors in `styles/admin.css` and `styles/saved.css` at line numbers past the end of those files. That
table was not produced by a runnable script — the `ws15.py` it cited **did not exist** at the time it was
written. It exists now, and its output is `_out/ws-scripts/ws15_out.txt`.

### Correction 5 — five guardrail "policy tests" never ran; two were fixed upstream, three are fixed here

This one is a live defect, not a measurement correction — and it is the finding that has now changed the
tree. `run_compat_group()` discovers its checks with `inspect.getmembers`, so all 59 `test_*` functions in
`suite_contract_policy.py` are candidates and only the **2** named in its `excluded` set stay out (57 run).
`run_workflow_group()` and `run_docs_group()` are the opposite style: they name their checks in **explicit
literal lists** (`repo_guardrails.py:702` and `:666`). Anything defined in those policy modules but absent
from the list is **dead code that looks like a gate** — and Q6 found the reason the list cannot simply absorb
most of it: `_run_python_check` (`repo_guardrails.py:125`) calls `check(repo_root=ROOT)` and nothing else, so
a policy function that also takes `tmp_path` raises `TypeError` the instant it is listed. A `tmp_path`
function in those modules is dead **by signature**, not by oversight.

| Never-run function (positions at `8ede5716`) | Size | Signature | Fate |
|---|---:|---|---|
| `workflow_policy.py:683` `test_package_json_uses_direct_frontend_unit_discovery` | 25 ln | `(repo_root)` — **wireable** | **Registered in the `workflow` group by Q6.** No pytest twin (`git grep` finds only the definition); it passes, and it is now the only live gate keeping retired frontend-unit manifest tooling out of `package.json` and the two workflows — the compat check that used to cover that (`test_frontend_test_patterns_disallow_generated_manifest_aggregators`) is one of the 2 `excluded` names |
| `workflow_policy.py:934` `test_dev_pipeline_targeted_npm_entrypoint_starts_without_relative_import_failure` | 30 ln | `(repo_root, tmp_path)` — un-wireable | **Deleted by Q6.** Drifted twin of the running pytest test, and it *fails* when called |
| `workflow_policy.py:966` `test_location_unknown_country_manifest_script_runs_from_repo_root` | 38 ln | `(repo_root, tmp_path)` — un-wireable | **Deleted by Q6.** Same contract as the running pytest twin at `tests/test_workflow_entrypoints.py:41` |
| `release_docs_policy.py:167` `test_release_notes_extractor_uses_top_changelog_section` | 29 ln | `(repo_root, tmp_path)` | **Already gone upstream.** `3c2c7e8f` deleted it as "a byte-identical duplicate of the real pytest test at `tests/test_release_docs.py:14`", for the same signature reason |
| `release_docs_policy.py:407` `test_package_json_build_aliases_use_leaf_builders` | 43 ln | `(repo_root)` | **Already fixed upstream.** `3c2c7e8f` corrected its drifted `release:preflight` pin (it *was* failing, silently, because nothing ran it) and registered it in the `docs` group |

That is the arithmetic, before and after — three states, because the tree moved twice under this finding.
`python _out/ws-verify/oq2_counts.py 2ee26c03 8ede5716 WORKTREE` prints all three:

| State | `workflow_policy` defined / listed | `release_docs_policy` defined / listed | Never-run |
|---|---|---|---|
| `2ee26c03` (when the lane run measured it) | 27 / 24 | 21 / 19 | **5 functions, 165 idle lines** |
| `8ede5716` (when Q6 started) | 31 / 28 | 21 / 21 | **3 functions, 93 idle lines** |
| Q6, this change | 29 / 29 | 21 / 21 | **0** |

`3c2c7e8f` closed the `release_docs_policy` half between those measurements — independently, from its own
run. Its message records that `test_package_json_build_aliases_use_leaf_builders` "was not merely dead, it was
**failing**", that the copy taking `tmp_path` "could never run as a guardrail entry (the runner calls these
with `repo_root` only)", and that removing `npm run security:js` from `release:preflight` now fails the `docs`
group by name. Same diagnosis, same two remedies: the finding reproduces without this doc's scripts, which is
the only kind of cross-check worth having.

Two consequences matter more than the idle lines:

1. **The `dev_pipeline` twin had drifted to the opposite contract, and only one copy could survive.** The
   running pytest copy (`tests/test_workflow_entrypoints.py:8`) asserts `returncode == 2`, the
   `No requested --only-sources entries matched available loaders` message, and `not report_path.exists()`;
   the policy copy asserted `returncode in (0, 2)` and `report_path.exists()`. Called in-process it failed on
   precisely that assertion (`_out/ws-verify/oq2_run.txt`). Q6 deleted the **policy** side, the pytest copy
   keeps the contract, and `tests/test_workflow_entrypoints.py` still passes (2 tests) afterwards. Had the
   drift run the other way, "just wire it up" would have turned CI red for the wrong reason — which is why the
   assertions get read before a side is chosen.
2. **The `release:preflight` content pin was orphaned, and stayed orphaned until someone ran it.** Until
   `3c2c7e8f`, `release_docs_policy.py` was the only place in the repo pinning the composition of
   `release:preflight`, and the function holding it was the one nobody ran — so `package.json` gaining
   `npm run check:published-version` (a step whose own docstring at `scripts/check_published_version.py:16`
   says it belongs "only in `release:preflight`… never into the always-on guardrail set") quietly outgrew the
   pin. Re-pinned **and** listed is the fix; deletion would have lost the only owner of that contract.
   Silence was the one option that kept the hole open, and this is the case study for it: a check that runs
   nowhere still fails loudly — for the one person who happens to call it.

The rule Q6 leaves behind, and the answer to what this doc first framed as "pytest-side redundancy" (Open
Question 2): when a policy check and a pytest check cover the same contract, delete the **policy** copy — the
pytest copies run in ordinary lanes and pass (14 tests across `tests/test_workflow_entrypoints.py` and
`tests/test_release_docs.py`). When the policy copy is *unique*, **register** it instead, because
`docs/testing.md:360` is explicit that repository policy checks belong in `repo_guardrails.py` "not pytest".
So Q6 is two deletions and one registration, verified by `python tools/repo_health/repo_guardrails.py` (all
groups pass; `workflow` now names 29 checks) plus `_out/ws-verify/q6_proof.py`, which mutates a throwaway copy
of `package.json` two different ways and shows the new check fails both times — the registration is not
vacuous.

## Implementation Queue

"Realistic" applies the program's measured yield rule: nominal duplication converts at roughly 30–45%,
because the shared host module costs 25–35% of the recovered lines back as import preamble, boundary
banner, and `__all__`. Do not plan against the nominal column.

| ID | Item | Identified | Realistic | Risk | Gate edits required |
|---|---|---:|---:|---|---|
| Q1 | ✅ **Landed 2026-09-29** — retired the stale grandfathered test caps (identified 24 at `2ee26c03`; see *Q1* below for why the landed set differs and how it was verified) | 0 LOC (679 headroom identified) | **−677 headroom realised** across 23 caps | none | none (`line-budget` group verifies). *Landed inside `e7dfe33c`, not its own commit — both sessions write the same baseline file* |
| Q2 | ✅ **Landed 2026-09-30** across `f33d783e`, `7051947e`, `ccaa1603` — all five analyzers converted; see *Q2* below for why it became **two** cores, not one | 800 tool lines (re-measured, not the 920 first recorded) + 941 test lines (not 1,100) | **−137 realised** (800 → 663) against a projected floor of 20 and ceiling of 362 | low-med | `loc_budget.py --update`; preserve `compat` wiring + 6 `compat_group_runs` tests. **The "no `duplicate_bodies_baseline.json` edit" claim above was wrong** — a third entry was needed, see *Q2* |
| Q3 | ◑ **Started 2026-09-30** — two genuine clusters extracted (`fake_run` ×3, `loader` ×2), −27 lines. **The −90…−140 projection is optimistic:** the largest cluster (`ok_loader`, 107 lines) is shape-similarity, not copy-paste, so the rest of the table needs per-cluster checking. See *Q3* | 282 cross-file + 250 same-file (claimed) | **−27 realised**; every remaining cluster unverified | medium | none (cap shrink is free) |
| Q4 | ✅ **Landed 2026-10-01** as `3f33e089` — deleted the **18** CSS selectors with no reachable producer (2 literal-dead + 16 proven unreachable by the per-selector read). **Unreleased**, rides the next version bump; see *Q4 and Q5: landed, unreleased* | 236 predicted | **−195 realised**, plus 4 selector lists hand-edited because they were only partly dead | medium (visual) — browser-passed | `loc_budget.py --update` (`styles` 10,250 → 10,073) |
| Q5 | ✅ **Landed 2026-10-01** as `3f33e089` — removed the 2 unreferenced CSS custom properties (`--bg-overlay`, `--surface-17`). **Unreleased**, rides the next version bump | ~10 predicted | **−4 realised** (2 properties × `:root` + `[data-theme="light"]`; the per-file "82 unused" reading was an artifact of scoping) | low — done | `loc_budget.py --update` |
| Q6 | ✅ **Landed 2026-09-29** — deleted the **2** un-wireable never-run policy copies (68 ln) and **registered** the 1 unique one (25 ln); see *Correction 5* | 93 lines | **−71 realised** (−72 deleted, +1 line listing the new check; the registered check costs one line and buys a live gate) | low | `loc_baseline.json` re-ratcheted (`tools` 15,618 → 15,547); `duplication` group unaffected |
| Q7 | ✅ **Landed 2026-09-30** as `0c3e0372` — `tools/repo_health/policy_wiring_policy.py`, three checks registered in the `workflow` group; mutation-verified, see *Q7* below | 0 LOC (+322 new, 1 pre-existing unused helper deleted) | ~0 (closes the class) | low | *Verify, all three confirmed by mutation:* deleting a name from `run_workflow_group` fails **by name**; registering a name with no definition fails by name; a `tmp_path`-taking `test_*` is rejected as un-callable |

| | **Total still open (Q3)** | | **−54**, the eleven remaining Q3 clusters — every one unverified, and the largest turned out not to be extractable | | *Q2, Q4, Q5 and Q7 are closed. Q4/Q5 landed on 2026-10-01 and shipped in `0.3.002`.* |
| | *Optional upside, not taken:* table-drive the five analyzer test files | 941 test lines (re-measured, not 1,100) | *further −250…−300* | low-med | same `compat` wiring. **Deferred rather than folded into Q2:** the five files carry 37 distinct `test_*` functions whose monkeypatch targets and asserted failure strings differ per tool, so a table would have to encode each variation anyway. Q2 took the orchestration and left the tests alone. |

Re-measured upward from the first draft of this doc, and downward in one place: Q2 originally covered only
the one clone *pair* (−120…−150), and prototyping the shared core across all five analyzers proved a much
larger seam; meanwhile the CSS item, first estimated at −150…−200, fell to −21 once dynamic class composition
was accounted for, and then climbed back to **−236 identified** once the per-selector producer read
(*Q4*) separated selectors that are *named* somewhere from selectors any code can actually *put on an
element*. Net effect: the honest total roughly doubled at both ends. It is still
**0.12–0.21% of 488,995 lines**.

Note the size of this queue relative to the report: **the entire defensible remaining reduction is about one
mid-sized module** — 1,036 lines at absolute best, against 488,995. Plan it as hygiene, not as a program. The
one item already landed (Q6, −71 lines) is the proof the queue is executable rather than aspirational.

Land one item per commit. Every commit that removes lines must run
`python tools/repo_health/loc_budget.py --update` **after staging**, because the ratchet compares the
index against `loc_baseline.json` and fails on an un-ratcheted reduction (`check_line_budget` alone will
not catch a shrink — it only enforces maxima).

### Q4 and Q5: landed, unreleased

**Q4 and Q5 both landed on 2026-10-01 in `3f33e089`** — the browser check this section used to say they
were blocked on is done. 195 rule lines across five stylesheets plus 4 custom-property declarations, and the
`styles` LOC baseline was ratcheted 10,250 → 10,073 in the same commit. The deletion is **visually a no-op**
and that was verified rather than assumed: `saved.html` pixel-identical, `jobs.html` 0 pixels differing above
threshold 40, `admin.html` matching HEAD except 74 pixels in two spots that are the fetcher clock
(`00:13:35` → `00:18:16`).

What did **not** happen is a release. `3f33e089` declares `Release-tag: v0.3.002` intent, but **no
`0.3.002` was cut**, so the CSS change is on `main` and not in any desktop release asset.

The reason is worth keeping, because it is the opposite of what the earlier draft of this section implied.
Pushing `3f33e089` **republished the live `0.3.001` container tag**, moving it from `sha256:f6fa5b37` to
`sha256:0d0f194c`. So the deletion is *already* published — inside the container labelled `0.3.001`, while
the `v0.3.001` release assets (built at `af63e63b`) do not contain it. Only the desktop assets lag, and
since the change renders identically, that lag costs nothing.

Cutting `0.3.002` was considered and declined: the entire user-visible delta would be a dead-code deletion
plus the six boilerplate compatibility phrases `release_docs_policy.py` asserts on the top changelog section,
against a ~22-minute `release:preflight` gate. The next real feature release carries it instead.

#### The hazard this leaves open

**A live version tag stays mutable for as long as shipped code lands on `main` without a bump.** `0.3.001`
has already moved once; deferring does not freeze that, it guarantees further moves. Concretely,
`container_version_policy.py:201`:

```python
if any(_has_valid_release_tag_intent(commit, current_version) for commit in shipped):
    return []
```

**One** intent line satisfies the gate for the **whole window**, and the window only resets at a version
bump. `3f33e089`'s `Release-tag: v0.3.002` has already discharged it, so every later push touching `src/`,
`styles/`, `frontend/` or `scripts/` passes the gate silently and republishes `0.3.001` again — with no
Umbrel update ever being offered, because that check is string equality. The tag degrades quietly; nothing
in the lane reports it.

`_has_valid_release_tag_intent` treats `>= current` as valid so that a follow-up fix can be retagged into
an already-bumped version. That is right for a bumped-but-unpublished version and wrong for an
already-published one, and the gate could not tell them apart.

**Resolved 2026-10-01, alongside the `0.3.002` bump.** `evaluate_window` gained
`current_released`, the gate computes it from `git tag -l v<current>`, and a released version now demands a
real bump — `Release-tag:` intent no longer authorises overwriting a published tag. Shipped commits that are
ancestors of the release tag are dropped first, because they are inside the published image already.

Choosing the **git tag** rather than a live GHCR query was the decision that mattered. The gate runs in
pre-commit and pre-push; a registry call there means two round-trips, an offline failure mode, and — because
`AGENTS.md` bans `--no-verify` — a stuck commit rather than a bypass. The cost is that a version which reached
GHCR without ever being tagged reads as unreleased, which is the inert window anyway: nothing can hold the
version string, and Umbrel's update check is string equality, so pre-release republishing reaches nobody and
is still allowed.

Verified by mutation: forcing `_version_is_released` to return `False` fails 2 of the new tests, and
short-circuiting the released branch fails 9. `docs/RELEASE.md` carries the rule as the canonical statement.

### Why Q4 Could Not Land Alone (superseded 2026-10-01)

> Kept because it is why Q4 was scheduled the way it was, and because the rule it states is still the rule:
> when `container_version_policy` blocks, the answer is a release decision or a split commit, never a
> bypass. What it got wrong is the blocker. It said the CSS deletion "has to ride a version bump" and
> framed the browser check as the outstanding step. Both were right *in principle* and wrong *in fact*: the
> browser check passed, the version bump was deliberately skipped, and the change landed anyway.

Q4 and Q5 touch `styles/`, and `styles/**` is **shipped container code**. `NON_SHIPPED_PATTERNS`
(`tools/repo_health/container_version_policy.py:66`) whitelists `docs/**`, `tests/**`, `tools/**`,
`memory/**`, `.github/**` and the root identity files — `styles/`, `frontend/`, `src/` and `scripts/` are not
in it, and the list is deliberately kept equal to `build-container.yml`'s `paths-ignore` so the gate and the
republish trigger cannot disagree. A CSS-only commit therefore republishes the container. That is exactly
what happened, and the consequence was not caught by any gate: see *The hazard this leaves open*.

It is also the whole difference between Q6 and Q4. `tools/**` and `docs/**` are both non-shipped, so the
guardrail cleanup landed while the tree stayed at `v0.3.0`; the 18 selectors of Q4 were proven dead and
listed in *Q4* long before anyone touched them.

### Q1 — Retire the stale test-budget caps ✅ landed

`tools/repo_health/test_line_budget_baseline.json` maps 55 files to caps and `check_line_budget` is a
**max-only** ratchet, so entries can sit far above their file's real length and absorb shrinkage in
silence. At audit time 24 entries carried **679** lines of unearned headroom. Tightening them is the
cheapest item in the repo and makes every future test shrink visible instead of silently absorbed.

The table below is the audit-time view at `2ee26c03`. It is kept as evidence, not as the landed diff:

| File | Cap | Actual | Slack |
|---|---:|---:|---:|
| `tests/admin/test_admin_bridge_report_history.py` | 673 | 368 | 305 |
| `tests/bridge/test_routes_smoke.py` | 413 | 340 | 73 |
| `tests/bridge/test_routes_post.py` | 793 | 746 | 47 |
| `tests/frontend/unit/jobs-desktop-update.test.mjs` | 898 | 855 | 43 |
| `tests/frontend/unit/admin-registry-controller.test.mjs` | 832 | 793 | 39 |
| `tests/frontend/packaged-desktop-smoke.mjs` | 468 | 430 | 38 |
| `tests/admin/test_admin_bridge_runtime_config.py` | 728 | 704 | 24 |
| `tests/frontend/unit/desktop-local-data-navigation.test.mjs` | 702 | 680 | 22 |
| `tests/test_desktop_updater.py` | 640 | 618 | 22 |
| `tests/test_pipeline_runtime.py` | 407 | 395 | 12 |
| `tests/source_discovery/test_web_search_directory_audit.py` | 816 | 805 | 11 |

The remaining stale entries each carry ≤9 lines of slack (`admin-ops-controller`,
`test_source_registry_seed_runtime`, `test_discovery_service`, `test_ship_update_manager`,
`test_routes_get`, `test_runtime_launcher`, the three `test_source_policy_soak_report*`,
`test_browser_session_watch`, `test_pipeline_stage_source_execution`,
`admin-ops-history-render`).

**Landed: 23 caps, 677 lines retired, not 24/679.** The set drifts because the baseline tracks *live file
length*, and between the audit and the landing the tree moved: `tests/frontend/unit/admin-fetcher-controller.test.mjs`
was raised to exact by concurrent log-tailing work, so it left the stale set. Re-measure rather than
re-apply a recorded list:

```bash
python tools/repo_health/repo_guardrails.py --group line-budget      # green baseline
# then, from a clean tree, set every cap to its file's current length and re-run the group
```

Four things worth keeping from doing this properly, all measured rather than assumed:

1. **A naive regeneration drops somebody else's raises.** While Q1 was being applied, another session
   raised two caps to match test files it had grown: `admin-discovery-controller.test.mjs` 1237→1283 and
   `admin-fetcher-controller.test.mjs` 1317→1343. A tightening tool that rebuilds the file from `HEAD` plus
   measured lengths silently drops those raises and leaves the *other* session's tree red, because the cap
   then sits below its file. The safe shape is what the applied edit did: load the **current** file, lower
   only caps strictly above a live length, never raise a cap you did not measure — then diff the result
   against `HEAD` and read every changed line before committing. That diff is exactly how the two foreign
   raises were found here.
2. **The group is not vacuous** — proved without touching the tree: point the module's baseline path at a
   copy in `_out/` with one cap set to `actual − 1`; `check_line_budget()` then fails naming that file, and
   passes again with the real baseline. Lowering a stale cap by one does *not* trip it (it is still above
   the file), which is the trap that makes a naive "lower by one" probe look like a pass.
3. **Both baseline JSONs are shared write-targets.** Any session that stages with `git add -A` absorbs
   another session's caps, so a hygiene-only item can land inside an unrelated commit — which is what
   happened here: Q1's 23 caps are in `e7dfe33c` (`fix(admin): stop the fetch/discovery log boxes rendering
   chopped half lines`) rather than in a commit of its own. The ratchet is safe (a cap never becomes
   silently looser than the tree), but the attribution is not. Land Q1-class items from a clean tree, and
   expect to co-author when another session is mid-edit on the same files.
4. **`loc` blocks unrelated commits while a neighbour's refactor is half-finished in the same checkout.**
   Measured while committing this doc: the pre-commit gate exited 1 with
   `src is 175549 lines; baseline says 175677` and `tests is 212887 lines; baseline says 213171` — numbers
   from a concurrent session's in-flight `src`/`tests` edits, none of them by this change, which touches only
   a markdown file. The telling part: `python tools/repo_health/repo_guardrails.py --group loc` **passes** in
   the shell at the same moment it **fails** inside the hook of `git commit -- docs/<one file>`. The hook
   pairs the *committed* `loc_baseline.json` with the *worktree's* file lengths, so it has no notion of "the
   lines you are committing" — it has a notion of "this checkout is mid-edit". Neither remedy is acceptable:
   `--no-verify` is barred outright, and `loc_budget.py --update` would write a baseline another session is
   actively editing *and* ratchet it to a half-finished tree. The correct move is to wait for the other
   commit to land, or work in a second checkout. Scope the commit itself with `git commit -F msg -- <paths>`
   so a neighbour's staged files are not swept in while you retry.

Zero LOC change, so no `loc_budget` edit. Verify with
`python tools/repo_health/repo_guardrails.py --group line-budget`.

### Q2 — Parameterise the five inventory analyzers over one SPEC core

Scope is all five analyzers, not just the clone pair: `desktop_update_facade_inventory` (157),
`desktop_update_root_dependency_inventory` (190), `desktop_updater_root_dependency_inventory` (295),
`update_manager_facade_inventory` (149), `update_manager_runtime_facade_inventory` (129) = **920 lines**,
plus their 5 test files = **1,100 lines**. Every one is the same shape — dual-import shim, `SPEC` consts,
row dataclass, `collect_*`, `check_*`, `main` — and the sections total shim 94 / spec-const 132 / row 22 /
helper 162 / collect 91 / check 182 / main 75 / guard 10. Only the facade pair exceeds 0.60 similarity
(0.789); the other pairs sit at 0.28–0.58, i.e. a shared *shape*, not shared *text*.

**The seam is proven, not hypothesised.** `_out/ws-families/proto_tool.py` (362 lines) drives all five
from spec dicts and prints `parity: PASS` — identical row output (2/0/0/1/0 rows) and all five `check()`
calls returning `[]`. So 920 → ~362 plus thin per-spec wrappers. Strictly byte-identical removable
top-level units total only **20 lines**: that is the floor, the prototype is the ceiling, and the truth
falls between them.

#### What actually landed, and why the prototype's number was a ceiling

Landed in three commits — `f33d783e` (core + one caller, to prove the seam before
four dependents), `7051947e` (the other two facade tools), `ccaa1603` (both
root-dependency tools). **Re-measured first, and the sizes above were stale:** the
five tools are **800** lines, not 920, and their tests **941**, not 1,100.

**It became two cores, not one.** `ImportInventorySpec` and `RootDependencySpec`
both live in `inventory_common.py`, because the import family and the dependency
family share no row shape, no detector, and no failure vocabulary. The prototype
assumed one SPEC-driven core could serve all five; it could not, and forcing it
would have meant a spec with a flag per axis. Realised: **800 → 663, −137**, against
a projected floor of 20 and ceiling of 362. The 362 was an upper bound, not an estimate.

**The binding constraint was the monkeypatch surface, not the duplication.** The tests
patch **seven** module-level names on the wrapper modules (`CLASSIFIED_IMPORTS`,
`DEPENDENCY_CATEGORIES`, `EXPECTED_DEPENDENCY_COUNT`, `EXPECTED_FACADE_IMPORT_COUNT`,
`EXPECTED_REFERENCE_COUNT`, `LEAF_FACADE_IMPORT_ALLOWLIST`, `RUNTIME_IMPORT_ALLOWLIST`).
The prototype bakes `expected_records=2` into a `SPECS` dict **at import**, which
would have ignored every one of those patches and left the drift tests green while
testing nothing. So each tool builds its spec **inside the function, from module
globals, on every call**. Check that before "simplifying" a spec into a module-level
constant.

**The `duplicate_bodies_baseline.json` claim above was wrong.** It predicted no edit
was needed. One was: the three public `collect_*_inventory` wrappers — one-line
delegations, which cannot be consolidated because `repo_guardrails.compat` and each
tool's own tests import those names — hash as three identical 4-line bodies, and
`MIN_AVERAGE_LINES` is 4. Baselined at 3 copies **with the reason recorded in the
file**, so a fourth copy still fails. The 2 pre-existing patterns (`_path_size`,
`_require_root`) were left untouched and the stale-baseline check passes.

**Verify, and note it is a byte comparison, not a test pass.** A golden capture of
all five `--check` outputs was taken *before the first edit*; all five are byte-identical
after, and all still exit 0. That is what makes this refactor safe — the tests alone
would not catch a row-ordering or JSON-shape change. All 37 tests across the five test
files (7/8/11/7/4) pass, `compat` passes, and ruff/mypy/vulture/complexity are clean.

**Two escape hatches, both earned.** `update_manager_runtime_facade_inventory` must
also catch bare `import update_manager` and relative imports, and its per-row rule is
not the shared `src/` allowlist — any `src/ship` import *is* the defect. The updater
dependency tool sees bindings through `getattr(module, "name")` and through test
monkeypatches. Both are spec hooks, not forks of the collection loop.

**Two labels per tool, and the tests pin every message verbatim.** Count and
unknown-category messages use the `<thing> inventory` label; stale-classification and
unclassified messages use the bare entity. One label field produced
"Update manager facade **inventory** import is unclassified" and failed two tests. Both
labels, plus the per-tool message tails, are spec fields.

**Blast radius is smaller than it looks.** The five module names appear only in their own file and
`tools/repo_health/repo_guardrails.py` — **nothing** in `.github/`, `docs/`, `scripts/`, or `package.json`
cites them, so the `docs` link-check gate cannot break and `workflow_policy.py` does not name them.
`inventory_common.py` (77 lines) is already imported by all five through their dual-import shim, so extend
that module instead of adding a sixth.

The pair that proves the merge design is safe is still worth reading member by member:

| Member | desktop | update_mgr | Identical lines |
|---|---|---|---|
| `class …LineCategoriesRow` | 5 | 5 | 4/5 |
| `_imported_facade_modules` | 17 | 17 | 14/17 |
| `collect_…_line_categories` | 17 | 17 | 13/17 |
| `check_…_facade_inventory` | 38 | 38 | 30/38 |
| `main` | 13 | 13 | 10/13 |

Only two differences are semantic, and together they are the merge design's whole risk:

1. **Target matching:** desktop tests membership (`alias.name in FACADE_MODULES`,
   `node.module in FACADE_MODULES`, `module in FACADE_MODULES`); update-manager tests equality against a
   single module (`== FACADE_MODULE`). One facade-target *set* parameter reproduces both, but a careless
   merge that keeps only equality silently stops flagging aliased imports — and membership testing alone
   would count some `FACADE_MODULE` uses differently than the current tool intends.
2. **Allowlist:** `LEAF_FACADE_IMPORT_ALLOWLIST` vs `RUNTIME_IMPORT_ALLOWLIST` — parameterise, do not union.

**Recipe:** extend `inventory_common.py` with one frozen dataclass spec (`row_type`,
`facade_targets: frozenset`, `allowlist`, `label`) plus generic `collect()` / `check()`, and keep **every
public name** (`collect_*`, `check_*`, the row dataclasses) and **all five CLI entry points** as thin
wrappers. Two surfaces are load-bearing, both re-verified here: (a) `repo_guardrails.py` imports the check
functions and wires them into `compat` under distinct names; (b) each analyzer's test asserts that wiring —
**six** files carry exactly one `test_repo_guardrails_compat_group_runs_*` test each (the five plus
`tests/test_bridge_api_field_inventory.py`). The tests also
`monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", …)`, so **module-level `EXPECTED_*`
constants must stay addressable on each wrapper module** — moving them into the core breaks the drift
tests silently. Do **not** fold the CLI into one flag-driven command: the five `--check` surfaces are what
`compat` and those tests drive.

> **Correction, kept visible as an example of the error class this doc exists to catch.** An earlier
> draft of this section cited `tests/tools/test_desktop_update_facade_inventory.py` and
> `test_ship_update_manager_cli_contract`, and claimed a test monkeypatches
> `repo_guardrails.check_desktop_update_facade_inventory`. **None of those exist** (`git ls-files` returns
> nothing for either path; `git grep` finds no such monkeypatch). They were plausible inferences, not
> measurements — the same defect as the external report this plan refutes. Rule: every path in a Verify
> command must come from `git ls-files`.

**Verify (paths confirmed against `git ls-files`):** run each tool's own gate —
`python tools/repo_health/desktop_update_facade_inventory.py --check`,
`…/desktop_update_root_dependency_inventory.py --check`, `…/desktop_updater_root_dependency_inventory.py --check`,
`…/update_manager_facade_inventory.py --check`, `…/update_manager_runtime_facade_inventory.py --check`;
then `python -m pytest tests/test_desktop_update_facade_inventory.py
tests/test_desktop_update_root_dependency_inventory.py
tests/test_desktop_updater_root_dependency_inventory.py tests/test_update_manager_facade_inventory.py
tests/test_update_manager_runtime_facade_inventory.py -q`; then
`python tools/repo_health/repo_guardrails.py --group compat`; finally `loc_budget.py --update`. If the dup
gate prints *"duplicate-body baseline is stale"*, prune only the entries the message names.

**Pairwise-merging** the second flagged pair stays wrong — `desktop_update_root_dependency_inventory.py`
(190) vs `desktop_updater_root_dependency_inventory.py` (295), 0.627 whole-file similarity: the stricter
AST detector does **not** group them, and their test files diverge the same way (206 vs 382 lines) because
the larger one *covers more*. Under the SPEC-core design above both remain expressible, but note the
asymmetry: that pair's helper sections are 88 vs 9 lines, so their collection logic is genuinely different
and the core must keep a hook for per-spec helpers rather than forcing one body. Renaming for legibility
(`update` vs `updater`) is the cheaper fix. See *Open Questions*.

### Q3 — Extract the duplicated local test fixtures

The 532 redundant lines the clone scan finds in `tests/*.py` are overwhelmingly **local fixtures
copy-pasted between files**, not duplicate coverage — which is why the report's "test corpus is full of
near-clones" framing is wrong even where duplication is real. Largest clusters:

| Cluster | Sites | Redundant | Shape |
|---|---|---:|---|
| `ok_loader` | 4 — `tests/test_jobs_fetcher_pipeline_basics.py:290`, `tests/test_jobs_fetcher_pipeline_contracts.py:17/372/458` | 75 | 15-line fake loader |
| `ok_loader` (16-line variant) | 3 — `tests/test_jobs_fetcher_pipeline_error_cases.py:60`, `tests/test_jobs_fetcher_pipeline_incremental.py:101/143` | 32 | same shape, one line longer, so the exact-match scan splits it into a second group — the two groups are **one** real cluster of 107 redundant lines over 7 sites |
| `check` | 4 — `tests/bridge/test_job_availability_atomic.py:127/199`, `tests/bridge/test_job_availability_routes.py:198/276` | 40 | 8-line assertion helper |
| `fake_run` | 3 — `tests/test_install_git_hooks.py:11/40/73` | 30 | 15-line fake subprocess run |
| `scraper_loader` | 2 — `tests/jobs_static/test_browser_and_regression_queue_pipeline.py:57/170` (a third `def scraper_loader` at `:102` is *not* in the group) | 25 | 25-line fake scraper |
| `_run_plugin` | 2 — `tests/jobs/adapters/plugins/static/test_wp11_leaf_plugins.py:17`, `tests/jobs/adapters/plugins/static/test_wp12_leaf_plugins.py:17` | 22 | cross-module |
| `loader` | 2 — `tests/test_jobs_fetcher_pipeline_incremental.py:177/219` | 18 | 18-line fake loader |
| `set_source_diagnostics` | 2 — `tests/test_provider_adapters.py:45`, `tests/test_provider_migration.py:47` | 17 | **cross-file**, 17-line fake store method |
| `_active_job` | 2 — `tests/test_jobs_lifecycle_retired_source_drain.py:52`, `tests/test_jobs_lifecycle_source_evidence.py:36` | 14 | cross-module |
| `delayed_fetch_with_retries` | 2 — `tests/test_provider_api_plugins.py:453/514` | 13 | same-file |
| `fake_fetch_directory_pages` | 2 — `tests/source_discovery/test_web_search_directory_candidates.py:208/322` | 11 | fake directory fetch |

Those clusters are 297 of the 532 redundant `tests/*.py` lines; the rest is a long tail of 8–13-line groups.

> **Correction, same error class again.** An earlier revision of this table gave `_run_plugin` as
> `tests/test_plugin_archival_wp11.py:27` ↔ `tests/test_plugin_archival_wp12.py:31` and `_active_job` as
> `tests/test_jobs_fetcher_bridge_source_hooks.py:50` ↔ `tests/test_source_policy_state_contract.py:35`.
> **None of those four files exist** (`git ls-files` returns nothing), `set_source_diagnostics` was labelled
> same-file when it spans two files, `fake_run` was given as `:174/264/297` instead of `:11/40/73`, and the
> four "same file" rows never named the file. The rows above are transcribed from
> `_out/audit_verify/ws10.out.txt` and every path was re-checked against `git ls-files`.

#### Measured 2026-09-30: the largest cluster is shape-similarity, not copy-paste

Started Q3 by checking the top cluster before extracting it. The two `ok_loader`
groups — **107 of the 297 redundant lines, the single biggest item in Q3** — are
**not extractable as duplicate fixtures.** The three copies in
`test_jobs_fetcher_pipeline_contracts.py` share a byte-identical *shape* and differ
in **every value**: title, company, jobLink, sourceJobId, postedAt, and even the
casing each test is exercising (`"remote"`/`"contract"`/`"gaming"` against
`"Remote"`/`"Full-time"`/`"Game"`). A shared `_job_row(**overrides)` factory would
save ~24 lines of field-name repetition, but it couples three tests whose payloads
are deliberately different, which the risk note below explicitly warns against.

So the clone scan's byte-identity is measuring **structure**, and for this cluster
structure is the only thing the tests share. **The −90…−140 projection treats
shape-clones as copy-paste and is optimistic.** Every remaining cluster needs the
same per-cluster check before anyone spends effort on it.

**Extracted, both checked to be byte-identical first:**

| Cluster | File | Lines | Note |
|---|---|---:|---|
| `fake_run` ×3 | `tests/test_install_git_hooks.py` | −14 | The three fakes returned a hand-rolled `class Result`; the shared helper returns the **real** `subprocess.CompletedProcess`, so the fake can no longer drift from the three attributes `install_git_hooks` reads |
| `loader` ×2 | `tests/test_jobs_fetcher_pipeline_incremental.py` | −13 | Row and call counter were the only variables; both are now parameters |

Total realised: **−27 lines** against Q3's −90…−140. Note that `tests/` is outside
the `duplication` gate's `SCANNED_ROOTS` (`src`, `scripts`, `tools`), so test
duplication is unmonitored and these reductions are invisible to it.

#### Measured 2026-10-01: the remaining eight clusters, cluster by cluster

Every cluster in the table above was re-measured rather than trusted, after the
`ok_loader` result showed the clone scan measures *structure*. **Three are
byte-identical, four are shape-similarity, and one is misfiled.**

**Extracted — `run_static_plugin`, 3 sites, −46 lines.** The plan listed
`_run_plugin` as 2 sites / 22 redundant lines. It is **five** `def _run_plugin` in
`tests/jobs/adapters/plugins/static/`, and only **two** are byte-identical
(`wp11`/`wp12`, same md5). `wp3` differs by taking `source_row` as a parameter
instead of computing it — but it passes `source_row("astrid")`, which is exactly
what `wp11` computes from `plugin.__name__.split(".")[-1]`. **Functionally
identical**, so one helper in `tests/helpers/jobs_rows.py` (which already owns
`source_row`) absorbs all three with **no call-site change** for `wp11`/`wp12` and
only the redundant `source_row=` argument dropped from `wp3`'s three calls.
Verified the value is not weakened: the computed string is the same expression.

**Rejected — shape-similarity, not copy-paste:**

| Cluster | Why rejected |
|---|---|
| `check` (40) | same string-literal count, **different values** — the assertions differ per test |
| `scraper_loader` (25) | same shape, different fixture values |
| `_active_job` (14) | same shape, different row payloads |
| `fake_fetch_directory_pages` (11) | **5** sites, not 2, and **4 distinct shapes** (8/7/8/17/0 literals); two take `(*_args, **_kwargs)`, three take `(_timeout_s, page_jobs, **_kwargs)` |

**Two byte-identical clusters deliberately not extracted**, because the arithmetic
does not support the churn:

- `set_source_diagnostics` (17 × 2) is a **method on a per-test fake class**
  (`_FakeDeps`), so sharing it means a mixin and restructures both test files.
  ≈ −15 lines for a structural change to two fakes.
- `delayed_fetch_with_retries` (13 × 2) is a **closure** over `fetches` and
  `fake_deps`. Hoisting it means passing both as parameters, which makes every
  call site noisier than the 13 lines saved.

**Another stale path.** The table cites
`tests/source_discovery/test_web_search_directory_candidates.py`, which **does not
exist**; the real file is `test_web_search_candidates.py`. That is the fourth
path error this table has produced (see the correction note above for the first
three), and it also undercounted the sites by three.

Verified by mutation: making the shared helper `return []` fails 10 tests, so the
suites are not passing vacuously through it. `tests/jobs/` green at 602.

Q3 total realised: **−73 lines** (27 previous + 46 here) against a −90…−140
projection that remains optimistic — four of the eight clusters checked so far are
not extractable, and the four never measured are the long tail of 8–13-line
groups this section already calls low-value.

Split by risk: **same-file repeats (250 lines = 532 total − 282 cross-file) are mechanical** — one
module-level helper per file, no import changes. **Cross-file clusters (282 lines)** need a shared host (`conftest.py` for pytest fixtures,
or a `tests/jobs_fetcher_support.py` helper); the host must not import product internals the tests do not
already touch, and must not couple tests that are deliberately independent. `tests/frontend/**` is
unaffected — beyond the two 0.60 `admin-ops-*controller` pairs named in *Refuted Claims*, the JS corpus shows
no clone signal.

**Verify:** `python -m pytest tests/test_jobs_fetcher_pipeline_basics.py
tests/test_jobs_fetcher_pipeline_contracts.py tests/bridge/test_job_availability_atomic.py
tests/bridge/test_job_availability_routes.py -q`, then `--group line-budget` (caps only shrink, so no
re-baseline), and confirm `tests/test_*_quality.py` budgets stay green.

### De-queued — the genuine cross-module `src` clones

This was item Q4 in the first draft. It is **de-queued**: 9 groups / 144 redundant lines converts at roughly
−40…−70, and every move crosses a module boundary. The reason is *size*, not gate leniency — the
duplicate-body gate is **enforcing** (see the correction below), so this work is what the gate actually
prefers; it is simply not worth 40-70 lines of cross-boundary churn right now. Recorded so the analysis is
not lost, not as a commitment. The defensible candidates were:

| Clone | Sites | Redundant |
|---|---|---:|
| `_family_tokens`, `_is_careerish_path` | `src/bridge/registry_conflicts_row_path.py:29/65` ↔ `src/source_registry_policy.py:183/217` | 37 |
| `_parse_html` | `src/jobs/adapters/plugins/static/immersity.py:36` ↔ `src/jobs/adapters/plugins/static/outerdawn.py:38` | 33 |
| `_is_expected_client_disconnect` | `src/bridge/handler.py:47` ↔ `src/ship/desktop_app/runtime_launcher.py:104` | 13 |
| `_registrable_host` | `src/jobs/adapters/static_cookie_retry.py:37` ↔ `src/jobs/common/browser_headers.py:51` | 13 |

**Step 1 is choosing the host from the import graph, not from line count.** AGENTS.md forbids importing
composition-root modules from narrow helpers, so the host must be the *leaf* of each pair — confirm with
`git grep -n "registry_conflicts_row_path\|source_registry_policy" src` before moving anything.
`_parse_html` is probably **not** worth merging: diff the two bodies first, because alpha-renaming
equality does not prove the adapters parse the same pages identically.

**Explicitly excluded:** `src/ship/desktop_app/_linux.py:56` ↔
`src/ship/desktop_app/_windows.py:227` `_stale_runtime_reclaim_result` (20 lines; an earlier revision of this
line cited `:116`/`:83`, which are not the clone sites). Those are intentional
per-platform twins; AGENTS.md requires keeping them in sync and testing both lanes
(`npm run test:py:linux` and `npm run test:py:extended`). Merging them would break the platform split.
`ws4.out.txt` counts **25 shared symbol names** between those two files — the whole set is deliberate, not
drift, so do not treat a clone report that reaches this pair as actionable.

The duplicate-body gate is **enforcing**, not warn-only — this doc had it exactly wrong, in three ways at
once. (1) The flag is `DUP_GATE_ENFORCING = True` at **`tools/repo_health/repo_guardrails.py:103`**; there is
no such symbol in `duplicate_body_policy.py`, which is why grepping that file for it returns nothing.
(2) Its comment (lines 99-102) says the baseline "holds exactly" **6 known groups**, which is stale:
`tools/repo_health/duplicate_bodies_baseline.json` contains **2** patterns — the baseline is the ground
truth, the comment is not. The two are `77490ab82fbd` = `_path_size` ×3 (`src/bridge/storage_health.py`,
`src/storage/baluffo_store.py`, `src/storage_metrics.py`) and `81362ab4777d` = `_require_root` ×3
(`src/source_discovery/orchestrator_{finalize,generation,probe}.py`), each at its `max_copies: 3` cap
(`python _out/ws-verify/oq2.py` §D prints them with their sample bodies). **Neither is a
`*_policy.py` mirror** — see (4). (3) Consequently the `duplication` group **fails the run** on a new
duplicate pattern; it does not merely warn. The baseline is digest-keyed under a `max_copies` ratchet, so removing a
baselined body trips the *stale-baseline* check and fails too. Run the group and prune only what its message
names; never guess baseline entries. Updating the stale comment at `repo_guardrails.py:99-102` is a free
cleanup, not a queue item.
(4) The gate cannot see the *Correction 5* mirrors at all, and will not protect or flag them: it scans only
`src/`, `scripts/`, `tools/` (so every `tests/**` twin is out of scope), and a group needs `MIN_COPIES = 3`
identical bodies to qualify. The policy/pytest mirrors are 2-copy groups, so they are invisible to it by
design — deleting or keeping them is a judgement this doc has to carry, not a gate.

### Q4 — CSS: 18 selectors have no reachable producer (236 rule lines)

The inventory is real and measured: 5 tracked files / 10,182 lines / **712 distinct class selectors** once
comments are stripped (1,396 rule heads; 1,312 `ws14`-style head matches), **27 `@media` blocks**,
**6 `!important`** declarations, and 684 distinct repeated-declaration kinds carrying **2,780 extra copies**
(`_out/ws-scripts/ws14.py`, re-run 2026-09-29: `TOTAL selectors=1312 !important=6`, per-file kinds
220/5/198/83/178 = 684, extra copies 1175/5/679/200/721 = 2,780; the 27 `@media` were counted independently
and agree). Selector-count conventions are settled in *Correction 4*; the two that still need stating here:

- **684 / 2,780** are *identical `prop: value` text repeated inside one file* (ws14 keys a `Counter` on the
  whole declaration). They are **not** "a selector re-sets a property it already sets" — that narrower
  reading, keyed on `(file, selector, property)`, gives 361 kinds / 4,754 extra copies. The nominal win from
  collapsing the ws14 set is far smaller than the copy count suggests, because most copies are legitimate
  state or media overrides.
- *"Nothing references `styles/`"* is scoped to the roots ws14 greps — `tools/repo_health`, `scripts`,
  `tests/frontend/unit` — and holds there (0 hits in `tools/`, `scripts/`, `package.json`,
  `.github/workflows`). It is **not** true repo-wide: the root entrypoints link the stylesheets
  (see *Correction 1*) and 14 tracked files match `\.css`, all of them frontend unit tests
  (`admin-css-guards`, `jobs-css-guards`, `startup-css-performance`, `helpers/admin-css-fixtures.mjs`, …).
  The accurate statement is: **no guardrail gate polices `styles/`; frontend tests assert on parts of it,
  and `npm run test:frontend:unit` is the only automatic thing between a deletion and a regression.**

A literal-name sweep over 2,126 tracked consumer files (22.9 MB of `.py/.js/.mjs/.cjs/.html/.json`) flags 23
selectors on raw text / **22** once comments are stripped, and the composition filter clears 21 / **20** of
them. **Passing that filter was never evidence of life** — it only means "some string somewhere shares a
dash-prefix with this class". `.phase-hint` is the clean illustration: it was cleared because
`tests/frontend/unit/admin-domain.test.mjs` uses `"phase-hint-before-report-progress"` as a **test case id**
for a `deriveDiscoveryProgressModel(…, { phaseHint: … })` assertion — a JS option name, not a class.

So the 20 survivors were read one at a time against the render sites (`_out/ws-verify/oq3.py` → `oq3b.py` →
`oq3c.py`, verdicts priced by `_out/ws-verify/oq3d.py`). Each was asked: is there a **producer** — a
`parent-${…}` template, an f-string, a concatenation, or a mid-string composition
(`parent-${expr}-tail`) — whose interpolated value domain can yield the exact tail?

| Verdict | Selectors | Rule lines | Evidence |
|---|---|---:|---|
| **EMITTED** | `.admin-dedup-audit-gate-detail-blocker`, `-detail-warning` | 5 + 5 | `ops-summary-dedup.js:517` builds `admin-dedup-audit-gate-detail-${escapeHtml(type)}`; the call sites at `:599`/`:603` pass the literals `"blocker"` and `"warning"` |
| **EMITTED** | `.inspector-severity-critical`, `-warning` | 6 + 6 | `inspector.js:149` builds `inspector-severity-${entityData.severity}`, and `inspector.js:83` fixes that domain to exactly `"critical" : "warning"` |
| **DEAD — producer cannot yield the value** | `.action-center-status-critical` | 6 | only producer is `action-center.js:131` `action-center-status-${meta.tone}`; `STATE_META` (`action-center.js:20-24`) admits tones `ok`/`neutral`/`neutral`/`neutral`/`warning` — never `critical`. Nothing asserted it: the CSS guard at `admin-action-center-css.test.mjs:35` iterates only `ok`, `warning` |
| **DEAD — no producer anywhere** | `.admin-dedup-audit-gate-flag`, `-flag-label`, `-flag-value`, `-flags`, `.admin-pin-row`, `.admin-task-badge`, `.country-selection-badge`, `.job-title-wrap`, `.jobs-pipeline-group`, `.jobs-top-nav`, `.phase-hint`, `.phase-row-actions`, `.popup-description`, `.saved-view-preset-btn`, `.timeline-open` | 195 | 15 selectors: for 14 of them `git grep -F <name> -- ':!styles'` returns **zero tracked files**; the 15th is `.phase-hint`, whose only two non-`styles` occurrences are the `phaseHint` test case ids named above — JS option names, never class lists. And no `parent-${`, `parent-{`, `parent-%s`, or mid-string producer exists for any of the 15 |

Together with the two selectors the composition filter already condemned — `.btn-close` (2 blocks, 11 rule
lines: `styles/components.css:586`, `:591`) and `.job-sector` (3 blocks, 24: `styles/components.css:1957`,
`:2418`, `styles/jobs.css:603`) — the deletion set is **18 selectors / 236 rule lines** (35 + 201), all priced
head-through-close inclusive, the same convention that yields the 223-line figure for the 20 survivors.

The near-misses are worth naming, because they are what a naive producer search reports as "live" and they
are all false: `.jobs-pipeline-group`'s only `jobs-pipeline-{…}` producer is a **Python thread name**
(`src/bridge/pipeline_service_stages.py:512`); `.job-title-wrap`'s parent appears in a **scraper regex for
third-party HTML** (`src/jobs/adapters/plugins/static/astrid.py:32`) and in `class="job-title-compact"`
(`frontend/shared/components/JobRow.js:175`), a different class; `.popup-description`'s parent appears as
`panel.className = "popup local-auth-dialog"`, again a different class; `.timeline-open`'s parent matches
only `activity-timeline`-shaped names in `saved.html`.

**Why this is now deletable rather than "needs a browser check":** for these 18 there is no code path in the
repo that can attach the class, and — checked the same way — **no test asserts their existence**, so the
change cannot break an assertion either. The honest residuals are (a) styling kept deliberately for future
use, (b) the fact that `styles/` has no gate, so run `npm run test:frontend:unit` (969 tests, green as of
this pass) plus a load of `index.html`, `jobs.html`, `admin.html`, `saved.html` to catch anything a static
read of a stylesheet cannot see.

**Re-verified 2026-09-30, after the 0.3.0 Admin rebuild and the reconnect banner.**
This analysis predates both, and the Admin rebuild in particular touched
`styles/admin.css`, so the deletion set was re-checked against the current tree
rather than trusted. **All 18 still have no reachable producer.** 15 return zero
hits outside `styles/` across 2,285 tracked consumers; the other three are the
documented non-obvious ones and all still hold:

| Selector | Why it is still dead |
|---|---|
| `.phase-hint` | its only non-`styles` hits are still the two **test-case ids** in `admin-domain.test.mjs` (`phase-hint-before-report-progress`, `phase-hint-overrides-stale-starting-shell`) — JS option names, never class lists |
| `.action-center-status-critical` | the only producer is still `action-center.js:131` `action-center-status-${meta.tone}`, and `STATE_META` (`action-center.js:19-25`) still admits exactly `ok` / `neutral` ×3 / `warning`. Never `critical` |
| `.job-sector` | every hit is the **distinct, live** `job-sector-line` class (`saved/render.js:133`, `JobRow.js:160`, plus two test assertions) or the `custom-job-sector` `<select>` id in `saved.html:152`. CSS class selectors match whole tokens, so none of those can attach `.job-sector` |

`.btn-close` now has **zero** hits outside `styles/` at all. Note the trap this
table exists to catch: `job-sector` was originally "cleared" by the composition
filter purely because `job-sector-line` shares its dash-prefix — clearing is not
being alive.

**The browser pass was the one outstanding step for Q4, and it was done on
2026-10-01** — see *Q4 and Q5: landed, unreleased*. Recording it because the
reason it was necessary generalises: nothing above substitutes for it, because
`styles/**` has no gate and a static read of a stylesheet cannot see a cascade
interaction. It was run as a before/after pixel diff of all three entry pages
rather than a visual pass, which turned "it looks the same" into a measurement:
`saved.html` identical, `jobs.html` 0 pixels over threshold 40, `admin.html`
matching except the live fetcher clock. **Any future `styles/` change needs the
same treatment, because the lane will not catch it.**

One prediction below was wrong and is corrected in place: the deletion set was
priced at **236 rule lines**, and the shipped change removed **195**. The
difference is the four selector lists that were only *partly* dead and had to be
edited by hand (`.job-sector` out of two lists, `.admin-dedup-audit-gate-flag-label`
and `-flags` out of two dedup-audit lists) rather than dropped whole.

**Method for any future sweep:** extract values of `class="…"`, `className`, and `classList.*` arguments
— not identifiers — then find the *producer* of each candidate and read its value domain. A prefix hit in an
unrelated string is not life; a template whose interpolated domain cannot contain the tail is not life
either. **Do not build a dead-selector gate**; Wave 4 already rejected one, and the 91% false-positive rate
of the naive filter — plus the 16-of-20 reversal in the other direction on this very pass — is the reason.


### Q5 — CSS: the two custom properties that are genuinely unused

`styles/` defines **106** custom properties. Taken per file, `base.css` looks like it defines 94 and
references 12, i.e. "82 unused" — that number is an **artifact of per-file scoping** and must not be used:
CSS custom properties cascade, so a token defined in `base.css` is consumed by the sheets loaded after it.
Over the repo-wide union (`var(--x)` anywhere in tracked source or any stylesheet), only **2** are
unreferenced: **`--bg-overlay`** and **`--surface-17`**.

Worth roughly 10–40 lines including any `!important` shadow of the same declaration — small enough that the
change should ride along with another CSS commit rather than stand on its own. Verify by
`python _out/ws-scripts/ws15.py` (prints the unreferenced set) and by loading `index.html`, `jobs.html`,
`admin.html`, `saved.html` after the edit; there is no guardrail gate covering `styles/`, so nothing will
fail automatically if a property is removed while still in use — the visual check *is* the test.

## Non-Goals

- **Do not touch the registry service files** — but not for the reason written here before. The four files
  this bullet named are mostly fiction: `source_registry_seed.py`, `source_registry_service.py` and
  `source_registry_deletion.py` **do not exist**. The real surface is **12 `src/source_registry*.py`
  modules (3,680 lines**, `src/source_registry.py` itself only 173**)** plus `src/storage/source_registry_runtime.py`
  and `src/storage/migrations/008_source_registry_runtime.sql` — 14 paths / 4,286 lines; `merge2` scopes the
  family at 13 files / 4,233 lines including its tests. "Wave 3 removed the 2,105-line god file" is also
  wrong: **W3 was route de-chaining at −8 lines** (`02a827af`) and no 2,105 figure appears anywhere in the
  closed programme's record. Leave the family alone anyway — `ws10` finds only two clone groups inside it
  (`_family_tokens`, `_is_careerish_path`, 37 lines total) and merging was priced at 119 lines (2.8%).
- **Do not delete anything from `scripts/`, and do not relocate it either.** Measured reachability is
  100%, and the three named "orphans" are imported by live siblings (see *Refuted Claims*). The area is
  **81 tracked files / 24,760 lines**. Moving dev-only scripts to `tools/` saves **0 LOC** while touching
  **40 `package.json` entries whose commands reference `scripts/`** — far more than the "≥12" this doc first
  wrote: `build`, `build:container-frontend`, `build:frontend-runtime-config`, `build:linux`,
  `build:portable-exe{,:prepare}`, `build:ship-bundle`, `prepare`, `verify`, `setup:hooks`,
  `prepush:warm`, `prepush:full`, `check:ai-env`, `check:published-version`, `check:python-version`,
  `lint:precommit:{all,changed,ci}`, `test:refactor:changed`, `security:js`, `security:python`,
  `measure:container-jobs-boot`, `probe:desktop:startup:*`, and the whole `perf:*` family — and **7 workflow
  files** (`benchmark`, `build-linux`, `build-portable-exe`, `jobs-boot-perf`, `perf-admin-flows`,
  `source-sync-checkpoints`, `validate-source-sync`). It also carries a *silent* hazard:
  `scripts/precommit_gate.py:191` watches `("src/","frontend/","tests/","scripts/",".github/","docs/")`
  (applied at `:204`), so a file moved into `tools/` keeps working and simply **stops being gated**. If
  anything ever moves: widen `watched_roots` first, one file per commit, then `npm run lint:precommit:all`.
- **Do not merge the `src` micro-split families.** Merging all nine (97 files, **32,038** lines) and deduping
  every docstring and module import saves **1,026 lines (3.2%)** — 1.4% for `local_data_store`, 8.3% at
  best for `active_audit` — and it means rebuilding a 5,769-line `source_sync.py` and a 4,568-line
  `registry_conflicts.py`. The closed programme deliberately *spent* **+2,663** lines (`W2a/W2b/W7b` god
  files **+2,090** at `065e0712`, `W2a` remainder **+573** at `50b5fa6b`) on the legibility this would undo.
  Rejected on the repo's own record, not on taste.
- **Do not touch `data/`.** It is product output, not source. Tracked it is **12 files / 140,042 lines =
  20.9%** of the tree's 669,050 tracked lines; the working tree including gitignored job reports is 147 files
  / 2,297,681 lines. The "1,000,391 tracked lines (65.5%)" figure this bullet carried is the closed
  programme's and is not reproducible today under either definition — the *conclusion* (out of bounds) does
  not depend on it.
- **Do not reduce test count for count's sake.** The behavioural surface is **4,736** Python test functions
  and **966** `node:test` call sites; collected on 2026-09-29 that is **5,655** Python units (5,460 in the
  developer lane, 195 deselected) and **969** JS units, all 969 passing. The coverage-verified collected
  Python lane stays **5,450 across 685 files** (`test-reduction-triage.md`, 2026-09-27). Wave/`W5` test
  consolidation already landed at **−444**,
  and the closed programme's own lesson is that test work converts badly. What this bullet said before —
  "Wave 4 harvested the 40 genuinely redundant units and landed at −23 net instead of the projected −430" —
  **is not in the closed programme's record**: `W4` is the re-export shim + Protocol consolidation item
  (estimated −1,500…−3,000 against a hard ceiling of 2,068 lines) and was **rejected**, and neither −23 nor
  −430 nor "40 units" appears in either closed doc. All rules in
  [`test-reduction-triage.md`](test-reduction-triage.md) still apply, including the retained
  coverage map and the "never delete the last coverage of a behavior" rule.
- **No CSS dead-selector gate, no frontend HTML minifier, no blanket CSS split** — Wave 4 rejections stand.
- **Do not merge the platform twins** `_linux.py` / `_windows.py` (AGENTS.md sync requirement).
- **Do not touch the compatibility surfaces the `compat` group guards.** The "4 `src/ui/web_*.py` compat
  shims" this bullet used to protect **do not exist** — `git ls-files src/ui/web*` returns nothing, and there
  is no `src/ui/` split of that shape. What actually exists and actually guards behaviour:
  `run_compat_group()` (`tools/repo_health/repo_guardrails.py:746`) imports `suite_contract_policy` and runs
  **every** `test_*` function in it except the two listed in its `excluded` set
  (`test_discovered_python_test_files_define_real_tests`,
  `test_frontend_test_patterns_disallow_generated_manifest_aggregators`), plus
  `check_bridge_api_field_inventory()` and the task-run-owner check. The compat-ish files are
  `src/ship/desktop_app/_compat.py` (with `tests/desktop_app/test_compat_dispatch_integrity.py`) and
  `src/jobs/fetcher_compat_runtime.py`. And do not prune the **2** patterns currently in
  `duplicate_bodies_baseline.json` — not the "5" claimed here before, and see *De-queued* on why the gate
  itself is enforcing rather than advisory.
- **Do not wire an orphan policy function into a group list without reading its signature and its twin.**
  Q6 landed this correctly and the rule is the residue: `_run_python_check` calls `check(repo_root=ROOT)`
  only, so a `tmp_path`-taking policy function **cannot** be wired at all — it is dead by signature, and the
  two that were (the `workflow_policy.py` copies once at `:934` and `:966`, now deleted — those line numbers
  no longer exist in the file). Of the ones that *can* be wired, decide by
  reading the running pytest twin: duplicate → delete the policy copy; unique → register it. Blindly listing
  a drifted copy turns a hidden hole into a red run and hides the real question, *which assertion is right*.
  The one that used to be listed here as "failing, so do not wire it"
  (`release_docs_policy.py:407`, the `release:preflight` pin) was re-pinned and registered upstream in
  `3c2c7e8f`. See *Correction 5*.
- **A `test_*` in a policy module is not a test until a group names it — now enforced, not remembered.**
  `run_workflow_group` and `run_release_group` name their checks explicitly (an unlisted function never
  runs), while `run_compat_group` enumerates `suite_contract_policy.py` by introspection and auto-joins
  everything except two named exclusions (so a function added there runs immediately, and a script that
  greps only for explicit lists reports all 21 of its functions as never-run — this audit's first pass
  did exactly that). Three unexecuted functions reached an audit to be found instead of being caught at
  commit time, and the wiring had been done by hand twice: `3c2c7e8f`, then `03bb67c3`.

  **Q7 landed as `0c3e0372`**: `tools/repo_health/policy_wiring_policy.py`, three checks registered in the
  `workflow` group. It reads the group lists out of `repo_guardrails.py` **by AST**, so it audits the real
  thing rather than a re-derivation that could itself drift, and models both registration styles — a
  discovery group counts as registered-by-construction, or `compat`'s 59 functions would all report as
  never-run. The three properties: every defined `test_*` is registered; every registered name has a
  definition; every registered check is callable the way the runner calls it. That last one is what turns
  "unlisted" into "un-wireable" — the real contract is narrower than "takes `repo_root`":
  `_run_python_check` does `check(repo_root=ROOT)` *if* the signature has `repo_root` and `check()`
  otherwise, so a registered check may require no parameter other than `repo_root`, and zero-argument
  checks are legitimate. My first draft required exactly `["repo_root"]` and rejected a dozen working
  checks; the mutation test caught it immediately, which is the reason to run one.

  Mutation-verified one at a time from a pristine tree: remove a name from `run_workflow_group`'s list →
  caught by name; register a name with no definition → caught by name; add a `tmp_path`-taking check and
  register it → caught as un-callable. My first harness chained four `-replace` operations that cancelled
  two of the three mutations, nearly proving a false pass — **a mutation harness is itself code and needs
  the same scepticism as the guard.**

- **Do not re-open the closed Wave 1–4 program.** Nothing here contradicts its outcome; it is a smaller,
  separate hygiene queue.

## Reproduction Kit

Scripts live in `_out/audit_verify/` (**scratch, gitignored — not part of the repo**). If a check becomes
durable it must move to `tools/repo_health/` with a test; until then the numbers are reproducible from the
recipes below, and the doc — not the scripts — is the record. Per-workspace evidence sits in
`_out/ws-tests/findings.md`, `_out/ws-families/findings.md`, and `_out/ws-scripts/findings.md` — the last of
which contains CSS figures that were later disproved; read its retraction banner before quoting it. Every
command in this doc was re-run and its exit status checked, because a batched run whose earlier step failed
leaves *all* of its output unverified (see *Correction 1*).

| Script | Prints | Still trustworthy? |
|---|---|---|
| `ws4.py` | Pairwise similarity (difflib on normalised lines) for three corpora: the inventory tool family (9 files / 2,464 lines, top pair 0.817, 7 pairs >0.40), the `jobs_fetcher` test cluster (38 files / 9,546 lines, **zero** pairs >0.55), and `tests/frontend/unit` (**222** `.mjs` files / 44,096 lines, 2 pairs >0.60). Scope note: 235 `.mjs` files are tracked under `tests/`; the 13 outside `unit/` are not in its corpus. It also lists the 25 shared `_linux`/`_windows` symbol names | Yes — its *null* results are the finding |
| `ws5.py` | Token reachability for `scripts/**` against workflows, `package.json`, tests, docs | Yes |
| `ws7.py` | Test-budget cap vs actual line count per entry; slack table | Yes |
| `ws8.py` | Member-wise AST diff of the two facade-inventory modules | Yes |
| `ws9.py` | First-pass CSS "dead selector" list | **No** — superseded by `ws15.py`, see Q4 |
| `ws10.py` | Function-level clone scan: AST bodies ≥8 lines, alpha-renamed (names→`N`, literals→`K`, docstrings dropped), exact `ast.dump` match, grouped per bucket with cross-file tallies | Yes, with the caveat that equality ≠ identical semantics |
| `ws11.py` | Read-only guardrail runs + `duplicate_bodies_baseline.json` / `test_line_budget_baseline.json` state | Yes |
| `ws12.py` | CSS composition re-check: for each flagged selector, search full name *and* dash-prefix stems inside template-literal / f-string lines | **This is the method to use** |
| `ws13.py` (`_out/ws-tests/`) | tests/ per-directory distribution, heaviest files, meta-test tally | Use with care — it counts `count("\n")+1`, i.e. **+1 line per file**. `splitlines()` is the `loc_budget` method and that is the one to quote |
| `ws14.py` (`_out/ws-scripts/`) | styles/ selector, `!important`, custom-property and repeated-declaration counts | Yes, but compute custom properties as a **repo-wide union**: per-file `defined−referenced` reported a bogus "82 unused" in `base.css` when the repo-wide truth is **2** |
| `ws15.py` (`_out/ws-scripts/`, output `ws15_out.txt`) | Self-contained dead-selector scan: parse the 5 tracked stylesheets, build one token set over the 2,126 tracked consumer files, then filter flagged selectors by dash-boundary prefixes found inside string literals | Yes — this is the authoritative selector number (23 flagged → 21 composed → **2 dead / 21 lines**) and it also prints the repo-wide unreferenced custom-property set. Written *after* an earlier revision of Q4 cited a `ws15.py` that did not exist |
| `_out/ws-families/` (`families.py`, `merge2.py`, `analyzers.py`, `proto_tool.py`) | Family merge arithmetic, analyzer similarity, and the five-tool parameterisation prototype with parity check | Yes for the arithmetic (`merge2_out.txt` per-family totals sum to **32,038**) and `proto_tool.py`'s parity run. **No** for three of its prose lines in `findings.md`: the `inventory_common` boolean column under-reports (the file has **6** importers — the five analysers *plus* `bridge_api_field_inventory.py`), its "11 groups / 87 lines" clone tally has no artifact behind it (`ws10.out.txt` says 9 groups / 144 lines for `src`), and its "exactly 1 src file imports a private sibling symbol" is attributed to a `ws8.py` that does something else |
| `_out/ws-verify/` (`claims.py`, `q1.py`–`q4.py`; outputs `claims_out2.txt`, `ws14_rerun.txt`) | The 2026-09-29 independent re-measurement pass: styles metrics with alternative tokenizers, analyzer similarity and rename blast, OQ2 body-line checks, budget-owner greps, family/clone/LOC/misc claims in one `OK`/`DIFF` table, Q1 slack table, tracked-line and `data/` totals, unit counts, cited-path existence | Yes, and re-runnable: `python _out/ws-verify/claims.py` prints an `OK`/`DIFF` line per claim with `DOC=`/`MEAS=` values. It is **scratch and gitignored** — anything here that earns permanence must move to `tools/repo_health/` with a test |
| `_out/ws-verify/` (`refresh.py`, `pyunits.py`; outputs inline) | Re-anchors absolute counts to the current HEAD: tracked files/lines, `data/` share, static + collected unit counts, styles line count, corpus size; `pyunits.py` splits module-level vs in-class `def test_` | Yes — **run this first after any commit**, it prints the HEAD it measured |
| `_out/ws-verify/` (`oq2.py`, `oq2_final.py`, `oq2_run.py`, `oq2_stale.py`) | OQ2 closure: which policy functions the group lists actually name, assert-level diff of each mirrored pair, the two baselined duplication patterns (`oq2.py` §D), and an **in-process call of each never-run function** to show whether it passes (`oq2_run.py`, `oq2_stale.py`) | Yes, but `oq2_run.py` **executes** policy functions — one spawns `npm run dev:pipeline` with a missing source — and the two it records as failing are the two the tree has since fixed or deleted. Read it as the `2ee26c03` state, not as today |
| `_out/ws-verify/` (`oq2_v3.py`, `oq2_counts.py`, `q6_proof.py`) | **The Q6 instruments, and the ones to re-run.** `oq2_v3.py` re-derives defined-vs-executed per policy module at the *current* HEAD (handling both wiring styles: explicit lists vs `inspect.getmembers` minus `excluded` — reading the compat `excluded` literals as an allow-list would falsely report 57 of 59 compat checks as dead), calls each never-run function, and pairs it with any pytest twin. `oq2_counts.py 2ee26c03 8ede5716 WORKTREE` prints the same comparison per commit. `q6_proof.py` mutates a throwaway `package.json` to show the newly registered check fails when it should | Yes — `oq2_final.py` and the prose built on it are pinned to `2ee26c03`; `oq2_v3.py`/`oq2_counts.py` are HEAD-agnostic and print the HEAD they measured. `q6_proof.py` touches nothing in the repo |
| `_out/ws-verify/` (`oq3.py`, `oq3b.py`, `oq3c.py`, `oq3d.py`; outputs `oq3.txt`, `oq3b.txt`, `oq3c.txt`, `oq3d.txt`) | OQ3 closure: reconciles both CSS counting methods, reproduces the composition filter, then hunts a real **producer** per surviving selector (`${…}`, f-string, `%s`, concat, mid-string) and prices each verdict head-through-close | Yes. `oq3b.py`/`oq3c.py` are the reusable part: a prefix hit is not a producer, and a producer whose value domain cannot yield the tail is not either |

Three recipes worth keeping regardless of scripts:

- **Clone scan:** parse every `.py`, take function bodies with ≥8 non-blank statements, replace every
  `Name`/`arg` with a shared placeholder, every constant with `K`, drop docstrings, hash `ast.dump`, then
  group equal hashes. Report lines beyond the first copy per group, split cross-file vs same-file.
- **CSS liveness:** parse class selectors out of the **comment-stripped** tracked stylesheets, build **one**
  token set over the tracked consumer corpus (a regex per selector is too slow — the naive version timed out
  at 30 s), then treat a selector as live if its full name appears **or** any dash-boundary prefix of it
  appears inside a string literal. Measured effect: 22 flagged → 20 ruled out as possibly-composed. **That is
  only stage one** — the prefix set is what makes the list usable, and it is also what makes it 80% wrong in
  the "still alive" direction. Stage two is the producer read: for each survivor, search for
  `parent-${`, `parent-{`, `parent-%s`, `parent-" +` and `parent-…}-${tail}` sites, open them, and check the
  interpolated value's domain. Then cross-check the survivors with `git grep -F <name> -- ':!styles'`: zero
  files outside `styles/` plus no producer means dead.
- **Collected unit counts:** `python -m pytest tests -q --collect-only` (add
  `-m "not slow and not packaging and not release"` for the developer lane) with `BALUFFO_DATA_DIR` pointed
  outside `data/`, then `git status --porcelain data/` to prove the run wrote nothing into the repo. For JS,
  `node --test --test-reporter=tap tests/frontend/unit/*.test.mjs` and read the `# tests / # pass / # fail`
  trailer — the `dot` reporter used by `npm run test:frontend:unit` does not print totals.

### Verification Log (2026-09-29)

A second pass re-measured every load-bearing number in this doc with a script that prints `DOC=` / `MEAS=` per
claim (`_out/ws-verify/claims.py`, output `claims_out2.txt`), plus `q1.py`–`q4.py` for the paths, unit counts,
tracked-line totals and cited-file existence checks.

**Reproduced exactly** (independent re-derivation, no change made): the 5 stylesheets / 10,182 lines and every
per-file count; 684 repeated-declaration kinds / 2,780 extra copies, 27 `@media`,
6 `!important`, `base.css` 94 defined / 12 referenced props, 2 unreferenced custom properties
(`--bg-overlay`, `--surface-17`), the 2,126-file / 22.9 MB consumer corpus; the Q4 dead-selector pair and its
per-block attribution (`btn-close` ×2 + `job-sector` ×3 blocks); the five analysers at
920 lines and their five test modules at 1,100; 10 similarity pairs, max 0.789, exactly one above 0.60;
`inventory_common.py` at 77 lines; Q1's 55 entries / 24 stale caps / 679 lines of slack / 31 tight / 0 missing
and every row of its slack table; the 3 linkers to
`test-reduction-triage.md`; 9 families / 97 files / 1,026 saved lines; the `precommit_gate.py:191` watch list;
the 7 workflow files; the absence of the analyzer module names from `.github/`, `docs/`, `scripts/` and
`package.json`. The 223-rule-line price of the 20 surviving selectors also reproduced exactly, and so did the
`git grep -F` cross-check under the later producer read.

**Two figures the log previously listed as "reproduced exactly" are convention-dependent, not exact**, and are
now stated with their convention instead of as facts: "1,312 selector occurrences" is `ws14.py`'s
rule-**head** count (1,396 heads → 1,312 matches; comma-split class tokens give 2,188), and "718 distinct
class selectors" is `ws15.py` counting class names that appear only inside `/* … */` comments — six phantom
names, including `.admin-ops-*` from `styles/admin.css:1892`. Comment-stripped, both methods agree on 712.
The "mirrored bodies at exactly 38 and 29 lines" claim was also half right: the 38-line and 29-line pairs are
`test_location_unknown_country_manifest_…` and `test_release_notes_extractor_…`; the `dev_pipeline` pair is
**31 lines on the pytest side and 30 on the policy side**, because the two have already diverged.

**Corrected by this pass** — each was a real error, not rounding: the LOC table date/totals; the `data/` share;
the inverted test-unit split; family total 32,638 → **32,038** and 3.1% → **3.2%**; the "W5 +573"
misattribution; the "W4 harvested 40 units at −23 vs −430" and "Wave 3 removed a 2,105-line god file"
sentences (neither figure exists in the record); the duplicate-body gate (warn-only → **enforcing**, flag in
`repo_guardrails.py:103` not `duplicate_body_policy.py`, **2** baselined patterns not "~10" or "5");
"all five tools import `inventory_common`" → **6** importers; 29 inventory test names → **37**; the registry
survivors (3 of the 4 named files do not exist); the four `src/ui/web_*.py` shims (none exist); `scripts/`
109 files → **81** and its blast radius ≥12 → **40** `package.json` entries; the soak/perf meta-test tally
(~2,778 in 5 → **4,562 in 14**); the inventory set 7 files / 2,345 → **9 files / 2,464** and its "all
near-clones" framing; five Q3 rows with paths that do not exist; `browser_headers`' directory; the
`_linux`/`_windows` clone-site line numbers; the `src` clone tally; OQ3's "9 selectors / 93 rule lines"; and
OQ4, now answered.

**Resolved by the lane run that followed this pass (2026-09-29)** — the three items previously held back as
"needs a lane run, do not assert" are now measured, and the answers are asserted above:

1. **OQ2 — the mirrored bodies: the pytest copies are not the redundant side.** The `workflow`/`docs` groups
   name their checks explicitly, and **none** of the three mirrored names appears in any group list, so the
   *policy* copies never execute. Called in-process, two of the five never-run functions **fail**:
   `workflow_policy.py:933` asserts a report file that the tool never writes for this input (the running
   pytest twin asserts the opposite and passes), and `release_docs_policy.py:407` pins a `release:preflight`
   string `package.json` has outgrown. Full write-up in *Correction 5*; scripts `oq2*.py`.
   **Landed the same day, and the finding shrank under re-measurement:** `3c2c7e8f` fixed the
   `release_docs_policy` half upstream (re-pinned + registered, and deleted the `tmp_path` copy as a
   byte-identical pytest duplicate), which is why the set is **3 functions / 93 lines** at `8ede5716` and not
   5 / 165; Q6 then deleted the two un-wireable copies and registered the third. Re-running `oq2_final.py`
   against today's tree no longer reproduces the five-function table — `oq2_v3.py` is the version that
   re-derives it at whatever HEAD it runs in.
2. **Collected unit counts** — 5,655 Python (5,460 developer lane, 195 deselected) and 969 JS executed,
   969 passing. `data/` verified untouched afterwards. In *Corrected Baseline*.
3. **OQ3 — the 20 composition-cleared selectors: 16 of them are unreachable.** 4 have a producer whose value
   domain is read from the call sites (`detail-blocker`/`-warning` from literal `"blocker"`/`"warning"`;
   `inspector-severity-critical`/`-warning` from a two-valued ternary), 1 has a producer that can never yield
   its value (`.action-center-status-critical`, tone domain is `ok`/`neutral`/`warning`), and **15 have no
   producer at all** (195 lines; 14 of them have zero literal occurrences outside `styles/`, the 15th being
   `.phase-hint`, whose only two are `phaseHint` test case ids). Deletion set becomes 18 selectors / 236 rule
   lines. In *Q4*; scripts `oq3*.py`.

**What the lane run does *not* settle:** the collected counts come from this Windows checkout, so
Windows-only-marked tests are collected here and would not be on the Linux lane — compare like-for-like
before citing either number against a CI log. And no browser was launched: Q4's claim is "no code path can
attach this class", which is a static claim about a closed tree, not a rendered-pixel proof. `npm run
test:frontend:unit` plus loading the four HTML entrypoints remains the check on the day the deletion lands.

**The tree moved under this pass, and it changed absolute numbers — twice.** Verification began at HEAD
`2ee26c03`, finished its measurement at `ebaff931` (v0.3.0 + two hook/CI fixes landed mid-run, one of them
editing `workflow_policy.py`), and the queue started at `8ede5716` — four more commits
(`3c2c7e8f`, `d94ba61d`, `27535f72`, `8ede5716`) that took the LOC total 489,028 → 489,066 and, in
`3c2c7e8f`, independently fixed half of what Correction 5 had just reported. Every count in this doc was
re-run at `8ede5716` and then re-run again after Q6; `python _out/ws-verify/refresh.py` and
`python _out/ws-verify/oq2_counts.py … WORKTREE` print the state they measured. Anything quoted from an
earlier column (488,761 total lines, 4,734 Python test functions, 668,771 tracked lines,
`workflow_policy.py` def lines 888/920, "five never-run policy functions") was true of a commit that is no
longer HEAD — that is why this doc names the commit beside every absolute figure, and why a finding worth
acting on should be acted on before the tree moves again.

## Gate Cheat-Sheet

`repo_guardrails.py` groups: `docs, workflow, compat, routes, frontend, repo-root, test-shape, fixtures,
line-budget, loc, release, registry, bundle, duplication, dead-code`.

| You change | Group that reacts | Also run |
|---|---|---|
| Any tracked file's line count (net reduction) | `loc` | `python tools/repo_health/loc_budget.py --update` **after staging** |
| A test file's length | `line-budget` | shrink caps in `test_line_budget_baseline.json` (max-only: shrinking never fails) |
| Duplicate function bodies | `duplication` | prune only entries the message names; **the gate is enforcing** (`DUP_GATE_ENFORCING = True`, `repo_guardrails.py:103`) — a new duplicate pattern *fails* the run, and so does a stale baseline entry (2 patterns are currently baselined) |
| Inventory analyzer tools (any of the five) | `compat` | each tool's `--check`, the five `test_repo_guardrails_compat_group_runs_*` tests, and all **37** `def test_` names across the five inventory test modules (7/8/11/7/4 — Q2) |
| Frontend markup / CSS | `frontend` | a real browser look at the affected view — no gate catches a deleted style |
| Dead-code deletions | `dead-code`, `test-shape`, `fixtures` | check the pre-push Vulture hook and `whitelist.py` first (AGENTS.md) |
| This doc or any doc link | `docs` | `python tools/repo_health/repo_guardrails.py --group docs` |

## Resume Protocol

1. Read this doc, then [`test-reduction-triage.md`](test-reduction-triage.md) if the item touches tests.
2. Pick **one** queue item. **Q2, Q4, Q5 and Q7 are closed** (Q4/Q5 landed 2026-10-01 in `3f33e089`,
   browser-verified), so **Q3 is the only item left**. Q4/Q5 landed *unreleased* on purpose and ride the
   next version bump — if you are picking up a release, `3f33e089`'s CSS deletion and the `0.3.001` tag
   drift are already waiting for it; see *Q4 and Q5: landed, unreleased*. Note that nothing gates
   `styles/`, so a future CSS change still needs the browser check, and `styles/**` is shipped code, so a
   live version tag can be republished by any push that does not bump.
3. Re-measure before editing — baselines moved **+15,262** lines in eleven days (473,733 → 488,995), and the
   re-measurement on 2026-09-30 found this doc's Q2 sizes overstated (920/1,100 against a real 800/941). Any
   number here older than a week is a hypothesis. Use the *Reproduction Kit*.
4. Implement, then run the item's **Verify** command plus the groups in the *Gate Cheat-Sheet*. Commit
   hygiene items from a clean tree: `loc_baseline.json` and `test_line_budget_baseline.json` are shared
   write-targets, and a session that stages with `git add -A` absorbs another session's caps — Q1 landed
   inside `e7dfe33c` that way rather than in a commit of its own.
5. Update this doc's queue table with what actually landed (identified vs realised), then close the item —
   Q6 is the worked example (93 lines identified, −71 realised, table row flipped to ✅ with the numbers that
   actually shipped). Record durable gotchas in Basic Memory per `tools/mcp/BASIC_MEMORY.md`; repo source
   stays canonical.

## Open Questions

1. **`desktop_update_root_dependency_inventory.py` vs `desktop_updater_root_dependency_inventory.py`** —
   0.627 whole-file similarity but no AST clone group, and their tests differ 206 vs 382 lines. Merge,
   or rename to make the split legible (`update` vs `updater` reads like an accident)? Rename is the
   cheaper fix and probably the right one.
2. **~~Tests mirrored between `tests/` and the guardrail that runs them~~ — answered 2026-09-29 by a lane
   run, inverted from how this question was framed, and closed by Q6.** The premise ("the guardrail group
   already executes the policy function") is false: `run_workflow_group`/`run_docs_group` name their checks
   explicitly and none of the mirrored names was in any list, so the **policy** side never ran. The pytest
   side runs and passes, and `docs/testing.md:360` says policy checks belong in the guardrail rather than in
   pytest — which made the *policy* copies the deletion candidates. What the framing missed is the reason
   they were never listed: `_run_python_check` passes `repo_root` only, so a `tmp_path`-taking copy cannot be
   listed at all. Q6 deleted the two un-wireable ones (68 lines), registered the unique wireable one
   (`test_package_json_uses_direct_frontend_unit_discovery`, which has no pytest twin and would otherwise have
   stayed dead code), and left **zero** unexecuted `test_*` in any policy module. One of the pairs had already
   drifted to contradictory assertions, which is why the pytest side is the one that survived. Full detail:
   *Correction 5*; `Q6` in the queue.
3. **~~The composition filter proves naming, not rendering~~ — answered 2026-09-29 by reading all 20.** The
   per-selector read is done (`_out/ws-verify/oq3.py` → `oq3b.py` → `oq3c.py`, priced by `oq3d.py`): of the
   **20 selectors / 223 rule lines**, **4 are genuinely emitted** (22 rule lines — producer plus value domain
   read from the call sites), **1 is emitted-from-a-domain-that-excludes-it**
   (`.action-center-status-critical`, 6 lines: `STATE_META` only ever yields `ok`/`neutral`/`warning`), and
   **15 have no producer at all** (195 lines — 14 with zero literal occurrences outside `styles/`, and
   `.phase-hint` with only the two `phaseHint` test case ids). So the queue grows
   rather than shrinks: **18 selectors / 236 rule lines** are now defensible deletions once `.btn-close` and
   `.job-sector` are added, and "needs a browser check to prove dead" was the wrong bar — `git grep -F <name>
   -- ':!styles'` returning zero files plus no `parent-${…}` producer is the bar that mattered. The named
   render sites did hold two live producers (`ops-summary-dedup.js:517`, `inspector.js:149`) and one dead one
   (`action-center.js:131`); `frontend/jobs/**` holds none — the only `jobs-pipeline-{…}` producer in the repo
   is a Python thread name. The "9 unproven selectors (≈93 rule lines)" figure this question previously
   carried is still not reproducible from any artifact.
4. **~~Q1 ceiling ownership~~ — answered 2026-09-29: three owners, no overlap, `suite_contract_policy` is
   not one of them.** `tools/repo_health/loc_budget.py` (240 lines) owns **area totals** in
   `loc_baseline.json`. `check_line_budget` (the `line-budget` group) owns **per-file test caps** in
   `test_line_budget_baseline.json` — 55 entries, `default_budget` 400, `integration_budget` 800 behind 11
   `integration_globs`, 2 `excluded_globs` — and is **max-only** (`if line_count > allowed` at
   `repo_guardrails.py:377`), which is why Q1's 24 stale caps are invisible to it.
   `check_deferred_source_line_budget` (`:390`, reading `deferred_source_line_budget.json`, path at `:20`) is a
   **third** owner whose file is currently empty (`files: []`). `suite_contract_policy.py` (1,622 lines) does
   mention 28 test *file names* but constrains **suite structure, not length**: it references neither baseline
   JSON and contains no `max_lines`/`ceiling` logic. Tightening Q1 caps therefore cannot collide with
   `compat`; the only re-baseline the queue needs is `loc_budget.py --update`.
5. **Where does `test-reduction-triage.md` belong?** It is linked from `docs/INDEX.md`,
   `docs/testing.md`, and `docs/plans/remaining-work-implementation-plan.md`, and it remains the live
   rulebook for test deletion — so it stays in `docs/plans/`, marked queue-complete. Archiving it would
   break those three links and strand the safety rules; if it is ever moved, update all three.

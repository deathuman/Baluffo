# Measurement Methods

> - **Status:** Active
> - **Use this when:** someone brings you a LOC-reduction, dead-code, duplication, or "merge these files" claim, or you are about to measure one yourself
> - **Canonical for:** how structural claims about this repo get verified — clone scanning, CSS liveness, staging-before-measuring, and what counts as proof
> - **Not canonical for:** what the current numbers are, test command routing (see [`testing.md`](testing.md)), or line budgets (see [`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md))
> - **Then inspect:** [`plans/test-reduction-triage.md`](plans/test-reduction-triage.md) for which tests may be deleted or merged
> - **Last updated:** 2026-10-01

Extracted from the retired structure/test audit programme. The programmes are gone; the
**methods** are not, because they were reused across every wave and every one of them
caught a wrong number at least once. Git history holds the per-item measurements.

## The rule everything else follows from

> **Never quote a number from a command whose exit status you did not check.**

In a batched run, one failed step makes **every** statement sourced from that batch
unverified — not just the failed step's own output. This is not hypothetical: a
`git ls-files styles` step ran in a batch *behind* an already-failed command, its output
was never seen, and the result was inferred into a table of fabrications. A later
revision then "corrected" those fabrications into a *second* set of fabrications. Five
cites in that programme pointed at files that did not exist.

Corollary: **a handoff that hides its own corrections is no better than the unsourced
report it refutes.** Corrections get recorded, not quietly rewritten.

## Clone scan

Function-level scan over every `.py`: take bodies with **≥8 non-blank statements**,
replace every `Name`/`arg` with a shared placeholder, every constant with `K`, drop
docstrings, hash `ast.dump`, group equal hashes. Report lines beyond the first copy per
group, split **cross-file** vs **same-file**.

**Byte-parity is necessary, not sufficient.** The scan measures *structure*, and the
gap between the two decides whether work happens:

| Cluster | Size | Outcome |
|---|---|---|
`run_static_plugin` | 3 sites, −46 | **extracted** — 2 byte-identical, 1 differing only by a redundant argument |
`set_source_diagnostics` | 17 × 2 | **declined** — method on a per-test fake class, so sharing means a mixin and restructures both files: ≈ −15 lines for a structural change |
`delayed_fetch_with_retries` | 13 × 2 | **declined** — closure over two variables; hoisting makes every call site noisier than the 13 lines saved |
`check` / `scraper_loader` / `_active_job` | 40 / 25 / 14 | **declined** — same shape, **different literal values** |
`ok_loader` | 107 | **declined** — the largest cluster, and shape-similarity only |

Measure **each cluster byte-for-byte before extracting**, and **record declined clusters
with their reasons**. A rejected extraction that gets written down stops the next pass
re-proposing it.

**Stale-path discipline.** Cluster tables drift: that one produced five wrong paths
across revisions (`fake_fetch_directory_pages` was 5 sites/4 shapes, not 2; `def
_run_plugin` was 5 sites, not 2). Re-derive sites from the tree, never from the table.

**Mutation-verify new helpers.** After extracting a shared helper, make it `return []`
and confirm failures appear. 10 failures proved the suites were not passing vacuously
through it.

## CSS liveness — two stages, and stage one lies

Dead-selector analysis needs both stages. Running only stage one is how a literal-grep
method returned **23 flagged, 21 of them false positives (91%)**.

**Stage one — prefix filter.** Parse class selectors out of the **comment-stripped**
tracked stylesheets. Build **one** token set over the tracked consumer corpus (a regex
per selector is too slow — the naive version timed out at 30 s). Treat a selector as
live if its full name appears **or** any dash-boundary prefix appears inside a string
literal.

**Stage two — producer read.** A prefix hit is not a producer, and a producer whose
value domain cannot yield the selector's tail is not either. For each survivor, search
for `parent-${`, `parent-{`, `parent-%s`, `parent-" +`, `parent-…}-${tail}` sites, open
them, and check the interpolated value's domain. Cross-check survivors with
`git grep -F <name> -- ':!styles'`: zero files outside `styles/` **plus** no producer
means dead.

Stage one is what makes the list *usable*; it is also what makes it wrong in the
"still alive" direction. Stage two moved the answer from "2 dead" to **18 unreachable
selectors / 236 rule lines**.

### Counting conventions — say which you used

| Convention | Result | Why it differs |
|---|---|---|
`count("\n") + 1` | 718 | counts **raw** text, so a class inside `/* … */` becomes a selector |
`splitlines()` on comment-stripped | **712** | the `loc_budget` method — **quote this one**; both parsers then agree with zero set difference |
rule-head regex | 1,312 | counts once per `… {` head: `a, b, c {` counts once, compound classes once |
comma-split head tokens | 2,188 | each class token counted |

All are correct under their own convention. An unnamed number is an unusable number.

**Custom properties are a repo-wide union.** Per-file `defined − referenced` reported a
bogus **82 unused** in `base.css`; the repo-wide truth is **2**.

**Browser-verify before deleting CSS.** Removing 18 dead selectors touched
`saved.html` byte-identically, `jobs.html` at 0 px drift, and `admin.html` with a 74 px
diff that turned out to be the fetcher's own clock (`00:13:35` → `00:18:16`). Static
proof does not replace a rendered check.

## Staging: measure the staged tree

The working tree is not what gets committed. Two waves reported wildly wrong signs
because of this:

| Wave | Reported | Actual |
|---|---|---|
W5 | −872 | **−444** |
W6 | −5,015 | **+1,342** |

Stage first, then measure. Any number quoted before staging is provisional.

## What the audit programme actually found

Kept because a specific external claim will otherwise be re-litigated: a report
asserting **15,000 LOC removable** measured at **~54 lines** — about **0.01%** of the
tree. Across a 44,096-line JS unit corpus, exactly **2** pairs exceeded 0.60 similarity
and both share one middle file; a 9,546-line Python cluster showed **zero** pairs above
0.55. A strict AST scan of all Python found 38 groups / 676 redundant lines. Family
merging — the report's largest structural claim — was a measured **non-goal**: merging
all nine families while deduplicating every docstring and import yields 1,026 lines
(3.2%) and reverses a decision already on the record.

Four- and five-figure targets are unsupported by anything in this tree.

## Related

- [`test-reduction-triage.md`](plans/test-reduction-triage.md) — which tests may be
  deleted or merged, and the retained-test boundaries that gate it
- [`testing.md`](testing.md) — test command routing and fixture layout
- A durable check belongs in `tools/repo_health/` with a test. Scratch under `_out/` is
  gitignored, and a finding that lives only there is not a guardrail.

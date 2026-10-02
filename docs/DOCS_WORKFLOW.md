# Documentation Workflow

> - **Status:** Active
> - **Use this when:** deciding which doc owns a topic, updating docs after a code change, or adding a new documentation page
> - **Canonical for:** documentation discovery, ownership, freshness checks, and maintenance workflow
> - **Not canonical for:** subsystem behavior, data contracts, release notes, or runtime behavior outside this guide's process scope
> - **Then inspect:** [`INDEX.md`](INDEX.md), then the smallest authoritative doc set for the task
> - **Last updated:** 2026-05-17

## Core Rules

- Baluffo is docs-first, not docs-only.
- Start with the smallest authoritative doc set, then read code when you need executable detail, verification, or the docs do not own the question.
- Canonical docs are authoritative only for the surface they declare.
- Prefer extending an existing authoritative doc over adding a new page.
- Use markdown links for checked-in repo targets and inline code for generated or usually-absent artifact paths such as `_out/`.
- Keep doc updates in the same change as the code or workflow change that made them necessary.

## Rule Placement

- Root [`AGENTS.md`](../AGENTS.md) owns always-loaded repo guardrails.
- [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) owns AI-coder workflow and editing behavior.
- [`tools/mcp/`](../tools/mcp/) owns MCP setup and tool-specific usage boundaries.
- Contract docs own stable payload/API rules.
- Subsystem docs own subsystem-specific workflow.

## Discovery and Update Loop

1. Open [`INDEX.md`](INDEX.md).
2. Pick the smallest authoritative doc set that matches the task.
3. Read code only for executable detail, clarification, or revalidation.
4. Update the owning doc when commands, routing, contracts, or workflow expectations move.
5. Re-check [`INDEX.md`](INDEX.md) so the document stays discoverable.

## Active Doc Header Standard

Active docs should start with a compact metadata block using this order:

1. `Status`
2. `Use this when`
3. `Canonical for`
4. `Not canonical for`
5. `Then inspect`
6. `Last updated`

Archive docs should stay rare and short. Use [`archive/README.md`](archive/README.md) for retired cleanup/refactor context and git history for detailed provenance instead of reintroducing long historical logs.

Active refactor plans, planning templates, and follow-up trackers belong in [`plans/`](plans/). Dated failure snapshots, evidence notes, and validation snapshots belong in [`snapshots/`](snapshots/). Retired records remain in [`archive/`](archive/).

## Gap Handling

- Extend an existing authoritative doc first when the topic already has a clear owner.
- Create a new doc only when no current page clearly owns the topic.
- When a new doc is necessary, give it one clear authority label, add it to [`INDEX.md`](INDEX.md), and cross-link it from the nearest related source-of-truth docs.
- Do not add overlapping overview pages when narrower canonical docs already cover the topic.
- Do not create a second documentation tree, append-only doc logs, Obsidian-style wiki links, or agent-specific root rule files for this workflow.

## Plan Lifecycle

A plan is a **temporary ledger**, not a documentation page. Its job is to refine the work
before execution, track progress while it runs, and then get out of the way:

1. **Author and refine.** Record the queue, the per-item gate, and the reproduction step.
   Give every open-items row a status or a code citation from the start — see
   [Reading a plan](#reading-a-plan-verify-before-you-trust-it).
2. **Execute and track.** Update as work lands. Re-measure rather than trusting an earlier
   figure — a status line that says "not started" while the body says otherwise costs a
   reader more than the work did.
3. **Close it.** When the work is done, **delete the plan** and move anything worth
   keeping into the regular docs. Do not leave it parked in `plans/`, and do not archive a
   long record — see the archive rule above; git history is the provenance.

A status of *Folded*, *Parked*, *Closed*, *Superseded*, *Implemented*, or *Fully executed*
means the plan is finished. If it still sits in `plans/`, that is the defect, not the state.

`test_plan_lifecycle_tripwires` in [`../tools/repo_health/release_docs_policy.py`](../tools/repo_health/release_docs_policy.py)
enforces this. It flags oversized plans, terminal-status plans still in `plans/`, and plans past
5,000 words.

**Deferral is not terminal.** A plan may legitimately wait, and the gate keeps it as long as its
status names the condition that unblocks it — "deferred until the next desktop portable release"
passes. A plan marked *Parked* or *Deferred* with **no stated trigger** fails, because an
unexplained reason to wait is indistinguishable from an abandoned one. That is the pressure the
rule applies: make the reason checkable, or delete the plan.

The word threshold exists because line counts miss the worst shape. One retired plan held 21,816
words across 65 lines, with 172,891 characters on a single line. A line-based rule passed it
cleanly.

## Reading a plan: verify before you trust it

**A plan's status is a claim about the world, not a description of the file. Verify it.**

Eleven plans were retired in 2026-10-01, and every one asserted work was outstanding that had
already landed: `hold-tail-repair`, the structure audit, `remaining-work`, `art-title-repair`,
`registry-hygiene`, `task-abort-control`, and others. In each case a reader who trusted the table
would have skipped the body and gotten the wrong answer. One plan's loophole table listed 17 open
items; all 17 were already implemented.

So before acting on any plan's open items:

1. **Read the body, not the status line.** A stale status is the normal failure here, not the rare one.
2. **Verify each open item against the code**, the same way you would verify any other claim. Search
   for the implementing symbol; check whether the required behaviour already exists.
3. **Treat a section heading as a claim too.** A heading reading "Awaiting human disposition" sat over
   five items that were all converged.
4. **Write down what you verified, and where.** See below.

**Never cite a plan as the only record of a code fact.** `DB_VERSION` and `BACKUP_SCHEMA_VERSION`
appeared in exactly one file repo-wide — a plan — and deleting it would have silently lost the
version-bump discipline. That rule now lives in [`DATA_CONTRACT.md`](DATA_CONTRACT.md) §2.5. Before
retiring a plan, confirm anything load-bearing it carries has a second home, usually the canonical
doc that owns the topic.

### The declaration block

Every plan opens with three lines that classify it. This is a **convention, not a gate** —
nothing checks it, and that is deliberate; see *Why this is a convention and not a gate* below.

```markdown
> - **Class:** shipped-defect | enhance-stable | cleanup
> - **Trigger:** <the condition that makes this actionable>
> - **Verified against:** <short sha>
```

- **`Class`** — which side of the line this sits on. A `shipped-defect` plan fixes something
  affecting a release users are running today. An `enhance-stable` plan proposes a new
  capability for software that already works. A `cleanup` plan reduces structure with no
  behaviour change. The class determines what "done" means, and it is the first thing to
  disagree with when a plan goes stale.
- **`Trigger`** — what makes this actionable now. A release, a condition, an operator
  decision. If you cannot name one, the plan is not deferred, it is abandoned.
- **`Verified against`** — the commit you last checked this plan against the tree.

**Why the three sit together.** The failure this repo hit repeatedly was a summary trusted
over a body: a status line said work was outstanding while the body recorded it landing.
`Class` next to `Status` puts the two claims adjacent, so `class: shipped-defect` beside
`status: active follow-up` is a contradiction you can see without reading further.

**Verify, then write the sha.** `Verified against` is a claim like any other. Re-read the
plan against the current code, confirm each open item still holds, *then* record the commit.
Writing today's sha onto a plan nobody checked makes the field worse than absent — it
launders an unverified claim into a timestamped one.

**Plans with no baseline in the current code are normal.** A plan for a genuinely new feature
has no code to verify against, and that is not a defect. Such a plan is `enhance-stable` and
its `Trigger` carries the weight. Do not retrofit one into a code-citation shape it does not
have.

### The citation convention

An open-items table with no per-row status is unfalsifiable on sight, and that is what makes a stale
one expensive — you cannot tell a landed row from an outstanding one without reading the code.

**Every row of an open-items table should carry either a citation to the code that implements it, or
an explicit status.** Cite the implementing location *and* the test that pins it:

```markdown
| Loophole | Where it is handled | Pinned by |
|---|---|---|
PID-only kill after PID reuse | `task_process_registry.py:194` refuses an unregistered PID | `test_pipeline_service_control_files.py` |
```

Applied to `task-abort-control`, this converted a 17-row wishlist into a verified "0 open" in one
pass — every claim falsifiable, so nothing had to be taken on trust.

### Why this is a convention and not a gate

A staleness detector was designed and measured against the eleven retired plans, and **none of the
three candidate mechanisms survived**:

- **Citation ratio** — no discrimination. `hold-tail-repair` scored 1.47 citations per table row and
  `jobs-coverage-improvement` scored 8.00; both were stale. The worst offender had the *highest*
  citation density in the repo.
- **Status-vs-body contradiction** — 20% recall, catching 1 of 5, and only on a weak token.
- **Status-blind table, scoped by heading** — fired on 16 tables, all false positives: benchmark
  tables, "What landed" records, and per-step progress tables. Section titles do not use the words
  the heuristic guessed.

Staleness is **semantic**. It is a claim about code, so catching it requires reading the code, which
is the manual step the convention makes explicit. A structural gate can only see shape, and shape
does not correlate. A check with 20% recall that also fires on benchmarks would be either
permanently red or permanently silent — both of which teach people to ignore it.

### What the declaration block deliberately does not check

The three questions a plan raises are classification, staleness, and value.

- **Classification** — answered by the `Class` line. A gate cannot infer it, but it can force the
  author to state it, which is the part that matters.
- **Staleness** — the mechanical version was measured and **fails**: a plan cites a path, that path
  changed since `Last updated`, flag the plan. Against the five remaining plans it fires on exactly
  one, and it is wrong — `optional-playwright-browser-download` cites `scripts/build_portable_exe.py`,
  which changed once, by a commit consolidating SHA-256 helpers. Path overlap cannot express
  "relevant", so it misfires on first contact.
- **Value** — a human judgment. No gate should attempt it. The block's only job is to make the
  judgment cheap and well-posed by requiring it in writing.

What *did* work for staleness was narrower and unambiguous: a **past date** in a trigger is a
lapsed deferral, full stop. No path analysis, no judgment, no false positives. That rule is enforced
above, and it is the shape worth extending if anything is extended later.


Durable results belong in the canonical doc that owns the topic — measurement method in
[`measurement-methods.md`](measurement-methods.md), pipeline behaviour in
[`scraping-pipeline.md`](scraping-pipeline.md), data shapes in
[`DATA_CONTRACT.md`](DATA_CONTRACT.md), dated evidence in [`snapshots/`](snapshots/). A
promised follow-up is a plan; a landed result is a doc.

## Freshness Check After Code Changes

Review the touched area and update docs in the same change when any of these moved:

- Routing or edit boundaries in [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) or [`architecture-ai-map.md`](architecture-ai-map.md)
- Commands or verification guidance in [`testing.md`](testing.md), [`LOCAL_SETUP.md`](LOCAL_SETUP.md), [`CONTRIBUTING.md`](../CONTRIBUTING.md), or [`RELEASE.md`](RELEASE.md)
- Data, API, or runtime contracts in the owning canonical contract doc plus the matching schemas or tests
- Historical or planning labels when a doc should be marked active, operational, historical, or refactor-record instead of leaving that status implicit
- `Last updated` markers on active docs you touched
- Archive links in [`INDEX.md`](INDEX.md) when the archive index changes
- Both the link target and any visible path text when an archive move changes how active docs or the changelog refer to archive material
- Basic Memory continuity notes when routing, ownership, recurring gotchas, current focus, handoff context, durable decisions, or stale-memory corrections change; repo docs remain canonical over memory

## Logging and History

- Use git history and PR context for routine doc maintenance history.
- Keep [`CHANGELOG.md`](CHANGELOG.md) reserved for product and release history.
- Do not add a separate append-only documentation log for normal maintenance updates.

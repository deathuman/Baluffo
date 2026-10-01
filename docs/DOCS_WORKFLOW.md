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
2. **Execute and track.** Update as work lands. Re-measure rather than trusting an earlier
   figure — a status line that says "not started" while the body says otherwise costs a
   reader more than the work did.
3. **Close it.** When the work is done, **delete the plan** and move anything worth
   keeping into the regular docs. Do not leave it parked in `plans/`, and do not archive a
   long record — see the archive rule above; git history is the provenance.

A status of *Folded*, *Parked*, *Closed*, *Superseded*, *Implemented*, or *Fully executed*
means the plan is finished. If it still sits in `plans/`, that is the defect, not the state.

`test_plan_lifecycle_tripwires` in [`../tools/repo_health/release_docs_policy.py`](../tools/repo_health/release_docs_policy.py)
enforces this: it flags oversized plans, terminal-status plans still in `plans/`, and plans
past 5,000 words. It is **warn-only** while the existing backlog drains — set
`PLAN_LIFECYCLE_ENFORCE = True` once the list is empty or the remaining plans are accepted.

The word threshold exists because line counts miss the worst shape. One retired plan held
21,816 words across 65 lines, with 172,891 characters on a single line. A line-based rule
passed it cleanly.

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

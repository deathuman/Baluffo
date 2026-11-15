# Admin Storage UI Simplification Plan

> - **Class:** cleanup
> - **Trigger:** the storage workflow is settled — rebuild, verified delete, and a
>   specified automatic path ([`automatic-storage-reclaim-plan.md`](automatic-storage-reclaim-plan.md))
>   — while the panel still opens expanded and presents nine manual controls at once
> - **Verified against:** `63592800` (markup, controller, renderers, routes, and the
>   store's own maintenance scheduling read from the tree; no UI change has landed)
> - **Status:** Planned — not started. Four slices, UI and its tests only.

## Scope

Make the Runtime Database Storage panel match the workflow that now exists: closed
until asked for, one primary action, and every state-mutating control behind one
Advanced disclosure that states its consequence before the click.

In scope: `admin.html`, `frontend/admin/app/storage-breakdown.js`,
`frontend/admin/render/storage-{tables,diagnostic}.js`, `styles/admin.css`, and the
frontend unit tests that pin their markup and behaviour.

Not in scope: route names, request shapes, controller polling budgets, rebuild or
delete semantics, the automatic reclaim itself, CSS redesign beyond the new
disclosure, and any change to what a check measures. No backend file is touched by
any slice here.

## What ships today (verified)

| Control (`admin.html`) | Line | What it does |
|---|---:|---|
| Panel `<details … open>` | 91 | Expanded on every Admin load |
| Refresh | 103 | Re-reads `/ops/storage-health` |
| Quick check | 104 | `POST /ops/storage-diagnostic {mode:"quick"}` |
| Preview cleanup | 105 | `GET /ops/storage-maintenance/preview` (read-only) |
| Run maintenance / Stop | 106, 107 | One runtime-retention + compaction pass now; stop applies to feed cleanup only |
| Rebuild current feed / Stop rebuild | 108, 109 | `POST /ops/storage-rebuild`, cancel |
| Roll back | 110 | Consumes the retained copy |
| Delete old database | 111 | Byte-guarded, validated background delete |
| Full storage audit / Cancel / Export | 115–117 | Exact `dbstat` allocation |

Two supporting facts decide the first two evaluations:

- **The maintenance pass is already scheduled by the store.** `Run maintenance`
  calls `start_storage_maintenance`, documented as "the same post-serve maintenance
  path, exposed for an explicit Admin action"
  (`src/bridge/storage_health.py:336`). The store runs that path once per instance
  after the bridge starts serving (`resume_maintenance_after_serving`,
  `src/storage/baluffo_store.py:1158`) and schedules runtime retention from a due
  timer (`schedule_runtime_retention`,
  `src/storage/baluffo_store.py:1406`; timer at `:1622`). The button's unique value
  is "now", not "at all".
- **The panel already renders a table-allocation snapshot.** The tables body
  renders `storage-health` `tableSizes` on every load
  (`frontend/admin/render/storage-tables.js:421`). Quick check's own cards are a
  feed card (`storage-diagnostic.js:123`) and a sampled current-versus-older
  estimate (`:186`); the first overlaps the tables body, the second does not.

## 1. Panel closed by default

**Change.** Remove `open` from `admin.html:91`. Do not persist expansion: the
requirement is the same starting state on every Admin visit, and per-user storage of
a disclosure is a new surface nothing else in this panel has.

**Keep the summary worth reading while collapsed.** The `<summary>` already carries
the status span (`admin-storage-tables-status`), so a closed panel still answers
"is storage healthy" in one line. Feed it from two places that already exist: the
Action Center's storage payload (`onStorageHealth`) and, once opened, the panel's
own read.

**Stop paying for a hidden panel at startup.** Today the startup lane always loads
storage (`frontend/admin/app/runtime/composition.js:630`, wired from
`boot-wiring.js:78`). Make the first read lazy: `load()` on first open, and accept
the Action Center payload for the summary until then. The status span's initial
text changes from "Waiting for startup" to "Open to load" so a closed, unread panel
is not described as pending.

**Acceptance.** A fresh Admin load leaves the panel collapsed; no
`/ops/storage-health` request is made until the panel is opened or the Action Center
payload arrives; opening it once loads normally.

## 2. Maintenance: keep the capability, demote the control

**Verdict: keep, do not delete; stop exposing it as a primary control.**

Reasons, in the order that decided it:

1. It is not a separate operation. The pass it starts is the store's own post-serve
   maintenance path (cited above), so removing the button removes "run it now", not
   a capability.
2. It is the only stop path for a running pass. `POST /ops/storage-maintenance/cancel`
   is the shipped Stop, and the reclaim plan's desktop presentation relies on the
   panel's Stop control to cancel by `runId`
   ([`automatic-storage-reclaim-plan.md`](automatic-storage-reclaim-plan.md) §4:
   "the shipped **Stop** control cancels by `runId`"). Deleting it would strand a
   running compaction with no operator abort short of a bridge restart.
3. A due compaction is not otherwise reachable on demand. Retention is timer-driven;
   compaction is due-driven. An operator who has just watched a large cleanup needs
   the same pass to run before the next page load, not on the next timer tick.

**Change.** Move Preview cleanup, Run maintenance, and Stop into the Advanced
disclosure (§4). Keep the progress line (`admin-storage-maintenance-progress`) and
the write-lock line in the panel body so a running or held pass is visible without
opening Advanced; show `Running`, `Held by a task`, or `Last pass …` in the
collapsed summary status line.

**Acceptance.** With Advanced collapsed, a running pass shows its state in the panel
body and the summary; Preview/Start/Stop are reachable only after opening Advanced;
no route, payload field, or polling constant changes.

## 3. Quick check: keep it, trim its report

**Verdict: keep the check, keep it primary, remove the duplicated card.** No
"Quick scan" string exists in the tree; the shipped label is **Quick check**
(`admin.html:104`), and this plan keeps that name.

Why it survives:

- It is the only view of the split between the current feed's stored rows and older
  saved versions, which is exactly the quantity the automatic reclaim will act on
  (`reclaimable` in
  [`automatic-storage-reclaim-plan.md`](automatic-storage-reclaim-plan.md) §1). The
  health table body shows allocation per table, not that split.
- It is seconds on a live database; the full audit is the hours-long one.

Why its reporting changes:

- The **Current feed** card (`storage-diagnostic.js:123`) restates feed counters
  (jobs, saved source rows, published-at) that the tables body and the collapsed
  summary already carry. Drop it from the quick report so each fact has one home;
  keep it for the full audit, where it is the context for exact allocation.
- Keep the sampled space card, including its existing "Partial estimate" warning and
  the current-versus-older bar, because that is the decision input.
- State the check's cost and freshness once, next to the button's result line,
  instead of in three places (advanced note, section copy, empty state).

**Acceptance.** A quick-check result renders the space estimate and the integrity
facts, not a second feed card; the full audit still renders its feed card; no
measured number disappears from the audit path.

## 4. Manual actions behind one Advanced disclosure

**Change.** One `<details class="admin-storage-advanced">` inside the panel body
holds every control that mutates or measures the database:

- maintenance: Preview cleanup, Run maintenance, Stop
- rebuild: Rebuild current feed, Stop rebuild, Roll back
- delete: Delete old database
- audit: Full storage audit, Cancel, Export JSON

Primary row keeps **Refresh** and **Quick check** only. Existing ids, `data-ui`
tokens, and hidden-state logic stay as they are, so
`frontend/shared/ui/tokens/admin.js` and both controller tests keep working; only
their DOM position moves.

**Warning tooltips, not just descriptions.** Today the destructive controls carry
`data-tooltip` prose written as documentation. Rework them to state what the click
costs, and mark the two irreversible ones as warnings:

| Control | Tooltip must say |
|---|---|
| Rebuild current feed | Copies the whole database first; minutes on desktop, tens of minutes on a container; fetch starts are refused while it holds |
| Roll back | Restores the retained copy and consumes it; the rebuilt database is replaced |
| Delete old database | Irreversible; after it the rebuilt database has no rollback copy; only possible when the live feed still validates |
| Run maintenance | Runs the pass the bridge would run on its own; compaction cannot be interrupted, feed cleanup stops after the current step |

Also required:

- Render the tooltip on keyboard focus as well as hover, so a control reached by Tab
  is not silent; warn-toned tooltips get a distinct class and `aria-describedby`.
- Keep the existing confirm dialog for the delete. Tooltip and dialog are not
  substitutes: the tooltip states the consequence, the dialog carries the byte count.
- Add one line to the panel body, visible without opening Advanced, when a retained
  copy exists ("Retained copy 50 MB — rollback available under Advanced"). Burying
  the fact that a rollback exists is the failure mode this move creates.

**Acceptance.** With Advanced collapsed, no mutating control is clickable; opening it
reveals all of them in the same groups as the table above; a retained copy is
announced in the body; tooltips render on focus and on hover.

## 5. Slices

| # | Slice | Files | Pinned by |
|---|---|---|---|
| S1 | Closed by default, lazy first-open load, "Open to load" status | `admin.html`, `runtime/composition.js`, `boot-wiring.js`, `storage-breakdown.js` | `tests/frontend/unit/admin-storage-tables-render.test.mjs`, controller test |
| S2 | Advanced disclosure for all mutating controls; warn tooltips; retained-copy line | `admin.html`, `styles/admin.css`, `storage-breakdown.js`, `storage-tables.js` | same render test + a new focus-tooltip assertion |
| S3 | Quick check report: drop the feed card, keep the space estimate | `render/storage-diagnostic.js` | storage-diagnostic render test |
| S4 | Wording pass: status line texts, one home per cost/freshness sentence; update `docs/INDEX.md` row when the plan closes | `storage-breakdown.js`, `render/*`, `docs/INDEX.md` | docs guardrails |

Each slice is independent and reversible; S1 is worth shipping alone if the rest
waits.

## Non-goals

- No new route, no new payload field, no polling change.
- No change to what the automatic reclaim will do, and no attempt to build it here.
- No panel-open persistence, no per-user preference.
- No restyling of cards, tables, or bars; only the disclosure, its warning tone, and
  the lines named above.

## Risks

| Risk | Mitigation |
|---|---|
| Hiding Rebuild/Delete costs discoverability in an incident | Advanced label, block-level warning tooltips, and the retained-copy line in the body |
| Lazy load leaves the collapsed summary blank on a slow bridge | Summary accepts the Action Center payload; status text says "Open to load", not an error |
| Moving markup breaks the controller's ref lookup | Ids are unchanged; both controller tests already resolve by token and run in the same slice |
| "Maintenance" removal is later requested again | §2 records why it is kept, so a future reader does not re-litigate it from memory |
| Quick check's card removal loses a fact | The feed counters remain in the tables body and audit; only the duplicate render is removed |

## Open questions

1. Once automatic reclaim ships, does **Delete old database** stay in Advanced as the
   free-space fallback, or does the reclaim's own copy row replace it? The reclaim
   plan leaves step 5 manual (§3 there), so the default is: it stays, in Advanced,
   with the same byte guard.
2. Should the collapsed summary show the last maintenance pass time at all, or only
   when a pass is running or held? Starting answer: only when running or held, to
   keep one line from becoming a status dashboard.

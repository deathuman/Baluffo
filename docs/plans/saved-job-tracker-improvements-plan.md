# Saved Jobs Deferred Backlog

> - **Status:** Decided 2026-10-02 — items 1 and 4 declined, item 2 deferred, item 3 **shipped** in 0.3.006
> - **Class:** enhance-stable
> - **Trigger:** item 2 only, and only if saved-page lists get large enough that a flat list plus Stage grouping stops being enough. Items 1 and 4 are decided and closed.
> - **Verified against:** 0.3.006
> - **Use this when:** deciding whether a deferred Saved Jobs decision has become active again
> - **Canonical for:** the four deferred items, their restart criteria, and their stated prohibitions
> - **Not canonical for:** saved-job row shape, backup payload shape, version-bump discipline, bridge route contracts, or current UI behavior. Those live in [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) §2 and are canonical over this file.
> - **Then inspect:** [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../../frontend/local-data/tracking.js`](../../frontend/local-data/tracking.js), [`../../src/local_data_store_tracking.py`](../../src/local_data_store_tracking.py), [`../../frontend/saved/app/view-model.js`](../../frontend/saved/app/view-model.js), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-10-02 (reduced to open work; v1 summary and guardrails moved to the canonical docs; declaration block added; Stage grouping re-verified as the only grouping; all four items re-verified against code and decided)

## Position

Saved Job Tracker v1 shipped and is complete. This file is **only** the deferred backlog — four
items, each with explicit restart criteria. None is in implementation, and none should be started
because it appears here.

The split tracking model is canonical and lives elsewhere:

```text
pipelinePhase: bookmark | applied | screening | assignment | interview_1 | interview_2 | final | offer
outcomeStatus: active | rejected | withdrawn | ghosted | closed | accepted
```

`applicationStatus` is a derived compatibility mirror. New code reads `pipelinePhase` and
`outcomeStatus`. Persisted row shape, backup payload, and **version-bump discipline** are in
[`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) §2 — including the `DB_VERSION` / `BACKUP_SCHEMA_VERSION`
rules, browser/desktop parity pairing, and the activity and attachment semantics that used to be
duplicated here.

## 1. V2 CRM-style tracking object

The only deferred item with clear product-expansion value. It is a **v2 design pass**, not a v1
cleanup task.

Possible fields:

```json
{
  "tracking": {
    "priority": "high",
    "nextAction": "Send follow-up",
    "nextActionAt": "...",
    "contactName": "...",
    "contactEmail": "...",
    "salaryRange": "...",
    "referral": "..."
  }
}
```

Restart only when Saved Jobs is intentionally becoming a lightweight application tracker or CRM,
the UI has a concrete workflow for next actions / contacts / salary notes / priority, and the
storage, backup, import/export, desktop parity, activity, and filtering implications are designed
together.

**Do not add a flexible `tracking` bag opportunistically.** It changes the product surface and
becomes an untyped dumping ground — which is exactly why `applicationStatus` exists as a derived
mirror rather than a bag.

## 2. Additional grouping modes

`Stage` grouping is implemented and default-off. More modes are not currently justified.

Candidates, to revisit only if usage shows list-management pain: **company**, **reminder week**,
**source status**.

Restart only when saved-page lists are large enough that a flat list plus Stage grouping is not
enough, the mode has a specific workflow benefit rather than being another way to slice the same
data, and it can consume `buildSavedJobViewModel()` and `groupSavedJobViews()` without rederiving
tracking or lifecycle rules.

## 3. Attachment tab polish

Attachment lazy-loading is hardened. Future work stays minor unless users hit real friction:
an explicit manual refresh action, and a clearer first-load state for an unloaded attachment tab.

**Do not change attachment storage, backup shape, or `attachmentsCount` semantics for this polish.**

## 4. Soft delete

V1 policy is hard remove plus immediate Undo, with attachments preserved separately unless
explicitly deleted. Soft delete must not be added casually.

Restart only when there is a real need for saved-job trash, later restore, cleanup, or audit
retention, and the design covers saved keys, counts, filters, subscriptions, backup/export/import,
activity, attachment listing, and hard cleanup.

**Do not add `deletedAt` as a narrow field-only change.** It changes storage semantics and
compatibility expectations, so it belongs with the full design above.

## Decisions, 2026-10-02

All four items were re-verified against the code before being decided, not carried forward on trust.

| Item | Decision | Basis |
|---|---|---|
1. V2 CRM object | **Declined** | It turns a job finder into an applicant tracker, and the storage, backup, import/export, desktop-parity, activity and filtering consequences are permanent. Nothing in the product asks for it. |
2. More grouping modes | **Deferred** | `buildSavedJobViewModel()` and `groupSavedJobViews()` are intact, so this is the cheapest to build — but there is no list large enough to need it, and a third way to slice the same data is the failure mode the restart criteria warn about. |
3. Attachment tab polish | **Shipped** in 0.3.006 | Manual refresh plus a real first-load and error state. Purely presentational. |
4. Soft delete | **Declined** | No trash, restore, cleanup or audit need. It would change storage semantics, counts, filters, subscriptions, backup/import, activity and attachment listing for a feature nobody has asked for. |

### What item 3 actually changed, and one bug found on the way

- Added a **Refresh** button beside Upload, mirroring the History tab's existing one. It calls the
  existing `hydrateAttachmentListForJob`, which needed `{ force: true }`: without it the function
  returns early for an already-loaded key, so the button would have looked broken the moment a
  list had been hydrated once.
- The panel said **"No attachments yet."** before anything had loaded. That is a claim the app had
  not made yet, so the first-paint state now says the list has not been loaded.
- **Both fetch failure paths rendered an empty list.** A thrown error *and* an `ok: false` response
  both produced `renderAttachmentList(jobKey, [])`, so a bridge that was offline looked exactly like
  a job with no attachments. There is now a distinct error state that also clears the loaded marker,
  which is what makes Refresh a real retry.

Attachment storage, backup shape, and `attachmentsCount` semantics are untouched, per the item's own
prohibition.

### A trap worth recording: `deletedAt` already exists here

`deletedAt` **is** present in this repo — `src/bridge/registry_tombstones.py:22,124`. It belongs to
the **source-registry tombstone** system, a different feature that happens to share the field name.
Zero files under `frontend/saved/`, `frontend/local-data/`, or `src/local_data_store_tracking.py`
reference it. Anyone grepping `deletedAt` to size up saved-job soft delete will find registry code
and could wrongly conclude item 4 is half-built. It is not started.

## Not backlog

Remove/restore confirmation and hard-remove policy, attachment lazy-load duplicate-read prevention,
Stage grouping, phase/outcome revert audit details, lifecycle badge `lastSeenAt` copy, action
clarity and sticky header polish, and the Applied milestone / tooltip / backward-override cleanup
are **shipped**. If one regresses, open a bug against the owning source and tests — do not revive
this file as an implementation checklist.

# Saved Job Tracker Improvement Plan

> - **Status:** Active next-step tracker
> - **Use this when:** improving saved-job persistence semantics, application tracking, filtering, activity logging, or source-lifecycle visibility in the Saved page
> - **Canonical for:** saved-job data model refinements, phase/outcome ergonomics, activity semantics, Saved page operations UX, and milestone sequencing toward Saved Jobs Tracker v1
> - **Not canonical for:** backend job discovery/fetch contracts, local data storage internals, or deployment/packaging behavior
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../../frontend/local-data/constants.js`](../../frontend/local-data/constants.js), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-12

## Current save-job flow

The current flow is structurally solid:

```text
Jobs page
→ user clicks save
→ auth/local-data availability check
→ job snapshot generated
→ saveJobForUser(uid, snapshot)
→ IndexedDB/local row written
→ activity event logged
→ saved-job subscribers notified
→ Jobs page saved-state updates
→ Saved page subscription re-renders row/table/timeline
```

On the Jobs page, `toggleSaveJob()` checks local API readiness, requires sign-in, computes `jobKey`, then removes or saves the job through `jobsSavedJobsService`. After success it updates `userState.savedJobKeys`, dispatches `SAVE_TOGGLED`, and re-renders the jobs list.

The persisted saved-job row is fairly complete: identity, display fields, custom-job metadata, reminder/contact fields, `applicationStatus`, `phaseTimestamps`, notes, attachment count, `savedAt`, and `updatedAt`. This shape is documented as the canonical saved-job contract.

## Phase tracking flow

Phase tracking currently behaves as:

```text
bookmark → applied → interview_1 → interview_2 → offer → rejected
```

The canonical status list lives in `frontend/local-data/constants.js`.

`normalizeApplicationStatus()` maps bad or missing values to `bookmark`, and `canTransitionPhase()` allows one-step forward moves, allows jump-to-`rejected`, and blocks any move after `rejected` unless override is used.

Saved page wraps this with useful UX: phase change checks transition validity, supports one global override, asks confirmation for locked transitions, requests timestamps for interview phases, writes status, logs `phase_changed`, refreshes activity, and provides a toast action to revert.

This is a good foundation. The main issue is not missing phase tracking; it is a model that is too linear for real application workflows.

## Saved page management flow

Saved page already includes:

```text
saved rows
phase bar
notes
attachments
history/activity timeline
custom job create/edit/duplicate
filters
sorts
reminders
backup export/import
```

Rendering preserves context (note-edit focus, anchor scroll, selected/expanded job, filters/sorts, workspace stats, attachment hydration).

The page subscribes to saved-job changes, updates `lastSavedJobsByKey`, refreshes activity, defers rerenders while notes are being edited, and updates shared state such as `savedCount` and `savedLastUpdated`.

Activity logging is profile-scoped and stores event type, job key, title/company snapshot, timestamp, and details. Activity filters support all / selected job / phase / notes / attachments.

## What is already good

1. Saved-job persistence is canonical enough.
   The saved row has a clear contract and is validated/documented through JS and Pydantic-side schemas.

2. Saving does not destroy existing local progress.
   `saveJobForUser()` preserves existing `savedAt`, phase timestamps, notes, attachment count, reminder/contact metadata, and prior application status when saving a known job.

3. Phase changes include lifecycle metadata.
   Each change updates `applicationStatus`, writes timestamp for new phase, optionally clears reverted phase timestamp, and logs an activity event.

4. Saved page handles real UX edge cases.
   It preserves textarea focus and scroll while notes autosave, delays rerender during edits, supports restore-after-delete, and handles auth restoration.

5. Custom jobs are integrated.
   Custom create/edit/duplicate flows use saved-job storage path, preserve history/status, and use `keySalt` only for duplicates.

## Main gaps and improvements

### 1. Phase model is too rigid for real job applications

Current progression is narrow:

```text
Saved → Applied → Interview 1 → Interview 2 → Final Round → Rejected
```

Common real workflows are missing:

```text
saved
applied
screening / recruiter call
technical test / art test / assignment
interview_1
interview_2
final_interview
offer
accepted
withdrawn
ghosted / no response
closed / job removed
```

The problem is using `rejected` for all negative outcomes. At minimum add terminal outcomes:

- `withdrawn`
- `ghosted`
- `closed`
- `accepted`

Recommended split model (smallest safe shape):

```text
pipelinePhase: bookmark | applied | screening | assignment | interview_1 | interview_2 | final | offer
outcomeStatus: active | rejected | withdrawn | ghosted | closed | accepted
```

Do **not** stage this by appending `withdrawn`, `ghosted`, `closed`, and `accepted` to the current linear `APPLICATION_STATUSES` list. The current transition helper treats that list as a one-step phase ladder and only special-cases `rejected` as terminal; adding more terminal values there would allow invalid terminal-to-terminal movement and would render every status as a phase button.

Required first implementation:

```text
pipelinePhase: bookmark | applied | screening | assignment | interview_1 | interview_2 | final | offer
outcomeStatus: active | rejected | withdrawn | ghosted | closed | accepted
```

Migration requirements:

- Existing `applicationStatus` rows migrate into `pipelinePhase` plus `outcomeStatus`.
- Existing `rejected` rows become `outcomeStatus="rejected"` with the best available previous active phase retained as `pipelinePhase`, falling back to `applied` only when no better phase evidence exists.
- Phase UI renders only pipeline phases.
- Outcome UI is separate and treats every non-`active` outcome as terminal unless a contextual override is confirmed.
- Transition logic uses explicit phase order plus a `TERMINAL_OUTCOME_STATUSES` set; terminal outcomes must not be modeled as phase steps.

### 2. `updatedAt` behavior is inconsistent

`saveJobForUser()` and attachment updates mutate `updatedAt`; phase changes currently set `updatedAt` to `current.updatedAt || current.savedAt || now`, and notes intentionally do not mutate it.

That makes "sort by updated" ambiguous.

Recommended split fields:

- `contentUpdatedAt` (title/company/link/custom metadata)
- `trackingUpdatedAt` (phase/reminder/contact/outcome)
- `notesUpdatedAt` (notes updates)
- `lastActivityAt` (any user-visible activity)

Backup/import requirement:

- Update `areSavedRowsEquivalent()` whenever these fields are added. It currently uses a hardcoded saved-row equivalence list, so new timestamp fields would otherwise be ignored during import merge checks.

Then Saved page sorting modes can be explicit:

```text
Recently active
Recently saved
Reminder due
Application stage
Custom first
```

### 3. Activity timeline is incomplete for notes

Notes autosave currently queues UI pulses, but `updateJobNotes()` does not persist a durable activity row.

Add lightweight debounced note logging:

```text
note_updated
details: { previousLength, nextLength, debounceWindow: true }
```

Log after idle/blur, not every keystroke.

Implementation requirement:

- Capture `previousLength` when the first debounced note save is queued, using the current row in `viewState.lastSavedJobsByKey`.
- Do not fetch the previous note body from IndexedDB during autosave just to compute the activity detail; that adds latency to the write path.
- Clear the captured previous-length state after the queued save succeeds or fails.

### 4. Phase timeline vs stored timestamps can drift semantically

Current behavior records phase timestamps in saved rows and phase events in activity log, but these are separate. Revert updates both phase state and timestamps via same event shape, which is harder to read.

Recommendation: add explicit revert event type:

```text
phase_reverted


details: {
  fromStatus,
  restoredStatus,
  removedTimestampFor,
  restoredTimestamp,
  overrideUsed
}
```

Keep transition and reversal semantically distinct.

### 5. Saved jobs should show source lifecycle context

Saved rows are snapshots, but source lifecycle in pipeline includes `active`, `likely_removed`, `archived`, `firstSeenAt`, `lastSeenAt`, and `removedAt`.

Recommended overlay per saved row:

```text
sourceStatus: active | likely_removed | archived | unknown
lastSeenAt
removedAt
sourceStillAvailable: boolean
sourceHealthReason
```

Display separately from application status, e.g.:

```text
Application: Interview 1
Source: likely removed 3 days ago
```

Implementation note:

- This is mostly an overlay/view-model task, not a new lifecycle badge system. `frontend/shared/lifecycle-badges.js` already handles `reappeared`, `preserved/source_failed`, `likely_removed`, and `archived`.
- Preserve and expose `lastSeenAt` in `toLifecycleOverlayRecord()`; current overlay construction already carries `removedAt` but drops `lastSeenAt`.
- Use the existing Saved relative-time formatter for copy such as "last seen 3d ago" or "removed 3d ago".

### 6. Filters are too coarse

Current filters are close to list-level only:

```text
all / custom / imported
```

Current sorts are also limited:

```text
updated / saved / reminder / personal
```

Recommended filters and grouping:

- Needs action
- Applied
- Interviewing
- Offer/final
- Rejected/closed
- Due soon
- No reminder
- Has notes
- Has attachments
- Missing link
- Likely removed from source
- Custom only / Imported only

Group by:

- phase
- company
- reminder week
- source status

### 7. Global phase override is risky UX

One-use global override affects all jobs and is easy to forget.

Recommendation: move to per-transition, contextual override modal:

```text
Click locked phase
→ reason modal: “This skips Applied → Interview 1”
→ optional reason
→ apply once
```

Persist in activity:

```text
overrideReason
overrideUsed
```

Implementation requirement:

- The current confirmation dialog is boolean-only. Add a dedicated reason dialog or extend the dialog layer with an optional text-input mode before removing the global override.
- The override reason is optional for the user, but the activity detail shape must explicitly record whether a reason was supplied.

### 8. Attachment hydration is correct but can be inefficient

Current flow hydrates attachment lists for visible rows and calls `listAttachmentsForJob()` after render. This is acceptable for small sets but expensive for many saved jobs.

Recommendation: lazy load attachments only when Attachment tab opens, using stored `attachmentsCount` for collapsed summaries.

Required lazy-load behavior:

- Track loaded attachment job keys in view state.
- Track loading attachment job keys so repeated clicks do not fan out duplicate reads.
- Render a "Loading..." state when the Attachments tab is opened for an unloaded job.
- Add a singular per-job hydrate path and call it from the details-tab switch when `tab === "attachments"`.
- Keep upload/delete paths refreshing that job's attachment list and marking it loaded.

### 9. Remove/restore flow is mostly good but not undo-safe

Undo restore currently rehydrates row but attachment behavior is implicit because attachments are separate rows and may remain orphaned across remove/restore semantics.

Make attachment behavior explicit. Prefer soft-delete semantics:

```text
savedJob.deletedAt
attachments remain linked
undo clears deletedAt
hard cleanup can run later
```

If changing this behavior, keep migration-safe compatibility and clearly define cleanup policy.

### 10. Saved and custom jobs need better intent modeling

Shared storage is good, but external and custom jobs need different tracking fields.

Add a flexible `tracking` object:

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

Render first-class fields first: priority, next action, next action date.

## Best next milestone

### Saved Jobs Tracker v1

1. Split saved-job tracking into `pipelinePhase` and `outcomeStatus`, including migration from legacy `applicationStatus`.
2. Add terminal outcomes through `outcomeStatus`: `accepted`, `withdrawn`, `ghosted`, `closed`, and legacy `rejected`.
3. Add `lastActivityAt`, `trackingUpdatedAt`, `contentUpdatedAt`, and `notesUpdatedAt`, and update backup/import equivalence checks.
4. Add saved-job source lifecycle overlay with `lastSeenAt` and `removedAt` relative-time copy.
5. Add phase/outcome-based filters: Applied, Interviewing, Offer, Rejected/Closed, Needs Action.
6. Persist note activity with debounce and captured previous/next note lengths.
7. Replace global phase override with per-transition override reason capture.
8. Lazy-load attachment lists on attachment tab open with loading/loaded state.
9. Add tests covering save → phase update → note update → attachment update → remove/restore.

## Implementation order

```text
1. Split phase/outcome contract and migrate legacy rows
2. Add derived saved-job view model
3. Add explicit activity timestamps and backup equivalence coverage
4. Add lifecycle/source overlay display with `lastSeenAt`
5. Improve filters/sorts
6. Improve activity logging and override reason capture
7. Lazy attachments / performance polish
8. Refine remove/restore semantics
9. Add tests
```

The highest-impact change is a shared saved-job view model to normalize:

```js
{
  jobKey,
  title,
  company,
  displayLocation,
  applicationPhase,
  outcomeStatus,
  phaseLabel,
  sourceLifecycleLabel,
  needsAction,
  reminderState,
  hasNotes,
  attachmentsCount,
  lastActivityAt,
  sortKeys,
  badges,
  allowedActions
}
```

Then table, filters, timeline, and future dashboard all consume the same interpretation instead of duplicate derivation per component.

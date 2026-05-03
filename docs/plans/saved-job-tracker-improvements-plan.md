# Saved Job Tracker Improvement Plan

> - **Status:** Active next-step tracker
> - **Use this when:** improving saved-job persistence semantics, application tracking, filtering, activity logging, or source-lifecycle visibility in the Saved page
> - **Canonical for:** saved-job data model refinements, phase/outcome ergonomics, activity semantics, Saved page operations UX, and milestone sequencing toward Saved Jobs Tracker v1
> - **Not canonical for:** backend job discovery/fetch contracts, local data storage internals, or deployment/packaging behavior
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../../frontend/local-data/constants.js`](../../frontend/local-data/constants.js), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-03

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

If staged, first add `withdrawn`, `ghosted`, `closed`, `accepted` as terminal outcome statuses.

### 2. `updatedAt` behavior is inconsistent

`saveJobForUser()` and attachment updates mutate `updatedAt`; phase changes currently set `updatedAt` to `current.updatedAt || current.savedAt || now`, and notes intentionally do not mutate it.

That makes "sort by updated" ambiguous.

Recommended split fields:

- `contentUpdatedAt` (title/company/link/custom metadata)
- `trackingUpdatedAt` (phase/reminder/contact/outcome)
- `notesUpdatedAt` (notes updates)
- `lastActivityAt` (any user-visible activity)

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

### 8. Attachment hydration is correct but can be inefficient

Current flow hydrates attachment lists for visible rows and calls `listAttachmentsForJob()` after render. This is acceptable for small sets but expensive for many saved jobs.

Recommendation: lazy load attachments only when Attachment tab opens, using stored `attachmentsCount` for collapsed summaries.

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

1. Add terminal outcomes: `accepted`, `withdrawn`, `ghosted`, `closed`.
2. Add `lastActivityAt` and `trackingUpdatedAt` so sort semantics match user intent.
3. Add saved-job source lifecycle overlay: active / likely removed / archived / unknown.
4. Add phase/outcome-based filters: Applied, Interviewing, Offer, Rejected/Closed, Needs Action.
5. Persist note activity with debounce.
6. Replace global phase override with per-transition override reason.
7. Lazy-load attachment lists on attachment tab open.
8. Add tests covering save → phase update → note update → attachment update → remove/restore.

## Implementation order

```text
1. Extend status contract carefully
2. Add derived saved-job view model
3. Add lifecycle/source overlay display
4. Improve filters/sorts
5. Improve activity logging
6. Refine remove/restore semantics
7. Add tests
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

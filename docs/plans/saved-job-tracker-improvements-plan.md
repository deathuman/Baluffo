# Saved Job Tracker Improvement Plan

> - **Status:** Active next-step tracker
> - **Use this when:** improving saved-job persistence semantics, application tracking, filtering, activity logging, or source-lifecycle visibility in the Saved page
> - **Canonical for:** saved-job data model refinements, phase/outcome ergonomics, activity semantics, Saved page operations UX, and milestone sequencing toward Saved Jobs Tracker v1
> - **Not canonical for:** backend job discovery/fetch contracts, local data storage internals, or deployment/packaging behavior
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../../frontend/local-data/constants.js`](../../frontend/local-data/constants.js), [`../../frontend/local-data/phase.js`](../../frontend/local-data/phase.js), [`../../src/local_data_store_shared.py`](../../src/local_data_store_shared.py), [`../../src/core/schemas.py`](../../src/core/schemas.py), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-16

## Implementation Status

Saved Job Tracker v1 is now implemented as a split phase/outcome model. The canonical contract is documented in [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md); this plan remains useful as rationale and future hardening context, not as the current row-shape source of truth.

Implemented guardrails:

- Tracking normalization is owned by paired modules: [`../../frontend/local-data/tracking.js`](../../frontend/local-data/tracking.js) and [`../../src/local_data_store_tracking.py`](../../src/local_data_store_tracking.py).
- Saved rows persist `pipelinePhase`, `outcomeStatus`, `outcomeTimestamps`, content/tracking/notes timestamps, and `lastActivityAt`; `applicationStatus` is a write-only compatibility mirror.
- Browser and desktop tracking use the same parity fixtures in [`../../tests/fixtures/saved_job_tracking_cases.json`](../../tests/fixtures/saved_job_tracking_cases.json).
- Saved page filtering/sorting consumes [`../../frontend/saved/app/view-model.js`](../../frontend/saved/app/view-model.js) instead of reinterpreting raw rows in render/filter/timeline code.
- Phase and outcome controls are rendered separately by [`../../frontend/saved/app/tracking-ui.js`](../../frontend/saved/app/tracking-ui.js).
- Backup export writes schema v3; import accepts v1/v2/v3.

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

## Implementation guardrails

These constraints are part of the v1 scope. Do not treat them as optional cleanup after the UI work.

### Contract, schema, and versioning

- Update [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) when saved-row fields, backup payloads, activity details, or local-data route payloads change.
- Update Pydantic schemas in [`../../src/core/schemas.py`](../../src/core/schemas.py), including `SavedJobSchema`, `LocalSavedJobRowSchema`, `LocalDataActivityRowSchema`, and `LocalDataBackupPayloadSchema`.
- Update frontend typedefs in [`../../frontend/shared/types.js`](../../frontend/shared/types.js).
- Bump `DB_VERSION` when IndexedDB object-store migration is required.
- Bump `BACKUP_SCHEMA_VERSION` when export/import shape changes in a way new clients should distinguish.
- Keep old exports/imports tolerant: schema v1/v2 payloads with only `applicationStatus`, `phaseTimestamps`, `updatedAt`, and no split fields must still import.
- Define default normalization explicitly: missing or invalid `pipelinePhase` becomes `bookmark`; missing or invalid `outcomeStatus` becomes `active`.
- Do not leave `assertLocalDataRuntime()` broken. Either keep a compatibility `APPLICATION_STATUSES` export for old callers or update the runtime validator and every caller in the same change.

### Browser and desktop parity

- Implement phase/outcome constants, normalization, transition rules, and migration in both browser IndexedDB local data and desktop bridge-backed local data.
- Browser-owned code currently routes through `frontend/local-data/*`; desktop-owned code currently routes through `src/local_data_store_*.py` and bridge local-data routes.
- Do not let browser and desktop disagree on allowed phases, terminal outcomes, timestamp semantics, backup payloads, or activity details.
- Add parity tests or shared fixtures that exercise both runtimes for the same legacy row and the same new split-row payload.

### Compatibility API boundary

- Keep compatibility wrappers for current public methods and routes unless a separate compatibility break is explicitly approved.
- Current public names include `canTransitionPhase`, `updateApplicationStatus`, and `/saved-jobs/status`; they may delegate to new split-model helpers but must not silently reinterpret terminal outcomes as phase steps.
- Prefer new internal names that encode the split, such as `normalizePipelinePhase`, `normalizeOutcomeStatus`, `canTransitionPipelinePhase`, `canSetOutcomeStatus`, and `updateApplicationTracking`.
- If a new route is added, keep the old route accepting legacy status payloads and normalize them through the migration path.
- Keep Jobs page saved-state callers working; saving/removing a job should still update saved keys and subscribers without needing to know the new tracking model.

### Migration ordering

- Read legacy rows first and derive the split shape in memory without losing the original `applicationStatus`.
- On the next successful write/export/import, persist the new shape and keep legacy-compatible fields only as explicit compatibility mirrors if needed.
- Import old backups by deriving `pipelinePhase`, `outcomeStatus`, phase timestamps, and outcome timestamps from legacy fields.
- Export new backups with the split fields and, if compatibility mirrors are kept, document whether old clients may ignore the new fields.
- Migration must be idempotent: running it twice must not move timestamps, duplicate activity, or alter terminal outcomes.

### Outcome timestamps and terminal events

- Do not store terminal outcomes without a timestamp.
- Add an explicit timestamp strategy, either `outcomeTimestamps`, `outcomeUpdatedAt`, or a durable tracking-event model.
- Preserve `phaseTimestamps` for pipeline phases only.
- Define timestamp behavior for new phases (`screening`, `assignment`, `final`) before implementation. Default to current time unless the UI intentionally requests a user-entered timestamp.
- Add activity event types for `outcome_changed` and, if undo/revert is supported, `outcome_reverted`.
- Keep `phase_changed` and `phase_reverted` for pipeline phase movement only.
- Activity details should distinguish `previousPhase` / `nextPhase` from `previousOutcome` / `nextOutcome` to avoid another ambiguous `status` field.
- Update `lastActivityAt` atomically when durable activity rows are written. Do not rely only on cached frontend activity to compute persisted sort keys.

### Timeline and formatting

- Update activity labels and detail formatting for new event types: `note_updated`, `phase_reverted`, `outcome_changed`, and `outcome_reverted`.
- Update timeline scoping so outcome events appear in the application-tracking scope instead of falling through to generic "Event" rows.
- If the UI keeps the label "phase activity", either rename it to "application activity" or include both phase and outcome events explicitly.

### Source lifecycle overlay semantics

- Treat source lifecycle as read-only overlay data, separate from user application tracking.
- Do not persist source lifecycle into saved rows as user-owned state unless a separate sync/cache policy is defined.
- Saved rows should display `sourceStatus`, `lastSeenAt`, and `removedAt` derived from current lifecycle overlay data when available, with `unknown` when overlay data is absent.
- User outcomes such as `closed`, `withdrawn`, or `ghosted` must not be inferred automatically from source lifecycle unless a separate explicit user action exists.
- Match saved rows to lifecycle rows through the same `generateJobKey` identity logic used by saved-job storage. Avoid title/company fuzzy matching for lifecycle overlays because it can attach source state to the wrong saved row.

### Filters, sorts, and persisted preferences

- Update filter/sort validation and persistence for the expanded filter set.
- Old persisted filter/sort preferences must fall back safely when the key no longer exists.
- The view model must own derived flags such as `needsAction`, `hasNotes`, `hasAttachments`, source lifecycle bucket, and outcome bucket so table rendering, filters, and timeline do not reimplement conflicting rules.

### Lazy attachment hydration

- Lazy loading must preserve the existing upload/delete/open/download behavior.
- Use `attachmentsCount` for collapsed summaries, but do not treat it as authoritative file metadata.
- Opening the Attachments tab for an unloaded job must show loading state, then real rows or an empty state after the list call completes.
- Upload/delete must refresh the singular job attachment list and update loaded/loading state for that job.
- Attachment previews and object URLs must still be revoked when a list is refreshed or a job row is removed from the DOM.
- Loaded/loading attachment state is profile-scoped; clear it on sign-out, profile switch, or saved-runtime reset so one profile never reuses another profile's attachment cache.

### Remove/restore and soft delete

- Decide before implementation whether soft delete belongs in v1 or is explicitly deferred.
- If soft delete is in v1, define how `deletedAt` affects saved keys, saved count, filters, subscriptions, backup/export, import, activity, attachment listing, and hard cleanup.
- If soft delete is deferred, keep current hard-remove behavior and make attachment orphan policy explicit in the plan before changing restore semantics.

### Minimum regression coverage

Add focused tests for:

- Legacy `applicationStatus` rows migrate to `pipelinePhase` + `outcomeStatus`.
- Legacy rejected rows retain the best available previous phase and become terminal rejected outcomes.
- Terminal outcomes cannot transition to another terminal outcome through phase transition logic.
- Old backups import and new backups export with expected version and field shape.
- Browser and desktop local-data runtimes produce equivalent normalized rows for the same input.
- `phase_changed`, `phase_reverted`, `outcome_changed`, `outcome_reverted`, and `note_updated` render readable activity details.
- Outcome events appear in the intended timeline scope.
- Source lifecycle overlay preserves `lastSeenAt` and does not mutate user-owned application tracking.
- Lazy attachment loading shows loading state, avoids duplicate reads, refreshes after upload/delete, and leaves collapsed counts usable.
- Remove/restore behavior preserves or intentionally cleans attachment links according to the chosen delete policy.

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

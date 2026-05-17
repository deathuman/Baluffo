# Saved Job Tracker Improvement Plan

> - **Status:** V1 implemented; this document now tracks hardening and deferred follow-up work
> - **Use this when:** improving saved-job tracking semantics, filtering, activity logging, source-lifecycle visibility, attachment hydration, or remove/restore behavior
> - **Canonical for:** saved-job tracker rationale, current implementation map, deferred decisions, and next-step backlog
> - **Not canonical for:** saved-job row shape, backup payload shape, or bridge route contracts. Use [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) for those contracts.
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../../frontend/local-data/tracking.js`](../../frontend/local-data/tracking.js), [`../../src/local_data_store_tracking.py`](../../src/local_data_store_tracking.py), [`../../frontend/saved/app/view-model.js`](../../frontend/saved/app/view-model.js), [`../../frontend/saved/app/tracking-ui.js`](../../frontend/saved/app/tracking-ui.js), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-17

## Current Position

Saved Job Tracker v1 is implemented. The app now uses a split tracking model:

```text
pipelinePhase: bookmark | applied | screening | assignment | interview_1 | interview_2 | final | offer
outcomeStatus: active | rejected | withdrawn | ghosted | closed | accepted
```

`applicationStatus` remains only as a legacy compatibility mirror. New code should read `pipelinePhase` and `outcomeStatus`.

The canonical persisted row and backup shape live in [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md). This plan is no longer the row-shape source of truth.

## Implemented V1 Scope

### Tracking Contract

- Browser tracking normalization lives in [`../../frontend/local-data/tracking.js`](../../frontend/local-data/tracking.js).
- Desktop tracking normalization lives in [`../../src/local_data_store_tracking.py`](../../src/local_data_store_tracking.py).
- Both runtimes normalize legacy `applicationStatus` rows into `pipelinePhase` plus `outcomeStatus`.
- Legacy rejected rows retain the best previous active phase when timestamp evidence exists.
- Phase transitions and terminal outcome transitions are separate.
- Terminal outcomes lock phase movement unless the contextual override flow is used.
- `APPLICATION_STATUSES` is retained only as a compatibility export for old callers.

### Persistence And Backup

- Saved rows persist `pipelinePhase`, `outcomeStatus`, `phaseTimestamps`, `outcomeTimestamps`, `contentUpdatedAt`, `trackingUpdatedAt`, `notesUpdatedAt`, and `lastActivityAt`.
- `applicationStatus` is derived from phase/outcome on writes.
- Backup export writes schema v3.
- Import accepts v1, v2, and v3 payloads.
- Backup equivalence includes the new tracking/timestamp fields.
- Pydantic schemas and frontend typedefs were updated.
- Packaging and ship-bundle checks include the new desktop tracking module.

### Activity Semantics

- Phase changes write `phase_changed`.
- Phase toast reverts write `phase_reverted`.
- Outcome changes write `outcome_changed`.
- Outcome toast reverts write `outcome_reverted`.
- Revert activity keeps generic previous/next fields and adds explicit restored/reverted fields for audit copy.
- Notes write durable `note_updated` activity when content changes.
- Note activity compares note content, not note length.
- Activity writes update `lastActivityAt` through the shared activity write path.
- Activity detail formatting handles phase, outcome, note, attachment, and remove events.

### Saved Page View Model

- Saved row interpretation is centralized in [`../../frontend/saved/app/view-model.js`](../../frontend/saved/app/view-model.js).
- The view model owns phase bucket, outcome bucket, source bucket, `needsAction`, action-reason metadata, notes/files flags, missing link, sort keys, and allowed actions.
- Saved filters are expanded beyond custom/imported.
- Saved sorts include recent activity and stage.
- Saved grouping is view-only and default-off. `none` preserves the flat list; `stage` groups filtered and sorted rows by active stage or terminal outcome.
- The grouping choice is persisted per local profile and invalid stored values normalize back to `none`.
- Rendering/filtering/sorting use the view model instead of reinterpreting raw rows independently.

### Tracking UI

- Phase and outcome controls are rendered separately in [`../../frontend/saved/app/tracking-ui.js`](../../frontend/saved/app/tracking-ui.js).
- Phase UI is a compact horizontal stepper.
- Outcome UI is a quiet status chip plus final-outcome menu.
- Active jobs show `Set final outcome`.
- Terminal jobs show `Change outcome` plus a reopen action.
- Offer is treated as the final active phase and shows an awaiting-outcome/final-stage indicator.
- The expanded action row shows current phase, entered time, last activity, and the primary attention reason when one exists.
- Locked transitions use a contextual override flow with optional reason capture.

### Saved Row Layout And UX

- Saved rows now use the Jobs page hierarchy: Position with Sector underneath; Location with Country and City stacked.
- Long Position text is constrained and only uses clipped-text tooltip behavior.
- Company/city tooltips that repeated visible text were removed.
- The link icon remains a framed link action.
- Remove uses a smaller danger affordance plus confirmation dialog before deletion.
- Reminder badges distinguish overdue reminders, due-soon reminders, and scheduled reminders.
- Expanded job cards are more compact and visually closer to the mockup direction.
- The details toggle uses the `Notes, Files & History` treatment.

### Source Lifecycle Overlay

- Saved page source lifecycle overlay is read-only and separate from user tracking.
- Lifecycle rows are matched by the same generated job key logic used by saved-job storage.
- The overlay preserves `status`, `removedAt`, `lastSeenAt`, `lifecycleEvent`, and `lifecycleReason`.
- Saved lifecycle badge tooltips include `lastSeenAt` relative copy for non-active lifecycle badges without changing Jobs page badge copy.
- `needsAction` is evidence-only: due/overdue reminders or active saved jobs whose source is likely removed/archived.
- Source lifecycle never auto-converts user outcomes.

### Attachments

- Attachment state tracks loaded and loading job keys.
- Attachment tab opening hydrates the selected job list instead of eagerly loading every row.
- Passive rerenders and repeated attachment-tab openings reuse the loaded job cache instead of forcing duplicate reads.
- Upload/delete refresh the affected job list.
- Attachment preview object URLs are still cleared when lists refresh.
- Loaded/loading attachment state is reset on auth/profile reset.

### Tests

Coverage exists for:

- Browser and Python tracking parity fixtures.
- Legacy rejected migration retaining best prior phase.
- Legacy source-over-existing split-row import behavior.
- `updateApplicationStatus`, `/saved-jobs/status`, and split tracking routes.
- Backup v1/v2 import and v3 export.
- `lastActivityAt` updates from phase/outcome/notes/attachments.
- Note updates with same-length different content.
- Saved view-model filters/sorts/needs-action/action-reason behavior.
- Phase/outcome UI rendering, including expanded action-row state summaries.
- Remove confirmation cancel/confirm behavior.
- Remove/Undo behavior preserving attachment rows linked by `profileId` and `jobKey`.
- Attachment lazy-load duplicate-read prevention on rerender and repeated tab open.
- Saved lifecycle badge copy for `lastSeenAt` while active source overlays stay visually quiet.
- Saved grouping model and render behavior, including filter-before-group and sorted order inside groups.
- Phase/outcome revert activity audit details in both browser and Python local-data runtimes.
- Saved row layout, clipped tooltips, compact tracking UI, and tooltip cleanup.

## Deferred Work

These items are intentionally not part of the completed v1 contract.

### 1. Remove/Restore Policy

V1 policy is now locked: hard remove plus immediate Undo. Attachments are stored separately and remain linked by `profileId` and `jobKey` unless explicitly deleted through attachment actions.

If soft delete is added later, define how it affects:

- saved keys
- saved count
- filters
- subscriptions
- backup/export/import
- activity
- attachment listing
- hard cleanup

Do not add `deletedAt` casually; it changes storage semantics and compatibility expectations.

### 2. Attachment Lazy-Load Polish

The loaded/loading state exists and passive render/tab paths now respect it. Forced reload should remain limited to actual attachment mutation paths.

Possible future polish:

- Add an explicit manual refresh action if users need it.
- Keep loading state visible for the first load of an unloaded attachment tab.
- Continue to verify upload/delete refreshes the affected job list.

### 3. Grouping

Stage grouping is implemented as a view-only Saved-page mode:

- `none` is the default and keeps the existing flat list.
- `stage` consumes `groupSavedJobViews()` after filtering and sorting.
- Active jobs group into Saved, Applied, Interviewing, and Final / Offer.
- Terminal jobs group by outcome label: Rejected, Withdrawn, Ghosted, Closed, and Accepted.
- Grouping does not change saved-job storage, routes, backup shape, tracking state, selection, expansion, or attachment hydration.
- The chosen group mode is stored as local UI preference data, not on saved-job rows.

Deferred candidate group modes:

- company
- reminder week
- source status

Add more modes only when the Saved page needs a denser list-management workflow. New modes should consume `buildSavedJobViewModel()` and not rederive tracking rules.

### 4. Richer Revert Details

`phase_reverted` and `outcome_reverted` now keep the generic previous/next fields and add explicit audit fields.

Phase revert details may include:

```json
{
  "revertedFromPhase": "offer",
  "restoredPhase": "final",
  "removedPhaseTimestampFor": "offer",
  "restoredPhaseTimestamp": "2026-05-16T20:52:00+00:00",
  "overrideUsed": true
}
```

Outcome revert details may include `revertedFromOutcome`, `restoredOutcome`, and `restoredOutcomeTimestamp`.

### 5. Flexible Tracking Object

The broader CRM-style tracking object is deferred:

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

This is v2 scope. It should not be mixed into v1 cleanup unless we are intentionally expanding Saved Jobs into a richer application tracker.

## Next Backlog

Recommended order:

1. Decide whether company/reminder/source-status grouping should become separate modes.
2. Leave flexible `tracking` object for a separate v2 design pass.

## Guardrails For Future Changes

### Contract, Schema, And Versioning

- Update [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) when saved-row fields, backup payloads, activity details, or local-data route payloads change.
- Update Pydantic schemas in [`../../src/core/schemas.py`](../../src/core/schemas.py).
- Update frontend typedefs in [`../../frontend/shared/types.js`](../../frontend/shared/types.js).
- Bump `DB_VERSION` only when IndexedDB migration behavior changes.
- Bump `BACKUP_SCHEMA_VERSION` only when the export/import shape changes in a way new clients should distinguish.
- Keep v1/v2 backup import tolerant.
- Keep `applicationStatus` as a derived compatibility mirror until compatibility callers are removed intentionally.

### Browser/Desktop Parity

- Keep browser and desktop tracking helpers paired.
- Add or update shared parity fixtures when normalization rules change.
- Do not let browser and desktop disagree on phases, outcomes, timestamp semantics, backup payloads, or activity details.

### Compatibility API Boundary

- Keep `canTransitionPhase`, `updateApplicationStatus`, and `/saved-jobs/status` as compatibility surfaces unless a separate breaking change is approved.
- New internal work should prefer split-model names such as `normalizePipelinePhase`, `normalizeOutcomeStatus`, `canTransitionPipelinePhase`, `canSetOutcomeStatus`, and `updateApplicationTracking`.
- Jobs page save/remove callers should not need to understand the split tracking model.

### Source Lifecycle Semantics

- Source lifecycle is read-only overlay data.
- Do not persist source lifecycle as user-owned tracking state.
- Do not infer user outcomes from source lifecycle.
- Match lifecycle rows by generated job key, not fuzzy title/company matching.

### Activity Semantics

- Durable user-visible activity should touch `lastActivityAt`.
- Outcome events belong in application-tracking timeline scope.
- Note activity should log content changes, not every keystroke.
- Attachment activity should remain profile/job scoped.

### Attachment Semantics

- Use `attachmentsCount` for collapsed summaries only.
- Do not treat `attachmentsCount` as full file metadata.
- Clear loaded/loading attachment cache on profile switch/sign-out/runtime reset.
- Refresh the affected job list after upload/delete.
- Revoke object URLs when attachment lists refresh or rows leave the DOM.

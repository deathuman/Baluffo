# Saved Job Tracker Deferred Plan

> - **Status:** Parked; Saved Job Tracker v1 is complete
> - **Use this when:** deciding whether to restart Saved Jobs tracking work, add v2 CRM-style tracking, or revisit deferred list-management behavior
> - **Canonical for:** parked Saved Jobs deferred decisions, restart criteria, and future-change guardrails
> - **Not canonical for:** saved-job row shape, backup payload shape, bridge route contracts, or current UI behavior. Use [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) and source files for those contracts.
> - **Then inspect:** [`../AI_ASSISTANT_GUIDE.md`](../AI_ASSISTANT_GUIDE.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md), [`../../frontend/local-data/tracking.js`](../../frontend/local-data/tracking.js), [`../../src/local_data_store_tracking.py`](../../src/local_data_store_tracking.py), [`../../frontend/saved/app/view-model.js`](../../frontend/saved/app/view-model.js), [`../../frontend/saved/app/tracking-ui.js`](../../frontend/saved/app/tracking-ui.js), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-17

## Current Position

Saved Job Tracker v1 is implemented, hardened, visually polished, tested, documented, and shipped. This document is no longer an active implementation tracker.

The current split tracking model is:

```text
pipelinePhase: bookmark | applied | screening | assignment | interview_1 | interview_2 | final | offer
outcomeStatus: active | rejected | withdrawn | ghosted | closed | accepted
```

`applicationStatus` remains a derived compatibility mirror. New code should read `pipelinePhase` and `outcomeStatus`.

The persisted row shape, backup schema, local-data payload expectations, and activity detail contracts live in [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md). Treat that contract and the implementation/tests as canonical over this parked plan.

## Shipped V1 Summary

V1 now includes:

- Browser and desktop tracking helpers with split phase/outcome normalization.
- Compatibility support for legacy `applicationStatus`, `updateApplicationStatus`, `canTransitionPhase`, and `/saved-jobs/status`.
- Backup schema v3 export and tolerant v1/v2/v3 import.
- Phase/outcome activity logging, revert audit details, note/attachment activity, and `lastActivityAt` updates through shared activity paths.
- Saved-page view-model ownership for filtering, sorting, grouping, lifecycle overlay, action metadata, reminder pressure, and allowed actions.
- Compact Saved tracking UI with separate phase and final-outcome controls.
- Applied as a visible date-only milestone when the stored Applied timestamp exists.
- Backward override cleanup that clears future `phaseTimestamps` in both browser and desktop runtimes.
- Read-only source lifecycle overlay and Saved-only lifecycle tooltip copy.
- Attachment lazy loading with loaded/loading caches and mutation refreshes.
- Hard remove plus confirmation and immediate Undo restore, with attachments preserved by `profileId` and `jobKey`.
- Default-off `Stage` grouping.
- Saved row layout and expanded-card UI polish.

Coverage exists for the browser/Python parity paths, compatibility surfaces, backup import/export, view-model behavior, grouping, activity semantics, attachment lazy loading, remove/restore, and tracking UI behavior. Use [`../testing.md`](../testing.md) and local test files for exact verification commands.

## Parked Deferred Backlog

Do not continue Saved Jobs work from this file unless one of these deferred decisions becomes active again.

### 1. V2 CRM-Style Tracking Object

This is the only deferred item with clear product-expansion value. It should be a separate v2 design pass, not a v1 cleanup task.

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

Restart criteria:

- Saved Jobs is intentionally becoming a lightweight application tracker or CRM.
- The UI has a concrete workflow for next actions, contacts, salary notes, or priority.
- The storage, backup, import/export, desktop parity, activity, and filtering implications are designed together.

Do not add a flexible `tracking` bag opportunistically. It changes the product surface and can become an untyped dumping ground.

### 2. Additional Grouping Modes

`Stage` grouping is already implemented and default-off. More modes are not currently justified.

Candidate modes to revisit only if usage shows list-management pain:

- company
- reminder week
- source status

Restart criteria:

- Saved-page lists are large enough that flat list plus Stage grouping is not enough.
- The grouping has a specific workflow benefit, not just another way to slice the same data.
- The mode can consume `buildSavedJobViewModel()` and `groupSavedJobViews()` without rederiving tracking or lifecycle rules.

### 3. Attachment Tab Polish

Attachment lazy-loading is already hardened. Future work should stay minor unless users hit real friction.

Possible polish:

- Add an explicit manual refresh action.
- Make first-load loading state clearer for an unloaded attachment tab.

Do not change attachment storage, backup shape, or `attachmentsCount` semantics for this polish.

### 4. Soft Delete

Soft delete is parked and should not be added casually. V1 policy is hard remove plus immediate Undo, with attachments preserved separately unless explicitly deleted.

Restart criteria:

- There is a real need for saved-job trash, later restore, cleanup, or audit retention.
- The design covers saved keys, counts, filters, subscriptions, backup/export/import, activity, attachment listing, and hard cleanup.

Do not add `deletedAt` as a narrow field-only change. It changes storage semantics and compatibility expectations.

## Not Active Backlog

These are shipped and should not remain as forward work:

- Remove/restore confirmation and hard-remove policy.
- Attachment lazy-load duplicate-read prevention.
- Stage grouping.
- Phase/outcome revert audit details.
- Lifecycle badge `lastSeenAt` copy.
- Action clarity and sticky Saved header polish.
- Phase tracker Applied milestone, tooltip cleanup, and backward override timestamp cleanup.

If one of these areas regresses, open a bug against the owning source/tests instead of reviving this plan as an implementation checklist.

## Guardrails For Future Changes

### Contract, Schema, And Versioning

- Update [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md) when saved-row fields, backup payloads, activity details, or local-data route payloads change.
- Update Pydantic schemas in [`../../src/core/schemas.py`](../../src/core/schemas.py) when desktop payload schemas change.
- Update frontend typedefs in [`../../frontend/shared/types.js`](../../frontend/shared/types.js) when frontend-facing shapes change.
- Bump `DB_VERSION` only when IndexedDB migration behavior changes.
- Bump `BACKUP_SCHEMA_VERSION` only when export/import shape changes in a way new clients should distinguish.
- Keep v1/v2 backup import tolerant.
- Keep `applicationStatus` as a derived compatibility mirror until compatibility callers are intentionally removed.

### Browser/Desktop Parity

- Keep browser and desktop tracking helpers paired.
- Add or update shared parity fixtures when normalization, transition, timestamp, backup, or activity semantics change.
- Do not let browser and desktop disagree on phases, outcomes, timestamp cleanup, backup payloads, or activity details.

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
- Clear loaded/loading attachment cache on profile switch, sign-out, and runtime reset.
- Refresh the affected job list after upload/delete.
- Revoke object URLs when attachment lists refresh or rows leave the DOM.

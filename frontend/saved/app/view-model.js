import {
  normalizeOutcomeStatus,
  normalizePipelinePhase,
  OUTCOME_STATUSES,
  PIPELINE_PHASES
} from "../../local-data/tracking.js";

export const SAVED_FILTER_ALL = "all";
export const SAVED_FILTER_NEEDS_ACTION = "needs_action";
export const SAVED_FILTER_APPLIED = "applied";
export const SAVED_FILTER_INTERVIEWING = "interviewing";
export const SAVED_FILTER_OFFER = "offer";
export const SAVED_FILTER_CLOSED = "closed";
export const SAVED_FILTER_DUE_SOON = "due_soon";
export const SAVED_FILTER_NO_REMINDER = "no_reminder";
export const SAVED_FILTER_HAS_NOTES = "has_notes";
export const SAVED_FILTER_HAS_ATTACHMENTS = "has_attachments";
export const SAVED_FILTER_MISSING_LINK = "missing_link";
export const SAVED_FILTER_LIKELY_REMOVED = "likely_removed";
export const SAVED_FILTER_CUSTOM = "custom";
export const SAVED_FILTER_IMPORTED = "imported";

export const SORT_UPDATED = "updated";
export const SORT_SAVED = "saved";
export const SORT_REMINDER = "reminder";
export const SORT_PERSONAL = "personal";
export const SORT_ACTIVITY = "activity";
export const SORT_STAGE = "stage";
export const REMINDER_SOON_HOURS = 72;
export const SAVED_GROUP_NONE = "none";
export const SAVED_GROUP_STAGE = "stage";

const VALID_FILTERS = new Set([
  SAVED_FILTER_ALL,
  SAVED_FILTER_NEEDS_ACTION,
  SAVED_FILTER_APPLIED,
  SAVED_FILTER_INTERVIEWING,
  SAVED_FILTER_OFFER,
  SAVED_FILTER_CLOSED,
  SAVED_FILTER_DUE_SOON,
  SAVED_FILTER_NO_REMINDER,
  SAVED_FILTER_HAS_NOTES,
  SAVED_FILTER_HAS_ATTACHMENTS,
  SAVED_FILTER_MISSING_LINK,
  SAVED_FILTER_LIKELY_REMOVED,
  SAVED_FILTER_CUSTOM,
  SAVED_FILTER_IMPORTED
]);

const VALID_SORTS = new Set([
  SORT_UPDATED,
  SORT_SAVED,
  SORT_REMINDER,
  SORT_PERSONAL,
  SORT_ACTIVITY,
  SORT_STAGE
]);

const VALID_GROUPS = new Set([
  SAVED_GROUP_NONE,
  SAVED_GROUP_STAGE
]);

const ACTIVE_STAGE_GROUPS = [
  { key: "stage_saved", label: "Saved", phaseBucket: "saved" },
  { key: "stage_applied", label: "Applied", phaseBucket: "applied" },
  { key: "stage_interviewing", label: "Interviewing", phaseBucket: "interviewing" },
  { key: "stage_offer", label: "Final / Offer", phaseBucket: "offer" }
];

const TERMINAL_STAGE_GROUPS = [
  { key: "outcome_rejected", label: "Rejected", outcomeStatus: "rejected" },
  { key: "outcome_withdrawn", label: "Withdrawn", outcomeStatus: "withdrawn" },
  { key: "outcome_ghosted", label: "Ghosted", outcomeStatus: "ghosted" },
  { key: "outcome_closed", label: "Closed", outcomeStatus: "closed" },
  { key: "outcome_accepted", label: "Accepted", outcomeStatus: "accepted" }
];

const STAGE_GROUPS = [
  ...ACTIVE_STAGE_GROUPS,
  ...TERMINAL_STAGE_GROUPS
];

export function isCustomJob(job) {
  return Boolean(job && job.isCustom);
}

function parseTime(value, parseIsoDate) {
  const parsed = typeof parseIsoDate === "function" ? parseIsoDate(value) : null;
  return parsed ? parsed.getTime() : 0;
}

function resolveNowMs(value) {
  if (typeof value === "function") return Number(value()) || Date.now();
  if (value instanceof Date) return value.getTime();
  return Number(value) || Date.now();
}

function reminderState(reminderAt, { parseIsoDate, now = Date.now } = {}) {
  const parsed = typeof parseIsoDate === "function" ? parseIsoDate(reminderAt) : null;
  if (!parsed) return { hasReminder: false, isDueSoon: false, isOverdue: false, weight: 3 };
  const diff = parsed.getTime() - resolveNowMs(now);
  const isOverdue = diff < 0;
  const isDueSoon = isOverdue || diff <= REMINDER_SOON_HOURS * 60 * 60 * 1000;
  return {
    hasReminder: true,
    isDueSoon,
    isOverdue,
    weight: isDueSoon ? 0 : 1
  };
}

function phaseEnteredAtFor(job, phase) {
  const timestamps = job?.phaseTimestamps && typeof job.phaseTimestamps === "object"
    ? job.phaseTimestamps
    : {};
  const timestamp = timestamps[phase];
  if (timestamp) return String(timestamp);
  return phase === "bookmark" ? String(job?.savedAt || "") : "";
}

function phaseBucketFor(phase) {
  if (phase === "applied" || phase === "screening" || phase === "assignment") return "applied";
  if (phase === "interview_1" || phase === "interview_2") return "interviewing";
  if (phase === "final" || phase === "offer") return "offer";
  return "saved";
}

function sourceBucketFor(lifecycleOverlay) {
  const status = String(lifecycleOverlay?.status || "").trim().toLowerCase();
  if (status === "likely_removed") return "likely_removed";
  if (status === "archived") return "archived";
  if (status === "active") return "active";
  return "unknown";
}

function attentionReason(key) {
  const labels = {
    reminder_overdue: "Overdue reminder",
    reminder_due_soon: "Reminder due soon",
    source_likely_removed: "Source likely removed",
    source_archived: "Source archived"
  };
  return {
    key,
    label: labels[key] || key
  };
}

export function buildSavedJobViewModel(job, options = {}) {
  const lifecycleOverlay = options.lifecycleOverlay || null;
  const parseIsoDate = options.parseIsoDate;
  const pipelinePhase = normalizePipelinePhase(job?.pipelinePhase || job?.applicationStatus);
  const outcomeStatus = normalizeOutcomeStatus(job?.outcomeStatus || job?.applicationStatus);
  const phaseBucket = phaseBucketFor(pipelinePhase);
  const outcomeBucket = outcomeStatus === "active" ? "active" : "closed";
  const sourceBucket = sourceBucketFor(lifecycleOverlay);
  const reminder = reminderState(job?.reminderAt, { parseIsoDate, now: options.now });
  const hasNotes = Boolean(String(job?.notes || "").trim());
  const hasAttachments = Number(job?.attachmentsCount || 0) > 0;
  const missingLink = !String(job?.jobLink || "").trim();
  const custom = isCustomJob(job);
  const sourceNeedsAction = outcomeStatus === "active" && (
    sourceBucket === "likely_removed" || sourceBucket === "archived"
  );
  const needsActionReasons = [
    reminder.isOverdue ? "reminder_overdue" : "",
    !reminder.isOverdue && reminder.isDueSoon ? "reminder_due_soon" : "",
    sourceNeedsAction ? `source_${sourceBucket}` : ""
  ].filter(Boolean);
  const attentionReasons = needsActionReasons.map(attentionReason);
  const primaryAttentionReason = attentionReasons[0] || null;
  const needsAction = attentionReasons.length > 0;
  const stageIndex = outcomeStatus === "active"
    ? PIPELINE_PHASES.indexOf(pipelinePhase)
    : PIPELINE_PHASES.length + Math.max(0, OUTCOME_STATUSES.indexOf(outcomeStatus));
  const activeAt = String(
    job?.lastActivityAt
      || job?.trackingUpdatedAt
      || job?.notesUpdatedAt
      || job?.contentUpdatedAt
      || job?.updatedAt
      || job?.savedAt
      || ""
  );
  const phaseEnteredAt = phaseEnteredAtFor(job, pipelinePhase);

  return {
    job,
    jobKey: String(job?.jobKey || ""),
    pipelinePhase,
    outcomeStatus,
    phaseBucket,
    outcomeBucket,
    sourceBucket,
    needsAction,
    needsActionReasons,
    attentionReasons,
    primaryAttentionReason,
    sourceNeedsAction,
    activeAt,
    phaseEnteredAt,
    hasNotes,
    hasAttachments,
    missingLink,
    isCustom: custom,
    isImported: !custom,
    reminder,
    lifecycleOverlay,
    sortKeys: {
      activeAt,
      savedAt: String(job?.savedAt || ""),
      updatedAt: String(job?.updatedAt || ""),
      reminderWeight: reminder.weight,
      reminderAtMs: parseTime(job?.reminderAt, parseIsoDate),
      customWeight: custom ? 0 : 1,
      stageIndex,
      title: String(job?.title || ""),
      jobKey: String(job?.jobKey || "")
    },
    allowedActions: {
      changeTracking: Boolean(options.currentUser),
      editNotes: Boolean(options.currentUser),
      uploadAttachments: Boolean(options.currentUser)
    }
  };
}

export function isValidSavedFilter(value) {
  return VALID_FILTERS.has(value);
}

export function isValidSavedSort(value) {
  return VALID_SORTS.has(value);
}

export function isValidSavedGroup(value) {
  return VALID_GROUPS.has(value);
}

export function normalizeSavedGroup(value) {
  const safeValue = String(value || "").trim().toLowerCase();
  return isValidSavedGroup(safeValue) ? safeValue : SAVED_GROUP_NONE;
}

function matchesFilter(view, filter) {
  if (filter === SAVED_FILTER_CUSTOM) return view.isCustom;
  if (filter === SAVED_FILTER_IMPORTED) return view.isImported;
  if (filter === SAVED_FILTER_NEEDS_ACTION) return view.needsAction;
  if (filter === SAVED_FILTER_APPLIED) return view.outcomeStatus === "active" && view.phaseBucket === "applied";
  if (filter === SAVED_FILTER_INTERVIEWING) return view.outcomeStatus === "active" && view.phaseBucket === "interviewing";
  if (filter === SAVED_FILTER_OFFER) return view.outcomeStatus === "active" && view.phaseBucket === "offer";
  if (filter === SAVED_FILTER_CLOSED) return view.outcomeStatus !== "active";
  if (filter === SAVED_FILTER_DUE_SOON) return view.reminder.isDueSoon;
  if (filter === SAVED_FILTER_NO_REMINDER) return !view.reminder.hasReminder;
  if (filter === SAVED_FILTER_HAS_NOTES) return view.hasNotes;
  if (filter === SAVED_FILTER_HAS_ATTACHMENTS) return view.hasAttachments;
  if (filter === SAVED_FILTER_MISSING_LINK) return view.missingLink;
  if (filter === SAVED_FILTER_LIKELY_REMOVED) return view.sourceBucket === "likely_removed" || view.sourceBucket === "archived";
  return true;
}

export function filterSavedJobViews(views, filter) {
  const rows = Array.isArray(views) ? views : [];
  return rows.filter(view => matchesFilter(view, filter));
}

export function sortSavedJobViews(views, mode) {
  const rows = Array.isArray(views) ? [...views] : [];
  const byKey = (a, b) => a.sortKeys.jobKey.localeCompare(b.sortKeys.jobKey);
  const byTitle = (a, b) => a.sortKeys.title.localeCompare(b.sortKeys.title);
  const bySaved = (a, b) => b.sortKeys.savedAt.localeCompare(a.sortKeys.savedAt);
  const byActive = (a, b) => b.sortKeys.activeAt.localeCompare(a.sortKeys.activeAt);
  const byUpdated = (a, b) => b.sortKeys.updatedAt.localeCompare(a.sortKeys.updatedAt);

  if (mode === SORT_SAVED) {
    return rows.sort((a, b) => bySaved(a, b) || byTitle(a, b) || byKey(a, b));
  }
  if (mode === SORT_PERSONAL) {
    return rows.sort((a, b) => (a.sortKeys.customWeight - b.sortKeys.customWeight) || byActive(a, b) || byTitle(a, b) || byKey(a, b));
  }
  if (mode === SORT_REMINDER) {
    return rows.sort((a, b) => (a.sortKeys.reminderWeight - b.sortKeys.reminderWeight)
      || (a.sortKeys.reminderAtMs - b.sortKeys.reminderAtMs)
      || byActive(a, b)
      || byTitle(a, b)
      || byKey(a, b));
  }
  if (mode === SORT_STAGE) {
    return rows.sort((a, b) => (a.sortKeys.stageIndex - b.sortKeys.stageIndex) || byActive(a, b) || byTitle(a, b) || byKey(a, b));
  }
  if (mode === SORT_ACTIVITY) {
    return rows.sort((a, b) => byActive(a, b) || byTitle(a, b) || byKey(a, b));
  }
  return rows.sort((a, b) => byActive(a, b) || byUpdated(a, b) || byTitle(a, b) || byKey(a, b));
}

function stageGroupForView(view) {
  if (view?.outcomeStatus && view.outcomeStatus !== "active") {
    return TERMINAL_STAGE_GROUPS.find(group => group.outcomeStatus === view.outcomeStatus)
      || TERMINAL_STAGE_GROUPS.find(group => group.outcomeStatus === "closed");
  }
  return ACTIVE_STAGE_GROUPS.find(group => group.phaseBucket === view?.phaseBucket)
    || ACTIVE_STAGE_GROUPS[0];
}

export function groupSavedJobViews(views, mode = SAVED_GROUP_NONE) {
  const rows = Array.isArray(views) ? views : [];
  const safeMode = normalizeSavedGroup(mode);
  if (safeMode !== SAVED_GROUP_STAGE) {
    return [{
      key: SAVED_GROUP_NONE,
      label: "",
      count: rows.length,
      views: rows
    }];
  }

  const groupsByKey = new Map(STAGE_GROUPS.map(group => [
    group.key,
    {
      key: group.key,
      label: group.label,
      count: 0,
      views: []
    }
  ]));

  rows.forEach(view => {
    const group = stageGroupForView(view);
    const bucket = groupsByKey.get(group.key);
    if (!bucket) return;
    bucket.views.push(view);
    bucket.count = bucket.views.length;
  });

  return Array.from(groupsByKey.values()).filter(group => group.views.length > 0);
}

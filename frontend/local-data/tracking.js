export const PIPELINE_PHASES = [
  "bookmark",
  "applied",
  "screening",
  "assignment",
  "interview_1",
  "interview_2",
  "final",
  "offer"
];

export const OUTCOME_STATUSES = [
  "active",
  "rejected",
  "withdrawn",
  "ghosted",
  "closed",
  "accepted"
];

export const TERMINAL_OUTCOME_STATUSES = OUTCOME_STATUSES.filter(status => status !== "active");

export const COMPAT_APPLICATION_STATUSES = [
  "bookmark",
  "applied",
  "interview_1",
  "interview_2",
  "offer",
  "rejected"
];

export const PIPELINE_PHASE_LABELS = {
  bookmark: "Saved",
  applied: "Applied",
  screening: "Screening",
  assignment: "Assignment",
  interview_1: "Interview 1",
  interview_2: "Interview 2",
  final: "Final Round",
  offer: "Offer"
};

export const OUTCOME_STATUS_LABELS = {
  active: "Active",
  rejected: "Rejected",
  withdrawn: "Withdrawn",
  ghosted: "Ghosted",
  closed: "Closed",
  accepted: "Accepted"
};

function normalizeTrackingToken(value) {
  return String(value || "").toLowerCase().trim();
}

export function normalizePipelinePhase(value) {
  const raw = normalizeTrackingToken(value);
  if (raw === "bookmarked" || raw === "saved") return "bookmark";
  if (raw === "recruiter" || raw === "recruiter_call" || raw === "phone_screen") return "screening";
  if (raw === "take_home" || raw === "technical_test" || raw === "art_test") return "assignment";
  if (raw === "final_round" || raw === "final_interview") return "final";
  return PIPELINE_PHASES.includes(raw) ? raw : "bookmark";
}

export function normalizeOutcomeStatus(value) {
  const raw = normalizeTrackingToken(value);
  if (raw === "no_response" || raw === "no-response") return "ghosted";
  return OUTCOME_STATUSES.includes(raw) ? raw : "active";
}

export function isTerminalOutcome(value) {
  return normalizeOutcomeStatus(value) !== "active";
}

function normalizeTimestampMap(value, allowedKeys) {
  const source = value && typeof value === "object" ? value : {};
  const normalized = {};
  Object.entries(source).forEach(([key, timestamp]) => {
    const safeKey = allowedKeys.includes(key) ? key : normalizePipelinePhase(key);
    if (!allowedKeys.includes(safeKey)) return;
    const text = String(timestamp || "").trim();
    if (text) normalized[safeKey] = text;
  });
  return normalized;
}

export function normalizePhaseTimestamps(value, { savedAt = "" } = {}) {
  const timestamps = normalizeTimestampMap(value, PIPELINE_PHASES);
  if (!timestamps.bookmark && savedAt) {
    timestamps.bookmark = String(savedAt);
  }
  return timestamps;
}

export function normalizeOutcomeTimestamps(value) {
  return normalizeTimestampMap(value, TERMINAL_OUTCOME_STATUSES);
}

export function bestPipelinePhaseFromTimestamps(phaseTimestamps, fallback = "applied") {
  const timestamps = phaseTimestamps && typeof phaseTimestamps === "object" ? phaseTimestamps : {};
  for (let idx = PIPELINE_PHASES.length - 1; idx >= 0; idx -= 1) {
    const phase = PIPELINE_PHASES[idx];
    if (phase !== "bookmark" && String(timestamps[phase] || "").trim()) {
      return phase;
    }
  }
  return normalizePipelinePhase(fallback) === "bookmark" ? "applied" : normalizePipelinePhase(fallback);
}

export function splitApplicationStatus(status, { phaseTimestamps = {}, fallbackPhase = "" } = {}) {
  const raw = normalizeTrackingToken(status);
  if (TERMINAL_OUTCOME_STATUSES.includes(raw)) {
    return {
      pipelinePhase: bestPipelinePhaseFromTimestamps(phaseTimestamps, fallbackPhase || "applied"),
      outcomeStatus: raw
    };
  }
  const pipelinePhase = normalizePipelinePhase(raw || fallbackPhase);
  return {
    pipelinePhase,
    outcomeStatus: "active"
  };
}

export function toApplicationStatusMirror(pipelinePhase, outcomeStatus) {
  const outcome = normalizeOutcomeStatus(outcomeStatus);
  if (outcome !== "active") return outcome;
  return normalizePipelinePhase(pipelinePhase);
}

export function normalizeApplicationStatusMirror(status) {
  const raw = normalizeTrackingToken(status);
  if (raw === "bookmarked") return "bookmark";
  if (TERMINAL_OUTCOME_STATUSES.includes(raw)) return raw;
  if (PIPELINE_PHASES.includes(normalizePipelinePhase(raw))) return normalizePipelinePhase(raw);
  return "bookmark";
}

export function canTransitionPipelinePhase(currentPhase, nextPhase, outcomeStatus = "active") {
  if (isTerminalOutcome(outcomeStatus)) return false;
  const current = normalizePipelinePhase(currentPhase);
  const next = normalizePipelinePhase(nextPhase);
  if (current === next) return true;
  const currentIdx = PIPELINE_PHASES.indexOf(current);
  const nextIdx = PIPELINE_PHASES.indexOf(next);
  return currentIdx >= 0 && nextIdx === currentIdx + 1;
}

export function canSetOutcomeStatus(currentOutcome, nextOutcome, { override = false } = {}) {
  const current = normalizeOutcomeStatus(currentOutcome);
  const next = normalizeOutcomeStatus(nextOutcome);
  if (current === next) return true;
  if (current !== "active" && !override) return false;
  return true;
}

export function canTransitionPhase(currentStatus, nextStatus) {
  const current = splitApplicationStatus(currentStatus);
  const next = splitApplicationStatus(nextStatus);
  const currentMirror = toApplicationStatusMirror(current.pipelinePhase, current.outcomeStatus);
  const nextMirror = toApplicationStatusMirror(next.pipelinePhase, next.outcomeStatus);
  if (currentMirror === nextMirror) return true;
  if (next.outcomeStatus !== "active") {
    return canSetOutcomeStatus(current.outcomeStatus, next.outcomeStatus);
  }
  return canTransitionPipelinePhase(current.pipelinePhase, next.pipelinePhase, current.outcomeStatus);
}

export function normalizeTrackingFields(source = {}, base = {}, options = {}) {
  const savedAt = String(options.savedAt || source.savedAt || base.savedAt || "").trim();
  const nowIso = typeof options.nowIso === "function" ? options.nowIso : () => new Date().toISOString();
  const normalizeIsoOrNow = typeof options.normalizeIsoOrNow === "function"
    ? options.normalizeIsoOrNow
    : (value, fallback = "") => String(value || fallback || "").trim();
  const currentIso = nowIso();
  const phaseTimestamps = normalizePhaseTimestamps({
    ...(base.phaseTimestamps && typeof base.phaseTimestamps === "object" ? base.phaseTimestamps : {}),
    ...(source.phaseTimestamps && typeof source.phaseTimestamps === "object" ? source.phaseTimestamps : {})
  }, { savedAt });

  const hasSourceField = key => Object.prototype.hasOwnProperty.call(source, key)
    && String(source[key] ?? "").trim();
  const sourcePhase = hasSourceField("pipelinePhase");
  const sourceOutcome = hasSourceField("outcomeStatus");
  const sourceApplicationStatus = hasSourceField("applicationStatus");
  const basePhase = String(base.pipelinePhase ?? "").trim();
  const baseOutcome = String(base.outcomeStatus ?? "").trim();
  const legacySplit = splitApplicationStatus(sourceApplicationStatus || base.applicationStatus, {
    phaseTimestamps,
    fallbackPhase: sourcePhase || basePhase
  });
  let pipelinePhase = legacySplit.pipelinePhase;
  if (sourcePhase) {
    pipelinePhase = normalizePipelinePhase(sourcePhase);
  } else if (!sourceApplicationStatus && basePhase) {
    pipelinePhase = normalizePipelinePhase(basePhase);
  }
  let outcomeStatus = legacySplit.outcomeStatus;
  if (sourceOutcome) {
    outcomeStatus = normalizeOutcomeStatus(sourceOutcome);
  } else if (!sourceApplicationStatus && baseOutcome) {
    outcomeStatus = normalizeOutcomeStatus(baseOutcome);
  }
  if (outcomeStatus !== "active" && pipelinePhase === "bookmark") {
    pipelinePhase = bestPipelinePhaseFromTimestamps(phaseTimestamps, "applied");
  }

  const outcomeTimestamps = {
    ...normalizeOutcomeTimestamps(base.outcomeTimestamps),
    ...normalizeOutcomeTimestamps(source.outcomeTimestamps)
  };
  if (outcomeStatus !== "active" && !outcomeTimestamps[outcomeStatus]) {
    outcomeTimestamps[outcomeStatus] = normalizeIsoOrNow(
      source.outcomeUpdatedAt || base.outcomeUpdatedAt || source.updatedAt || base.updatedAt,
      currentIso
    );
  }

  const fallbackUpdatedAt = source.updatedAt ?? base.updatedAt ?? savedAt;
  return {
    pipelinePhase,
    outcomeStatus,
    applicationStatus: toApplicationStatusMirror(pipelinePhase, outcomeStatus),
    phaseTimestamps,
    outcomeTimestamps,
    contentUpdatedAt: normalizeIsoOrNow(source.contentUpdatedAt ?? base.contentUpdatedAt ?? fallbackUpdatedAt, savedAt || currentIso),
    trackingUpdatedAt: normalizeIsoOrNow(source.trackingUpdatedAt ?? base.trackingUpdatedAt ?? fallbackUpdatedAt, savedAt || currentIso),
    notesUpdatedAt: normalizeIsoOrNow(source.notesUpdatedAt ?? base.notesUpdatedAt, ""),
    lastActivityAt: normalizeIsoOrNow(source.lastActivityAt ?? base.lastActivityAt ?? fallbackUpdatedAt, savedAt || currentIso)
  };
}

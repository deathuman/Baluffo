import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js";
import {
  OUTCOME_STATUSES,
  OUTCOME_STATUS_LABELS,
  PIPELINE_PHASE_LABELS,
  PIPELINE_PHASES,
  isTerminalOutcome,
  normalizeOutcomeStatus,
  normalizePipelinePhase
} from "../../local-data/tracking.js";

function formatTrackingTimestamp(value, options = {}) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "";
  if (options.compact) {
    return parsed.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit"
    });
  }
  return parsed.toLocaleString();
}

function resolveNowMs(value) {
  if (typeof value === "function") return Number(value()) || Date.now();
  if (value instanceof Date) return value.getTime();
  return Number(value) || Date.now();
}

function formatRelativeTimestamp(value, options = {}) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "";
  const deltaMin = Math.round((resolveNowMs(options.now) - parsed.getTime()) / 60000);
  if (deltaMin < 1) return "just now";
  if (deltaMin < 60) return `${deltaMin}m ago`;
  const deltaHours = Math.round(deltaMin / 60);
  if (deltaHours < 24) return `${deltaHours}h ago`;
  const deltaDays = Math.round(deltaHours / 24);
  if (deltaDays < 8) return `${deltaDays}d ago`;
  return parsed.toLocaleDateString();
}

function activeTimestampFor(jobView) {
  const job = jobView?.job || {};
  return String(
    jobView?.activeAt
      || jobView?.sortKeys?.activeAt
      || job?.lastActivityAt
      || job?.trackingUpdatedAt
      || job?.notesUpdatedAt
      || job?.contentUpdatedAt
      || job?.updatedAt
      || job?.savedAt
      || ""
  );
}

function normalizeAttentionReason(reason) {
  if (reason && typeof reason === "object") {
    return {
      key: String(reason.key || "").trim(),
      label: String(reason.label || reason.key || "").trim()
    };
  }
  const key = String(reason || "").trim();
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

function attentionReasonsFor(jobView) {
  const richReasons = Array.isArray(jobView?.attentionReasons)
    ? jobView.attentionReasons
    : [];
  const rawReasons = richReasons.length > 0
    ? richReasons
    : Array.isArray(jobView?.needsActionReasons)
      ? jobView.needsActionReasons
      : [];
  return rawReasons
    .map(normalizeAttentionReason)
    .filter(reason => reason.key && reason.label);
}

function renderAttentionChip(jobView) {
  const reasons = attentionReasonsFor(jobView);
  const primary = normalizeAttentionReason(jobView?.primaryAttentionReason || reasons[0]);
  if (!primary.key || !primary.label) return "";
  const allLabels = reasons.map(reason => reason.label).filter(Boolean);
  const tooltip = allLabels.length > 1
    ? `Needs action: ${allLabels.join("; ")}`
    : `Needs action: ${primary.label}`;
  return `
    <span class="tracking-attention-chip" data-attention-reason="${escapeHtml(primary.key)}"${tooltipAttrs(tooltip)}>
      <span class="tracking-attention-dot" aria-hidden="true"></span>
      <span>Needs action:</span>
      <strong>${escapeHtml(primary.label)}</strong>
    </span>
  `;
}

function isContextFor(context, jobKey, kind, value) {
  if (!context) return false;
  if (String(context.jobKey || "") !== String(jobKey || "")) return false;
  if (String(context.kind || "phase") !== kind) return false;
  if (kind === "outcome") {
    return String(context.outcomeStatus || "") === String(value || "");
  }
  return String(context.phase || "") === String(value || "");
}

function getPhaseTimestamp(timestamps, phase, savedAt = "") {
  const normalizedPhase = normalizePipelinePhase(phase);
  const fallback = normalizedPhase === "bookmark" ? savedAt : "";
  return timestamps?.[normalizedPhase] || fallback;
}

function buildPhaseStepAriaLabel(label, status, timestampLabel) {
  if (status === "current") {
    return timestampLabel
      ? `${label}, current phase, entered ${timestampLabel}`
      : `${label}, current phase`;
  }
  if (status === "completed") {
    return timestampLabel
      ? `${label}, completed ${timestampLabel}`
      : `${label}, completed`;
  }
  return `${label}, not reached`;
}

function renderPhaseActionButton({
  jobKey,
  phase,
  currentPhase,
  currentOutcome,
  label,
  classes,
  currentUser,
  tooltip = ""
}) {
  const normalizedPhase = normalizePipelinePhase(phase);
  const safeJobKey = escapeHtml(String(jobKey || ""));
  return `
    <button
      type="button"
      class="${classes}"
      data-job-key="${safeJobKey}"
      data-ui="phase-step-btn"
      data-phase="${escapeHtml(normalizedPhase)}"
      data-current-phase="${escapeHtml(currentPhase)}"
      data-current-outcome="${escapeHtml(currentOutcome)}"
      ${currentUser ? "" : "disabled"}
      aria-label="Set phase to ${escapeHtml(label)}"
      ${tooltipAttrs(tooltip)}
    >
      ${escapeHtml(label)}
    </button>
  `;
}

function renderOverrideContext(jobKey, context, options = {}) {
  const safeJobKey = escapeHtml(String(jobKey || ""));
  const kind = String(context?.kind || "phase");
  const phase = String(context?.phase || "");
  const outcomeStatus = String(context?.outcomeStatus || "");
  const reason = kind === "outcome"
    ? "This outcome change is normally locked because the job already has a terminal outcome."
    : isTerminalOutcome(context?.fromOutcome)
      ? "This phase change is normally locked because the job has a terminal outcome."
      : "This phase change is normally locked because it skips or rewinds an application step.";
  const label = kind === "outcome" ? "Override outcome" : "Override phase";
  const disabled = !options.currentUser ? "disabled" : "";
  return `
    <div
      class="phase-override-context tracking-override-context"
      data-job-key="${safeJobKey}"
      data-tracking-kind="${escapeHtml(kind)}"
      data-phase="${escapeHtml(phase)}"
      data-outcome-status="${escapeHtml(outcomeStatus)}"
    >
      <span>${escapeHtml(reason)}</span>
      <button
        class="btn back-btn tracking-override-confirm-btn"
        data-ui="tracking-override-confirm-btn"
        data-job-key="${safeJobKey}"
        data-tracking-kind="${escapeHtml(kind)}"
        data-phase="${escapeHtml(phase)}"
        data-outcome-status="${escapeHtml(outcomeStatus)}"
        ${disabled}
      >${escapeHtml(label)}</button>
      <button
        class="btn back-btn tracking-override-cancel-btn"
        data-ui="tracking-override-cancel-btn"
        data-job-key="${safeJobKey}"
      >Cancel</button>
    </div>
  `;
}

export function renderPhaseBar(jobKey, activePhase, phaseTimestamps, savedAt, options = {}) {
  const {
    phaseOptions = PIPELINE_PHASES,
    phaseLabels = PIPELINE_PHASE_LABELS,
    canTransition = () => false,
    currentUser = null,
    trackingOverrideContext = null,
    outcomeStatus = "active"
  } = options;
  const normalizedPhase = normalizePipelinePhase(activePhase);
  const normalizedOutcome = normalizeOutcomeStatus(outcomeStatus);
  const activeIndex = phaseOptions.indexOf(normalizedPhase);
  const safeJobKey = escapeHtml(String(jobKey || ""));
  const hasContext = isContextFor(trackingOverrideContext, jobKey, "phase", trackingOverrideContext?.phase);
  const timestamps = phaseTimestamps && typeof phaseTimestamps === "object" ? phaseTimestamps : {};
  const lockedByOutcome = isTerminalOutcome(normalizedOutcome);
  const progressRatio = phaseOptions.length > 1 && activeIndex > 0
    ? Math.min(1, activeIndex / (phaseOptions.length - 1))
    : 0;
  const segments = phaseOptions.map((phase, idx) => {
    const normalizedOption = normalizePipelinePhase(phase);
    const isActive = normalizedOption === normalizedPhase;
    const isComplete = activeIndex >= 0 && idx < activeIndex;
    const canChangeNormally = canTransition(normalizedPhase, normalizedOption, normalizedOutcome);
    const canClick = Boolean(currentUser);
    const selectedTimestamp = getPhaseTimestamp(timestamps, normalizedOption, savedAt);
    const selectedAtFull = formatTrackingTimestamp(selectedTimestamp);
    const status = isActive ? "current" : isComplete ? "completed" : "future";
    const classes = [
      "phase-step-btn",
      "phase-timeline-step",
      isActive ? "active" : "",
      isComplete ? "complete" : "",
      !canChangeNormally ? "locked" : ""
    ].filter(Boolean).join(" ");
    const label = phaseLabels[normalizedOption] || normalizedOption;
    const tooltip = !canClick
      ? "Sign in to change application phase."
      : lockedByOutcome
        ? [
          selectedAtFull ? `${label} ${status === "current" ? "entered" : "completed"} ${selectedAtFull}.` : "",
          "Set the outcome back to Active before changing phase."
        ].filter(Boolean).join(" ")
        : selectedAtFull
          ? `${label} ${status === "current" ? "entered" : "completed"} ${selectedAtFull}.`
          : "";
    const ariaLabel = buildPhaseStepAriaLabel(label, status, selectedAtFull);

    return `
      <button
        class="${classes}"
        data-job-key="${safeJobKey}"
        data-ui="phase-step-btn"
        data-phase="${escapeHtml(normalizedOption)}"
        data-phase-status="${status}"
        ${selectedAtFull ? `data-phase-time="${escapeHtml(selectedAtFull)}"` : ""}
        data-current-phase="${escapeHtml(normalizedPhase)}"
        data-current-outcome="${escapeHtml(normalizedOutcome)}"
        ${canClick ? "" : "disabled"}
        ${isActive ? 'aria-current="step"' : ""}
        aria-label="${escapeHtml(ariaLabel)}"
        ${tooltipAttrs(tooltip)}
      >
        <span class="phase-step-node" aria-hidden="true">
          ${isComplete && !isActive ? '<span class="phase-step-check">✓</span>' : ""}
        </span>
        <span class="phase-step-text">${escapeHtml(label)}</span>
      </button>
    `;
  }).join("");
  const overrideMessage = hasContext
    ? renderOverrideContext(jobKey, trackingOverrideContext, { currentUser })
    : "";
  return `<div class="phase-bar" role="group" aria-label="Application phases" style="--phase-step-count: ${phaseOptions.length}; --phase-progress-ratio: ${progressRatio.toFixed(4)};">${segments}</div>${overrideMessage}`;
}

function renderPhaseActions(jobKey, activePhase, phaseTimestamps, savedAt, options = {}) {
  const {
    phaseOptions = PIPELINE_PHASES,
    phaseLabels = PIPELINE_PHASE_LABELS,
    canTransition = () => false,
    currentUser = null,
    outcomeStatus = "active",
    jobView = null,
    now = Date.now
  } = options;
  const normalizedPhase = normalizePipelinePhase(activePhase);
  const normalizedOutcome = normalizeOutcomeStatus(outcomeStatus);
  const activeIndex = phaseOptions.indexOf(normalizedPhase);
  const timestamps = phaseTimestamps && typeof phaseTimestamps === "object" ? phaseTimestamps : {};
  const currentLabel = phaseLabels[normalizedPhase] || normalizedPhase;
  const currentTimestamp = String(jobView?.phaseEnteredAt || "")
    || getPhaseTimestamp(timestamps, normalizedPhase, savedAt);
  const enteredAt = formatTrackingTimestamp(currentTimestamp, { compact: true }) || "Not recorded";
  const enteredAtFull = formatTrackingTimestamp(currentTimestamp);
  const activeAt = activeTimestampFor(jobView);
  const lastActivity = formatRelativeTimestamp(activeAt, { now }) || "Not recorded";
  const lastActivityFull = formatTrackingTimestamp(activeAt);
  const summaryTooltip = [
    enteredAtFull ? `Entered ${enteredAtFull}` : "",
    lastActivityFull ? `Last activity ${lastActivityFull}` : ""
  ].filter(Boolean).join(" · ");
  const attentionChip = renderAttentionChip(jobView);
  const isFinalPhase = activeIndex >= 0 && activeIndex === phaseOptions.length - 1;
  const finalIndicator = isFinalPhase
    ? `<span class="tracking-final-indicator">${isTerminalOutcome(normalizedOutcome) ? "Final stage" : "Awaiting outcome"}</span>`
    : "";
  const nextPhase = activeIndex >= 0 && phaseOptions[activeIndex + 1]
    ? normalizePipelinePhase(phaseOptions[activeIndex + 1])
    : "";
  const canMoveNext = Boolean(
    nextPhase &&
    currentUser &&
    !isTerminalOutcome(normalizedOutcome) &&
    canTransition(normalizedPhase, nextPhase, normalizedOutcome)
  );
  const nextLabel = nextPhase ? phaseLabels[nextPhase] || nextPhase : "";
  const moveButton = canMoveNext
    ? renderPhaseActionButton({
      jobKey,
      phase: nextPhase,
      currentPhase: normalizedPhase,
      currentOutcome: normalizedOutcome,
      label: `Move to ${nextLabel}`,
      classes: "btn back-btn phase-next-btn",
      currentUser
    })
    : "";
  const menuItems = phaseOptions
    .map(option => normalizePipelinePhase(option))
    .filter((option, index, optionsList) => option !== normalizedPhase && optionsList.indexOf(option) === index)
    .map(option => {
      const label = phaseLabels[option] || option;
      const canChangeNormally = canTransition(normalizedPhase, option, normalizedOutcome);
      const classes = [
        "phase-step-btn",
        "phase-menu-item",
        !canChangeNormally ? "locked" : ""
      ].filter(Boolean).join(" ");
      const tooltip = isTerminalOutcome(normalizedOutcome)
        ? "This phase change requires an override because the job has a final outcome."
        : canChangeNormally
          ? ""
          : "This phase change requires an override.";
      return renderPhaseActionButton({
        jobKey,
        phase: option,
        currentPhase: normalizedPhase,
        currentOutcome: normalizedOutcome,
        label,
        classes,
        currentUser,
        tooltip
      });
    }).join("");
  const changeMenu = currentUser && menuItems
    ? `
      <details class="phase-change-menu">
        <summary class="btn back-btn phase-change-toggle" aria-label="Change phase">
          Change phase
        </summary>
        <div class="phase-change-popover" role="menu" aria-label="Phase options">
          ${menuItems}
        </div>
      </details>
    `
    : `
      <button class="btn back-btn phase-change-toggle" type="button" disabled${tooltipAttrs("Sign in to change phase.")}>
        Change phase
      </button>
    `;
  return `
    <div class="tracking-phase-summary"${summaryTooltip ? tooltipAttrs(summaryTooltip) : ""}>
      <span class="tracking-current-line">
        <span>Current phase:</span>
        <strong>${escapeHtml(currentLabel)}</strong>
        <span class="tracking-meta-separator" aria-hidden="true">&middot;</span>
        <span>Entered:</span>
        <strong>${escapeHtml(enteredAt)}</strong>
        <span class="tracking-meta-separator" aria-hidden="true">&middot;</span>
        <span>Last activity:</span>
        <strong class="tracking-last-activity">${escapeHtml(lastActivity)}</strong>
        ${finalIndicator}
        ${attentionChip}
      </span>
    </div>
    <div class="tracking-action-controls">
      ${moveButton}
      ${changeMenu}
    </div>
  `;
}

export function renderOutcomeControls(jobKey, outcomeStatus, outcomeTimestamps, options = {}) {
  const {
    outcomeOptions = OUTCOME_STATUSES,
    outcomeLabels = OUTCOME_STATUS_LABELS,
    canSetOutcome = () => false,
    currentUser = null,
    trackingOverrideContext = null
  } = options;
  const normalizedOutcome = normalizeOutcomeStatus(outcomeStatus);
  const safeJobKey = escapeHtml(String(jobKey || ""));
  const timestamps = outcomeTimestamps && typeof outcomeTimestamps === "object" ? outcomeTimestamps : {};
  const terminalOutcome = isTerminalOutcome(normalizedOutcome);
  const outcomeLabel = outcomeLabels[normalizedOutcome] || normalizedOutcome;
  const selectedTimestamp = terminalOutcome ? timestamps[normalizedOutcome] : "";
  const selectedAt = terminalOutcome
    ? formatTrackingTimestamp(selectedTimestamp, { compact: true })
    : "";
  const selectedAtFull = terminalOutcome ? formatTrackingTimestamp(selectedTimestamp) : "";
  const menuLabel = terminalOutcome ? "Change outcome" : "Set final outcome";
  const statusClasses = [
    "outcome-status-chip",
    terminalOutcome ? "terminal" : "active"
  ].join(" ");
  const normalizedOptions = outcomeOptions
    .map(option => normalizeOutcomeStatus(option))
    .filter((option, index, optionsList) => optionsList.indexOf(option) === index);
  const terminalOptions = normalizedOptions.filter(option => option !== "active" && option !== normalizedOutcome);
  const menuOptions = terminalOutcome
    ? ["active", ...terminalOptions]
    : terminalOptions;
  const hasContext = isContextFor(
    trackingOverrideContext,
    jobKey,
    "outcome",
    trackingOverrideContext?.outcomeStatus
  );
  const menuItems = menuOptions.map(normalizedOption => {
    const canChangeNormally = canSetOutcome(normalizedOutcome, normalizedOption);
    const classes = [
      "outcome-status-btn",
      "outcome-menu-item",
      normalizedOption === "active" ? "reopen" : "",
      normalizedOption !== "active" ? "terminal" : "",
      !canChangeNormally ? "locked" : ""
    ].filter(Boolean).join(" ");
    const label = outcomeLabels[normalizedOption] || normalizedOption;
    const actionLabel = normalizedOption === "active" ? "Reopen as Active" : label;
    return `
      <button
        type="button"
        class="${classes}"
        data-job-key="${safeJobKey}"
        data-ui="outcome-status-btn"
        data-outcome-status="${escapeHtml(normalizedOption)}"
        data-current-outcome="${escapeHtml(normalizedOutcome)}"
        role="menuitem"
        aria-label="${escapeHtml(actionLabel)}"
      >
        <span class="phase-step-text">${escapeHtml(actionLabel)}</span>
      </button>
    `;
  }).join("");
  const menuControl = currentUser && menuItems
    ? `
      <details class="outcome-menu">
        <summary class="btn back-btn outcome-menu-toggle" aria-label="${escapeHtml(menuLabel)}">
          ${escapeHtml(menuLabel)}
        </summary>
        <div class="outcome-menu-popover" role="menu" aria-label="Final outcome options">
          ${menuItems}
        </div>
      </details>
    `
    : `
      <button class="btn back-btn outcome-menu-toggle" type="button" disabled${tooltipAttrs("Sign in to change outcome.")}>
        ${escapeHtml(menuLabel)}
      </button>
    `;
  const overrideMessage = hasContext
    ? renderOverrideContext(jobKey, trackingOverrideContext, { currentUser })
    : "";
  return `
    <div class="outcome-compact" role="group" aria-label="Final outcome">
      <span class="${statusClasses}"${selectedAtFull ? tooltipAttrs(`${outcomeLabel} since ${selectedAtFull}`) : ""}>
        <span class="outcome-status-dot" aria-hidden="true"></span>
        <span class="outcome-status-label">${escapeHtml(outcomeLabel)}</span>
        ${selectedAt ? `<span class="outcome-status-time">${escapeHtml(selectedAt)}</span>` : ""}
      </span>
      ${menuControl}
    </div>
    ${overrideMessage}
  `;
}

export function renderApplicationTrackingControls(jobView, options = {}) {
  const job = jobView?.job || jobView || {};
  const jobKey = String(jobView?.jobKey || job?.jobKey || job?.id || "");
  const pipelinePhase = normalizePipelinePhase(jobView?.pipelinePhase || job?.pipelinePhase || job?.applicationStatus);
  const outcomeStatus = normalizeOutcomeStatus(jobView?.outcomeStatus || job?.outcomeStatus || job?.applicationStatus);
  const phaseTimestamps = jobView?.phaseTimestamps || job?.phaseTimestamps || {};
  const outcomeTimestamps = jobView?.outcomeTimestamps || job?.outcomeTimestamps || {};
  const savedAt = String(jobView?.savedAt || job?.savedAt || "");
  const renderPhase = options.renderPhaseBar || renderPhaseBar;
  const renderOutcome = options.renderOutcomeControls || renderOutcomeControls;
  const phaseActions = renderPhaseActions(jobKey, pipelinePhase, phaseTimestamps, savedAt, {
    ...options,
    outcomeStatus,
    jobView
  });
  return `
    <div class="saved-phase-row saved-tracking-phase-row">
      <div class="phase-label">Phase</div>
      <div class="phase-value">
        ${renderPhase(jobKey, pipelinePhase, phaseTimestamps, savedAt, {
          ...options,
          outcomeStatus
        })}
      </div>
    </div>
    <div class="saved-tracking-action-row">
      <div class="tracking-status-slot">
        ${renderOutcome(jobKey, outcomeStatus, outcomeTimestamps, options)}
      </div>
      ${phaseActions}
    </div>
  `;
}

import { clippedTooltipAttrs, escapeHtml, tooltipAttrs } from "../shared/ui/index.js";
import { renderLifecycleBadgeHtml } from "../shared/lifecycle-badges.js";
import { formatJobLocationColumns } from "../shared/location-display.js";
import {
  normalizeOutcomeStatus,
  normalizePipelinePhase
} from "../local-data/tracking.js";
import {
  renderApplicationTrackingControls as renderApplicationTrackingControlsFromModule,
  renderPhaseBar as renderPhaseBarFromTrackingUi
} from "./app/tracking-ui.js";

export function renderSavedJobBlockHtml(job, options = {}) {
  const {
    isCustomJob,
    customSourceLabel,
    normalizeSavedSector,
    fullCountryName,
    sanitizeUrl,
    toContractClass,
    normalizePhase,
    expandedJobKey,
    selectedJobKey,
    getJobDetailsTab,
    renderDetailsSummary,
    getReminderMeta,
    renderMissingInfoChips,
    renderUpdatedHint,
    getJobHistoryEntries,
    renderWebIcon,
    renderPhaseBar,
    renderApplicationTrackingControls = renderApplicationTrackingControlsFromModule,
    lifecycleOverlay,
    canManageAvailability = false,
    jobView,
    currentUser,
    maxAttachmentsPerJob,
    maxAttachmentBytes
  } = options;

  const isCustom = isCustomJob(job);
  const safeTitle = escapeHtml(job.title || "");
  const safeCompany = escapeHtml(job.company || "");
  const customSourceRaw = String(job.customSourceLabel || customSourceLabel || "Custom");
  const customSource = escapeHtml(customSourceRaw);
  const sectorLabel = normalizeSavedSector(job);
  const safeSector = escapeHtml(sectorLabel);
  const locationColumns = formatJobLocationColumns(job, { fullCountryName });
  const safeCity = escapeHtml(locationColumns.cityLabel);
  const safeCountry = escapeHtml(locationColumns.countryLabel);
  const safeContract = escapeHtml(job.contractType || "Unknown");
  const safeWorkType = escapeHtml(job.workType || "Onsite");
  const safeLink = sanitizeUrl(job.jobLink || "");
  const hasLink = Boolean(safeLink);
  const contractClass = toContractClass(job.contractType || "Unknown");
  const rawJobKey = String(job.jobKey || job.id || "");
  const jobKey = escapeHtml(rawJobKey);
  const normalizedPhase = normalizePipelinePhase(
    jobView?.pipelinePhase || job.pipelinePhase || normalizePhase(job.applicationStatus)
  );
  const normalizedOutcome = normalizeOutcomeStatus(
    jobView?.outcomeStatus || job.outcomeStatus || job.applicationStatus
  );
  const trackingJobView = {
    ...(jobView || {}),
    job,
    jobKey: rawJobKey,
    pipelinePhase: normalizedPhase,
    outcomeStatus: normalizedOutcome,
    phaseTimestamps: jobView?.phaseTimestamps || job.phaseTimestamps || {},
    outcomeTimestamps: jobView?.outcomeTimestamps || job.outcomeTimestamps || {},
    savedAt: jobView?.savedAt || job.savedAt || ""
  };
  const isExpanded = expandedJobKey === rawJobKey;
  const isSelected = selectedJobKey === rawJobKey;
  const activeTab = getJobDetailsTab(rawJobKey);
  const detailsSummary = renderDetailsSummary(job);
  const reminderMeta = getReminderMeta(job.reminderAt);
  const missingChips = renderMissingInfoChips(job);
  const updateHint = renderUpdatedHint(job);
  const historyRows = getJobHistoryEntries(rawJobKey);
  const tabClassNotes = activeTab === "notes" ? "active" : "";
  const tabClassAttachments = activeTab === "attachments" ? "active" : "";
  const tabClassHistory = activeTab === "history" ? "active" : "";
  const reminderBadge = reminderMeta.hasReminder
    ? `<span class="saved-reminder-badge ${escapeHtml(reminderMeta.badgeClass || "scheduled")}"${reminderMeta.label ? tooltipAttrs(reminderMeta.label) : ""}>${escapeHtml(reminderMeta.badgeLabel || "Reminder set")}</span>`
    : "";
  const lifecycleBadge = renderLifecycleBadgeHtml(lifecycleOverlay, { includeLastSeenAt: true });
  const availabilityEvents = Array.isArray(job?.availabilityAttention?.events)
    ? job.availabilityAttention.events
    : [];
  const unreadAvailability = availabilityEvents.filter(event => event?.alert && !event?.acknowledgedAt);
  const availabilityAttentionBadge = unreadAvailability.length
    ? `<button class="saved-availability-attention-btn" data-ui="saved-availability-attention-btn" data-job-key="${jobKey}"${tooltipAttrs("Availability update needs acknowledgement")}>Availability update</button>`
    : "";
  const availabilityStatus = String(lifecycleOverlay?.availabilityStatus || "").toLowerCase();
  const monitored = Boolean(String(job.availabilityId || "").trim()) && hasLink;

  return `
    <div class="saved-job-block ${isExpanded ? "expanded" : ""}" data-job-key="${jobKey}" data-ui="saved-job-block" data-selected="${isSelected ? "true" : "false"}">
      <div class="saved-job-row">
        <button class="remove-saved-btn remove-inline-btn" data-job-key="${jobKey}" data-ui="remove-saved-btn" aria-label="Remove saved job" ${tooltipAttrs("Remove saved job")}>${renderRemoveSavedIcon()}</button>
        <div class="col-title job-cell" data-label="Position">
          <div class="saved-title-stack">
            <div class="saved-title-line">
              <span class="saved-title-main"${clippedTooltipAttrs(job.title || "")}>${safeTitle}</span>
            </div>
            <div class="job-sector-line">${safeSector}</div>
            <div class="saved-title-meta">
              ${isCustom ? `<span class="saved-custom-badge"${tooltipAttrs("Custom job source")}>${customSource}</span>` : ""}
              ${reminderBadge}
              ${lifecycleBadge}
              ${availabilityAttentionBadge}
              ${isCustom && !monitored ? `<span class="saved-custom-badge"${tooltipAttrs("A public application URL is required for monitoring")}>Not monitored</span>` : ""}
              ${missingChips}
            </div>
            ${updateHint}
          </div>
          ${isCustom ? `
            <div class="saved-personal-actions">
              <button class="btn back-btn personal-edit-btn" data-job-key="${jobKey}" data-ui="personal-edit-btn" aria-label="Edit custom job">Edit</button>
              <button class="btn back-btn personal-duplicate-btn" data-job-key="${jobKey}" data-ui="personal-duplicate-btn" aria-label="Duplicate custom job">Duplicate</button>
            </div>
          ` : ""}
        </div>
        <div class="col-company job-cell" data-label="Company">
          <span class="job-company-compact">${safeCompany}</span>
        </div>
        <div class="col-location job-cell" data-label="Location">
          <div class="job-location-stack">
            <span class="job-country-main">${safeCountry}</span>
            <span class="job-city-sub">${safeCity}</span>
          </div>
        </div>
        <div class="col-contract job-cell" data-label="Contract">
          <span class="job-contract ${contractClass}">${safeContract}</span>
        </div>
        <div class="col-type job-cell" data-label="Type">
          <span class="job-tag ${safeWorkType.toLowerCase()}">${safeWorkType}</span>
        </div>
        <div class="col-link job-cell" data-label="Link">
          ${hasLink ? `<a class="saved-open-link-icon ${availabilityStatus === "unavailable" ? "availability-warning" : ""}" href="${safeLink}" target="_blank" rel="noopener noreferrer" aria-label="${availabilityStatus === "unavailable" ? "Open original link with warning" : "Open job link"}"${tooltipAttrs(availabilityStatus === "unavailable" ? "Confirmed unavailable; open the original link anyway" : "Open job link")}>${renderWebIcon()}</a>` : `<span class="saved-no-link ${isCustom ? "saved-no-link-custom" : ""}">${isCustom ? "No link" : "N/A"}</span>`}
          ${canManageAvailability && monitored ? `<button class="btn back-btn saved-check-availability-btn" data-ui="saved-check-availability-btn" data-availability-id="${escapeHtml(job.availabilityId)}">Check now</button>` : ""}
          ${canManageAvailability ? `<button class="btn back-btn saved-report-unavailable-btn" data-ui="saved-report-unavailable-btn" data-job-key="${jobKey}" data-action="${job?.availabilityAttention?.hiddenByReport ? "clear" : "report"}">${job?.availabilityAttention?.hiddenByReport ? "Clear unavailable report" : "Report unavailable"}</button>` : ""}
        </div>
      </div>
      ${renderApplicationTrackingControls(trackingJobView, {
        ...options,
        renderPhaseBar,
        currentUser
      })}
      <div class="saved-details-toggle-row">
        <div class="details-toggle-spacer"></div>
        <button
          class="details-toggle-btn"
          data-job-key="${jobKey}"
          data-ui="details-toggle-btn"
          aria-expanded="${isExpanded ? "true" : "false"}"
          aria-label="${isExpanded ? "Collapse" : "Expand"} notes and attachments"
        >
          <span class="details-toggle-icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" focusable="false">
              <path d="M7 3h7l4 4v14H7z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/>
              <path d="M14 3v5h5M9.5 12h5M9.5 16h5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
            </svg>
          </span>
          <span class="details-toggle-text">${detailsSummary}Notes, Files & History</span>
          <span class="details-toggle-arrow ${isExpanded ? "expanded" : ""}" aria-hidden="true"></span>
        </button>
      </div>
      <div class="saved-details-section ${isExpanded ? "" : "collapsed"}" data-job-key="${jobKey}" aria-hidden="${isExpanded ? "false" : "true"}">
        <div class="saved-details-tabs" role="tablist" aria-label="Saved job details tabs">
          <button class="saved-details-tab-btn ${tabClassNotes}" data-job-key="${jobKey}" data-ui="saved-details-tab-btn" data-details-tab="notes" role="tab" aria-selected="${activeTab === "notes" ? "true" : "false"}">Notes</button>
          <button class="saved-details-tab-btn ${tabClassAttachments}" data-job-key="${jobKey}" data-ui="saved-details-tab-btn" data-details-tab="attachments" role="tab" aria-selected="${activeTab === "attachments" ? "true" : "false"}">Attachments</button>
          <button class="saved-details-tab-btn ${tabClassHistory}" data-job-key="${jobKey}" data-ui="saved-details-tab-btn" data-details-tab="history" role="tab" aria-selected="${activeTab === "history" ? "true" : "false"}">History</button>
        </div>
        <div class="saved-details-panels">
          <div class="saved-notes-row saved-details-panel ${activeTab === "notes" ? "" : "hidden"}" data-tab-panel="notes">
            <div class="notes-label">Notes</div>
            <div class="notes-value">
              <textarea class="job-notes-input" data-job-key="${jobKey}" data-ui="job-notes-input" placeholder="Add notes, links, interview reminders..." ${!currentUser ? "disabled" : ""}>${escapeHtml(job.notes || "")}</textarea>
              <div class="note-save-state" data-job-key="${jobKey}">Saved</div>
            </div>
          </div>
          <div class="saved-attachments-row saved-details-panel ${activeTab === "attachments" ? "" : "hidden"}" data-tab-panel="attachments">
            <div class="attachments-label">Attachments</div>
            <div class="attachments-value">
              <div class="attachments-toolbar">
                <button class="btn back-btn attach-upload-btn" data-job-key="${jobKey}" data-ui="attach-upload-btn" ${!currentUser ? "disabled" : ""} ${!currentUser ? tooltipAttrs("Sign in to upload attachments.") : ""}>Upload</button>
                <span class="attachments-hint">Max ${maxAttachmentsPerJob} files, ${Math.round(maxAttachmentBytes / (1024 * 1024))}MB each</span>
              </div>
              <input class="attach-file-input hidden" type="file" multiple data-job-key="${jobKey}" accept=".pdf,.doc,.docx,.txt,.png,.jpg,.jpeg">
              <div class="attachments-list" data-job-key="${jobKey}">
                <div class="muted">No attachments yet.</div>
              </div>
            </div>
          </div>
          <div class="saved-history-row saved-details-panel ${activeTab === "history" ? "" : "hidden"}" data-tab-panel="history">
            <div class="attachments-label">History</div>
            <div class="attachments-value">
              <div class="job-history-toolbar">
                <button class="btn back-btn job-history-refresh-btn" data-job-key="${jobKey}" data-ui="job-history-refresh-btn">Refresh</button>
              </div>
              <div class="job-history-list">
                ${historyRows}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
}

export function renderActivityEntryHtml(entry, options = {}) {
  const { formatPhaseTimestamp, lastSavedJobsByKey, formatActivityDetail, activityTypeLabel } = options;
  const type = String(entry?.type || "event");
  const createdAt = formatPhaseTimestamp(entry?.createdAt) || "Unknown time";
  const key = String(entry?.jobKey || "");
  const snapshot = key ? lastSavedJobsByKey.get(key) : null;
  const title = escapeHtml(entry?.title || snapshot?.title || "(Untitled job)");
  const company = escapeHtml(entry?.company || snapshot?.company || "");
  const detailText = escapeHtml(formatActivityDetail(entry));
  const typeLabel = escapeHtml(activityTypeLabel(type));
  return `
    <div class="activity-entry">
      <div class="activity-entry-top">
        <span class="activity-type">${typeLabel}</span>
        <span class="activity-time">${escapeHtml(createdAt)}</span>
      </div>
      <div class="activity-entry-title">${title}</div>
      ${company ? `<div class="activity-entry-company">${company}</div>` : ""}
      <div class="activity-entry-detail">${detailText}</div>
    </div>
  `;
}

export function parseIsoDate(value) {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function resolveNowMs(value) {
  if (typeof value === "function") return Number(value()) || Date.now();
  if (value instanceof Date) return value.getTime();
  return Number(value) || Date.now();
}

export function getReminderMeta(reminderAt, options = {}) {
  const { reminderSoonHours = 72, now = Date.now } = options;
  const parsed = parseIsoDate(reminderAt);
  if (!parsed) {
    return {
      hasReminder: false,
      isSoon: false,
      isDueSoon: false,
      isOverdue: false,
      label: "",
      badgeLabel: "",
      badgeClass: ""
    };
  }
  const diffMs = parsed.getTime() - resolveNowMs(now);
  const soonMs = Number(reminderSoonHours) * 60 * 60 * 1000;
  const isOverdue = diffMs < 0;
  const isDueSoon = !isOverdue && diffMs <= soonMs;
  const isSoon = isOverdue || isDueSoon;
  return {
    hasReminder: true,
    isSoon,
    isDueSoon,
    isOverdue,
    label: parsed.toLocaleString(),
    badgeLabel: isOverdue ? "Overdue" : isDueSoon ? "Due soon" : "Reminder set",
    badgeClass: isOverdue ? "overdue" : isDueSoon ? "due-soon" : "scheduled"
  };
}

export function formatRelativeTime(value) {
  const parsed = parseIsoDate(value);
  if (!parsed) return "";
  const deltaMs = Date.now() - parsed.getTime();
  const deltaMin = Math.round(deltaMs / 60000);
  if (deltaMin < 1) return "just now";
  if (deltaMin < 60) return `${deltaMin}m ago`;
  const deltaHours = Math.round(deltaMin / 60);
  if (deltaHours < 24) return `${deltaHours}h ago`;
  const deltaDays = Math.round(deltaHours / 24);
  if (deltaDays < 8) return `${deltaDays}d ago`;
  return parsed.toLocaleDateString();
}

export function getJobHistoryEntries(jobKey, options = {}) {
  const {
    cachedActivityEntries = [],
    activityTypeLabel,
    formatPhaseTimestamp: formatPhaseTime,
    formatActivityDetail
  } = options;
  const key = String(jobKey || "");
  const rows = (cachedActivityEntries || [])
    .filter(entry => String(entry?.jobKey || "") === key)
    .slice(0, 12);
  if (rows.length === 0) {
    return '<div class="muted">No activity for this job yet.</div>';
  }
  return rows.map(entry => {
    const type = escapeHtml(activityTypeLabel(String(entry?.type || "event")));
    const time = escapeHtml(formatPhaseTime(entry?.createdAt) || "");
    const detail = escapeHtml(formatActivityDetail(entry));
    return `
      <div class="job-history-item">
        <div class="job-history-top"><span>${type}</span><span>${time}</span></div>
        <div class="job-history-detail">${detail}</div>
      </div>
    `;
  }).join("");
}

export function renderPhaseBar(jobKey, activePhase, phaseTimestamps, savedAt, options = {}) {
  return renderPhaseBarFromTrackingUi(jobKey, activePhase, phaseTimestamps, savedAt, {
    ...options,
    trackingOverrideContext: options.trackingOverrideContext || options.phaseOverrideContext || null
  });
}

export function renderRemoveSavedIcon() {
  return `
    <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true" focusable="false">
      <path fill="currentColor" d="M9 3h6l1 2h4v2H4V5h4l1-2Zm-2 6h10l-.7 11H7.7L7 9Zm3 2v7h1.6v-7H10Zm2.4 0v7H14v-7h-1.6Z"/>
    </svg>
  `;
}

export function renderWebIcon() {
  return `
    <svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true" focusable="false">
      <path fill="currentColor" d="M14 3h7v7h-2V6.41l-8.29 8.3-1.42-1.42 8.3-8.29H14V3z"/>
      <path fill="currentColor" d="M5 5h6v2H7v10h10v-4h2v6H5V5z"/>
    </svg>
  `;
}

export function formatPhaseTimestamp(value) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleString();
}

export function renderDetailsSummary(job) {
  const notes = String(job?.notes || "").trim();
  const attachmentsCount = Math.max(0, Number(job?.attachmentsCount) || 0);
  const hasAny = notes.length > 0 || attachmentsCount > 0;
  if (!hasAny) return "";

  const count = attachmentsCount > 0
    ? `<span class="details-attachments-count">(${attachmentsCount})</span>`
    : "";
  return `<span class="details-has-content"><span class="details-has-icon" aria-hidden="true"></span>${count}</span>`;
}

import { escapeHtml, showToast, setText } from "../shared/ui/index.js";

export function savedEscapeHtml(value) {
  return escapeHtml(value);
}

export function setSavedStatus(el, text) {
  setText(el, text);
}

export function showSavedToast(message, type = "info", options = {}) {
  showToast(message, type, options);
}

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
    getJobDetailsTab,
    renderDetailsSummary,
    getReminderMeta,
    renderMissingInfoChips,
    renderUpdatedHint,
    getJobHistoryEntries,
    renderWebIcon,
    renderPhaseBar,
    currentUser,
    maxAttachmentsPerJob,
    maxAttachmentBytes
  } = options;

  const isCustom = isCustomJob(job);
  const safeTitle = escapeHtml(job.title || "");
  const safeCompany = escapeHtml(job.company || "");
  const customSource = escapeHtml(String(job.customSourceLabel || customSourceLabel || "Custom"));
  const safeSector = escapeHtml(normalizeSavedSector(job));
  const safeCity = escapeHtml(job.city || "");
  const safeCountry = escapeHtml(fullCountryName(job.country || ""));
  const safeContract = escapeHtml(job.contractType || "Unknown");
  const safeWorkType = escapeHtml(job.workType || "Onsite");
  const safeLink = sanitizeUrl(job.jobLink || "");
  const hasLink = Boolean(safeLink);
  const contractClass = toContractClass(job.contractType || "Unknown");
  const rawJobKey = String(job.jobKey || job.id || "");
  const jobKey = escapeHtml(rawJobKey);
  const normalizedPhase = normalizePhase(job.applicationStatus);
  const isExpanded = expandedJobKey === rawJobKey;
  const activeTab = getJobDetailsTab(rawJobKey);
  const detailsSummary = renderDetailsSummary(job);
  const reminderMeta = getReminderMeta(job.reminderAt);
  const missingChips = renderMissingInfoChips(job);
  const updateHint = renderUpdatedHint(job);
  const historyRows = getJobHistoryEntries(rawJobKey);
  const tabClassNotes = activeTab === "notes" ? "active" : "";
  const tabClassAttachments = activeTab === "attachments" ? "active" : "";
  const tabClassHistory = activeTab === "history" ? "active" : "";
  const reminderBadge = reminderMeta.isSoon
    ? `<span class="saved-reminder-badge" title="${escapeHtml(reminderMeta.label)}">Due soon</span>`
    : "";

  return `
    <div class="saved-job-block" data-job-key="${jobKey}">
      <div class="saved-job-row">
        <button class="remove-saved-btn remove-inline-btn" data-job-key="${jobKey}" aria-label="Remove saved job">X</button>
        <div class="col-title job-cell" data-label="Position" title="${safeTitle}">
          <div class="saved-title-stack">
            <span class="saved-title-main">${safeTitle}</span>
            <div class="saved-title-meta">
              ${isCustom ? `<span class="saved-custom-badge" title="Custom job source">${customSource}</span>` : ""}
              ${reminderBadge}
              ${missingChips}
            </div>
            ${updateHint}
          </div>
          ${isCustom ? `
            <div class="saved-personal-actions">
              <button class="btn back-btn personal-edit-btn" data-job-key="${jobKey}" aria-label="Edit custom job">Edit</button>
              <button class="btn back-btn personal-duplicate-btn" data-job-key="${jobKey}" aria-label="Duplicate custom job">Duplicate</button>
            </div>
          ` : ""}
        </div>
        <div class="col-company job-cell" data-label="Company" title="${safeCompany}">${safeCompany}</div>
        <div class="col-sector job-cell" data-label="Sector" title="${safeSector}">${safeSector}</div>
        <div class="col-city job-cell" data-label="City" title="${safeCity}">${safeCity}</div>
        <div class="col-country job-cell" data-label="Country" title="${safeCountry}">${safeCountry}</div>
        <div class="col-contract job-cell" data-label="Contract" title="${safeContract}">
          <span class="job-contract ${contractClass}">${safeContract}</span>
        </div>
        <div class="col-type job-cell" data-label="Type" title="${safeWorkType}">
          <span class="job-tag ${safeWorkType.toLowerCase()}">${safeWorkType}</span>
        </div>
        <div class="col-link job-cell" data-label="Link">
          ${hasLink ? `<a class="saved-open-link-icon" href="${safeLink}" target="_blank" rel="noopener noreferrer" aria-label="Open job link" title="Open job link">${renderWebIcon()}</a>` : `<span class="saved-no-link ${isCustom ? "saved-no-link-custom" : ""}" title="${isCustom ? "Custom entry without external URL" : "No URL available"}">${isCustom ? "No link" : "N/A"}</span>`}
        </div>
      </div>
      <div class="saved-phase-row">
        <div class="phase-label">Application Phase</div>
        <div class="phase-value">
          ${renderPhaseBar(jobKey, normalizedPhase, job.phaseTimestamps, job.savedAt)}
        </div>
      </div>
      <div class="saved-details-toggle-row">
        <div class="details-toggle-spacer"></div>
        <button
          class="details-toggle-btn"
          data-job-key="${jobKey}"
          aria-expanded="${isExpanded ? "true" : "false"}"
          aria-label="${isExpanded ? "Collapse" : "Expand"} notes and attachments"
        >
          <span class="details-toggle-text">${detailsSummary}Notes, Files & History</span>
          <span class="details-toggle-arrow" aria-hidden="true">${isExpanded ? "v" : ">"}</span>
        </button>
      </div>
      <div class="saved-details-section ${isExpanded ? "" : "collapsed"}" data-job-key="${jobKey}" aria-hidden="${isExpanded ? "false" : "true"}">
        <div class="saved-details-tabs" role="tablist" aria-label="Saved job details tabs">
          <button class="saved-details-tab-btn ${tabClassNotes}" data-job-key="${jobKey}" data-details-tab="notes" role="tab" aria-selected="${activeTab === "notes" ? "true" : "false"}">Notes</button>
          <button class="saved-details-tab-btn ${tabClassAttachments}" data-job-key="${jobKey}" data-details-tab="attachments" role="tab" aria-selected="${activeTab === "attachments" ? "true" : "false"}">Attachments</button>
          <button class="saved-details-tab-btn ${tabClassHistory}" data-job-key="${jobKey}" data-details-tab="history" role="tab" aria-selected="${activeTab === "history" ? "true" : "false"}">History</button>
        </div>
        <div class="saved-details-panels">
          <div class="saved-notes-row saved-details-panel ${activeTab === "notes" ? "" : "hidden"}" data-tab-panel="notes">
            <div class="notes-label">Notes</div>
            <div class="notes-value">
              <textarea class="job-notes-input" data-job-key="${jobKey}" placeholder="Add notes, links, interview reminders..." ${!currentUser ? "disabled" : ""}>${escapeHtml(job.notes || "")}</textarea>
              <div class="note-save-state" data-job-key="${jobKey}">Saved</div>
            </div>
          </div>
          <div class="saved-attachments-row saved-details-panel ${activeTab === "attachments" ? "" : "hidden"}" data-tab-panel="attachments">
            <div class="attachments-label">Attachments</div>
            <div class="attachments-value">
              <div class="attachments-toolbar">
                <button class="btn back-btn attach-upload-btn" data-job-key="${jobKey}" ${!currentUser ? "disabled" : ""}>Upload</button>
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
                <button class="btn back-btn job-history-refresh-btn" data-job-key="${jobKey}">Refresh</button>
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

export function getReminderMeta(reminderAt, options = {}) {
  const { reminderSoonHours = 72 } = options;
  const parsed = parseIsoDate(reminderAt);
  if (!parsed) return { isSoon: false, label: "" };
  const now = Date.now();
  const diffMs = parsed.getTime() - now;
  const soonMs = Number(reminderSoonHours) * 60 * 60 * 1000;
  const isSoon = diffMs >= 0 && diffMs <= soonMs;
  return {
    isSoon,
    label: parsed.toLocaleString()
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
  const {
    phaseOptions = [],
    phaseLabels = {},
    canTransition = () => false,
    currentUser = null,
    phaseOverrideArmedGlobal = false
  } = options;
  const activeIndex = phaseOptions.indexOf(activePhase);
  const timestamps = phaseTimestamps && typeof phaseTimestamps === "object" ? phaseTimestamps : {};
  const segments = phaseOptions.map((phase, idx) => {
    const isActive = idx === activeIndex;
    const isComplete = idx <= activeIndex;
    const canChangeNormally = canTransition(activePhase, phase);
    const canClick = currentUser && (canChangeNormally || phaseOverrideArmedGlobal);
    const fallback = phase === "bookmark" ? savedAt : "";
    const selectedAt = formatPhaseTimestamp(timestamps[phase] || fallback);
    const classes = [
      "phase-step-btn",
      isActive ? "active" : "",
      isComplete ? "complete" : "",
      !canChangeNormally ? "locked" : "",
      phaseOverrideArmedGlobal && !canChangeNormally ? "override-enabled" : ""
    ].filter(Boolean).join(" ");

    return `
      <button
        class="${classes}"
        data-job-key="${jobKey}"
        data-phase="${phase}"
        data-current-phase="${escapeHtml(activePhase)}"
        ${canClick ? "" : "disabled"}
        aria-label="Set phase to ${escapeHtml(phaseLabels[phase] || phase)}"
      >
        <span class="phase-step-text">${escapeHtml(phaseLabels[phase] || phase)}</span>
        ${selectedAt ? `<span class="phase-step-time">${escapeHtml(selectedAt)}</span>` : ""}
      </button>
    `;
  }).join("");

  return `<div class="phase-bar" role="group" aria-label="Application phases">${segments}</div>`;
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

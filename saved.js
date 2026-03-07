let savedJobsListEl;
let savedSourceStatusEl;
let savedAuthStatusEl;
let savedAuthStatusHintEl;
let savedAuthAvatarEl;
let signInBtnEl;
let signOutBtnEl;
let jobsPageBtnEl;
let adminPageBtnEl;
let addCustomJobBtnEl;
let customJobPanelEl;
let customJobFormEl;
let customJobTitleEl;
let customJobCompanyEl;
let customJobCityEl;
let customJobCountryEl;
let customJobWorkTypeEl;
let customJobContractTypeEl;
let customJobSectorEl;
let customJobProfessionEl;
let customJobLinkEl;
let customJobNotesEl;
let customJobReminderEl;
let customJobLinkWarningEl;
let customJobCancelBtnEl;
let customJobPanelTitleEl;
let customJobPanelHintEl;
let customJobSaveBtnEl;
let savedCustomFilterBarEl;
let savedCustomFilterCountEl;
let savedCustomFilterBtnEls = [];
let savedSortBarEl;
let savedSortBtnEls = [];
let savedReminderCounterEl;
let historyPanelToggleBtnEl;
let exportBackupBtnEl;
let exportIncludeFilesEl;
let importBackupBtnEl;
let importBackupInputEl;
let globalPhaseOverrideBtnEl;
let activityPanelEl;
let activityPanelBodyEl;
let activityPanelStatusEl;
let activityRefreshBtnEl;
let activityCollapseBtnEl;

let currentUser = null;
let unsubscribeSavedJobs = () => {};
let expandedJobKey = null;
let phaseOverrideArmedGlobal = false;
let activityPanelOpen = false;
let customJobPanelOpen = false;
let customJobMode = "create";
let customJobTargetKey = "";
let activeSavedSort = "updated";
let jobDetailTabByKey = new Map();
let cachedActivityEntries = [];
let lastSavedJobsByKey = new Map();
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";
const CUSTOM_SOURCE_LABEL = "Custom";
const SAVED_FILTER_ALL = "all";
const SAVED_FILTER_CUSTOM = "custom";
const SAVED_FILTER_IMPORTED = "imported";
const DEFAULT_SAVED_FILTER = SAVED_FILTER_ALL;
const SORT_UPDATED = "updated";
const SORT_SAVED = "saved";
const SORT_REMINDER = "reminder";
const SORT_PERSONAL = "personal";
const REMINDER_SOON_HOURS = 72;
let activeSavedFilter = DEFAULT_SAVED_FILTER;

const PHASE_OPTIONS = ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"];
const PHASE_LABELS = {
  bookmark: "Saved",
  applied: "Applied",
  interview_1: "Interview 1",
  interview_2: "Interview 2",
  offer: "Offer",
  rejected: "Rejected"
};

const MAX_ATTACHMENTS_PER_JOB = 20;
const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;
const NOTE_AUTOSAVE_MS = 600;
const ALLOWED_EXTENSIONS = new Set(["pdf", "doc", "docx", "txt", "png", "jpg", "jpeg"]);

const noteSaveTimers = new Map();
const noteSaveInFlight = new Map();
const notePendingValues = new Map();
const attachmentPreviewUrls = new Map();

document.addEventListener("DOMContentLoaded", () => {
  cacheDom();
  bindEvents();
  initSavedJobsPage();
});

function cacheDom() {
  savedJobsListEl = document.getElementById("saved-jobs-list");
  savedSourceStatusEl = document.getElementById("saved-source-status");
  savedAuthStatusEl = document.getElementById("saved-auth-status");
  savedAuthStatusHintEl = document.getElementById("saved-auth-status-hint");
  savedAuthAvatarEl = document.getElementById("saved-auth-avatar");
  signInBtnEl = document.getElementById("saved-auth-sign-in-btn");
  signOutBtnEl = document.getElementById("saved-auth-sign-out-btn");
  jobsPageBtnEl = document.getElementById("jobs-page-btn");
  adminPageBtnEl = document.getElementById("admin-page-btn");
  addCustomJobBtnEl = document.getElementById("add-custom-job-btn");
  customJobPanelEl = document.getElementById("custom-job-panel");
  customJobFormEl = document.getElementById("custom-job-form");
  customJobTitleEl = document.getElementById("custom-job-title");
  customJobCompanyEl = document.getElementById("custom-job-company");
  customJobCityEl = document.getElementById("custom-job-city");
  customJobCountryEl = document.getElementById("custom-job-country");
  customJobWorkTypeEl = document.getElementById("custom-job-work-type");
  customJobContractTypeEl = document.getElementById("custom-job-contract-type");
  customJobSectorEl = document.getElementById("custom-job-sector");
  customJobProfessionEl = document.getElementById("custom-job-profession");
  customJobLinkEl = document.getElementById("custom-job-link");
  customJobNotesEl = document.getElementById("custom-job-notes");
  customJobReminderEl = document.getElementById("custom-job-reminder");
  customJobLinkWarningEl = document.getElementById("custom-job-link-warning");
  customJobCancelBtnEl = document.getElementById("custom-job-cancel-btn");
  customJobPanelTitleEl = document.getElementById("custom-job-panel-title");
  customJobPanelHintEl = document.getElementById("custom-job-panel-hint");
  customJobSaveBtnEl = document.getElementById("custom-job-save-btn");
  savedCustomFilterBarEl = document.getElementById("saved-custom-filter-bar");
  savedCustomFilterCountEl = document.getElementById("saved-custom-filter-count");
  savedCustomFilterBtnEls = Array.from(document.querySelectorAll(".saved-custom-filter-btn"));
  savedSortBarEl = document.getElementById("saved-sort-bar");
  savedSortBtnEls = Array.from(document.querySelectorAll(".saved-sort-btn"));
  savedReminderCounterEl = document.getElementById("saved-reminder-counter");
  historyPanelToggleBtnEl = document.getElementById("history-panel-toggle-btn");
  exportBackupBtnEl = document.getElementById("export-backup-btn");
  exportIncludeFilesEl = document.getElementById("export-include-files");
  importBackupBtnEl = document.getElementById("import-backup-btn");
  importBackupInputEl = document.getElementById("import-backup-input");
  globalPhaseOverrideBtnEl = document.getElementById("global-phase-override-btn");
  activityPanelEl = document.getElementById("activity-panel");
  activityPanelBodyEl = document.getElementById("activity-panel-body");
  activityPanelStatusEl = document.getElementById("activity-panel-status");
  activityRefreshBtnEl = document.getElementById("activity-refresh-btn");
  activityCollapseBtnEl = document.getElementById("activity-collapse-btn");
}

function bindEvents() {
  if (jobsPageBtnEl) {
    jobsPageBtnEl.addEventListener("click", () => {
      const target = getLastJobsUrl();
      window.location.href = target;
    });
  }

  if (adminPageBtnEl) {
    adminPageBtnEl.addEventListener("click", () => {
      window.location.href = "admin.html";
    });
  }

  if (addCustomJobBtnEl) {
    addCustomJobBtnEl.addEventListener("click", () => {
      if (!currentUser) {
        showToast("Sign in to add custom jobs.", "info");
        return;
      }
      setCustomJobPanelOpen(!customJobPanelOpen);
      if (customJobPanelOpen) {
        customJobTitleEl?.focus();
      }
    });
  }

  if (customJobCancelBtnEl) {
    customJobCancelBtnEl.addEventListener("click", () => {
      setCustomJobPanelOpen(false);
    });
  }

  if (customJobFormEl) {
    customJobFormEl.addEventListener("submit", async event => {
      event.preventDefault();
      await createCustomJob();
    });
  }

  if (customJobLinkEl) {
    customJobLinkEl.addEventListener("input", updateCustomJobWarning);
  }

  savedCustomFilterBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const nextFilter = String(btn.dataset.savedFilter || DEFAULT_SAVED_FILTER).toLowerCase();
      setSavedFilter(nextFilter);
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  });

  savedSortBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const sortKey = String(btn.dataset.savedSort || SORT_UPDATED).toLowerCase();
      setSavedSort(sortKey);
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  });

  if (historyPanelToggleBtnEl) {
    historyPanelToggleBtnEl.addEventListener("click", () => {
      setActivityPanelOpen(!activityPanelOpen);
    });
  }

  if (activityCollapseBtnEl) {
    activityCollapseBtnEl.addEventListener("click", () => {
      setActivityPanelOpen(false);
    });
  }

  if (activityRefreshBtnEl) {
    activityRefreshBtnEl.addEventListener("click", async () => {
      await refreshActivityLog();
    });
  }

  if (signInBtnEl) {
    signInBtnEl.addEventListener("click", async () => {
      await signInUser();
    });
  }

  if (signOutBtnEl) {
    signOutBtnEl.addEventListener("click", async () => {
      await signOutUser();
    });
  }

  if (exportBackupBtnEl) {
    exportBackupBtnEl.addEventListener("click", async () => {
      await exportBackup();
    });
  }

  if (importBackupBtnEl && importBackupInputEl) {
    importBackupBtnEl.addEventListener("click", () => {
      importBackupInputEl.click();
    });
    importBackupInputEl.addEventListener("change", async () => {
      const file = importBackupInputEl.files && importBackupInputEl.files[0];
      if (!file) return;
      await importBackup(file);
      importBackupInputEl.value = "";
    });
  }

  if (globalPhaseOverrideBtnEl) {
    globalPhaseOverrideBtnEl.addEventListener("click", () => {
      if (!currentUser) return;
      phaseOverrideArmedGlobal = !phaseOverrideArmedGlobal;
      updateGlobalOverrideButton();
      showToast(
        phaseOverrideArmedGlobal
          ? "Global override armed for one locked phase change."
          : "Global override cancelled.",
        "info"
      );
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  }
}

function initSavedJobsPage() {
  setActivityPanelOpen(false);
  setCustomJobPanelOpen(false);
  setCustomJobAvailability(false);
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    setAuthStatus("Browsing as guest");
    setSourceStatus("Local storage provider unavailable.");
    setActivityStatus("Local provider unavailable.");
    toggleAuthButtons(false);
    setCustomJobAvailability(false);
    setSavedSortBarVisible(false);
    renderAuthRequired("Local auth provider is unavailable.");
    renderActivityEntries([]);
    return;
  }

  api.onAuthStateChanged(user => {
    currentUser = user || null;
    unsubscribeSavedJobs();
    unsubscribeSavedJobs = () => {};
    clearNoteSaveQueues();
    expandedJobKey = null;
    phaseOverrideArmedGlobal = false;
    jobDetailTabByKey = new Map();
    cachedActivityEntries = [];
    lastSavedJobsByKey = new Map();
    setSavedFilter(DEFAULT_SAVED_FILTER);
    setSavedSort(SORT_UPDATED);

    if (!currentUser) {
      setAuthStatus("Browsing as guest");
      setSourceStatus("Sign in to view your saved jobs.");
      setActivityStatus("Sign in to view history.");
      toggleAuthButtons(false);
      setBackupButtonsEnabled(false);
      setCustomJobAvailability(false);
      setCustomJobPanelOpen(false);
      setSavedFilterBarVisible(false);
      setSavedSortBarVisible(false);
      renderAuthRequired("Sign in to access your custom saved jobs table.");
      renderActivityEntries([]);
      return;
    }

    setAuthStatus(`Signed in as ${currentUser.displayName || currentUser.email || "user"}`);
    setSourceStatus("Loading your saved jobs...");
    setActivityStatus("Loading activity...");
    toggleAuthButtons(true);
    setBackupButtonsEnabled(true);
    setCustomJobAvailability(true);
    subscribeToSavedJobs(currentUser.uid);
    refreshActivityLog().catch(err => {
      console.error("Failed to load activity:", err);
      setActivityStatus("Could not load activity.");
    });
  });
}

function subscribeToSavedJobs(uid) {
  const api = window.JobAppLocalData;
  unsubscribeSavedJobs = api.subscribeSavedJobs(
    uid,
    jobs => {
      setSourceStatus(`Loaded ${jobs.length} saved jobs.`);
      lastSavedJobsByKey = new Map(
        (jobs || []).map(job => [String(job.jobKey || job.id || ""), job])
      );
      renderSavedJobs(jobs);
      refreshActivityLog().catch(() => {
        // Best-effort refresh.
      });
    },
    err => {
      console.error("Saved jobs subscription failed:", err);
      setSourceStatus("Could not load saved jobs.");
      showToast("Could not load saved jobs.", "error");
      renderAuthRequired("Unable to load your saved jobs right now.");
    }
  );
}

function renderAuthRequired(message) {
  if (!savedJobsListEl) return;
  savedJobsListEl.innerHTML = `<div class="no-results">${escapeHtml(message)}</div>`;
}

function renderSavedJobs(jobs) {
  if (!savedJobsListEl) return;
  const allJobs = Array.isArray(jobs) ? jobs : [];
  const filteredJobs = sortSavedJobs(filterSavedJobs(allJobs, activeSavedFilter), activeSavedSort);
  setSavedFilterBarVisible(allJobs.length > 0 && Boolean(currentUser));
  setSavedSortBarVisible(allJobs.length > 0 && Boolean(currentUser));
  renderSavedFilterMeta(allJobs.length, filteredJobs.length);
  renderReminderCounter(allJobs);

  if (!allJobs || allJobs.length === 0) {
    expandedJobKey = null;
    savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs yet.</div>';
    return;
  }
  if (!filteredJobs.some(job => String(job.jobKey || job.id || "") === expandedJobKey)) {
    expandedJobKey = null;
  }

  if (filteredJobs.length === 0) {
    savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs match this filter.</div>';
    return;
  }

  savedJobsListEl.innerHTML = `
    <div class="jobs-table-header">
      <div class="saved-row-header">
        <div class="col-title">Position</div>
        <div class="col-company">Company</div>
        <div class="col-sector">Sector</div>
        <div class="col-city">City</div>
        <div class="col-country">Country</div>
        <div class="col-contract">Contract</div>
        <div class="col-type">Type</div>
        <div class="col-link">Link</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${filteredJobs.map(renderSavedJobBlock).join("")}
    </div>
  `;

  savedJobsListEl.querySelectorAll(".remove-saved-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      await removeSavedJob(btn.dataset.jobKey || "");
    });
  });

  savedJobsListEl.querySelectorAll(".phase-step-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const jobKey = btn.dataset.jobKey || "";
      const phase = btn.dataset.phase || "";
      await updatePhase(jobKey, phase);
    });
  });

  savedJobsListEl.querySelectorAll(".details-toggle-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const jobKey = btn.dataset.jobKey || "";
      toggleDetailsForJob(jobKey);
    });
  });

  savedJobsListEl.querySelectorAll(".personal-edit-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      openCustomJobEditor(btn.dataset.jobKey || "", false);
    });
  });

  savedJobsListEl.querySelectorAll(".personal-duplicate-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      openCustomJobEditor(btn.dataset.jobKey || "", true);
    });
  });

  savedJobsListEl.querySelectorAll(".saved-details-tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const jobKey = btn.dataset.jobKey || "";
      const tab = btn.dataset.detailsTab || "notes";
      setJobDetailsTab(jobKey, tab);
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  });

  savedJobsListEl.querySelectorAll(".job-history-refresh-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      await refreshActivityLog();
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  });

  savedJobsListEl.querySelectorAll(".job-notes-input").forEach(textarea => {
    textarea.addEventListener("input", () => {
      queueNotesSave(textarea.dataset.jobKey || "", textarea.value);
    });
    textarea.addEventListener("blur", async () => {
      await flushNotesSave(textarea.dataset.jobKey || "", textarea.value);
    });
  });

  savedJobsListEl.querySelectorAll(".attach-upload-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const key = btn.dataset.jobKey || "";
      const input = savedJobsListEl.querySelector(`.attach-file-input[data-job-key="${cssEscape(key)}"]`);
      if (input) input.click();
    });
  });

  savedJobsListEl.querySelectorAll(".attach-file-input").forEach(input => {
    input.addEventListener("change", async () => {
      const files = input.files ? Array.from(input.files) : [];
      if (files.length === 0) return;
      await uploadAttachments(input.dataset.jobKey || "", files);
      input.value = "";
    });
  });

  bindAttachmentActionButtons();
  applyDetailsAccordion();

  hydrateAttachmentLists(filteredJobs).catch(err => {
    console.error("Could not load attachment lists:", err);
  });
}

function renderSavedJobBlock(job) {
  const isCustom = isCustomJob(job);
  const safeTitle = escapeHtml(job.title || "");
  const safeCompany = escapeHtml(job.company || "");
  const customSource = escapeHtml(String(job.customSourceLabel || CUSTOM_SOURCE_LABEL));
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
                <span class="attachments-hint">Max ${MAX_ATTACHMENTS_PER_JOB} files, ${Math.round(MAX_ATTACHMENT_BYTES / (1024 * 1024))}MB each</span>
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

function normalizeSavedSector(job) {
  const raw = String(job?.sector || "").trim();
  const lower = raw.toLowerCase();
  if (lower === "game" || lower === "game company" || lower === "gaming") return "Game";
  if (lower === "tech" || lower === "tech company" || lower === "technology") return "Tech";

  const ct = String(job?.companyType || "").trim().toLowerCase();
  if (ct === "game" || ct === "game company") return "Game";
  if (ct === "tech" || ct === "tech company") return "Tech";
  return raw || "Tech";
}

function renderMissingInfoChips(job) {
  if (!isCustomJob(job)) return "";
  const chips = [];
  if (!sanitizeUrl(job.jobLink || "")) chips.push("No link");
  if (!String(job.city || "").trim()) chips.push("No city");
  if (!String(job.contractType || "").trim() || String(job.contractType || "").toLowerCase() === "unknown") chips.push("No contract");
  if (chips.length === 0) return "";
  return chips.map(label => `<span class="saved-missing-chip">${escapeHtml(label)}</span>`).join("");
}

function renderUpdatedHint(job) {
  if (!isCustomJob(job)) return "";
  const label = String(job?.updatedBy || "").trim();
  if (!label) return "";
  const time = formatRelativeTime(job.updatedAt);
  if (label && time) {
    return `<div class="saved-updated-hint">Updated: ${escapeHtml(label)} · ${escapeHtml(time)}</div>`;
  }
  return `<div class="saved-updated-hint">Updated: ${escapeHtml(label)}</div>`;
}

function parseIsoDate(value) {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function getReminderMeta(reminderAt) {
  const parsed = parseIsoDate(reminderAt);
  if (!parsed) return { isSoon: false, label: "" };
  const now = Date.now();
  const diffMs = parsed.getTime() - now;
  const soonMs = REMINDER_SOON_HOURS * 60 * 60 * 1000;
  const isSoon = diffMs >= 0 && diffMs <= soonMs;
  return {
    isSoon,
    label: parsed.toLocaleString()
  };
}

function formatRelativeTime(value) {
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

function getJobDetailsTab(jobKey) {
  const key = String(jobKey || "");
  return jobDetailTabByKey.get(key) || "notes";
}

function setJobDetailsTab(jobKey, tab) {
  const safeTab = tab === "attachments" || tab === "history" ? tab : "notes";
  jobDetailTabByKey.set(String(jobKey || ""), safeTab);
}

function getJobHistoryEntries(jobKey) {
  const key = String(jobKey || "");
  const rows = (cachedActivityEntries || [])
    .filter(entry => String(entry?.jobKey || "") === key)
    .slice(0, 12);
  if (rows.length === 0) {
    return '<div class="muted">No activity for this job yet.</div>';
  }
  return rows.map(entry => {
    const type = escapeHtml(activityTypeLabel(String(entry?.type || "event")));
    const time = escapeHtml(formatPhaseTimestamp(entry?.createdAt) || "");
    const detail = escapeHtml(formatActivityDetail(entry));
    return `
      <div class="job-history-item">
        <div class="job-history-top"><span>${type}</span><span>${time}</span></div>
        <div class="job-history-detail">${detail}</div>
      </div>
    `;
  }).join("");
}

function renderPhaseBar(jobKey, activePhase, phaseTimestamps, savedAt) {
  const activeIndex = PHASE_OPTIONS.indexOf(activePhase);
  const timestamps = phaseTimestamps && typeof phaseTimestamps === "object" ? phaseTimestamps : {};
  const segments = PHASE_OPTIONS.map((phase, idx) => {
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
        aria-label="Set phase to ${escapeHtml(PHASE_LABELS[phase] || phase)}"
      >
        <span class="phase-step-text">${escapeHtml(PHASE_LABELS[phase] || phase)}</span>
        ${selectedAt ? `<span class="phase-step-time">${escapeHtml(selectedAt)}</span>` : ""}
      </button>
    `;
  }).join("");

  return `<div class="phase-bar" role="group" aria-label="Application phases">${segments}</div>`;
}

function renderWebIcon() {
  return `
    <svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true" focusable="false">
      <path fill="currentColor" d="M14 3h7v7h-2V6.41l-8.29 8.3-1.42-1.42 8.3-8.29H14V3z"/>
      <path fill="currentColor" d="M5 5h6v2H7v10h10v-4h2v6H5V5z"/>
    </svg>
  `;
}

function formatPhaseTimestamp(value) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleString();
}

function renderDetailsSummary(job) {
  const notes = String(job?.notes || "").trim();
  const attachmentsCount = Math.max(0, Number(job?.attachmentsCount) || 0);
  const hasAny = notes.length > 0 || attachmentsCount > 0;
  if (!hasAny) return "";

  const count = attachmentsCount > 0
    ? `<span class="details-attachments-count">(${attachmentsCount})</span>`
    : "";
  return `<span class="details-has-content"><span class="details-has-icon" aria-hidden="true"></span>${count}</span>`;
}

function normalizePhase(phase) {
  const raw = String(phase || "").toLowerCase().trim();
  if (raw === "bookmarked") return "bookmark";
  return PHASE_OPTIONS.includes(raw) ? raw : "bookmark";
}

function canTransition(currentPhase, nextPhase) {
  const api = window.JobAppLocalData;
  if (api && typeof api.canTransitionPhase === "function") {
    return Boolean(api.canTransitionPhase(currentPhase, nextPhase));
  }
  const current = normalizePhase(currentPhase);
  const next = normalizePhase(nextPhase);
  if (current === next) return true;
  if (current === "rejected") return false;
  if (next === "rejected") return true;
  const currentIdx = PHASE_OPTIONS.indexOf(current);
  const nextIdx = PHASE_OPTIONS.indexOf(next);
  return currentIdx >= 0 && nextIdx >= 0 && nextIdx === currentIdx + 1;
}

async function removeSavedJob(jobKey) {
  const api = window.JobAppLocalData;
  if (!currentUser) {
    showToast("Sign in required.", "error");
    return;
  }
  const removedSnapshot = lastSavedJobsByKey.get(String(jobKey || "")) || null;
  try {
    await api.removeSavedJobForUser(currentUser.uid, jobKey);
    showToast("Removed saved job.", "success", {
      durationMs: 6500,
      actionLabel: "Revert",
      onAction: async () => {
        if (!currentUser || !removedSnapshot) return;
        try {
          await api.saveJobForUser(currentUser.uid, removedSnapshot);
          showToast("Saved job restored.", "success");
        } catch (restoreErr) {
          console.error("Could not restore removed job:", restoreErr);
          showToast("Could not restore removed job.", "error");
        }
      }
    });
  } catch (err) {
    console.error("Could not remove saved job:", err);
    showToast("Could not remove job.", "error");
  }
}

async function updatePhase(jobKey, phase) {
  const api = window.JobAppLocalData;
  if (!currentUser) {
    showToast("Sign in required.", "error");
    return;
  }

  const row = lastSavedJobsByKey.get(String(jobKey || ""));
  const currentPhase = normalizePhase(row?.applicationStatus);
  const normalized = normalizePhase(phase);
  const regularAllowed = canTransition(currentPhase, normalized);
  const overrideArmed = phaseOverrideArmedGlobal;
  if (!regularAllowed && !overrideArmed) {
    showToast("Locked transition. Use Override Phase Lock for exceptional changes.", "info");
    return;
  }

  if (!regularAllowed && overrideArmed) {
    const from = PHASE_LABELS[currentPhase] || currentPhase;
    const to = PHASE_LABELS[normalized] || normalized;
    const ok = window.confirm(`Override phase lock?\n\n${from} -> ${to}`);
    if (!ok) return;
  }

  try {
    const previousPhaseTimestamp = String(row?.phaseTimestamps?.[currentPhase] || "").trim();
    await api.updateApplicationStatus(currentUser.uid, jobKey, normalized, {
      override: !regularAllowed && overrideArmed
    });
    if (overrideArmed) {
      phaseOverrideArmedGlobal = false;
      updateGlobalOverrideButton();
    }
    const previousPhase = currentPhase;
    showToast(`Phase updated to ${PHASE_LABELS[normalized] || normalized}.`, "success", {
      durationMs: 6500,
      actionLabel: "Revert",
      onAction: async () => {
        if (!currentUser) return;
        try {
          await api.updateApplicationStatus(currentUser.uid, jobKey, previousPhase, {
            override: true,
            cleanupPhase: normalized,
            preserveTimestamp: previousPhaseTimestamp
          });
          showToast(`Phase reverted to ${PHASE_LABELS[previousPhase] || previousPhase}.`, "success");
          await refreshActivityLog();
          renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
        } catch (revertErr) {
          console.error("Could not revert phase change:", revertErr);
          showToast("Could not revert phase.", "error");
        }
      }
    });
    await refreshActivityLog();
  } catch (err) {
    console.error("Could not update phase:", err);
    showToast(err?.message || "Could not update phase.", "error");
  } finally {
    renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
  }
}

function queueNotesSave(jobKey, value) {
  if (!jobKey) return;
  notePendingValues.set(jobKey, String(value || ""));
  setNoteSaveState(jobKey, "saving");
  if (noteSaveTimers.has(jobKey)) {
    clearTimeout(noteSaveTimers.get(jobKey));
  }
  const timer = setTimeout(() => {
    flushNotesSave(jobKey).catch(() => {
      // Handled in flush.
    });
  }, NOTE_AUTOSAVE_MS);
  noteSaveTimers.set(jobKey, timer);
}

async function flushNotesSave(jobKey, value) {
  if (!jobKey || !currentUser) return;
  if (typeof value === "string") {
    notePendingValues.set(jobKey, value);
  }
  if (noteSaveTimers.has(jobKey)) {
    clearTimeout(noteSaveTimers.get(jobKey));
    noteSaveTimers.delete(jobKey);
  }
  if (!notePendingValues.has(jobKey)) return;
  if (noteSaveInFlight.get(jobKey)) return;
  noteSaveInFlight.set(jobKey, true);

  const api = window.JobAppLocalData;
  setNoteSaveState(jobKey, "saving");
  const saveValue = notePendingValues.get(jobKey);
  try {
    await api.updateJobNotes(currentUser.uid, jobKey, saveValue);
    if (notePendingValues.get(jobKey) === saveValue) {
      notePendingValues.delete(jobKey);
      setNoteSaveState(jobKey, "saved");
    } else {
      setNoteSaveState(jobKey, "saving");
    }
  } catch (err) {
    console.error("Could not save notes:", err);
    setNoteSaveState(jobKey, "error");
  } finally {
    noteSaveInFlight.delete(jobKey);
    if (notePendingValues.has(jobKey) && currentUser) {
      setTimeout(() => {
        flushNotesSave(jobKey).catch(() => {
          // Handled in flush.
        });
      }, 0);
    }
  }
}

function clearNoteSaveQueues() {
  noteSaveTimers.forEach(timer => clearTimeout(timer));
  noteSaveTimers.clear();
  noteSaveInFlight.clear();
  notePendingValues.clear();
}

function toggleDetailsForJob(jobKey) {
  if (!jobKey) return;
  const nextKey = expandedJobKey === jobKey ? null : jobKey;
  if (nextKey && !jobDetailTabByKey.has(nextKey)) {
    jobDetailTabByKey.set(nextKey, "notes");
  }
  expandedJobKey = nextKey;
  applyDetailsAccordion();
}

function applyDetailsAccordion() {
  if (!savedJobsListEl) return;
  savedJobsListEl.querySelectorAll(".saved-job-block").forEach(block => {
    const key = block.dataset.jobKey || "";
    const expanded = Boolean(expandedJobKey) && key === expandedJobKey;
    const details = block.querySelector(".saved-details-section");
    const toggle = block.querySelector(".details-toggle-btn");
    const arrow = block.querySelector(".details-toggle-arrow");
    if (details) {
      details.classList.toggle("collapsed", !expanded);
      details.setAttribute("aria-hidden", expanded ? "false" : "true");
    }
    if (toggle) {
      toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
      toggle.setAttribute("aria-label", `${expanded ? "Collapse" : "Expand"} notes, attachments, and history`);
    }
    if (arrow) {
      arrow.textContent = expanded ? "v" : ">";
    }
  });
}

function setNoteSaveState(jobKey, state) {
  const el = savedJobsListEl?.querySelector(`.note-save-state[data-job-key="${cssEscape(jobKey)}"]`);
  if (!el) return;
  if (state === "saving") {
    el.textContent = "Saving...";
    el.classList.add("saving");
    el.classList.remove("error");
    return;
  }
  if (state === "error") {
    el.textContent = "Error";
    el.classList.remove("saving");
    el.classList.add("error");
    return;
  }
  el.textContent = "Saved";
  el.classList.remove("saving");
  el.classList.remove("error");
}

async function hydrateAttachmentLists(jobs) {
  if (!currentUser || !Array.isArray(jobs)) return;
  const api = window.JobAppLocalData;

  for (const job of jobs) {
    const jobKey = String(job.jobKey || job.id || "");
    if (!jobKey) continue;
    try {
      const rows = await api.listAttachmentsForJob(currentUser.uid, jobKey);
      renderAttachmentList(jobKey, rows);
    } catch (err) {
      console.error("Could not list attachments:", err);
      renderAttachmentList(jobKey, []);
    }
  }
}

async function uploadAttachments(jobKey, files) {
  if (!currentUser || !jobKey || !Array.isArray(files) || files.length === 0) return;
  const api = window.JobAppLocalData;

  let currentList = [];
  try {
    currentList = await api.listAttachmentsForJob(currentUser.uid, jobKey);
  } catch {
    currentList = [];
  }

  const remainingSlots = MAX_ATTACHMENTS_PER_JOB - currentList.length;
  if (remainingSlots <= 0) {
    showToast(`Max ${MAX_ATTACHMENTS_PER_JOB} attachments per job.`, "error");
    return;
  }

  let accepted = 0;
  for (const file of files) {
    if (accepted >= remainingSlots) {
      showToast(`Max ${MAX_ATTACHMENTS_PER_JOB} attachments per job.`, "error");
      break;
    }
    if (!isAllowedAttachment(file)) {
      showToast(`Unsupported file type: ${file.name}`, "error");
      continue;
    }
    if (file.size > MAX_ATTACHMENT_BYTES) {
      showToast(`File too large: ${file.name}`, "error");
      continue;
    }

    try {
      await api.addAttachmentForJob(
        currentUser.uid,
        jobKey,
        { name: file.name, type: file.type, size: file.size },
        file
      );
      accepted += 1;
    } catch (err) {
      console.error("Attachment upload failed:", err);
      showToast(`Could not upload ${file.name}`, "error");
    }
  }

  try {
    const next = await api.listAttachmentsForJob(currentUser.uid, jobKey);
    renderAttachmentList(jobKey, next);
    showToast("Attachments updated.", "success");
  } catch {
    showToast("Could not refresh attachments.", "error");
  }
}

async function openAttachment(jobKey, attachmentId) {
  if (!currentUser) return;
  const api = window.JobAppLocalData;
  try {
    const blob = await api.getAttachmentBlob(currentUser.uid, jobKey, attachmentId);
    if (!blob) {
      showToast("Attachment data not available.", "error");
      return;
    }
    const url = URL.createObjectURL(blob);
    window.open(url, "_blank", "noopener,noreferrer");
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
  } catch (err) {
    console.error("Could not open attachment:", err);
    showToast("Could not open attachment.", "error");
  }
}

async function downloadAttachment(jobKey, attachmentId, filename) {
  if (!currentUser) return;
  const api = window.JobAppLocalData;
  try {
    const blob = await api.getAttachmentBlob(currentUser.uid, jobKey, attachmentId);
    if (!blob) {
      showToast("Attachment data not available.", "error");
      return;
    }
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename || "attachment";
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (err) {
    console.error("Could not download attachment:", err);
    showToast("Could not download attachment.", "error");
  }
}

async function deleteAttachment(jobKey, attachmentId) {
  if (!currentUser) return;
  const api = window.JobAppLocalData;
  try {
    await api.deleteAttachmentForJob(currentUser.uid, jobKey, attachmentId);
    const next = await api.listAttachmentsForJob(currentUser.uid, jobKey);
    renderAttachmentList(jobKey, next);
    showToast("Attachment removed.", "success");
  } catch (err) {
    console.error("Could not delete attachment:", err);
    showToast("Could not delete attachment.", "error");
  }
}

function renderAttachmentList(jobKey, attachments) {
  const container = savedJobsListEl?.querySelector(`.attachments-list[data-job-key="${cssEscape(jobKey)}"]`);
  if (!container) return;
  clearAttachmentPreviewUrls(jobKey);

  if (!attachments || attachments.length === 0) {
    container.innerHTML = '<div class="muted">No attachments yet.</div>';
    return;
  }

  container.innerHTML = attachments.map(att => {
    const id = escapeHtml(att.id || "");
    const name = escapeHtml(att.name || "attachment");
    const size = formatFileSize(att.size || 0);
    const previewUrl = getAttachmentPreviewUrl(jobKey, att);
    const previewHtml = previewUrl
      ? `<img class="attachment-preview" src="${escapeHtml(previewUrl)}" alt="${name} preview" loading="lazy">`
      : "";
    return `
      <div class="attachment-item">
        <div class="attachment-meta">
          ${previewHtml}
          <span class="attachment-name">${name}</span>
          <span class="attachment-size">${size}</span>
        </div>
        <div class="attachment-actions">
          <button class="btn back-btn att-open-btn" data-job-key="${escapeHtml(jobKey)}" data-attachment-id="${id}">Open</button>
          <button class="btn back-btn att-download-btn" data-job-key="${escapeHtml(jobKey)}" data-attachment-id="${id}" data-file-name="${name}">Download</button>
          <button class="btn back-btn att-delete-btn" data-job-key="${escapeHtml(jobKey)}" data-attachment-id="${id}">Delete</button>
        </div>
      </div>
    `;
  }).join("");
  bindAttachmentActionButtons();
}

function bindAttachmentActionButtons() {
  if (!savedJobsListEl) return;

  savedJobsListEl.querySelectorAll(".att-open-btn").forEach(btn => {
    btn.onclick = async () => {
      await openAttachment(btn.dataset.jobKey || "", btn.dataset.attachmentId || "");
    };
  });

  savedJobsListEl.querySelectorAll(".att-download-btn").forEach(btn => {
    btn.onclick = async () => {
      await downloadAttachment(
        btn.dataset.jobKey || "",
        btn.dataset.attachmentId || "",
        btn.dataset.fileName || "attachment"
      );
    };
  });

  savedJobsListEl.querySelectorAll(".att-delete-btn").forEach(btn => {
    btn.onclick = async () => {
      await deleteAttachment(btn.dataset.jobKey || "", btn.dataset.attachmentId || "");
    };
  });
}

function getAttachmentPreviewUrl(jobKey, attachment) {
  if (!isImageAttachment(attachment)) return "";
  if (!(attachment.blob instanceof Blob)) return "";
  try {
    const url = URL.createObjectURL(attachment.blob);
    const key = String(jobKey || "");
    const urls = attachmentPreviewUrls.get(key) || [];
    urls.push(url);
    attachmentPreviewUrls.set(key, urls);
    return url;
  } catch {
    return "";
  }
}

function clearAttachmentPreviewUrls(jobKey) {
  const key = String(jobKey || "");
  const urls = attachmentPreviewUrls.get(key) || [];
  urls.forEach(url => {
    try {
      URL.revokeObjectURL(url);
    } catch {
      // no-op
    }
  });
  attachmentPreviewUrls.delete(key);
}

function isImageAttachment(attachment) {
  const type = String(attachment?.type || "").toLowerCase();
  if (type === "image/png" || type === "image/jpeg") return true;
  const ext = getFileExtension(attachment?.name || "");
  return ext === "png" || ext === "jpg" || ext === "jpeg";
}

function isAllowedAttachment(file) {
  const ext = getFileExtension(file.name || "");
  if (ALLOWED_EXTENSIONS.has(ext)) return true;
  const type = String(file.type || "").toLowerCase();
  return (
    type === "application/pdf" ||
    type === "application/msword" ||
    type === "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ||
    type === "text/plain" ||
    type === "image/png" ||
    type === "image/jpeg"
  );
}

function getFileExtension(name) {
  const idx = String(name || "").lastIndexOf(".");
  if (idx === -1) return "";
  return name.slice(idx + 1).toLowerCase();
}

function formatFileSize(bytes) {
  const value = Number(bytes) || 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function cssEscape(value) {
  if (window.CSS && typeof window.CSS.escape === "function") {
    return window.CSS.escape(value);
  }
  return String(value).replace(/["\\]/g, "\\$&");
}

function setAuthStatus(text) {
  if (!savedAuthStatusEl) return;
  const raw = String(text || "").trim();
  let label = raw || "Guest";
  let hint = "";
  const signedInMatch = raw.match(/^signed\s+in\s+as\s+(.+)$/i);

  if (!raw || /^browsing\s+as\s+guest$/i.test(raw) || /^guest$/i.test(raw)) {
    label = "Guest";
    hint = "Browsing as guest";
  } else if (signedInMatch) {
    label = String(signedInMatch[1] || "").trim() || "User";
    hint = "Signed in";
  }

  savedAuthStatusEl.textContent = label;
  if (savedAuthStatusHintEl) {
    savedAuthStatusHintEl.textContent = hint;
  }
  if (savedAuthAvatarEl) {
    const initial = label.charAt(0).toUpperCase();
    savedAuthAvatarEl.textContent = initial && /[A-Z0-9]/.test(initial) ? initial : "U";
  }
}

function isCustomJob(job) {
  return Boolean(job && job.isCustom);
}

function filterSavedJobs(jobs, filter) {
  if (!Array.isArray(jobs)) return [];
  if (filter === SAVED_FILTER_CUSTOM) {
    return jobs.filter(isCustomJob);
  }
  if (filter === SAVED_FILTER_IMPORTED) {
    return jobs.filter(job => !isCustomJob(job));
  }
  return jobs;
}

function isValidSavedFilter(value) {
  return value === SAVED_FILTER_ALL || value === SAVED_FILTER_CUSTOM || value === SAVED_FILTER_IMPORTED;
}

function setSavedFilter(nextFilter) {
  activeSavedFilter = isValidSavedFilter(nextFilter) ? nextFilter : DEFAULT_SAVED_FILTER;
  savedCustomFilterBtnEls.forEach(btn => {
    const isActive = String(btn.dataset.savedFilter || "").toLowerCase() === activeSavedFilter;
    btn.classList.toggle("active", isActive);
  });
}

function isValidSavedSort(value) {
  return value === SORT_UPDATED || value === SORT_SAVED || value === SORT_REMINDER || value === SORT_PERSONAL;
}

function setSavedSort(nextSort) {
  activeSavedSort = isValidSavedSort(nextSort) ? nextSort : SORT_UPDATED;
  savedSortBtnEls.forEach(btn => {
    const isActive = String(btn.dataset.savedSort || "").toLowerCase() === activeSavedSort;
    btn.classList.toggle("active", isActive);
  });
}

function setSavedSortBarVisible(visible) {
  if (!savedSortBarEl) return;
  savedSortBarEl.classList.toggle("hidden", !visible);
  savedSortBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
}

function sortSavedJobs(jobs, mode) {
  const rows = Array.isArray(jobs) ? [...jobs] : [];
  const byUpdated = (a, b) => String(b.updatedAt || b.savedAt || "").localeCompare(String(a.updatedAt || a.savedAt || ""));
  const bySaved = (a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || ""));
  const byTitle = (a, b) => String(a.title || "").localeCompare(String(b.title || ""));
  if (mode === SORT_SAVED) {
    return rows.sort((a, b) => bySaved(a, b) || byTitle(a, b));
  }
  if (mode === SORT_PERSONAL) {
    return rows.sort((a, b) => {
      const customA = isCustomJob(a) ? 0 : 1;
      const customB = isCustomJob(b) ? 0 : 1;
      if (customA !== customB) return customA - customB;
      return byUpdated(a, b) || byTitle(a, b);
    });
  }
  if (mode === SORT_REMINDER) {
    return rows.sort((a, b) => {
      const reminderA = getReminderWeight(a.reminderAt);
      const reminderB = getReminderWeight(b.reminderAt);
      if (reminderA !== reminderB) return reminderA - reminderB;
      return byUpdated(a, b) || byTitle(a, b);
    });
  }
  return rows.sort((a, b) => byUpdated(a, b) || byTitle(a, b));
}

function getReminderWeight(reminderAt) {
  const parsed = parseIsoDate(reminderAt);
  if (!parsed) return 3;
  const diff = parsed.getTime() - Date.now();
  if (diff < 0) return 2;
  if (diff <= REMINDER_SOON_HOURS * 60 * 60 * 1000) return 0;
  return 1;
}

function setSavedFilterBarVisible(visible) {
  if (!savedCustomFilterBarEl) return;
  savedCustomFilterBarEl.classList.toggle("hidden", !visible);
  savedCustomFilterBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
}

function renderSavedFilterMeta(totalCount, filteredCount) {
  if (!savedCustomFilterCountEl) return;
  const safeTotal = Math.max(0, Number(totalCount) || 0);
  const safeFiltered = Math.max(0, Number(filteredCount) || 0);
  if (safeTotal <= 0) {
    savedCustomFilterCountEl.textContent = "";
    return;
  }
  savedCustomFilterCountEl.textContent = `${safeFiltered}/${safeTotal}`;
}

function renderReminderCounter(allJobs) {
  if (!savedReminderCounterEl) return;
  const rows = Array.isArray(allJobs) ? allJobs : [];
  const soonCount = rows.filter(job => getReminderMeta(job?.reminderAt).isSoon).length;
  savedReminderCounterEl.textContent = soonCount > 0 ? `${soonCount} due soon` : "";
}

function toCanonicalCountry(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const upper = raw.toUpperCase();
  if (upper === "NETHERLANDS") return "NL";
  if (upper === "UNITED STATES" || upper === "USA" || upper === "US") return "US";
  if (upper === "UNITED KINGDOM" || upper === "UK" || upper === "GB") return "GB";
  return raw.length === 2 ? upper : raw;
}

function normalizeCustomJobInput(values) {
  const title = String(values?.title || "").trim();
  const company = String(values?.company || "").trim();
  const reminderAt = normalizeReminderInput(values?.reminderAt);
  return {
    title,
    company,
    city: String(values?.city || "").trim(),
    country: toCanonicalCountry(values?.country),
    workType: String(values?.workType || "").trim() || "Onsite",
    contractType: String(values?.contractType || "").trim() || "Unknown",
    sector: String(values?.sector || "").trim() || "Tech",
    profession: String(values?.profession || "").trim(),
    jobLink: String(values?.jobLink || "").trim(),
    notes: String(values?.notes || "").trim(),
    reminderAt,
    isCustom: true,
    customSourceLabel: CUSTOM_SOURCE_LABEL
  };
}

function normalizeReminderInput(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString();
}

function toDatetimeLocalValue(value) {
  const parsed = parseIsoDate(value);
  if (!parsed) return "";
  const offsetMs = parsed.getTimezoneOffset() * 60 * 1000;
  const local = new Date(parsed.getTime() - offsetMs);
  return local.toISOString().slice(0, 16);
}

function resetCustomJobForm() {
  customJobMode = "create";
  customJobTargetKey = "";
  customJobFormEl?.reset();
  if (customJobWorkTypeEl) customJobWorkTypeEl.value = "";
  if (customJobContractTypeEl) customJobContractTypeEl.value = "";
  if (customJobSectorEl) customJobSectorEl.value = "";
  if (customJobReminderEl) customJobReminderEl.value = "";
  if (customJobPanelTitleEl) customJobPanelTitleEl.textContent = "Add Custom Job";
  if (customJobPanelHintEl) customJobPanelHintEl.textContent = "Required: Title and Company. Job link is optional.";
  if (customJobSaveBtnEl) customJobSaveBtnEl.textContent = "Save Custom Job";
  updateCustomJobWarning();
}

function updateCustomJobWarning() {
  if (!customJobLinkWarningEl) return;
  const hasLink = Boolean(String(customJobLinkEl?.value || "").trim());
  customJobLinkWarningEl.classList.toggle("hidden", hasLink);
}

function setCustomJobAvailability(enabled) {
  if (!addCustomJobBtnEl) return;
  addCustomJobBtnEl.disabled = !enabled;
}

function setCustomJobPanelOpen(open) {
  customJobPanelOpen = Boolean(open);
  if (!customJobPanelEl) return;
  customJobPanelEl.classList.toggle("hidden", !customJobPanelOpen);
  customJobPanelEl.setAttribute("aria-hidden", customJobPanelOpen ? "false" : "true");
  if (addCustomJobBtnEl) {
    addCustomJobBtnEl.classList.toggle("active", customJobPanelOpen);
    addCustomJobBtnEl.textContent = customJobPanelOpen ? "Close Custom Job Form" : "+ Add Custom Job";
  }
  if (!customJobPanelOpen) {
    resetCustomJobForm();
  } else {
    updateCustomJobWarning();
  }
}

function openCustomJobEditor(jobKey, duplicate) {
  const row = lastSavedJobsByKey.get(String(jobKey || ""));
  if (!row || !isCustomJob(row)) {
    showToast("Custom job not found.", "error");
    return;
  }
  customJobMode = duplicate ? "duplicate" : "edit";
  customJobTargetKey = duplicate ? "" : String(row.jobKey || "");
  if (customJobTitleEl) customJobTitleEl.value = row.title || "";
  if (customJobCompanyEl) customJobCompanyEl.value = row.company || "";
  if (customJobCityEl) customJobCityEl.value = row.city || "";
  if (customJobCountryEl) customJobCountryEl.value = row.country || "";
  if (customJobWorkTypeEl) customJobWorkTypeEl.value = row.workType || "";
  if (customJobContractTypeEl) customJobContractTypeEl.value = row.contractType || "";
  if (customJobSectorEl) customJobSectorEl.value = row.sector || "";
  if (customJobProfessionEl) customJobProfessionEl.value = row.profession || "";
  if (customJobLinkEl) customJobLinkEl.value = row.jobLink || "";
  if (customJobNotesEl) customJobNotesEl.value = row.notes || "";
  if (customJobReminderEl) customJobReminderEl.value = toDatetimeLocalValue(row.reminderAt);
  if (customJobPanelTitleEl) {
    customJobPanelTitleEl.textContent = duplicate ? "Duplicate Custom Job" : "Edit Custom Job";
  }
  if (customJobPanelHintEl) {
    customJobPanelHintEl.textContent = duplicate
      ? "Create a new custom entry using this job as a template."
      : "Update this custom job while keeping its history and status.";
  }
  if (customJobSaveBtnEl) {
    customJobSaveBtnEl.textContent = duplicate ? "Save Duplicate" : "Update Custom Job";
  }
  setCustomJobPanelOpen(true);
  customJobTitleEl?.focus();
  updateCustomJobWarning();
}

async function createCustomJob() {
  const api = window.JobAppLocalData;
  if (!api || !currentUser) {
    showToast("Sign in required.", "error");
    return;
  }
  const normalized = normalizeCustomJobInput({
    title: customJobTitleEl?.value,
    company: customJobCompanyEl?.value,
    city: customJobCityEl?.value,
    country: customJobCountryEl?.value,
    workType: customJobWorkTypeEl?.value,
    contractType: customJobContractTypeEl?.value,
    sector: customJobSectorEl?.value,
    profession: customJobProfessionEl?.value,
    jobLink: customJobLinkEl?.value,
    notes: customJobNotesEl?.value,
    reminderAt: customJobReminderEl?.value
  });

  if (!normalized.title || !normalized.company) {
    showToast("Title and Company are required.", "error");
    return;
  }

  try {
    let eventType = "custom_job_created";
    let message = "Custom job saved.";
    if (customJobMode === "edit") {
      normalized.jobKey = customJobTargetKey;
      normalized.updatedBy = "manual_edit";
      eventType = "custom_job_updated";
      message = "Custom job updated.";
    } else if (customJobMode === "duplicate") {
      normalized.updatedBy = "manual_duplicate";
      normalized.keySalt = String(Date.now());
      eventType = "custom_job_duplicated";
      message = "Custom job duplicated.";
    } else {
      normalized.updatedBy = "manual_create";
    }
    await api.saveJobForUser(currentUser.uid, normalized, { eventType });
    showToast(message, "success");
    setCustomJobPanelOpen(false);
    await refreshActivityLog();
  } catch (err) {
    console.error("Could not save custom job:", err);
    showToast("Could not save custom job.", "error");
  }
}

function setSourceStatus(text) {
  if (!savedSourceStatusEl) return;
  savedSourceStatusEl.textContent = text;
}

function setActivityStatus(text) {
  if (!activityPanelStatusEl) return;
  activityPanelStatusEl.textContent = text;
}

function setActivityPanelOpen(open) {
  activityPanelOpen = Boolean(open);
  if (!activityPanelEl) return;
  activityPanelEl.classList.toggle("collapsed", !activityPanelOpen);
  activityPanelEl.setAttribute("aria-hidden", activityPanelOpen ? "false" : "true");
  if (historyPanelToggleBtnEl) {
    historyPanelToggleBtnEl.classList.toggle("hidden", activityPanelOpen);
    historyPanelToggleBtnEl.classList.toggle("active", activityPanelOpen);
  }
}

function toggleAuthButtons(isSignedIn) {
  if (signInBtnEl) signInBtnEl.classList.toggle("hidden", isSignedIn);
  if (signOutBtnEl) signOutBtnEl.classList.toggle("hidden", !isSignedIn);
}

function setBackupButtonsEnabled(enabled) {
  if (exportBackupBtnEl) exportBackupBtnEl.disabled = !enabled;
  if (exportIncludeFilesEl) exportIncludeFilesEl.disabled = !enabled;
  if (importBackupBtnEl) importBackupBtnEl.disabled = !enabled;
  if (globalPhaseOverrideBtnEl) globalPhaseOverrideBtnEl.disabled = !enabled;
  updateGlobalOverrideButton();
}

function updateGlobalOverrideButton() {
  if (!globalPhaseOverrideBtnEl) return;
  globalPhaseOverrideBtnEl.classList.toggle("active", phaseOverrideArmedGlobal);
  globalPhaseOverrideBtnEl.textContent = phaseOverrideArmedGlobal
    ? "Override Armed (One Use)"
    : "Override Phase Lock";
}

async function refreshActivityLog() {
  const api = window.JobAppLocalData;
  if (!activityPanelBodyEl) return;
  if (!currentUser || !api) {
    setActivityStatus("Sign in to view history.");
    renderActivityEntries([]);
    return;
  }

  setActivityStatus("Loading activity...");
  try {
    const entries = await api.listActivityForUser(currentUser.uid, 400);
    cachedActivityEntries = Array.isArray(entries) ? entries : [];
    renderActivityEntries(entries);
    setActivityStatus(`Showing ${entries.length} recent events.`);
  } catch (err) {
    console.error("Could not load activity log:", err);
    cachedActivityEntries = [];
    setActivityStatus("Could not load history.");
    renderActivityEntries([]);
  }
}

function renderActivityEntries(entries) {
  if (!activityPanelBodyEl) return;
  if (!Array.isArray(entries) || entries.length === 0) {
    activityPanelBodyEl.innerHTML = '<div class="muted">No activity yet.</div>';
    return;
  }

  activityPanelBodyEl.innerHTML = entries.map(renderActivityEntry).join("");
}

function renderActivityEntry(entry) {
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

function activityTypeLabel(type) {
  switch (type) {
    case "job_saved":
      return "Saved";
    case "job_removed":
      return "Removed";
    case "phase_changed":
      return "Phase Changed";
    case "attachment_added":
      return "Attachment Added";
    case "attachment_deleted":
      return "Attachment Deleted";
    case "custom_job_created":
      return "Custom Job";
    case "custom_job_removed":
      return "Custom Job Removed";
    case "custom_job_updated":
      return "Custom Job Updated";
    case "custom_job_duplicated":
      return "Custom Job Duplicated";
    case "reminder_set":
      return "Reminder Set";
    case "reminder_cleared":
      return "Reminder Cleared";
    default:
      return "Event";
  }
}

function formatActivityDetail(entry) {
  const type = String(entry?.type || "event");
  const details = entry?.details && typeof entry.details === "object" ? entry.details : {};
  if (type === "phase_changed") {
    const from = PHASE_LABELS[normalizePhase(details.previousStatus)] || "Unknown";
    const to = PHASE_LABELS[normalizePhase(details.nextStatus)] || "Unknown";
    const override = details.overrideUsed ? " (override)" : "";
    return `${from} -> ${to}${override}`;
  }
  if (type === "job_removed") {
    const from = PHASE_LABELS[normalizePhase(details.fromStatus)] || "Saved";
    return `Removed from ${from}`;
  }
  if (type === "custom_job_created") {
    return "Created custom job entry";
  }
  if (type === "custom_job_removed") {
    return "Deleted custom job entry";
  }
  if (type === "custom_job_updated") {
    return "Updated custom job fields";
  }
  if (type === "custom_job_duplicated") {
    return "Created a duplicate custom entry";
  }
  if (type === "reminder_set") {
    if (details.reminderAt) {
      return `Reminder set for ${formatPhaseTimestamp(details.reminderAt) || "scheduled time"}`;
    }
    return "Reminder set";
  }
  if (type === "reminder_cleared") {
    return "Reminder removed";
  }
  if (type === "attachment_added") {
    const name = String(details.fileName || "file");
    return `Uploaded ${name}`;
  }
  if (type === "attachment_deleted") {
    return "Deleted an attachment";
  }
  return "Job table updated";
}

function getLastJobsUrl() {
  try {
    const url = sessionStorage.getItem(JOBS_LAST_URL_KEY);
    if (!url) return "jobs.html";
    if (!url.startsWith("/") && !url.startsWith("jobs.html")) return "jobs.html";
    return url;
  } catch {
    return "jobs.html";
  }
}

async function signInUser() {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    showToast("Local auth provider unavailable.", "error");
    return;
  }
  try {
    await api.signIn();
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("cancel")) return;
    console.error("Sign-in failed:", err);
    showToast("Sign-in failed.", "error");
  }
}

async function signOutUser() {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) return;
  try {
    await api.signOut();
  } catch (err) {
    console.error("Sign-out failed:", err);
    showToast("Sign-out failed.", "error");
  }
}

async function exportBackup() {
  const api = window.JobAppLocalData;
  if (!currentUser || !api) return;

  try {
    const includeFiles = Boolean(exportIncludeFilesEl?.checked);
    const payload = await api.exportProfileData(currentUser.uid, {
      includeFiles
    });
    const date = new Date().toISOString().slice(0, 10);
    let blob;
    let filename;
    if (includeFiles) {
      blob = await buildBackupZipBlob(payload);
      filename = `baluffo-backup-${currentUser.uid}-${date}.zip`;
    } else {
      const text = JSON.stringify(payload, null, 2);
      blob = new Blob([text], { type: "application/json" });
      filename = `baluffo-backup-${currentUser.uid}-${date}.json`;
    }
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    const counts = payload?.counts || {};
    const schemaVersion = payload?.schemaVersion ?? payload?.version ?? "?";
    const jobsCount = Number(counts.savedJobs) || 0;
    const historyCount = Number(counts.historyEvents) || 0;
    const attachmentsCount = Number(counts.attachments) || 0;
    showToast(
      `Backup exported (v${schemaVersion}) · Jobs ${jobsCount} · History ${historyCount} · Attachments ${attachmentsCount}`,
      "success",
      { durationMs: 4600 }
    );
  } catch (err) {
    console.error("Backup export failed:", err);
    showToast("Could not export backup.", "error");
  }
}

async function importBackup(file) {
  const api = window.JobAppLocalData;
  if (!currentUser || !api) return;

  try {
    const payload = await parseBackupInputFile(file);
    const result = await api.importProfileData(currentUser.uid, payload);
    const created = Number(result?.created) || 0;
    const updated = Number(result?.updated) || 0;
    const skippedInvalid = Number(result?.skippedInvalid) || 0;
    const historyAdded = Number(result?.historyAdded) || 0;
    const attachmentsAdded = Number(result?.attachmentsAdded) || 0;
    const attachmentsHydrated = Number(result?.attachmentsHydrated) || 0;
    const warningCount = Array.isArray(result?.warnings) ? result.warnings.length : 0;
    showToast(
      `Backup imported · Created ${created} · Updated ${updated} · Skipped ${skippedInvalid} · History +${historyAdded} · Attachments +${attachmentsAdded} · Files hydrated ${attachmentsHydrated}`,
      "success",
      { durationMs: 6400 }
    );
    if (warningCount > 0) {
      showToast(`${warningCount} non-fatal import warnings.`, "info", { durationMs: 4200 });
    }
    await refreshActivityLog();
  } catch (err) {
    console.error("Backup import failed:", err);
    showToast("Could not import backup file.", "error");
  }
}

async function parseBackupInputFile(file) {
  const name = String(file?.name || "").toLowerCase();
  const type = String(file?.type || "").toLowerCase();
  const isZip = name.endsWith(".zip") || type === "application/zip" || type === "application/x-zip-compressed";
  if (!isZip) {
    const text = await file.text();
    return JSON.parse(text);
  }
  return readBackupPayloadFromZip(file);
}

async function buildBackupZipBlob(payload) {
  const packPayload = JSON.parse(JSON.stringify(payload || {}));
  const attachments = Array.isArray(packPayload.attachments) ? packPayload.attachments : [];
  const entries = [];
  const filePathByFingerprint = new Map();

  attachments.forEach((att, idx) => {
    const dataUrl = String(att?.blobDataUrl || "");
    if (!dataUrl) return;
    const parsed = parseDataUrl(dataUrl);
    if (!parsed) return;
    const safeName = sanitizeBackupFileName(att?.name || `attachment-${idx + 1}`);
    const crc = getCrc32(parsed.bytes);
    const fingerprint = `${String(parsed.mime || "").toLowerCase()}|${parsed.bytes.length}|${crc}`;
    let filePath = filePathByFingerprint.get(fingerprint) || "";
    if (!filePath) {
      filePath = `files/${String(att?.id || `att_${idx + 1}`)}-${safeName}`;
      entries.push({
        name: filePath,
        bytes: parsed.bytes
      });
      filePathByFingerprint.set(fingerprint, filePath);
    }
    att.filePath = filePath;
    delete att.blobDataUrl;
  });

  packPayload.packageFormat = "zip-v1";
  packPayload.includesFiles = true;
  entries.unshift({
    name: "backup.json",
    bytes: utf8Encode(JSON.stringify(packPayload, null, 2))
  });

  return buildZipStoreOnly(entries);
}

async function readBackupPayloadFromZip(file) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  const files = parseZipStoreOnly(bytes);
  const backupEntry = files.get("backup.json");
  if (!backupEntry) {
    throw new Error("ZIP backup is missing backup.json.");
  }
  const payload = JSON.parse(utf8Decode(backupEntry));
  const attachments = Array.isArray(payload.attachments) ? payload.attachments : [];
  for (const att of attachments) {
    if (!att || typeof att !== "object") continue;
    if (att.blobDataUrl) continue;
    const filePath = String(att.filePath || "").trim();
    if (!filePath) continue;
    const content = files.get(filePath);
    if (!content) continue;
    att.blobDataUrl = toDataUrl(content, String(att.type || "application/octet-stream"));
  }
  return payload;
}

function sanitizeBackupFileName(name) {
  const raw = String(name || "").trim() || "file.bin";
  return raw.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function parseDataUrl(dataUrl) {
  const text = String(dataUrl || "");
  const parts = text.split(",");
  if (parts.length !== 2) return null;
  const header = parts[0];
  const body = parts[1];
  if (!/;base64$/i.test(header)) return null;
  const mimeMatch = header.match(/^data:([^;]+);base64$/i);
  const mime = mimeMatch ? String(mimeMatch[1] || "").trim().toLowerCase() : "application/octet-stream";
  const binary = atob(body);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return { bytes: out, mime };
}

function toDataUrl(bytes, mimeType) {
  let binary = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return `data:${mimeType};base64,${btoa(binary)}`;
}

function utf8Encode(text) {
  return new TextEncoder().encode(String(text || ""));
}

function utf8Decode(bytes) {
  return new TextDecoder().decode(bytes);
}

function getCrc32(bytes) {
  let crc = -1;
  for (let i = 0; i < bytes.length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ bytes[i]) & 0xff];
  }
  return (crc ^ -1) >>> 0;
}

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[n] = c >>> 0;
  }
  return table;
})();

function makeDosTimeDate(date = new Date()) {
  const year = Math.max(1980, date.getFullYear());
  const month = date.getMonth() + 1;
  const day = date.getDate();
  const hours = date.getHours();
  const mins = date.getMinutes();
  const secs = Math.floor(date.getSeconds() / 2);
  const dosTime = (hours << 11) | (mins << 5) | secs;
  const dosDate = ((year - 1980) << 9) | (month << 5) | day;
  return { dosTime, dosDate };
}

function concatUint8(parts) {
  const total = parts.reduce((sum, p) => sum + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function buildZipStoreOnly(entries) {
  const localParts = [];
  const centralParts = [];
  let offset = 0;
  const now = new Date();

  for (const entry of entries) {
    const nameBytes = utf8Encode(entry.name);
    const dataBytes = entry.bytes instanceof Uint8Array ? entry.bytes : new Uint8Array();
    const crc = getCrc32(dataBytes);
    const size = dataBytes.length >>> 0;
    const { dosTime, dosDate } = makeDosTimeDate(now);

    const localHeader = new Uint8Array(30 + nameBytes.length);
    const lh = new DataView(localHeader.buffer);
    lh.setUint32(0, 0x04034b50, true);
    lh.setUint16(4, 20, true);
    lh.setUint16(6, 0, true);
    lh.setUint16(8, 0, true);
    lh.setUint16(10, dosTime, true);
    lh.setUint16(12, dosDate, true);
    lh.setUint32(14, crc, true);
    lh.setUint32(18, size, true);
    lh.setUint32(22, size, true);
    lh.setUint16(26, nameBytes.length, true);
    lh.setUint16(28, 0, true);
    localHeader.set(nameBytes, 30);
    localParts.push(localHeader, dataBytes);

    const central = new Uint8Array(46 + nameBytes.length);
    const ch = new DataView(central.buffer);
    ch.setUint32(0, 0x02014b50, true);
    ch.setUint16(4, 20, true);
    ch.setUint16(6, 20, true);
    ch.setUint16(8, 0, true);
    ch.setUint16(10, 0, true);
    ch.setUint16(12, dosTime, true);
    ch.setUint16(14, dosDate, true);
    ch.setUint32(16, crc, true);
    ch.setUint32(20, size, true);
    ch.setUint32(24, size, true);
    ch.setUint16(28, nameBytes.length, true);
    ch.setUint16(30, 0, true);
    ch.setUint16(32, 0, true);
    ch.setUint16(34, 0, true);
    ch.setUint16(36, 0, true);
    ch.setUint32(38, 0, true);
    ch.setUint32(42, offset, true);
    central.set(nameBytes, 46);
    centralParts.push(central);

    offset += localHeader.length + dataBytes.length;
  }

  const centralData = concatUint8(centralParts);
  const eocd = new Uint8Array(22);
  const e = new DataView(eocd.buffer);
  e.setUint32(0, 0x06054b50, true);
  e.setUint16(4, 0, true);
  e.setUint16(6, 0, true);
  e.setUint16(8, entries.length, true);
  e.setUint16(10, entries.length, true);
  e.setUint32(12, centralData.length, true);
  e.setUint32(16, offset, true);
  e.setUint16(20, 0, true);

  return new Blob([concatUint8([...localParts, centralData, eocd])], { type: "application/zip" });
}

function parseZipStoreOnly(bytes) {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let eocdOffset = -1;
  for (let i = bytes.length - 22; i >= Math.max(0, bytes.length - 65557); i--) {
    if (dv.getUint32(i, true) === 0x06054b50) {
      eocdOffset = i;
      break;
    }
  }
  if (eocdOffset < 0) {
    throw new Error("Invalid ZIP: end of central directory not found.");
  }
  const totalEntries = dv.getUint16(eocdOffset + 10, true);
  const centralSize = dv.getUint32(eocdOffset + 12, true);
  const centralOffset = dv.getUint32(eocdOffset + 16, true);
  const out = new Map();

  let ptr = centralOffset;
  const centralEnd = centralOffset + centralSize;
  for (let i = 0; i < totalEntries && ptr < centralEnd; i++) {
    if (dv.getUint32(ptr, true) !== 0x02014b50) {
      throw new Error("Invalid ZIP: bad central directory header.");
    }
    const method = dv.getUint16(ptr + 10, true);
    const compressedSize = dv.getUint32(ptr + 20, true);
    const fileNameLen = dv.getUint16(ptr + 28, true);
    const extraLen = dv.getUint16(ptr + 30, true);
    const commentLen = dv.getUint16(ptr + 32, true);
    const localOffset = dv.getUint32(ptr + 42, true);
    const nameBytes = bytes.subarray(ptr + 46, ptr + 46 + fileNameLen);
    const fileName = utf8Decode(nameBytes);
    ptr += 46 + fileNameLen + extraLen + commentLen;

    if (method !== 0) {
      throw new Error(`Unsupported ZIP compression for ${fileName}.`);
    }

    if (dv.getUint32(localOffset, true) !== 0x04034b50) {
      throw new Error("Invalid ZIP: bad local header.");
    }
    const localNameLen = dv.getUint16(localOffset + 26, true);
    const localExtraLen = dv.getUint16(localOffset + 28, true);
    const dataOffset = localOffset + 30 + localNameLen + localExtraLen;
    const fileBytes = bytes.subarray(dataOffset, dataOffset + compressedSize);
    out.set(fileName, new Uint8Array(fileBytes));
  }
  return out;
}

function sanitizeUrl(url) {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.href;
    }
    return "";
  } catch {
    return "";
  }
}

function toContractClass(contractType) {
  const normalized = (contractType || "").toLowerCase();
  if (normalized === "full-time") return "full-time";
  if (normalized === "internship") return "internship";
  if (normalized === "temporary") return "temporary";
  return "unknown";
}

function fullCountryName(code) {
  const map = {
    US: "United States",
    CA: "Canada",
    GB: "United Kingdom",
    DE: "Germany",
    FI: "Finland",
    JP: "Japan",
    AU: "Australia",
    SG: "Singapore",
    FR: "France",
    NL: "Netherlands",
    SE: "Sweden",
    NO: "Norway",
    DK: "Denmark",
    ES: "Spain",
    IT: "Italy",
    BR: "Brazil",
    IN: "India",
    Remote: "Remote"
  };
  return map[code] || code;
}

function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function showToast(message, type = "info", options = {}) {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  const messageSpan = document.createElement("span");
  messageSpan.textContent = message;
  toast.appendChild(messageSpan);

  const hasAction = typeof options?.onAction === "function" && options?.actionLabel;
  if (hasAction) {
    const actionBtn = document.createElement("button");
    actionBtn.type = "button";
    actionBtn.className = "toast-action-btn";
    actionBtn.textContent = String(options.actionLabel);
    actionBtn.addEventListener("click", async () => {
      try {
        await options.onAction();
      } finally {
        toast.classList.remove("visible");
        setTimeout(() => toast.remove(), 220);
      }
    });
    toast.appendChild(actionBtn);
  }

  document.body.appendChild(toast);

  requestAnimationFrame(() => toast.classList.add("visible"));

  const durationMs = Number(options?.durationMs) > 0 ? Number(options.durationMs) : 2600;
  setTimeout(() => {
    toast.classList.remove("visible");
    setTimeout(() => toast.remove(), 220);
  }, durationMs);
}

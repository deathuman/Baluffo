let savedJobsListEl;
let savedSourceStatusEl;
let savedAuthStatusEl;
let savedAuthStatusHintEl;
let savedAuthAvatarEl;
let signInBtnEl;
let signOutBtnEl;
let jobsPageBtnEl;
let adminPageBtnEl;
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
let lastSavedJobsByKey = new Map();
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";

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
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    setAuthStatus("Browsing as guest");
    setSourceStatus("Local storage provider unavailable.");
    setActivityStatus("Local provider unavailable.");
    toggleAuthButtons(false);
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
    lastSavedJobsByKey = new Map();

    if (!currentUser) {
      setAuthStatus("Browsing as guest");
      setSourceStatus("Sign in to view your saved jobs.");
      setActivityStatus("Sign in to view history.");
      toggleAuthButtons(false);
      setBackupButtonsEnabled(false);
      renderAuthRequired("Sign in to access your personal saved jobs table.");
      renderActivityEntries([]);
      return;
    }

    setAuthStatus(`Signed in as ${currentUser.displayName || currentUser.email || "user"}`);
    setSourceStatus("Loading your saved jobs...");
    setActivityStatus("Loading activity...");
    toggleAuthButtons(true);
    setBackupButtonsEnabled(true);
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

  if (!jobs || jobs.length === 0) {
    expandedJobKey = null;
    savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs yet.</div>';
    return;
  }
  if (!jobs.some(job => String(job.jobKey || job.id || "") === expandedJobKey)) {
    expandedJobKey = null;
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
      ${jobs.map(renderSavedJobBlock).join("")}
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

  hydrateAttachmentLists(jobs).catch(err => {
    console.error("Could not load attachment lists:", err);
  });
}

function renderSavedJobBlock(job) {
  const safeTitle = escapeHtml(job.title || "");
  const safeCompany = escapeHtml(job.company || "");
  const safeSector = escapeHtml(normalizeSavedSector(job));
  const safeCity = escapeHtml(job.city || "");
  const safeCountry = escapeHtml(fullCountryName(job.country || ""));
  const safeContract = escapeHtml(job.contractType || "Unknown");
  const safeWorkType = escapeHtml(job.workType || "Onsite");
  const safeLink = sanitizeUrl(job.jobLink || "");
  const contractClass = toContractClass(job.contractType || "Unknown");
  const rawJobKey = String(job.jobKey || job.id || "");
  const jobKey = escapeHtml(rawJobKey);
  const normalizedPhase = normalizePhase(job.applicationStatus);
  const isExpanded = expandedJobKey === rawJobKey;
  const detailsSummary = renderDetailsSummary(job);

  return `
    <div class="saved-job-block" data-job-key="${jobKey}">
      <div class="saved-job-row">
        <button class="remove-saved-btn remove-inline-btn" data-job-key="${jobKey}" aria-label="Remove saved job">X</button>
        <div class="col-title job-cell" data-label="Position">${safeTitle}</div>
        <div class="col-company job-cell" data-label="Company">${safeCompany}</div>
        <div class="col-sector job-cell" data-label="Sector">${safeSector}</div>
        <div class="col-city job-cell" data-label="City">${safeCity}</div>
        <div class="col-country job-cell" data-label="Country">${safeCountry}</div>
        <div class="col-contract job-cell" data-label="Contract">
          <span class="job-contract ${contractClass}">${safeContract}</span>
        </div>
        <div class="col-type job-cell" data-label="Type">
          <span class="job-tag ${safeWorkType.toLowerCase()}">${safeWorkType}</span>
        </div>
        <div class="col-link job-cell" data-label="Link">
          ${safeLink ? `<a class="saved-open-link-icon" href="${safeLink}" target="_blank" rel="noopener noreferrer" aria-label="Open job link" title="Open job link">${renderWebIcon()}</a>` : '<span class="muted">N/A</span>'}
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
          <span class="details-toggle-text">${detailsSummary}Notes & Attachments</span>
          <span class="details-toggle-arrow" aria-hidden="true">${isExpanded ? "v" : ">"}</span>
        </button>
      </div>
      <div class="saved-details-section ${isExpanded ? "" : "collapsed"}" data-job-key="${jobKey}" aria-hidden="${isExpanded ? "false" : "true"}">
        <div class="saved-notes-row">
          <div class="notes-label">Notes</div>
          <div class="notes-value">
            <textarea class="job-notes-input" data-job-key="${jobKey}" placeholder="Add notes, links, interview reminders..." ${!currentUser ? "disabled" : ""}>${escapeHtml(job.notes || "")}</textarea>
            <div class="note-save-state" data-job-key="${jobKey}">Saved</div>
          </div>
        </div>
        <div class="saved-attachments-row">
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
  try {
    await api.removeSavedJobForUser(currentUser.uid, jobKey);
    showToast("Removed saved job.", "success");
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
    await api.updateApplicationStatus(currentUser.uid, jobKey, normalized, {
      override: !regularAllowed && overrideArmed
    });
    if (overrideArmed) {
      phaseOverrideArmedGlobal = false;
      updateGlobalOverrideButton();
    }
    showToast(`Phase updated to ${PHASE_LABELS[normalized] || normalized}.`, "success");
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
  expandedJobKey = expandedJobKey === jobKey ? null : jobKey;
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
      toggle.setAttribute("aria-label", `${expanded ? "Collapse" : "Expand"} notes and attachments`);
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
    renderActivityEntries(entries);
    setActivityStatus(`Showing ${entries.length} recent events.`);
  } catch (err) {
    console.error("Could not load activity log:", err);
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
    const payload = await api.exportProfileData(currentUser.uid, {
      includeFiles: Boolean(exportIncludeFilesEl?.checked)
    });
    const text = JSON.stringify(payload, null, 2);
    const blob = new Blob([text], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const date = new Date().toISOString().slice(0, 10);
    a.href = url;
    a.download = `baluffo-backup-${currentUser.uid}-${date}.json`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    showToast("Backup exported.", "success");
  } catch (err) {
    console.error("Backup export failed:", err);
    showToast("Could not export backup.", "error");
  }
}

async function importBackup(file) {
  const api = window.JobAppLocalData;
  if (!currentUser || !api) return;

  try {
    const text = await file.text();
    const payload = JSON.parse(text);
    await api.importProfileData(currentUser.uid, payload);
    showToast("Backup imported.", "success");
  } catch (err) {
    console.error("Backup import failed:", err);
    showToast("Could not import backup file.", "error");
  }
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

function showToast(message, type = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  document.body.appendChild(toast);

  requestAnimationFrame(() => toast.classList.add("visible"));

  setTimeout(() => {
    toast.classList.remove("visible");
    setTimeout(() => toast.remove(), 220);
  }, 2600);
}

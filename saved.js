let savedJobsListEl;
let savedSourceStatusEl;
let savedAuthStatusEl;
let signInBtnEl;
let signOutBtnEl;
let jobsPageBtnEl;
let adminPageBtnEl;
let exportBackupBtnEl;
let exportIncludeFilesEl;
let importBackupBtnEl;
let importBackupInputEl;

let currentUser = null;
let unsubscribeSavedJobs = () => {};
let expandedJobKey = null;
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";

const PHASE_OPTIONS = ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"];
const PHASE_LABELS = {
  bookmark: "Bookmark",
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
  signInBtnEl = document.getElementById("saved-auth-sign-in-btn");
  signOutBtnEl = document.getElementById("saved-auth-sign-out-btn");
  jobsPageBtnEl = document.getElementById("jobs-page-btn");
  adminPageBtnEl = document.getElementById("admin-page-btn");
  exportBackupBtnEl = document.getElementById("export-backup-btn");
  exportIncludeFilesEl = document.getElementById("export-include-files");
  importBackupBtnEl = document.getElementById("import-backup-btn");
  importBackupInputEl = document.getElementById("import-backup-input");
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
}

function initSavedJobsPage() {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    setAuthStatus("Browsing as guest");
    setSourceStatus("Local storage provider unavailable.");
    toggleAuthButtons(false);
    renderAuthRequired("Local auth provider is unavailable.");
    return;
  }

  api.onAuthStateChanged(user => {
    currentUser = user || null;
    unsubscribeSavedJobs();
    unsubscribeSavedJobs = () => {};
    clearNoteSaveQueues();
    expandedJobKey = null;

    if (!currentUser) {
      setAuthStatus("Browsing as guest");
      setSourceStatus("Sign in to view your saved jobs.");
      toggleAuthButtons(false);
      setBackupButtonsEnabled(false);
      renderAuthRequired("Sign in to access your personal saved jobs table.");
      return;
    }

    setAuthStatus(`Signed in as ${currentUser.displayName || currentUser.email || "user"}`);
    setSourceStatus("Loading your saved jobs...");
    toggleAuthButtons(true);
    setBackupButtonsEnabled(true);
    subscribeToSavedJobs(currentUser.uid);
  });
}

function subscribeToSavedJobs(uid) {
  const api = window.JobAppLocalData;
  unsubscribeSavedJobs = api.subscribeSavedJobs(
    uid,
    jobs => {
      setSourceStatus(`Loaded ${jobs.length} saved jobs.`);
      renderSavedJobs(jobs);
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
        <div class="col-city">City</div>
        <div class="col-country">Country</div>
        <div class="col-contract">Contract</div>
        <div class="col-type">Type</div>
        <div class="col-link">Link</div>
        <div class="col-saved-date">Saved</div>
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
  const safeCompanyType = escapeHtml(job.companyType || "Tech");
  const safeCity = escapeHtml(job.city || "");
  const safeCountry = escapeHtml(fullCountryName(job.country || ""));
  const safeContract = escapeHtml(job.contractType || "Unknown");
  const safeWorkType = escapeHtml(job.workType || "Onsite");
  const safeLink = sanitizeUrl(job.jobLink || "");
  const savedDate = formatDate(job.savedAt);
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
        <div class="col-company job-cell" data-label="Company">${safeCompanyType}</div>
        <div class="col-city job-cell" data-label="City">${safeCity}</div>
        <div class="col-country job-cell" data-label="Country">${safeCountry}</div>
        <div class="col-contract job-cell" data-label="Contract">
          <span class="job-contract ${contractClass}">${safeContract}</span>
        </div>
        <div class="col-type job-cell" data-label="Type">
          <span class="job-tag ${safeWorkType.toLowerCase()}">${safeWorkType}</span>
        </div>
        <div class="col-link job-cell" data-label="Link">
          ${safeLink ? `<a class="saved-open-link" href="${safeLink}" target="_blank" rel="noopener noreferrer">Open</a>` : '<span class="muted">N/A</span>'}
        </div>
        <div class="col-saved-date job-cell" data-label="Saved">${escapeHtml(savedDate)}</div>
      </div>
      <div class="saved-phase-row">
        <div class="phase-label">Application Phase</div>
        <div class="phase-value">
          ${renderPhaseBar(jobKey, normalizedPhase)}
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

function renderPhaseBar(jobKey, activePhase) {
  const activeIndex = PHASE_OPTIONS.indexOf(activePhase);
  const segments = PHASE_OPTIONS.map((phase, idx) => {
    const isActive = idx === activeIndex;
    const isComplete = idx <= activeIndex;
    const classes = [
      "phase-step-btn",
      isActive ? "active" : "",
      isComplete ? "complete" : ""
    ].filter(Boolean).join(" ");

    return `
      <button
        class="${classes}"
        data-job-key="${jobKey}"
        data-phase="${phase}"
        ${!currentUser ? "disabled" : ""}
        aria-label="Set phase to ${escapeHtml(PHASE_LABELS[phase] || phase)}"
      >
        <span class="phase-step-text">${escapeHtml(PHASE_LABELS[phase] || phase)}</span>
      </button>
    `;
  }).join("");

  return `<div class="phase-bar" role="group" aria-label="Application phases">${segments}</div>`;
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

  const normalized = normalizePhase(phase);
  try {
    await api.updateApplicationStatus(currentUser.uid, jobKey, normalized);
    showToast(`Phase updated to ${PHASE_LABELS[normalized] || normalized}.`, "success");
  } catch (err) {
    console.error("Could not update phase:", err);
    showToast("Could not update phase.", "error");
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
  savedAuthStatusEl.textContent = text;
}

function setSourceStatus(text) {
  if (!savedSourceStatusEl) return;
  savedSourceStatusEl.textContent = text;
}

function toggleAuthButtons(isSignedIn) {
  if (signInBtnEl) signInBtnEl.classList.toggle("hidden", isSignedIn);
  if (signOutBtnEl) signOutBtnEl.classList.toggle("hidden", !isSignedIn);
}

function setBackupButtonsEnabled(enabled) {
  if (exportBackupBtnEl) exportBackupBtnEl.disabled = !enabled;
  if (exportIncludeFilesEl) exportIncludeFilesEl.disabled = !enabled;
  if (importBackupBtnEl) importBackupBtnEl.disabled = !enabled;
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

function formatDate(value) {
  if (!value) return "Unknown";
  if (value.toDate) {
    return value.toDate().toLocaleDateString();
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "Unknown";
  return parsed.toLocaleDateString();
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

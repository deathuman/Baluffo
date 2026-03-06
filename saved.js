let savedJobsListEl;
let savedSourceStatusEl;
let savedAuthStatusEl;
let signInBtnEl;
let signOutBtnEl;
let jobsPageBtnEl;
let exportBackupBtnEl;
let importBackupBtnEl;
let importBackupInputEl;

let currentUser = null;
let unsubscribeSavedJobs = () => {};
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
  exportBackupBtnEl = document.getElementById("export-backup-btn");
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
    savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs yet.</div>';
    return;
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
  const jobKey = escapeHtml(job.jobKey || job.id || "");
  const normalizedPhase = normalizePhase(job.applicationStatus);

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
    const payload = await api.exportProfileData(currentUser.uid);
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

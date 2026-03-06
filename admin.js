let adminSourceStatusEl;
let adminPinGateEl;
let adminContentEl;
let adminPinInputEl;
let adminUnlockBtnEl;
let adminLockBtnEl;
let adminRefreshBtnEl;
let adminTotalsEl;
let adminUsersListEl;
let adminJobsBtnEl;
let adminSavedBtnEl;

let adminPin = "";
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";

document.addEventListener("DOMContentLoaded", () => {
  cacheDom();
  bindEvents();
  initAdminPage();
});

function cacheDom() {
  adminSourceStatusEl = document.getElementById("admin-source-status");
  adminPinGateEl = document.getElementById("admin-pin-gate");
  adminContentEl = document.getElementById("admin-content");
  adminPinInputEl = document.getElementById("admin-pin-input");
  adminUnlockBtnEl = document.getElementById("admin-unlock-btn");
  adminLockBtnEl = document.getElementById("admin-lock-btn");
  adminRefreshBtnEl = document.getElementById("admin-refresh-btn");
  adminTotalsEl = document.getElementById("admin-totals");
  adminUsersListEl = document.getElementById("admin-users-list");
  adminJobsBtnEl = document.getElementById("admin-jobs-btn");
  adminSavedBtnEl = document.getElementById("admin-saved-btn");
}

function bindEvents() {
  if (adminJobsBtnEl) {
    adminJobsBtnEl.addEventListener("click", () => {
      const target = getLastJobsUrl();
      window.location.href = target;
    });
  }

  if (adminSavedBtnEl) {
    adminSavedBtnEl.addEventListener("click", () => {
      window.location.href = "saved.html";
    });
  }

  if (adminUnlockBtnEl) {
    adminUnlockBtnEl.addEventListener("click", () => {
      unlockAdmin();
    });
  }

  if (adminPinInputEl) {
    adminPinInputEl.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        unlockAdmin();
      }
    });
  }

  if (adminRefreshBtnEl) {
    adminRefreshBtnEl.addEventListener("click", async () => {
      await refreshOverview();
    });
  }

  if (adminLockBtnEl) {
    adminLockBtnEl.addEventListener("click", () => {
      lockAdmin();
    });
  }
}

function initAdminPage() {
  const api = window.JobAppLocalData;
  if (!api || !api.isReady()) {
    setSourceStatus("Local storage provider unavailable.");
    if (adminPinGateEl) adminPinGateEl.classList.add("hidden");
    renderUsersEmpty("Admin view is unavailable in this browser.");
    return;
  }
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

function setSourceStatus(text) {
  if (!adminSourceStatusEl) return;
  adminSourceStatusEl.textContent = text;
}

function unlockAdmin() {
  const api = window.JobAppLocalData;
  const nextPin = String(adminPinInputEl?.value || "").trim();
  if (!nextPin) {
    showToast("Enter admin PIN.", "error");
    return;
  }
  if (!api || !api.verifyAdminPin || !api.verifyAdminPin(nextPin)) {
    showToast("Invalid admin PIN.", "error");
    setSourceStatus("Invalid PIN. Access denied.");
    return;
  }

  adminPin = nextPin;
  setSourceStatus("Admin access granted.");
  if (adminPinGateEl) adminPinGateEl.classList.add("hidden");
  if (adminContentEl) adminContentEl.classList.remove("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.remove("hidden");
  if (adminPinInputEl) adminPinInputEl.value = "";
  refreshOverview().catch(err => {
    console.error("Failed to refresh admin overview:", err);
  });
}

function lockAdmin() {
  adminPin = "";
  if (adminPinGateEl) adminPinGateEl.classList.remove("hidden");
  if (adminContentEl) adminContentEl.classList.add("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.add("hidden");
  renderUsersEmpty("");
  if (adminTotalsEl) adminTotalsEl.innerHTML = "";
  setSourceStatus("Enter admin PIN to access user overview.");
}

async function refreshOverview() {
  const api = window.JobAppLocalData;
  if (!api || !adminPin) return;

  setSourceStatus("Loading admin overview...");
  try {
    const overview = await api.getAdminOverview(adminPin);
    renderTotals(overview.totals);
    renderUsers(overview.users);
    setSourceStatus(`Loaded ${overview.users.length} user profiles.`);
  } catch (err) {
    console.error("Could not load admin overview:", err);
    showToast("Could not load admin overview.", "error");
    if (String(err?.message || "").toLowerCase().includes("pin")) {
      lockAdmin();
      return;
    }
    setSourceStatus("Could not load admin overview.");
  }
}

function renderTotals(totals) {
  if (!adminTotalsEl) return;
  if (!totals) {
    adminTotalsEl.innerHTML = "";
    return;
  }

  adminTotalsEl.innerHTML = `
    <div class="admin-total-card">
      <div class="admin-total-label">Users</div>
      <div class="admin-total-value">${Number(totals.usersCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Saved Jobs</div>
      <div class="admin-total-value">${Number(totals.savedJobsCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Notes Size</div>
      <div class="admin-total-value">${formatBytes(totals.notesBytes || 0)}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Attachments</div>
      <div class="admin-total-value">${Number(totals.attachmentsCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Attachment Size</div>
      <div class="admin-total-value">${formatBytes(totals.attachmentsBytes || 0)}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Total Size</div>
      <div class="admin-total-value">${formatBytes(totals.totalBytes || 0)}</div>
    </div>
  `;
}

function renderUsers(users) {
  if (!Array.isArray(users) || users.length === 0) {
    renderUsersEmpty("No stored profiles found.");
    return;
  }
  if (!adminUsersListEl) return;

  adminUsersListEl.innerHTML = `
    <div class="jobs-table-header">
      <div class="admin-row-header">
        <div>Name</div>
        <div>User ID</div>
        <div>Saved Jobs</div>
        <div>Notes Size</div>
        <div>Attachments</div>
        <div>Attachment Size</div>
        <div>Total Size</div>
        <div>Actions</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${users.map(renderUserRow).join("")}
    </div>
  `;

  adminUsersListEl.querySelectorAll(".admin-wipe-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const uid = btn.dataset.uid || "";
      const name = btn.dataset.name || uid;
      await wipeAccount(uid, name);
    });
  });
}

function renderUserRow(user) {
  const uid = escapeHtml(user.uid || "");
  const name = escapeHtml(user.name || user.uid || "Unknown");
  const email = escapeHtml(user.email || "");
  const label = email ? `${name} (${email})` : name;

  return `
    <div class="admin-user-row">
      <div class="admin-cell" data-label="Name">${label}</div>
      <div class="admin-cell admin-uid" data-label="User ID">${uid}</div>
      <div class="admin-cell" data-label="Saved Jobs">${Number(user.savedJobsCount || 0).toLocaleString()}</div>
      <div class="admin-cell" data-label="Notes Size">${formatBytes(user.notesBytes || 0)}</div>
      <div class="admin-cell" data-label="Attachments">${Number(user.attachmentsCount || 0).toLocaleString()}</div>
      <div class="admin-cell" data-label="Attachment Size">${formatBytes(user.attachmentsBytes || 0)}</div>
      <div class="admin-cell" data-label="Total Size">${formatBytes(user.totalBytes || 0)}</div>
      <div class="admin-cell" data-label="Actions">
        <button class="btn back-btn admin-wipe-btn" data-uid="${uid}" data-name="${name}">Wipe Account</button>
      </div>
    </div>
  `;
}

function renderUsersEmpty(message) {
  if (!adminUsersListEl) return;
  adminUsersListEl.innerHTML = message
    ? `<div class="no-results">${escapeHtml(message)}</div>`
    : "";
}

async function wipeAccount(uid, name) {
  if (!uid || !adminPin) return;
  const api = window.JobAppLocalData;
  if (!api) return;

  const confirmed = window.confirm(`Permanently wipe account "${name}"? This deletes profile, saved jobs, notes, and attachments.`);
  if (!confirmed) return;

  try {
    await api.wipeAccountAdmin(adminPin, uid);
    showToast(`Wiped account ${name}.`, "success");
    await refreshOverview();
  } catch (err) {
    console.error("Could not wipe account:", err);
    showToast("Could not wipe account.", "error");
    if (String(err?.message || "").toLowerCase().includes("pin")) {
      lockAdmin();
    }
  }
}

function formatBytes(bytes) {
  const value = Math.max(0, Number(bytes) || 0);
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
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

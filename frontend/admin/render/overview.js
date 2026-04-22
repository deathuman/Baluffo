import { escapeHtml } from "../../shared/ui/index.js";

export function renderTotalsHtml(totals, formatBytes) {
  if (!totals) return "";
  return `
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

function renderUserRowHtml(user, formatBytes) {
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
        <button class="btn back-btn admin-wipe-btn" data-ui="admin-wipe-btn" data-uid="${uid}" data-name="${name}">Wipe Account</button>
      </div>
    </div>
  `;
}

export function renderUsersTableHtml(users, formatBytes) {
  return `
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
      ${users.map(user => renderUserRowHtml(user, formatBytes)).join("")}
    </div>
  `;
}

export function renderUsersEmptyHtml(message) {
  return message ? `<div class="no-results">${escapeHtml(message)}</div>` : "";
}

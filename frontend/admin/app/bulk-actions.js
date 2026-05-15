function createElement(doc, tagName, attributes = {}, text = "") {
  const el = doc.createElement(tagName);
  Object.entries(attributes).forEach(([name, value]) => {
    if (value !== null && value !== undefined) el.setAttribute(name, String(value));
  });
  if (text) el.textContent = text;
  return el;
}

function moveButton(actionRow, button) {
  if (!actionRow || !button) return;
  actionRow.appendChild(button);
}

function removeIfEmpty(row) {
  if (!row || !row.parentNode) return;
  const hasElementChildren = Array.from(row.children || []).some(child => child && child.nodeType === 1);
  if (!hasElementChildren) {
    row.parentNode.removeChild(row);
  }
}

export function applyAdminAdvancedBulkLayout({ doc = globalThis.document, refs = {} } = {}) {
  if (!doc) return null;

  const approveBtn = refs.adminApproveSourcesBtnEl || doc.querySelector?.('[data-ui="admin-approve-sources-btn"]');
  const rejectBtn = refs.adminRejectSourcesBtnEl || doc.querySelector?.('[data-ui="admin-reject-sources-btn"]');
  const restoreBtn = refs.adminRestoreRejectedBtnEl || doc.querySelector?.('[data-ui="admin-restore-rejected-btn"]');
  const demoteBtn = refs.adminDemoteActiveBtnEl || doc.querySelector?.('[data-ui="admin-demote-active-btn"]');
  const deleteBtn = refs.adminDeleteSourcesBtnEl || doc.querySelector?.('[data-ui="admin-delete-sources-btn"]');
  const bulkCard = approveBtn?.closest?.(".admin-discovery-bulk-card") || doc.querySelector?.(".admin-discovery-bulk-card");
  if (!bulkCard || !approveBtn || !rejectBtn || !restoreBtn || !demoteBtn || !deleteBtn) return null;

  if (bulkCard.querySelector?.('[data-ui="admin-advanced-bulk-actions"]')) {
    refs.adminBulkBusyMessageEl = bulkCard.querySelector('[data-ui="admin-bulk-busy-message"]');
    return refs.adminBulkBusyMessageEl;
  }

  const primaryRow = approveBtn.closest?.(".admin-fetcher-actions") || bulkCard.querySelector?.(".admin-fetcher-actions");
  const demoteRow = demoteBtn.closest?.(".admin-fetcher-actions");
  const busyMessage = createElement(doc, "p", {
    "data-ui": "admin-bulk-busy-message",
    class: "admin-bulk-busy-message hidden",
    role: "status",
    "aria-live": "polite"
  });
  const details = createElement(doc, "details", {
    "data-ui": "admin-advanced-bulk-actions",
    class: "admin-advanced-bulk-details"
  });
  const summary = createElement(doc, "summary", { class: "admin-advanced-bulk-summary" }, "Advanced bulk actions");
  const note = createElement(
    doc,
    "p",
    { class: "admin-advanced-bulk-note" },
    "These actions change source registry state and may affect future job imports."
  );
  const advancedRow = createElement(doc, "div", { class: "admin-fetcher-actions admin-advanced-bulk-action-row" });

  details.appendChild(summary);
  details.appendChild(note);
  details.appendChild(advancedRow);
  bulkCard.insertBefore(busyMessage, primaryRow?.nextSibling || null);
  bulkCard.insertBefore(details, busyMessage.nextSibling || null);

  moveButton(primaryRow, approveBtn);
  moveButton(primaryRow, rejectBtn);
  moveButton(advancedRow, restoreBtn);
  moveButton(advancedRow, demoteBtn);
  moveButton(advancedRow, deleteBtn);
  removeIfEmpty(demoteRow);

  refs.adminBulkBusyMessageEl = busyMessage;
  return busyMessage;
}

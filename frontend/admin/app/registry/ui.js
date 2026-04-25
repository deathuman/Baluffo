import { renderSourcesTableHtml } from "../../render.js";

function isCheckedSourceInput(el) {
  if (typeof HTMLInputElement === "undefined") {
    return Boolean(el?.checked);
  }
  return el instanceof HTMLInputElement && el.checked;
}

function getBucketContainer(refs, type) {
  if (type === "pending") return refs.adminPendingSourcesEl;
  if (type === "active") return refs.adminActiveSourcesEl;
  if (type === "rejected") return refs.adminRejectedSourcesEl;
  return null;
}

function queryScopedSelector(container, selector) {
  if (container && typeof container.querySelectorAll === "function") {
    return Array.from(container.querySelectorAll(selector));
  }
  return [];
}

function selectedIds(container, selector) {
  return queryScopedSelector(container, selector)
    .filter(isCheckedSourceInput)
    .map(el => String(el?.dataset?.sourceId || ""))
    .filter(Boolean);
}

function selectedSourcesAcrossDiscoveryBuckets(refs) {
  const out = [];
  const seen = new Set();

  [
    ["pending", ".pending-source-checkbox"],
    ["active", ".active-source-checkbox"],
    ["rejected", ".rejected-source-checkbox"]
  ].forEach(([type, selector]) => {
    const container = getBucketContainer(refs, type);
    queryScopedSelector(container, selector)
      .filter(isCheckedSourceInput)
      .map(el => ({
        id: String(el?.dataset?.sourceId || "").trim(),
        url: String(el?.dataset?.sourceUrl || "").trim()
      }))
      .filter(item => item.id || item.url)
      .forEach(item => {
        const key = `${item.id}|${item.url}`;
        if (!key || seen.has(key)) return;
        seen.add(key);
        out.push(item);
      });
  });

  return out;
}

function toggleSelectAllSources(refs, type, checkAll) {
  const classMap = {
    pending: ".pending-source-checkbox",
    active: ".active-source-checkbox",
    rejected: ".rejected-source-checkbox"
  };
  const selector = classMap[type];
  if (!selector) return;
  queryScopedSelector(getBucketContainer(refs, type), selector).forEach(cb => {
    cb.checked = Boolean(checkAll);
  });
}

export function createRegistryUi({
  refs,
  getSourceJobsFoundCount,
  getSourceDiscoveryJobsCount = getSourceJobsFoundCount,
  deriveSourceStatus,
  deriveSourceApprovalStatus,
  renderSourcesTableHtml: renderSourcesTableHtmlImpl = renderSourcesTableHtml
}) {
  function setManualSourceFeedback(message, level = "muted") {
    if (!refs.adminManualSourceFeedbackEl) return;
    const normalized = String(level || "muted").toLowerCase();
    refs.adminManualSourceFeedbackEl.textContent = String(message || "");
    refs.adminManualSourceFeedbackEl.classList.remove("success", "warn", "error", "muted");
    refs.adminManualSourceFeedbackEl.classList.add(
      normalized === "success" ? "success" : normalized === "warn" ? "warn" : normalized === "error" ? "error" : "muted"
    );
  }

  function renderSourcesTable(container, rows, mode = "pending") {
    if (!container) return;
    container.innerHTML = renderSourcesTableHtmlImpl(rows, mode, row => {
      const value = mode === "pending"
        ? getSourceDiscoveryJobsCount(row)
        : getSourceJobsFoundCount(row);
      return Number.isFinite(value) && value >= 0 ? value.toLocaleString() : "N/A";
    }, deriveSourceStatus, deriveSourceApprovalStatus);
  }

  return {
    setManualSourceFeedback,
    renderSourcesTable,
    getBucketContainer: type => getBucketContainer(refs, type),
    selectedIds,
    selectedSourcesAcrossDiscoveryBuckets: () => selectedSourcesAcrossDiscoveryBuckets(refs),
    toggleSelectAllSources: (type, checkAll) => toggleSelectAllSources(refs, type, checkAll)
  };
}

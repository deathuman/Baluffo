const ENTITY_TYPES = {
  source: {
    label: "Source",
    dataAttr: "data-source-id",
    resolveData: function (target) {
      const checkbox = target.closest("[data-source-id]");
      if (!checkbox) return null;
      const sourceId = checkbox.dataset.sourceId || "";
      const sourceUrl = checkbox.dataset.sourceUrl || "";
      if (!sourceId && !sourceUrl) return null;
      const row = checkbox.closest(".admin-user-row");
      if (!row) return null;
      const nameCell = row.querySelector("[data-label=\"Name\"]");
      const adapterCell = row.querySelector("[data-label=\"Adapter\"]");
      const statusCell = row.querySelector("[data-label=\"Status\"]");
      const jobsCell = row.querySelector("[data-label=\"Jobs\"]");
      return {
        id: sourceId,
        url: sourceUrl,
        name: nameCell?.textContent?.trim() || sourceUrl || sourceId,
        adapter: adapterCell?.textContent?.trim() || "unknown",
        status: statusCell?.textContent?.trim() || "unknown",
        jobsFound: jobsCell?.textContent?.trim() || "0"
      };
    },
    recoveryActions: ["retry_source", "copy_diagnostics"]
  },
  task_run: {
    label: "Task Run",
    dataAttr: "data-run-key",
    resolveData: function (target) {
      const row = target.closest("[data-run-key]");
      if (!row) return null;
      const runKey = row.dataset.runKey || "";
      const rowArea = row.dataset.rowArea || "";
      if (!runKey) return null;
      const typeCell = row.querySelector(".admin-cell:first-child");
      const statusChip = row.querySelector(".admin-status-chip");
      const cells = Array.from(row.querySelectorAll(".admin-cell"));
      const durationText = cells.length > 2 ? cells[2]?.textContent?.trim() || "" : "";
      const outputText = cells.length > 3 ? cells[3]?.textContent?.trim() || "" : "";
      return {
        id: runKey,
        rowArea,
        typeText: typeCell?.textContent?.trim() || "",
        statusText: statusChip?.textContent?.trim() || "",
        statusClass: statusChip?.className?.replace("admin-status-chip", "").trim() || "",
        durationText,
        outputText
      };
    },
    recoveryActions: ["copy_diagnostics"]
  },
  alert: {
    label: "Alert",
    dataAttr: "data-alert-id",
    resolveData: function (target) {
      const banner = target.closest("[data-alert-id]");
      if (!banner) return null;
      const messageEl = banner.querySelector(".admin-alert-message");
      return {
        id: banner.dataset.alertId || "",
        message: messageEl?.textContent?.trim() || "",
        severity: banner.classList.contains("critical") ? "critical" : "warning"
      };
    },
    recoveryActions: ["ack_alert", "copy_diagnostics"]
  },
  registry_conflict: {
    label: "Registry Conflict",
    dataAttr: "data-conflict-key",
    resolveData: function (target) {
      const row = target.closest("[data-conflict-key]");
      if (!row) return null;
      const key = row.dataset.conflictKey || "";
      if (!key) return null;
      const summaryEl = row.querySelector(".admin-registry-conflict-summary, strong, .admin-cell:first-child");
      return {
        id: key,
        summary: summaryEl?.textContent?.trim() || key
      };
    },
    recoveryActions: ["copy_diagnostics"]
  },
  storage: {
    label: "Storage",
    noDom: true,
    recoveryActions: ["copy_diagnostics"]
  }
};

function renderEntityContent(entityType, entityData) {
  const spec = ENTITY_TYPES[entityType];
  if (!spec) return "";

  let html = `<div class="inspector-entity-header">
    <span class="inspector-entity-type">${spec.label}</span>
    <span class="inspector-entity-id">${typeof entityData.id === "string" ? entityData.id : ""}</span>
  </div>`;

  html += `<div class="inspector-entity-detail">`;

  switch (entityType) {
    case "source":
      html += `<div class="inspector-field"><span class="inspector-field-label">Name</span><span class="inspector-field-value">${escapeHtml(String(entityData.name || ""))}</span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">URL</span><span class="inspector-field-value inspector-field-value-wrap">${escapeHtml(String(entityData.url || ""))}</span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">Adapter</span><span class="inspector-field-value">${escapeHtml(String(entityData.adapter || ""))}</span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">Status</span><span class="inspector-field-value">${escapeHtml(String(entityData.status || ""))}</span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">Jobs Found</span><span class="inspector-field-value">${escapeHtml(String(entityData.jobsFound || "0"))}</span></div>`;
      break;
    case "task_run":
      html += `<div class="inspector-field"><span class="inspector-field-label">Type</span><span class="inspector-field-value">${escapeHtml(String(entityData.typeText || ""))}</span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">Status</span><span class="inspector-field-value"><span class="admin-status-chip ${escapeHtml(entityData.statusClass || "")}">${escapeHtml(String(entityData.statusText || ""))}</span></span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">Run Key</span><span class="inspector-field-value inspector-field-value-mono">${escapeHtml(String(entityData.id || ""))}</span></div>`;
      if (entityData.durationText) {
        html += `<div class="inspector-field"><span class="inspector-field-label">Duration</span><span class="inspector-field-value">${escapeHtml(entityData.durationText)}</span></div>`;
      }
      if (entityData.outputText && entityData.outputText !== "-") {
        html += `<div class="inspector-field"><span class="inspector-field-label">Output</span><span class="inspector-field-value">${escapeHtml(entityData.outputText)}</span></div>`;
      }
      break;
    case "alert":
      html += `<div class="inspector-field"><span class="inspector-field-label">Severity</span><span class="inspector-field-value"><span class="inspector-severity-badge inspector-severity-${entityData.severity}"">${escapeHtml(entityData.severity || "")}</span></span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">Message</span><span class="inspector-field-value">${escapeHtml(String(entityData.message || ""))}</span></div>`;
      break;
    case "registry_conflict":
      html += `<div class="inspector-field"><span class="inspector-field-label">Summary</span><span class="inspector-field-value">${escapeHtml(String(entityData.summary || ""))}</span></div>`;
      html += `<div class="inspector-field"><span class="inspector-field-label">Key</span><span class="inspector-field-value inspector-field-value-mono">${escapeHtml(String(entityData.id || ""))}</span></div>`;
      break;
    case "storage":
      html += `<div class="inspector-field"><span class="inspector-field-label">Info</span><span class="inspector-field-value">Storage health data is available via Copy diagnostics below.</span></div>`;
      break;
    default:
      html += `<div class="inspector-raw">${escapeHtml(JSON.stringify(entityData, null, 2))}</div>`;
  }

  html += `</div>`;

  const actions = spec.recoveryActions || [];
  if (actions.length > 0) {
    html += `<div class="inspector-actions">`;
    for (const action of actions) {
      html += `<button class="btn clear-filters-btn inspector-action-btn" data-inspector-action="${action}" data-entity-type="${entityType}">${actionLabel(action)}</button>`;
    }
    html += `</div>`;
  }

  return html;
}

function actionLabel(action) {
  switch (action) {
    case "retry_source": return "\uD83D\uDD04 Retry";
    case "copy_diagnostics": return "\uD83D\uDCCB Copy diagnostics";
    case "ack_alert": return "\u2715 Dismiss alert";
    case "restore_source": return "\u21A9 Restore";
    default: return action.replace(/_/g, " ");
  }
}

function escapeHtml(text) {
  const div = globalThis.document?.createElement?.("div");
  if (div) {
    div.textContent = String(text || "");
    return div.innerHTML;
  }
  return String(text || "")?.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;") || "";
}

export function createAdminInspectorController({
  refs,
  getBridge,
  postBridge,
  showToast,
  logAdminError
}) {
  let open = false;
  let currentEntityType = null;
  let currentEntityData = null;
  let clickDelegateBound = false;

  function showOverlay() {
    const overlay = refs.inspectorOverlayEl;
    const panel = refs.inspectorPanelEl;
    if (overlay) overlay.classList.add("inspector-overlay-visible");
    if (panel) panel.classList.add("inspector-panel-visible");
    open = true;
  }

  function hideOverlay() {
    const overlay = refs.inspectorOverlayEl;
    const panel = refs.inspectorPanelEl;
    if (overlay) overlay.classList.remove("inspector-overlay-visible");
    if (panel) panel.classList.remove("inspector-panel-visible");
    open = false;
    currentEntityType = null;
    currentEntityData = null;
  }

  function renderContent(entityType, entityData) {
    const contentEl = refs.inspectorContentEl;
    const headerEl = refs.inspectorTitleEl;
    if (!contentEl) return;

    const html = renderEntityContent(entityType, entityData);
    contentEl.innerHTML = html;

    const spec = ENTITY_TYPES[entityType];
    if (headerEl) {
      headerEl.textContent = spec ? spec.label : entityType;
    }

    contentEl.querySelectorAll("[data-inspector-action]").forEach(btn => {
      btn.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        const action = btn.dataset.inspectorAction;
        handleAction(action, entityType, entityData);
      });
    });
  }

  function openInspector(entityType, entityData) {
    if (!entityData) return;
    currentEntityType = entityType;
    currentEntityData = entityData;
    renderContent(entityType, entityData);
    showOverlay();
  }

  async function handleAction(action, entityType, entityData) {
    try {
      if (action === "retry_source") {
        const result = await postBridge("/tasks/run-fetcher", {
          preset: "retry_failed"
        });
        if (result?.alreadyRunning) {
          showToast("Fetch is already running", "info");
        } else if (result?.alreadyCompleted) {
          showToast("Task already completed", "info");
        } else if (result?.started) {
          showToast("Retry started", "success");
        } else {
          showToast("Retry could not be started", "warn");
        }
      } else if (action === "copy_diagnostics") {
        const payload = {
          _meta: { entityType, inspectedAt: new Date().toISOString() },
          entity: entityData || {}
        };
        try {
          await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
          showToast("Diagnostics copied", "success");
        } catch {
          showToast("Could not copy diagnostics", "warn");
        }
      } else if (action === "ack_alert") {
        const alertId = String(entityData?.id || "").trim();
        if (!alertId) {
          showToast("No alert ID to dismiss", "warn");
          return;
        }
        await postBridge("/ops/alerts/ack", { id: alertId });
        showToast("Alert dismissed", "success");
        hideOverlay();
      }
    } catch (err) {
      showToast("Action failed: " + (err?.message || "unknown error"), "error");
    }
  }

  function bindClickDelegate() {
    if (clickDelegateBound) return;
    clickDelegateBound = true;

    document.addEventListener("click", function delegateHandler(event) {
      const target = event.target;
      for (const [type, spec] of Object.entries(ENTITY_TYPES)) {
        if (spec.noDom) continue;
        const matched = target.closest(`[${spec.dataAttr}]`);
        if (!matched) continue;

        const isActionButton = target.closest("button") || target.closest("input") || target.closest("a");
        if (isActionButton && !target.closest(".admin-ops-history-row")) continue;

        const rows = Array.from(document.querySelectorAll(`[${spec.dataAttr}]`));
        rows.forEach(row => row.classList.remove("inspector-row-selected"));
        matched.classList.add("inspector-row-selected");

        const data = spec.resolveData(matched);
        if (!data) return;
        openInspector(type, data);
        event.preventDefault();
        return;
      }
    });
  }

  function bindOverlayClose() {
    const overlay = refs.inspectorOverlayEl;
    const closeBtn = refs.inspectorCloseBtnEl;
    if (overlay && !overlay._boundClose) {
      overlay._boundClose = true;
      overlay.addEventListener("click", event => {
        if (event.target === overlay) hideOverlay();
      });
    }
    if (closeBtn && !closeBtn._boundClose) {
      closeBtn._boundClose = true;
      closeBtn.addEventListener("click", () => hideOverlay());
    }

    document.addEventListener("keydown", function escHandler(event) {
      if (event.key === "Escape" && open) {
        hideOverlay();
      }
    });
  }

  function init() {
    bindClickDelegate();
    bindOverlayClose();
  }

  return {
    init,
    openInspector,
    closeInspector: hideOverlay,
    isOpen: () => open
  };
}

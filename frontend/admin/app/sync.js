export function createAdminSyncController({
  state,
  refs,
  getBridge,
  postBridge,
  isSyncBusy,
  setBusyFlag,
  getErrorMessage,
  showToast,
  toLocalTime,
  loadOpsHealthData,
  scheduleOpsHealthPolling,
  escapeHtml
}) {
  function populateSyncConfigForm(savedConfig, options = {}) {
    if (state.syncConfigDirty && !options.force) return;
    const config = savedConfig || {};
    if (refs.adminSyncEnabledEl) {
      refs.adminSyncEnabledEl.checked = config.enabled === null ? true : Boolean(config.enabled);
    }
  }

  function collectSyncConfigPayload() {
    return { enabled: Boolean(refs.adminSyncEnabledEl?.checked) };
  }

  function renderSyncStatus(statusPayload, options = {}) {
    if (!refs.adminSyncStatusEl) return;
    populateSyncConfigForm(statusPayload?.savedConfig || {}, { force: Boolean(options.forceForm) });
    const config = statusPayload?.config || {};
    const runtime = statusPayload?.runtime || {};
    const stateToken = String(config?.state || "disabled");
    const missing = Array.isArray(config?.missing) ? config.missing : [];
    const configMessage = String(config?.message || "").trim();
    const authMode = String(config?.authMode || "github_app");
    const configPath = String(config?.configPath || "").trim();
    if (refs.adminSyncConfigHintEl) {
      refs.adminSyncConfigHintEl.textContent = configPath
        ? `GitHub App mode: ${authMode}. Packaged config: ${configPath}`
        : "GitHub App credentials are packaged with the app.";
    }
    const repo = String(config?.repo || "unknown");
    const branch = String(config?.branch || "main");
    const path = String(config?.path || "baluffo/source-sync.json");
    const lastPullAt = String(runtime?.lastPullAt || "");
    const lastPushAt = String(runtime?.lastPushAt || "");
    const lastError = String(runtime?.lastError || "");
    const lastResult = String(runtime?.lastResult || "");
    const lastAction = String(runtime?.lastAction || "");
    const badgeLabel = stateToken === "ready"
      ? "Ready"
      : stateToken === "rate_limited"
        ? "Rate Limited"
        : stateToken === "remote_conflict"
          ? "Remote Conflict"
          : stateToken === "misconfigured"
            ? "Needs Attention"
            : "Disabled";
    const summaryText = stateToken === "disabled"
      ? "Source sync is disabled on this machine. Remote state remains untouched until you enable it again."
      : stateToken === "rate_limited"
        ? `Source sync is temporarily rate limited.${configMessage ? ` ${configMessage}` : ""}`
        : stateToken === "remote_conflict"
          ? `Source sync detected a remote write conflict.${configMessage ? ` ${configMessage}` : ""}`
          : stateToken === "misconfigured"
            ? `Source sync cannot run yet.${missing.length ? ` Missing: ${missing.join(", ")}.` : ""}${configMessage ? ` ${configMessage}` : ""}`
            : `Connected to ${repo} and ready to keep the shared source registry in sync.`;
    const meta = [
      ["Mode", authMode],
      ["Repository", repo],
      ["Branch", branch],
      ["Remote Path", path],
      ["Last Pull", lastPullAt ? toLocalTime(new Date(lastPullAt)) : "Never"],
      ["Last Push", lastPushAt ? toLocalTime(new Date(lastPushAt)) : "Never"],
      ["Last Action", lastAction || "None"],
      ["Last Result", lastResult || "None"]
    ];
    const metaHtml = meta.map(([label, value]) => `
      <div class="admin-sync-meta-item">
        <span class="admin-sync-meta-label">${escapeHtml(label)}</span>
        <div class="admin-sync-meta-value">${escapeHtml(value)}</div>
      </div>
    `).join("");
    const errorHtml = lastError ? `<div class="admin-sync-error">${escapeHtml(lastError)}</div>` : "";
    refs.adminSyncStatusEl.innerHTML = `
      <div class="admin-sync-status-head">
        <span class="admin-sync-badge ${escapeHtml(stateToken)}">${escapeHtml(badgeLabel)}</span>
        <span class="admin-sync-inline-note">${escapeHtml(config?.enabled ? "Local sync enabled" : "Local sync disabled")}</span>
      </div>
      <p class="admin-sync-summary">${escapeHtml(summaryText)}</p>
      <div class="admin-sync-meta-grid">${metaHtml}</div>
      ${errorHtml}
    `;
  }

  async function loadSyncStatus(options = {}) {
    if (!state.adminPin) return null;
    const silent = Boolean(options?.silent);
    const forceForm = Boolean(options?.forceForm);
    try {
      const payload = await getBridge("/sync/status");
      state.latestSyncStatusCache = payload || null;
      renderSyncStatus(payload || {}, { forceForm });
      return payload || null;
    } catch (err) {
      if (refs.adminSyncStatusEl) refs.adminSyncStatusEl.textContent = `Sync status unavailable: ${getErrorMessage(err)}`;
      if (!silent) showToast(`Could not load sync status: ${getErrorMessage(err)}`, "error");
      throw err;
    }
  }

  async function saveSyncConfig() {
    if (!state.adminPin) {
      showToast("Unlock admin to save sync settings.", "error");
      return;
    }
    if (isSyncBusy()) {
      showToast("Sync task is already running.", "info");
      return;
    }
    setBusyFlag("syncRun", true);
    try {
      const result = await postBridge("/sync/config", collectSyncConfigPayload());
      state.latestSyncStatusCache = result || null;
      state.syncConfigDirty = false;
      renderSyncStatus(result || {}, { forceForm: true });
      showToast("Source sync preference updated.", "success");
    } catch (err) {
      showToast(`Could not save sync settings: ${getErrorMessage(err)}`, "error");
    } finally {
      setBusyFlag("syncRun", false);
    }
  }

  async function testSyncConfig() {
    if (!state.adminPin) {
      showToast("Unlock admin to test sync settings.", "error");
      return;
    }
    if (isSyncBusy()) {
      showToast("Sync task is already running.", "info");
      return;
    }
    setBusyFlag("syncRun", true);
    try {
      const result = await postBridge("/sync/test", {});
      if (result?.ok) {
        showToast(
          Boolean(result?.remoteFound)
            ? "Sync test passed. Remote snapshot found."
            : "Sync test passed. Remote snapshot not created yet.",
          "success"
        );
        await loadSyncStatus({ silent: true });
        return;
      }
      showToast(`Sync test failed: ${String(result?.error || "unknown error")}`, "error");
    } catch (err) {
      showToast(`Sync test failed: ${getErrorMessage(err)}`, "error");
    } finally {
      setBusyFlag("syncRun", false);
    }
  }

  async function pullSourcesSync() {
    if (!state.adminPin) {
      showToast("Unlock admin to pull source sync.", "error");
      return;
    }
    if (isSyncBusy()) {
      showToast("Sync task is already running.", "info");
      return;
    }
    setBusyFlag("syncRun", true);
    try {
      const result = await postBridge("/tasks/run-sync-pull", {});
      if (result?.started) {
        showToast("Sources sync pull started.", "success");
        await loadOpsHealthData();
        scheduleOpsHealthPolling(900);
        return;
      }
      showToast(`Sources sync pull failed: ${String(result?.error || "unknown error")}`, "error");
    } catch (err) {
      showToast(`Sources sync pull failed: ${getErrorMessage(err)}`, "error");
    } finally {
      setBusyFlag("syncRun", false);
    }
  }

  async function pushSourcesSync() {
    if (!state.adminPin) {
      showToast("Unlock admin to push source sync.", "error");
      return;
    }
    if (isSyncBusy()) {
      showToast("Sync task is already running.", "info");
      return;
    }
    setBusyFlag("syncRun", true);
    try {
      const result = await postBridge("/tasks/run-sync-push", {});
      if (result?.started) {
        showToast("Sources sync push started.", "success");
        await loadOpsHealthData();
        scheduleOpsHealthPolling(900);
        return;
      }
      showToast(`Sources sync push failed: ${String(result?.error || "unknown error")}`, "error");
    } catch (err) {
      showToast(`Sources sync push failed: ${getErrorMessage(err)}`, "error");
    } finally {
      setBusyFlag("syncRun", false);
    }
  }

  return {
    populateSyncConfigForm,
    collectSyncConfigPayload,
    renderSyncStatus,
    loadSyncStatus,
    saveSyncConfig,
    testSyncConfig,
    pullSourcesSync,
    pushSourcesSync
  };
}

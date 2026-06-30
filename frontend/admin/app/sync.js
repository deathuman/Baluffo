import { formatTaskProgressDetail } from "../../shared/task-progress.js";
import { setTooltip } from "../../shared/ui/index.js?v=6";

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
  function setLiveSyncRunning(value) {
    const nextValue = Boolean(value);
    const currentValue = Boolean(
      typeof state.liveSyncRunning === "boolean"
        ? state.liveSyncRunning
        : state.adminBusyState?.liveSyncRunning
    );
    if (currentValue === nextValue) return;
    state.liveSyncRunning = nextValue;
    if (state.adminBusyState && typeof state.adminBusyState === "object") {
      state.adminBusyState.liveSyncRunning = nextValue;
    }
    setBusyFlag("liveSyncRunning", nextValue);
  }

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
    const hasConfig = Boolean(
      statusPayload?.config
      && typeof statusPayload.config === "object"
      && !Array.isArray(statusPayload.config)
    );
    const isDelayed = Boolean(statusPayload?.degraded || statusPayload?.delayed) && !hasConfig;
    if (isDelayed) {
      if (refs.adminSyncConfigHintEl) {
        refs.adminSyncConfigHintEl.textContent = "GitHub App credentials are packaged with the app.";
        setTooltip(refs.adminSyncConfigHintEl, "");
      }
      refs.adminSyncStatusEl.innerHTML = `
        <div class="admin-sync-status-head">
          <span class="admin-sync-badge pending">Loading</span>
          <span class="admin-sync-inline-note">Sync status delayed</span>
        </div>
        <p class="admin-sync-summary">Source sync status is loading. Admin will refresh the compact sync summary after active work settles.</p>
      `;
      return;
    }
    populateSyncConfigForm(statusPayload?.savedConfig || {}, { force: Boolean(options.forceForm) });
    const config = statusPayload?.config || {};
    const runtime = statusPayload?.runtime || {};
    const livePayload = options?.livePayload && typeof options.livePayload === "object"
      ? options.livePayload
      : {};
    const stateToken = String(config?.state || "disabled");
    const missing = Array.isArray(config?.missing) ? config.missing : [];
    const configMessage = String(config?.message || "").trim();
    const authMode = String(config?.authMode || "github_app");
    const configPath = String(config?.configPath || "").trim();
    if (refs.adminSyncConfigHintEl) {
      refs.adminSyncConfigHintEl.textContent = configPath
        ? `GitHub App mode: ${authMode}. Packaged config: available.`
        : "GitHub App credentials are packaged with the app.";
      if (configPath) {
        setTooltip(refs.adminSyncConfigHintEl, "Full packaged config path is available in diagnostics.");
      } else {
        setTooltip(refs.adminSyncConfigHintEl, "");
      }
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
    const liveSummary = livePayload?.active
      ? formatTaskProgressDetail("sync", livePayload?.taskProgress, livePayload?.summary || {})
      : "";
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
      ${liveSummary ? `<div class="admin-sync-summary">${escapeHtml(liveSummary)}</div>` : ""}
      <div class="admin-sync-meta-grid">${metaHtml}</div>
      ${errorHtml}
    `;
  }

  async function loadSyncStatus(options = {}) {
    const silent = Boolean(options?.silent);
    const forceForm = Boolean(options?.forceForm);
    const includeLive = options?.includeLive !== false;
    const summary = Boolean(options?.summary);
    try {
      const [payload, livePayload] = await Promise.all([
        getBridge(summary ? "/sync/status?view=summary" : "/sync/status"),
        includeLive ? getBridge("/ops/task-live/sync?view=summary").catch(() => null) : Promise.resolve(null)
      ]);
      state.latestSyncStatusCache = payload || null;
      setLiveSyncRunning(Boolean(livePayload?.active));
      renderSyncStatus(payload || {}, { forceForm, livePayload });
      return payload || null;
    } catch (err) {
      setLiveSyncRunning(false);
      if (refs.adminSyncStatusEl) refs.adminSyncStatusEl.textContent = `Sync status unavailable: ${getErrorMessage(err)}`;
      if (!silent) showToast(`Could not load sync status: ${getErrorMessage(err)}`, "error");
      throw err;
    }
  }

  async function saveSyncConfig() {
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
    if (isSyncBusy()) {
      showToast("Sync task is already running.", "info");
      return;
    }
    setBusyFlag("syncRun", true);
    try {
      const result = await postBridge("/sync/test", {});
      if (result?.ok) {
        showToast(
          result?.remoteFound
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

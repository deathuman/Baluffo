/**
 * Admin registry — source-tables view state.
 *
 * Placeholders, load-state transitions, active-work detection, and deferred
 * render scheduling for the three source-table buckets.
 *
 * Split out of ``load.js``; that module stays the coordinator that composes
 * the registry load controller.
 *
 * @module source-tables-state
 */

import { deriveAdminActiveWorkContext, pipelineStatusIndicatesActive, pipelineStatusIndicatesFetch } from "../active-work-policy.js";

const PIPELINE_STATUS_PREFLIGHT_TIMEOUT_MS = 3000;
const ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL = "Source tables delayed while job update is running.";
const BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL = "Source tables delayed while Admin data is retrying.";
const BRIDGE_DEGRADED_SOURCE_TABLES_BACKOFF_MS = 30000;
const JOBS_PIPELINE_STATUS_PATH = "/tasks/run-jobs-pipeline-status";
const SOURCE_TABLE_RECOVERY_STATES = new Set(["delayed-active", "delayed-bridge", "retrying-active", "recovering-idle", "unavailable"]);

/**
 * Build the source-tables view-state helpers for one registry controller.
 *
 * @param {{ state: Object, refs: Object, getBridge: Function, setBusyFlag: Function, renderScheduler: Function }} input
 * @returns {Object} the source-tables helpers, keyed by name
 */
function createSourceTablesState({ state, refs, getBridge, setBusyFlag, renderScheduler }) {
function setSourceTablePlaceholder(container, bucketLabel) {
    if (!container) return;
    container.innerHTML = `<div class="muted">Loading ${bucketLabel} sources...</div>`;
  }

  function setSourceTableDelayedPlaceholder(container) {
    if (!container) return;
    const label = state.sourceTablesBridgeDegraded
      ? BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL
      : ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL;
    container.innerHTML = `<div class="muted">${label}</div>`;
  }

  function formatSourceTablesSummaryCounts(summary = {}) {
    const pending = Number(summary?.pendingCount || 0);
    const active = Number(summary?.activeCount || 0);
    const rejected = Number(summary?.rejectedCount || 0);
    return `pending ${pending.toLocaleString()}, active ${active.toLocaleString()}, rejected ${rejected.toLocaleString()}`;
  }

  function setSourceTableRefreshingPlaceholder(container, summary = {}) {
    if (!container) return;
    container.innerHTML = `<div class="muted">Source tables refreshing; ${formatSourceTablesSummaryCounts(summary)}.</div>`;
  }

  function setSourceTableUnavailablePlaceholder(container, bucketLabel) {
    if (!container) return;
    container.innerHTML = `<div class="no-results">Could not load ${bucketLabel} sources. Retry after the running job update finishes.</div>`;
  }

  function setSourceTablesLoadState(status, reason = "") {
    state.sourceTablesLoadState = String(status || "");
    state.sourceTablesLoadReason = String(reason || "");
    state.sourceTablesLoadUpdatedAtMs = Date.now();
  }

  function sourceTablesLoadStateNeedsRecovery() {
    return SOURCE_TABLE_RECOVERY_STATES.has(String(state.sourceTablesLoadState || ""));
  }

  function sourceTableNeedsDelayedPlaceholder(container) {
    if (!container) return false;
    const currentText = String(container.textContent || container.innerHTML || "").trim();
    return !currentText
      || /Loading (pending|active|rejected) sources/i.test(currentText)
      || currentText.includes(ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL)
      || currentText.includes(BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL);
  }

  function sourcePayloadRowsAreEmpty(sources = {}) {
    return ["pending", "active", "rejected"].every(bucket => (
      !Array.isArray(sources?.[bucket]) || sources[bucket].length === 0
    ));
  }

  function sourcePayloadSummaryHasRows(summary = {}) {
    return ["pendingCount", "activeCount", "rejectedCount"].some(key => Number(summary?.[key] || 0) > 0);
  }

  function sourcePayloadIsDegradedEmpty(payload = {}) {
    return Boolean(
      payload?.degraded === true
      && sourcePayloadRowsAreEmpty(payload?.sources || {})
      && sourcePayloadSummaryHasRows(payload?.summary || {})
    );
  }

  function activeFetchRunning() {
    return deriveAdminActiveWorkContext({ state }).fetchActive;
  }

  function activePipelineOrFetchRunning() {
    return deriveAdminActiveWorkContext({ state }).pipelineOrFetchActive;
  }

  function activeSyncRunning() {
    return deriveAdminActiveWorkContext({ state }).syncActive;
  }

  function activeAdminRegistryWorkRunning() {
    return deriveAdminActiveWorkContext({ state }).isActive;
  }

  function rememberPipelineStatusActivity(payload = {}) {
    if (!pipelineStatusIndicatesActive(payload)) return;
    state.discoveryPipelineStatusPayload = payload;
    setBusyFlag("livePipelineRunning", true);
    setBusyFlag(
      "liveFetchRunning",
      pipelineStatusIndicatesFetch(payload)
    );
    state.discoveryPipelineStatusLastActiveAtMs = Date.now();
  }

  async function refreshActivePipelineStatus({ force = false } = {}) {
    if (!force && activeFetchRunning()) return true;
    try {
      const payload = await getBridge(JOBS_PIPELINE_STATUS_PATH, { timeoutMs: PIPELINE_STATUS_PREFLIGHT_TIMEOUT_MS });
      if (pipelineStatusIndicatesActive(payload)) {
        rememberPipelineStatusActivity(payload);
        return true;
      }
      state.discoveryPipelineStatusLastActiveAtMs = 0;
      state.discoveryPipelineStatusPayload = null;
      if (force) {
        setBusyFlag("livePipelineRunning", false);
        setBusyFlag("liveFetchRunning", false);
      }
    } catch {
      // Source tables should remain available when the fast control-plane preflight is unavailable.
    }
    return activePipelineOrFetchRunning();
  }

  function sourceTablesActiveContext({ livePipelineOrFetchRunning = false } = {}) {
    const context = deriveAdminActiveWorkContext({
      state,
      livePipelineOrFetchRunning
    });
    return {
      active: context.isActive,
      canLoadCompact: context.sourceTablesCanLoadCompact || !context.isActive,
      reason: context.reason,
      taskType: context.taskType
    };
  }

  function recentlyDetectedActivePipeline() {
    const lastActiveAtMs = Number(state.discoveryPipelineStatusLastActiveAtMs || 0);
    return lastActiveAtMs > 0 && Date.now() - lastActiveAtMs < 120000;
  }

  function renderSourceTablesDelayed({ onlyIfPlaceholder = false } = {}) {
    if (!onlyIfPlaceholder || sourceTableNeedsDelayedPlaceholder(refs.adminPendingSourcesEl)) {
      setSourceTableDelayedPlaceholder(refs.adminPendingSourcesEl);
    }
    if (!onlyIfPlaceholder || sourceTableNeedsDelayedPlaceholder(refs.adminActiveSourcesEl)) {
      setSourceTableDelayedPlaceholder(refs.adminActiveSourcesEl);
    }
    if (!onlyIfPlaceholder || sourceTableNeedsDelayedPlaceholder(refs.adminRejectedSourcesEl)) {
      setSourceTableDelayedPlaceholder(refs.adminRejectedSourcesEl);
    }
  }

  function markSourceTablesLoadingForBootstrap() {
    setSourceTablesLoadState("loading", "bootstrap");
    setSourceTablePlaceholder(refs.adminPendingSourcesEl, "pending");
    setSourceTablePlaceholder(refs.adminActiveSourcesEl, "active");
    setSourceTablePlaceholder(refs.adminRejectedSourcesEl, "rejected");
  }

  function markSourceTablesDelayedForActiveWork(reason = "active_admin_work", options = {}) {
    state.sourceTablesDelayedDuringActiveRun = true;
    state.sourceTablesBridgeDegraded = reason === "bridge_degraded";
    setSourceTablesLoadState("delayed-active", reason);
    renderSourceTablesDelayed({ onlyIfPlaceholder: options?.onlyIfPlaceholder !== false });
  }

  function markSourceTablesDelayedForBridgeDegraded(options = {}) {
    state.sourceTablesBridgeDegraded = true;
    state.sourceTablesDelayedDuringActiveRun = true;
    state.adminBridgeHeavyRouteDegradedUntilMs = Date.now() + BRIDGE_DEGRADED_SOURCE_TABLES_BACKOFF_MS;
    setSourceTablesLoadState("delayed-bridge", "bridge_degraded");
    renderSourceTablesDelayed({ onlyIfPlaceholder: options?.onlyIfPlaceholder !== false });
  }

  function bridgeHeavyRoutesRecentlyDegraded() {
    return Date.now() < Number(state.adminBridgeHeavyRouteDegradedUntilMs || 0);
  }

  function scheduleDeferredRender(callback) {
    const scheduleRender = typeof renderScheduler === "function"
      ? renderScheduler
      : renderCallback => {
        renderCallback();
        return () => {};
      };
    scheduleRender(callback);
  }

  return {
    setSourceTablePlaceholder,
    setSourceTableDelayedPlaceholder,
    formatSourceTablesSummaryCounts,
    setSourceTableRefreshingPlaceholder,
    setSourceTableUnavailablePlaceholder,
    setSourceTablesLoadState,
    sourceTablesLoadStateNeedsRecovery,
    sourceTableNeedsDelayedPlaceholder,
    sourcePayloadRowsAreEmpty,
    sourcePayloadSummaryHasRows,
    sourcePayloadIsDegradedEmpty,
    activeFetchRunning,
    activePipelineOrFetchRunning,
    activeSyncRunning,
    activeAdminRegistryWorkRunning,
    rememberPipelineStatusActivity,
    refreshActivePipelineStatus,
    sourceTablesActiveContext,
    recentlyDetectedActivePipeline,
    renderSourceTablesDelayed,
    markSourceTablesLoadingForBootstrap,
    markSourceTablesDelayedForActiveWork,
    markSourceTablesDelayedForBridgeDegraded,
    bridgeHeavyRoutesRecentlyDegraded,
    scheduleDeferredRender
  };
}

export {
  createSourceTablesState,
  ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL,
  BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL
};

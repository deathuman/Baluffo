/**
 * Admin registry — load controller.
 *
 * Thin coordinator: builds the source-tables view state and the discovery-load
 * helpers, then exposes the six keys the registry controller consumes.
 *
 * Leaf modules:
 * - source-tables-state.js  placeholders, load-state, active-work detection
 * - discovery-load.js       report resolution, endpoint loading, retry, render
 *
 * No leaf module imports from here.
 */

import { createSourceTablesState } from "./source-tables-state.js";
import { createDiscoveryLoad } from "./discovery-load.js";

export function createRegistryLoadController({
  state,
  refs,
  getBridge,
  fetchJobsFetchReportJson,
  mergeSourceDiscoveryCandidates = rows => rows,
  mergeSourceStatusFromReport,
  applySourceFilter,
  getSourceJobsFoundCount,
  getSourceDiscoveryJobsCount = getSourceJobsFoundCount,
  normalizeSourceFilter,
  readShowZeroJobs,
  adminDispatch,
  adminActions,
  appendDiscoveryLog,
  getErrorMessage,
  setBusyFlag,
  renderSourcesTable,
  renderScheduler
}) {
  const sourceTables = createSourceTablesState({ state, refs, getBridge, setBusyFlag, renderScheduler });

  const {
    sourceTablesLoadStateNeedsRecovery,
    sourceTableNeedsDelayedPlaceholder,
    activeSyncRunning,
    refreshActivePipelineStatus,
    renderSourceTablesDelayed,
    markSourceTablesLoadingForBootstrap,
    markSourceTablesDelayedForActiveWork,
  } = sourceTables;

  const {
    scheduleRegistryRefreshRetry,
    loadDiscoveryData
  } = createDiscoveryLoad({
    state,
    refs,
    getBridge,
    fetchJobsFetchReportJson,
    mergeSourceDiscoveryCandidates,
    mergeSourceStatusFromReport,
    applySourceFilter,
    getSourceJobsFoundCount,
    getSourceDiscoveryJobsCount,
    normalizeSourceFilter,
    readShowZeroJobs,
    adminDispatch,
    adminActions,
    appendDiscoveryLog,
    getErrorMessage,
    setBusyFlag,
    renderSourcesTable,
    sourceTables
  });

async function syncSourceTablesAfterTaskCompletion({
    taskType,
    completionSignature,
    fetchReport = null
  } = {}) {
    const normalizedTaskType = String(taskType || "").trim().toLowerCase();
    const signature = String(completionSignature || "").trim();
    if (!normalizedTaskType || !signature) return null;
    const signatureKey = normalizedTaskType === "fetch"
      ? "fetcherSourceSyncSignature"
      : "discoverySourceSyncSignature";
    if (String(state[signatureKey] || "") === signature) {
      return null;
    }
    const result = await loadDiscoveryData({
      background: true,
      fetchReport,
      forceFetchReport: Boolean(fetchReport),
      logChanges: false,
      completionRefresh: true,
      suppressPlaceholders: true
    });
    if (result) {
      if (result.partialLoadFailed) {
        state[signatureKey] = "";
        scheduleRegistryRefreshRetry({ fetchReport });
      } else {
        state[signatureKey] = signature;
      }
    }
    return result;
  }

  async function refreshSourceTablesAfterActiveRunIdle(options = {}) {
    const needsRecovery = Boolean(
      sourceTablesLoadStateNeedsRecovery()
      || state.sourceTablesDelayedDuringActiveRun
      || !state.discoveryTablesRendered
      || sourceTableNeedsDelayedPlaceholder(refs.adminPendingSourcesEl)
      || sourceTableNeedsDelayedPlaceholder(refs.adminActiveSourcesEl)
      || sourceTableNeedsDelayedPlaceholder(refs.adminRejectedSourcesEl)
    );
    if (!needsRecovery) return null;
    if (state.adminBusyState?.discoveryLoad) return state.discoveryLoadPromise || null;
    if (activeSyncRunning()) return null;
    const stillActive = await refreshActivePipelineStatus({ force: true });
    if (stillActive) return null;
    const result = await loadDiscoveryData({
      sourceTablesOnly: true,
      background: true,
      completionRefresh: true,
      suppressPlaceholders: true,
      logChanges: false,
      fetchReport: options?.fetchReport || null,
      forceFetchReport: Boolean(options?.fetchReport)
    });
    if (result && !result.partialLoadFailed && !result.skipped) {
      state.sourceTablesDelayedDuringActiveRun = false;
    }
    return result;
  }

  return {
    loadDiscoveryData,
    syncSourceTablesAfterTaskCompletion,
    refreshSourceTablesAfterActiveRunIdle,
    renderSourceTablesDelayed,
    markSourceTablesLoadingForBootstrap,
    markSourceTablesDelayedForActiveWork
  };
}

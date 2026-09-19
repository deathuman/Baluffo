/**
 * Admin registry — discovery payload loading.
 *
 * The registry refresh entrypoint: report resolution, registry signature
 * computation, bounded endpoint loading, retry backoff, and the
 * ``loadDiscoveryData`` orchestration.  The per-pass fetch/render work lives in
 * ``discovery-render.js``.
 *
 * Split out of ``load.js``; that module stays the coordinator that composes
 * the registry load controller.
 *
 * @module discovery-load
 */

import {
  ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL,
  BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL
} from "./source-tables-state.js";
import { runDiscoveryLoad } from "./discovery-render.js";

const REGISTRY_REFRESH_RETRY_DELAY_MS = 5000;
const REGISTRY_REFRESH_RETRY_MAX_DELAY_MS = 30000;

const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";

function fnv1a32(value, seed = 0x811c9dc5) {
  let hash = seed >>> 0;
  const text = String(value ?? "");
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

function toDigestHex(value) {
  return (value >>> 0).toString(16).padStart(8, "0");
}

/**
 * Build the discovery-loading helpers for one registry controller.
 *
 * @param {Object} input the registry controller's dependencies
 * @param {Object} input.sourceTables the shared source-tables view state
 * @returns {Object} the loading helpers, keyed by name
 */
function createDiscoveryLoad({
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
}) {
  let registryRenderToken = 0;

  const {
    activeAdminRegistryWorkRunning,
    activeSyncRunning,
    bridgeHeavyRoutesRecentlyDegraded,
    markSourceTablesDelayedForActiveWork,
    markSourceTablesDelayedForBridgeDegraded,
    recentlyDetectedActivePipeline,
    refreshActivePipelineStatus,
    renderSourceTablesDelayed,
    scheduleDeferredRender,
    setSourceTablePlaceholder,
    setSourceTableRefreshingPlaceholder,
    setSourceTableUnavailablePlaceholder,
    setSourceTablesLoadState,
    sourcePayloadIsDegradedEmpty,
    sourceTablesActiveContext,
  } = sourceTables;

function resolveLatestFetchReport(options = {}) {
    const providedReport = options?.fetchReport;
    if (providedReport && typeof providedReport === "object" && !Array.isArray(providedReport)) {
      state.latestFetcherReportCache = providedReport;
      return Promise.resolve(providedReport);
    }
    if (!options?.forceFetchReport && state.latestFetcherReportCache) {
      return Promise.resolve(state.latestFetcherReportCache);
    }
    return Promise.resolve(fetchJobsFetchReportJson())
      .then(report => {
        if (report && typeof report === "object" && !Array.isArray(report)) {
          state.latestFetcherReportCache = report;
        }
        return report || state.latestFetcherReportCache || null;
      });
  }

  function toAdminFilterState() {
    return {
      activeSourceFilter: normalizeSourceFilter(state.activeSourceFilter),
      showZeroJobs: readShowZeroJobs(ADMIN_SHOW_ZERO_JOBS_KEY)
    };
  }

  function buildDiscoveryRegistrySignature(rowsByBucket) {
    const buckets = ["pending", "active", "rejected"];
    return buckets.map(bucket => {
      const rows = Array.isArray(rowsByBucket?.[bucket]) ? rowsByBucket[bucket] : [];
      let count = 0;
      let xorHash = 0;
      let sumHash = 0;
      let lengthHash = 0;
      rows.forEach(row => {
        const rowText = [
          String(row?.id || row?.sourceId || row?.name || ""),
          String(row?.name || ""),
          String(row?.adapter || ""),
          String(row?.studio || ""),
          String(row?.status || ""),
          String(Number(getSourceJobsFoundCount(row) || 0)),
          String(row?.sourceId || ""),
          String(row?.url || row?.sourceUrl || "")
        ].join("\u001f");
        const rowHash = fnv1a32(rowText);
        count += 1;
        xorHash = (xorHash ^ rowHash) >>> 0;
        sumHash = (sumHash + rowHash) >>> 0;
        lengthHash = (lengthHash + fnv1a32(rowText.length, rowHash)) >>> 0;
      });
      return `${bucket}:${count}:${toDigestHex(xorHash)}:${toDigestHex(sumHash)}:${toDigestHex(lengthHash)}`;
    }).join("|");
  }

async function loadDiscoveryEndpoint(label, promise, fallback, options = {}) {
    try {
      return await promise;
    } catch (err) {
      const message = getErrorMessage(err);
      const registryRefreshDelayedByActiveWork = Boolean(
        options?.registryRefresh
        && (
          activeAdminRegistryWorkRunning()
          || recentlyDetectedActivePipeline()
        )
        && /(timed out|timeout|HTTP 504|\b504\b)/i.test(message)
      );
      const registryRefreshDelayedByBridgeDegraded = Boolean(
        options?.registryRefresh
        && !registryRefreshDelayedByActiveWork
        && /(timed out|timeout|HTTP 504|\b504\b|bridge_degraded)/i.test(message)
      );
      if (registryRefreshDelayedByActiveWork) {
        markSourceTablesDelayedForActiveWork("active_registry_timeout", { onlyIfPlaceholder: true });
        appendDiscoveryLog(ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL, "warn");
      } else if (registryRefreshDelayedByBridgeDegraded) {
        markSourceTablesDelayedForBridgeDegraded({ onlyIfPlaceholder: true });
        appendDiscoveryLog(BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL, "warn");
      } else if (options?.registryRefresh && options?.background && /timed out/i.test(message)) {
        appendDiscoveryLog("Source table refresh delayed; retrying.", "warn");
      } else {
        appendDiscoveryLog(`Could not load ${label}: ${message}`, "error");
      }
      return {
        ...(fallback && typeof fallback === "object" && !Array.isArray(fallback) ? fallback : {}),
        __loadFailed: true,
        __delayedDuringActiveRun: registryRefreshDelayedByActiveWork || registryRefreshDelayedByBridgeDegraded
      };
    }
  }

  function nextRegistryRefreshRetryDelay() {
    const currentDelay = Number(state.discoveryRegistryRefreshRetryDelayMs || REGISTRY_REFRESH_RETRY_DELAY_MS);
    const delay = Math.min(
      REGISTRY_REFRESH_RETRY_MAX_DELAY_MS,
      Math.max(REGISTRY_REFRESH_RETRY_DELAY_MS, currentDelay)
    );
    state.discoveryRegistryRefreshRetryDelayMs = Math.min(
      REGISTRY_REFRESH_RETRY_MAX_DELAY_MS,
      delay * 2
    );
    return delay;
  }

  function resetRegistryRefreshRetryDelay() {
    state.discoveryRegistryRefreshRetryDelayMs = REGISTRY_REFRESH_RETRY_DELAY_MS;
  }

  function scheduleRegistryRefreshRetry(options = {}) {
    if (state.discoveryRegistryRefreshRetryTimer || typeof globalThis.setTimeout !== "function") {
      return;
    }
    const delay = nextRegistryRefreshRetryDelay();
    state.discoveryRegistryRefreshRetryTimer = globalThis.setTimeout(() => {
      state.discoveryRegistryRefreshRetryTimer = null;
      loadDiscoveryData({
        background: true,
        sourceTablesOnly: true,
        completionRefresh: true,
        suppressPlaceholders: true,
        logChanges: false,
        fetchReport: options?.fetchReport || null,
        forceFetchReport: Boolean(options?.fetchReport),
        forcePipelinePreflight: Boolean(options?.forcePipelinePreflight)
      }).catch(() => {});
    }, delay);
    if (typeof state.discoveryRegistryRefreshRetryTimer?.unref === "function") {
      state.discoveryRegistryRefreshRetryTimer.unref();
    }
  }

async function loadDiscoveryData(options = {}) {
    if (state.adminBusyState.discoveryLoad) return state.discoveryLoadPromise || null;
    const nowMs = Date.now();
    if (activeSyncRunning()) {
      const background = Boolean(options?.background);
      if (options?.suppressPlaceholders !== true) {
        markSourceTablesDelayedForActiveWork("sync_running", { onlyIfPlaceholder: true });
      } else {
        state.sourceTablesDelayedDuringActiveRun = true;
        setSourceTablesLoadState("delayed-active", "sync_running");
      }
      const lastNoticeAtMs = Number(state.discoveryPipelineDeferredLoadNoticeAtMs || 0);
      if (!background && nowMs - lastNoticeAtMs > 5000) {
        state.discoveryPipelineDeferredLoadNoticeAtMs = nowMs;
        appendDiscoveryLog(ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL, "warn");
      }
      if (options?.suppressRegistryRetry !== true) {
        scheduleRegistryRefreshRetry({ forcePipelinePreflight: true });
      }
      return {
        skipped: true,
        reason: "sync_running",
        sourceTablesDelayed: true,
        report: state.latestDiscoveryReportCache || null,
        pendingRows: [],
        activeRows: [],
        rejectedRows: [],
        partialLoadFailed: false
      };
    }
    const livePipelineOrFetchRunning = await refreshActivePipelineStatus({
      force: Boolean(options?.forcePipelinePreflight)
    });
    const activeContext = sourceTablesActiveContext({ livePipelineOrFetchRunning });
    const activeCompactSourceTables = Boolean(activeContext.active && activeContext.canLoadCompact);
    if (activeContext.active && !activeContext.canLoadCompact && !options?.forceFullDiscoveryDuringActiveRun) {
      const background = Boolean(options?.background);
      if (options?.suppressPlaceholders !== true) {
        markSourceTablesDelayedForActiveWork(activeContext.reason, { onlyIfPlaceholder: true });
      } else {
        state.sourceTablesDelayedDuringActiveRun = true;
        setSourceTablesLoadState("delayed-active", activeContext.reason);
      }
      const lastNoticeAtMs = Number(state.discoveryPipelineDeferredLoadNoticeAtMs || 0);
      if (!background && nowMs - lastNoticeAtMs > 5000) {
        state.discoveryPipelineDeferredLoadNoticeAtMs = nowMs;
        appendDiscoveryLog(ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL, "warn");
      }
      if (options?.suppressRegistryRetry !== true) {
        scheduleRegistryRefreshRetry({ forcePipelinePreflight: true });
      }
      return {
        skipped: true,
        reason: activeContext.reason,
        sourceTablesDelayed: true,
        report: state.latestDiscoveryReportCache || null,
        pendingRows: [],
        activeRows: [],
        rejectedRows: [],
        partialLoadFailed: false
      };
    }
    if (bridgeHeavyRoutesRecentlyDegraded() && !options?.forceFullDiscoveryDuringActiveRun) {
      const background = Boolean(options?.background);
      if (options?.suppressPlaceholders !== true) {
        markSourceTablesDelayedForBridgeDegraded({ onlyIfPlaceholder: true });
      } else {
        state.sourceTablesBridgeDegraded = true;
        state.sourceTablesDelayedDuringActiveRun = true;
        setSourceTablesLoadState("delayed-bridge", "bridge_degraded");
      }
      const lastNoticeAtMs = Number(state.discoveryBridgeDegradedDeferredLoadNoticeAtMs || 0);
      if (!background && nowMs - lastNoticeAtMs > 5000) {
        state.discoveryBridgeDegradedDeferredLoadNoticeAtMs = nowMs;
        appendDiscoveryLog(BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL, "warn");
      }
      if (options?.suppressRegistryRetry !== true) {
        scheduleRegistryRefreshRetry({ forcePipelinePreflight: true });
      }
      return {
        skipped: true,
        reason: "bridge_degraded",
        sourceTablesDelayed: true,
        report: state.latestDiscoveryReportCache || null,
        pendingRows: [],
        activeRows: [],
        rejectedRows: [],
        partialLoadFailed: false
      };
    }
    if (activeCompactSourceTables && options?.suppressPlaceholders !== true) {
      markSourceTablesDelayedForActiveWork(activeContext.reason, { onlyIfPlaceholder: true });
    }
    const background = Boolean(options?.background);
    const showPlaceholders = !background && !activeCompactSourceTables && options?.suppressPlaceholders !== true;
    if (showPlaceholders) {
      setSourceTablePlaceholder(refs.adminPendingSourcesEl, "pending");
      setSourceTablePlaceholder(refs.adminActiveSourcesEl, "active");
      setSourceTablePlaceholder(refs.adminRejectedSourcesEl, "rejected");
    }
    const skipIfFreshMs = Math.max(0, Number(options?.skipIfFreshMs || 0));
    const lastLoadAtMs = Number(state.discoveryLastLoadSucceededAtMs || 0);
    if (skipIfFreshMs > 0 && lastLoadAtMs > 0 && nowMs - lastLoadAtMs < skipIfFreshMs) {
      return state.discoveryLoadPromise || null;
    }
    state.discoveryLastLoadStartedAtMs = nowMs;
    const renderToken = ++registryRenderToken;
    setBusyFlag("discoveryLoad", true);
    setSourceTablesLoadState(
      activeCompactSourceTables
        ? "retrying-active"
        : options?.completionRefresh
          ? "recovering-idle"
          : "loading",
      activeContext.reason
    );

    state.discoveryLoadPromise = runDiscoveryLoad({
      state,
      refs,
      getBridge,
      options,
      background,
      activeContext,
      activeCompactSourceTables,
      renderToken,
      registryRenderToken,
      toAdminFilterState,
      resolveLatestFetchReport,
      loadDiscoveryEndpoint,
      buildDiscoveryRegistrySignature,
      resetRegistryRefreshRetryDelay,
      scheduleRegistryRefreshRetry,
      appendDiscoveryLog,
      adminDispatch,
      adminActions,
      getErrorMessage,
      setBusyFlag,
      renderSourcesTable,
      mergeSourceDiscoveryCandidates,
      mergeSourceStatusFromReport,
      applySourceFilter,
      getSourceDiscoveryJobsCount,
      sourcePayloadIsDegradedEmpty,
      setSourceTableRefreshingPlaceholder,
      setSourceTableUnavailablePlaceholder,
      setSourceTablesLoadState,
      renderSourceTablesDelayed,
      markSourceTablesDelayedForActiveWork,
      scheduleDeferredRender
    });

    return state.discoveryLoadPromise;
  }

  return {
    resolveLatestFetchReport,
    toAdminFilterState,
    buildDiscoveryRegistrySignature,
    loadDiscoveryEndpoint,
    nextRegistryRefreshRetryDelay,
    resetRegistryRefreshRetryDelay,
    scheduleRegistryRefreshRetry,
    loadDiscoveryData
  };
}

export { createDiscoveryLoad };

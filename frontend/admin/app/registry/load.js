import { deriveDiscoveryLifecycleCounts, deriveDiscoveryQueuedCount } from "../../domain.js";
import { renderDiscoveryCandidateReviewHtml } from "../../render.js?v=20";
import {
  deriveAdminActiveWorkContext,
  pipelineStatusIndicatesActive,
  pipelineStatusIndicatesDiscovery,
  pipelineStatusIndicatesFetch
} from "../active-work-policy.js";

const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";
const CAP_DEFER_REASONS = new Set(["adapter_cap", "domain_cap", "top_n_cap"]);
const FULL_REGISTRY_LOAD_TIMEOUT_MS = 60000;
const ACTIVE_REGISTRY_LOAD_TIMEOUT_MS = 10000;
const PIPELINE_STATUS_PREFLIGHT_TIMEOUT_MS = 3000;
const REGISTRY_REFRESH_RETRY_DELAY_MS = 5000;
const REGISTRY_REFRESH_RETRY_MAX_DELAY_MS = 30000;
const ACTIVE_PIPELINE_SOURCE_TABLES_DELAYED_LABEL = "Source tables delayed while job update is running.";
const BRIDGE_DEGRADED_SOURCE_TABLES_DELAYED_LABEL = "Source tables delayed while Admin data is retrying.";
const BRIDGE_DEGRADED_SOURCE_TABLES_BACKOFF_MS = 30000;
const JOBS_PIPELINE_STATUS_PATH = "/tasks/run-jobs-pipeline-status";
const SOURCE_TABLE_RECOVERY_STATES = new Set(["delayed-active", "delayed-bridge", "retrying-active", "recovering-idle", "unavailable"]);

function getDiscoveryCandidatesRows(payload) {
  return Array.isArray(payload?.candidates) ? payload.candidates : [];
}

function countCapDeferredCandidates(rows) {
  return rows.filter(row => row?.deferred && CAP_DEFER_REASONS.has(String(row?.deferReason || row?.dropReason || ""))).length;
}

function countJobPositiveDeferredCandidates(rows) {
  return rows.filter(row => row?.deferred && Number(row?.jobsFound ?? row?.sampleCount ?? 0) > 0).length;
}

function formatRegistryCountBasis(summary) {
  if (summary?.summaryExact === true || String(summary?.countBasis || "").toLowerCase() === "normalized") {
    return "normalized counts";
  }
  if (summary?.summaryExact === false || String(summary?.countBasis || "").toLowerCase() === "storage") {
    return "storage snapshot counts";
  }
  return "loaded counts";
}

function formatPendingApprovalBreakdown(summary) {
  const pendingApproval = summary?.pendingApproval && typeof summary.pendingApproval === "object"
    ? summary.pendingApproval
    : null;
  if (!pendingApproval) return "";
  const buckets = pendingApproval.reviewBucketCounts && typeof pendingApproval.reviewBucketCounts === "object"
    ? pendingApproval.reviewBucketCounts
    : {};
  const blockers = pendingApproval.blockerCounts && typeof pendingApproval.blockerCounts === "object"
    ? pendingApproval.blockerCounts
    : {};
  const autoEligible = Number(pendingApproval.autoApprovalEligibleCount || buckets.auto_approvable || 0);
  const weakSignal = Number(buckets.weak_signal || blockers.weak_signal || 0);
  const zeroJobs = Number(buckets.zero_jobs || blockers.zero_jobs || 0);
  const conflictDemoted = Number(buckets.conflict_demoted || blockers.conflict_demoted || 0);
  const existingMatch = Number(buckets.existing_match || blockers.existing_match || 0);
  const deferred = Number(buckets.deferred || blockers.deferred || 0);
  const parts = [
    `auto-eligible ${autoEligible.toLocaleString()}`,
    `weak ${weakSignal.toLocaleString()}`,
    `zero jobs ${zeroJobs.toLocaleString()}`,
    `conflict-demoted ${conflictDemoted.toLocaleString()}`,
    `existing-match ${existingMatch.toLocaleString()}`,
    `deferred ${deferred.toLocaleString()}`
  ];
  return parts.join(", ");
}

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
  let registryRenderToken = 0;

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

  function activeDiscoveryRunning() {
    return deriveAdminActiveWorkContext({ state }).discoveryActive;
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
      scheduleRegistryRefreshRetry({ forcePipelinePreflight: true });
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
    const activeCompactSourceTables = false;
    if (activeContext.active && !options?.forceFullDiscoveryDuringActiveRun) {
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
      scheduleRegistryRefreshRetry({ forcePipelinePreflight: true });
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
      scheduleRegistryRefreshRetry({ forcePipelinePreflight: true });
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
    state.discoveryLoadPromise = (async () => {
      try {
        const filterState = toAdminFilterState();
        const sourceTablesOnly = Boolean(options?.sourceTablesOnly || activeCompactSourceTables);
        const reportPromise = sourceTablesOnly
          ? Promise.resolve(null)
          : loadDiscoveryEndpoint(
            "source discovery report",
            getBridge("/discovery/report"),
            state.latestDiscoveryReportCache || { summary: {}, candidates: [], failures: [] },
            { background }
          );
        const loadRegistrySummary = Boolean(options?.completionRefresh && !activeCompactSourceTables);
        const registrySummaryPromise = loadRegistrySummary
          ? loadDiscoveryEndpoint(
            "Admin registry summary",
            getBridge("/registry/summary"),
            { ok: true, summary: {} },
            { background }
          )
          : Promise.resolve({ ok: true, summary: {} });
        const discoveryCandidatesPromise = sourceTablesOnly
          ? Promise.resolve({ candidates: [] })
          : loadDiscoveryEndpoint(
            "source discovery candidates",
            getBridge("/discovery/candidates"),
            { candidates: [] }
          );
        const latestFetchReportPromise = sourceTablesOnly
          ? Promise.resolve(state.latestFetcherReportCache || {})
          : loadDiscoveryEndpoint(
            "latest fetch report",
            resolveLatestFetchReport(options),
            state.latestFetcherReportCache || {}
          );
        const registrySourcesPath = `/registry/sources?view=table&buckets=pending,active,rejected&includeHiddenPending=${filterState.showZeroJobs ? "1" : "0"}`;
        const registrySourcesTimeoutMs = activeCompactSourceTables
          ? ACTIVE_REGISTRY_LOAD_TIMEOUT_MS
          : FULL_REGISTRY_LOAD_TIMEOUT_MS;
        const registrySourcesPromise = registrySummaryPromise
          .then(registrySummary => loadDiscoveryEndpoint(
            "Admin registry source tables",
            getBridge(registrySourcesPath, { timeoutMs: registrySourcesTimeoutMs }),
            {
              ok: false,
              sources: { pending: [], active: [], rejected: [] },
              summary: registrySummary?.summary || {}
            },
            { registryRefresh: true, background }
          ));
        registrySourcesPromise.then(payload => {
          if (!payload?.__loadFailed) {
            state.sourceTablesBridgeDegraded = false;
            state.adminBridgeHeavyRouteDegradedUntilMs = 0;
          }
        }).catch(() => {});

        const pendingRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
            const pending = {
              sources: Array.isArray(registrySources?.sources?.pending)
                ? registrySources.sources.pending
                : [],
              summary: registrySources?.summary || {},
              __loadFailed: Boolean(registrySources?.__loadFailed),
              __delayedDuringActiveRun: Boolean(registrySources?.__delayedDuringActiveRun)
            };
            const loadFailed = Boolean(pending?.__loadFailed);
            const rows = mergeSourceStatusFromReport(
              mergeSourceDiscoveryCandidates(Array.isArray(pending?.sources) ? pending.sources : [], discoveryCandidates),
              latestFetchReport,
              "pending"
            );
            const hiddenZeroJobsCount = Math.max(
              Number(pending?.summary?.hiddenPendingCount || 0),
              rows.filter(row => getSourceDiscoveryJobsCount(row) === 0).length
            );
            const visibleRows = applySourceFilter(
              filterState.showZeroJobs ? rows : rows.filter(row => getSourceDiscoveryJobsCount(row) !== 0)
            );
            scheduleDeferredRender(() => {
              if (background || renderToken !== registryRenderToken) return;
              if (loadFailed) {
                if (pending.__delayedDuringActiveRun) {
                  renderSourceTablesDelayed({ onlyIfPlaceholder: true });
                } else {
                  setSourceTableUnavailablePlaceholder(refs.adminPendingSourcesEl, "pending");
                }
                return;
              }
              renderSourcesTable(refs.adminPendingSourcesEl, visibleRows, "pending");
              if (
                refs.adminPendingSourcesEl
                && !filterState.showZeroJobs
                && visibleRows.length === 0
                && hiddenZeroJobsCount > 0
              ) {
                refs.adminPendingSourcesEl.innerHTML = `<div class="no-results">${hiddenZeroJobsCount.toLocaleString()} pending sources have 0 discovery jobs and are hidden. Enable "Show zero-jobs pending sources" to view them.</div>`;
              }
            });
            return {
              payload: pending,
              rows,
              hiddenZeroJobsCount,
              visibleRows,
              loadFailed,
              delayedDuringActiveRun: Boolean(pending?.__delayedDuringActiveRun)
            };
          });
        const activeRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
            const active = {
              sources: Array.isArray(registrySources?.sources?.active)
                ? registrySources.sources.active
                : [],
              summary: registrySources?.summary || {},
              __loadFailed: Boolean(registrySources?.__loadFailed),
              __delayedDuringActiveRun: Boolean(registrySources?.__delayedDuringActiveRun)
            };
            const loadFailed = Boolean(active?.__loadFailed);
            const rows = mergeSourceStatusFromReport(
              mergeSourceDiscoveryCandidates(Array.isArray(active?.sources) ? active.sources : [], discoveryCandidates),
              latestFetchReport,
              "active"
            );
            const visibleRows = applySourceFilter(rows);
            scheduleDeferredRender(() => {
              if (background || renderToken !== registryRenderToken) return;
              if (loadFailed) {
                if (active.__delayedDuringActiveRun) {
                  renderSourceTablesDelayed({ onlyIfPlaceholder: true });
                } else {
                  setSourceTableUnavailablePlaceholder(refs.adminActiveSourcesEl, "active");
                }
                return;
              }
              renderSourcesTable(refs.adminActiveSourcesEl, visibleRows, "active");
            });
            return {
              payload: active,
              rows,
              visibleRows,
              loadFailed,
              delayedDuringActiveRun: Boolean(active?.__delayedDuringActiveRun)
            };
          });
        const rejectedRowsPromise = Promise.all([registrySourcesPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([registrySources, discoveryCandidates, latestFetchReport]) => {
            const rejected = {
              sources: Array.isArray(registrySources?.sources?.rejected)
                ? registrySources.sources.rejected
                : [],
              summary: registrySources?.summary || {},
              __loadFailed: Boolean(registrySources?.__loadFailed),
              __delayedDuringActiveRun: Boolean(registrySources?.__delayedDuringActiveRun)
            };
            const loadFailed = Boolean(rejected?.__loadFailed);
            const rows = mergeSourceStatusFromReport(
              mergeSourceDiscoveryCandidates(Array.isArray(rejected?.sources) ? rejected.sources : [], discoveryCandidates),
              latestFetchReport,
              "rejected"
            );
            const visibleRows = applySourceFilter(rows);
            scheduleDeferredRender(() => {
              if (background || renderToken !== registryRenderToken) return;
              if (loadFailed) {
                if (rejected.__delayedDuringActiveRun) {
                  renderSourceTablesDelayed({ onlyIfPlaceholder: true });
                } else {
                  setSourceTableUnavailablePlaceholder(refs.adminRejectedSourcesEl, "rejected");
                }
                return;
              }
              renderSourcesTable(refs.adminRejectedSourcesEl, visibleRows, "rejected");
            });
            return {
              payload: rejected,
              rows,
              visibleRows,
              loadFailed,
              delayedDuringActiveRun: Boolean(rejected?.__delayedDuringActiveRun)
            };
          });
        const [report, discoveryCandidates, pendingResult, activeResult, rejectedResult] = await Promise.all([
          reportPromise,
          discoveryCandidatesPromise,
          pendingRowsPromise,
          activeRowsPromise,
          rejectedRowsPromise
        ]);
        const pending = pendingResult.payload;
        const active = activeResult.payload;
        const rejected = rejectedResult.payload;
        if (report && typeof report === "object" && !Array.isArray(report)) {
          state.latestDiscoveryReportCache = report;
        }
        const summary = report?.summary || {};
        const foundCount = Number(summary.foundEndpointCount ?? summary.probedCount ?? 0);
        const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
        const queuedCount = deriveDiscoveryQueuedCount(report);
        const deferredCount = Number(summary.discoverableButDeferredCount ?? 0);
        const lifecycleCounts = deriveDiscoveryLifecycleCounts(report);
        const skippedCount = Number(summary.skippedDuplicateCount || 0);
        const failedCount = Number(summary.failedProbeCount || 0);
        const discoveryCandidateRows = getDiscoveryCandidatesRows(discoveryCandidates);
        const capDeferredCount = countCapDeferredCandidates(discoveryCandidateRows);
        const jobPositiveDeferredCount = countJobPositiveDeferredCandidates(discoveryCandidateRows);
        const runtimeAutoApproval = report?.runtime?.autoApproval && typeof report.runtime.autoApproval === "object"
          ? report.runtime.autoApproval
          : {};
        const autoApprovedCount = Number(summary.approvedCandidateCount ?? runtimeAutoApproval.approvedCount ?? 0);
        const activeRegistryCount = Number(active?.summary?.activeCount || 0);
        const registryCountBasisLabel = formatRegistryCountBasis(
          active?.summary || pending?.summary || rejected?.summary || {}
        );
        const pendingRows = pendingResult.rows;
        const activeRows = activeResult.rows;
        const rejectedRows = rejectedResult.rows;
        const partialLoadFailed = Boolean(
          report?.__loadFailed
          || pendingResult.loadFailed
          || activeResult.loadFailed
          || rejectedResult.loadFailed
        );
        const registryDelayedDuringActiveRun = Boolean(
          pending?.__delayedDuringActiveRun
          || active?.__delayedDuringActiveRun
          || rejected?.__delayedDuringActiveRun
          || activeResult.delayedDuringActiveRun
          || rejectedResult.delayedDuringActiveRun
        );
        const registrySignature = buildDiscoveryRegistrySignature({
          pending: pendingRows,
          active: activeRows,
          rejected: rejectedRows
        });
        const hiddenZeroJobsCount = pendingResult.hiddenZeroJobsCount;

        if (!sourceTablesOnly && refs.adminDiscoverySummaryEl) {
          const pendingApprovalBreakdown = formatPendingApprovalBreakdown(pending?.summary);
          const pendingApprovalText = pendingApprovalBreakdown
            ? ` | Pending source blockers: ${pendingApprovalBreakdown}`
            : "";
          const summaryText = `Found ${foundCount} | Probed ${probedCount} | Review queue ${queuedCount} | Deferred review ${deferredCount} | Deferred by caps ${capDeferredCount} | Job-positive deferred ${jobPositiveDeferredCount} | Validated ${lifecycleCounts.validated} | Auto-approved this run ${autoApprovedCount} | Active registry ${activeRegistryCount} (${registryCountBasisLabel}) | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending sources ${Number(pending?.summary?.pendingCount || 0)}${pendingApprovalText} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
          refs.adminDiscoverySummaryEl.textContent = summaryText;
          refs.adminDiscoverySummaryEl.innerHTML = `<div>${summaryText}</div>`;
        }
        if (refs.discoveryPendingBadgeEl) {
          const pendingCount = Number(pending?.summary?.pendingCount || 0);
          if (pendingCount > 0) {
            refs.discoveryPendingBadgeEl.textContent = pendingCount > 999 ? "999+" : pendingCount.toLocaleString();
            refs.discoveryPendingBadgeEl.classList.remove("hidden");
          } else {
            refs.discoveryPendingBadgeEl.classList.add("hidden");
          }
        }
        if (!sourceTablesOnly && refs.adminDiscoveryReviewEl) {
          refs.adminDiscoveryReviewEl.innerHTML = renderDiscoveryCandidateReviewHtml(
            report?.candidateReview,
            { showEmpty: true }
          );
        }
        const registryChanged = registrySignature !== String(state.discoveryRegistrySignature || "");
        if (!partialLoadFailed) {
          state.discoveryRegistrySignature = registrySignature;
        }
        const shouldRenderTables = Boolean(
          !partialLoadFailed
          && (
            options?.forceRender
            || registryChanged
            || !state.discoveryTablesRendered
          )
        );
        if (background && shouldRenderTables) {
          scheduleDeferredRender(() => {
            if (renderToken !== registryRenderToken) return;
            renderSourcesTable(refs.adminPendingSourcesEl, pendingResult.visibleRows || [], "pending");
            if (
              refs.adminPendingSourcesEl
              && !filterState.showZeroJobs
              && (pendingResult.visibleRows || []).length === 0
              && hiddenZeroJobsCount > 0
            ) {
              refs.adminPendingSourcesEl.innerHTML = `<div class="no-results">${hiddenZeroJobsCount.toLocaleString()} pending sources have 0 discovery jobs and are hidden. Enable "Show zero-jobs pending sources" to view them.</div>`;
            }
            renderSourcesTable(refs.adminActiveSourcesEl, activeResult.visibleRows || [], "active");
            renderSourcesTable(refs.adminRejectedSourcesEl, rejectedResult.visibleRows || [], "rejected");
            state.discoveryTablesRendered = true;
          });
        } else if (!background) {
          state.discoveryTablesRendered = true;
        }
        if (!partialLoadFailed && registryChanged && options?.logChanges !== false) {
          if (sourceTablesOnly) {
            appendDiscoveryLog("Source registry tables loaded.", "success");
          } else {
            appendDiscoveryLog("Loading source discovery report and registries...");
            appendDiscoveryLog(
              `Discovery summary: found ${foundCount}, probed ${probedCount}, review queue ${queuedCount}, auto-approved ${autoApprovedCount}, failed ${failedCount}, skipped duplicates ${skippedCount}.`,
              "info"
            );
            const topFailures = Array.isArray(report?.topFailures) ? report.topFailures : [];
            if (topFailures.length) {
              const line = topFailures
                .slice(0, 3)
                .map(item => `${String(item?.key || "unknown")} (${Number(item?.count || 0)})`)
                .join(", ");
              appendDiscoveryLog(`Top failures: ${line}`, "warn");
            }
            appendDiscoveryLog("Source discovery data loaded.", "success");
          }
        }
        adminDispatch.dispatch({ type: adminActions.DISCOVERY_REFRESHED, payload: { at: new Date().toISOString() } });
        if (!partialLoadFailed) {
          state.sourceTablesDelayedDuringActiveRun = false;
          state.discoveryLastLoadSucceededAtMs = Date.now();
          setSourceTablesLoadState("loaded", activeContext.reason);
          resetRegistryRefreshRetryDelay();
        } else if (registryDelayedDuringActiveRun) {
          markSourceTablesDelayedForActiveWork("active_registry_timeout", { onlyIfPlaceholder: true });
          scheduleRegistryRefreshRetry({ forcePipelinePreflight: true, fetchReport: options?.fetchReport || null });
        } else {
          setSourceTablesLoadState("unavailable", activeContext.reason);
        }
        return {
          report,
          pendingRows,
          activeRows,
          rejectedRows,
          partialLoadFailed
        };
      } catch (err) {
        setSourceTablesLoadState("unavailable", activeContext.reason);
        appendDiscoveryLog(`Could not load source discovery data: ${getErrorMessage(err)}`, "error");
        if (refs.adminDiscoverySummaryEl) {
          const message = getErrorMessage(err);
          if (String(message || "").includes("bridge unreachable")) {
            refs.adminDiscoverySummaryEl.textContent = "Source discovery bridge unavailable. Start `Run admin bridge` task.";
          }
        }
        if (refs.adminDiscoveryReviewEl) {
          refs.adminDiscoveryReviewEl.innerHTML = '<div class="no-results">Discovery review unavailable.</div>';
        }
        return null;
      } finally {
        state.discoveryLoadPromise = null;
        state.discoveryLastLoadCompletedAtMs = Date.now();
        setBusyFlag("discoveryLoad", false);
      }
    })();
    return state.discoveryLoadPromise;
  }

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
    markSourceTablesDelayedForActiveWork
  };
}

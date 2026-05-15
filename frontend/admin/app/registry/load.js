import { deriveDiscoveryLifecycleCounts, deriveDiscoveryQueuedCount } from "../../domain.js";
import { renderDiscoveryCandidateReviewHtml } from "../../render.js?v=15";

const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";
const CAP_DEFER_REASONS = new Set(["adapter_cap", "domain_cap", "top_n_cap"]);

function getDiscoveryCandidatesRows(payload) {
  return Array.isArray(payload?.candidates) ? payload.candidates : [];
}

function countCapDeferredCandidates(rows) {
  return rows.filter(row => row?.deferred && CAP_DEFER_REASONS.has(String(row?.deferReason || row?.dropReason || ""))).length;
}

function countJobPositiveDeferredCandidates(rows) {
  return rows.filter(row => row?.deferred && Number(row?.jobsFound ?? row?.sampleCount ?? 0) > 0).length;
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

  function scheduleDeferredRender(callback) {
    const scheduleRender = typeof renderScheduler === "function"
      ? renderScheduler
      : renderCallback => {
        renderCallback();
        return () => {};
      };
    scheduleRender(callback);
  }

  async function loadDiscoveryEndpoint(label, promise, fallback) {
    try {
      return await promise;
    } catch (err) {
      appendDiscoveryLog(`Could not load ${label}: ${getErrorMessage(err)}`, "error");
      return {
        ...(fallback && typeof fallback === "object" && !Array.isArray(fallback) ? fallback : {}),
        __loadFailed: true
      };
    }
  }

  async function loadDiscoveryData(options = {}) {
    if (state.adminBusyState.discoveryLoad) return state.discoveryLoadPromise || null;
    const nowMs = Date.now();
    const liveDiscoveryRunning = Boolean(
      state.adminBusyState?.liveDiscoveryRunning
      || state.adminBusyState?.discoveryWatch
      || state.discoveryLiveProgressState
    );
    const allowDuringLiveDiscovery = Boolean(
      options?.allowDuringLiveDiscovery
      || options?.completionRefresh
      || options?.forceDuringLiveDiscovery
    );
    if (liveDiscoveryRunning && !allowDuringLiveDiscovery) {
      const background = Boolean(options?.background);
      const lastNoticeAtMs = Number(state.discoveryDeferredLoadNoticeAtMs || 0);
      if (!background && nowMs - lastNoticeAtMs > 5000) {
        state.discoveryDeferredLoadNoticeAtMs = nowMs;
        appendDiscoveryLog(
          "Discovery is running; source tables will refresh after this run completes.",
          "info"
        );
      }
      return {
        skipped: true,
        reason: "discovery_running",
        report: state.latestDiscoveryReportCache || null,
        pendingRows: [],
        activeRows: [],
        rejectedRows: [],
        partialLoadFailed: false
      };
    }
    const skipIfFreshMs = Math.max(0, Number(options?.skipIfFreshMs || 0));
    const lastLoadAtMs = Number(state.discoveryLastLoadSucceededAtMs || 0);
    if (skipIfFreshMs > 0 && lastLoadAtMs > 0 && nowMs - lastLoadAtMs < skipIfFreshMs) {
      return state.discoveryLoadPromise || null;
    }
    state.discoveryLastLoadStartedAtMs = nowMs;
    const renderToken = ++registryRenderToken;
    const background = Boolean(options?.background);
    const showPlaceholders = !background && options?.suppressPlaceholders !== true;
    setBusyFlag("discoveryLoad", true);
    state.discoveryLoadPromise = (async () => {
      try {
        const filterState = toAdminFilterState();
        const pendingPath = filterState.showZeroJobs ? "/registry/pending?includeHidden=1" : "/registry/pending";
        if (showPlaceholders) {
          setSourceTablePlaceholder(refs.adminPendingSourcesEl, "pending");
          setSourceTablePlaceholder(refs.adminActiveSourcesEl, "active");
          setSourceTablePlaceholder(refs.adminRejectedSourcesEl, "rejected");
        }
        const reportPromise = loadDiscoveryEndpoint(
          "source discovery report",
          getBridge("/discovery/report"),
          state.latestDiscoveryReportCache || { summary: {}, candidates: [], failures: [] }
        );
        const discoveryCandidatesPromise = loadDiscoveryEndpoint(
          "source discovery candidates",
          getBridge("/discovery/candidates"),
          { candidates: [] }
        );
        const latestFetchReportPromise = loadDiscoveryEndpoint(
          "latest fetch report",
          resolveLatestFetchReport(options),
          state.latestFetcherReportCache || {}
        );
        const pendingPromise = loadDiscoveryEndpoint(
          "pending registry",
          getBridge(pendingPath),
          { sources: [], summary: {} }
        );
        const activePromise = loadDiscoveryEndpoint(
          "active registry",
          getBridge("/registry/active"),
          { sources: [], summary: {} }
        );
        const rejectedPromise = loadDiscoveryEndpoint(
          "rejected registry",
          getBridge("/registry/rejected"),
          { sources: [], summary: {} }
        );

        const pendingRowsPromise = Promise.all([pendingPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([pending, discoveryCandidates, latestFetchReport]) => {
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
              if (background || loadFailed || renderToken !== registryRenderToken) return;
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
            return { payload: pending, rows, hiddenZeroJobsCount, visibleRows, loadFailed };
          });
        const activeRowsPromise = Promise.all([activePromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([active, discoveryCandidates, latestFetchReport]) => {
            const loadFailed = Boolean(active?.__loadFailed);
            const rows = mergeSourceStatusFromReport(
              mergeSourceDiscoveryCandidates(Array.isArray(active?.sources) ? active.sources : [], discoveryCandidates),
              latestFetchReport,
              "active"
            );
            const visibleRows = applySourceFilter(rows);
            scheduleDeferredRender(() => {
              if (background || loadFailed || renderToken !== registryRenderToken) return;
              renderSourcesTable(refs.adminActiveSourcesEl, visibleRows, "active");
            });
            return { payload: active, rows, visibleRows, loadFailed };
          });
        const rejectedRowsPromise = Promise.all([rejectedPromise, discoveryCandidatesPromise, latestFetchReportPromise])
          .then(([rejected, discoveryCandidates, latestFetchReport]) => {
            const loadFailed = Boolean(rejected?.__loadFailed);
            const rows = mergeSourceStatusFromReport(
              mergeSourceDiscoveryCandidates(Array.isArray(rejected?.sources) ? rejected.sources : [], discoveryCandidates),
              latestFetchReport,
              "rejected"
            );
            const visibleRows = applySourceFilter(rows);
            scheduleDeferredRender(() => {
              if (background || loadFailed || renderToken !== registryRenderToken) return;
              renderSourcesTable(refs.adminRejectedSourcesEl, visibleRows, "rejected");
            });
            return { payload: rejected, rows, visibleRows, loadFailed };
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
        const pendingRows = pendingResult.rows;
        const activeRows = activeResult.rows;
        const rejectedRows = rejectedResult.rows;
        const partialLoadFailed = Boolean(
          report?.__loadFailed
          || pendingResult.loadFailed
          || activeResult.loadFailed
          || rejectedResult.loadFailed
        );
        const registrySignature = buildDiscoveryRegistrySignature({
          pending: pendingRows,
          active: activeRows,
          rejected: rejectedRows
        });
        const hiddenZeroJobsCount = pendingResult.hiddenZeroJobsCount;

        if (refs.adminDiscoverySummaryEl) {
          const summaryText = `Found ${foundCount} | Probed ${probedCount} | Review queue ${queuedCount} | Deferred review ${deferredCount} | Deferred by caps ${capDeferredCount} | Job-positive deferred ${jobPositiveDeferredCount} | Validated ${lifecycleCounts.validated} | Auto-approved this run ${autoApprovedCount} | Active registry ${activeRegistryCount} | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending ${Number(pending?.summary?.pendingCount || 0)} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
          refs.adminDiscoverySummaryEl.textContent = summaryText;
          refs.adminDiscoverySummaryEl.innerHTML = `<div>${summaryText}</div>`;
        }
        if (refs.adminDiscoveryReviewEl) {
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
        adminDispatch.dispatch({ type: adminActions.DISCOVERY_REFRESHED, payload: { at: new Date().toISOString() } });
        if (!partialLoadFailed) {
          state.discoveryLastLoadSucceededAtMs = Date.now();
        }
        return {
          report,
          pendingRows,
          activeRows,
          rejectedRows,
          partialLoadFailed
        };
      } catch (err) {
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
      state[signatureKey] = signature;
    }
    return result;
  }

  return {
    loadDiscoveryData,
    syncSourceTablesAfterTaskCompletion
  };
}

import { deriveDiscoveryLifecycleCounts, deriveDiscoveryQueuedCount } from "../../domain.js";

const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";

export function createRegistryLoadController({
  state,
  refs,
  getBridge,
  fetchJobsFetchReportJson,
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
  renderSourcesTable
}) {
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
    return JSON.stringify(
      buckets.map(bucket => {
        const rows = Array.isArray(rowsByBucket?.[bucket]) ? rowsByBucket[bucket] : [];
        return rows
          .map(row => ({
            id: String(row?.id || row?.sourceId || row?.name || ""),
            name: String(row?.name || ""),
            adapter: String(row?.adapter || ""),
            studio: String(row?.studio || ""),
            status: String(row?.status || ""),
            jobsFound: Number(getSourceJobsFoundCount(row) || 0),
            sourceId: String(row?.sourceId || ""),
            sourceUrl: String(row?.url || row?.sourceUrl || "")
          }))
          .sort((left, right) => {
            const leftKey = `${left.id}|${left.name}|${left.sourceUrl}`;
            const rightKey = `${right.id}|${right.name}|${right.sourceUrl}`;
            return leftKey.localeCompare(rightKey);
          });
      })
    );
  }

  async function loadDiscoveryData(options = {}) {
    if (state.adminBusyState.discoveryLoad) return state.discoveryLoadPromise || null;
    setBusyFlag("discoveryLoad", true);
    state.discoveryLoadPromise = (async () => {
      try {
        const [report, pending, active, rejected, latestFetchReport] = await Promise.all([
          getBridge("/discovery/report"),
          getBridge("/registry/pending"),
          getBridge("/registry/active"),
          getBridge("/registry/rejected"),
          resolveLatestFetchReport(options)
        ]);
        const summary = report?.summary || {};
        const foundCount = Number(summary.foundEndpointCount ?? summary.probedCount ?? 0);
        const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
        const queuedCount = deriveDiscoveryQueuedCount(report);
        const deferredCount = Number(summary.discoverableButDeferredCount ?? 0);
        const lifecycleCounts = deriveDiscoveryLifecycleCounts(report);
        const skippedCount = Number(summary.skippedDuplicateCount || 0);
        const failedCount = Number(summary.failedProbeCount || 0);
        const pendingRows = mergeSourceStatusFromReport(Array.isArray(pending?.sources) ? pending.sources : [], latestFetchReport, "pending");
        const activeRows = mergeSourceStatusFromReport(Array.isArray(active?.sources) ? active.sources : [], latestFetchReport, "active");
        const rejectedRows = mergeSourceStatusFromReport(Array.isArray(rejected?.sources) ? rejected.sources : [], latestFetchReport, "rejected");
        const registrySignature = buildDiscoveryRegistrySignature({
          pending: pendingRows,
          active: activeRows,
          rejected: rejectedRows
        });
        const filterState = toAdminFilterState();
        const hiddenZeroJobsCount = pendingRows.filter(row => getSourceDiscoveryJobsCount(row) === 0).length;
        const visiblePendingRows = applySourceFilter(
          filterState.showZeroJobs ? pendingRows : pendingRows.filter(row => getSourceDiscoveryJobsCount(row) !== 0)
        );
        const visibleActiveRows = applySourceFilter(activeRows);
        const visibleRejectedRows = applySourceFilter(rejectedRows);

        if (refs.adminDiscoverySummaryEl) {
          refs.adminDiscoverySummaryEl.textContent =
            `Found ${foundCount} | Probed ${probedCount} | Queued (new) ${queuedCount} | Deferred review ${deferredCount} | Validated ${lifecycleCounts.validated} | Live ${lifecycleCounts.live} | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending ${Number(pending?.summary?.pendingCount || 0)} | Active ${Number(active?.summary?.activeCount || 0)} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
        }
        renderSourcesTable(refs.adminPendingSourcesEl, visiblePendingRows, "pending");
        renderSourcesTable(refs.adminActiveSourcesEl, visibleActiveRows, "active");
        renderSourcesTable(refs.adminRejectedSourcesEl, visibleRejectedRows, "rejected");
        const registryChanged = registrySignature !== String(state.discoveryRegistrySignature || "");
        state.discoveryRegistrySignature = registrySignature;
        if (registryChanged && options?.logChanges !== false) {
          appendDiscoveryLog("Loading source discovery report and registries...");
          appendDiscoveryLog(
            `Discovery summary: found ${foundCount}, probed ${probedCount}, queued (new) ${queuedCount}, failed ${failedCount}, skipped duplicates ${skippedCount}.`,
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
        return {
          report,
          pendingRows,
          activeRows,
          rejectedRows
        };
      } catch (err) {
        appendDiscoveryLog(`Could not load source discovery data: ${getErrorMessage(err)}`, "error");
        if (refs.adminDiscoverySummaryEl) {
          refs.adminDiscoverySummaryEl.textContent = "Source discovery bridge unavailable. Start `Run admin bridge` task.";
        }
        return null;
      } finally {
        state.discoveryLoadPromise = null;
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
      fetchReport,
      forceFetchReport: Boolean(fetchReport),
      logChanges: false
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

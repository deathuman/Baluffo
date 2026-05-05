import { deriveDiscoveryLifecycleCounts, deriveDiscoveryQueuedCount } from "../../domain.js";
import { renderDiscoveryCandidateReviewHtml } from "../../render.js";

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

  async function loadDiscoveryData(options = {}) {
    if (state.adminBusyState.discoveryLoad) return state.discoveryLoadPromise || null;
    setBusyFlag("discoveryLoad", true);
    state.discoveryLoadPromise = (async () => {
      try {
        const filterState = toAdminFilterState();
        const pendingPath = filterState.showZeroJobs ? "/registry/pending?includeHidden=1" : "/registry/pending";
        const [report, discoveryCandidates, pending, active, rejected, latestFetchReport] = await Promise.all([
          getBridge("/discovery/report"),
          getBridge("/discovery/candidates").catch(() => ({ candidates: [] })),
          getBridge(pendingPath),
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
        const discoveryCandidateRows = getDiscoveryCandidatesRows(discoveryCandidates);
        const capDeferredCount = countCapDeferredCandidates(discoveryCandidateRows);
        const jobPositiveDeferredCount = countJobPositiveDeferredCandidates(discoveryCandidateRows);
        const runtimeAutoApproval = report?.runtime?.autoApproval && typeof report.runtime.autoApproval === "object"
          ? report.runtime.autoApproval
          : {};
        const autoApprovedCount = Number(summary.approvedCandidateCount ?? runtimeAutoApproval.approvedCount ?? 0);
        const activeRegistryCount = Number(active?.summary?.activeCount || 0);
        const pendingRows = mergeSourceStatusFromReport(
          mergeSourceDiscoveryCandidates(Array.isArray(pending?.sources) ? pending.sources : [], discoveryCandidates),
          latestFetchReport,
          "pending"
        );
        const activeRows = mergeSourceStatusFromReport(
          mergeSourceDiscoveryCandidates(Array.isArray(active?.sources) ? active.sources : [], discoveryCandidates),
          latestFetchReport,
          "active"
        );
        const rejectedRows = mergeSourceStatusFromReport(
          mergeSourceDiscoveryCandidates(Array.isArray(rejected?.sources) ? rejected.sources : [], discoveryCandidates),
          latestFetchReport,
          "rejected"
        );
        const registrySignature = buildDiscoveryRegistrySignature({
          pending: pendingRows,
          active: activeRows,
          rejected: rejectedRows
        });
        const hiddenZeroJobsCount = Math.max(
          Number(pending?.summary?.hiddenPendingCount || 0),
          pendingRows.filter(row => getSourceDiscoveryJobsCount(row) === 0).length
        );
        const visiblePendingRows = applySourceFilter(
          filterState.showZeroJobs ? pendingRows : pendingRows.filter(row => getSourceDiscoveryJobsCount(row) !== 0)
        );
        const visibleActiveRows = applySourceFilter(activeRows);
        const visibleRejectedRows = applySourceFilter(rejectedRows);

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
        renderSourcesTable(refs.adminPendingSourcesEl, visiblePendingRows, "pending");
        if (
          refs.adminPendingSourcesEl
          && !filterState.showZeroJobs
          && visiblePendingRows.length === 0
          && hiddenZeroJobsCount > 0
        ) {
          refs.adminPendingSourcesEl.innerHTML = `<div class="no-results">${hiddenZeroJobsCount.toLocaleString()} pending sources have 0 discovery jobs and are hidden. Enable "Show zero-jobs pending sources" to view them.</div>`;
        }
        renderSourcesTable(refs.adminActiveSourcesEl, visibleActiveRows, "active");
        renderSourcesTable(refs.adminRejectedSourcesEl, visibleRejectedRows, "rejected");
        const registryChanged = registrySignature !== String(state.discoveryRegistrySignature || "");
        state.discoveryRegistrySignature = registrySignature;
        if (registryChanged && options?.logChanges !== false) {
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
        if (refs.adminDiscoveryReviewEl) {
          refs.adminDiscoveryReviewEl.innerHTML = '<div class="no-results">Discovery review unavailable.</div>';
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

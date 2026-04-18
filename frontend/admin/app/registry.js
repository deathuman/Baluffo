import { requestConfirmationDialog } from "../../local-data/profile-name-dialog.js";
import { deriveDiscoveryLifecycleCounts, deriveDiscoveryQueuedCount } from "../domain.js";

const DISCOVERY_OPERATION_BLOCKED_MESSAGE = "Another discovery operation is running.";

function isDiscoveryOperationBlocked(state) {
  return Boolean(
    state.adminBusyState.discoveryRun
    || state.adminBusyState.discoveryWatch
    || state.adminBusyState.discoveryLoad
    || state.adminBusyState.discoveryWrite
    || state.adminBusyState.manualAdd
    || state.adminBusyState.manualCheck
    || state.adminBusyState.liveDiscoveryRunning
  );
}

async function runRegistryMutation({
  state,
  busyKey,
  setBusyFlag,
  showToast,
  execute,
  onError
}) {
  if (isDiscoveryOperationBlocked(state)) {
    showToast(DISCOVERY_OPERATION_BLOCKED_MESSAGE, "info");
    return null;
  }
  if (state.adminBusyState[busyKey]) {
    showToast("This registry action is already in progress.", "info");
    return null;
  }
  setBusyFlag(busyKey, true);
  try {
    return await execute();
  } catch (err) {
    if (typeof onError === "function") {
      return onError(err);
    }
    throw err;
  } finally {
    setBusyFlag(busyKey, false);
  }
}

export function createAdminRegistryController({
  state,
  refs,
  getBridge,
  postBridge,
  fetchJobsFetchReportJson,
  mergeSourceStatusFromReport,
  applySourceFilter,
  getSourceJobsFoundCount,
  deriveSourceStatus,
  renderSourcesTableHtml,
  readShowZeroJobs,
  normalizeSourceFilter,
  adminDispatch,
  adminActions,
  appendDiscoveryLog,
  formatManualCheckFailureMessage,
  loadOpsHealthData,
  setBusyFlag,
  showToast,
  getErrorMessage
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

  function setManualSourceFeedback(message, level = "muted") {
    if (!refs.adminManualSourceFeedbackEl) return;
    const normalized = String(level || "muted").toLowerCase();
    refs.adminManualSourceFeedbackEl.textContent = String(message || "");
    refs.adminManualSourceFeedbackEl.classList.remove("success", "warn", "error", "muted");
    refs.adminManualSourceFeedbackEl.classList.add(
      normalized === "success" ? "success" : normalized === "warn" ? "warn" : normalized === "error" ? "error" : "muted"
    );
  }

  function renderSourcesTable(container, rows, mode = "pending") {
    if (!container) return;
    container.innerHTML = renderSourcesTableHtml(rows, mode, row => {
      const value = getSourceJobsFoundCount(row);
      return Number.isFinite(value) && value >= 0 ? value.toLocaleString() : "N/A";
    }, deriveSourceStatus);
  }

  function getBucketContainer(type) {
    if (type === "pending") return refs.adminPendingSourcesEl;
    if (type === "active") return refs.adminActiveSourcesEl;
    if (type === "rejected") return refs.adminRejectedSourcesEl;
    return null;
  }

  function queryScopedSelector(container, selector) {
    if (container && typeof container.querySelectorAll === "function") {
      return Array.from(container.querySelectorAll(selector));
    }
    return [];
  }

  function selectedIds(container, selector) {
    return queryScopedSelector(container, selector)
      .filter(el => el instanceof HTMLInputElement && el.checked)
      .map(el => String(el.dataset.sourceId || ""))
      .filter(Boolean);
  }

  function selectedSourcesAcrossDiscoveryBuckets() {
    const out = [];
    const seen = new Set();
    [
      ["pending", ".pending-source-checkbox"],
      ["active", ".active-source-checkbox"],
      ["rejected", ".rejected-source-checkbox"]
    ].forEach(([type, selector]) => {
      const container = getBucketContainer(type);
      queryScopedSelector(container, selector)
        .filter(el => el instanceof HTMLInputElement && el.checked)
        .map(el => ({
          id: String(el.dataset.sourceId || "").trim(),
          url: String(el.dataset.sourceUrl || "").trim()
        }))
        .filter(item => item.id || item.url)
        .forEach(item => {
          const key = `${item.id}|${item.url}`;
          if (!key || seen.has(key)) return;
          seen.add(key);
          out.push(item);
        });
    });
    return out;
  }

  function toggleSelectAllSources(type, checkAll) {
    const classMap = {
      pending: ".pending-source-checkbox",
      active: ".active-source-checkbox",
      rejected: ".rejected-source-checkbox"
    };
    const selector = classMap[type];
    if (!selector) return;
    queryScopedSelector(getBucketContainer(type), selector).forEach(cb => {
      cb.checked = Boolean(checkAll);
    });
  }

  function toAdminFilterState() {
    return {
      activeSourceFilter: normalizeSourceFilter(state.activeSourceFilter),
      showZeroJobs: readShowZeroJobs("baluffo_admin_show_zero_jobs_sources")
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

  async function addManualSource() {
    return runRegistryMutation({
      state,
      busyKey: "manualAdd",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Manual source add failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not add manual source.", "error");
        return null;
      },
      execute: async () => {
        const url = String(refs.adminManualSourceUrlEl?.value || "").trim();
        if (!url) {
          setManualSourceFeedback("invalid URL", "error");
          showToast("Enter a source URL.", "error");
          return null;
        }

        const addResult = await postBridge("/sources/manual", { url });
        const status = String(addResult?.status || "").toLowerCase();

        if (status === "invalid") {
          setManualSourceFeedback("invalid URL", "error");
          appendDiscoveryLog(`Manual source invalid: ${String(addResult?.message || "invalid URL")}`, "error");
          showToast(String(addResult?.message || "Invalid source URL."), "error");
          return null;
        }
        if (status === "duplicate") {
          setManualSourceFeedback("duplicate skipped", "warn");
          appendDiscoveryLog("Manual source duplicate skipped.", "warn");
          showToast("Source already exists. Skipped duplicate.", "info");
          return null;
        }
        if (status !== "added") {
          setManualSourceFeedback("check failed", "error");
          showToast("Could not add manual source.", "error");
          return null;
        }

        if (refs.adminManualSourceUrlEl) refs.adminManualSourceUrlEl.value = "";
        setManualSourceFeedback("added", "success");
        if (String(addResult?.source?.adapter || "").toLowerCase() === "static") {
          appendDiscoveryLog("No known provider detected, using generic website scraping.", "warn");
        }
        appendDiscoveryLog("Manual source added.", "success");

        const sourceId = String(addResult?.sourceId || "");
        if (sourceId) {
          setBusyFlag("manualCheck", true);
          try {
            setManualSourceFeedback("check started", "muted");
            const checkResult = await postBridge("/discovery/check-source", { sourceId });
            if (!checkResult?.started || checkResult?.ok === false) {
              setManualSourceFeedback("check failed", "error");
              appendDiscoveryLog(`Manual source check failed: ${String(checkResult?.error || "unknown error")}`, "error");
              if (Array.isArray(checkResult?.suggestedUrls) && checkResult.suggestedUrls.length) {
                appendDiscoveryLog(`Try alternate URL(s): ${checkResult.suggestedUrls.join(" | ")}`, "warn");
              }
              if (checkResult?.browserFallbackAttempted) {
                appendDiscoveryLog("Browser fallback was attempted during this check.", "muted");
              }
              showToast(formatManualCheckFailureMessage(checkResult), "error");
            } else {
              appendDiscoveryLog(
                `Manual source check completed (jobs found: ${Number(checkResult?.jobsFound || 0)}${checkResult?.weakSignal ? ", weak signal" : ""}).`,
                "success"
              );
              if (checkResult?.browserFallbackUsed) {
                appendDiscoveryLog("Generic browser fallback was used to bypass a blocked page.", "warn");
              }
              showToast("Manual source added and checked.", "success");
            }
          } finally {
            setBusyFlag("manualCheck", false);
          }
        }

        await loadDiscoveryData();
        await loadOpsHealthData();
        return addResult || null;
      }
    });
  }

  async function approveSelectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Approve failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not approve sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("pending"), ".pending-source-checkbox");
        if (!ids.length) {
          showToast("Select pending sources to approve.", "info");
          return null;
        }
        const result = await postBridge("/registry/approve", { ids });
        appendDiscoveryLog(`Approved ${Number(result?.approved || 0)} source(s).`, "success");
        showToast("Sources approved.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function rejectSelectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Reject failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not reject sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("pending"), ".pending-source-checkbox");
        if (!ids.length) {
          showToast("Select pending sources to reject.", "info");
          return null;
        }
        const result = await postBridge("/registry/reject", { ids });
        appendDiscoveryLog(`Rejected ${Number(result?.rejected || 0)} source(s).`, "warn");
        showToast("Sources rejected.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function restoreRejectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Restore failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not restore rejected sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("rejected"), ".rejected-source-checkbox");
        if (!ids.length) {
          showToast("Select rejected sources to restore.", "info");
          return null;
        }
        const result = await postBridge("/registry/restore-rejected", { ids });
        appendDiscoveryLog(`Restored ${Number(result?.restored || 0)} rejected source(s) to pending.`, "success");
        showToast("Rejected sources restored to pending.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function demoteActiveSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Demote failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not demote sources.", "error");
        return null;
      },
      execute: async () => {
        const ids = selectedIds(getBucketContainer("active"), ".active-source-checkbox");
        const result = await postBridge("/registry/demote-active", { ids: ids.length ? ids : [] });
        appendDiscoveryLog(`Demoted ${Number(result?.demoted || 0)} zero-job source(s) to pending.`, "success");
        showToast("Sources demoted to pending.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
  }

  async function deleteSelectedSources() {
    return runRegistryMutation({
      state,
      busyKey: "discoveryWrite",
      setBusyFlag,
      showToast,
      onError: err => {
        appendDiscoveryLog(`Delete failed: ${getErrorMessage(err)}`, "error");
        showToast("Could not delete selected sources.", "error");
        return null;
      },
      execute: async () => {
        const sources = selectedSourcesAcrossDiscoveryBuckets();
        const ids = Array.from(new Set(sources.map(item => item.id).filter(Boolean)));
        const urls = Array.from(new Set(sources.map(item => item.url).filter(Boolean)));
        if (!ids.length && !urls.length) {
          showToast("Select sources to delete.", "info");
          return null;
        }
        const confirmed = await requestConfirmationDialog({
          title: "Delete selected sources?",
          description: `Delete ${sources.length} selected source(s) from registry? This cannot be undone.`,
          confirmLabel: "Delete sources"
        });
        if (!confirmed) {
          return null;
        }
        const result = await postBridge("/registry/delete", { ids, urls });
        appendDiscoveryLog(`Deleted ${Number(result?.deleted || 0)} source(s).`, "warn");
        showToast("Selected sources deleted.", "success");
        await loadDiscoveryData();
        await loadOpsHealthData();
        return result || null;
      }
    });
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
        const hiddenZeroJobsCount = pendingRows.filter(row => getSourceJobsFoundCount(row) === 0).length;
        const visiblePendingRows = applySourceFilter(
          filterState.showZeroJobs ? pendingRows : pendingRows.filter(row => getSourceJobsFoundCount(row) !== 0)
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
    setManualSourceFeedback,
    loadDiscoveryData,
    syncSourceTablesAfterTaskCompletion,
    addManualSource,
    approveSelectedSources,
    rejectSelectedSources,
    restoreRejectedSources,
    demoteActiveSources,
    deleteSelectedSources,
    toggleSelectAllSources
  };
}

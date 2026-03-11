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

  function selectedIds(selector) {
    return Array.from(document.querySelectorAll(selector))
      .filter(el => el instanceof HTMLInputElement && el.checked)
      .map(el => String(el.dataset.sourceId || ""))
      .filter(Boolean);
  }

  function selectedSourcesAcrossDiscoveryBuckets() {
    const out = [];
    const seen = new Set();
    const rows = [".pending-source-checkbox", ".active-source-checkbox", ".rejected-source-checkbox"]
      .flatMap(selector => Array.from(document.querySelectorAll(selector)))
      .filter(el => el instanceof HTMLInputElement && el.checked)
      .map(el => ({
        id: String(el.dataset.sourceId || "").trim(),
        url: String(el.dataset.sourceUrl || "").trim()
      }))
      .filter(item => item.id || item.url);
    rows.forEach(item => {
      const key = `${item.id}|${item.url}`;
      if (!key || seen.has(key)) return;
      seen.add(key);
      out.push(item);
    });
    return out;
  }

  function toAdminFilterState() {
    return {
      activeSourceFilter: normalizeSourceFilter(state.activeSourceFilter),
      showZeroJobs: readShowZeroJobs("baluffo_admin_show_zero_jobs_sources")
    };
  }

  async function addManualSource() {
    if (!state.adminPin) {
      showToast("Unlock admin before adding a source.", "error");
      return;
    }
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Another discovery operation is running.", "info");
      return;
    }
    setBusyFlag("manualAdd", true);
    const url = String(refs.adminManualSourceUrlEl?.value || "").trim();
    if (!url) {
      setManualSourceFeedback("invalid URL", "error");
      showToast("Enter a source URL.", "error");
      setBusyFlag("manualAdd", false);
      return;
    }
    try {
      const addResult = await postBridge("/sources/manual", { url });
      const status = String(addResult?.status || "").toLowerCase();

      if (status === "invalid") {
        setManualSourceFeedback("invalid URL", "error");
        appendDiscoveryLog(`Manual source invalid: ${String(addResult?.message || "invalid URL")}`, "error");
        showToast(String(addResult?.message || "Invalid source URL."), "error");
        return;
      }
      if (status === "duplicate") {
        setManualSourceFeedback("duplicate skipped", "warn");
        appendDiscoveryLog("Manual source duplicate skipped.", "warn");
        showToast("Source already exists. Skipped duplicate.", "info");
        return;
      }
      if (status !== "added") {
        setManualSourceFeedback("check failed", "error");
        showToast("Could not add manual source.", "error");
        return;
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
      }

      await loadDiscoveryData();
      await loadOpsHealthData();
    } finally {
      setBusyFlag("manualCheck", false);
      setBusyFlag("manualAdd", false);
    }
  }

  async function approveSelectedSources() {
    if (!state.adminPin) return;
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Another discovery operation is running.", "info");
      return;
    }
    const ids = selectedIds(".pending-source-checkbox");
    if (!ids.length) {
      showToast("Select pending sources to approve.", "info");
      return;
    }
    setBusyFlag("discoveryWrite", true);
    try {
      const result = await postBridge("/registry/approve", { ids });
      appendDiscoveryLog(`Approved ${Number(result?.approved || 0)} source(s).`, "success");
      showToast("Sources approved.", "success");
      await loadDiscoveryData();
      await loadOpsHealthData();
    } catch (err) {
      appendDiscoveryLog(`Approve failed: ${getErrorMessage(err)}`, "error");
      showToast("Could not approve sources.", "error");
    } finally {
      setBusyFlag("discoveryWrite", false);
    }
  }

  async function rejectSelectedSources() {
    if (!state.adminPin) return;
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Another discovery operation is running.", "info");
      return;
    }
    const ids = selectedIds(".pending-source-checkbox");
    if (!ids.length) {
      showToast("Select pending sources to reject.", "info");
      return;
    }
    setBusyFlag("discoveryWrite", true);
    try {
      const result = await postBridge("/registry/reject", { ids });
      appendDiscoveryLog(`Rejected ${Number(result?.rejected || 0)} source(s).`, "warn");
      showToast("Sources rejected.", "success");
      await loadDiscoveryData();
      await loadOpsHealthData();
    } catch (err) {
      appendDiscoveryLog(`Reject failed: ${getErrorMessage(err)}`, "error");
      showToast("Could not reject sources.", "error");
    } finally {
      setBusyFlag("discoveryWrite", false);
    }
  }

  async function restoreRejectedSources() {
    if (!state.adminPin) return;
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Another discovery operation is running.", "info");
      return;
    }
    const ids = selectedIds(".rejected-source-checkbox");
    if (!ids.length) {
      showToast("Select rejected sources to restore.", "info");
      return;
    }
    setBusyFlag("discoveryWrite", true);
    try {
      const result = await postBridge("/registry/restore-rejected", { ids });
      appendDiscoveryLog(`Restored ${Number(result?.restored || 0)} rejected source(s) to pending.`, "success");
      showToast("Rejected sources restored to pending.", "success");
      await loadDiscoveryData();
      await loadOpsHealthData();
    } catch (err) {
      appendDiscoveryLog(`Restore failed: ${getErrorMessage(err)}`, "error");
      showToast("Could not restore rejected sources.", "error");
    } finally {
      setBusyFlag("discoveryWrite", false);
    }
  }

  async function deleteSelectedSources() {
    if (!state.adminPin) return;
    if (state.adminBusyState.discoveryRun || state.adminBusyState.discoveryWatch || state.adminBusyState.discoveryLoad || state.adminBusyState.discoveryWrite || state.adminBusyState.manualAdd || state.adminBusyState.manualCheck || state.adminBusyState.liveDiscoveryRunning) {
      showToast("Another discovery operation is running.", "info");
      return;
    }
    const sources = selectedSourcesAcrossDiscoveryBuckets();
    const ids = Array.from(new Set(sources.map(item => item.id).filter(Boolean)));
    const urls = Array.from(new Set(sources.map(item => item.url).filter(Boolean)));
    if (!ids.length && !urls.length) {
      showToast("Select sources to delete.", "info");
      return;
    }
    if (!window.confirm(`Delete ${sources.length} selected source(s) from registry? This cannot be undone.`)) {
      return;
    }
    setBusyFlag("discoveryWrite", true);
    try {
      const result = await postBridge("/registry/delete", { ids, urls });
      appendDiscoveryLog(`Deleted ${Number(result?.deleted || 0)} source(s).`, "warn");
      showToast("Selected sources deleted.", "success");
      await loadDiscoveryData();
      await loadOpsHealthData();
    } catch (err) {
      appendDiscoveryLog(`Delete failed: ${getErrorMessage(err)}`, "error");
      showToast("Could not delete selected sources.", "error");
    } finally {
      setBusyFlag("discoveryWrite", false);
    }
  }

  async function loadDiscoveryData() {
    if (!state.adminPin) return;
    if (state.adminBusyState.discoveryLoad) return;
    setBusyFlag("discoveryLoad", true);
    appendDiscoveryLog("Loading source discovery report and registries...");
    try {
      const [report, pending, active, rejected] = await Promise.all([
        getBridge("/discovery/report"),
        getBridge("/registry/pending"),
        getBridge("/registry/active"),
        getBridge("/registry/rejected")
      ]);
      const latestFetchReport = state.latestFetcherReportCache || await fetchJobsFetchReportJson();
      state.latestFetcherReportCache = latestFetchReport || state.latestFetcherReportCache;
      const summary = report?.summary || {};
      const foundCount = Number(summary.foundEndpointCount ?? summary.probedCount ?? 0);
      const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
      const queuedCount = Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0);
      const skippedCount = Number(summary.skippedDuplicateCount || 0);
      const failedCount = Number(summary.failedProbeCount || 0);
      const pendingRows = mergeSourceStatusFromReport(Array.isArray(pending?.sources) ? pending.sources : [], latestFetchReport, "pending");
      const activeRows = mergeSourceStatusFromReport(Array.isArray(active?.sources) ? active.sources : [], latestFetchReport, "active");
      const rejectedRows = mergeSourceStatusFromReport(Array.isArray(rejected?.sources) ? rejected.sources : [], latestFetchReport, "rejected");
      const filterState = toAdminFilterState();
      const hiddenZeroJobsCount = pendingRows.filter(row => getSourceJobsFoundCount(row) === 0).length;
      const visiblePendingRows = applySourceFilter(
        filterState.showZeroJobs ? pendingRows : pendingRows.filter(row => getSourceJobsFoundCount(row) !== 0)
      );
      const visibleActiveRows = applySourceFilter(activeRows);
      const visibleRejectedRows = applySourceFilter(rejectedRows);

      if (refs.adminDiscoverySummaryEl) {
        refs.adminDiscoverySummaryEl.textContent =
          `Found ${foundCount} | Probed ${probedCount} | Queued (new) ${queuedCount} | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending ${Number(pending?.summary?.pendingCount || 0)} | Active ${Number(active?.summary?.activeCount || 0)} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
      }
      renderSourcesTable(refs.adminPendingSourcesEl, visiblePendingRows, "pending");
      renderSourcesTable(refs.adminActiveSourcesEl, visibleActiveRows, "active");
      renderSourcesTable(refs.adminRejectedSourcesEl, visibleRejectedRows, "rejected");
      appendDiscoveryLog(
        `Discovery summary: found ${foundCount}, probed ${probedCount}, queued (new) ${queuedCount}, failed ${failedCount}, skipped duplicates ${skippedCount}.`,
        "info"
      );
      const topFailures = Array.isArray(report?.topFailures) ? report.topFailures : [];
      if (topFailures.length) {
        const line = topFailures.slice(0, 3).map(item => `${String(item?.key || "unknown")} (${Number(item?.count || 0)})`).join(", ");
        appendDiscoveryLog(`Top failures: ${line}`, "warn");
      }
      appendDiscoveryLog("Source discovery data loaded.", "success");
      adminDispatch.dispatch({ type: adminActions.DISCOVERY_REFRESHED, payload: { at: new Date().toISOString() } });
    } catch (err) {
      appendDiscoveryLog(`Could not load source discovery data: ${getErrorMessage(err)}`, "error");
      if (refs.adminDiscoverySummaryEl) {
        refs.adminDiscoverySummaryEl.textContent = "Source discovery bridge unavailable. Start `Run admin bridge` task.";
      }
    } finally {
      setBusyFlag("discoveryLoad", false);
    }
  }

  return {
    setManualSourceFeedback,
    loadDiscoveryData,
    addManualSource,
    approveSelectedSources,
    rejectSelectedSources,
    restoreRejectedSources,
    deleteSelectedSources
  };
}

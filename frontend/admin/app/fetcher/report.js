import {
  appendLiveTaskActivity,
  buildTaskWorkItemActivitySignature
} from "../live-task.js";
import { formatScrapyStaticSourcesTailBadge } from "../../../shared/task-progress.js";
import {
  deriveFetcherFailureSummary,
  deriveFetcherProgressModel,
  deriveFetcherTaskProgress
} from "../../domain/progress.js";
import {
  fetchJobsFetchReportJsonWithRetry,
  formatDurationCompact,
  formatStageTopSummary,
  selectSlowSources
} from "../fetcher-summary.js";

export function createAdminFetcherReportController({
  state,
  refs,
  fetchJobsFetchReportJson,
  writeJobsAutoRefreshSignal,
  showToast,
  setBusyFlag,
  loadOpsHealthData,
  jobsAutoRefreshSignalKey,
  setFetcherProgress,
  appendFetcherLog,
  appendFetcherLogEvent
}) {
  function hasLiveFetcherSummaryState() {
    const liveState = state.fetcherLiveProgressState;
    if (!liveState || typeof liveState !== "object") return false;
    if (String(liveState.summarySignature || "").trim()) return true;
    if (String(liveState.workItemSignature || "").trim()) return true;
    if (liveState.recentEventSignatures instanceof Set && liveState.recentEventSignatures.size > 0) {
      return true;
    }
    return false;
  }

  function hasVisibleFetcherProgressLabel() {
    return Boolean(String(refs.adminFetcherProgressLabelEl?.textContent || "").trim());
  }

  function getFetcherTaskProgress(report, { running = false } = {}) {
    return deriveFetcherTaskProgress(report, { running }) || {
      active: Boolean(running),
      phaseKey: "",
      phaseLabel: "",
      mode: "indeterminate",
      ratio: 0,
      counts: {}
    };
  }

  function updateFetcherProgressFromReport(report, { running = false } = {}) {
    setFetcherProgress(deriveFetcherProgressModel(report, { running }));
  }

  async function loadLatestFetcherReport(options = {}) {
    const silent = Boolean(options.silent);
    const hydrateActiveProgress = Boolean(options.hydrateActiveProgress);
    if (state.adminBusyState.fetcherReportLoad) {
      if (!silent) showToast("Fetch report loading already in progress.", "info");
      return null;
    }
    setBusyFlag("fetcherReportLoad", true);
    try {
      if (!silent) appendFetcherLog("Loading latest jobs fetch report...");
      const report = await fetchJobsFetchReportJsonWithRetry(fetchJobsFetchReportJson);
      if (!report) {
        appendFetcherLog("Fetch report is not available yet. It may still be generating.", "warn");
        updateFetcherProgressFromReport(null, { running: Boolean(state.adminBusyState.fetcherWatch || state.adminBusyState.liveFetchRunning) });
        if (!silent) showToast("Fetch report not available yet. Retry in a few seconds.", "info");
        return null;
      }
      const liveWatchActive = Boolean(state.adminBusyState.fetcherWatch || state.adminBusyState.liveFetchRunning);
      const reportFinished = Boolean(String(report?.finishedAt || "").trim());
      state.latestFetcherReportCache = report;
      if (!liveWatchActive || reportFinished) {
        updateFetcherProgressFromReport(report, { running: false });
      } else if (hydrateActiveProgress || (!hasLiveFetcherSummaryState() && !hasVisibleFetcherProgressLabel())) {
        updateFetcherProgressFromReport(report, { running: true });
      }

      if (liveWatchActive && !reportFinished) {
        return report;
      }

      const summary = report?.summary || {};
      const progress = getFetcherTaskProgress(report, { running: false });
      const counts = progress.counts && typeof progress.counts === "object" ? progress.counts : {};
      const resolvedSources = Math.max(0, Number(counts.resolvedSources ?? (Number(summary.successfulSources || 0) + Number(summary.failedSources || 0) + Number(summary.excludedSources || 0))));
      const totalSources = progress.mode === "determinate" ? Math.max(0, Number(counts.sourceCount || 0)) : 0;
      const outputCount = Math.max(0, Number(counts.outputCount ?? summary.outputCount ?? 0));
      const failedSourceCount = Math.max(0, Number(counts.failedSources ?? summary.failedSources ?? 0));
      const excludedSourceCount = Math.max(0, Number(counts.excludedSources ?? summary.excludedSources ?? 0));
      appendFetcherLog(
        `Fetcher summary: ${totalSources > 0 ? `${resolvedSources}/${totalSources} sources resolved` : `${resolvedSources} sources resolved`}, output ${outputCount.toLocaleString()}, failed ${failedSourceCount}, excluded ${excludedSourceCount}.`,
        failedSourceCount > 0 ? "warn" : "success"
      );

      const sources = Array.isArray(report?.sources) ? report.sources : [];
      if (!sources.length) {
        appendFetcherLog("No source entries found in report.", "warn");
        return report;
      }

      const failedSources = sources
        .filter(source => String(source?.status || "").toLowerCase() === "error")
        .slice(0, 3)
        .map(source => `${String(source?.name || "unknown")}${source?.error ? ` [${String(source.error)}]` : ""}`);
      if (failedSources.length) {
        appendFetcherLog(`Failures: ${failedSources.join(" | ")}`, "warn");
      }
      const failureSummary = deriveFetcherFailureSummary(report);
      if (Array.isArray(failureSummary?.buckets) && failureSummary.buckets.length) {
        const bucketLine = failureSummary.buckets
          .map(bucket => `${String(bucket.key || "").replaceAll("_", " ")} ${Number(bucket.count || 0)}`)
          .join(" | ");
        appendFetcherLog(`Failure buckets: ${bucketLine}`, "warn");
      }
      const slowSources = selectSlowSources(report)
        .slice(0, 2)
        .map(source => `${String(source?.name || "unknown")} ${formatDurationCompact(source?.durationMs)}`);
      if (slowSources.length) {
        appendFetcherLog(`Slowest sources: ${slowSources.join(" | ")}`, "muted");
      }
      const slowStages = formatStageTopSummary(report);
      if (slowStages) {
        appendFetcherLog(`Slowest stages: ${slowStages}`, "muted");
      }

      loadOpsHealthData().catch(() => {});
      return report;
    } finally {
      setBusyFlag("fetcherReportLoad", false);
    }
  }

  async function copyLatestFailureSummary() {
    const report = state.latestFetcherReportCache || await fetchJobsFetchReportJson();
    if (!report) {
      showToast("No fetch report available to copy.", "error");
      return;
    }
    state.latestFetcherReportCache = report;
    const failureSummary = deriveFetcherFailureSummary(report);
    const failures = (Array.isArray(report?.sources) ? report.sources : []).filter(
      row => String(row?.status || "").toLowerCase() === "error"
    );
    if (!failures.length && !(Array.isArray(failureSummary?.buckets) && failureSummary.buckets.length)) {
      showToast("No failed sources in latest report.", "info");
      return;
    }
    const summaryLines = [];
    summaryLines.push(`Top-level failed sources: ${Number(failureSummary?.topLevelFailedSources || failures.length)}`);
    summaryLines.push(`Grouped detail failures: ${Number(failureSummary?.detailFailureCount || 0)}`);
    if (Array.isArray(failureSummary?.buckets) && failureSummary.buckets.length) {
      summaryLines.push("");
      summaryLines.push("Failure buckets:");
      failureSummary.buckets.forEach(bucket => {
        const examples = Array.isArray(bucket?.examples) && bucket.examples.length ? ` (${bucket.examples.join(" | ")})` : "";
        summaryLines.push(`- ${String(bucket?.key || "uncategorized")}: ${Number(bucket?.count || 0)}${examples}`);
      });
    }
    if (failures.length) {
      summaryLines.push("");
      summaryLines.push("Top-level failures:");
      failures.forEach(row => {
        summaryLines.push(`${row?.name || "unknown"}: ${row?.error || "error"}`);
      });
    }
    const summary = summaryLines.join("\n");
    if (navigator?.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(summary);
        showToast("Failure summary copied.", "success");
        return;
      } catch {
        // Fallback to log append below.
      }
    }
    appendFetcherLog(`Failure summary:\n${summary}`, "warn");
    showToast("Could not access clipboard. Summary logged.", "warn");
  }

  function emitJobsAutoRefreshSignal(report) {
    const signal = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      createdAt: new Date().toISOString(),
      finishedAt: String(report?.finishedAt || ""),
      source: "admin_fetcher"
    };
    try {
      writeJobsAutoRefreshSignal(jobsAutoRefreshSignalKey, signal);
      appendFetcherLog("Signaled jobs page to auto-refresh from unified feed.", "success");
    } catch {
      appendFetcherLog("Could not write auto-refresh signal to localStorage.", "warn");
    }
  }

  function appendFetcherProgressFromReport(report, nowMs) {
    const liveState = state.fetcherLiveProgressState;
    if (!liveState) return;
    updateFetcherProgressFromReport(report, { running: true });
    const summary = report?.summary || {};
    const progress = getFetcherTaskProgress(report, { running: true });
    const counts = progress.counts && typeof progress.counts === "object" ? progress.counts : {};
    const outputCount = Math.max(0, Number(counts.outputCount ?? summary.outputCount ?? 0));
    const selectedSourceCount = progress.mode === "determinate" ? Math.max(0, Number(counts.sourceCount || 0)) : 0;
    const runningSources = Math.max(0, Number(counts.runningTasks ?? counts.running ?? summary.running ?? 0));
    const queuedSources = Math.max(0, Number(counts.queuedTasks ?? counts.queued ?? summary.queued ?? 0));
    const failedSources = Math.max(0, Number(counts.failedSources ?? summary.failedSources ?? 0));
    const excludedSources = Math.max(0, Number(counts.excludedSources ?? summary.excludedSources ?? 0));
    const resolvedSources = Math.max(
      0,
      Number(
        counts.resolvedSources
        ?? (Number(summary.successfulSources || 0) + Number(summary.failedSources || 0) + Number(summary.excludedSources || 0))
      )
    );
    const fallbackTailBadge = formatScrapyStaticSourcesTailBadge(report?.workItems);
    const fallbackTailSuffix = fallbackTailBadge ? ` | ${fallbackTailBadge}` : "";
    const summarySignature = [
      outputCount,
      selectedSourceCount,
      resolvedSources,
      runningSources,
      queuedSources,
      failedSources,
      excludedSources,
      fallbackTailBadge
    ].join("|");
    appendLiveTaskActivity({
      payload: report,
      liveState,
      nowMs,
      appendEvent: event => appendFetcherLogEvent(event, "muted"),
      scope: "fetch",
      summarySignature,
      workItemSignature: buildTaskWorkItemActivitySignature(report),
      onSummaryChange: () => {
        appendFetcherLog(
          `Fetcher: ${selectedSourceCount > 0 ? `${resolvedSources}/${selectedSourceCount} sources resolved` : `${resolvedSources} sources resolved`}, running ${runningSources}, queued ${queuedSources}, output ${outputCount.toLocaleString()}, failed ${failedSources}, excluded ${excludedSources}${fallbackTailSuffix}.`,
          failedSources > 0 ? "warn" : "info"
        );
      },
      onHeartbeat: () => {
        appendFetcherLog(
          `Fetcher active: ${selectedSourceCount > 0 ? `${resolvedSources}/${selectedSourceCount} sources resolved` : `${resolvedSources} sources resolved`}, running ${runningSources}, queued ${queuedSources}, output ${outputCount.toLocaleString()}${fallbackTailSuffix}.`,
          "muted"
        );
      }
    });
  }

  return {
    getFetcherTaskProgress,
    updateFetcherProgressFromReport,
    loadLatestFetcherReport,
    copyLatestFailureSummary,
    emitJobsAutoRefreshSignal,
    appendFetcherProgressFromReport
  };
}

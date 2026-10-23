/**
 * Jobs feed — thin coordinator.
 *
 * Owns the public entrypoints and the boot sequence; every helper family
 * lives in a sibling leaf:
 * - feed-constants.js        messages, storage keys, bootstrap constants
 * - feed-report-probe.js     fetcher-report and progress probing
 * - feed-bootstrap-marker.js marker storage and cold-start decisions
 * - feed-filter-match.js     filter comparison, fast-path gate, URL overrides
 * - feed-first-run-flow.js   the first-run reattach/start/poll/retry sequence
 * - feed-refresh.js          refreshJobsFeed
 * - feed-startup-preview.js  loadStartupPreviewJobsFeed
 * - feed-auto-refresh.js     admin auto-refresh signal entrypoints
 *
 * No leaf module imports from here.
 */

import {
  JOBS_FULL_FEED_SYNC_DELAY_MS,
  LOCAL_FEED_MISSING_MESSAGE
} from "./feed-constants.js";
import {
  bootstrapAutoStartMarker,
  bootstrapColdStartAction,
  bootstrapRetryMessage,
  markLaunchColdStartHandled,
  launchColdStartAlreadyHandled
} from "./feed-bootstrap-marker.js";
import {
  isActiveBootstrapReport,
  isSuccessfulJobsFetchReport,
  reportFinishedTimestamp
} from "./feed-report-probe.js";
import { createFirstRunBootstrapFlow } from "./feed-first-run-flow.js";

export { jobsFirstRunBootstrapNumberOverride, canUseStartupPreviewFastPath } from "./feed-filter-match.js";
export { refreshJobsFeed } from "./feed-refresh.js";
export { loadStartupPreviewJobsFeed } from "./feed-startup-preview.js";
export {
  handleJobsAutoRefreshSignalValue,
  applyPendingJobsAutoRefreshSignal,
  triggerJobsAutoRefreshFromSignal
} from "./feed-auto-refresh.js";

/**
 * Boot the jobs feed: cache fast-path, first-run bootstrap, startup preview,
 * then a full refresh.
 *
 * @param {Object} deps
 */
export async function initJobsFeed(deps) {
  const {
    hasJobsList,
    emitMetric,
    markJobsStep = () => {},
    isDesktopRuntimeMode,
    isContainerRuntimeMode = () => false,
    readCachedJobs,
    normalizeRows,
    recalculateItemsPerPage,
    updateFilterOptions,
    applyStateToFilters,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    isJobsCacheStale,
    cacheTtlMs,
    setSourceStatus,
    refreshJobsNow,
    updateLastUpdatedText,
    fetchJobsReport,
    desktopJobsColdStart = false,
    windowObject,
    setHasInitializedJobsFeed,
    scheduleNonCriticalStartupWork,
    applyPendingAutoRefreshSignal,
    loadStartupPreviewJobs,
    showError,
    getAllJobs
  } = deps;

  if (!hasJobsList) return;
  markJobsStep("jobs_boot_start");
  emitMetric("jobs_init_start");

  const firstRunFlow = createFirstRunBootstrapFlow(deps);
  const {
    showBootstrapNoticeOnce,
    renderFirstRunBootstrapState,
    startBootstrapAndLoad,
    retryBootstrap
  } = firstRunFlow;

try {
    // No auth handshake here: boot starts it behind the first rows, so the
    // snapshot read below is the first thing this path waits on.
    const desktopMode = isDesktopRuntimeMode();
    // The report probe is a gate, not feed data: it decides whether a first-run
    // bootstrap is needed. Start it here and read it where its answer matters, so
    // the first paint never waits on it -- a report that lands after rows are on
    // screen is applied behind them (``applyFirstRunGateBehindRenderedRows``).
    const reportProbe = desktopMode && typeof fetchJobsReport === "function"
      ? fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
      : Promise.resolve(null);

    function evaluateFirstRunGate(localReport) {
      const launchColdStartPending = desktopMode
        && Boolean(desktopJobsColdStart)
        && !launchColdStartAlreadyHandled(windowObject);
      const localReportSuccessful = isSuccessfulJobsFetchReport(localReport);
      const bootstrapMarker = desktopMode ? bootstrapAutoStartMarker(windowObject) : { status: "none" };
      const bootstrapMarkerRunning = bootstrapMarker.status === "running"
        || bootstrapMarker.status === "legacy";
      const firstRunRequired = Boolean(
        desktopMode
        && !localReportSuccessful
        && (launchColdStartPending || bootstrapMarkerRunning || isActiveBootstrapReport(localReport))
      );
      return {
        launchColdStartPending,
        localReportSuccessful,
        firstRunRequired,
        firstRunAction: firstRunRequired
          ? bootstrapColdStartAction(localReport, windowObject, { forceStart: true })
          : false
      };
    }

    function emitGateEvaluated(gate, extra = {}) {
      emitMetric("jobs_first_run_gate_evaluated", {
        desktopMode,
        runtimeColdStart: Boolean(desktopJobsColdStart),
        launchColdStartPending: gate.launchColdStartPending,
        reportSuccessful: gate.localReportSuccessful,
        action: gate.firstRunAction || "skip",
        ...extra
      });
    }

    // The report landed after the snapshot painted: apply its verdict behind the
    // rows already on screen. Swapping in the empty bootstrap state or its modal
    // notice here is exactly the flash this path exists to avoid, so the bootstrap
    // runs quietly and the status line explains it.
    async function applyFirstRunGateBehindRenderedRows(localReport) {
      const gate = evaluateFirstRunGate(localReport);
      const rowsRendered = getAllJobs().length;
      emitGateEvaluated(gate, { late: true, rowsRendered });
      // The snapshot painted before this report existed, so its "Last updated"
      // stamp had nothing to show: fill it in now instead of waiting for the
      // delayed full-feed refresh.
      const finishedAt = reportFinishedTimestamp(localReport);
      if (finishedAt) updateLastUpdatedText(finishedAt);
      if (!gate.firstRunRequired) return;
      emitMetric("jobs_first_run_gate_applied_late", {
        action: gate.firstRunAction || "skip",
        rowsRendered
      });
      if (gate.launchColdStartPending) markLaunchColdStartHandled(windowObject);
      if (gate.firstRunAction !== "start" && gate.firstRunAction !== "reattach") {
        // A terminal failure cannot be repaired by clearing the screen, and the
        // snapshot rows are still readable: report it without replacing them.
        setSourceStatus(bootstrapRetryMessage(localReport));
        return;
      }
      try {
        await startBootstrapAndLoad();
      } catch (err) {
        showError(String(err?.message || "Unable to refresh first-run jobs."), retryBootstrap);
      }
    }

    // ponytail: container boots on the bounded startup snapshot; normalizing a
    // full IndexedDB feed here blocks boot for seconds. Full feed via explicit Reload.
    const cached = (desktopMode || isContainerRuntimeMode()) ? null : await readCachedJobs();
    emitMetric("jobs_cache_checked", {
      desktopMode,
      hasCache: Boolean(cached?.jobs && cached.jobs.length > 0)
    });
    emitMetric(cached?.jobs && cached.jobs.length > 0 ? "jobs_cache_hit" : "jobs_cache_miss");

    if (cached?.jobs && cached.jobs.length > 0) {
      normalizeRows(cached.jobs);
      recalculateItemsPerPage();
      updateFilterOptions();
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: false });
      markStartupRendered("cache", getAllJobs().length);
      markJobsFirstInteractive("cache");

      if (isJobsCacheStale(cached.savedAt, cacheTtlMs)) {
        if (isContainerRuntimeMode()) {
          setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from local cache.`);
        } else {
          setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from cache. Updating stale cache...`);
          refreshJobsNow({ manual: false }).catch(() => {});
        }
      } else {
        setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from local cache.`);
      }
      updateLastUpdatedText(cached.savedAt);
      setHasInitializedJobsFeed(true);
      scheduleNonCriticalStartupWork();
      await applyPendingAutoRefreshSignal(isContainerRuntimeMode() ? { acknowledgeOnly: true } : {});
      return;
    }

    // Rows first, gate second: the snapshot decides what the page paints, and the
    // report only decides what happens behind it.
    const previewLoaded = await loadStartupPreviewJobs();
    if (previewLoaded) {
      setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs from startup snapshot. Syncing full feed...`);
      setHasInitializedJobsFeed(true);
      scheduleNonCriticalStartupWork();
      await applyPendingAutoRefreshSignal(isContainerRuntimeMode() ? { acknowledgeOnly: true } : {});
      reportProbe
        .then(localReport => applyFirstRunGateBehindRenderedRows(localReport))
        .catch(() => {});
      // ponytail: auto-hydrate the complete feed right after interactive so
      // the full list never requires pressing Reload; snapshot keeps boot fast.
      windowObject.setTimeout(() => {
        refreshJobsNow({ manual: false }).catch(() => {});
      }, JOBS_FULL_FEED_SYNC_DELAY_MS);
      return;
    }

    // Nothing on screen yet, so the gate's answer is what decides what to show.
    const localReport = await reportProbe;
    const gate = evaluateFirstRunGate(localReport);
    emitGateEvaluated(gate);

    if (gate.firstRunRequired) {
      renderFirstRunBootstrapState();
      setHasInitializedJobsFeed(true);
      markStartupRendered("first_run_bootstrap", 0);
      markJobsFirstInteractive("first_run_bootstrap");
      scheduleNonCriticalStartupWork();
      if (gate.firstRunAction === "start" || gate.firstRunAction === "reattach") {
        try {
          if (gate.launchColdStartPending) markLaunchColdStartHandled(windowObject);
          showBootstrapNoticeOnce(gate.firstRunAction);
          const ok = await startBootstrapAndLoad();
          if (!ok) {
            showError("Unable to load job listings right now.", retryBootstrap);
          }
          return;
        } catch (err) {
          showError(String(err?.message || "Unable to refresh first-run jobs."), retryBootstrap);
          return;
        }
      }
      showError(bootstrapRetryMessage(localReport), retryBootstrap);
      return;
    }

    const ok = await refreshJobsNow({ manual: false, firstLoad: true });
    setHasInitializedJobsFeed(true);
    scheduleNonCriticalStartupWork();
    await applyPendingAutoRefreshSignal(isContainerRuntimeMode() ? { acknowledgeOnly: true } : {});
    if (!ok) {
      if (isDesktopRuntimeMode() && isSuccessfulJobsFetchReport(localReport)) {
        try {
          const recovered = await startBootstrapAndLoad();
          if (!recovered) showError(LOCAL_FEED_MISSING_MESSAGE, retryBootstrap);
          return;
        } catch (err) {
          showError(String(err?.message || LOCAL_FEED_MISSING_MESSAGE), retryBootstrap);
          return;
        }
      }
      showError("Unable to load job listings right now.");
    }
  } catch (err) {
    setHasInitializedJobsFeed(true);
    showError("Unable to load job listings right now.");
    if (typeof deps.logError === "function") {
      deps.logError("Jobs feed init failed", err);
    } else {
      console.error("[jobs] init failed:", err);
    }
  }
}

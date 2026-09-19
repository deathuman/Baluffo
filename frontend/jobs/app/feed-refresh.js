/**
 * Jobs feed — manual/first-load refresh entrypoint.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 *
 * @module feed-refresh
 */

import { EMPTY_TITLE_FEED_MESSAGE } from "./feed-constants.js";
import { coverageScope, reportFinishedTimestamp } from "./feed-report-probe.js";

async function refreshJobsFeed({ manual, firstLoad = false }, deps) {
  const {
    getRefreshInFlight,
    setRefreshInFlight,
    dispatchRefreshRequested,
    setProgress,
    setSourceStatus,
    firstLoadRequestTimeoutMs,
    fetchUnifiedJobs,
    dispatchRefreshFailed,
    showToast,
    logError,
    getAllJobs,
    setAllJobs,
    normalizeRows,
    isDesktopRuntimeMode,
    writeCachedJobs,
    fetchJobsReport,
    updateLastUpdatedText,
    recalculateItemsPerPage,
    updateFilterOptions,
    applyStateToFilters,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    markJobsStep = () => {},
    measureJobsStep = () => {},
    emitMetric,
    dispatchRefreshCompleted,
    renderDataSources
  } = deps;

  if (getRefreshInFlight()) return false;
  setRefreshInFlight(true);
  dispatchRefreshRequested();

  // Keep the page interactive while noncritical background refreshes run after
  // startup-preview/cache boot. Manual and first-load refreshes show progress.
  if (manual || firstLoad) setProgress(true);
  if (manual) setSourceStatus("Reloading jobs...");

  try {
    const refreshStartedAt = Date.now();
    if (firstLoad) {
      markJobsStep("jobs_feed_fetch_start");
      emitMetric("jobs_first_load_refresh_start");
    }
    const result = await fetchUnifiedJobs({
      timeoutMs: firstLoad ? firstLoadRequestTimeoutMs : 20000,
      allowSheetsFallback: !firstLoad
    });
    if (firstLoad) {
      markJobsStep("jobs_feed_fetch_done", {
        ok: Boolean(result.jobs && result.jobs.length > 0)
      });
      measureJobsStep("jobs_feed_fetch", "jobs_feed_fetch_start", "jobs_feed_fetch_done", {
        ok: Boolean(result.jobs && result.jobs.length > 0)
      });
    }
    if (!result.jobs || result.jobs.length === 0) {
      if (firstLoad) {
        setSourceStatus(result.error || "Could not fetch listings from local unified feeds.");
      }
      if (manual) showToast(result.error || "Could not reload jobs.", "error");
      dispatchRefreshFailed(result.error || "Could not reload jobs.");
      return false;
    }

    const previousLength = getAllJobs().length;
    const normalizedJobs = normalizeRows(result.jobs);
    if (!normalizedJobs.length) {
      setAllJobs([]);
      if (firstLoad) setSourceStatus(EMPTY_TITLE_FEED_MESSAGE);
      if (manual) showToast(EMPTY_TITLE_FEED_MESSAGE, "error");
      dispatchRefreshFailed(EMPTY_TITLE_FEED_MESSAGE);
      return false;
    }
    setAllJobs(normalizedJobs);
    const latestReport = typeof fetchJobsReport === "function"
      ? await fetchJobsReport({ timeoutMs: 1500 }).catch(() => null)
      : null;
    const reportTimestamp = reportFinishedTimestamp(latestReport);
    if (!isDesktopRuntimeMode()) {
      await writeCachedJobs(getAllJobs());
    }
    updateLastUpdatedText(reportTimestamp);
    recalculateItemsPerPage();
    updateFilterOptions();
    applyStateToFilters();
    if (firstLoad) {
      markJobsStep("jobs_render_start", { rowCount: getAllJobs().length });
    }
    applyFiltersAndRender({ resetPage: false });
    if (firstLoad) {
      markJobsStep("jobs_render_end", { rowCount: getAllJobs().length });
      measureJobsStep("jobs_render", "jobs_render_start", "jobs_render_end", {
        rowCount: getAllJobs().length
      });
      markStartupRendered("first_load_refresh", getAllJobs().length);
      markJobsFirstInteractive("first_load_refresh");
      emitMetric("jobs_first_load_refresh_done", {
        ok: true,
        durationMs: Math.max(0, Date.now() - refreshStartedAt),
        rowCount: getAllJobs().length
      });
    }

    if (manual) {
      showToast("Jobs reloaded.", "success");
    } else if (previousLength > 0) {
      showToast("Job cache auto-updated.", "info");
    }

    const sourceLabel = result.sourceName ? ` from ${result.sourceName}` : "";
    const limitedScope = coverageScope(latestReport) === "bootstrap_sheets";
    const coverageNote = limitedScope
      ? " Sheet-limited first-run refresh; run Update jobs for full coverage."
      : "";
    setSourceStatus(`Loaded ${getAllJobs().length.toLocaleString()} jobs${sourceLabel}.${coverageNote}`);
    renderDataSources().catch(() => {});
    dispatchRefreshCompleted();
    return true;
  } catch (err) {
    logError("Refresh failed", err);
    if (firstLoad) {
      markJobsStep("jobs_feed_fetch_done", {
        ok: false,
        error: String(err?.message || "unknown error")
      });
      measureJobsStep("jobs_feed_fetch", "jobs_feed_fetch_start", "jobs_feed_fetch_done", {
        ok: false
      });
      emitMetric("jobs_first_load_refresh_done", {
        ok: false,
        error: String(err?.message || "unknown error")
      });
    }
    if (manual) showToast("Could not reload jobs.", "error");
    dispatchRefreshFailed(err?.message || "Could not reload jobs.");
    return false;
  } finally {
    setRefreshInFlight(false);
    setProgress(false);
  }
}

export {
  refreshJobsFeed
};

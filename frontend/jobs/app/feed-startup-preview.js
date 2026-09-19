/**
 * Jobs feed — startup-preview entrypoint.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 *
 * @module feed-startup-preview
 */

import { canUseStartupPreviewFastPath } from "./feed-filter-match.js";

async function loadStartupPreviewJobsFeed(deps) {
  const {
    emitMetric,
    fetchJsonFromCandidates,
    startupPreviewJsonUrls,
    parseUnifiedJobsPayload,
    normalizeRows,
    updateLastUpdatedText,
    startupLastUpdatedTimestamp = null,
    recalculateItemsPerPage,
    pageState,
    defaultFilters,
    buildStartupPreviewFastPathPlan,
    applyFilterOptionsSnapshot,
    updateFilterOptions,
    applyStateToFilters,
    renderStartupPreviewFastPath,
    scheduleStartupPreviewMaterialization,
    applyFiltersAndRender,
    markStartupRendered,
    markJobsFirstInteractive,
    markJobsStep = () => {},
    measureJobsStep = () => {},
    setSkipInitialGuestAuthRerender,
    getAllJobs
  } = deps;

  try {
    const startedAt = Date.now();
    markJobsStep("jobs_startup_preview_fetch_start");
    emitMetric("jobs_startup_preview_fetch_start");
    const payload = await fetchJsonFromCandidates(startupPreviewJsonUrls, { timeoutMs: 3000 });
    markJobsStep("jobs_startup_preview_fetch_done", {
      hasPayload: Boolean(payload)
    });
    measureJobsStep(
      "jobs_startup_preview_fetch",
      "jobs_startup_preview_fetch_start",
      "jobs_startup_preview_fetch_done",
      { hasPayload: Boolean(payload) }
    );
    emitMetric("jobs_startup_preview_fetch_complete", {
      durationMs: Math.max(0, Date.now() - startedAt),
      hasPayload: Boolean(payload)
    });
    markJobsStep("jobs_startup_preview_parse_start");
    emitMetric("jobs_startup_preview_parse_start");
    const rows = parseUnifiedJobsPayload(payload);
    markJobsStep("jobs_startup_preview_parse_done", {
      rowCount: Array.isArray(rows) ? rows.length : 0
    });
    measureJobsStep(
      "jobs_startup_preview_parse",
      "jobs_startup_preview_parse_start",
      "jobs_startup_preview_parse_done",
      { rowCount: Array.isArray(rows) ? rows.length : 0 }
    );
    emitMetric("jobs_startup_preview_parse_complete", {
      rowCount: Array.isArray(rows) ? rows.length : 0
    });
    if (!Array.isArray(rows) || rows.length === 0) return false;
    emitMetric("jobs_startup_preview_normalize_start");
    const normalizedJobs = normalizeRows(rows);
    emitMetric("jobs_startup_preview_normalize_complete", {
      rowCount: getAllJobs().length
    });
    updateLastUpdatedText(startupLastUpdatedTimestamp);
    recalculateItemsPerPage();
    markJobsStep("jobs_startup_preview_render_start", {
      rowCount: getAllJobs().length
    });
    emitMetric("jobs_startup_preview_render_start", {
      rowCount: getAllJobs().length
    });
    const useFastPath = canUseStartupPreviewFastPath(pageState, defaultFilters)
      && typeof buildStartupPreviewFastPathPlan === "function"
      && typeof renderStartupPreviewFastPath === "function";
    if (useFastPath) {
      const startupPreviewPlan = buildStartupPreviewFastPathPlan(normalizedJobs);
      if (startupPreviewPlan?.filterOptions && typeof applyFilterOptionsSnapshot === "function") {
        applyFilterOptionsSnapshot(startupPreviewPlan.filterOptions);
      } else {
        updateFilterOptions();
      }
      applyStateToFilters();
      renderStartupPreviewFastPath(startupPreviewPlan);
      if (typeof scheduleStartupPreviewMaterialization === "function") {
        scheduleStartupPreviewMaterialization(startupPreviewPlan?.materializeFilteredJobs);
      }
    } else {
      updateFilterOptions();
      applyStateToFilters();
      applyFiltersAndRender({ resetPage: false });
    }
    emitMetric("jobs_startup_preview_render_returned", {
      rowCount: getAllJobs().length
    });
    markJobsStep("jobs_startup_preview_render_done", {
      rowCount: getAllJobs().length
    });
    measureJobsStep(
      "jobs_startup_preview_render",
      "jobs_startup_preview_render_start",
      "jobs_startup_preview_render_done",
      { rowCount: getAllJobs().length }
    );
    markStartupRendered("startup_preview", getAllJobs().length);
    markJobsFirstInteractive("startup_preview");
    markJobsStep("jobs_preview_ready", { rowCount: getAllJobs().length });
    emitMetric("jobs_startup_preview_render_complete", {
      rowCount: getAllJobs().length
    });
    if (typeof setSkipInitialGuestAuthRerender === "function") {
      setSkipInitialGuestAuthRerender(true);
    }
    emitMetric("jobs_startup_preview_loaded", {
      rowCount: getAllJobs().length,
      durationMs: Math.max(0, Date.now() - startedAt)
    });
    return true;
  } catch (error) {
    emitMetric("jobs_startup_preview_miss", {
      message: String(error?.message || error || "unknown startup preview error")
    });
    return false;
  }
}

export {
  loadStartupPreviewJobsFeed
};

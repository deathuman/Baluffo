import { showToast } from "../../../shared/ui/index.js";
import { normalizeToken } from "../../../shared/text-utils.js";
import {
  applyPendingJobsAutoRefreshSignal,
  handleJobsAutoRefreshSignalValue,
  triggerJobsAutoRefreshFromSignal
} from "../feed.js?v=9";
import {
  getAutoRefreshStatusText,
  parseAutoRefreshSignal as parseAutoRefreshSignalFromStartup,
  parseJobsPageUrlState
} from "../startup.js";
import { matchesCountrySelection as matchesCountrySelectionForJobs } from "../countries.js";
import { displayJobs as displayJobsFromView, goToPage as goToPageFromView, updateResultsSummary as updateResultsSummaryFromView } from "./list-view.js?v=5";
import { filterJobs, sortJobs as sortJobsFromQuery } from "./query.js?v=4";

export function createJobsPageFlow(deps) {
  function ensureJobsPipelineStatusWatch() {
    return deps.pipelineController.ensureJobsPipelineStatusWatch();
  }

  async function triggerJobsPipelineRun() {
    return deps.pipelineController.triggerJobsPipelineRun();
  }

  function markAutoRefreshSignalHandled(signalId) {
    if (!signalId) return;
    deps.runtimeState.lastHandledAutoRefreshSignalId = signalId;
    deps.writeAutoRefreshAppliedId(deps.jobsAutoRefreshAppliedKey, signalId);
  }

  function handleAutoRefreshSignalValue(rawValue) {
    return handleJobsAutoRefreshSignalValue(rawValue, {
      parseAutoRefreshSignal: parseAutoRefreshSignalFromStartup,
      getLastHandledAutoRefreshSignalId: () => deps.runtimeState.lastHandledAutoRefreshSignalId,
      getHasInitializedJobsFeed: () => deps.runtimeState.hasInitializedJobsFeed,
      setPendingAutoRefreshSignal: value => {
        deps.runtimeState.pendingAutoRefreshSignal = value;
      },
      triggerAutoRefreshFromSignal,
      logError: deps.logJobsError
    });
  }

  async function applyPendingAutoRefreshSignal() {
    return applyPendingJobsAutoRefreshSignal({
      getPendingAutoRefreshSignal: () => deps.runtimeState.pendingAutoRefreshSignal,
      setPendingAutoRefreshSignal: value => {
        deps.runtimeState.pendingAutoRefreshSignal = value;
      },
      readAutoRefreshSignal: deps.readAutoRefreshSignal,
      autoRefreshSignalKey: deps.jobsAutoRefreshSignalKey,
      handleAutoRefreshSignalValue,
      triggerAutoRefreshFromSignal
    });
  }

  async function triggerAutoRefreshFromSignal(signal) {
    return triggerJobsAutoRefreshFromSignal(signal, {
      getLastHandledAutoRefreshSignalId: () => deps.runtimeState.lastHandledAutoRefreshSignalId,
      setSourceStatus: text => deps.feedController.setSourceStatus(text),
      getAutoRefreshStatusText,
      refreshJobsNow: options => deps.feedController.refreshJobsNow(options),
      markAutoRefreshSignalHandled,
      showToast
    });
  }

  function readStateFromUrl() {
    const nextState = parseJobsPageUrlState(deps.windowObject.location.search, {
      defaultFilters: deps.defaultFilters,
      normalizeLifecycleStatus: deps.normalizeLifecycleStatus
    });
    deps.state.currentPage = nextState.currentPage;
    deps.state.filters = {
      ...deps.state.filters,
      ...nextState.filters,
      countries: Array.from(nextState.filters.countries || [])
    };
  }

  function writeStateToUrl() {
    deps.jobsUrlPersistence.writeStateToUrl(deps.state);
  }

  function rememberCurrentJobsUrl() {
    deps.jobsUrlPersistence.rememberCurrentJobsUrl();
  }

  function applyFiltersAndRender({ resetPage }) {
    deps.startupPreviewController.clearPendingStartupPreviewMaterialization();
    if (resetPage) {
      deps.state.currentPage = 1;
    }

    deps.emitDesktopStartupMetric("jobs_apply_filters_start", {
      resetPage: Boolean(resetPage),
      totalJobs: deps.runtimeState.allJobs.length
    });
    deps.filtersController.syncStateFromFilters();
    deps.runtimeState.filteredJobs = filterJobs(deps.runtimeState.allJobs, deps.state.filters, {
      currentUser: deps.userState.currentUser,
      seenJobKeys: deps.userState.seenJobKeys,
      getJobKeyForJob: deps.getJobKeyForJob,
      getJobLocationCities: deps.getJobLocationCities,
      getJobLocationCountries: deps.getJobLocationCountries,
      isInternshipJob: deps.isInternshipJob,
      matchesCountrySelection: matchesCountrySelectionForJobs
    });

    deps.emitDesktopStartupMetric("jobs_apply_filters_complete", {
      filteredCount: deps.runtimeState.filteredJobs.length
    });
    deps.runtimeState.filteredJobs = sortJobsFromQuery(deps.runtimeState.filteredJobs, deps.state.filters.sort, {
      fullCountryName: deps.fullCountryName
    });
    deps.emitDesktopStartupMetric("jobs_sort_complete", {
      filteredCount: deps.runtimeState.filteredJobs.length,
      sortMode: String(deps.state.filters.sort || "relevance")
    });
    displayJobs(deps.runtimeState.filteredJobs);
    deps.emitDesktopStartupMetric("jobs_write_state_start");
    writeStateToUrl();
    deps.emitDesktopStartupMetric("jobs_write_state_complete");
  }

  function displayJobs(jobs, options = {}) {
    return displayJobsFromView(jobs, {
      jobsList: deps.dom.jobsList,
      pagination: deps.dom.pagination,
      resultsSummary: deps.dom.resultsSummary,
      state: deps.state,
      allJobs: deps.runtimeState.allJobs,
      currentUser: deps.userState.currentUser,
      seenJobKeys: deps.userState.seenJobKeys,
      savedJobKeys: deps.userState.savedJobKeys,
      isJobsApiReady: deps.isJobsApiReady,
      getJobKeyForJob: deps.getJobKeyForJob,
      fullCountryName: deps.fullCountryName,
      goToPage,
      emitDesktopStartupMetric: deps.emitDesktopStartupMetric,
      renderJobRowHtml: deps.renderJobRowHtml
    }, options);
  }

  function goToPage(page) {
    if (page !== deps.state.currentPage) {
      deps.startupPreviewController.materializePendingStartupPreview();
    }
    return goToPageFromView(page, {
      filteredJobs: deps.runtimeState.filteredJobs,
      state: deps.state,
      displayJobs,
      writeStateToUrl
    });
  }

  function updateResultsSummary(total, from, to, loadedTotal = total) {
    return updateResultsSummaryFromView(deps.dom.resultsSummary, total, from, to, loadedTotal);
  }

  function setJobsStartupState(state, detail = "") {
    if (!deps.documentObject?.body) return;
    const normalized = normalizeToken(state) || "loading";
    deps.documentObject.body.setAttribute("data-jobs-startup-state", normalized);
    if (detail) {
      deps.documentObject.body.setAttribute("data-jobs-startup-detail", String(detail));
    } else {
      deps.documentObject.body.removeAttribute("data-jobs-startup-detail");
    }
  }

  function showError(message, onRetry = null) {
    setJobsStartupState("error", "load_error");
    deps.showJobsError(deps.dom.jobsList, deps.dom.pagination, message, () => {
      const retry = typeof onRetry === "function"
        ? onRetry
        : () => deps.retryInit().catch(err => handleJobsStartupFailure("Retry failed", err));
      return retry();
    });
    updateResultsSummary(0, 0, 0, deps.runtimeState.allJobs.length);
  }

  function handleJobsStartupFailure(context, err, options = {}) {
    deps.logJobsError(context, err);
    deps.feedController.setProgress(false);
    deps.feedController.setSourceStatus("Jobs page failed to start.");
    const retry = options.allowRetryReload
      ? () => deps.windowObject.location.reload()
      : () => deps.retryInit().catch(nextErr => handleJobsStartupFailure("Retry failed", nextErr));
    showError("Unable to load job listings right now.", retry);
  }

  return {
    ensureJobsPipelineStatusWatch,
    triggerJobsPipelineRun,
    handleAutoRefreshSignalValue,
    applyPendingAutoRefreshSignal,
    readStateFromUrl,
    writeStateToUrl,
    rememberCurrentJobsUrl,
    applyFiltersAndRender,
    displayJobs,
    goToPage,
    updateResultsSummary,
    setJobsStartupState,
    showError,
    handleJobsStartupFailure
  };
}

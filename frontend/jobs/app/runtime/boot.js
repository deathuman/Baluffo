import { bindAsyncClick, showToast } from "../../../shared/ui/index.js";
import { fetchJson, postJson } from "../../../shared/api-client.js";
import { awaitDesktopBootstrap, navigateDesktopPage } from "../../../shared/local-data/desktop-client.js";
import { createAdminBridgeButtonWatcher } from "../../../shared/admin-bridge-button.js";
import { openReleaseNotesDialog } from "../../../shared/ui/release-notes-dialog.js";
import { cacheJobsDom } from "../dom.js";
import { createJobsDesktopUpdateController } from "../desktop-update.js";
import { initJobsFeed } from "../feed.js";
import { scheduleNonCriticalStartup } from "../startup.js";

export function createJobsBoot(deps) {
  function cacheDom() {
    Object.assign(deps.dom, cacheJobsDom(deps.documentObject));
  }

  function startAdminBridgeButtonWatch() {
    if (!deps.runtimeState.adminBridgeWatcher) return;
    deps.runtimeState.adminBridgeWatcher.startAdminBridgeButtonWatch();
  }

  async function openAdminPageFromJobs() {
    if (deps.runtimeState.adminBridgeButtonState !== "online") {
      showToast("Admin bridge is offline.", "info");
      return;
    }
    deps.rememberCurrentJobsUrl();
    navigateDesktopPage("admin.html");
  }

  function scheduleNonCriticalStartupWork() {
    if (deps.runtimeState.nonCriticalStartupScheduled) return;
    deps.runtimeState.nonCriticalStartupScheduled = true;
    scheduleNonCriticalStartup(deps.windowObject, () => {
      deps.feedController.renderDataSources().catch(() => {});
      deps.ensureJobsPipelineStatusWatch();
    });
  }

  async function init() {
    return initJobsFeed({
      hasJobsList: Boolean(deps.dom.jobsList),
      emitMetric: deps.emitDesktopStartupMetric,
      initAuth: () => deps.authController.initAuth(),
      isDesktopRuntimeMode: deps.isDesktopRuntimeMode,
      readCachedJobs: () => deps.feedController.readCachedJobs(),
      normalizeRows: rows => {
        deps.runtimeState.allJobs = deps.normalizeJobs(rows, {
          professionLabels: deps.professionLabels,
          sanitizeUrl: deps.sanitizeUrl
        });
        return deps.runtimeState.allJobs;
      },
      recalculateItemsPerPage: () => deps.eventsController.recalculateItemsPerPage(),
      updateFilterOptions: () => deps.filtersController.updateFilterOptions(deps.runtimeState.allJobs),
      applyStateToFilters: () => deps.filtersController.applyStateToFilters(),
      applyFiltersAndRender: (...args) => deps.applyFiltersAndRender(...args),
      markStartupRendered: deps.markStartupRendered,
      markJobsFirstInteractive: deps.markJobsFirstInteractive,
      isJobsCacheStale: deps.isJobsCacheStale,
      cacheTtlMs: deps.jobsCacheTtlMs,
      setSourceStatus: text => deps.feedController.setSourceStatus(text),
      setProgress: visible => deps.feedController.setProgress(visible),
      refreshJobsNow: options => deps.feedController.refreshJobsNow(options),
      updateLastUpdatedText: timestamp => deps.feedController.updateLastUpdatedText(timestamp),
      fetchJobsReport: options => deps.feedController.fetchJobsReport(options),
      startJobsBootstrap: () => deps.callJobsBridge("/tasks/run-jobs-bootstrap", {
        method: "POST",
        body: { source: "jobs_cold_start" },
        allowStatuses: [409]
      }),
      windowObject: deps.windowObject,
      setHasInitializedJobsFeed: value => {
        deps.runtimeState.hasInitializedJobsFeed = Boolean(value);
      },
      scheduleNonCriticalStartupWork,
      applyPendingAutoRefreshSignal: (...args) => deps.applyPendingAutoRefreshSignal(...args),
      loadStartupPreviewJobs: () => deps.feedController.loadStartupPreviewJobs(),
      showError: (...args) => deps.showError(...args),
      getAllJobs: () => deps.runtimeState.allJobs
    });
  }

  function bootJobsPage() {
    cacheDom();
    deps.runtimeState.adminBridgeWatcher = createAdminBridgeButtonWatcher({
      buttonEl: deps.dom.adminPageBtn,
      baseUrl: deps.adminBridgeBase,
      fetchJson,
      applyState: deps.applyJobsAdminBridgeState,
      awaitBridgeReady: deps.isDesktopRuntimeMode() ? awaitDesktopBootstrap : async () => true
    });
    deps.runtimeState.desktopUpdateController = createJobsDesktopUpdateController({
      refs: deps.dom,
      baseUrl: deps.adminBridgeBase,
      fetchJson,
      postJson,
      bindAsyncClick,
      showToast,
      requestConfirmationDialog: deps.requestConfirmationDialog,
      isDesktopRuntimeMode: deps.isDesktopRuntimeMode,
      awaitDesktopBootstrap,
      showReleaseNotesDialog: options => openReleaseNotesDialog(options),
      openExternalUrl: url => deps.openJobLinkInDefaultBrowser(url)
    });
    startAdminBridgeButtonWatch();
    (async () => {
      try {
        await deps.runtimeState.desktopUpdateController.mount();
      } catch (err) {
        deps.logJobsError("Failed to initialize desktop update UI", err);
        return;
      }
      try {
        await deps.runtimeState.desktopUpdateController.startAutoCheck();
      } catch (err) {
        deps.logJobsError("Failed to auto-check desktop updates", err);
      }
    })();
    deps.eventsController.setupJobsListDelegation();
    deps.setJobsStartupState("loading", "booting");
    deps.eventsController.bindCoreEvents();
    try {
      deps.filtersController.initializeQuickFilters();
      deps.eventsController.bindEvents();
      deps.readStateFromUrl();
      deps.filtersController.applyStateToStaticFilters();
    } catch (err) {
      deps.handleJobsStartupFailure("Jobs page boot failed", err, { allowRetryReload: true });
      return;
    }
    init().catch(err => deps.handleJobsStartupFailure("Error initializing jobs", err));
  }

  return {
    bootJobsPage,
    init,
    openAdminPageFromJobs
  };
}

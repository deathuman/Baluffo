import { bindAsyncClick, showToast } from "../../../shared/ui/index.js";
import { fetchJson, postJson } from "../../../shared/api-client.js";
import { awaitDesktopBootstrap, navigateDesktopPage } from "../../../shared/local-data/desktop-client.js";
import { createAdminBridgeButtonWatcherForPage } from "../../../shared/admin-bridge-button.js";
import { openReleaseNotesDialog } from "../../../shared/ui/release-notes-dialog.js";
import { cacheJobsDom } from "../dom.js";
import { createJobsDesktopUpdateController } from "../desktop-update.js";
import { initJobsFeed } from "../feed.js";
import { scheduleNonCriticalStartup } from "../startup.js";
import { listFilterPresets, applyFilterPreset, saveFilterPreset, deleteFilterPreset } from "../saved-views.js";

export function createJobsBoot(deps) {
  function cacheDom() {
    Object.assign(deps.dom, cacheJobsDom(deps.documentObject));
  }

  function startAdminBridgeButtonWatch() {
    if (!deps.runtimeState.adminBridgeWatcher) return;
    deps.runtimeState.adminBridgeWatcher.startAdminBridgeButtonWatch();
  }

  async function openAdminPageFromJobs() {
    const adminBridgeState = deps.runtimeState.adminBridgeButtonState;
    const canOpenAdmin = adminBridgeState === "online" || adminBridgeState === "degraded";
    if (!canOpenAdmin && deps.isDesktopRuntimeMode()) {
      showToast("Admin bridge is offline.", "info");
      return;
    }
    if (adminBridgeState !== "online") {
      showToast("Admin status is delayed; opening Admin anyway.", "info");
    }
    deps.rememberCurrentJobsUrl();
    navigateDesktopPage("admin.html");
  }

  function scheduleNonCriticalStartupWork() {
    if (deps.runtimeState.nonCriticalStartupScheduled) return;
    deps.runtimeState.nonCriticalStartupScheduled = true;
    scheduleNonCriticalStartup(deps.windowObject, () => {
      if (!deps.isContainerRuntimeMode?.()) {
        deps.feedController.renderDataSources().catch(() => {});
      }
      deps.ensureJobsPipelineStatusWatch();
    });
  }

  function scheduleDesktopUpdateAutoCheck() {
    if (deps.runtimeState.desktopUpdateAutoCheckScheduled) return;
    deps.runtimeState.desktopUpdateAutoCheckScheduled = true;
    scheduleNonCriticalStartup(deps.windowObject, () => {
      deps.windowObject.setTimeout(() => {
        deps.runtimeState.desktopUpdateController?.startAutoCheck().catch(err => {
          deps.logJobsError("Failed to auto-check desktop updates", err);
        });
      }, 2500);
    });
  }

  async function init() {
    return (deps.initJobsFeed ?? initJobsFeed)({
      hasJobsList: Boolean(deps.dom.jobsList),
      emitMetric: deps.emitDesktopStartupMetric,
      initAuth: () => deps.authController.initAuth(),
      isDesktopRuntimeMode: deps.isDesktopRuntimeMode,
      isContainerRuntimeMode: deps.isContainerRuntimeMode,
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
      markJobsFirstInteractive: () => {
        deps.markJobsFirstInteractive();
        scheduleDesktopUpdateAutoCheck();
      },
      isJobsCacheStale: deps.isJobsCacheStale,
      cacheTtlMs: deps.jobsCacheTtlMs,
      setSourceStatus: text => deps.feedController.setSourceStatus(text),
      setProgress: visible => deps.feedController.setProgress(visible),
      refreshJobsNow: options => deps.feedController.refreshJobsNow(options),
      updateLastUpdatedText: timestamp => deps.feedController.updateLastUpdatedText(timestamp),
      fetchJobsReport: options => deps.feedController.fetchJobsReport(options),
      fetchJobsTaskLive: (options = {}) => deps.callJobsBridge("/ops/task-live/fetch?view=summary", {
        timeoutMs: Number(options?.timeoutMs) > 0 ? Number(options.timeoutMs) : 1500
      }),
      desktopJobsColdStart: deps.desktopJobsColdStart,
      startJobsBootstrap: (options = {}) => deps.callJobsBridge("/tasks/run-jobs-bootstrap", {
        method: "POST",
        body: { source: "jobs_first_run" },
        allowStatuses: [409],
        timeoutMs: Number(options?.timeoutMs) > 0
          ? Number(options.timeoutMs)
          : deps.bootstrapStartTimeoutMs
      }),
      windowObject: deps.windowObject,
      setJobsStartupState: deps.setJobsStartupState,
      bootstrapStartTimeoutMs: deps.bootstrapStartTimeoutMs,
      bootstrapConfirmTimeoutMs: deps.bootstrapConfirmTimeoutMs,
      bootstrapConfirmIntervalMs: deps.bootstrapConfirmIntervalMs,
      setHasInitializedJobsFeed: value => {
        deps.runtimeState.hasInitializedJobsFeed = Boolean(value);
      },
      scheduleNonCriticalStartupWork,
      applyPendingAutoRefreshSignal: (...args) => deps.applyPendingAutoRefreshSignal(...args),
      loadStartupPreviewJobs: () => deps.feedController.loadStartupPreviewJobs(),
      showError: (...args) => deps.showError(...args),
      getAllJobs: () => deps.runtimeState.allJobs,
      setAllJobs: jobs => {
        deps.runtimeState.allJobs = Array.isArray(jobs) ? jobs : [];
      },
      showFirstRunBootstrapNotice: deps.showFirstRunBootstrapNotice
    });
  }

  function bootJobsPage() {
    cacheDom();
    deps.runtimeState.adminBridgeWatcher = createAdminBridgeButtonWatcherForPage({
      buttonEl: deps.dom.adminPageBtn,
      baseUrl: deps.adminBridgeBase,
      fetchJson,
      applyState: deps.applyJobsAdminBridgeState,
      isDesktopRuntimeMode: deps.isDesktopRuntimeMode,
      awaitDesktopBootstrap,
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
    scheduleNonCriticalStartup(deps.windowObject, () => { initSavedViews(); });
  }

  function initSavedViews() {
    const w = deps.windowObject;
    const savedBar = w.document.getElementById("saved-views-bar");
    const dropdown = w.document.getElementById("saved-views-dropdown");
    const deleteBtn = w.document.getElementById("saved-views-delete-btn");

    function renderPresets() {
      if (!savedBar || !dropdown) return;
      const presets = listFilterPresets();
      if (presets.length === 0) { savedBar.classList.add("hidden"); return; }
      savedBar.classList.remove("hidden");
      dropdown.classList.remove("hidden");
      dropdown.innerHTML = `<option value="">Load...</option>${presets.map(p => `<option value="${escapeAttr(p.name)}">${escapeHtml(p.label)}</option>`).join("")}`;
      if (deleteBtn) deleteBtn.classList.remove("hidden");
    }

    if (dropdown) {
      dropdown.addEventListener("change", () => {
        const name = dropdown.value;
        if (!name) return;
        const state = applyFilterPreset(name, { filters: deps.runtimeState.pageState?.filters || {} });
        if (state) {
          deps.runtimeState.pageState = { ...deps.runtimeState.pageState, ...state };
          if (typeof deps.writeStateToUrl === "function") deps.writeStateToUrl(deps.runtimeState.pageState);
          deps.applyFiltersAndRender({ resetPage: true });
          showToast(`Loaded view: ${name}`, "success");
        }
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", () => {
        const name = dropdown.value;
        if (!name) return;
        deleteFilterPreset(name);
        renderPresets();
        showToast(`Deleted view: ${name}`, "info");
      });
    }

    const saveBtn = w.document.getElementById("saved-views-save-btn");
    if (!saveBtn && savedBar) {
      const btn = w.document.createElement("button");
      btn.id = "saved-views-save-btn";
      btn.className = "saved-view-save-btn";
      btn.textContent = "Save current";
      btn.addEventListener("click", () => {
        const name = prompt("Name this view:");
        if (!name || !String(name).trim()) return;
        const state = deps.runtimeState.pageState || { filters: {}, currentPage: 1 };
        saveFilterPreset(String(name).trim(), state, deps.defaultFilters || {});
        renderPresets();
        showToast(`Saved view: ${name}`, "success");
      });
      savedBar.insertBefore(btn, deleteBtn);
    }

    renderPresets();
  }

  function escapeAttr(s) { return String(s || "").replace(/"/g, "&quot;").replace(/</g, "&lt;"); }
  function escapeHtml(s) { const d = deps.documentObject.createElement("div"); d.textContent = String(s || ""); return d.innerHTML; }

  return {
    bootJobsPage,
    init,
    openAdminPageFromJobs
  };
}

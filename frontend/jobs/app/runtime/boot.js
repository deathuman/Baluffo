import { showToast } from "../../../shared/ui/index.js";
import { fetchJson } from "../../../shared/api-client.js";
import { awaitBridgeBootstrap } from "../../../shared/local-data/bridge-client.js";
import { navigateDesktopPage } from "../../../shared/local-data/desktop/navigation.js";
import { createAdminBridgeButtonWatcherForPage } from "../../../shared/admin-bridge-button.js";
import { cacheJobsDom } from "../dom.js";
import { initJobsFeed } from "../feed.js";
import { scheduleNonCriticalStartup } from "../startup.js";
import { listFilterPresets, applyFilterPreset, saveFilterPreset, deleteFilterPreset } from "../saved-views.js";

// The stale panel is the first thing a returning user sees, so the auto-check
// runs as soon as the page has painted instead of waiting on the feed: a check
// no longer depends on a jobs render finishing first, and the controller
// retries the bridge-not-ready case itself.
const DESKTOP_UPDATE_AUTO_CHECK_DELAY_MS = 250;

// Filter chrome, event binding and auth are interaction work: none of it can
// change what the first rows look like, so all of it waits for them. A page
// whose feed never paints -- no snapshot, a bridge that never answers -- must
// still get working controls and the update panel, so boot arms this fallback.
const BOOT_FOLLOW_UP_FALLBACK_MS = 1500;

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

  // One chunk, one controller: `desktop-update-surface.js` carries the panel,
  // its model, the release-notes dialog and the click diagnostics, none of which
  // can matter before the first rows are on screen.
  function loadDesktopUpdateSurface() {
    const runtimeState = deps.runtimeState;
    if (runtimeState.desktopUpdateSurfacePromise) return runtimeState.desktopUpdateSurfacePromise;
    runtimeState.desktopUpdateSurfacePromise = import("../desktop-update-surface.js")
      .then(({ createJobsDesktopUpdateSurface }) => createJobsDesktopUpdateSurface({
        refs: deps.dom,
        documentObject: deps.documentObject,
        emitDiagnostic: deps.emitDesktopStartupMetric,
        baseUrl: deps.adminBridgeBase,
        isDesktopRuntimeMode: deps.isDesktopRuntimeMode,
        openExternalUrl: url => deps.openJobLinkInDefaultBrowser(url)
      }))
      .then(controller => {
        runtimeState.desktopUpdateController = controller;
        return controller.mount().then(() => controller);
      })
      .catch(err => {
        deps.logJobsError("Failed to initialize desktop update UI", err);
        return null;
      });
    return runtimeState.desktopUpdateSurfacePromise;
  }

  function scheduleDesktopUpdateAutoCheck() {
    if (deps.runtimeState.desktopUpdateAutoCheckScheduled) return;
    deps.runtimeState.desktopUpdateAutoCheckScheduled = true;
    scheduleNonCriticalStartup(deps.windowObject, () => {
      deps.windowObject.setTimeout(() => {
        loadDesktopUpdateSurface()
          .then(controller => controller?.startAutoCheck?.())
          .catch(err => deps.logJobsError("Failed to auto-check desktop updates", err));
      }, DESKTOP_UPDATE_AUTO_CHECK_DELAY_MS);
    });
  }

  async function init() {
    return (deps.initJobsFeed ?? initJobsFeed)({
      hasJobsList: Boolean(deps.dom.jobsList),
      emitMetric: deps.emitDesktopStartupMetric,
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
        // Rows are on screen, so the rest of boot can run without sitting in
        // front of them. A new task, not a microtask: the first paint is next.
        scheduleBootFollowUp();
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

  // Everything that is not needed to draw the first rows. Runs once, either on
  // the first rows (`markJobsFirstInteractive`) or on the fallback armed by
  // `bootJobsPage`, whichever comes first. `init()` on its own -- a retry, or a
  // harness driving the feed directly -- leaves this alone.
  function scheduleBootFollowUp() {
    if (!deps.runtimeState.bootFollowUpArmed || deps.runtimeState.bootFollowUpRan) return;
    deps.windowObject.setTimeout(runBootFollowUp, 0);
  }

  function runBootFollowUp() {
    if (deps.runtimeState.bootFollowUpRan) return;
    deps.runtimeState.bootFollowUpRan = true;
    startAdminBridgeButtonWatch();
    // Auth is a handshake with the local profile, not feed data: the snapshot
    // paints as guest and re-renders with the profile's saved/seen state. Its
    // failure must not cost the controls below.
    try {
      deps.authController.initAuth();
    } catch (err) {
      deps.logJobsError("Failed to start auth", err);
    }
    try {
      deps.eventsController.bindCoreEvents();
      deps.filtersController.initializeQuickFilters();
      deps.eventsController.bindEvents();
      deps.filtersController.applyStateToStaticFilters();
    } catch (err) {
      deps.handleJobsStartupFailure("Jobs page boot failed", err, { allowRetryReload: true });
    }
    void loadDesktopUpdateSurface();
    scheduleDesktopUpdateAutoCheck();
    scheduleNonCriticalStartup(deps.windowObject, () => { initSavedViews(); });
  }

  function bootJobsPage() {
    // The snapshot read comes first. `init()` reaches it without waiting on the
    // filter DOM, the event bindings or the auth handshake, all of which now run
    // in `runBootFollowUp` behind the first rows.
    cacheDom();
    deps.runtimeState.adminBridgeWatcher = createAdminBridgeButtonWatcherForPage({
      buttonEl: deps.dom.adminPageBtn,
      baseUrl: deps.adminBridgeBase,
      fetchJson,
      applyState: deps.applyJobsAdminBridgeState,
      isDesktopRuntimeMode: deps.isDesktopRuntimeMode,
      awaitDesktopBootstrap: awaitBridgeBootstrap,
    });
    deps.eventsController.setupJobsListDelegation();
    deps.setJobsStartupState("loading", "booting");
    deps.readStateFromUrl();
    deps.runtimeState.bootFollowUpArmed = true;
    init().catch(err => deps.handleJobsStartupFailure("Error initializing jobs", err));
    deps.windowObject.setTimeout(runBootFollowUp, BOOT_FOLLOW_UP_FALLBACK_MS);
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
        const name = globalThis.prompt("Name this view:");
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
    openAdminPageFromJobs,
    // Called by the runtime's `markStartupRendered` the moment rows exist: that
    // is the first moment this chunk cannot delay them.
    startDesktopUpdateSurface: loadDesktopUpdateSurface,
    // Same trigger for the deferred half of boot. A fresh task, so the render
    // that produced the rows can paint before the bindings run.
    startBootFollowUp: scheduleBootFollowUp
  };
}

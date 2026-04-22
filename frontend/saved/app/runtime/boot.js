import { emitStartupMetric, markFirstInteractive } from "../../../shared/app-boot.js";
import { fetchJson } from "../../../shared/api-client.js";
import { createAdminBridgeButtonWatcher } from "../../../shared/admin-bridge-button.js";
import { navigateDesktopPage } from "../../../shared/local-data/desktop-client.js";
import { set as stateHubSet } from "../../../shared/state-hub.js";
import { bindAsyncClick, bindUi, showToast } from "../../../shared/ui/index.js";
import { runExportBackup as runExportBackupFromModule, runImportBackup as runImportBackupFromModule } from "../backup.js";
import { cacheSavedDom } from "../dom.js";
import { isEditingNotesField, shouldDeferSavedJobsRerender } from "../notes.js";
import { cacheSavedDomState } from "./state.js";
import { bindSavedJobsListDelegation, bindSavedPageEvents } from "./events.js";

export function createSavedBoot(deps) {
  function emitSavedStartupMetric(event, payload = {}) {
    emitStartupMetric(deps.startupMetrics, event, payload);
  }

  function markSavedFirstInteractive(reason) {
    markFirstInteractive(deps.startupMetrics, reason);
    deps.viewState.savedInteractiveMetricSent = true;
  }

  function applySavedAdminBridgeState(params) {
    return deps.applySavedAdminBridgeState(params);
  }

  function subscribeToSavedJobs(uid) {
    deps.viewState.unsubscribeSavedJobs = deps.savedPageService.subscribeSavedJobs(
      uid,
      jobs => {
        const count = Array.isArray(jobs) ? jobs.length : 0;
        stateHubSet("savedCount", count);
        stateHubSet("savedLastUpdated", Date.now());
        deps.setSourceStatus(`Loaded ${count} saved jobs.`);
        const isEditingNotes = isEditingNotesField();
        deps.viewState.lastSavedJobsByKey = new Map(
          (jobs || [])
            .map(job => [String(job?.jobKey || "").trim(), job])
            .filter(([jobKey]) => Boolean(jobKey))
        );
        if (shouldDeferSavedJobsRerender({
          isEditingNotes,
          inFlightCount: deps.noteSaveState.inFlight.size,
          pendingCount: deps.noteSaveState.pendingValues.size,
          lastInteractionAt: deps.noteSaveState.lastInteractionAt
        })) {
          deps.renderWorkspaceStats(jobs);
          deps.renderSelectedJobHint();
          deps.renderTimeline();
          return;
        }
        deps.renderSavedJobs(jobs);
        deps.refreshActivityLog().catch(() => {});
      },
      err => {
        console.error("Saved jobs subscription failed:", err);
        deps.setSourceStatus("Could not load saved jobs.");
        showToast("Could not load saved jobs.", "error");
        deps.renderAuthRequired("Unable to load your saved jobs right now.");
      }
    );
  }

  async function exportBackup() {
    await runExportBackupFromModule({
      currentUser: deps.viewState.currentUser,
      savedPageService: deps.savedPageService,
      includeFiles: Boolean(deps.dom.exportIncludeFilesEl?.checked),
      showToast
    });
  }

  async function importBackup(file) {
    await runImportBackupFromModule(file, {
      currentUser: deps.viewState.currentUser,
      savedPageService: deps.savedPageService,
      showToast,
      refreshActivityLog: deps.refreshActivityLog
    });
  }

  function bootSavedPage() {
    cacheSavedDomState(deps.dom, cacheSavedDom(deps.documentObject));
    deps.viewState.adminBridgeWatcher = createAdminBridgeButtonWatcher({
      buttonEl: deps.dom.adminPageBtnEl,
      baseUrl: deps.adminBridgeBase,
      fetchJson,
      applyState: applySavedAdminBridgeState
    });
    deps.viewState.adminBridgeWatcher?.startAdminBridgeButtonWatch();
    bindSavedJobsListDelegation({
      dom: deps.dom,
      viewState: deps.viewState,
      cssEscape: deps.cssEscape,
      setSelectedJobKey: deps.setSelectedJobKey,
      removeSavedJob: deps.removeSavedJob,
      updatePhase: deps.updatePhase,
      toggleDetailsForJob: deps.toggleDetailsForJob,
      openCustomJobEditor: deps.openCustomJobEditor,
      setJobDetailsTab: deps.setJobDetailsTab,
      applyJobDetailsTab: deps.applyJobDetailsTab,
      refreshActivityLog: deps.refreshActivityLog,
      renderSavedJobs: deps.renderSavedJobs,
      queueNotesSave: deps.queueNotesSave,
      flushNotesSave: deps.flushNotesSave,
      uploadAttachments: deps.uploadAttachments
    });
    bindSavedPageEvents({
      dom: deps.dom,
      viewState: deps.viewState,
      bindUi,
      bindAsyncClick,
      getLastJobsUrl: deps.getLastJobsUrl,
      navigateDesktopPage,
      showToast,
      defaultSavedFilter: deps.defaultSavedFilter,
      defaultSavedSort: deps.defaultSavedSort,
      timelineScopeAll: deps.timelineScopeAll,
      setCustomJobPanelOpen: deps.setCustomJobPanelOpen,
      createCustomJob: deps.createCustomJob,
      updateCustomJobWarning: deps.updateCustomJobWarning,
      setSavedFilter: deps.setSavedFilter,
      setSavedSort: deps.setSavedSort,
      renderSavedJobs: deps.renderSavedJobs,
      setActivityPanelOpen: deps.setActivityPanelOpen,
      refreshActivityLog: deps.refreshActivityLog,
      signInUser: () => deps.savedAuthController.signInUser(),
      signOutUser: () => deps.savedAuthController.signOutUser(),
      exportBackup,
      importBackup,
      updateGlobalOverrideButton: deps.updateGlobalOverrideButton,
      setTimelineScope: deps.setTimelineScope,
      renderTimeline: deps.renderTimeline
    });
    deps.savedAuthController.initSavedJobsPage();
  }

  return {
    bootSavedPage,
    emitSavedStartupMetric,
    markSavedFirstInteractive,
    subscribeToSavedJobs,
    exportBackup,
    importBackup
  };
}

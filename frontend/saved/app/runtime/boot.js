import { emitStartupMetric, markFirstInteractive } from "../../../shared/app-boot.js";
import { fetchJson } from "../../../shared/api-client.js";
import { createAdminBridgeButtonWatcherForPage } from "../../../shared/admin-bridge-button.js";
import { awaitDesktopBootstrap, navigateDesktopPage } from "../../../shared/local-data/desktop-client.js";
import { availabilityCheckVerdict, runJobAvailabilityCheck } from "../../../shared/job-availability-check.js";
import { createPerfMarks } from "../../../shared/perf-marks.js";
import { bindAsyncClick, bindUi, showToast } from "../../../shared/ui/index.js";
import { sanitizeUrl } from "../../../shared/data/index.js";
import { runExportBackup as runExportBackupFromModule, runImportBackup as runImportBackupFromModule } from "../backup.js";
import { cacheSavedDom } from "../dom.js";
import { isEditingNotesField, shouldDeferSavedJobsRerender } from "../notes.js";
import { cacheSavedDomState } from "./state.js";
import { bindSavedJobsListDelegation, bindSavedPageEvents } from "./events.js";

export async function runSavedAvailabilityReport(
  deps,
  refreshAvailabilityAttention = async () => {},
  jobKey,
  action = "report"
) {
  if (!deps.canManageAvailability?.()) return;
  const uid = String(deps.viewState.currentUser?.uid || "").trim();
  if (!uid) return;
  const notify = deps.showToast || showToast;
  const safeJobKey = String(jobKey || "").trim();
  const savedJob = deps.viewState.lastSavedJobsByKey.get(safeJobKey) || {};
  const label = [String(savedJob.title || "").trim(), String(savedJob.company || "").trim()]
    .filter(Boolean)
    .join(" at ") || "this saved job";
  const monitored = Boolean(
    String(savedJob.availabilityId || "").trim()
    && String(savedJob.jobLink || "").trim()
  );
  if (action === "report" && typeof deps.requestConfirmationDialog === "function") {
    const confirmed = await deps.requestConfirmationDialog({
      title: "Report job unavailable?",
      description: monitored
        ? `Report "${label}" as unavailable? It will remain visible in Saved jobs, and an independent availability check will be queued. You can clear the report any time.`
        : `Report "${label}" as unavailable? It will remain visible in Saved jobs until you clear the report.`,
      confirmLabel: "Report unavailable",
      cancelLabel: "Cancel"
    });
    if (!confirmed) return;
    if (String(deps.viewState.currentUser?.uid || "").trim() !== uid) {
      notify("Profile changed; unavailable report was not updated.", "info");
      return;
    }
  }
  const result = await deps.savedPageService.manageAvailabilityReport(
    uid, safeJobKey, action
  );
  const queued = Boolean(result.data?.queuedForCheck);
  const successMessage = action === "clear"
    ? "Unavailable report cleared."
    : queued
      ? "Reported unavailable; verification queued."
      : "Reported unavailable for this profile. You can clear the report any time.";
  if (!result.ok) {
    notify(result.error, "error");
    return;
  }
  // Keep the current row interactive while the subscription catches up.
  if (
    String(deps.viewState.currentUser?.uid || "").trim() === uid
    && savedJob
    && typeof savedJob === "object"
  ) {
    const attention = savedJob.availabilityAttention && typeof savedJob.availabilityAttention === "object"
      ? { ...savedJob.availabilityAttention }
      : {};
    if (action === "clear") {
      attention.localReport = {};
      attention.hiddenByReport = false;
    } else {
      attention.localReport = {
        reportedAt: String(result.data?.reportedAt || new Date().toISOString()),
        availabilityId: String(savedJob.availabilityId || ""),
        queuedForCheck: queued
      };
      attention.hiddenByReport = true;
    }
    savedJob.availabilityAttention = attention;
    deps.viewState.lastSavedJobsByKey.set(safeJobKey, savedJob);
    deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
  }
  notify(successMessage, "info", action === "report" ? {
    durationMs: 7000,
    actionLabel: "Undo",
    onAction: async () => {
      const undo = await deps.savedPageService.manageAvailabilityReport(
        uid, safeJobKey, "clear"
      );
      if (!undo.ok) {
        notify(undo.error, "error");
        return;
      }
      if (String(deps.viewState.currentUser?.uid || "").trim() === uid) {
        const current = deps.viewState.lastSavedJobsByKey.get(safeJobKey);
        if (current) {
          current.availabilityAttention = {
            ...(current.availabilityAttention || {}),
            localReport: {},
            hiddenByReport: false
          };
          deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
        }
        await refreshAvailabilityAttention();
      }
      notify("Unavailable report cleared.", "success");
    }
  } : { durationMs: 3500 });
  if (String(deps.viewState.currentUser?.uid || "").trim() === uid) {
    await refreshAvailabilityAttention();
  }
}

export function createSavedBoot(deps) {
  const savedPerfMarks = createPerfMarks(deps.startupMetrics);

  function renderAvailabilityAttention(summary = {}) {
    const count = Math.max(0, Number(summary.count || 0));
    deps.viewState.availabilityAttentionSummary = {
      count,
      events: Array.isArray(summary.events) ? summary.events : []
    };
    deps.dom.availabilityAttentionBannerEl?.classList.toggle("hidden", count === 0);
    if (deps.dom.availabilityAttentionCountEl) {
      deps.dom.availabilityAttentionCountEl.textContent = count === 1
        ? "1 availability update needs attention."
        : `${count} availability updates need attention.`;
    }
    const filterButton = Array.from(deps.dom.savedCustomFilterBtnEls || []).find(
      button => button.dataset.savedFilter === "availability_attention"
    );
    if (filterButton) {
      filterButton.textContent = count ? `Availability attention (${count})` : "Availability attention";
    }
  }

  async function refreshAvailabilityAttention() {
    const uid = deps.viewState.currentUser?.uid || "";
    if (!uid) {
      renderAvailabilityAttention({ count: 0, events: [] });
      return { ok: true, data: { count: 0, events: [] } };
    }
    const result = await deps.savedPageService.getAvailabilityAttention(uid);
    renderAvailabilityAttention(result.data || { count: 0, events: [] });
    return result;
  }

  function emitSavedStartupMetric(event, payload = {}) {
    emitStartupMetric(deps.startupMetrics, event, payload);
  }

  function markSavedStep(name, payload = {}) {
    savedPerfMarks.markStep(name, payload);
  }

  function measureSavedStep(name, startMark, endMark, payload = {}) {
    savedPerfMarks.measureStep(name, startMark, endMark, payload);
  }

  function markSavedFirstRender(stage, rowCount = 0) {
    if (typeof deps.startupMetrics?.markRendered === "function") {
      deps.startupMetrics.markRendered(stage, rowCount);
    }
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
        const overlayRequestId = (Number(deps.viewState.savedLifecycleOverlayRequestId) || 0) + 1;
        deps.viewState.savedLifecycleOverlayRequestId = overlayRequestId;
        const count = Array.isArray(jobs) ? jobs.length : 0;
        deps.setSourceStatus(`Loaded ${count} saved jobs.`);
        const isEditingNotes = isEditingNotesField();
        deps.viewState.lastSavedJobsByKey = new Map(
          (jobs || [])
            .map(job => [String(job?.jobKey || "").trim(), job])
            .filter(([jobKey]) => Boolean(jobKey))
        );
        refreshAvailabilityAttention().catch(() => {});
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
        markSavedFirstRender("saved_jobs", count);
        deps.refreshActivityLog().catch(() => {});
        deps.loadSavedLifecycleOverlay()
          .then(overlayByJobKey => {
            if (deps.viewState.savedLifecycleOverlayRequestId !== overlayRequestId) return;
            deps.viewState.savedLifecycleOverlayByJobKey = overlayByJobKey instanceof Map
              ? overlayByJobKey
              : new Map();
            if (shouldDeferSavedJobsRerender({
              isEditingNotes: isEditingNotesField(),
              inFlightCount: deps.noteSaveState.inFlight.size,
              pendingCount: deps.noteSaveState.pendingValues.size,
              lastInteractionAt: deps.noteSaveState.lastInteractionAt
            })) {
              return;
            }
            deps.renderSavedJobs(Array.isArray(jobs) ? jobs : []);
          })
          .catch(() => {
            if (deps.viewState.savedLifecycleOverlayRequestId !== overlayRequestId) return;
            deps.viewState.savedLifecycleOverlayByJobKey = new Map();
          });
      },
      err => {
        console.error("Saved jobs subscription failed:", err);
        deps.setSourceStatus("Could not load saved jobs.");
          showToast("Could not load saved jobs.", "error");
        deps.renderAuthRequired("Unable to load your saved jobs right now.");
        markSavedFirstRender("auth_error", 0);
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
    markSavedStep("saved_boot_start");
    markSavedStep("saved_dom_cache_start");
    cacheSavedDomState(deps.dom, cacheSavedDom(deps.documentObject));
    markSavedStep("saved_dom_cache_end");
    measureSavedStep("saved_dom_cache", "saved_dom_cache_start", "saved_dom_cache_end");
    deps.viewState.adminBridgeWatcher = createAdminBridgeButtonWatcherForPage({
      buttonEl: deps.dom.adminPageBtnEl,
      baseUrl: deps.adminBridgeBase,
      fetchJson,
      applyState: applySavedAdminBridgeState,
      isDesktopRuntimeMode: deps.isDesktopRuntimeMode,
      awaitDesktopBootstrap,
    });
    deps.viewState.adminBridgeWatcher?.startAdminBridgeButtonWatch();
    bindSavedJobsListDelegation({
      dom: deps.dom,
      viewState: deps.viewState,
      cssEscape: deps.cssEscape,
      setSelectedJobKey: deps.setSelectedJobKey,
      removeSavedJob: deps.removeSavedJob,
      updatePhase: deps.updatePhase,
      updateOutcome: deps.updateOutcome,
      toggleDetailsForJob: deps.toggleDetailsForJob,
      openCustomJobEditor: deps.openCustomJobEditor,
      setJobDetailsTab: deps.setJobDetailsTab,
      applyJobDetailsTab: deps.applyJobDetailsTab,
      refreshActivityLog: deps.refreshActivityLog,
      renderSavedJobs: deps.renderSavedJobs,
      queueNotesSave: deps.queueNotesSave,
      flushNotesSave: deps.flushNotesSave,
      uploadAttachments: deps.uploadAttachments,
      checkAvailability: async availabilityId => {
        if (!deps.canManageAvailability?.()) return;
        const savedJob = Array.from(deps.viewState.lastSavedJobsByKey.values())
          .find(job => String(job?.availabilityId || "") === String(availabilityId || "")) || null;
        const jobLink = sanitizeUrl(String(savedJob?.jobLink || ""));
        const progress = showToast("Checking availability…", "info", { durationMs: 0 });
        const result = await runJobAvailabilityCheck(
          deps.savedPageService,
          availabilityId,
          {
            onProgress: ({ elapsedS }) => progress.update({
              message: `Checking availability… ${Number(elapsedS) || 1}s`
            })
          }
        );
        progress.dismiss();
        if (!result.ok) {
          showToast(String(result.error || "The availability check failed — try again."), "error");
          return;
        }
        const verdict = availabilityCheckVerdict(result.data);
        const options = verdict.conclusive || !jobLink
          ? {}
          : {
              actionLabel: "Open job page",
              onAction: () => window.open(jobLink, "_blank", "noopener,noreferrer"),
              durationMs: 7000
            };
        showToast(verdict.message, verdict.tone, options);
        if (result.ok) {
          const overlay = await deps.loadSavedLifecycleOverlay();
          deps.viewState.savedLifecycleOverlayByJobKey = overlay instanceof Map
            ? overlay
            : new Map();
          deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
        }
      },
      reportUnavailable: (jobKey, action = "report") => (
        runSavedAvailabilityReport(deps, refreshAvailabilityAttention, jobKey, action)
      ),
      acknowledgeAvailability: async jobKey => {
        if (!deps.viewState.currentUser) return;
        const job = deps.viewState.lastSavedJobsByKey.get(jobKey);
        const events = Array.isArray(job?.availabilityAttention?.events) ? job.availabilityAttention.events : [];
        const unread = events.find(event => event?.alert && !event?.acknowledgedAt);
        if (!unread) return;
        const result = await deps.savedPageService.acknowledgeAvailabilityAttention(
          deps.viewState.currentUser.uid, { transitionId: unread.transitionId }
        );
        showToast(result.ok ? "Availability update acknowledged." : result.error, result.ok ? "info" : "error");
        if (result.ok) await refreshAvailabilityAttention();
      }
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
      defaultSavedGroup: deps.defaultSavedGroup,
      timelineScopeAll: deps.timelineScopeAll,
      setCustomJobPanelOpen: deps.setCustomJobPanelOpen,
      createCustomJob: deps.createCustomJob,
      updateCustomJobWarning: deps.updateCustomJobWarning,
      setSavedFilter: deps.setSavedFilter,
      setSavedSort: deps.setSavedSort,
      setSavedGroup: deps.setSavedGroup,
      persistSavedListPreferences: deps.persistSavedListPreferences,
      renderSavedJobs: deps.renderSavedJobs,
      setActivityPanelOpen: deps.setActivityPanelOpen,
      refreshActivityLog: deps.refreshActivityLog,
      signInUser: () => deps.savedAuthController.signInUser(),
      signOutUser: () => deps.savedAuthController.signOutUser(),
      exportBackup,
      importBackup,
      setTimelineScope: deps.setTimelineScope,
      renderTimeline: deps.renderTimeline,
      showAvailabilityAttention: () => {
        deps.setSavedFilter("availability_attention");
        deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
      },
      acknowledgeAllAvailability: async () => {
        const uid = deps.viewState.currentUser?.uid || "";
        if (!uid) return;
        const result = await deps.savedPageService.acknowledgeAvailabilityAttention(
          uid,
          { allCurrent: true }
        );
        showToast(
          result.ok ? "All current availability updates acknowledged." : result.error,
          result.ok ? "success" : "error"
        );
        if (result.ok) await refreshAvailabilityAttention();
      }
    });
    deps.savedAuthController.initSavedJobsPage();
    markSavedStep("saved_boot_end");
    measureSavedStep("saved_boot", "saved_boot_start", "saved_boot_end");
  }

  return {
    bootSavedPage,
    emitSavedStartupMetric,
    markSavedStep,
    measureSavedStep,
    markSavedFirstRender,
    markSavedFirstInteractive,
    subscribeToSavedJobs,
    exportBackup,
    importBackup,
    refreshAvailabilityAttention
  };
}

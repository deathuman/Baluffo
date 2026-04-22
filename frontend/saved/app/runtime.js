import { AdminConfig as adminConfig } from "../../shared/config/admin-config.js";
import {
  showToast,
  setText,
  bindUi,
  bindAsyncClick
} from "../../shared/ui/index.js";
import { emitStartupMetric, markFirstInteractive } from "../../shared/app-boot.js";
import {
  normalizeCustomJobInput as normalizeCustomJobInputFromDomain,
  toDatetimeLocalValue as toDatetimeLocalValueFromDomain,
  activityTypeLabel as activityTypeLabelFromDomain,
  formatActivityDetail as formatActivityDetailFromDomain
} from "../domain.js";
import {
  isSavedApiReady,
  savedAuthService,
  savedPageService
} from "../services.js";
import {
  parseIsoDate,
  getReminderMeta,
  renderSavedJobBlockHtml,
  renderActivityEntryHtml,
  formatRelativeTime,
  getJobHistoryEntries,
  renderPhaseBar,
  renderWebIcon,
  formatPhaseTimestamp,
  renderDetailsSummary
} from "../render.js";
import { createSavedDispatcher, SAVED_ACTIONS } from "../actions.js";
import {
  loadSavedTimelinePreferences,
  persistSavedTimelinePreferences,
  readSavedLastJobsUrl
} from "../state-sync/index.js";
import { set as stateHubSet } from "../../shared/state-hub.js";
import { fetchJson, postJson } from "../../shared/api-client.js";
import { createAdminBridgeButtonWatcher } from "../../shared/admin-bridge-button.js";
import { createAuthReadyPoller } from "../../shared/auth-ready-poll.js";
import { cacheSavedDom } from "./dom.js";
import { applySavedAdminBridgeState as applySavedAdminBridgeStateFromModule } from "./admin-bridge-state.js";
import { requestConfirmationDialog, requestTextInputDialog } from "../../local-data/profile-name-dialog.js";
import { navigateDesktopPage } from "../../shared/local-data/desktop-client.js";
import { runExportBackup as runExportBackupFromModule, runImportBackup as runImportBackupFromModule } from "./backup.js";
import {
  isEditingNotesField,
  shouldDeferSavedJobsRerender,
  queueNotesSave as queueNotesSaveFromModule,
  flushNotesSave as flushNotesSaveFromModule,
  clearNoteSaveQueues as clearNoteSaveQueuesFromModule
} from "./notes.js";
import { computeAnchorScrollDelta } from "./render-cycle.js";
import {
  SAVED_FILTER_ALL,
  SORT_UPDATED,
  isValidSavedFilter,
  isValidSavedSort,
  REMINDER_SOON_HOURS
} from "./view-state.js";
import { createSavedPageState, cacheSavedDomState } from "./runtime/state.js";
import { createSavedAuthController } from "./runtime/auth-controller.js";
import { createSavedStartupMetrics } from "./runtime/effects.js";
import { setStatusText, setElementText } from "./runtime/view.js";
import { bindSavedPageEvents, bindSavedJobsListDelegation } from "./runtime/events.js";
import { createSavedActivityController } from "./runtime/activity-controller.js";
import { createSavedAttachmentsController } from "./runtime/attachments-controller.js";
import { createSavedCustomJobController } from "./runtime/custom-job-controller.js";
import { createSavedRenderController } from "./runtime/render-controller.js";
const savedAuthReadyPoller = createAuthReadyPoller({
  isReady: () => savedPageService.isAvailable() && isSavedApiReady(),
  onReady: () => savedAuthController.initSavedJobsPage()
});
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";
const TIMELINE_PREF_PREFIX = "baluffo_saved_timeline_prefs";
const CUSTOM_SOURCE_LABEL = "Custom";
const DEFAULT_SAVED_FILTER = SAVED_FILTER_ALL;
const ACTIVITY_HIGHLIGHT_MS = 2600;
const TIMELINE_SCOPE_ALL = "all";
const TIMELINE_SCOPE_SELECTED = "selected";
const TIMELINE_SCOPE_PHASE = "phase";
const TIMELINE_SCOPE_NOTES = "notes";
const TIMELINE_SCOPE_ATTACHMENTS = "attachments";

const PHASE_OPTIONS = ["bookmark", "applied", "interview_1", "interview_2", "offer", "rejected"];
const PHASE_LABELS = {
  bookmark: "Saved",
  applied: "Applied",
  interview_1: "Interview 1",
  interview_2: "Interview 2",
  offer: "Final Round",
  rejected: "Rejected"
};

const MAX_ATTACHMENTS_PER_JOB = 20;
const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;
const NOTE_AUTOSAVE_MS = 600;

const pageState = createSavedPageState({
  defaultSavedFilter: DEFAULT_SAVED_FILTER,
  defaultSavedSort: SORT_UPDATED,
  defaultTimelineScope: TIMELINE_SCOPE_ALL
});
const dom = pageState.dom;
const viewState = pageState.viewState;
const savedDispatch = createSavedDispatcher();
const noteSaveState = pageState.noteSaveState;
const attachmentPreviewUrls = pageState.attachmentPreviewUrls;
const savedActivityController = createSavedActivityController({
  dom,
  viewState,
  savedPageService,
  setActivityStatus,
  timelinePrefPrefix: TIMELINE_PREF_PREFIX,
  timelineScopeAll: TIMELINE_SCOPE_ALL,
  activityHighlightMs: ACTIVITY_HIGHLIGHT_MS,
  renderActivityEntryHtml,
  getReminderMeta,
  loadSavedTimelinePreferences,
  persistSavedTimelinePreferences,
  activityTypeLabel,
  formatActivityDetail,
  formatPhaseTimestamp
});
const savedCustomJobController = createSavedCustomJobController({
  dom,
  viewState,
  savedPageService,
  normalizeCustomJobInput,
  toDatetimeLocalValue,
  savedDispatch,
  savedActions: SAVED_ACTIONS,
  queueActivityPulse,
  timelineScopeAll: TIMELINE_SCOPE_ALL,
  refreshActivityLog
});
const savedAttachmentsController = createSavedAttachmentsController({
  dom,
  viewState,
  savedPageService,
  savedDispatch,
  savedActions: SAVED_ACTIONS,
  queueActivityPulse,
  timelineScopeAttachments: TIMELINE_SCOPE_ATTACHMENTS,
  maxAttachmentsPerJob: MAX_ATTACHMENTS_PER_JOB,
  maxAttachmentBytes: MAX_ATTACHMENT_BYTES,
  attachmentPreviewUrls,
  cssEscape,
  setSelectedJobKey
});
const savedRenderController = createSavedRenderController({
  dom,
  viewState,
  savedPageService,
  timelineScopeAll: TIMELINE_SCOPE_ALL,
  timelineScopeSelected: TIMELINE_SCOPE_SELECTED,
  phaseOptions: PHASE_OPTIONS,
  phaseLabels: PHASE_LABELS,
  customSourceLabel: CUSTOM_SOURCE_LABEL,
  reminderSoonHours: REMINDER_SOON_HOURS,
  maxAttachmentsPerJob: MAX_ATTACHMENTS_PER_JOB,
  maxAttachmentBytes: MAX_ATTACHMENT_BYTES,
  computeAnchorScrollDelta,
  cssEscape,
  renderTimeline,
  renderWorkspaceStats,
  renderSelectedJobHint,
  updateTimelineScopeButtons,
  setSavedFilterBarVisible,
  setSavedSortBarVisible,
  renderSavedFilterMeta,
  renderReminderCounter,
  hydrateAttachmentLists,
  bindAttachmentActionButtons,
  renderSavedJobBlockHtml,
  parseIsoDate,
  getReminderMeta,
  formatRelativeTime,
  getJobHistoryEntries,
  renderPhaseBar,
  renderWebIcon,
  formatPhaseTimestamp,
  renderDetailsSummary,
  activityTypeLabel,
  formatActivityDetail
});
const startupMetrics = createSavedStartupMetrics({
  emitMetric: (event, payload) => {
    postJson(adminConfig.ADMIN_BRIDGE_BASE, "/desktop-local-data/startup-metric", { event, payload: payload || {} }).catch(() => {});
  }
});
const savedAuthController = createSavedAuthController({
  refs: dom,
  viewState,
  savedPageService,
  savedAuthService,
  savedAuthReadyPoller,
  isSavedApiReady,
  savedDispatch,
  SAVED_ACTIONS,
  clearNoteSaveQueues,
  setActivityPanelOpen,
  setCustomJobPanelOpen,
  setCustomJobAvailability,
  updateTimelineScopeButtons,
  renderWorkspaceStats,
  emitSavedStartupMetric,
  setSourceStatus,
  setActivityStatus,
  renderAuthRequired,
  renderTimeline,
  markSavedFirstInteractive,
  setSavedFilter,
  defaultSavedFilter: DEFAULT_SAVED_FILTER,
  setSavedSort,
  defaultSavedSort: SORT_UPDATED,
  renderSelectedJobHint,
  setBackupButtonsEnabled,
  setSavedFilterBarVisible,
  setSavedSortBarVisible,
  loadTimelinePreferences,
  subscribeToSavedJobs,
  refreshActivityLog,
  timelineScopeAll: TIMELINE_SCOPE_ALL,
  showToast
});

/**
 * Entry map (Saved runtime):
 * - boot initializes refs, bindings, auth/session and initial render.
 * - state concern: ./runtime/state.js
 * - effects concern: ./runtime/effects.js
 * - actions concern: ./runtime/actions.js
 * - view concern: ./runtime/view.js
 * - events concern: ./runtime/events.js
 */

function applySavedAdminBridgeState(params) {
  return applySavedAdminBridgeStateFromModule({ ...params, viewState });
}

function bootSavedPage() {
  cacheSavedDomState(dom, cacheSavedDom(document));
  viewState.adminBridgeWatcher = createAdminBridgeButtonWatcher({
    buttonEl: dom.adminPageBtnEl,
    baseUrl: adminConfig.ADMIN_BRIDGE_BASE,
    fetchJson,
    applyState: applySavedAdminBridgeState
  });
  viewState.adminBridgeWatcher?.startAdminBridgeButtonWatch();
  bindSavedJobsListDelegation({
    dom,
    viewState,
    cssEscape,
    setSelectedJobKey,
    removeSavedJob,
    updatePhase,
    toggleDetailsForJob,
    openCustomJobEditor,
    setJobDetailsTab,
    applyJobDetailsTab,
    refreshActivityLog,
    renderSavedJobs,
    queueNotesSave,
    flushNotesSave,
    uploadAttachments
  });
  bindSavedPageEvents({
    dom,
    viewState,
    bindUi,
    bindAsyncClick,
    getLastJobsUrl,
    navigateDesktopPage,
    showToast,
    defaultSavedFilter: DEFAULT_SAVED_FILTER,
    defaultSavedSort: SORT_UPDATED,
    timelineScopeAll: TIMELINE_SCOPE_ALL,
    setCustomJobPanelOpen,
    createCustomJob,
    updateCustomJobWarning,
    setSavedFilter,
    setSavedSort,
    renderSavedJobs,
    setActivityPanelOpen,
    refreshActivityLog,
    signInUser: () => savedAuthController.signInUser(),
    signOutUser: () => savedAuthController.signOutUser(),
    exportBackup,
    importBackup,
    updateGlobalOverrideButton,
    setTimelineScope,
    renderTimeline
  });
  savedAuthController.initSavedJobsPage();
}

function emitSavedStartupMetric(event, payload = {}) {
  emitStartupMetric(startupMetrics, event, payload);
}

function markSavedFirstInteractive(reason) {
  markFirstInteractive(startupMetrics, reason);
  viewState.savedInteractiveMetricSent = true;
}

function subscribeToSavedJobs(uid) {
  viewState.unsubscribeSavedJobs = savedPageService.subscribeSavedJobs(
    uid,
    jobs => {
      const count = Array.isArray(jobs) ? jobs.length : 0;
      stateHubSet("savedCount", count);
      stateHubSet("savedLastUpdated", Date.now());
      setSourceStatus(`Loaded ${count} saved jobs.`);
      const isEditingNotes = isEditingNotesField();
      viewState.lastSavedJobsByKey = new Map(
        (jobs || [])
          .map(job => [String(job?.jobKey || "").trim(), job])
          .filter(([jobKey]) => Boolean(jobKey))
      );
      if (shouldDeferSavedJobsRerender({
        isEditingNotes,
        inFlightCount: noteSaveState.inFlight.size,
        pendingCount: noteSaveState.pendingValues.size,
        lastInteractionAt: noteSaveState.lastInteractionAt
      })) {
        renderWorkspaceStats(jobs);
        renderSelectedJobHint();
        renderTimeline();
        return;
      }
      renderSavedJobs(jobs);
      refreshActivityLog().catch(() => {
        // Best-effort refresh.
      });
    },
    err => {
      console.error("Saved jobs subscription failed:", err);
      setSourceStatus("Could not load saved jobs.");
      showToast("Could not load saved jobs.", "error");
      renderAuthRequired("Unable to load your saved jobs right now.");
    }
  );
}

function renderAuthRequired(message) {
  return savedRenderController.renderAuthRequired(message);
}

function renderSavedJobs(jobs) {
  return savedRenderController.renderSavedJobs(jobs);
}

function getJobDetailsTab(jobKey) {
  return savedRenderController.getJobDetailsTab(jobKey);
}

function setJobDetailsTab(jobKey, tab) {
  return savedRenderController.setJobDetailsTab(jobKey, tab);
}

function normalizePhase(phase) {
  return savedRenderController.normalizePhase(phase);
}

function canTransition(currentPhase, nextPhase) {
  return savedRenderController.canTransition(currentPhase, nextPhase);
}

async function removeSavedJob(jobKey) {
  if (!viewState.currentUser) {
    showToast("Sign in required.", "error");
    return;
  }
  const removedSnapshot = viewState.lastSavedJobsByKey.get(String(jobKey || "")) || null;
  try {
    const removeResult = await savedPageService.removeSavedJobForUser(viewState.currentUser.uid, jobKey);
    if (!removeResult.ok) throw new Error(removeResult.error || "Could not remove job.");
    showToast("Removed saved job.", "success", {
      durationMs: 6500,
      actionLabel: "Revert",
      onAction: async () => {
        if (!viewState.currentUser || !removedSnapshot) return;
        try {
          const restoreResult = await savedPageService.saveJobForUser(viewState.currentUser.uid, removedSnapshot);
          if (!restoreResult.ok) throw new Error(restoreResult.error || "Could not restore job.");
          showToast("Saved job restored.", "success");
        } catch (restoreErr) {
          console.error("Could not restore removed job:", restoreErr);
          showToast("Could not restore removed job.", "error");
        }
      }
    });
  } catch (err) {
    console.error("Could not remove saved job:", err);
    showToast("Could not remove job.", "error");
  }
}

async function updatePhase(jobKey, phase) {
  if (!viewState.currentUser) {
    showToast("Sign in required.", "error");
    return;
  }

  const safeJobKey = String(jobKey || "").trim();
  if (!safeJobKey) {
    showToast("Invalid saved job key.", "error");
    return;
  }
  const row = viewState.lastSavedJobsByKey.get(safeJobKey);
  if (!row) {
    showToast("Saved job not found. Refresh and retry.", "error");
    return;
  }
  const currentPhase = normalizePhase(row?.applicationStatus);
  const normalized = normalizePhase(phase);
  if (normalized === currentPhase) {
    return;
  }
  const regularAllowed = canTransition(currentPhase, normalized);
  const overrideArmed = viewState.phaseOverrideArmedGlobal;
  if (!regularAllowed && !overrideArmed) {
    showToast("Locked transition. Use Override Phase Lock for exceptional changes.", "info");
    return;
  }

  if (!regularAllowed && overrideArmed) {
    const from = PHASE_LABELS[currentPhase] || currentPhase;
    const to = PHASE_LABELS[normalized] || normalized;
    const ok = await requestConfirmationDialog({
      title: "Override phase lock?",
      description: `${from} -> ${to}`,
      confirmLabel: "Override"
    });
    if (!ok) return;
  }

  try {
    const interviewTimestamp = needsInterviewTimestamp(normalized)
      ? await requestInterviewTimestamp(normalized, row?.phaseTimestamps?.[normalized] || "")
      : "";
    if (needsInterviewTimestamp(normalized) && !interviewTimestamp) {
      return;
    }
    const previousPhaseTimestamp = String(row?.phaseTimestamps?.[currentPhase] || "").trim();
    const updateOptions = {
      override: !regularAllowed && overrideArmed
    };
    if (interviewTimestamp) {
      updateOptions.preserveTimestamp = interviewTimestamp;
    }
    const updateResult = await savedPageService.updateApplicationStatus(
      viewState.currentUser.uid,
      safeJobKey,
      normalized,
      updateOptions
    );
    if (!updateResult.ok) throw new Error(updateResult.error || "Could not update phase.");
    if (overrideArmed) {
      viewState.phaseOverrideArmedGlobal = false;
      updateGlobalOverrideButton();
    }
    const previousPhase = currentPhase;
    showToast(`Phase updated to ${PHASE_LABELS[normalized] || normalized}.`, "success", {
      durationMs: 6500,
      actionLabel: "Revert",
      onAction: async () => {
        if (!viewState.currentUser) return;
        try {
          const revertResult = await savedPageService.updateApplicationStatus(viewState.currentUser.uid, safeJobKey, previousPhase, {
            override: true,
            cleanupPhase: normalized,
            preserveTimestamp: previousPhaseTimestamp
          });
          if (!revertResult.ok) throw new Error(revertResult.error || "Could not revert phase.");
          showToast(`Phase reverted to ${PHASE_LABELS[previousPhase] || previousPhase}.`, "success");
          await refreshActivityLog();
          renderSavedJobs(Array.from(viewState.lastSavedJobsByKey.values()));
        } catch (revertErr) {
          console.error("Could not revert phase change:", revertErr);
          showToast("Could not revert phase.", "error");
        }
      }
    });
    queueActivityPulse(safeJobKey, TIMELINE_SCOPE_PHASE);
    await refreshActivityLog();
  } catch (err) {
    console.error("Could not update phase:", err);
    showToast(err?.message || "Could not update phase.", "error");
  } finally {
    renderSavedJobs(Array.from(viewState.lastSavedJobsByKey.values()));
  }
}

function queueNotesSave(jobKey, value) {
  return queueNotesSaveFromModule(jobKey, value, {
    noteSaveState,
    noteAutosaveMs: NOTE_AUTOSAVE_MS,
    dispatchQueued: safeJobKey => {
      savedDispatch.dispatch({ type: SAVED_ACTIONS.NOTES_QUEUED, payload: { jobKey: safeJobKey } });
    },
    setNoteSaveState,
    flushNotesSave
  });
}

async function flushNotesSave(jobKey, value) {
  return flushNotesSaveFromModule(jobKey, value, {
    noteSaveState,
    currentUser: viewState.currentUser,
    updateJobNotes: (uid, safeJobKey, saveValue) => savedPageService.updateJobNotes(uid, safeJobKey, saveValue),
    setNoteSaveState,
    dispatchSaved: safeJobKey => {
      savedDispatch.dispatch({ type: SAVED_ACTIONS.NOTES_SAVED, payload: { jobKey: safeJobKey } });
    },
    dispatchFailed: (safeJobKey, error) => {
      savedDispatch.dispatch({
        type: SAVED_ACTIONS.NOTES_SAVE_FAILED,
        payload: { jobKey: safeJobKey, error }
      });
    },
    queueActivityPulse,
    timelineScopeNotes: TIMELINE_SCOPE_NOTES,
    flushNotesSave
  });
}

function clearNoteSaveQueues() {
  clearNoteSaveQueuesFromModule(noteSaveState);
}

function setSelectedJobKey(jobKey, options = {}) {
  return savedRenderController.setSelectedJobKey(jobKey, options);
}

function needsInterviewTimestamp(phase) {
  const safe = normalizePhase(phase);
  return safe === "interview_1" || safe === "interview_2";
}

function toPromptLocalDateTime(value) {
  const parsed = parseIsoDate(value) || new Date();
  const yyyy = parsed.getFullYear();
  const mm = String(parsed.getMonth() + 1).padStart(2, "0");
  const dd = String(parsed.getDate()).padStart(2, "0");
  const hh = String(parsed.getHours()).padStart(2, "0");
  const min = String(parsed.getMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
}

function parseScheduledTimestampInput(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) return "";

  const compact = raw.replace(/\s+/g, " ");
  if (/^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$/.test(compact)) {
    const parsed = new Date(compact.replace(" ", "T") + ":00");
    return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
  }
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(compact)) {
    const parsed = new Date(`${compact}:00`);
    return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
  }

  const parsed = new Date(compact);
  return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
}

async function requestInterviewTimestamp(phase, previousTimestamp = "") {
  const phaseLabel = PHASE_LABELS[normalizePhase(phase)] || "Interview";
  const promptDefault = toPromptLocalDateTime(previousTimestamp);
  const raw = await requestTextInputDialog({
    title: `${phaseLabel} time`,
    description: "Enter interview time as YYYY-MM-DD HH:MM.",
    label: `${phaseLabel} time`,
    submitLabel: "Save time",
    defaultValue: promptDefault
  });
  if (raw == null) return "";
  const parsed = parseScheduledTimestampInput(raw);
  if (!parsed) {
    showToast("Invalid interview time. Use YYYY-MM-DD HH:MM.", "error");
    return "";
  }
  return parsed;
}

function toggleDetailsForJob(jobKey) {
  return savedRenderController.toggleDetailsForJob(jobKey);
}

function applyDetailsAccordion() {
  return savedRenderController.applyDetailsAccordion();
}

function setNoteSaveState(jobKey, state) {
  return savedRenderController.setNoteSaveState(jobKey, state);
}

async function hydrateAttachmentLists(jobs) {
  return savedAttachmentsController.hydrateAttachmentLists(jobs);
}

async function uploadAttachments(jobKey, files) {
  return savedAttachmentsController.uploadAttachments(jobKey, files);
}

function renderAttachmentList(jobKey, attachments) {
  return savedAttachmentsController.renderAttachmentList(jobKey, attachments);
}

function bindAttachmentActionButtons() {
  return savedAttachmentsController.bindAttachmentActionButtons();
}

function applyJobDetailsTab(jobKey, tab) {
  return savedRenderController.applyJobDetailsTab(jobKey, tab);
}

function cssEscape(value) {
  if (window.CSS && typeof window.CSS.escape === "function") {
    return window.CSS.escape(value);
  }
  return String(value).replace(/["\\]/g, "\\$&");
}

function setSavedFilter(nextFilter) {
  viewState.activeSavedFilter = isValidSavedFilter(nextFilter) ? nextFilter : DEFAULT_SAVED_FILTER;
  dom.savedCustomFilterBtnEls.forEach(btn => {
    const isActive = String(btn.dataset.savedFilter || "").toLowerCase() === viewState.activeSavedFilter;
    btn.classList.toggle("active", isActive);
  });
}

function setSavedSort(nextSort) {
  viewState.activeSavedSort = isValidSavedSort(nextSort) ? nextSort : SORT_UPDATED;
  dom.savedSortBtnEls.forEach(btn => {
    const isActive = String(btn.dataset.savedSort || "").toLowerCase() === viewState.activeSavedSort;
    btn.classList.toggle("active", isActive);
  });
}

function setSavedSortBarVisible(visible) {
  if (!dom.savedSortBarEl) return;
  dom.savedSortBarEl.classList.toggle("hidden", !visible);
  dom.savedSortBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
}

function setSavedFilterBarVisible(visible) {
  if (!dom.savedCustomFilterBarEl) return;
  dom.savedCustomFilterBarEl.classList.toggle("hidden", !visible);
  dom.savedCustomFilterBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
}

function renderSavedFilterMeta(totalCount, filteredCount) {
  if (!dom.savedCustomFilterCountEl) return;
  const safeTotal = Math.max(0, Number(totalCount) || 0);
  const safeFiltered = Math.max(0, Number(filteredCount) || 0);
  if (safeTotal <= 0) {
    dom.savedCustomFilterCountEl.textContent = "";
    return;
  }
  dom.savedCustomFilterCountEl.textContent = `${safeFiltered}/${safeTotal}`;
}

function renderReminderCounter(allJobs) {
  if (!dom.savedReminderCounterEl) return;
  const rows = Array.isArray(allJobs) ? allJobs : [];
  const soonCount = rows.filter(job => getReminderMeta(job?.reminderAt).isSoon).length;
  dom.savedReminderCounterEl.textContent = soonCount > 0 ? `${soonCount} due soon` : "";
}

function normalizeCustomJobInput(values) {
  return normalizeCustomJobInputFromDomain(values, { customSourceLabel: CUSTOM_SOURCE_LABEL });
}

function toDatetimeLocalValue(value) {
  return toDatetimeLocalValueFromDomain(value, parseIsoDate);
}

function activityTypeLabel(type) {
  return activityTypeLabelFromDomain(type);
}

function formatActivityDetail(entry) {
  return formatActivityDetailFromDomain(entry, {
    normalizePhase,
    phaseLabels: PHASE_LABELS,
    formatPhaseTimestamp
  });
}

function updateCustomJobWarning() {
  return savedCustomJobController.updateCustomJobWarning();
}

function setCustomJobAvailability(enabled) {
  return savedCustomJobController.setCustomJobAvailability(enabled);
}

function setCustomJobPanelOpen(open) {
  return savedCustomJobController.setCustomJobPanelOpen(open);
}

function openCustomJobEditor(jobKey, duplicate) {
  return savedCustomJobController.openCustomJobEditor(jobKey, duplicate);
}

async function createCustomJob() {
  return savedCustomJobController.createCustomJob();
}

function setSourceStatus(text) {
  setStatusText(setText, dom.savedSourceStatusEl, text);
}

function setActivityStatus(text) {
  setElementText(dom.activityPanelStatusEl, text);
}

function setActivityPanelOpen(open, options = {}) {
  return savedActivityController.setActivityPanelOpen(open, options);
}

function buildTimelinePrefsKey(uid) {
  return savedActivityController.buildTimelinePrefsKey(uid);
}

function loadTimelinePreferences(uid) {
  return savedActivityController.loadTimelinePreferences(uid);
}

function setTimelineScope(nextScope) {
  return savedActivityController.setTimelineScope(nextScope);
}

function updateTimelineScopeButtons() {
  return savedActivityController.updateTimelineScopeButtons();
}

function queueActivityPulse(jobKey, category) {
  return savedActivityController.queueActivityPulse(jobKey, category);
}

function clearExpiredPulse() {
  return savedActivityController.clearExpiredPulse();
}

function renderSelectedJobHint() {
  return savedActivityController.renderSelectedJobHint();
}

function renderWorkspaceStats(jobs = null) {
  return savedActivityController.renderWorkspaceStats(jobs);
}

function setBackupButtonsEnabled(enabled) {
  if (dom.exportBackupBtnEl) dom.exportBackupBtnEl.disabled = !enabled;
  if (dom.exportIncludeFilesEl) dom.exportIncludeFilesEl.disabled = !enabled;
  if (dom.importBackupBtnEl) dom.importBackupBtnEl.disabled = !enabled;
  if (dom.globalPhaseOverrideBtnEl) dom.globalPhaseOverrideBtnEl.disabled = !enabled;
  updateGlobalOverrideButton();
}

function updateGlobalOverrideButton() {
  if (!dom.globalPhaseOverrideBtnEl) return;
  dom.globalPhaseOverrideBtnEl.classList.toggle("active", viewState.phaseOverrideArmedGlobal);
  dom.globalPhaseOverrideBtnEl.textContent = viewState.phaseOverrideArmedGlobal
    ? "Override Armed (One Use)"
    : "Override Phase Lock";
}

async function refreshActivityLog() {
  return savedActivityController.refreshActivityLog();
}

function renderTimeline() {
  return savedActivityController.renderTimeline();
}

function getLastJobsUrl() {
  return readSavedLastJobsUrl(JOBS_LAST_URL_KEY, "jobs.html");
}

async function exportBackup() {
  await runExportBackupFromModule({
    currentUser: viewState.currentUser,
    savedPageService,
    includeFiles: Boolean(dom.exportIncludeFilesEl?.checked),
    showToast
  });
}

async function importBackup(file) {
  await runImportBackupFromModule(file, {
    currentUser: viewState.currentUser,
    savedPageService,
    showToast,
    refreshActivityLog
  });
}

export {
  bootSavedPage as boot,
  needsInterviewTimestamp,
  toPromptLocalDateTime,
  parseScheduledTimestampInput,
  buildTimelinePrefsKey
};

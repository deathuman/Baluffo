import { AdminConfig as adminConfig } from "../../shared/config/admin-config.js";
import {
  escapeHtml,
  showToast,
  setText,
  bindUi,
  bindAsyncClick
} from "../../shared/ui/index.js";
import { emitStartupMetric, markFirstInteractive } from "../../shared/app-boot.js";
import {
  sanitizeUrl,
  toContractClass,
  fullCountryName
} from "../../shared/data/index.js";
import {
  toCanonicalCountry as toCanonicalCountryFromDomain,
  normalizeCustomJobInput as normalizeCustomJobInputFromDomain,
  normalizeReminderInput as normalizeReminderInputFromDomain,
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
  renderSavedJobBlockHtml,
  renderActivityEntryHtml,
  parseIsoDate,
  getReminderMeta,
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
import { normalizeToken } from "../../shared/text-utils.js";
import { cacheSavedDom } from "./dom.js";
import { applySavedAdminBridgeState as applySavedAdminBridgeStateFromModule } from "./admin-bridge-state.js";
import { requestConfirmationDialog, requestTextInputDialog } from "../../local-data/profile-name-dialog.js";
import { navigateDesktopPage } from "../../shared/local-data/desktop-client.js";
import { updateCustomJobWarning as updateCustomJobWarningUi } from "./custom-job.js";
import { runExportBackup as runExportBackupFromModule, runImportBackup as runImportBackupFromModule } from "./backup.js";
import {
  buildTimelinePrefsKey as buildTimelinePrefsKeyFromActivity,
  normalizeTimelineScope,
  countRecentActivityEntries,
  setActivityPanelOpen as setActivityPanelOpenFromModule,
  setTimelineScope as setTimelineScopeFromModule,
  updateTimelineScopeButtons as updateTimelineScopeButtonsFromModule,
  queueActivityPulse as queueActivityPulseFromModule,
  clearExpiredPulse as clearExpiredPulseFromModule,
  renderSelectedJobHint as renderSelectedJobHintFromModule,
  renderTimeline as renderTimelineFromModule,
  renderActivityEntries as renderActivityEntriesFromModule,
  shouldPulseEntry as shouldPulseEntryFromModule
} from "./activity.js";
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
  SAVED_FILTER_CUSTOM as _SAVED_FILTER_CUSTOM,
  SAVED_FILTER_IMPORTED as _SAVED_FILTER_IMPORTED,
  SORT_UPDATED,
  SORT_SAVED as _SORT_SAVED,
  SORT_REMINDER as _SORT_REMINDER,
  SORT_PERSONAL as _SORT_PERSONAL,
  isCustomJob,
  filterSavedJobs,
  isValidSavedFilter,
  isValidSavedSort,
  sortSavedJobs,
  REMINDER_SOON_HOURS,
} from "./view-state.js";
import {
  isAllowedAttachment as _isAllowedAttachment,
  formatFileSize as _formatFileSize,
  hydrateAttachmentLists as hydrateAttachmentListsFromModule,
  uploadAttachments as uploadAttachmentsFromModule,
  getAttachmentPreviewUrl as getAttachmentPreviewUrlFromModule,
  clearAttachmentPreviewUrls as clearAttachmentPreviewUrlsFromModule,
  renderAttachmentList as renderAttachmentListFromModule
} from "./attachments.js";
import { createSavedPageState, cacheSavedDomState } from "./runtime/state.js";
import { createSavedAuthController } from "./runtime/auth-controller.js";
import { createSavedStartupMetrics } from "./runtime/effects.js";
import { setStatusText, setElementText } from "./runtime/view.js";
import { bindSavedPageEvents, bindSavedJobsListDelegation } from "./runtime/events.js";
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
const _NOTES_RERENDER_SETTLE_MS = 1200;
const _ALLOWED_EXTENSIONS = new Set(["pdf", "doc", "docx", "txt", "png", "jpg", "jpeg"]);
const ADMIN_BRIDGE_BASE = adminConfig.ADMIN_BRIDGE_BASE || "http://127.0.0.1:8877";

/**
 * @typedef {Object} SavedFilterState
 * @property {string} activeFilter
 * @property {string} activeSort
 * @property {string} timelineScope
 */

/**
 * @typedef {Object} SavedViewState
 * @property {string|null} expandedJobKey
 * @property {string} selectedJobKey
 * @property {boolean} activityPanelOpen
 * @property {boolean} customJobPanelOpen
 */

/**
 * @typedef {Object} SavedAuthViewModel
 * @property {boolean} isSignedIn
 * @property {string} uid
 * @property {string} label
 */

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
const startupMetrics = createSavedStartupMetrics({
  emitMetric: (event, payload) => {
    postJson(ADMIN_BRIDGE_BASE, "/desktop-local-data/startup-metric", { event, payload: payload || {} }).catch(() => {});
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
  const { savedJobsListEl } = dom;
  if (!savedJobsListEl) return;
  savedJobsListEl.innerHTML = `<div class="no-results">${escapeHtml(message)}</div>`;
}

function renderSavedJobs(jobs) {
  const { savedJobsListEl } = dom;
  if (!savedJobsListEl) return;
  const renderContext = captureRenderContext();
  const allJobs = Array.isArray(jobs) ? jobs : [];
  const filteredJobs = sortSavedJobs(
    filterSavedJobs(allJobs, viewState.activeSavedFilter),
    viewState.activeSavedSort,
    { parseIsoDate }
  );
  setSavedFilterBarVisible(allJobs.length > 0 && Boolean(viewState.currentUser));
  setSavedSortBarVisible(allJobs.length > 0 && Boolean(viewState.currentUser));
  renderSavedFilterMeta(allJobs.length, filteredJobs.length);
  renderReminderCounter(allJobs);
  renderWorkspaceStats(allJobs);

  if (!allJobs || allJobs.length === 0) {
    viewState.expandedJobKey = null;
    viewState.selectedJobKey = "";
    renderSelectedJobHint();
    savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs yet.</div>';
    renderTimeline();
    return;
  }
  if (!allJobs.some(job => String(job?.jobKey || "").trim() === viewState.selectedJobKey)) {
    viewState.selectedJobKey = "";
    renderSelectedJobHint();
    updateTimelineScopeButtons();
    if (viewState.timelineScope === TIMELINE_SCOPE_SELECTED) {
      viewState.timelineScope = TIMELINE_SCOPE_ALL;
      updateTimelineScopeButtons();
    }
  }
  if (!filteredJobs.some(job => String(job?.jobKey || "").trim() === viewState.expandedJobKey)) {
    viewState.expandedJobKey = null;
  }

  if (filteredJobs.length === 0) {
    savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs match this filter.</div>';
    renderTimeline();
    return;
  }

  savedJobsListEl.innerHTML = `
    <div class="jobs-table-header">
      <div class="saved-row-header">
        <div class="col-title">Position</div>
        <div class="col-company">Company</div>
        <div class="col-sector">Sector</div>
        <div class="col-city">City</div>
        <div class="col-country">Country</div>
        <div class="col-contract">Contract</div>
        <div class="col-type">Type</div>
        <div class="col-link">Link</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${filteredJobs.map(renderSavedJobBlock).join("")}
    </div>
  `;

  bindAttachmentActionButtons();
  applyDetailsAccordion();
  renderTimeline();
  restoreRenderContext(renderContext);

  hydrateAttachmentLists(filteredJobs).catch(err => {
    console.error("Could not load attachment lists:", err);
  });
}

function captureActiveNotesContext() {
  const active = document.activeElement;
  if (!(active instanceof HTMLTextAreaElement)) return null;
  if (!active.classList.contains("job-notes-input")) return null;
  const jobKey = String(active.dataset.jobKey || "").trim();
  if (!jobKey) return null;
  return {
    jobKey,
    selectionStart: Number(active.selectionStart) || 0,
    selectionEnd: Number(active.selectionEnd) || 0,
    scrollTop: Number(active.scrollTop) || 0,
    pageScrollX: Number(window.scrollX) || 0,
    pageScrollY: Number(window.scrollY) || 0
  };
}

function restoreActiveNotesContext(context, options = {}) {
  const { restorePage = true } = options;
  const { savedJobsListEl } = dom;
  if (!context || !savedJobsListEl) return;
  const selector = `.job-notes-input[data-job-key="${cssEscape(context.jobKey)}"]`;
  const textarea = savedJobsListEl.querySelector(selector);
  if (!(textarea instanceof HTMLTextAreaElement)) return;
  try {
    textarea.focus({ preventScroll: true });
  } catch {
    textarea.focus();
  }
  try {
    textarea.setSelectionRange(context.selectionStart, context.selectionEnd);
  } catch {
    // Ignore selection restore issues.
  }
  textarea.scrollTop = context.scrollTop;
  if (restorePage) {
    window.scrollTo(context.pageScrollX, context.pageScrollY);
  }
}

function captureRenderContext() {
  const { savedJobsListEl } = dom;
  const notesContext = captureActiveNotesContext();
  const anchorKey = String(notesContext?.jobKey || viewState.selectedJobKey || viewState.expandedJobKey || "").trim();
  let anchorTop = NaN;
  let listScrollTop = 0;
  if (savedJobsListEl) {
    listScrollTop = Number(savedJobsListEl.scrollTop) || 0;
    if (anchorKey) {
      const anchorSelector = `.saved-job-block[data-job-key="${cssEscape(anchorKey)}"]`;
      const anchorEl = savedJobsListEl.querySelector(anchorSelector);
      if (anchorEl instanceof HTMLElement) {
        anchorTop = Number(anchorEl.getBoundingClientRect().top);
      }
    }
  }
  return {
    notesContext,
    anchorKey,
    anchorTop,
    listScrollTop,
    pageScrollX: Number(window.scrollX) || 0,
    pageScrollY: Number(window.scrollY) || 0
  };
}

function restoreRenderContext(context) {
  const { savedJobsListEl } = dom;
  if (!context || !savedJobsListEl) return;
  const notesContext = context.notesContext || null;
  if (notesContext) {
    restoreActiveNotesContext(notesContext, { restorePage: false });
  }

  savedJobsListEl.scrollTop = Number(context.listScrollTop) || 0;

  const anchorKey = String(context.anchorKey || "").trim();
  if (anchorKey) {
    const anchorSelector = `.saved-job-block[data-job-key="${cssEscape(anchorKey)}"]`;
    const anchorEl = savedJobsListEl.querySelector(anchorSelector);
    if (anchorEl instanceof HTMLElement) {
      const delta = computeAnchorScrollDelta(context.anchorTop, anchorEl.getBoundingClientRect().top);
      if (Math.abs(delta) > 1) {
        window.scrollBy(0, delta);
      }
    }
  }

  if (!notesContext) {
    window.scrollTo(Number(context.pageScrollX) || 0, Number(context.pageScrollY) || 0);
  }
}

function renderSavedJobBlock(job) {
  return renderSavedJobBlockHtml(job, {
    isCustomJob,
    customSourceLabel: CUSTOM_SOURCE_LABEL,
    normalizeSavedSector,
    fullCountryName,
    sanitizeUrl,
    toContractClass,
    normalizePhase,
    expandedJobKey: viewState.expandedJobKey,
    selectedJobKey: viewState.selectedJobKey,
    getJobDetailsTab,
    renderDetailsSummary,
    getReminderMeta: reminderAt => getReminderMeta(reminderAt, { reminderSoonHours: REMINDER_SOON_HOURS }),
    renderMissingInfoChips,
    renderUpdatedHint,
    getJobHistoryEntries: jobKey => getJobHistoryEntries(jobKey, {
      cachedActivityEntries: viewState.cachedActivityEntries,
      activityTypeLabel,
      formatPhaseTimestamp,
      formatActivityDetail
    }),
    renderWebIcon,
    renderPhaseBar: (jobKey, activePhase, phaseTimestamps, savedAt) => renderPhaseBar(
      jobKey,
      activePhase,
      phaseTimestamps,
      savedAt,
      {
        phaseOptions: PHASE_OPTIONS,
        phaseLabels: PHASE_LABELS,
        canTransition,
        currentUser: viewState.currentUser,
        phaseOverrideArmedGlobal: viewState.phaseOverrideArmedGlobal
      }
    ),
    currentUser: viewState.currentUser,
    maxAttachmentsPerJob: MAX_ATTACHMENTS_PER_JOB,
    maxAttachmentBytes: MAX_ATTACHMENT_BYTES
  });
}

function normalizeSavedSector(job) {
  const raw = String(job?.sector || "").trim();
  const lower = raw.toLowerCase();
  if (lower === "game" || lower === "game company" || lower === "gaming") return "Game";
  if (lower === "tech" || lower === "tech company" || lower === "technology") return "Tech";

  const ct = normalizeToken(job?.companyType);
  if (ct === "game" || ct === "game company") return "Game";
  if (ct === "tech" || ct === "tech company") return "Tech";
  return raw || "Tech";
}

function renderMissingInfoChips(job) {
  if (!isCustomJob(job)) return "";
  const chips = [];
  if (!sanitizeUrl(job.jobLink || "")) chips.push("No link");
  if (!String(job.city || "").trim()) chips.push("No city");
  if (!String(job.contractType || "").trim() || String(job.contractType || "").toLowerCase() === "unknown") chips.push("No contract");
  if (chips.length === 0) return "";
  return chips.map(label => `<span class="saved-missing-chip">${escapeHtml(label)}</span>`).join("");
}

function renderUpdatedHint(job) {
  if (!isCustomJob(job)) return "";
  const label = String(job?.updatedBy || "").trim();
  if (!label) return "";
  const time = formatRelativeTime(job.updatedAt);
  if (label && time) {
    return `<div class="saved-updated-hint">Updated: ${escapeHtml(label)} · ${escapeHtml(time)}</div>`;
  }
  return `<div class="saved-updated-hint">Updated: ${escapeHtml(label)}</div>`;
}

function getJobDetailsTab(jobKey) {
  const key = String(jobKey || "");
  return viewState.jobDetailTabByKey.get(key) || "notes";
}

function setJobDetailsTab(jobKey, tab) {
  const safeTab = tab === "attachments" || tab === "history" ? tab : "notes";
  viewState.jobDetailTabByKey.set(String(jobKey || ""), safeTab);
}

function normalizePhase(phase) {
  const raw = String(phase || "").toLowerCase().trim();
  if (raw === "bookmarked") return "bookmark";
  return PHASE_OPTIONS.includes(raw) ? raw : "bookmark";
}

function canTransition(currentPhase, nextPhase) {
  const transitionResult = savedPageService.canTransitionPhase(currentPhase, nextPhase);
  if (typeof transitionResult === "boolean") {
    return transitionResult;
  }
  const current = normalizePhase(currentPhase);
  const next = normalizePhase(nextPhase);
  if (current === next) return true;
  if (current === "rejected") return false;
  if (next === "rejected") return true;
  const currentIdx = PHASE_OPTIONS.indexOf(current);
  const nextIdx = PHASE_OPTIONS.indexOf(next);
  return currentIdx >= 0 && nextIdx >= 0 && nextIdx === currentIdx + 1;
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
  const { savedJobsListEl } = dom;
  const { rerenderTimeline = true } = options;
  const nextKey = String(jobKey || "").trim();
  if (nextKey === viewState.selectedJobKey) return;
  viewState.selectedJobKey = nextKey;
  renderSelectedJobHint();
  updateTimelineScopeButtons();
  if (viewState.timelineScope === TIMELINE_SCOPE_SELECTED && !viewState.selectedJobKey) {
    viewState.timelineScope = TIMELINE_SCOPE_ALL;
    updateTimelineScopeButtons();
  }
  if (rerenderTimeline) {
    renderTimeline();
  }
  if (savedJobsListEl) {
    savedJobsListEl.querySelectorAll(".saved-job-block").forEach(block => {
      block.classList.toggle("selected", String(block.dataset.jobKey || "") === viewState.selectedJobKey);
    });
  }
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
  if (!jobKey) return;
  setSelectedJobKey(jobKey, { rerenderTimeline: false });
  const nextKey = viewState.expandedJobKey === jobKey ? null : jobKey;
  if (nextKey && !viewState.jobDetailTabByKey.has(nextKey)) {
    viewState.jobDetailTabByKey.set(nextKey, "notes");
  }
  viewState.expandedJobKey = nextKey;
  applyDetailsAccordion();
}

function applyDetailsAccordion() {
  const { savedJobsListEl } = dom;
  if (!savedJobsListEl) return;
  savedJobsListEl.querySelectorAll(".saved-job-block").forEach(block => {
    const key = block.dataset.jobKey || "";
    const expanded = Boolean(viewState.expandedJobKey) && key === viewState.expandedJobKey;
    const details = block.querySelector(".saved-details-section");
    const toggle = block.querySelector(".details-toggle-btn");
    const arrow = block.querySelector(".details-toggle-arrow");
    if (details) {
      details.classList.toggle("collapsed", !expanded);
      details.setAttribute("aria-hidden", expanded ? "false" : "true");
    }
    if (toggle) {
      toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
      toggle.setAttribute("aria-label", `${expanded ? "Collapse" : "Expand"} notes, attachments, and history`);
    }
    if (arrow) {
      arrow.textContent = expanded ? "v" : ">";
    }
  });
}

function setNoteSaveState(jobKey, state) {
  const { savedJobsListEl } = dom;
  const el = savedJobsListEl?.querySelector(`.note-save-state[data-job-key="${cssEscape(jobKey)}"]`);
  if (!el) return;
  if (state === "saving") {
    el.textContent = "Saving...";
    el.classList.add("saving");
    el.classList.remove("error");
    return;
  }
  if (state === "error") {
    el.textContent = "Error";
    el.classList.remove("saving");
    el.classList.add("error");
    return;
  }
  el.textContent = "Saved";
  el.classList.remove("saving");
  el.classList.remove("error");
}

async function hydrateAttachmentLists(jobs) {
  return hydrateAttachmentListsFromModule(jobs, {
    currentUser: viewState.currentUser,
    listAttachmentsForJob: (uid, jobKey) => savedPageService.listAttachmentsForJob(uid, jobKey),
    renderAttachmentList
  });
}

async function uploadAttachments(jobKey, files) {
  return uploadAttachmentsFromModule(jobKey, files, {
    currentUser: viewState.currentUser,
    listAttachmentsForJob: (uid, safeJobKey) => savedPageService.listAttachmentsForJob(uid, safeJobKey),
    maxAttachmentsPerJob: MAX_ATTACHMENTS_PER_JOB,
    maxAttachmentBytes: MAX_ATTACHMENT_BYTES,
    addAttachmentForJob: (uid, safeJobKey, meta, file) => savedPageService.addAttachmentForJob(uid, safeJobKey, meta, file),
    renderAttachmentList,
    showToast,
    dispatchAttachmentMutated: safeJobKey => {
      savedDispatch.dispatch({ type: SAVED_ACTIONS.ATTACHMENT_MUTATED, payload: { jobKey: safeJobKey } });
    },
    queueActivityPulse,
    timelineScopeAttachments: TIMELINE_SCOPE_ATTACHMENTS
  });
}

async function openAttachment(jobKey, attachmentId) {
  if (!viewState.currentUser) return;
  try {
    const directUrl = savedPageService.getAttachmentOpenUrl(viewState.currentUser.uid, jobKey, attachmentId);
    if (directUrl) {
      window.open(directUrl, "_blank", "noopener,noreferrer");
      return;
    }
    const blobResult = await savedPageService.getAttachmentBlob(viewState.currentUser.uid, jobKey, attachmentId);
    if (!blobResult.ok) throw new Error(blobResult.error || "Could not read attachment.");
    const blob = blobResult.data?.blob;
    if (!blob) {
      showToast("Attachment data not available.", "error");
      return;
    }
    const url = URL.createObjectURL(blob);
    window.open(url, "_blank", "noopener,noreferrer");
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
  } catch (err) {
    console.error("Could not open attachment:", err);
    showToast("Could not open attachment.", "error");
  }
}

async function downloadAttachment(jobKey, attachmentId, filename) {
  if (!viewState.currentUser) return;
  try {
    const directUrl = savedPageService.getAttachmentDownloadUrl(viewState.currentUser.uid, jobKey, attachmentId);
    if (directUrl) {
      window.open(directUrl, "_blank", "noopener,noreferrer");
      return;
    }

    const blobResult = await savedPageService.getAttachmentBlob(viewState.currentUser.uid, jobKey, attachmentId);
    if (!blobResult.ok) throw new Error(blobResult.error || "Could not read attachment.");
    const blob = blobResult.data?.blob;
    if (!blob) {
      showToast("Attachment data not available.", "error");
      return;
    }
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = blobResult.data?.filename || filename || "attachment";
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (err) {
    console.error("Could not download attachment:", err);
    showToast("Could not download attachment.", "error");
  }
}

async function deleteAttachment(jobKey, attachmentId) {
  if (!viewState.currentUser) return;
  try {
    const deleteResult = await savedPageService.deleteAttachmentForJob(viewState.currentUser.uid, jobKey, attachmentId);
    if (!deleteResult.ok) throw new Error(deleteResult.error || "Could not delete attachment.");
    const nextResult = await savedPageService.listAttachmentsForJob(viewState.currentUser.uid, jobKey);
    if (!nextResult.ok) throw new Error(nextResult.error || "Could not list attachments.");
    renderAttachmentList(jobKey, nextResult.data);
    showToast("Attachment removed.", "success");
    savedDispatch.dispatch({ type: SAVED_ACTIONS.ATTACHMENT_MUTATED, payload: { jobKey } });
    queueActivityPulse(jobKey, TIMELINE_SCOPE_ATTACHMENTS);
  } catch (err) {
    console.error("Could not delete attachment:", err);
    showToast("Could not delete attachment.", "error");
  }
}

function renderAttachmentList(jobKey, attachments) {
  return renderAttachmentListFromModule(jobKey, attachments, {
    savedJobsListEl: dom.savedJobsListEl,
    cssEscape,
    clearAttachmentPreviewUrls,
    getAttachmentPreviewUrl,
    escapeHtml,
    bindAttachmentActionButtons
  });
}

function bindAttachmentActionButtons() {
  const { savedJobsListEl } = dom;
  if (!savedJobsListEl) return;

  savedJobsListEl.querySelectorAll(".att-open-btn").forEach(btn => {
    btn.onclick = async () => {
      const jobKey = btn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      await openAttachment(jobKey, btn.dataset.attachmentId || "");
    };
  });

  savedJobsListEl.querySelectorAll(".att-download-btn").forEach(btn => {
    btn.onclick = async () => {
      setSelectedJobKey(btn.dataset.jobKey || "", { rerenderTimeline: false });
      await downloadAttachment(
        btn.dataset.jobKey || "",
        btn.dataset.attachmentId || "",
        btn.dataset.fileName || "attachment"
      );
    };
  });

  savedJobsListEl.querySelectorAll(".att-delete-btn").forEach(btn => {
    btn.onclick = async () => {
      const jobKey = btn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      await deleteAttachment(jobKey, btn.dataset.attachmentId || "");
    };
  });
}

function applyJobDetailsTab(jobKey, tab) {
  const { savedJobsListEl } = dom;
  if (!savedJobsListEl || !jobKey) return;
  const safeTab = tab === "attachments" || tab === "history" ? tab : "notes";
  const block = savedJobsListEl.querySelector(`.saved-job-block[data-job-key="${cssEscape(jobKey)}"]`);
  if (!(block instanceof HTMLElement)) return;
  const buttons = Array.from(block.querySelectorAll(".saved-details-tab-btn"));
  const panels = Array.from(block.querySelectorAll(".saved-details-panel"));
  buttons.forEach(btn => {
    const active = String(btn.dataset.detailsTab || "") === safeTab;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-selected", active ? "true" : "false");
  });
  panels.forEach(panel => {
    const active = String(panel.getAttribute("data-tab-panel") || "") === safeTab;
    panel.classList.toggle("hidden", !active);
  });
}

function getAttachmentPreviewUrl(jobKey, attachment) {
  return getAttachmentPreviewUrlFromModule(jobKey, attachment, attachmentPreviewUrls);
}

function clearAttachmentPreviewUrls(jobKey) {
  clearAttachmentPreviewUrlsFromModule(jobKey, attachmentPreviewUrls);
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

function _toCanonicalCountry(value) {
  return toCanonicalCountryFromDomain(value);
}

function normalizeCustomJobInput(values) {
  return normalizeCustomJobInputFromDomain(values, { customSourceLabel: CUSTOM_SOURCE_LABEL });
}

function _normalizeReminderInput(value) {
  return normalizeReminderInputFromDomain(value);
}

function toDatetimeLocalValue(value) {
  return toDatetimeLocalValueFromDomain(value, parseIsoDate);
}

function resetCustomJobForm() {
  viewState.customJobMode = "create";
  viewState.customJobTargetKey = "";
  dom.customJobFormEl?.reset();
  if (dom.customJobWorkTypeEl) dom.customJobWorkTypeEl.value = "";
  if (dom.customJobContractTypeEl) dom.customJobContractTypeEl.value = "";
  if (dom.customJobSectorEl) dom.customJobSectorEl.value = "";
  if (dom.customJobReminderEl) dom.customJobReminderEl.value = "";
  if (dom.customJobPanelTitleEl) dom.customJobPanelTitleEl.textContent = "Add Custom Job";
  if (dom.customJobPanelHintEl) dom.customJobPanelHintEl.textContent = "Required: Title and Company. Job link is optional.";
  if (dom.customJobSaveBtnEl) dom.customJobSaveBtnEl.textContent = "Save Custom Job";
  updateCustomJobWarning();
}

function updateCustomJobWarning() {
  updateCustomJobWarningUi(dom.customJobLinkEl, dom.customJobLinkWarningEl);
}

function setCustomJobAvailability(enabled) {
  if (!dom.addCustomJobBtnEl) return;
  dom.addCustomJobBtnEl.disabled = !enabled;
}

function setCustomJobPanelOpen(open) {
  viewState.customJobPanelOpen = Boolean(open);
  if (!dom.customJobPanelEl) return;
  dom.customJobPanelEl.classList.toggle("hidden", !viewState.customJobPanelOpen);
  dom.customJobPanelEl.setAttribute("aria-hidden", viewState.customJobPanelOpen ? "false" : "true");
  if (dom.addCustomJobBtnEl) {
    dom.addCustomJobBtnEl.classList.toggle("active", viewState.customJobPanelOpen);
    dom.addCustomJobBtnEl.textContent = viewState.customJobPanelOpen ? "Close Custom Job Form" : "+ Add Custom Job";
  }
  if (!viewState.customJobPanelOpen) {
    resetCustomJobForm();
  } else {
    updateCustomJobWarning();
  }
}

function openCustomJobEditor(jobKey, duplicate) {
  const row = viewState.lastSavedJobsByKey.get(String(jobKey || ""));
  if (!row || !isCustomJob(row)) {
    showToast("Custom job not found.", "error");
    return;
  }
  viewState.customJobMode = duplicate ? "duplicate" : "edit";
  viewState.customJobTargetKey = duplicate ? "" : String(row.jobKey || "");
  if (dom.customJobTitleEl) dom.customJobTitleEl.value = row.title || "";
  if (dom.customJobCompanyEl) dom.customJobCompanyEl.value = row.company || "";
  if (dom.customJobCityEl) dom.customJobCityEl.value = row.city || "";
  if (dom.customJobCountryEl) dom.customJobCountryEl.value = row.country || "";
  if (dom.customJobWorkTypeEl) dom.customJobWorkTypeEl.value = row.workType || "";
  if (dom.customJobContractTypeEl) dom.customJobContractTypeEl.value = row.contractType || "";
  if (dom.customJobSectorEl) dom.customJobSectorEl.value = row.sector || "";
  if (dom.customJobProfessionEl) dom.customJobProfessionEl.value = row.profession || "";
  if (dom.customJobLinkEl) dom.customJobLinkEl.value = row.jobLink || "";
  if (dom.customJobNotesEl) dom.customJobNotesEl.value = row.notes || "";
  if (dom.customJobReminderEl) dom.customJobReminderEl.value = toDatetimeLocalValue(row.reminderAt);
  if (dom.customJobPanelTitleEl) {
    dom.customJobPanelTitleEl.textContent = duplicate ? "Duplicate Custom Job" : "Edit Custom Job";
  }
  if (dom.customJobPanelHintEl) {
    dom.customJobPanelHintEl.textContent = duplicate
      ? "Create a new custom entry using this job as a template."
      : "Update this custom job while keeping its history and status.";
  }
  if (dom.customJobSaveBtnEl) {
    dom.customJobSaveBtnEl.textContent = duplicate ? "Save Duplicate" : "Update Custom Job";
  }
  setCustomJobPanelOpen(true);
  dom.customJobTitleEl?.focus();
  updateCustomJobWarning();
}

async function createCustomJob() {
  if (!savedPageService.isAvailable() || !viewState.currentUser) {
    showToast("Sign in required.", "error");
    return;
  }
  const normalized = normalizeCustomJobInput({
    title: dom.customJobTitleEl?.value,
    company: dom.customJobCompanyEl?.value,
    city: dom.customJobCityEl?.value,
    country: dom.customJobCountryEl?.value,
    workType: dom.customJobWorkTypeEl?.value,
    contractType: dom.customJobContractTypeEl?.value,
    sector: dom.customJobSectorEl?.value,
    profession: dom.customJobProfessionEl?.value,
    jobLink: dom.customJobLinkEl?.value,
    notes: dom.customJobNotesEl?.value,
    reminderAt: dom.customJobReminderEl?.value
  });

  if (!normalized.title || !normalized.company) {
    showToast("Title and Company are required.", "error");
    return;
  }

  try {
    let eventType = "custom_job_created";
    let message = "Custom job saved.";
    if (viewState.customJobMode === "edit") {
      normalized.jobKey = viewState.customJobTargetKey;
      normalized.updatedBy = "manual_edit";
      eventType = "custom_job_updated";
      message = "Custom job updated.";
    } else if (viewState.customJobMode === "duplicate") {
      normalized.updatedBy = "manual_duplicate";
      normalized.keySalt = String(Date.now());
      eventType = "custom_job_duplicated";
      message = "Custom job duplicated.";
    } else {
      normalized.updatedBy = "manual_create";
    }
    const saveResult = await savedPageService.saveJobForUser(viewState.currentUser.uid, normalized, { eventType });
    if (!saveResult.ok) throw new Error(saveResult.error || "Could not save custom job.");
    showToast(message, "success");
    savedDispatch.dispatch({
      type: SAVED_ACTIONS.CUSTOM_JOB_MUTATED,
      payload: { at: new Date().toISOString() }
    });
    setCustomJobPanelOpen(false);
    queueActivityPulse(String(saveResult?.data?.jobKey || normalized.jobKey || viewState.customJobTargetKey || ""), TIMELINE_SCOPE_ALL);
    await refreshActivityLog();
  } catch (err) {
    console.error("Could not save custom job:", err);
    showToast("Could not save custom job.", "error");
  }
}

function setSourceStatus(text) {
  setStatusText(setText, dom.savedSourceStatusEl, text);
}

function setActivityStatus(text) {
  setElementText(dom.activityPanelStatusEl, text);
}

function setActivityPanelOpen(open, options = {}) {
  return setActivityPanelOpenFromModule(open, {
    activityPanelEl: dom.activityPanelEl,
    historyPanelToggleBtnEl: dom.historyPanelToggleBtnEl,
    persist: options.persist,
    currentUser: viewState.currentUser,
    persistTimelinePreferences,
    setActivityPanelOpenState: value => {
      viewState.activityPanelOpen = value;
    }
  });
}

function buildTimelinePrefsKey(uid) {
  return buildTimelinePrefsKeyFromActivity(TIMELINE_PREF_PREFIX, uid);
}

function loadTimelinePreferences(uid) {
  return loadSavedTimelinePreferences(
    TIMELINE_PREF_PREFIX,
    uid,
    normalizeTimelineScope,
    TIMELINE_SCOPE_ALL
  );
}

function persistTimelinePreferences(uid) {
  persistSavedTimelinePreferences(TIMELINE_PREF_PREFIX, uid, normalizeTimelineScope, {
    visible: Boolean(viewState.activityPanelOpen),
    scope: normalizeTimelineScope(viewState.timelineScope)
  });
}

function setTimelineScope(nextScope) {
  return setTimelineScopeFromModule(nextScope, {
    selectedJobKey: viewState.selectedJobKey,
    persistTimelinePreferences,
    currentUser: viewState.currentUser,
    updateTimelineScopeState: value => {
      viewState.timelineScope = value;
    },
    updateTimelineScopeButtons
  });
}

function updateTimelineScopeButtons() {
  updateTimelineScopeButtonsFromModule(dom.activityScopeBtnEls, viewState.timelineScope, viewState.selectedJobKey);
}

function queueActivityPulse(jobKey, category) {
  viewState.lastActivityPulse = queueActivityPulseFromModule(jobKey, category);
}

function clearExpiredPulse() {
  viewState.lastActivityPulse = clearExpiredPulseFromModule(viewState.lastActivityPulse);
}

function renderSelectedJobHint() {
  renderSelectedJobHintFromModule(dom.activitySelectedJobEl, viewState.selectedJobKey, viewState.lastSavedJobsByKey);
}

function renderWorkspaceStats(jobs = null) {
  const rows = Array.isArray(jobs) ? jobs : Array.from(viewState.lastSavedJobsByKey.values());
  if (dom.savedMetricTotalEl) dom.savedMetricTotalEl.textContent = String(rows.length);
  if (dom.savedMetricRemindersEl) {
    const dueSoon = rows.filter(job => getReminderMeta(job?.reminderAt).isSoon).length;
    dom.savedMetricRemindersEl.textContent = String(dueSoon);
  }
  if (dom.savedMetricActivityEl) {
    dom.savedMetricActivityEl.textContent = String(countRecentActivityEntries(viewState.cachedActivityEntries, 24));
  }
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
  if (!dom.activityPanelBodyEl) return;
  if (!viewState.currentUser || !savedPageService.isAvailable()) {
    setActivityStatus("Sign in to view history.");
    renderTimeline();
    renderWorkspaceStats();
    return;
  }

  setActivityStatus("Loading activity...");
  try {
    const entriesResult = await savedPageService.listActivityForUser(viewState.currentUser.uid, 400);
    if (!entriesResult.ok) throw new Error(entriesResult.error || "Could not load history.");
    const entries = Array.isArray(entriesResult.data) ? entriesResult.data : [];
    viewState.cachedActivityEntries = entries;
    renderTimeline();
    renderWorkspaceStats();
  } catch (err) {
    console.error("Could not load activity log:", err);
    viewState.cachedActivityEntries = [];
    setActivityStatus("Could not load history.");
    renderTimeline();
    renderWorkspaceStats();
  }
}

function renderTimeline() {
  renderTimelineFromModule({
    cachedActivityEntries: viewState.cachedActivityEntries,
    timelineScope: viewState.timelineScope,
    selectedJobKey: viewState.selectedJobKey,
    currentUser: viewState.currentUser,
    setActivityStatus,
    renderActivityEntries
  });
}

function _shouldPulseEntry(entry) {
  clearExpiredPulse();
  return shouldPulseEntryFromModule(entry, viewState.lastActivityPulse);
}

function renderActivityEntries(entries) {
  renderActivityEntriesFromModule(entries, {
    activityPanelBodyEl: dom.activityPanelBodyEl,
    lastActivityPulse: viewState.lastActivityPulse,
    renderActivityEntry,
    renderTimeline,
    clearExpiredPulseState: clearExpiredPulse,
    activityHighlightMs: ACTIVITY_HIGHLIGHT_MS
  });
}

function renderActivityEntry(entry) {
  return renderActivityEntryHtml(entry, {
    formatPhaseTimestamp,
    lastSavedJobsByKey: viewState.lastSavedJobsByKey,
    formatActivityDetail,
    activityTypeLabel
  });
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

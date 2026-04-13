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
import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";
import { set as stateHubSet } from "../../shared/state-hub.js";
import { fetchJson, postJson } from "../../shared/api-client.js";
import { createAdminBridgeButtonWatcher } from "../../shared/admin-bridge-button.js";
import { createAuthReadyPoller } from "../../shared/auth-ready-poll.js";
import { normalizeToken } from "../../shared/text-utils.js";
import { cacheSavedDom } from "./dom.js";
import { setSavedAuthControlsReady, setSavedAuthStatus, toggleSavedAuthButtons } from "./auth.js";
import { requestConfirmationDialog, requestTextInputDialog } from "../../local-data/profile-name-dialog.js";
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
import { createSavedPageState } from "./runtime/state.js";
import { createSavedStartupMetrics } from "./runtime/effects.js";
import { setStatusText, setElementText } from "./runtime/view.js";
import { runSavedAction as _runSavedAction } from "./runtime/actions.js";
import { bindDocumentKeydown as _bindDocumentKeydown } from "./runtime/events.js";
let savedJobsListEl;
let savedSourceStatusEl;
let savedAuthStatusEl;
let savedAuthStatusHintEl;
let savedAuthAvatarEl;
let signInBtnEl;
let signOutBtnEl;
let jobsPageBtnEl;
let adminPageBtnEl;
let addCustomJobBtnEl;
let customJobPanelEl;
let customJobFormEl;
let customJobTitleEl;
let customJobCompanyEl;
let customJobCityEl;
let customJobCountryEl;
let customJobWorkTypeEl;
let customJobContractTypeEl;
let customJobSectorEl;
let customJobProfessionEl;
let customJobLinkEl;
let customJobNotesEl;
let customJobReminderEl;
let customJobLinkWarningEl;
let customJobCancelBtnEl;
let customJobPanelTitleEl;
let customJobPanelHintEl;
let customJobSaveBtnEl;
let savedCustomFilterBarEl;
let savedCustomFilterCountEl;
let savedCustomFilterBtnEls = [];
let savedSortBarEl;
let savedSortBtnEls = [];
let savedReminderCounterEl;
let historyPanelToggleBtnEl;
let savedWorkspaceLayoutEl; // eslint-disable-line no-unused-vars -- used in cacheDom destructuring
let savedMetricTotalEl;
let savedMetricRemindersEl;
let savedMetricActivityEl;
let exportBackupBtnEl;
let exportIncludeFilesEl;
let importBackupBtnEl;
let importBackupInputEl;
let globalPhaseOverrideBtnEl;
let activityPanelEl;
let activityPanelBodyEl;
let activityPanelStatusEl;
let activityRefreshBtnEl;
let activityScopeBtnEls = [];
let activitySelectedJobEl;

let currentUser = null;
let unsubscribeSavedJobs = () => {};
let expandedJobKey = null;
let phaseOverrideArmedGlobal = false;
let activityPanelOpen = false;
let customJobPanelOpen = false;
let customJobMode = "create";
let customJobTargetKey = "";
let activeSavedSort = "updated";
let jobDetailTabByKey = new Map();
let cachedActivityEntries = [];
let lastSavedJobsByKey = new Map();
let selectedJobKey = "";
let timelineScope = "all";
let lastActivityPulse = null;
const savedAuthReadyPoller = createAuthReadyPoller({
  isReady: () => savedPageService.isAvailable() && isSavedApiReady(),
  onReady: () => initSavedJobsPage()
});
let savedAuthListenerBound = false;
let savedInteractiveMetricSent = false; // eslint-disable-line no-unused-vars -- used in markSavedFirstInteractive
let adminBridgeButtonState = "checking";
let adminBridgeWatcher = null;
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";
const TIMELINE_PREF_PREFIX = "baluffo_saved_timeline_prefs";
const CUSTOM_SOURCE_LABEL = "Custom";
const DEFAULT_SAVED_FILTER = SAVED_FILTER_ALL;
const ACTIVITY_HIGHLIGHT_MS = 2600;
let activeSavedFilter = DEFAULT_SAVED_FILTER;
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

const pageState = createSavedPageState();
const savedDispatch = createSavedDispatcher();
const noteSaveState = pageState.noteSaveState;
const attachmentPreviewUrls = pageState.attachmentPreviewUrls;
const startupMetrics = createSavedStartupMetrics({
  emitMetric: (event, payload) => {
    postJson(ADMIN_BRIDGE_BASE, "/desktop-local-data/startup-metric", { event, payload: payload || {} }).catch(() => {});
  }
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

/**
 * Applies page-specific presentation for admin bridge button state.
 * @param {Object} params
 * @param {HTMLElement} params.buttonEl
 * @param {string} params.state - "online", "offline", or "checking"
 * @param {string} params.label
 * @param {string} params.title
 * @param {number} params.activeAlerts
 */
function applySavedAdminBridgeState({ buttonEl, state, label, title }) {
  if (!buttonEl) return;
  adminBridgeButtonState = state;
  buttonEl.dataset.bridgeState = state;
  buttonEl.classList.remove("online", "offline", "checking");
  buttonEl.classList.add(state);
  buttonEl.textContent = label || "Admin Checking...";
  buttonEl.title = title || label || "Checking admin bridge status";
  const enabled = state === "online";
  buttonEl.disabled = !enabled;
  buttonEl.setAttribute("aria-disabled", enabled ? "false" : "true");
}

function startAdminBridgeButtonWatch() {
  if (!adminBridgeWatcher) return;
  adminBridgeWatcher.startAdminBridgeButtonWatch();
}

function bootSavedPage() {
  cacheDom();
  adminBridgeWatcher = createAdminBridgeButtonWatcher({
    buttonEl: adminPageBtnEl,
    baseUrl: adminConfig.ADMIN_BRIDGE_BASE,
    fetchJson,
    applyState: applySavedAdminBridgeState
  });
  startAdminBridgeButtonWatch();
  setupSavedJobsListDelegation();
  bindEvents();
  initSavedJobsPage();
}

function emitSavedStartupMetric(event, payload = {}) {
  emitStartupMetric(startupMetrics, event, payload);
}

function markSavedFirstInteractive(reason) {
  markFirstInteractive(startupMetrics, reason);
  savedInteractiveMetricSent = true;
}

function cacheDom() {
  ({
    savedJobsListEl,
    savedSourceStatusEl,
    savedAuthStatusEl,
    savedAuthStatusHintEl,
    savedAuthAvatarEl,
    signInBtnEl,
    signOutBtnEl,
    jobsPageBtnEl,
    adminPageBtnEl,
    addCustomJobBtnEl,
    customJobPanelEl,
    customJobFormEl,
    customJobTitleEl,
    customJobCompanyEl,
    customJobCityEl,
    customJobCountryEl,
    customJobWorkTypeEl,
    customJobContractTypeEl,
    customJobSectorEl,
    customJobProfessionEl,
    customJobLinkEl,
    customJobNotesEl,
    customJobReminderEl,
    customJobLinkWarningEl,
    customJobCancelBtnEl,
    customJobPanelTitleEl,
    customJobPanelHintEl,
    customJobSaveBtnEl,
    savedCustomFilterBarEl,
    savedCustomFilterCountEl,
    savedCustomFilterBtnEls,
    savedSortBarEl,
    savedSortBtnEls,
    savedReminderCounterEl,
    historyPanelToggleBtnEl,
    savedWorkspaceLayoutEl,
    savedMetricTotalEl,
    savedMetricRemindersEl,
    savedMetricActivityEl,
    exportBackupBtnEl,
    exportIncludeFilesEl,
    importBackupBtnEl,
    importBackupInputEl,
    globalPhaseOverrideBtnEl,
    activityPanelEl,
    activityPanelBodyEl,
    activityPanelStatusEl,
    activityRefreshBtnEl,
    activityScopeBtnEls,
    activitySelectedJobEl
  } = cacheSavedDom(document));
}

function bindEvents() {

  bindUi(jobsPageBtnEl, "click", () => {
    const target = getLastJobsUrl();
    window.location.href = target;
  });
  bindUi(adminPageBtnEl, "click", () => {
    if (adminBridgeButtonState !== "online") {
      showToast("Admin bridge is offline.", "info");
      return;
    }
    window.location.href = "admin.html";
  });
  bindUi(addCustomJobBtnEl, "click", () => {
    if (!currentUser) {
      showToast("Sign in to add custom jobs.", "info");
      return;
    }
    setCustomJobPanelOpen(!customJobPanelOpen);
    if (customJobPanelOpen) customJobTitleEl?.focus();
  });
  bindUi(customJobCancelBtnEl, "click", () => {
    setCustomJobPanelOpen(false);
  });

  if (customJobFormEl) {
    customJobFormEl.addEventListener("submit", async event => {
      event.preventDefault();
      await createCustomJob();
    });
  }

  if (customJobLinkEl) {
    customJobLinkEl.addEventListener("input", updateCustomJobWarning);
  }

  savedCustomFilterBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const nextFilter = String(btn.dataset.savedFilter || DEFAULT_SAVED_FILTER).toLowerCase();
      setSavedFilter(nextFilter);
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  });

  savedSortBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const sortKey = String(btn.dataset.savedSort || SORT_UPDATED).toLowerCase();
      setSavedSort(sortKey);
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  });

  bindUi(historyPanelToggleBtnEl, "click", () => {
    setActivityPanelOpen(!activityPanelOpen);
  });
  bindAsyncClick(activityRefreshBtnEl, refreshActivityLog);
  bindAsyncClick(signInBtnEl, signInUser);
  bindAsyncClick(signOutBtnEl, signOutUser);
  bindAsyncClick(exportBackupBtnEl, exportBackup);

  if (importBackupBtnEl && importBackupInputEl) {
    importBackupBtnEl.addEventListener("click", () => {
      importBackupInputEl.click();
    });
    importBackupInputEl.addEventListener("change", async () => {
      const file = importBackupInputEl.files && importBackupInputEl.files[0];
      if (!file) return;
      await importBackup(file);
      importBackupInputEl.value = "";
    });
  }

  if (globalPhaseOverrideBtnEl) {
    globalPhaseOverrideBtnEl.addEventListener("click", () => {
      if (!currentUser) return;
      phaseOverrideArmedGlobal = !phaseOverrideArmedGlobal;
      updateGlobalOverrideButton();
      showToast(
        phaseOverrideArmedGlobal
          ? "Global override armed for one locked phase change."
          : "Global override cancelled.",
        "info"
      );
      renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
    });
  }

  activityScopeBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const scope = String(btn.dataset.timelineScope || TIMELINE_SCOPE_ALL);
      if (scope === TIMELINE_SCOPE_SELECTED && !selectedJobKey) {
        showToast("Select or expand a job first.", "info");
        return;
      }
      setTimelineScope(scope);
      renderTimeline();
    });
  });
}

function initSavedJobsPage() {
  setActivityPanelOpen(false);
  setCustomJobPanelOpen(false);
  setCustomJobAvailability(false);
  updateTimelineScopeButtons();
  renderWorkspaceStats();

  const pageServiceAvailable = savedPageService.isAvailable();
  const apiReady = isSavedApiReady();

  if (!pageServiceAvailable || !apiReady) {
    emitSavedStartupMetric("saved_auth_waiting");
    setAuthStatus("Local auth starting...");
    setSourceStatus("Local auth provider is starting...");
    setActivityStatus("Local provider is starting...");
    toggleAuthButtons(false);
    setAuthControlsReady(false);
    savedAuthReadyPoller.schedulePoll();
    setCustomJobAvailability(false);
    setSavedSortBarVisible(false);
    renderAuthRequired("Local auth provider is starting. Please wait...");
    renderTimeline();
    return;
  }
  savedAuthReadyPoller.stopPoll();
  emitSavedStartupMetric("saved_auth_ready");
  setAuthControlsReady(true);
  markSavedFirstInteractive("auth_ready");
  if (savedAuthListenerBound) return;
  savedAuthListenerBound = true;

  savedAuthService.onAuthStateChanged(user => {
    currentUser = user || null;
    savedDispatch.dispatch({
      type: SAVED_ACTIONS.AUTH_CHANGED,
      payload: { uid: currentUser?.uid || "" }
    });
    unsubscribeSavedJobs();
    unsubscribeSavedJobs = () => {};
    clearNoteSaveQueues();
    expandedJobKey = null;
    phaseOverrideArmedGlobal = false;
    jobDetailTabByKey = new Map();
    cachedActivityEntries = [];
    lastSavedJobsByKey = new Map();
    selectedJobKey = "";
    timelineScope = TIMELINE_SCOPE_ALL;
    lastActivityPulse = null;
    setSavedFilter(DEFAULT_SAVED_FILTER);
    setSavedSort(SORT_UPDATED);
    updateTimelineScopeButtons();
    renderSelectedJobHint();
    renderWorkspaceStats();

    if (!currentUser) {
      setAuthStatus("Browsing as guest");
      setSourceStatus("Sign in to view your saved jobs.");
      setActivityStatus("Sign in to view history.");
      toggleAuthButtons(false);
      setBackupButtonsEnabled(false);
      setCustomJobAvailability(false);
      setCustomJobPanelOpen(false);
      setSavedFilterBarVisible(false);
      setSavedSortBarVisible(false);
      renderAuthRequired("Sign in to access your custom saved jobs table.");
      renderTimeline();
      return;
    }

    setAuthStatus(`Signed in as ${currentUser.displayName || currentUser.email || "user"}`);
    setSourceStatus("Loading your saved jobs...");
    setActivityStatus("Loading activity...");
    toggleAuthButtons(true);
    setBackupButtonsEnabled(true);
    setCustomJobAvailability(true);
    const timelinePrefs = loadTimelinePreferences(currentUser.uid);
    timelineScope = timelinePrefs.scope;
    setActivityPanelOpen(false, { persist: false });
    updateTimelineScopeButtons();
    renderSelectedJobHint();
    subscribeToSavedJobs(currentUser.uid);
    refreshActivityLog().catch(err => {
      console.error("Failed to load activity:", err);
      setActivityStatus("Could not load activity.");
    });
  });
}

function subscribeToSavedJobs(uid) {
  unsubscribeSavedJobs = savedPageService.subscribeSavedJobs(
    uid,
    jobs => {
      const count = Array.isArray(jobs) ? jobs.length : 0;
      stateHubSet("savedCount", count);
      stateHubSet("savedLastUpdated", Date.now());
      setSourceStatus(`Loaded ${count} saved jobs.`);
      const isEditingNotes = isEditingNotesField();
      lastSavedJobsByKey = new Map(
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
  if (!savedJobsListEl) return;
  savedJobsListEl.innerHTML = `<div class="no-results">${escapeHtml(message)}</div>`;
}

/**
 * Sets up event delegation on the saved jobs list container.
 * Called once during boot to avoid reattaching listeners after each render.
 */
function setupSavedJobsListDelegation() {
  if (!savedJobsListEl) return;
  const t = UI_TOKENS.saved;

  savedJobsListEl.addEventListener("click", event => {
    const target = event.target;
    if (!(target instanceof Element)) return;

    const removeBtn = target.closest(ui(t.removeBtn));
    if (removeBtn) {
      const jobKey = removeBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      removeSavedJob(jobKey).catch(() => {});
      return;
    }

    const phaseBtn = target.closest(ui(t.phaseBtn));
    if (phaseBtn) {
      const jobKey = phaseBtn.dataset.jobKey || "";
      const phase = phaseBtn.dataset.phase || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      updatePhase(jobKey, phase).catch(() => {});
      return;
    }

    const detailsToggle = target.closest(ui(t.detailsToggle));
    if (detailsToggle) {
      const jobKey = detailsToggle.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      toggleDetailsForJob(jobKey);
      return;
    }

    const personalEditBtn = target.closest(ui(t.personalEditBtn));
    if (personalEditBtn) {
      const jobKey = personalEditBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      openCustomJobEditor(jobKey, false);
      return;
    }

    const personalDuplicateBtn = target.closest(ui(t.personalDuplicateBtn));
    if (personalDuplicateBtn) {
      const jobKey = personalDuplicateBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      openCustomJobEditor(jobKey, true);
      return;
    }

    const detailsTabBtn = target.closest(ui(t.detailsTabBtn));
    if (detailsTabBtn) {
      const jobKey = detailsTabBtn.dataset.jobKey || "";
      const tab = detailsTabBtn.dataset.detailsTab || "notes";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      setJobDetailsTab(jobKey, tab);
      applyJobDetailsTab(jobKey, tab);
      return;
    }

    const historyRefreshBtn = target.closest(ui(t.historyRefreshBtn));
    if (historyRefreshBtn) {
      const jobKey = historyRefreshBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      refreshActivityLog().then(() => {
        renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
      }).catch(() => {});
      return;
    }

    const attachUploadBtn = target.closest(ui(t.attachUploadBtn));
    if (attachUploadBtn) {
      const key = attachUploadBtn.dataset.jobKey || "";
      setSelectedJobKey(key, { rerenderTimeline: false });
      const input = savedJobsListEl.querySelector(`.attach-file-input[data-job-key="${cssEscape(key)}"]`);
      if (input) input.click();
      return;
    }

    const itemBlock = target.closest(ui(t.itemBlock));
    if (itemBlock && !target.closest("button,a,input,textarea,select,label")) {
      setSelectedJobKey(itemBlock.dataset.jobKey || "", { rerenderTimeline: false });
    }
  });

  savedJobsListEl.addEventListener("input", event => {
    const target = event.target;
    if (!(target instanceof HTMLTextAreaElement)) return;
    if (!target.classList.contains("job-notes-input")) return;
    const jobKey = target.dataset.jobKey || "";
    setSelectedJobKey(jobKey, { rerenderTimeline: false });
    queueNotesSave(jobKey, target.value);
  });

  savedJobsListEl.addEventListener("focusout", event => {
    const target = event.target;
    if (!(target instanceof HTMLTextAreaElement)) return;
    if (!target.classList.contains("job-notes-input")) return;
    const jobKey = target.dataset.jobKey || "";
    setSelectedJobKey(jobKey, { rerenderTimeline: false });
    flushNotesSave(jobKey, target.value).catch(() => {});
  });

  savedJobsListEl.addEventListener("change", event => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (!target.classList.contains("attach-file-input")) return;
    const files = target.files ? Array.from(target.files) : [];
    if (files.length === 0) return;
    const jobKey = target.dataset.jobKey || "";
    setSelectedJobKey(jobKey, { rerenderTimeline: false });
    uploadAttachments(jobKey, files).catch(() => {});
    target.value = "";
  });
}

function renderSavedJobs(jobs) {
  if (!savedJobsListEl) return;
  const renderContext = captureRenderContext();
  const allJobs = Array.isArray(jobs) ? jobs : [];
  const filteredJobs = sortSavedJobs(filterSavedJobs(allJobs, activeSavedFilter), activeSavedSort, { parseIsoDate });
  setSavedFilterBarVisible(allJobs.length > 0 && Boolean(currentUser));
  setSavedSortBarVisible(allJobs.length > 0 && Boolean(currentUser));
  renderSavedFilterMeta(allJobs.length, filteredJobs.length);
  renderReminderCounter(allJobs);
  renderWorkspaceStats(allJobs);

  if (!allJobs || allJobs.length === 0) {
    expandedJobKey = null;
    selectedJobKey = "";
    renderSelectedJobHint();
    savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs yet.</div>';
    renderTimeline();
    return;
  }
  if (!allJobs.some(job => String(job?.jobKey || "").trim() === selectedJobKey)) {
    selectedJobKey = "";
    renderSelectedJobHint();
    updateTimelineScopeButtons();
    if (timelineScope === TIMELINE_SCOPE_SELECTED) {
      timelineScope = TIMELINE_SCOPE_ALL;
      updateTimelineScopeButtons();
    }
  }
  if (!filteredJobs.some(job => String(job?.jobKey || "").trim() === expandedJobKey)) {
    expandedJobKey = null;
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
  const notesContext = captureActiveNotesContext();
  const anchorKey = String(notesContext?.jobKey || selectedJobKey || expandedJobKey || "").trim();
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
    expandedJobKey,
    selectedJobKey,
    getJobDetailsTab,
    renderDetailsSummary,
    getReminderMeta: reminderAt => getReminderMeta(reminderAt, { reminderSoonHours: REMINDER_SOON_HOURS }),
    renderMissingInfoChips,
    renderUpdatedHint,
    getJobHistoryEntries: jobKey => getJobHistoryEntries(jobKey, {
      cachedActivityEntries,
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
        currentUser,
        phaseOverrideArmedGlobal
      }
    ),
    currentUser,
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
  return jobDetailTabByKey.get(key) || "notes";
}

function setJobDetailsTab(jobKey, tab) {
  const safeTab = tab === "attachments" || tab === "history" ? tab : "notes";
  jobDetailTabByKey.set(String(jobKey || ""), safeTab);
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
  if (!currentUser) {
    showToast("Sign in required.", "error");
    return;
  }
  const removedSnapshot = lastSavedJobsByKey.get(String(jobKey || "")) || null;
  try {
    const removeResult = await savedPageService.removeSavedJobForUser(currentUser.uid, jobKey);
    if (!removeResult.ok) throw new Error(removeResult.error || "Could not remove job.");
    showToast("Removed saved job.", "success", {
      durationMs: 6500,
      actionLabel: "Revert",
      onAction: async () => {
        if (!currentUser || !removedSnapshot) return;
        try {
          const restoreResult = await savedPageService.saveJobForUser(currentUser.uid, removedSnapshot);
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
  if (!currentUser) {
    showToast("Sign in required.", "error");
    return;
  }

  const safeJobKey = String(jobKey || "").trim();
  if (!safeJobKey) {
    showToast("Invalid saved job key.", "error");
    return;
  }
  const row = lastSavedJobsByKey.get(safeJobKey);
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
  const overrideArmed = phaseOverrideArmedGlobal;
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
      currentUser.uid,
      safeJobKey,
      normalized,
      updateOptions
    );
    if (!updateResult.ok) throw new Error(updateResult.error || "Could not update phase.");
    if (overrideArmed) {
      phaseOverrideArmedGlobal = false;
      updateGlobalOverrideButton();
    }
    const previousPhase = currentPhase;
    showToast(`Phase updated to ${PHASE_LABELS[normalized] || normalized}.`, "success", {
      durationMs: 6500,
      actionLabel: "Revert",
      onAction: async () => {
        if (!currentUser) return;
        try {
          const revertResult = await savedPageService.updateApplicationStatus(currentUser.uid, safeJobKey, previousPhase, {
            override: true,
            cleanupPhase: normalized,
            preserveTimestamp: previousPhaseTimestamp
          });
          if (!revertResult.ok) throw new Error(revertResult.error || "Could not revert phase.");
          showToast(`Phase reverted to ${PHASE_LABELS[previousPhase] || previousPhase}.`, "success");
          await refreshActivityLog();
          renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
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
    renderSavedJobs(Array.from(lastSavedJobsByKey.values()));
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
    currentUser,
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
  const { rerenderTimeline = true } = options;
  const nextKey = String(jobKey || "").trim();
  if (nextKey === selectedJobKey) return;
  selectedJobKey = nextKey;
  renderSelectedJobHint();
  updateTimelineScopeButtons();
  if (timelineScope === TIMELINE_SCOPE_SELECTED && !selectedJobKey) {
    timelineScope = TIMELINE_SCOPE_ALL;
    updateTimelineScopeButtons();
  }
  if (rerenderTimeline) {
    renderTimeline();
  }
  if (savedJobsListEl) {
    savedJobsListEl.querySelectorAll(".saved-job-block").forEach(block => {
      block.classList.toggle("selected", String(block.dataset.jobKey || "") === selectedJobKey);
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
  const nextKey = expandedJobKey === jobKey ? null : jobKey;
  if (nextKey && !jobDetailTabByKey.has(nextKey)) {
    jobDetailTabByKey.set(nextKey, "notes");
  }
  expandedJobKey = nextKey;
  applyDetailsAccordion();
}

function applyDetailsAccordion() {
  if (!savedJobsListEl) return;
  savedJobsListEl.querySelectorAll(".saved-job-block").forEach(block => {
    const key = block.dataset.jobKey || "";
    const expanded = Boolean(expandedJobKey) && key === expandedJobKey;
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
    currentUser,
    listAttachmentsForJob: (uid, jobKey) => savedPageService.listAttachmentsForJob(uid, jobKey),
    renderAttachmentList
  });
}

async function uploadAttachments(jobKey, files) {
  return uploadAttachmentsFromModule(jobKey, files, {
    currentUser,
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
  if (!currentUser) return;
  try {
    const directUrl = savedPageService.getAttachmentOpenUrl(currentUser.uid, jobKey, attachmentId);
    if (directUrl) {
      window.open(directUrl, "_blank", "noopener,noreferrer");
      return;
    }
    const blobResult = await savedPageService.getAttachmentBlob(currentUser.uid, jobKey, attachmentId);
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
  if (!currentUser) return;
  try {
    const directUrl = savedPageService.getAttachmentDownloadUrl(currentUser.uid, jobKey, attachmentId);
    if (directUrl) {
      window.open(directUrl, "_blank", "noopener,noreferrer");
      return;
    }

    const blobResult = await savedPageService.getAttachmentBlob(currentUser.uid, jobKey, attachmentId);
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
  if (!currentUser) return;
  try {
    const deleteResult = await savedPageService.deleteAttachmentForJob(currentUser.uid, jobKey, attachmentId);
    if (!deleteResult.ok) throw new Error(deleteResult.error || "Could not delete attachment.");
    const nextResult = await savedPageService.listAttachmentsForJob(currentUser.uid, jobKey);
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
    savedJobsListEl,
    cssEscape,
    clearAttachmentPreviewUrls,
    getAttachmentPreviewUrl,
    escapeHtml,
    bindAttachmentActionButtons
  });
}

function bindAttachmentActionButtons() {
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

function setAuthStatus(text) {
  setSavedAuthStatus({
    savedAuthStatusEl,
    savedAuthStatusHintEl,
    savedAuthAvatarEl
  }, text);
}

function setSavedFilter(nextFilter) {
  activeSavedFilter = isValidSavedFilter(nextFilter) ? nextFilter : DEFAULT_SAVED_FILTER;
  savedCustomFilterBtnEls.forEach(btn => {
    const isActive = String(btn.dataset.savedFilter || "").toLowerCase() === activeSavedFilter;
    btn.classList.toggle("active", isActive);
  });
}

function setSavedSort(nextSort) {
  activeSavedSort = isValidSavedSort(nextSort) ? nextSort : SORT_UPDATED;
  savedSortBtnEls.forEach(btn => {
    const isActive = String(btn.dataset.savedSort || "").toLowerCase() === activeSavedSort;
    btn.classList.toggle("active", isActive);
  });
}

function setSavedSortBarVisible(visible) {
  if (!savedSortBarEl) return;
  savedSortBarEl.classList.toggle("hidden", !visible);
  savedSortBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
}

function setSavedFilterBarVisible(visible) {
  if (!savedCustomFilterBarEl) return;
  savedCustomFilterBarEl.classList.toggle("hidden", !visible);
  savedCustomFilterBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
}

function renderSavedFilterMeta(totalCount, filteredCount) {
  if (!savedCustomFilterCountEl) return;
  const safeTotal = Math.max(0, Number(totalCount) || 0);
  const safeFiltered = Math.max(0, Number(filteredCount) || 0);
  if (safeTotal <= 0) {
    savedCustomFilterCountEl.textContent = "";
    return;
  }
  savedCustomFilterCountEl.textContent = `${safeFiltered}/${safeTotal}`;
}

function renderReminderCounter(allJobs) {
  if (!savedReminderCounterEl) return;
  const rows = Array.isArray(allJobs) ? allJobs : [];
  const soonCount = rows.filter(job => getReminderMeta(job?.reminderAt).isSoon).length;
  savedReminderCounterEl.textContent = soonCount > 0 ? `${soonCount} due soon` : "";
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
  customJobMode = "create";
  customJobTargetKey = "";
  customJobFormEl?.reset();
  if (customJobWorkTypeEl) customJobWorkTypeEl.value = "";
  if (customJobContractTypeEl) customJobContractTypeEl.value = "";
  if (customJobSectorEl) customJobSectorEl.value = "";
  if (customJobReminderEl) customJobReminderEl.value = "";
  if (customJobPanelTitleEl) customJobPanelTitleEl.textContent = "Add Custom Job";
  if (customJobPanelHintEl) customJobPanelHintEl.textContent = "Required: Title and Company. Job link is optional.";
  if (customJobSaveBtnEl) customJobSaveBtnEl.textContent = "Save Custom Job";
  updateCustomJobWarning();
}

function updateCustomJobWarning() {
  updateCustomJobWarningUi(customJobLinkEl, customJobLinkWarningEl);
}

function setCustomJobAvailability(enabled) {
  if (!addCustomJobBtnEl) return;
  addCustomJobBtnEl.disabled = !enabled;
}

function setCustomJobPanelOpen(open) {
  customJobPanelOpen = Boolean(open);
  if (!customJobPanelEl) return;
  customJobPanelEl.classList.toggle("hidden", !customJobPanelOpen);
  customJobPanelEl.setAttribute("aria-hidden", customJobPanelOpen ? "false" : "true");
  if (addCustomJobBtnEl) {
    addCustomJobBtnEl.classList.toggle("active", customJobPanelOpen);
    addCustomJobBtnEl.textContent = customJobPanelOpen ? "Close Custom Job Form" : "+ Add Custom Job";
  }
  if (!customJobPanelOpen) {
    resetCustomJobForm();
  } else {
    updateCustomJobWarning();
  }
}

function openCustomJobEditor(jobKey, duplicate) {
  const row = lastSavedJobsByKey.get(String(jobKey || ""));
  if (!row || !isCustomJob(row)) {
    showToast("Custom job not found.", "error");
    return;
  }
  customJobMode = duplicate ? "duplicate" : "edit";
  customJobTargetKey = duplicate ? "" : String(row.jobKey || "");
  if (customJobTitleEl) customJobTitleEl.value = row.title || "";
  if (customJobCompanyEl) customJobCompanyEl.value = row.company || "";
  if (customJobCityEl) customJobCityEl.value = row.city || "";
  if (customJobCountryEl) customJobCountryEl.value = row.country || "";
  if (customJobWorkTypeEl) customJobWorkTypeEl.value = row.workType || "";
  if (customJobContractTypeEl) customJobContractTypeEl.value = row.contractType || "";
  if (customJobSectorEl) customJobSectorEl.value = row.sector || "";
  if (customJobProfessionEl) customJobProfessionEl.value = row.profession || "";
  if (customJobLinkEl) customJobLinkEl.value = row.jobLink || "";
  if (customJobNotesEl) customJobNotesEl.value = row.notes || "";
  if (customJobReminderEl) customJobReminderEl.value = toDatetimeLocalValue(row.reminderAt);
  if (customJobPanelTitleEl) {
    customJobPanelTitleEl.textContent = duplicate ? "Duplicate Custom Job" : "Edit Custom Job";
  }
  if (customJobPanelHintEl) {
    customJobPanelHintEl.textContent = duplicate
      ? "Create a new custom entry using this job as a template."
      : "Update this custom job while keeping its history and status.";
  }
  if (customJobSaveBtnEl) {
    customJobSaveBtnEl.textContent = duplicate ? "Save Duplicate" : "Update Custom Job";
  }
  setCustomJobPanelOpen(true);
  customJobTitleEl?.focus();
  updateCustomJobWarning();
}

async function createCustomJob() {
  if (!savedPageService.isAvailable() || !currentUser) {
    showToast("Sign in required.", "error");
    return;
  }
  const normalized = normalizeCustomJobInput({
    title: customJobTitleEl?.value,
    company: customJobCompanyEl?.value,
    city: customJobCityEl?.value,
    country: customJobCountryEl?.value,
    workType: customJobWorkTypeEl?.value,
    contractType: customJobContractTypeEl?.value,
    sector: customJobSectorEl?.value,
    profession: customJobProfessionEl?.value,
    jobLink: customJobLinkEl?.value,
    notes: customJobNotesEl?.value,
    reminderAt: customJobReminderEl?.value
  });

  if (!normalized.title || !normalized.company) {
    showToast("Title and Company are required.", "error");
    return;
  }

  try {
    let eventType = "custom_job_created";
    let message = "Custom job saved.";
    if (customJobMode === "edit") {
      normalized.jobKey = customJobTargetKey;
      normalized.updatedBy = "manual_edit";
      eventType = "custom_job_updated";
      message = "Custom job updated.";
    } else if (customJobMode === "duplicate") {
      normalized.updatedBy = "manual_duplicate";
      normalized.keySalt = String(Date.now());
      eventType = "custom_job_duplicated";
      message = "Custom job duplicated.";
    } else {
      normalized.updatedBy = "manual_create";
    }
    const saveResult = await savedPageService.saveJobForUser(currentUser.uid, normalized, { eventType });
    if (!saveResult.ok) throw new Error(saveResult.error || "Could not save custom job.");
    showToast(message, "success");
    savedDispatch.dispatch({
      type: SAVED_ACTIONS.CUSTOM_JOB_MUTATED,
      payload: { at: new Date().toISOString() }
    });
    setCustomJobPanelOpen(false);
    queueActivityPulse(String(saveResult?.data?.jobKey || normalized.jobKey || customJobTargetKey || ""), TIMELINE_SCOPE_ALL);
    await refreshActivityLog();
  } catch (err) {
    console.error("Could not save custom job:", err);
    showToast("Could not save custom job.", "error");
  }
}

function setSourceStatus(text) {
  setStatusText(setText, savedSourceStatusEl, text);
}

function setActivityStatus(text) {
  setElementText(activityPanelStatusEl, text);
}

function setActivityPanelOpen(open, options = {}) {
  return setActivityPanelOpenFromModule(open, {
    activityPanelEl,
    historyPanelToggleBtnEl,
    persist: options.persist,
    currentUser,
    persistTimelinePreferences,
    setActivityPanelOpenState: value => {
      activityPanelOpen = value;
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
    visible: Boolean(activityPanelOpen),
    scope: normalizeTimelineScope(timelineScope)
  });
}

function setTimelineScope(nextScope) {
  return setTimelineScopeFromModule(nextScope, {
    selectedJobKey,
    persistTimelinePreferences,
    currentUser,
    updateTimelineScopeState: value => {
      timelineScope = value;
    },
    updateTimelineScopeButtons
  });
}

function updateTimelineScopeButtons() {
  updateTimelineScopeButtonsFromModule(activityScopeBtnEls, timelineScope, selectedJobKey);
}

function queueActivityPulse(jobKey, category) {
  lastActivityPulse = queueActivityPulseFromModule(jobKey, category);
}

function clearExpiredPulse() {
  lastActivityPulse = clearExpiredPulseFromModule(lastActivityPulse);
}

function renderSelectedJobHint() {
  renderSelectedJobHintFromModule(activitySelectedJobEl, selectedJobKey, lastSavedJobsByKey);
}

function renderWorkspaceStats(jobs = null) {
  const rows = Array.isArray(jobs) ? jobs : Array.from(lastSavedJobsByKey.values());
  if (savedMetricTotalEl) savedMetricTotalEl.textContent = String(rows.length);
  if (savedMetricRemindersEl) {
    const dueSoon = rows.filter(job => getReminderMeta(job?.reminderAt).isSoon).length;
    savedMetricRemindersEl.textContent = String(dueSoon);
  }
  if (savedMetricActivityEl) {
    savedMetricActivityEl.textContent = String(countRecentActivityEntries(cachedActivityEntries, 24));
  }
}

function toggleAuthButtons(isSignedIn) {
  toggleSavedAuthButtons({ signInBtnEl, signOutBtnEl }, isSignedIn);
}

function setAuthControlsReady(ready) {
  setSavedAuthControlsReady({ signInBtnEl, signOutBtnEl }, ready);
}

function setBackupButtonsEnabled(enabled) {
  if (exportBackupBtnEl) exportBackupBtnEl.disabled = !enabled;
  if (exportIncludeFilesEl) exportIncludeFilesEl.disabled = !enabled;
  if (importBackupBtnEl) importBackupBtnEl.disabled = !enabled;
  if (globalPhaseOverrideBtnEl) globalPhaseOverrideBtnEl.disabled = !enabled;
  updateGlobalOverrideButton();
}

function updateGlobalOverrideButton() {
  if (!globalPhaseOverrideBtnEl) return;
  globalPhaseOverrideBtnEl.classList.toggle("active", phaseOverrideArmedGlobal);
  globalPhaseOverrideBtnEl.textContent = phaseOverrideArmedGlobal
    ? "Override Armed (One Use)"
    : "Override Phase Lock";
}

async function refreshActivityLog() {
  if (!activityPanelBodyEl) return;
  if (!currentUser || !savedPageService.isAvailable()) {
    setActivityStatus("Sign in to view history.");
    renderTimeline();
    renderWorkspaceStats();
    return;
  }

  setActivityStatus("Loading activity...");
  try {
    const entriesResult = await savedPageService.listActivityForUser(currentUser.uid, 400);
    if (!entriesResult.ok) throw new Error(entriesResult.error || "Could not load history.");
    const entries = Array.isArray(entriesResult.data) ? entriesResult.data : [];
    cachedActivityEntries = entries;
    renderTimeline();
    renderWorkspaceStats();
  } catch (err) {
    console.error("Could not load activity log:", err);
    cachedActivityEntries = [];
    setActivityStatus("Could not load history.");
    renderTimeline();
    renderWorkspaceStats();
  }
}

function renderTimeline() {
  renderTimelineFromModule({
    cachedActivityEntries,
    timelineScope,
    selectedJobKey,
    currentUser,
    setActivityStatus,
    renderActivityEntries
  });
}

function _shouldPulseEntry(entry) {
  clearExpiredPulse();
  return shouldPulseEntryFromModule(entry, lastActivityPulse);
}

function renderActivityEntries(entries) {
  renderActivityEntriesFromModule(entries, {
    activityPanelBodyEl,
    lastActivityPulse,
    renderActivityEntry,
    renderTimeline,
    clearExpiredPulseState: clearExpiredPulse,
    activityHighlightMs: ACTIVITY_HIGHLIGHT_MS
  });
}

function renderActivityEntry(entry) {
  return renderActivityEntryHtml(entry, {
    formatPhaseTimestamp,
    lastSavedJobsByKey,
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

async function signInUser() {
  const pageServiceAvailable = savedPageService.isAvailable();
  const apiReady = isSavedApiReady();

  if (!apiReady || !pageServiceAvailable) {
    setAuthControlsReady(false);
    savedAuthReadyPoller.schedulePoll();
    showToast("Local auth provider is starting. Try again in a moment.", "info");
    return;
  }
  if (!savedAuthListenerBound) {
    initSavedJobsPage();
  }
  setAuthControlsReady(true);

  const result = await savedAuthService.signIn();

  if (!result.ok) {
    if (String(result.error || "").toLowerCase().includes("cancel")) return;
    console.error("Sign-in failed:", result.error);
    showToast("Sign-in failed.", "error");
    return;
  }
  const focusTarget = addCustomJobBtnEl || signOutBtnEl || jobsPageBtnEl;
  if (focusTarget) {
    try {
      focusTarget.focus({ preventScroll: true });
    } catch {
      focusTarget.focus();
    }
  }
}

async function signOutUser() {
  if (!isSavedApiReady() || !savedPageService.isAvailable()) {
    setAuthControlsReady(false);
    savedAuthReadyPoller.schedulePoll();
    return;
  }
  if (!savedAuthListenerBound) {
    initSavedJobsPage();
  }
  setAuthControlsReady(true);
  const result = await savedAuthService.signOut();
  if (!result.ok) {
    console.error("Sign-out failed:", result.error);
    showToast("Sign-out failed.", "error");
  }
}

async function exportBackup() {
  await runExportBackupFromModule({
    currentUser,
    savedPageService,
    includeFiles: Boolean(exportIncludeFilesEl?.checked),
    showToast
  });
}

async function importBackup(file) {
  await runImportBackupFromModule(file, {
    currentUser,
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

import { AdminConfig as adminConfig } from "../../shared/config/admin-config.js";
import {
  activityTypeLabel as activityTypeLabelFromDomain,
  formatActivityDetail as formatActivityDetailFromDomain,
  normalizeCustomJobInput as normalizeCustomJobInputFromDomain,
  toDatetimeLocalValue as toDatetimeLocalValueFromDomain
} from "../domain.js";
import { createSavedDispatcher, SAVED_ACTIONS } from "../actions.js";
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
import {
  isSavedApiReady,
  savedAuthService,
  savedPageService
} from "../services.js";
import {
  loadSavedTimelinePreferences,
  persistSavedTimelinePreferences,
  readSavedLastJobsUrl
} from "../state-sync/index.js";
import { requestConfirmationDialog, requestTextInputDialog } from "../../local-data/profile-name-dialog.js";
import { computeAnchorScrollDelta } from "./render-cycle.js";
import {
  SAVED_FILTER_ALL,
  SORT_UPDATED,
  isValidSavedFilter,
  isValidSavedSort,
  REMINDER_SOON_HOURS
} from "./view-state.js";
import { createSavedBoot } from "./runtime/boot.js";
import { createSavedChrome } from "./runtime/chrome.js";
import { composeSavedRuntime } from "./runtime/composition.js";
import { createSavedMutations } from "./runtime/mutations.js";
import { createSavedRuntimeNotes } from "./runtime/notes.js";
import { createSavedPhaseTime } from "./runtime/phase-time.js";
import { applySavedAdminBridgeState as applySavedAdminBridgeStateFromModule } from "./admin-bridge-state.js";

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
const windowObject = typeof window === "undefined"
  ? (globalThis.window || {})
  : window;
const documentObject = typeof document === "undefined"
  ? (globalThis.document || null)
  : document;

let savedRuntime;
let savedChrome;
let savedNotes;
let savedPhaseTime;
let savedMutations;
let savedBoot;

savedRuntime = composeSavedRuntime({
  adminBridgeBase: adminConfig.ADMIN_BRIDGE_BASE,
  createSavedDispatcher,
  defaultSavedFilter: DEFAULT_SAVED_FILTER,
  defaultSavedSort: SORT_UPDATED,
  defaultTimelineScope: TIMELINE_SCOPE_ALL,
  savedPageService,
  savedAuthService,
  isSavedApiReady,
  savedActions: SAVED_ACTIONS,
  timelinePrefPrefix: TIMELINE_PREF_PREFIX,
  activityHighlightMs: ACTIVITY_HIGHLIGHT_MS,
  renderActivityEntryHtml,
  getReminderMeta,
  loadSavedTimelinePreferences,
  persistSavedTimelinePreferences,
  activityTypeLabel: type => activityTypeLabelFromDomain(type),
  formatActivityDetail: entry => formatActivityDetailFromDomain(entry, {
    normalizePhase: phase => savedRuntime.normalizePhase(phase),
    phaseLabels: PHASE_LABELS,
    formatPhaseTimestamp
  }),
  formatPhaseTimestamp,
  normalizeCustomJobInput: values => normalizeCustomJobInputFromDomain(values, { customSourceLabel: CUSTOM_SOURCE_LABEL }),
  toDatetimeLocalValue: value => toDatetimeLocalValueFromDomain(value, parseIsoDate),
  timelineScopeAll: TIMELINE_SCOPE_ALL,
  timelineScopeSelected: TIMELINE_SCOPE_SELECTED,
  timelineScopeAttachments: TIMELINE_SCOPE_ATTACHMENTS,
  phaseOptions: PHASE_OPTIONS,
  phaseLabels: PHASE_LABELS,
  customSourceLabel: CUSTOM_SOURCE_LABEL,
  reminderSoonHours: REMINDER_SOON_HOURS,
  maxAttachmentsPerJob: MAX_ATTACHMENTS_PER_JOB,
  maxAttachmentBytes: MAX_ATTACHMENT_BYTES,
  computeAnchorScrollDelta,
  renderSavedJobBlockHtml,
  parseIsoDate,
  formatRelativeTime,
  getJobHistoryEntries,
  renderPhaseBar,
  renderWebIcon,
  renderDetailsSummary,
  cssEscape: (...args) => savedChrome.cssEscape(...args),
  setActivityStatus: (...args) => savedChrome.setActivityStatus(...args),
  setSavedFilterBarVisible: (...args) => savedChrome.setSavedFilterBarVisible(...args),
  setSavedSortBarVisible: (...args) => savedChrome.setSavedSortBarVisible(...args),
  renderSavedFilterMeta: (...args) => savedChrome.renderSavedFilterMeta(...args),
  renderReminderCounter: (...args) => savedChrome.renderReminderCounter(...args),
  emitSavedStartupMetric: (...args) => savedBoot.emitSavedStartupMetric(...args),
  markSavedFirstInteractive: (...args) => savedBoot.markSavedFirstInteractive(...args),
  setSourceStatus: (...args) => savedChrome.setSourceStatus(...args),
  setSavedFilter: (...args) => savedChrome.setSavedFilter(...args),
  setSavedSort: (...args) => savedChrome.setSavedSort(...args),
  setBackupButtonsEnabled: (...args) => savedChrome.setBackupButtonsEnabled(...args),
  clearNoteSaveQueues: (...args) => savedNotes.clearNoteSaveQueues(...args),
  subscribeToSavedJobs: (...args) => savedBoot.subscribeToSavedJobs(...args),
  applySavedAdminBridgeStateFromModule
});

savedChrome = createSavedChrome({
  dom: savedRuntime.dom,
  viewState: savedRuntime.viewState,
  isValidSavedFilter,
  defaultSavedFilter: DEFAULT_SAVED_FILTER,
  isValidSavedSort,
  defaultSavedSort: SORT_UPDATED,
  getReminderMeta,
  readSavedLastJobsUrl,
  jobsLastUrlKey: JOBS_LAST_URL_KEY,
  windowObject
});

savedPhaseTime = createSavedPhaseTime({
  normalizePhase: (...args) => savedRuntime.normalizePhase(...args),
  parseIsoDate,
  phaseLabels: PHASE_LABELS,
  requestTextInputDialog
});

savedNotes = createSavedRuntimeNotes({
  noteSaveState: savedRuntime.noteSaveState,
  noteAutosaveMs: NOTE_AUTOSAVE_MS,
  savedDispatch: savedRuntime.savedDispatch,
  savedActions: SAVED_ACTIONS,
  setNoteSaveState: (...args) => savedRuntime.setNoteSaveState(...args),
  getCurrentUser: () => savedRuntime.viewState.currentUser,
  updateJobNotes: (uid, safeJobKey, saveValue) => savedPageService.updateJobNotes(uid, safeJobKey, saveValue),
  queueActivityPulse: (...args) => savedRuntime.queueActivityPulse(...args),
  timelineScopeNotes: TIMELINE_SCOPE_NOTES
});

savedMutations = createSavedMutations({
  viewState: savedRuntime.viewState,
  savedPageService,
  normalizePhase: (...args) => savedRuntime.normalizePhase(...args),
  canTransition: (...args) => savedRuntime.canTransition(...args),
  requestConfirmationDialog,
  needsInterviewTimestamp: (...args) => savedPhaseTime.needsInterviewTimestamp(...args),
  requestInterviewTimestamp: (...args) => savedPhaseTime.requestInterviewTimestamp(...args),
  phaseLabels: PHASE_LABELS,
  updateGlobalOverrideButton: (...args) => savedChrome.updateGlobalOverrideButton(...args),
  refreshActivityLog: (...args) => savedRuntime.refreshActivityLog(...args),
  renderSavedJobs: (...args) => savedRuntime.renderSavedJobs(...args),
  queueActivityPulse: (...args) => savedRuntime.queueActivityPulse(...args),
  timelineScopePhase: TIMELINE_SCOPE_PHASE
});

savedBoot = createSavedBoot({
  adminBridgeBase: adminConfig.ADMIN_BRIDGE_BASE,
  startupMetrics: savedRuntime.startupMetrics,
  dom: savedRuntime.dom,
  viewState: savedRuntime.viewState,
  noteSaveState: savedRuntime.noteSaveState,
  savedPageService,
  savedAuthController: savedRuntime.savedAuthController,
  applySavedAdminBridgeState: (...args) => savedRuntime.applySavedAdminBridgeState(...args),
  cssEscape: (...args) => savedChrome.cssEscape(...args),
  setSelectedJobKey: (...args) => savedRuntime.setSelectedJobKey(...args),
  removeSavedJob: (...args) => savedMutations.removeSavedJob(...args),
  updatePhase: (...args) => savedMutations.updatePhase(...args),
  toggleDetailsForJob: (...args) => savedRuntime.toggleDetailsForJob(...args),
  openCustomJobEditor: (...args) => savedRuntime.openCustomJobEditor(...args),
  setJobDetailsTab: (...args) => savedRuntime.setJobDetailsTab(...args),
  applyJobDetailsTab: (...args) => savedRuntime.applyJobDetailsTab(...args),
  refreshActivityLog: (...args) => savedRuntime.refreshActivityLog(...args),
  renderSavedJobs: (...args) => savedRuntime.renderSavedJobs(...args),
  loadSavedLifecycleOverlay: (...args) => savedRuntime.loadSavedLifecycleOverlay(...args),
  queueNotesSave: (...args) => savedNotes.queueNotesSave(...args),
  flushNotesSave: (...args) => savedNotes.flushNotesSave(...args),
  uploadAttachments: (...args) => savedRuntime.uploadAttachments(...args),
  getLastJobsUrl: (...args) => savedChrome.getLastJobsUrl(...args),
  defaultSavedFilter: DEFAULT_SAVED_FILTER,
  defaultSavedSort: SORT_UPDATED,
  timelineScopeAll: TIMELINE_SCOPE_ALL,
  setCustomJobPanelOpen: (...args) => savedRuntime.setCustomJobPanelOpen(...args),
  createCustomJob: (...args) => savedRuntime.createCustomJob(...args),
  updateCustomJobWarning: (...args) => savedRuntime.updateCustomJobWarning(...args),
  setSavedFilter: (...args) => savedChrome.setSavedFilter(...args),
  setSavedSort: (...args) => savedChrome.setSavedSort(...args),
  setActivityPanelOpen: (...args) => savedRuntime.setActivityPanelOpen(...args),
  updateGlobalOverrideButton: (...args) => savedChrome.updateGlobalOverrideButton(...args),
  setTimelineScope: (...args) => savedRuntime.setTimelineScope(...args),
  renderTimeline: (...args) => savedRuntime.renderTimeline(...args),
  renderWorkspaceStats: (...args) => savedRuntime.renderWorkspaceStats(...args),
  renderSelectedJobHint: (...args) => savedRuntime.renderSelectedJobHint(...args),
  renderAuthRequired: (...args) => savedRuntime.renderAuthRequired(...args),
  setSourceStatus: (...args) => savedChrome.setSourceStatus(...args),
  documentObject
});

const boot = (...args) => savedBoot.bootSavedPage(...args);
const needsInterviewTimestamp = (...args) => savedPhaseTime.needsInterviewTimestamp(...args);
const toPromptLocalDateTime = (...args) => savedPhaseTime.toPromptLocalDateTime(...args);
const parseScheduledTimestampInput = (...args) => savedPhaseTime.parseScheduledTimestampInput(...args);
const buildTimelinePrefsKey = (...args) => savedRuntime.buildTimelinePrefsKey(...args);

export {
  boot,
  needsInterviewTimestamp,
  toPromptLocalDateTime,
  parseScheduledTimestampInput,
  buildTimelinePrefsKey
};

import { UI_TOKENS, ui } from "../../shared/ui/selectors.js?v=12";

const ADMIN_REF_DEFINITIONS = Object.freeze({
  adminSourceStatusEl: { token: "sourceStatus" },
  adminContentEl: { token: "content" },
  adminRefreshBtnEl: { token: "refreshBtn" },
  adminRunFetcherBtnEl: { token: "runFetcherBtn" },
  adminRunFetcherIncrementalBtnEl: { token: "runFetcherIncrementalBtn" },
  adminRunFetcherUncappedBtnEl: { token: "runFetcherUncappedBtn" },
  adminRunFetcherForceBtnEl: { token: "runFetcherForceBtn" },
  adminRefreshReportBtnEl: { token: "refreshReportBtn" },
  adminClearLogBtnEl: { token: "clearLogBtn" },
  adminClearDiscoveryLogBtnEl: { token: "clearDiscoveryLogBtn" },
  adminRetryFailedBtnEl: { token: "retryFailedBtn" },
  adminCopyFailuresBtnEl: { token: "copyFailuresBtn" },
  adminTotalsEl: { token: "totals" },
  adminUsersListEl: { token: "usersList" },
  adminJobsBtnEl: { token: "jobsPageBtn" },
  adminSavedBtnEl: { token: "savedPageBtn" },
  adminFetcherLogEl: { token: "fetcherLog" },
  adminFetcherProgressEl: { token: "fetcherProgress" },
  adminFetcherProgressBarEl: { token: "fetcherProgressBar" },
  adminFetcherProgressLabelEl: { token: "fetcherProgressLabel" },
  adminRunDiscoveryBtnEl: { token: "runDiscoveryBtn" },
  adminRunDiscoveryUncappedBtnEl: { token: "runDiscoveryUncappedBtn" },
  adminLoadDiscoveryBtnEl: { token: "loadDiscoveryBtn" },
  adminDiscoveryAutoApproveToggleEl: { token: "discoveryAutoApproveToggle" },
  adminApproveSourcesBtnEl: { token: "approveSourcesBtn" },
  adminRejectSourcesBtnEl: { token: "rejectSourcesBtn" },
  adminDeleteSourcesBtnEl: { token: "deleteSourcesBtn" },
  adminDiscoverySummaryEl: { token: "discoverySummary" },
  adminManualSourceUrlEl: { token: "manualSourceUrl" },
  adminAddManualSourceBtnEl: { token: "addManualSourceBtn" },
  adminManualSourceFeedbackEl: { token: "manualSourceFeedback" },
  adminPendingSourcesEl: { token: "pendingSources" },
  adminPendingSourcesSelectAllEl: { token: "pendingSourcesSelectAll" },
  adminActiveSourcesEl: { token: "activeSources" },
  adminActiveSourcesSelectAllEl: { token: "activeSourcesSelectAll" },
  adminRejectedSourcesEl: { token: "rejectedSources" },
  adminRejectedSourcesSelectAllEl: { token: "rejectedSourcesSelectAll" },
  adminRestoreRejectedBtnEl: { token: "restoreRejectedBtn" },
  adminDemoteActiveBtnEl: { token: "demoteActiveBtn" },
  adminDiscoveryLogEl: { token: "discoveryLog" },
  adminDiscoveryProgressEl: { token: "discoveryProgress" },
  adminDiscoveryProgressBarEl: { token: "discoveryProgressBar" },
  adminDiscoveryProgressLabelEl: { token: "discoveryProgressLabel" },
  adminDiscoveryLogDetailsEl: { token: "discoveryLogDetails" },
  adminBridgeStatusBadgeEl: { token: "bridgeStatusBadge" },
  adminShowZeroJobsToggleEl: { token: "showZeroJobsToggle" },
  adminSyncPullBtnEl: { token: "syncPullBtn" },
  adminSyncPushBtnEl: { token: "syncPushBtn" },
  adminSyncTestBtnEl: { token: "syncTestBtn" },
  adminSyncStatusEl: { token: "syncStatus" },
  adminSyncEnabledEl: { token: "syncEnabled" },
  adminSyncConfigHintEl: { token: "syncConfigHint" },
  adminOpsAlertsEl: { token: "opsAlerts" },
  adminOpsKpisEl: { token: "opsKpis" },
  adminOpsScheduleEl: { token: "opsSchedule" },
  adminOpsTabBtnEls: { token: "opsTabBtn", all: true },
  adminOpsTabBadgeEls: { token: "opsTabBadge", all: true },
  adminOpsTabOverviewEl: { token: "opsTabOverview" },
  adminOpsTabDiscoveryEl: { token: "opsTabDiscovery" },
  adminOpsTabSourcePolicyEl: { token: "opsTabSourcePolicy" },
  adminOpsTabRegistryConflictsEl: { token: "opsTabRegistryConflicts" },
  adminOpsTabDedupEl: { token: "opsTabDedup" },
  adminDiscoveryReviewEl: { token: "discoveryReview" },
  adminSourcePolicyReviewEl: { token: "sourcePolicyReview" },
  adminRegistryConflictsReviewEl: { token: "registryConflictsReview" },
  adminOpsFetcherMetricsEl: { token: "opsFetcherMetrics" },
  adminOpsDedupListsEl: { token: "opsDedupLists" },
  adminOpsTrendsEl: { token: "opsTrends" },
  adminOpsHistoryEl: { token: "opsHistory" },
  adminFetcherProgressBadgeEl: { token: "fetcherProgressBadge" },
  adminDiscoveryProgressBadgeEl: { token: "discoveryProgressBadge" },
  adminOpsProgressBadgeEl: { token: "opsProgressBadge" },
  adminSourceFilterBtnEls: { token: "sourceFilterBtn", all: true },
  actionCenterPanelEl: { token: "actionCenterPanel" },
  actionCenterItemsEl: { token: "actionCenterItems" },
  actionCenterCopyBtnEl: { token: "actionCenterCopyBtn" },
  discoveryPendingBadgeEl: { token: "discoveryPendingBadge" },
  inspectorOverlayEl: { token: "inspectorOverlay" },
  inspectorPanelEl: { token: "inspectorPanel" },
  inspectorTitleEl: { token: "inspectorTitle" },
  inspectorContentEl: { token: "inspectorContent" },
  inspectorCloseBtnEl: { token: "inspectorCloseBtn" }
});

function resolveAdminRef(doc, tokens, definition) {
  const selector = ui(tokens[definition.token]);
  return definition.all
    ? Array.from(doc.querySelectorAll(selector))
    : doc.querySelector(selector);
}

export function cacheAdminDom(doc = document) {
  const t = UI_TOKENS.admin;
  const refs = {};
  const cache = new Map();

  Object.entries(ADMIN_REF_DEFINITIONS).forEach(([name, definition]) => {
    Object.defineProperty(refs, name, {
      enumerable: true,
      configurable: true,
      get() {
        if (!cache.has(name)) {
          cache.set(name, resolveAdminRef(doc, t, definition));
        }
        return cache.get(name);
      },
      set(value) {
        cache.set(name, value);
      }
    });
  });

  // Validate critical progress elements are available
  const progressElements = [
    { name: "adminFetcherProgressEl", el: refs.adminFetcherProgressEl },
    { name: "adminFetcherProgressBarEl", el: refs.adminFetcherProgressBarEl },
    { name: "adminFetcherProgressLabelEl", el: refs.adminFetcherProgressLabelEl },
    { name: "adminDiscoveryProgressEl", el: refs.adminDiscoveryProgressEl },
    { name: "adminDiscoveryProgressBarEl", el: refs.adminDiscoveryProgressBarEl },
    { name: "adminDiscoveryProgressLabelEl", el: refs.adminDiscoveryProgressLabelEl }
  ];

  const missingProgressElements = progressElements.filter(({ name, el }) => {
    if (!el) {
      console.warn(`[Admin DOM] Progress element missing: ${name}`);
      return true;
    }
    return false;
  });

  if (missingProgressElements.length > 0) {
    console.warn(`[Admin DOM] ${missingProgressElements.length} progress elements not found. Progress bars may not display correctly.`);
  }

  return refs;
}

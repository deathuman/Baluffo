import { UI_TOKENS, ui } from "../../shared/ui/selectors.js";

export function cacheAdminDom(doc = document) {
  const t = UI_TOKENS.admin;

  const refs = {
    adminSourceStatusEl: doc.querySelector(ui(t.sourceStatus)),
    adminContentEl: doc.querySelector(ui(t.content)),
    adminRefreshBtnEl: doc.querySelector(ui(t.refreshBtn)),
    adminRunFetcherBtnEl: doc.querySelector(ui(t.runFetcherBtn)),
    adminRunFetcherIncrementalBtnEl: doc.querySelector(ui(t.runFetcherIncrementalBtn)),
    adminRunFetcherUncappedBtnEl: doc.querySelector(ui(t.runFetcherUncappedBtn)),
    adminRunFetcherForceBtnEl: doc.querySelector(ui(t.runFetcherForceBtn)),
    adminRefreshReportBtnEl: doc.querySelector(ui(t.refreshReportBtn)),
    adminClearLogBtnEl: doc.querySelector(ui(t.clearLogBtn)),
    adminClearDiscoveryLogBtnEl: doc.querySelector(ui(t.clearDiscoveryLogBtn)),
    adminRetryFailedBtnEl: doc.querySelector(ui(t.retryFailedBtn)),
    adminCopyFailuresBtnEl: doc.querySelector(ui(t.copyFailuresBtn)),
    adminTotalsEl: doc.querySelector(ui(t.totals)),
    adminUsersListEl: doc.querySelector(ui(t.usersList)),
    adminJobsBtnEl: doc.querySelector(ui(t.jobsPageBtn)),
    adminSavedBtnEl: doc.querySelector(ui(t.savedPageBtn)),
    adminFetcherLogEl: doc.querySelector(ui(t.fetcherLog)),
    adminFetcherProgressEl: doc.querySelector(ui(t.fetcherProgress)),
    adminFetcherProgressBarEl: doc.querySelector(ui(t.fetcherProgressBar)),
    adminFetcherProgressLabelEl: doc.querySelector(ui(t.fetcherProgressLabel)),
    adminRunDiscoveryBtnEl: doc.querySelector(ui(t.runDiscoveryBtn)),
    adminRunDiscoveryUncappedBtnEl: doc.querySelector(ui(t.runDiscoveryUncappedBtn)),
    adminLoadDiscoveryBtnEl: doc.querySelector(ui(t.loadDiscoveryBtn)),
    adminDiscoveryAutoApproveToggleEl: doc.querySelector(ui(t.discoveryAutoApproveToggle)),
    adminApproveSourcesBtnEl: doc.querySelector(ui(t.approveSourcesBtn)),
    adminRejectSourcesBtnEl: doc.querySelector(ui(t.rejectSourcesBtn)),
    adminDeleteSourcesBtnEl: doc.querySelector(ui(t.deleteSourcesBtn)),
    adminDiscoverySummaryEl: doc.querySelector(ui(t.discoverySummary)),
    adminManualSourceUrlEl: doc.querySelector(ui(t.manualSourceUrl)),
    adminAddManualSourceBtnEl: doc.querySelector(ui(t.addManualSourceBtn)),
    adminManualSourceFeedbackEl: doc.querySelector(ui(t.manualSourceFeedback)),
    adminPendingSourcesEl: doc.querySelector(ui(t.pendingSources)),
    adminPendingSourcesSelectAllEl: doc.querySelector(ui(t.pendingSourcesSelectAll)),
    adminActiveSourcesEl: doc.querySelector(ui(t.activeSources)),
    adminActiveSourcesSelectAllEl: doc.querySelector(ui(t.activeSourcesSelectAll)),
    adminRejectedSourcesEl: doc.querySelector(ui(t.rejectedSources)),
    adminRejectedSourcesSelectAllEl: doc.querySelector(ui(t.rejectedSourcesSelectAll)),
    adminRestoreRejectedBtnEl: doc.querySelector(ui(t.restoreRejectedBtn)),
    adminDemoteActiveBtnEl: doc.querySelector(ui(t.demoteActiveBtn)),
    adminDiscoveryLogEl: doc.querySelector(ui(t.discoveryLog)),
    adminDiscoveryProgressEl: doc.querySelector(ui(t.discoveryProgress)),
    adminDiscoveryProgressBarEl: doc.querySelector(ui(t.discoveryProgressBar)),
    adminDiscoveryProgressLabelEl: doc.querySelector(ui(t.discoveryProgressLabel)),
    adminDiscoveryLogDetailsEl: doc.querySelector(ui(t.discoveryLogDetails)),
    adminBridgeStatusBadgeEl: doc.querySelector(ui(t.bridgeStatusBadge)),
    adminShowZeroJobsToggleEl: doc.querySelector(ui(t.showZeroJobsToggle)),
    adminRefreshOpsBtnEl: doc.querySelector(ui(t.refreshOpsBtn)),
    adminSyncPullBtnEl: doc.querySelector(ui(t.syncPullBtn)),
    adminSyncPushBtnEl: doc.querySelector(ui(t.syncPushBtn)),
    adminSyncTestBtnEl: doc.querySelector(ui(t.syncTestBtn)),
    adminSyncStatusEl: doc.querySelector(ui(t.syncStatus)),
    adminSyncEnabledEl: doc.querySelector(ui(t.syncEnabled)),
    adminSyncConfigHintEl: doc.querySelector(ui(t.syncConfigHint)),
    adminOpsAlertsEl: doc.querySelector(ui(t.opsAlerts)),
    adminOpsKpisEl: doc.querySelector(ui(t.opsKpis)),
    adminOpsScheduleEl: doc.querySelector(ui(t.opsSchedule)),
    adminSourcePolicyReviewEl: doc.querySelector(ui(t.sourcePolicyReview)),
    adminOpsFetcherMetricsEl: doc.querySelector(ui(t.opsFetcherMetrics)),
    adminOpsTrendsEl: doc.querySelector(ui(t.opsTrends)),
    adminOpsHistoryEl: doc.querySelector(ui(t.opsHistory)),
    adminFetcherProgressBadgeEl: doc.querySelector(ui(t.fetcherProgressBadge)),
    adminDiscoveryProgressBadgeEl: doc.querySelector(ui(t.discoveryProgressBadge)),
    adminOpsProgressBadgeEl: doc.querySelector(ui(t.opsProgressBadge)),
    adminSourceFilterBtnEls: Array.from(doc.querySelectorAll(ui(t.sourceFilterBtn)))
  };

  // Validate critical progress elements are available
  const progressElements = [
    { name: 'adminFetcherProgressEl', el: refs.adminFetcherProgressEl },
    { name: 'adminFetcherProgressBarEl', el: refs.adminFetcherProgressBarEl },
    { name: 'adminFetcherProgressLabelEl', el: refs.adminFetcherProgressLabelEl },
    { name: 'adminDiscoveryProgressEl', el: refs.adminDiscoveryProgressEl },
    { name: 'adminDiscoveryProgressBarEl', el: refs.adminDiscoveryProgressBarEl },
    { name: 'adminDiscoveryProgressLabelEl', el: refs.adminDiscoveryProgressLabelEl }
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

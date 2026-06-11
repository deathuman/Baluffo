import { createOpsBridgeStatusController } from "./ops/bridge-status.js?v=13";
import { createOpsHealthController } from "./ops/health.js?v=19";
import { scheduleAdminRender } from "./ops/render-scheduler.js";
import { createOpsTaskStateController } from "./ops/task-state.js";

export { formatBytes } from "./ops/format.js";

export function createAdminOpsController({
  state,
  refs,
  getBridge,
  postBridge,
  deriveAdminRunsModel,
  getOpsPollIntervalMs,
  renderAdminOpsAlerts: renderAdminOpsAlertsImpl,
  renderAdminOpsKpis: renderAdminOpsKpisImpl,
  renderAdminOpsSchedule: renderAdminOpsScheduleImpl,
  renderAdminOpsDedupLists: renderAdminOpsDedupListsImpl,
  renderAdminOpsFetcherMetrics: renderAdminOpsFetcherMetricsImpl,
  renderAdminSourcePolicyReview: renderAdminSourcePolicyReviewImpl,
  renderAdminRegistryConflicts: renderAdminRegistryConflictsImpl,
  renderAdminOpsTrends: renderAdminOpsTrendsImpl,
  renderAdminOpsHistory: renderAdminOpsHistoryImpl,
  setBusyFlag,
  showToast,
  getErrorMessage,
  adminDispatch,
  adminActions,
  escapeHtml,
  onBridgeStatusChange,
  attachToActiveFetchRun,
  loadLatestFetcherReport,
  attachToActiveDiscoveryRun,
  loadLatestDiscoveryReport,
  bridgeStatusPollIntervalMs,
  idlePollIntervalMs,
  awaitBridgeReady,
  markAdminStep,
  measureAdminStep,
  renderScheduler = scheduleAdminRender
}) {
  const bridgeStatusController = createOpsBridgeStatusController({
    state,
    refs,
    getBridge,
    onBridgeStatusChange,
    bridgeStatusPollIntervalMs
  });

  const taskStateController = createOpsTaskStateController({
    state,
    setBusyFlag,
    attachToActiveFetchRun,
    loadLatestFetcherReport,
    attachToActiveDiscoveryRun,
    loadLatestDiscoveryReport
  });

  const healthController = createOpsHealthController({
    state,
    refs,
    getBridge,
    postBridge,
    deriveAdminRunsModel,
    getOpsPollIntervalMs,
    renderAdminOpsAlerts: renderAdminOpsAlertsImpl,
    renderAdminOpsKpis: renderAdminOpsKpisImpl,
    renderAdminOpsSchedule: renderAdminOpsScheduleImpl,
    renderAdminOpsDedupLists: renderAdminOpsDedupListsImpl,
    renderAdminOpsFetcherMetrics: renderAdminOpsFetcherMetricsImpl,
    renderAdminSourcePolicyReview: renderAdminSourcePolicyReviewImpl,
    renderAdminRegistryConflicts: renderAdminRegistryConflictsImpl,
    renderAdminOpsTrends: renderAdminOpsTrendsImpl,
    renderAdminOpsHistory: renderAdminOpsHistoryImpl,
    setBusyFlag,
    showToast,
    getErrorMessage,
    adminDispatch,
    adminActions,
    escapeHtml,
    idlePollIntervalMs,
    taskStateController,
    getBridgeStatus: bridgeStatusController.getBridgeStatus,
    awaitBridgeReady,
    markAdminStep,
    measureAdminStep,
    renderScheduler
  });

  return {
    setOpsPlaceholders: healthController.setOpsPlaceholders,
    setOpsReadinessShell: healthController.setOpsReadinessShell,
    selectOpsTab: healthController.selectOpsTab,
    stopOpsHealthPolling: healthController.stopOpsHealthPolling,
    scheduleOpsHealthPolling: healthController.scheduleOpsHealthPolling,
    applyBootstrapPayload: healthController.applyBootstrapPayload,
    loadOpsHealthData: healthController.loadOpsHealthData,
    loadOpsHistoryData: healthController.loadOpsHistoryData,
    loadOpsOverviewDetailData: healthController.loadOpsOverviewDetailData,
    setBridgeStatusBadge: bridgeStatusController.setBridgeStatusBadge,
    startBridgeStatusWatch: bridgeStatusController.startBridgeStatusWatch,
    stopBridgeStatusWatch: bridgeStatusController.stopBridgeStatusWatch,
    pollBridgeStatus: bridgeStatusController.pollBridgeStatus
  };
}

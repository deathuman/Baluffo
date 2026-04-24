import { createOpsBridgeStatusController } from "./ops/bridge-status.js";
import { createOpsHealthController } from "./ops/health.js";
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
  renderAdminOpsFetcherMetrics: renderAdminOpsFetcherMetricsImpl,
  renderAdminOpsTrends: renderAdminOpsTrendsImpl,
  renderAdminOpsHistory: renderAdminOpsHistoryImpl,
  loadSyncStatus,
  setBusyFlag,
  showToast,
  getErrorMessage,
  adminDispatch,
  adminActions,
  escapeHtml,
  onBridgeStatusChange,
  loadDiscoveryData,
  attachToActiveFetchRun,
  loadLatestFetcherReport,
  attachToActiveDiscoveryRun,
  loadLatestDiscoveryReport,
  bridgeStatusPollIntervalMs,
  idlePollIntervalMs,
  awaitBridgeReady
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
    renderAdminOpsFetcherMetrics: renderAdminOpsFetcherMetricsImpl,
    renderAdminOpsTrends: renderAdminOpsTrendsImpl,
    renderAdminOpsHistory: renderAdminOpsHistoryImpl,
    loadSyncStatus,
    setBusyFlag,
    showToast,
    getErrorMessage,
    adminDispatch,
    adminActions,
    escapeHtml,
    loadDiscoveryData,
    idlePollIntervalMs,
    taskStateController,
    getBridgeStatus: bridgeStatusController.getBridgeStatus,
    awaitBridgeReady
  });

  return {
    setOpsPlaceholders: healthController.setOpsPlaceholders,
    stopOpsHealthPolling: healthController.stopOpsHealthPolling,
    scheduleOpsHealthPolling: healthController.scheduleOpsHealthPolling,
    loadOpsHealthData: healthController.loadOpsHealthData,
    setBridgeStatusBadge: bridgeStatusController.setBridgeStatusBadge,
    startBridgeStatusWatch: bridgeStatusController.startBridgeStatusWatch,
    stopBridgeStatusWatch: bridgeStatusController.stopBridgeStatusWatch,
    pollBridgeStatus: bridgeStatusController.pollBridgeStatus
  };
}

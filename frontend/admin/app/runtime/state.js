export function createAdminBusyState() {
  return {
    fetcherRun: false,
    fetcherWatch: false,
    fetcherReportLoad: false,
    syncRun: false,
    discoveryRun: false,
    discoveryWatch: false,
    discoveryLoad: false,
    discoveryWrite: false,
    manualAdd: false,
    manualCheck: false,
    opsLoad: false,
    liveFetchRunning: false,
    liveDiscoveryRunning: false,
    liveSyncRunning: false,
    livePipelineRunning: false
  };
}

export function createAdminRuntimeState({
  discoveryReportPollIntervalMs,
  discoveryReportPollTimeoutMs
}) {
  return {
    adminPin: "",
    activeSourceFilter: "all",
    latestFetcherReportCache: null,
    latestOpsHealthCache: null,
    latestSyncStatusCache: null,
    syncConfigDirty: false,
    adminApiReadyPollTimer: null,
    bridgeStatusPollTimer: null,
    opsHealthPollTimer: null,
    fetcherCompletionPollTimer: null,
    fetcherCompletionPollDeadline: 0,
    fetcherLaunchAtMs: 0,
    fetcherLiveProgressState: null,
    discoveryCompletionPollTimer: null,
    discoveryCompletionPollDeadline: 0,
    discoveryLaunchAtMs: 0,
    discoveryLiveProgressState: null,
    discoveryLogRemoteOffset: 0,
    discoveryLogDetailsSyncing: false,
    discoveryLogUserToggled: false,
    discoveryLogPreferredOpen: true,
    adminInteractiveMetricSent: false,
    discoveryReportPollIntervalMs,
    discoveryReportPollTimeoutMs,
    adminBusyState: createAdminBusyState()
  };
}

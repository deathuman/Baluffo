function createAdminBusyState() {
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
  discoveryReportPollIntervalMs
}) {
  return {
    activeSourceFilter: "all",
    latestFetcherReportCache: null,
    latestOpsHealthCache: null,
    latestSyncStatusCache: null,
    latestDiscoveryConfigCache: null,
    syncConfigDirty: false,
    discoveryConfigDirty: false,
    bridgeStatusPollTimer: null,
    opsHealthPollTimer: null,
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLaunchAtMs: 0,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: null,
    fetcherLogRemoteOffset: 0,
    discoveryCompletionPollTimer: null,
    discoveryLaunchAtMs: 0,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    discoveryLogRemoteOffset: 0,
    discoveryLogDetailsSyncing: false,
    discoveryLogUserToggled: false,
    discoveryLogPreferredOpen: true,
    adminInteractiveMetricSent: false,
    discoveryReportPollIntervalMs,
    adminBusyState: createAdminBusyState()
  };
}

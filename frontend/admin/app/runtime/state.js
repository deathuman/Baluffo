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

export function createAdminRuntimeState() {
  return {
    activeSourceFilter: "all",
    latestFetcherReportCache: null,
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    opsHistoryLoadPending: false,
    opsHistoryLoaded: false,
    opsHistoryFullLoaded: false,
    latestSourcePolicyRecommendationsPayload: null,
    latestRegistryConflictsPayload: null,
    latestTaskStatePayload: null,
    waitingForTaskState: false,
    // ponytail: distinguish "server answered with empty task list" from
    // "task-state fetch failed"; populated by ops/task-state.js controller.
    lastTaskStateError: "",
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
    fetcherSourceSyncSignature: "",
    fetchOptimisticRun: null,
    fetcherLogRemoteOffset: 0,
    discoveryCompletionPollTimer: null,
    discoveryLaunchAtMs: 0,
    discoveryLiveProgressState: null,
    discoverySourceSyncSignature: "",
    discoveryRegistrySignature: "",
    discoveryTablesRendered: false,
    adminSectionLoadState: {},
    discoveryLastLoadStartedAtMs: 0,
    discoveryLastLoadCompletedAtMs: 0,
    discoveryLastLoadSucceededAtMs: 0,
    discoveryOptimisticRun: null,
    discoveryLoadPromise: null,
    discoveryLogRemoteOffset: 0,
    discoveryLogDetailsSyncing: false,
    discoveryLogUserToggled: false,
    discoveryLogPreferredOpen: true,
    adminInteractiveMetricSent: false,
    adminBusyState: createAdminBusyState()
  };
}

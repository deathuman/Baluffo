import { deriveFetcherFailureSummary } from "../../domain.js";
import {
  FETCH_KPI_LOADING_LABEL,
  buildFetchKpiPendingLabels,
  maskDeferredFetchKpisForRender
} from "../../domain/ops-fetch-kpi-model.js";
import { renderOpsTabBadges } from "./health-badges.js";

/**
 * Render helpers and the ops render hub: readiness/placeholder shells, tab
 * badges, the fetcher-metrics payload assembly, deferred panel renders and the
 * full health snapshot paint.
 */
export function createOpsRenderHub({
  state,
  refs,
  postBridge,
  showToast,
  getErrorMessage,
  escapeHtml,
  adminDispatch,
  adminActions,
  getOpsPollIntervalMs,
  renderScheduler,
  getFrontendPerfCounters,
  currentRenderToken,
  deriveAdminRunsModel,
  renderAdminOpsAlertsImpl,
  renderAdminOpsKpisImpl,
  renderAdminOpsDedupListsImpl,
  renderAdminOpsFetcherMetricsImpl,
  renderAdminOpsTrendsImpl,
  renderAdminOpsHistoryImpl,
  getCachedTaskStatePayload,
  getCachedHistoryPayload,
  getCachedSourcePolicyPayload,
  getCachedRegistryConflictsPayload,
  getCachedDiscoveryAuditArtifactsPayload,
  getCachedTaskFailureAttemptsPayload,
  getCachedPerformanceProfilePayload,
  hasActiveRows,
  hasActivePipelineOrFetchRows,
  deriveLiveRunContext,
  getHistoryElement,
  renderPipelineScheduleModel,
  renderSourcePolicyReviewQueue,
  renderRegistryConflictsQueue,
  scheduleOpsHealthPolling,
  scheduleOpsOverviewDetailData,
  loadOpsHealthData,
  taskStateController,
  handleDedupReviewAction,
  handleCopySectionDiagnostics,
  handleCopyRunDiagnostics,
  handleRefreshAuditArtifacts,
  handleRefreshTaskFailureAttempts,
  handleRefreshPerformanceProfile,
  handleAbortRun,
  handleLoadDebugDiagnostics
}) {
  function getRenderScheduler() {
    return typeof renderScheduler === "function"
      ? renderScheduler
      : callback => {
        callback();
        return () => {};
      };
  }

  function buildFetcherMetricsPayload(fetcherMetrics = state.latestOpsFetcherMetricsPayload || {}, health = state.latestOpsHealthCache || {}) {
    const frontendPerfCounters = getFrontendPerfCounters();
    return {
      ...(fetcherMetrics && typeof fetcherMetrics === "object" ? fetcherMetrics : {}),
      discoveryAuditArtifacts: getCachedDiscoveryAuditArtifactsPayload(),
      taskFailureAttempts: getCachedTaskFailureAttemptsPayload(),
      performanceProfile: getCachedPerformanceProfilePayload(),
      frontendPerfCounters: (
        frontendPerfCounters
        && typeof frontendPerfCounters === "object"
        && !Array.isArray(frontendPerfCounters)
      )
        ? frontendPerfCounters
        : {},
      latestRun: {
        ...(
          fetcherMetrics?.latestRun && typeof fetcherMetrics.latestRun === "object"
            ? fetcherMetrics.latestRun
            : {}
        ),
        conservativeStaticCleanupProposals:
          health?.kpis?.conservativeStaticCleanupProposals
          && typeof health.kpis.conservativeStaticCleanupProposals === "object"
            ? health.kpis.conservativeStaticCleanupProposals
            : (
              fetcherMetrics?.latestRun?.conservativeStaticCleanupProposals
              && typeof fetcherMetrics.latestRun.conservativeStaticCleanupProposals === "object"
                ? fetcherMetrics.latestRun.conservativeStaticCleanupProposals
                : {}
            )
      }
    };
  }

  function rerenderOpsTabBadges() {
    const activePipelineOrFetch = Boolean(
      state.opsActivePipelineOrFetchLastActive
      || state.adminBusyState?.livePipelineRunning
      || state.adminBusyState?.liveFetchRunning
      || hasActivePipelineOrFetchRows(getCachedTaskStatePayload())
    );
    renderOpsTabBadges(refs, {
      health: state.latestOpsHealthCache || {},
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations: getCachedSourcePolicyPayload(),
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      fetcherMetricsPayload: buildFetcherMetricsPayload(),
      tabCountsPayload: state.latestOpsTabCountsPayload || null,
      activePipelineOrFetch,
      tabCountsUnavailable: Boolean(state.opsTabCountsUnavailable && !activePipelineOrFetch)
    });
  }

  function setOpsPlaceholders(message = "Operations health unavailable.") {
    if (refs.adminSyncStatusEl) {
      refs.adminSyncStatusEl.textContent = message;
    }
    if (refs.adminSyncConfigHintEl) {
      refs.adminSyncConfigHintEl.textContent = "GitHub App credentials are packaged with the app.";
    }
    if (refs.adminOpsAlertsEl) {
      refs.adminOpsAlertsEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
    }
    if (refs.adminOpsKpisEl) refs.adminOpsKpisEl.innerHTML = "";
    renderPipelineScheduleModel();
    if (refs.adminSourcePolicyReviewEl) {
      refs.adminSourcePolicyReviewEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
    }
    if (refs.adminOpsFetcherMetricsEl) refs.adminOpsFetcherMetricsEl.innerHTML = "";
    if (refs.adminOpsDedupListsEl) refs.adminOpsDedupListsEl.innerHTML = "";
    if (refs.adminOpsTrendsEl) refs.adminOpsTrendsEl.textContent = message;
    renderOpsTabBadges(refs, {
      health: { alerts: [] },
      discoveryReport: {},
      sourcePolicyRecommendations: {},
      fetcherMetricsPayload: {},
      tabCountsPayload: state.latestOpsTabCountsPayload || null
    });
  }

  function setOpsReadinessShell() {
    if (refs.adminOpsAlertsEl) refs.adminOpsAlertsEl.innerHTML = "";
    if (refs.adminOpsKpisEl) refs.adminOpsKpisEl.innerHTML = "";
    renderPipelineScheduleModel();
    if (refs.adminSourcePolicyReviewEl) refs.adminSourcePolicyReviewEl.innerHTML = "";
    if (refs.adminOpsFetcherMetricsEl) refs.adminOpsFetcherMetricsEl.innerHTML = "";
    if (refs.adminOpsDedupListsEl) refs.adminOpsDedupListsEl.innerHTML = "";
    if (refs.adminOpsTrendsEl) {
      refs.adminOpsTrendsEl.textContent = "No run trend data yet.";
    }
    renderOpsTabBadges(refs, {
      health: { alerts: [] },
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations: getCachedSourcePolicyPayload(),
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      fetcherMetricsPayload: buildFetcherMetricsPayload(),
      tabCountsPayload: state.latestOpsTabCountsPayload || null
    });
  }

  function historyRenderOptions() {
    return {
      onCopyRunDiagnostics: handleCopyRunDiagnostics,
      onAbortRun: handleAbortRun,
      // Run detail renders into the inspector drawer, which is a sibling of the
      // history container rather than a child of it. Passing it in keeps the
      // dependency explicit; `renderAdminOpsHistory` falls back to a document
      // lookup so the renderer still works on its own.
      runDetailHost: refs?.inspectorContentEl || null,
      waitingForTaskState: Boolean(state.waitingForTaskState),
      taskStateUnavailable: Boolean(state.taskStateUnavailable),
      taskStateError: String(state.lastTaskStateError || "").trim(),
      historyPending: Boolean(state.opsHistoryLoadPending),
      historyLoaded: Boolean(state.opsHistoryLoaded),
      historyError: state.opsHistoryLastError,
      historyFullLoaded: Boolean(state.opsHistoryFullLoaded)
    };
  }

  function fetcherMetricsRenderOptions(runModel) {
    return {
      onDedupReviewAction: handleDedupReviewAction,
      onCopySectionDiagnostics: handleCopySectionDiagnostics,
      onRefreshAuditArtifacts: handleRefreshAuditArtifacts,
      onRefreshTaskFailureAttempts: handleRefreshTaskFailureAttempts,
      onRefreshPerformanceProfile: handleRefreshPerformanceProfile,
      onLoadDebugDiagnostics: handleLoadDebugDiagnostics,
      includeDebugDiagnostics: Boolean(state.opsDebugDiagnosticsLoaded),
      debugDiagnosticsLoading: Boolean(state.opsDebugDiagnosticsLoading),
      runModel
    };
  }

  function renderDeferredOverviewDetails(renderToken = currentRenderToken()) {
    if (renderToken !== currentRenderToken()) return;
    const fetcherMetricsPayload = buildFetcherMetricsPayload();
    rerenderOpsTabBadges();
    getRenderScheduler()(() => {
      if (renderToken !== currentRenderToken()) return;
      const historyPayload = getCachedHistoryPayload();
      const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
      const taskStatePayload = state.latestOpsTaskStatePayload || { tasks: [] };
      const runModel = deriveAdminRunsModel(
        {
          taskState: taskStatePayload || {},
          historyRuns
        },
        Date.now()
      );
      renderAdminOpsFetcherMetricsImpl(
        refs.adminOpsFetcherMetricsEl,
        fetcherMetricsPayload,
        deriveFetcherFailureSummary(state.latestFetcherReportCache || {}),
        fetcherMetricsRenderOptions(runModel)
      );
      renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, fetcherMetricsPayload, {
        onDedupReviewAction: handleDedupReviewAction
      });
      renderAdminOpsHistoryImpl(getHistoryElement(), runModel, historyRenderOptions());
      if (state.opsHistoryLoaded) {
        renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
      }
    });
  }

  function renderDeferredHistoryDetails(renderToken = null) {
    if (renderToken !== null && renderToken !== currentRenderToken()) return;
    getRenderScheduler()(() => {
      if (renderToken !== null && renderToken !== currentRenderToken()) return;
      const historyPayload = getCachedHistoryPayload();
      const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
      const taskStatePayload = state.latestOpsTaskStatePayload || { tasks: [] };
      const runModel = deriveAdminRunsModel(
        {
          taskState: taskStatePayload || {},
          historyRuns
        },
        Date.now()
      );
      renderAdminOpsHistoryImpl(getHistoryElement(), runModel, historyRenderOptions());
      if (state.opsHistoryLoaded) {
        renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
      }
    });
  }

  function renderOpsHealthSnapshot(renderToken, health, {
    taskStatePayload = getCachedTaskStatePayload(),
    registryConflictsPayload = getCachedRegistryConflictsPayload(),
    syncTaskState = false,
    dispatchRefresh = false,
    scheduleDetails = false,
    renderDeferredPanels = true,
    renderActivityPanel = false,
    schedulePolling = true
  } = {}) {
    if (renderToken !== currentRenderToken()) return;
    const sourcePolicyRecommendations = getCachedSourcePolicyPayload();
    const {
      historyRuns,
      runModel,
      liveTaskRows,
      liveTypes,
      registryConflictRunning
    } = deriveLiveRunContext(taskStatePayload, registryConflictsPayload);
    if (syncTaskState) {
      taskStateController.syncLiveBusyFlags(liveTypes);
      taskStateController.maybeAttachLiveTaskRows(liveTaskRows);
    }
    const fetcherMetricsPayload = buildFetcherMetricsPayload(
      state.latestOpsFetcherMetricsPayload || {},
      health || {}
    );
    const controlPlanePipelineActive = Boolean(
      taskStatePayload?.source === "pipeline-status" && hasActiveRows(taskStatePayload)
    );
    const activePipelineOrFetch = Boolean(
      controlPlanePipelineActive || liveTypes.has("pipeline") || liveTypes.has("fetch")
    );
    if (activePipelineOrFetch) {
      state.opsTabCountsDelayedDuringActiveRun = true;
    }
    const fetchKpiPendingLabels = buildFetchKpiPendingLabels(health, activePipelineOrFetch);
    const fetchKpiPendingLabel = String(fetchKpiPendingLabels.default || FETCH_KPI_LOADING_LABEL);
    const renderKpis = maskDeferredFetchKpisForRender(
      health?.kpis || {},
      health || {}
    );

    renderAdminOpsAlertsImpl(refs.adminOpsAlertsEl, health?.alerts || [], {
      onAck: async alertId => {
        if (!alertId) return;
        try {
          await postBridge("/ops/alerts/ack", { id: alertId });
          await loadOpsHealthData();
        } catch (err) {
          showToast(`Could not dismiss alert: ${getErrorMessage(err)}`, "error");
        }
      }
    });
    renderAdminOpsKpisImpl(
      refs.adminOpsKpisEl,
      renderKpis,
      String(health?.status || "healthy"),
      { fetchKpiPendingLabel, fetchKpiPendingLabels }
    );
    renderPipelineScheduleModel();
    renderOpsTabBadges(refs, {
      health,
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations,
      registryConflictsPayload,
      fetcherMetricsPayload,
      tabCountsPayload: state.latestOpsTabCountsPayload || null,
      activePipelineOrFetch,
      tabCountsUnavailable: Boolean(state.opsTabCountsUnavailable && !activePipelineOrFetch)
    });
    renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
    const renderOptions = historyRenderOptions();
    if (renderActivityPanel) {
      renderAdminOpsHistoryImpl(getHistoryElement(), runModel, renderOptions);
    }
    if (renderDeferredPanels) {
      getRenderScheduler()(() => {
        if (renderToken !== currentRenderToken()) return;
        const deferredContext = deriveLiveRunContext(
          getCachedTaskStatePayload(),
          getCachedRegistryConflictsPayload()
        );
        renderSourcePolicyReviewQueue(getCachedSourcePolicyPayload());
        renderRegistryConflictsQueue(getCachedRegistryConflictsPayload());
        renderAdminOpsFetcherMetricsImpl(
          refs.adminOpsFetcherMetricsEl,
          fetcherMetricsPayload,
          deriveFetcherFailureSummary(state.latestFetcherReportCache || {}),
          fetcherMetricsRenderOptions(deferredContext.runModel)
        );
        renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, fetcherMetricsPayload, {
          onDedupReviewAction: handleDedupReviewAction
        });
        renderAdminOpsHistoryImpl(getHistoryElement(), deferredContext.runModel, {
          ...renderOptions
        });
      });
    }
    if (dispatchRefresh) {
      adminDispatch.dispatch({ type: adminActions.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
    }
    if (schedulePolling) {
      scheduleOpsHealthPolling(getOpsPollIntervalMs(liveTypes.size > 0 || registryConflictRunning));
    }
    if (scheduleDetails) {
      scheduleOpsOverviewDetailData(renderToken);
    }
  }

  return {
    getRenderScheduler,
    buildFetcherMetricsPayload,
    rerenderOpsTabBadges,
    setOpsPlaceholders,
    setOpsReadinessShell,
    renderDeferredOverviewDetails,
    renderDeferredHistoryDetails,
    renderOpsHealthSnapshot
  };
}

export function createFetchKpisHydration({
  state,
  canHydrateCompactDuringActiveRun,
  hasPossibleActiveRunEvidence,
  markFetchKpisDeferredDuringActiveRun,
  renderOpsHealthSnapshot,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  mergeOpsHealth,
  measuredGetBridge,
  currentRenderToken,
  isStaleRenderToken,
  OPS_FETCH_KPIS_SUMMARY_PATH
}) {
  let fetchKpisLoad = null;

  async function loadFetchKpisSummaryData(renderToken = currentRenderToken(), options = {}) {
    if (
      !canHydrateCompactDuringActiveRun()
      && !options?.force
      && hasPossibleActiveRunEvidence({ includeRecent: false })
    ) {
      markFetchKpisDeferredDuringActiveRun();
      if (!isStaleRenderToken(renderToken)) {
        renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
          taskStatePayload: getCachedTaskStatePayload(),
          registryConflictsPayload: getCachedRegistryConflictsPayload(),
          renderDeferredPanels: false,
          renderActivityPanel: false,
          schedulePolling: false
        });
      }
      return state.latestOpsHealthCache || null;
    }
    if (!fetchKpisLoad) {
      fetchKpisLoad = measuredGetBridge(
        OPS_FETCH_KPIS_SUMMARY_PATH,
        "admin_ops_fetch_kpis_summary_fetch",
        { enabled: !options?.fromPoll && !options?.silent }
      ).finally(() => {
        fetchKpisLoad = null;
      });
    }
    let payload;
    try {
      payload = await fetchKpisLoad;
    } catch (err) {
      if (
        !canHydrateCompactDuringActiveRun()
        && !isStaleRenderToken(renderToken)
        && hasPossibleActiveRunEvidence()
      ) {
        markFetchKpisDeferredDuringActiveRun();
        renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
          taskStatePayload: getCachedTaskStatePayload(),
          registryConflictsPayload: getCachedRegistryConflictsPayload(),
          renderDeferredPanels: false,
          renderActivityPanel: false,
          schedulePolling: false
        });
      }
      throw err;
    }
    const targetRenderToken = options?.renderWithCurrentToken ? currentRenderToken() : renderToken;
    if (isStaleRenderToken(targetRenderToken)) return payload || null;
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      state.latestOpsHealthCache = mergeOpsHealth(
        state.latestOpsHealthCache || {},
        {
          kpis: payload.kpis || {},
          status: payload.status,
          alerts: payload.alerts,
          suppressedAlertsCount: payload.suppressedAlertsCount,
          alertsEvaluated: payload.alertsEvaluated,
          alertBasis: payload.alertBasis,
          fetchKpisLoaded: true,
          fetchKpisDelayedDuringActiveRun: false,
          summaryView: true
        },
        { summary: true }
      );
      renderOpsHealthSnapshot(targetRenderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload: getCachedTaskStatePayload(),
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        renderDeferredPanels: false,
        renderActivityPanel: false,
        schedulePolling: false
      });
    }
    return payload || null;
  }

  return { loadFetchKpisSummaryData };
}

export function createDashboardHealthHydration({
  state,
  hasPossibleActiveRunEvidence,
  isOpsRouteBackedOff,
  markOpsRouteFailure,
  clearOpsRouteFailure,
  measuredGetBridge,
  mergeOpsHealth,
  renderOpsHealthSnapshot,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  currentRenderToken,
  isStaleRenderToken,
  OPS_DASHBOARD_HEALTH_SUMMARY_PATH,
  OPS_HEAVY_ROUTE_DASHBOARD
}) {
  let dashboardHealthSummaryLoad = null;

  async function loadDashboardHealthSummaryData(renderToken = currentRenderToken(), options = {}) {
    if (!options?.force && hasPossibleActiveRunEvidence({ includeRecent: false })) {
      return state.latestOpsHealthCache || null;
    }
    if (isOpsRouteBackedOff(OPS_HEAVY_ROUTE_DASHBOARD)) {
      return state.latestOpsHealthCache || null;
    }
    if (!dashboardHealthSummaryLoad) {
      dashboardHealthSummaryLoad = measuredGetBridge(
        OPS_DASHBOARD_HEALTH_SUMMARY_PATH,
        "admin_dashboard_health_summary_fetch",
        { enabled: !options?.fromPoll && !options?.silent }
      ).finally(() => {
        dashboardHealthSummaryLoad = null;
      });
    }
    let payload;
    try {
      payload = await dashboardHealthSummaryLoad;
    } catch (err) {
      markOpsRouteFailure(OPS_HEAVY_ROUTE_DASHBOARD);
      throw err;
    }
    if (isStaleRenderToken(renderToken)) return payload || null;
    clearOpsRouteFailure(OPS_HEAVY_ROUTE_DASHBOARD);
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      state.latestOpsHealthCache = mergeOpsHealth(
        state.latestOpsHealthCache || {},
        payload || {},
        { summary: true }
      );
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || payload || {}, {
        taskStatePayload: getCachedTaskStatePayload(),
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        renderDeferredPanels: false,
        renderActivityPanel: false,
        schedulePolling: false
      });
    }
    return payload || null;
  }

  return { loadDashboardHealthSummaryData };
}

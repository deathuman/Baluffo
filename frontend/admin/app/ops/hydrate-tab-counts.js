export function createTabCountsHydration({
  state,
  isOpsRouteBackedOff,
  markOpsRouteFailure,
  clearOpsRouteFailure,
  measuredGetBridge,
  rerenderOpsTabBadges,
  currentRenderToken,
  isStaleRenderToken,
  OPS_TAB_COUNTS_SUMMARY_PATH,
  OPS_HEAVY_ROUTE_TAB_COUNTS
}) {
  let opsTabCountsLoad = null;

  async function loadOpsTabCountsSummaryData(renderToken = currentRenderToken(), options = {}) {
    if (!options?.force && isOpsRouteBackedOff(OPS_HEAVY_ROUTE_TAB_COUNTS)) {
      if (!isStaleRenderToken(renderToken) && !state.latestOpsTabCountsPayload) {
        state.opsTabCountsUnavailable = true;
        rerenderOpsTabBadges();
      }
      return state.latestOpsTabCountsPayload || null;
    }
    if (!opsTabCountsLoad) {
      opsTabCountsLoad = measuredGetBridge(
        OPS_TAB_COUNTS_SUMMARY_PATH,
        "admin_ops_tab_counts_summary_fetch",
        { enabled: !options?.fromPoll && !options?.silent }
      ).finally(() => {
        opsTabCountsLoad = null;
      });
    }
    let payload;
    try {
      payload = await opsTabCountsLoad;
    } catch (err) {
      markOpsRouteFailure(OPS_HEAVY_ROUTE_TAB_COUNTS);
      if (!isStaleRenderToken(renderToken) && !state.latestOpsTabCountsPayload) {
        state.opsTabCountsUnavailable = true;
        rerenderOpsTabBadges();
      }
      throw err;
    }
    const targetRenderToken = options?.renderWithCurrentToken ? currentRenderToken() : renderToken;
    if (isStaleRenderToken(targetRenderToken)) return payload || null;
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      clearOpsRouteFailure(OPS_HEAVY_ROUTE_TAB_COUNTS);
      state.opsTabCountsDelayedDuringActiveRun = false;
      state.opsTabCountsUnavailable = false;
      state.latestOpsTabCountsPayload = payload;
      rerenderOpsTabBadges();
    }
    return payload || null;
  }

  return { loadOpsTabCountsSummaryData };
}

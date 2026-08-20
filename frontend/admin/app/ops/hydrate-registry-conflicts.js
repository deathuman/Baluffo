export function createRegistryConflictsHydration({
  state,
  isOpsRouteBackedOff,
  markOpsRouteFailure,
  clearOpsRouteFailure,
  measuredGetBridge,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  renderOpsHealthSnapshot,
  renderRegistryConflictsQueue,
  isStaleRenderToken,
  REGISTRY_CONFLICTS_SUMMARY_PATH,
  OPS_HEAVY_ROUTE_REGISTRY_CONFLICTS
}) {
  async function loadRegistryConflictsSummaryData(renderToken, options = {}) {
    if (state.registryConflictsDetailLoaded) return getCachedRegistryConflictsPayload();
    if (isOpsRouteBackedOff(OPS_HEAVY_ROUTE_REGISTRY_CONFLICTS)) {
      return getCachedRegistryConflictsPayload();
    }
    try {
      const payload = await measuredGetBridge(
        REGISTRY_CONFLICTS_SUMMARY_PATH,
        "admin_registry_conflicts_summary_fetch",
        { enabled: !options?.fromPoll }
      );
      if (isStaleRenderToken(renderToken) || state.registryConflictsDetailLoaded) return null;
      clearOpsRouteFailure(OPS_HEAVY_ROUTE_REGISTRY_CONFLICTS);
      const registryConflictsPayload = payload
        && typeof payload === "object"
        && !Array.isArray(payload)
        ? payload
        : { summary: { conflictCount: 0 }, summaryStatus: "unavailable", conflicts: [], summaryView: true };
      state.latestRegistryConflictsPayload = registryConflictsPayload;
      state.registryConflictsDetailLoaded = !registryConflictsPayload?.summaryView;
      state.registryConflictCheckRunning = String(registryConflictsPayload?.adjudication?.status || "") === "running";
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload: getCachedTaskStatePayload(),
        registryConflictsPayload,
        renderDeferredPanels: false
      });
      renderRegistryConflictsQueue(registryConflictsPayload);
      return registryConflictsPayload;
    } catch {
      if (isStaleRenderToken(renderToken) || state.registryConflictsDetailLoaded) return null;
      markOpsRouteFailure(OPS_HEAVY_ROUTE_REGISTRY_CONFLICTS);
      const registryConflictsPayload = {
        summary: { conflictCount: 0 },
        summaryStatus: "unavailable",
        conflicts: [],
        summaryView: true
      };
      state.latestRegistryConflictsPayload = registryConflictsPayload;
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload: getCachedTaskStatePayload(),
        registryConflictsPayload,
        renderDeferredPanels: false
      });
      renderRegistryConflictsQueue(registryConflictsPayload);
      return null;
    }
  }

  return { loadRegistryConflictsSummaryData };
}

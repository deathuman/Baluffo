export function createRegistrySyncHydration({
  state,
  renderOpsHealthSnapshot,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  hasRegistrySyncDetails,
  measuredGetBridge,
  mergeOpsHealth,
  currentRenderToken,
  isStaleRenderToken,
  OPS_DASHBOARD_HEALTH_SUMMARY_PATH
}) {
  async function loadRegistrySyncDiagnosticsData(options = {}) {
    const renderToken = currentRenderToken();
    renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
      taskStatePayload: getCachedTaskStatePayload(),
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      renderDeferredPanels: false,
      renderActivityPanel: false,
      schedulePolling: false
    });
    const registrySync = state.latestOpsHealthCache?.kpis?.registrySync;
    const needsRegistrySync = !hasRegistrySyncDetails(registrySync);
    if (!needsRegistrySync) {
      return {
        dashboardHealth: null,
        registrySync,
        kpis: null,
        registryConflicts: null
      };
    }
    const payload = await measuredGetBridge(
      OPS_DASHBOARD_HEALTH_SUMMARY_PATH,
      "admin_registry_sync_diagnostics_fetch",
      { enabled: !options?.silent }
    );
    if (isStaleRenderToken(renderToken)) {
      return {
        dashboardHealth: payload || null,
        registrySync: null,
        kpis: null,
        registryConflicts: null
      };
    }
    const nextRegistrySync = payload?.kpis?.registrySync;
    if (nextRegistrySync && typeof nextRegistrySync === "object" && !Array.isArray(nextRegistrySync)) {
      state.latestOpsHealthCache = mergeOpsHealth(
        state.latestOpsHealthCache || {},
        {
          kpis: {
            registrySync: nextRegistrySync
          },
          summaryView: true
        },
        { summary: true }
      );
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload: getCachedTaskStatePayload(),
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        renderDeferredPanels: false,
        renderActivityPanel: false,
        schedulePolling: false
      });
    }
    return {
      dashboardHealth: payload || null,
      registrySync: nextRegistrySync || null,
      kpis: null,
      registryConflicts: null
    };
  }

  return { loadRegistrySyncDiagnosticsData };
}

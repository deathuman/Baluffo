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
  OPS_FETCH_KPIS_SUMMARY_PATH,
  fetchKpisSummaryFeed = null
}) {
  let fetchKpisLoad = null;

  function applyFetchKpisPayload(payload, targetRenderToken) {
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

  if (fetchKpisSummaryFeed?.subscribe) {
    fetchKpisSummaryFeed.subscribe(payload => {
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) return;
      // A shared refresh is still a fetch-KPI write. Keep the same active-run
      // protection the loader applies before merging it into the Ops cache.
      if (
        !canHydrateCompactDuringActiveRun()
        && hasPossibleActiveRunEvidence({ includeRecent: false })
      ) {
        return;
      }
      applyFetchKpisPayload(payload, currentRenderToken());
    });
  }

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
      const request = fetchKpisSummaryFeed
        ? fetchKpisSummaryFeed.load(
            () => measuredGetBridge(
              OPS_FETCH_KPIS_SUMMARY_PATH,
              "admin_ops_fetch_kpis_summary_fetch",
              { enabled: !options?.fromPoll && !options?.silent }
            ),
            { maxAgeMs: options?.fromPoll || options?.force ? 0 : 5000 }
          )
        : measuredGetBridge(
            OPS_FETCH_KPIS_SUMMARY_PATH,
            "admin_ops_fetch_kpis_summary_fetch",
            { enabled: !options?.fromPoll && !options?.silent }
          );
      fetchKpisLoad = request.finally(() => {
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
    return applyFetchKpisPayload(payload, targetRenderToken);
  }

  return { loadFetchKpisSummaryData };
}

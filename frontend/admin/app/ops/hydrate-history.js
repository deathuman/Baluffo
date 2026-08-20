export function createHistoryHydration({
  state,
  getErrorMessage,
  markStep,
  measuredGetBridge,
  mergeOpsHistoryPayload,
  OPS_HISTORY_STARTUP_PATH,
  OPS_AUTHORITY_FETCH_TIMEOUT_MS,
  scheduleOpsHistoryRetry,
  renderDeferredHistoryDetails
}) {
  let opsHistoryLoad = null;
  let opsHistoryLoadLimit = 0;

  function loadOpsHistoryData(options = {}) {
    const renderToken = Object.prototype.hasOwnProperty.call(options, "renderToken")
      ? Number(options?.renderToken)
      : null;
    const requestedLimit = Math.max(1, Math.min(80, Number(options?.limit) || 2));
    if (opsHistoryLoad) {
      if (requestedLimit <= opsHistoryLoadLimit) return opsHistoryLoad;
      return opsHistoryLoad.catch(() => null).then(() => loadOpsHistoryData(options));
    }
    const path = requestedLimit === 2
      ? OPS_HISTORY_STARTUP_PATH
      : `/ops/history?limit=${encodeURIComponent(String(requestedLimit))}`;
    opsHistoryLoadLimit = requestedLimit;
    state.opsHistoryLoadPending = true;
    state.opsHistoryLastError = "";
    markStep("admin_ops_history_model_fetch_start", {
      limit: requestedLimit
    });
    renderDeferredHistoryDetails(renderToken);
    opsHistoryLoad = measuredGetBridge(
      path,
      "admin_ops_history_fetch",
      {
        enabled: !options?.silent,
        requestOptions: { timeoutMs: OPS_AUTHORITY_FETCH_TIMEOUT_MS }
      }
    )
      .then(payload => {
        if (payload && typeof payload === "object" && !Array.isArray(payload)) {
          const shouldMerge = Boolean(
            state.opsHistoryFullLoaded
            || requestedLimit >= 80
          );
          state.latestOpsHistoryPayload = shouldMerge
            ? mergeOpsHistoryPayload(state.latestOpsHistoryPayload, payload, 80)
            : payload;
          state.opsHistoryLoaded = true;
          if (requestedLimit >= 80) state.opsHistoryFullLoaded = true;
          state.opsHistoryLastError = "";
          state.opsHistoryFailureCount = 0;
          markStep("admin_ops_history_model_fetch_done", {
            ok: true,
            limit: requestedLimit,
            runCount: Array.isArray(payload?.runs) ? payload.runs.length : 0
          });
          renderDeferredHistoryDetails(renderToken);
        }
        return payload || null;
      })
      .catch(err => {
        state.opsHistoryLastError = getErrorMessage(err);
        state.opsHistoryFailureCount = Math.max(1, Number(state.opsHistoryFailureCount || 0) + 1);
        markStep("admin_ops_history_model_fetch_done", {
          ok: false,
          limit: requestedLimit,
          error: String(state.opsHistoryLastError || "unknown error")
        });
        renderDeferredHistoryDetails(renderToken);
        scheduleOpsHistoryRetry();
        return null;
      })
      .finally(() => {
        state.opsHistoryLoadPending = false;
        opsHistoryLoad = null;
        opsHistoryLoadLimit = 0;
        renderDeferredHistoryDetails(renderToken);
      });
    return opsHistoryLoad;
  }

  return { loadOpsHistoryData };
}

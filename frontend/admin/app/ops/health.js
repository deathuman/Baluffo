import { deriveFetcherFailureSummary } from "../../domain.js";
import {
  renderAdminOpsAlerts,
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsHistory,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsTrends
} from "../../render.js?v=12";
import { renderAdminSourcePolicyReview } from "../../render/source-policy-review.js";

function maybeUnrefTimer(timer) {
  timer?.unref?.();
  return timer;
}

const OPS_TAB_KEYS = new Set(["overview", "discovery", "source-policy", "dedup"]);

export function createOpsHealthController({
  state,
  refs,
  getBridge,
  postBridge,
  deriveAdminRunsModel,
  getOpsPollIntervalMs,
  renderAdminOpsAlerts: renderAdminOpsAlertsImpl = renderAdminOpsAlerts,
  renderAdminOpsKpis: renderAdminOpsKpisImpl = renderAdminOpsKpis,
  renderAdminOpsSchedule: renderAdminOpsScheduleImpl = renderAdminOpsSchedule,
  renderAdminOpsDedupLists: renderAdminOpsDedupListsImpl = renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics: renderAdminOpsFetcherMetricsImpl = renderAdminOpsFetcherMetrics,
  renderAdminSourcePolicyReview: renderAdminSourcePolicyReviewImpl = renderAdminSourcePolicyReview,
  renderAdminOpsTrends: renderAdminOpsTrendsImpl = renderAdminOpsTrends,
  renderAdminOpsHistory: renderAdminOpsHistoryImpl = renderAdminOpsHistory,
  loadSyncStatus,
  setBusyFlag,
  showToast,
  getErrorMessage,
  adminDispatch,
  adminActions,
  escapeHtml,
  loadDiscoveryData,
  idlePollIntervalMs,
  taskStateController,
  getBridgeStatus,
  awaitBridgeReady = async () => true
}) {
  let lastDiscoveryRegistryRefreshAtMs = 0;
  let initialBridgeReadyResolved = false;

  function getOpsTabPanels() {
    return {
      overview: refs.adminOpsTabOverviewEl,
      discovery: refs.adminOpsTabDiscoveryEl,
      "source-policy": refs.adminOpsTabSourcePolicyEl,
      dedup: refs.adminOpsTabDedupEl
    };
  }

  function selectOpsTab(tabKey = "overview") {
    const activeKey = OPS_TAB_KEYS.has(tabKey) ? tabKey : "overview";
    state.adminOpsActiveTab = activeKey;
    const buttons = Array.isArray(refs.adminOpsTabBtnEls) ? refs.adminOpsTabBtnEls : [];
    buttons.forEach(button => {
      const buttonKey = String(button?.dataset?.opsTab || button?.getAttribute?.("data-ops-tab") || "");
      const active = buttonKey === activeKey;
      button?.setAttribute?.("aria-selected", active ? "true" : "false");
      button?.classList?.toggle?.("active", active);
      if (button) button.tabIndex = active ? 0 : -1;
    });
    Object.entries(getOpsTabPanels()).forEach(([key, panel]) => {
      if (!panel) return;
      const active = key === activeKey;
      panel.hidden = !active;
      panel.classList?.toggle?.("hidden", !active);
      if (active) {
        panel.removeAttribute?.("hidden");
      } else {
        panel.setAttribute?.("hidden", "");
      }
    });
  }

  function setupOpsTabs() {
    const buttons = Array.isArray(refs.adminOpsTabBtnEls) ? refs.adminOpsTabBtnEls : [];
    if (!buttons.length || state.adminOpsTabsInitialized) {
      selectOpsTab(state.adminOpsActiveTab || "overview");
      return;
    }
    state.adminOpsTabsInitialized = true;
    buttons.forEach(button => {
      button?.addEventListener?.("click", () => {
        selectOpsTab(String(button?.dataset?.opsTab || button?.getAttribute?.("data-ops-tab") || "overview"));
      });
    });
    selectOpsTab(state.adminOpsActiveTab || "overview");
  }

  setupOpsTabs();

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
    if (refs.adminOpsScheduleEl) refs.adminOpsScheduleEl.innerHTML = "";
    if (refs.adminSourcePolicyReviewEl) {
      refs.adminSourcePolicyReviewEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
    }
    if (refs.adminOpsFetcherMetricsEl) refs.adminOpsFetcherMetricsEl.innerHTML = "";
    if (refs.adminOpsDedupListsEl) refs.adminOpsDedupListsEl.innerHTML = "";
    if (refs.adminOpsTrendsEl) refs.adminOpsTrendsEl.textContent = message;
    if (refs.adminOpsHistoryEl) {
      refs.adminOpsHistoryEl.innerHTML = `<div class="no-results">${escapeHtml(message)}</div>`;
    }
  }

  function stopOpsHealthPolling() {
    if (!state.opsHealthPollTimer) return;
    clearTimeout(state.opsHealthPollTimer);
    state.opsHealthPollTimer = null;
  }

  function scheduleOpsHealthPolling(delayMs) {
    stopOpsHealthPolling();
    const waitMs = Math.max(600, Number(delayMs) || 10000);
    state.opsHealthPollTimer = maybeUnrefTimer(setTimeout(() => {
      loadOpsHealthData({ fromPoll: true }).catch(() => {});
    }, waitMs));
  }

  function buildSourcePolicyActionPayload(row, action) {
    const payload = {
      action,
      staticSourceId: String(row?.staticSourceId || ""),
      staticSourceName: String(row?.staticSourceName || ""),
      providerSourceId: String(row?.providerSourceId || ""),
      providerSourceName: String(row?.providerSourceName || "")
    };
    if (action === "snooze") {
      payload.snoozedUntil = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
    }
    return payload;
  }

  function buildMigrationLinkActionPayload(candidate, action) {
    if (action === "apply_migration_identity_link") {
      return { ...(candidate?.recommendedApiPayload || {}) };
    }
    if (action === "clear_migration_identity_link") {
      return {
        action,
        providerSourceId: String(candidate?.providerSourceId || candidate?.recommendedApiPayload?.providerSourceId || ""),
        staticSourceId: String(
          candidate?.selectedStaticSourceId
          || candidate?.staticSourceId
          || candidate?.migrationSourceIdentity
          || candidate?.recommendedApiPayload?.staticSourceId
          || ""
        )
      };
    }
    return { action };
  }

  function buildDedupReviewActionPayload(row, action) {
    return {
      action,
      title: String(row?.title || ""),
      company: String(row?.company || ""),
      dedupKey: String(row?.dedupKey || ""),
      bundleEvidenceOrigin: String(row?.bundleEvidenceOrigin || ""),
      disagreementClassification: String(row?.disagreementClassification || ""),
      providerSourceJobIds: Array.isArray(row?.providerSourceJobIds) ? row.providerSourceJobIds : [],
      staticSourceJobIds: Array.isArray(row?.staticSourceJobIds) ? row.staticSourceJobIds : [],
      providerSources: Array.isArray(row?.providerSources) ? row.providerSources : [],
      staticSources: Array.isArray(row?.staticSources) ? row.staticSources : [],
      providerUrls: Array.isArray(row?.providerUrls) ? row.providerUrls : [],
      staticUrls: Array.isArray(row?.staticUrls) ? row.staticUrls : [],
      sharedIdentifierTokens: Array.isArray(row?.sharedIdentifierTokens) ? row.sharedIdentifierTokens : [],
      distinctLocationCount: Number(row?.distinctLocationCount || 0),
      sampleLocations: Array.isArray(row?.sampleLocations) ? row.sampleLocations : [],
      identityQuality: String(row?.identityQuality || ""),
      carriedLocationPollutionAudit: String(row?.carriedLocationPollutionAudit || "")
    };
  }

  async function handleSourcePolicyAction(row, action) {
    if (!row || !action) return;
    try {
      await postBridge("/source-policy/review-action", buildSourcePolicyActionPayload(row, action));
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not update source policy review: ${getErrorMessage(err)}`, "error");
    }
  }

  async function handleDedupReviewAction(row, action) {
    if (!row || !action) return;
    try {
      await postBridge("/dedup/review-action", buildDedupReviewActionPayload(row, action));
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not update dedup review: ${getErrorMessage(err)}`, "error");
    }
  }

  async function handleCopySectionDiagnostics(section) {
    if (!section || typeof section !== "object" || Array.isArray(section)) return;
    const title = String(section?.title || section?.key || "section");
    const payload = JSON.stringify(section, null, 2);
    if (globalThis.navigator?.clipboard?.writeText) {
      try {
        await globalThis.navigator.clipboard.writeText(payload);
        showToast(`${title} diagnostics copied.`, "success");
        return;
      } catch {
        // Fall through to toast-only failure below.
      }
    }
    showToast(`Could not copy ${title} diagnostics.`, "warn");
  }

  async function handleCopyRunDiagnostics(payload) {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return;
    const title = String(payload?.title || payload?.taskType || "Run");
    const serialized = JSON.stringify(payload, null, 2);
    if (globalThis.navigator?.clipboard?.writeText) {
      try {
        await globalThis.navigator.clipboard.writeText(serialized);
        showToast(`${title} run diagnostics copied.`, "success");
        return;
      } catch {
        // Fall through to toast-only failure below.
      }
    }
    showToast(`Could not copy ${title} run diagnostics.`, "warn");
  }

  async function handleMigrationLinkAction(candidate, action) {
    if (!candidate || !action) return;
    try {
      await postBridge("/source-policy/migration-link-action", buildMigrationLinkActionPayload(candidate, action));
      showToast(
        action === "clear_migration_identity_link"
          ? "Migration identity link cleared."
          : "Migration identity link applied.",
        "success"
      );
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not update migration identity link: ${getErrorMessage(err)}`, "error");
    }
  }

  function renderSourcePolicyReviewQueue(payload = state.latestSourcePolicyRecommendationsPayload || {}) {
    renderAdminSourcePolicyReviewImpl(refs.adminSourcePolicyReviewEl, payload || {}, {
      selectedFilter: state.sourcePolicyReviewFilter || "all",
      onSourcePolicyFilter: filter => {
        state.sourcePolicyReviewFilter = filter || "all";
        renderSourcePolicyReviewQueue(state.latestSourcePolicyRecommendationsPayload || payload || {});
      },
      onSourcePolicyAction: handleSourcePolicyAction,
      onMigrationLinkAction: handleMigrationLinkAction
    });
  }

  async function loadOpsHealthData(options = {}) {
    if (state.adminBusyState.opsLoad) {
      if (options?.fromPoll) scheduleOpsHealthPolling(idlePollIntervalMs);
      return;
    }
    if (!initialBridgeReadyResolved) {
      initialBridgeReadyResolved = true;
      if (!(await awaitBridgeReady())) {
        scheduleOpsHealthPolling(idlePollIntervalMs);
        return;
      }
    }
    setBusyFlag("opsLoad", true);
    const showLoadingState = !options?.fromPoll && !state.latestOpsHealthCache;
    if (showLoadingState && refs.adminOpsTrendsEl) refs.adminOpsTrendsEl.textContent = "Loading operations health...";
    try {
      const [healthResult, historyResult, taskStateResult, fetcherMetricsResult, sourcePolicyResult] = await Promise.allSettled([
        getBridge("/ops/health"),
        getBridge("/ops/history?limit=80"),
        getBridge("/ops/task-state"),
        getBridge("/ops/fetcher-metrics?windowRuns=80"),
        getBridge("/source-policy/recommendations")
      ]);
      const health = (
        healthResult.status === "fulfilled"
        && healthResult.value
        && typeof healthResult.value === "object"
        && !Array.isArray(healthResult.value)
      )
        ? healthResult.value
        : state.latestOpsHealthCache;
      const historyPayload = (
        historyResult.status === "fulfilled"
        && historyResult.value
        && typeof historyResult.value === "object"
        && !Array.isArray(historyResult.value)
      )
        ? historyResult.value
        : (
          state.latestOpsHistoryPayload
          && typeof state.latestOpsHistoryPayload === "object"
          && !Array.isArray(state.latestOpsHistoryPayload)
            ? state.latestOpsHistoryPayload
            : { runs: [] }
        );
      if (healthResult.status === "fulfilled" && health && typeof health === "object") {
        state.latestOpsHealthCache = health || null;
      }
      if (historyResult.status === "fulfilled" && historyPayload && typeof historyPayload === "object") {
        state.latestOpsHistoryPayload = historyPayload;
      }
      const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
      const taskStatePayload = taskStateController.resolveTaskStatePayload(taskStateResult, historyRuns);
      const fetcherMetrics = fetcherMetricsResult.status === "fulfilled"
        ? fetcherMetricsResult.value
        : null;
      const sourcePolicyRecommendations = (
        sourcePolicyResult.status === "fulfilled"
        && sourcePolicyResult.value
        && typeof sourcePolicyResult.value === "object"
        && !Array.isArray(sourcePolicyResult.value)
      )
        ? sourcePolicyResult.value
        : (
          state.latestSourcePolicyRecommendationsPayload
          && typeof state.latestSourcePolicyRecommendationsPayload === "object"
          && !Array.isArray(state.latestSourcePolicyRecommendationsPayload)
            ? state.latestSourcePolicyRecommendationsPayload
            : { recommendations: { pairs: [] } }
        );
      if (sourcePolicyResult.status === "fulfilled" && sourcePolicyRecommendations && typeof sourcePolicyRecommendations === "object") {
        state.latestSourcePolicyRecommendationsPayload = sourcePolicyRecommendations;
      }
      const runModel = deriveAdminRunsModel(
        {
          taskState: taskStatePayload || {},
          historyRuns
        },
        Date.now()
      );
      const liveTaskRows = taskStateController.getActiveTaskRows(taskStatePayload);
      const liveTypes = new Set(
        liveTaskRows
          .map(row => taskStateController.getTaskType(row))
          .filter(Boolean)
      );
      taskStateController.syncLiveBusyFlags(liveTypes);
      taskStateController.maybeAttachLiveTaskRows(liveTaskRows);
      const nowMs = Date.now();
      const discoveryLive = liveTypes.has("discovery");
      if (!discoveryLive) {
        lastDiscoveryRegistryRefreshAtMs = 0;
      } else if (typeof loadDiscoveryData === "function" && nowMs - lastDiscoveryRegistryRefreshAtMs >= 5000) {
        lastDiscoveryRegistryRefreshAtMs = nowMs;
        loadDiscoveryData().catch(() => {});
      }

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
      renderAdminOpsKpisImpl(refs.adminOpsKpisEl, health?.kpis || {}, String(health?.status || "healthy"));
      renderAdminOpsScheduleImpl(refs.adminOpsScheduleEl, health?.schedule || {}, state.latestOpsHealthCache);
      renderSourcePolicyReviewQueue(sourcePolicyRecommendations);
      const fetcherMetricsPayload = {
        ...(fetcherMetrics && typeof fetcherMetrics === "object" ? fetcherMetrics : {}),
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
      renderAdminOpsFetcherMetricsImpl(
        refs.adminOpsFetcherMetricsEl,
        fetcherMetricsPayload,
        deriveFetcherFailureSummary(state.latestFetcherReportCache || {}),
        {
          onDedupReviewAction: handleDedupReviewAction,
          onCopySectionDiagnostics: handleCopySectionDiagnostics,
          runModel
        }
      );
      renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, fetcherMetricsPayload, {
        onDedupReviewAction: handleDedupReviewAction
      });
      renderAdminOpsHistoryImpl(refs.adminOpsHistoryEl, runModel, {
        onCopyRunDiagnostics: handleCopyRunDiagnostics
      });
      renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
      loadSyncStatus({ silent: true }).catch(() => {});
      adminDispatch.dispatch({ type: adminActions.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
      scheduleOpsHealthPolling(getOpsPollIntervalMs(liveTypes.size > 0));
    } catch (err) {
      const retainedLiveTypes = new Set(
        taskStateController.getActiveTaskRows(state.latestTaskStatePayload)
          .map(row => taskStateController.getTaskType(row))
          .filter(Boolean)
      );
      if (getBridgeStatus?.() === "offline" || retainedLiveTypes.size === 0) {
        taskStateController.clearRetainedTaskState();
        setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
        taskStateController.syncLiveBusyFlags(new Set());
        scheduleOpsHealthPolling(idlePollIntervalMs);
      } else {
        taskStateController.syncLiveBusyFlags(retainedLiveTypes);
        scheduleOpsHealthPolling(getOpsPollIntervalMs(true));
      }
    } finally {
      setBusyFlag("opsLoad", false);
    }
  }

  return {
    setOpsPlaceholders,
    stopOpsHealthPolling,
    scheduleOpsHealthPolling,
    loadOpsHealthData,
    selectOpsTab
  };
}

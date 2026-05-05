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
import {
  filterSourcePolicyReviewPairs,
  getMigrationLinkLinkedActions,
  getMigrationLinkReviewActions,
  renderAdminSourcePolicyReview
} from "../../render/source-policy-review.js";
import { renderAdminRegistryConflicts } from "../../render/registry-conflicts.js";

function maybeUnrefTimer(timer) {
  timer?.unref?.();
  return timer;
}

const OPS_TAB_KEYS = new Set(["overview", "discovery", "source-policy", "registry-conflicts", "dedup"]);

function getObjectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function formatBadgeTitle(count, singular, plural = `${singular}s`) {
  const total = Math.max(0, Number(count) || 0);
  return total ? `${total.toLocaleString()} ${total === 1 ? singular : plural}` : `No ${plural}`;
}

function toAlertBadgeState(alerts = []) {
  const rows = Array.isArray(alerts) ? alerts : [];
  const criticalCount = rows.filter(alert => String(alert?.severity || "").toLowerCase() === "critical").length;
  const count = rows.length;
  return {
    count,
    tone: count > 0 ? (criticalCount > 0 ? "critical" : "warning") : "neutral",
    title: count > 0 ? formatBadgeTitle(count, "active alert", "active alerts") : "No active alerts"
  };
}

function toDiscoveryBadgeState(report = {}) {
  const review = getObjectValue(report?.candidateReview);
  const count = Number(review?.totalCandidates || 0);
  return {
    count,
    tone: count > 0 ? "warning" : "neutral",
    title: count > 0 ? formatBadgeTitle(count, "discovery review item", "discovery review items") : "No discovery review items"
  };
}

function toSourcePolicyBadgeState(payload = {}) {
  const rows = Array.isArray(payload?.recommendations?.pairs) ? payload.recommendations.pairs : [];
  const needsActionCount = filterSourcePolicyReviewPairs(rows, "needs_action").length;
  const linkBackfill = getObjectValue(payload?.providerCoverageLinkBackfill);
  const reviewCandidates = Array.isArray(linkBackfill.reviewCandidates) ? linkBackfill.reviewCandidates : [];
  const linkedCandidates = Array.isArray(linkBackfill.linkedCandidates) ? linkBackfill.linkedCandidates : [];
  const blockedCandidates = Array.isArray(linkBackfill.blockedCandidates) ? linkBackfill.blockedCandidates : [];
  const actionableMigrationCount = reviewCandidates.filter(candidate => getMigrationLinkReviewActions(candidate).length > 0).length
    + linkedCandidates.filter(candidate => getMigrationLinkLinkedActions(candidate).length > 0).length;
  const count = needsActionCount + actionableMigrationCount + blockedCandidates.length;
  return {
    count,
    tone: blockedCandidates.length > 0 ? "critical" : count > 0 ? "warning" : "neutral",
    title: count > 0
      ? formatBadgeTitle(count, "source policy review item", "source policy review items")
      : "No source policy review items"
  };
}

function toDedupBadgeState(dedupEvidence = {}) {
  const gate = getObjectValue(dedupEvidence?.dedupAuditGate);
  const providerStaticRows = Array.isArray(dedupEvidence?.providerStaticDisagreementExamples) ? dedupEvidence.providerStaticDisagreementExamples : [];
  const titleCompanyRows = Array.isArray(dedupEvidence?.providerStaticTitleCompanyCollisionExamples) ? dedupEvidence.providerStaticTitleCompanyCollisionExamples : [];
  const reviewQueueRows = Array.isArray(dedupEvidence?.reviewQueue) ? dedupEvidence.reviewQueue : [];
  const reviewCount = providerStaticRows.length + titleCompanyRows.length + reviewQueueRows.length;
  const pressureCount = Math.max(
    Number(gate?.currentRunHighRiskReviewQueueCount || 0),
    Number(gate?.carriedHighRiskReviewQueueCount || 0)
  );
  const gateCount = Number(gate?.blockers?.length || 0) + Number(gate?.warnings?.length || 0);
  const count = Math.max(reviewCount, pressureCount + gateCount);
  return {
    count,
    tone: String(gate?.status || "").toLowerCase() === "blocked" || Number(gate?.blockers?.length || 0) > 0
      ? "critical"
      : count > 0
        ? "warning"
        : "neutral",
    title: count > 0
      ? formatBadgeTitle(count, "dedup review item", "dedup review items")
      : "No dedup review items"
  };
}

function renderOpsTabBadges(refs, {
  health = {},
  discoveryReport = null,
  sourcePolicyRecommendations = {},
  registryConflictsPayload = {},
  fetcherMetricsPayload = {}
} = {}) {
  const badges = Array.isArray(refs?.adminOpsTabBadgeEls) ? refs.adminOpsTabBadgeEls : [];
  if (!badges.length) return;
  const discovery = discoveryReport && typeof discoveryReport === "object"
    ? discoveryReport
    : {};
  const dedupEvidence = getObjectValue(fetcherMetricsPayload?.latestRun?.dedupEvidence);
  const registryConflictsSummary = getObjectValue(registryConflictsPayload?.summary);
  const registryConflictCount = Number(registryConflictsSummary?.conflictCount || 0);
  const badgeStates = {
    overview: toAlertBadgeState(health?.alerts || []),
    discovery: toDiscoveryBadgeState(discovery),
    "source-policy": toSourcePolicyBadgeState(sourcePolicyRecommendations || {}),
    "registry-conflicts": {
      count: registryConflictCount,
      tone: registryConflictCount > 0 ? "warning" : "neutral",
      title: registryConflictCount > 0
        ? formatBadgeTitle(registryConflictCount, "registry conflict", "registry conflicts")
        : "No registry conflicts"
    },
    dedup: toDedupBadgeState(dedupEvidence)
  };
  badges.forEach(badge => {
    const key = String(badge?.dataset?.opsTab || badge?.getAttribute?.("data-ops-tab") || "");
    const state = badgeStates[key] || { count: 0, tone: "neutral", title: "No items" };
    if (badge) {
      badge.textContent = Number(state.count || 0).toLocaleString();
      badge.setAttribute?.("data-badge-tone", state.tone);
      badge.title = state.title;
    }
  });
}

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
  renderAdminRegistryConflicts: renderAdminRegistryConflictsImpl = renderAdminRegistryConflicts,
  renderAdminOpsTrends: renderAdminOpsTrendsImpl = renderAdminOpsTrends,
  renderAdminOpsHistory: renderAdminOpsHistoryImpl = renderAdminOpsHistory,
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
      "registry-conflicts": refs.adminOpsTabRegistryConflictsEl,
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
    renderOpsTabBadges(refs, {
      health: { alerts: [] },
      discoveryReport: {},
      sourcePolicyRecommendations: {},
      fetcherMetricsPayload: {}
    });
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

  function renderRegistryConflictsQueue(payload = state.latestRegistryConflictsPayload || {}) {
    renderAdminRegistryConflictsImpl(refs.adminRegistryConflictsReviewEl, payload || {}, {
      onRegistryConflictAction: handleRegistryConflictAction
    });
  }

  async function handleRegistryConflictAction(row, action) {
    if (!row || !action) return;
    const route = String(action?.route || "").trim();
    const ids = Array.isArray(action?.ids) && action.ids.length > 0
      ? action.ids.map(id => String(id).trim()).filter(Boolean)
      : [row?.id, row?.sourceId]
          .map(id => String(id || "").trim())
          .filter(Boolean);
    if (!route || !ids.length) return;
    try {
      const result = await postBridge(route, { ids });
      const count = Number(
        result?.approved
        ?? result?.rejected
        ?? result?.demoted
        ?? result?.restored
        ?? ids.length
      );
      const actionKey = String(action?.action || "").trim().toLowerCase();
      const noun = count === 1 ? "source" : "sources";
      const message = actionKey === "approve"
        ? `Promoted ${count} ${noun}.`
        : actionKey === "reject"
          ? `Rejected ${count} ${noun}.`
          : actionKey === "demote-active"
            ? `Demoted ${count} ${noun}.`
            : actionKey === "restore-rejected"
              ? `Restored ${count} ${noun}.`
              : `${String(action?.label || "Action")} applied to ${count} ${noun}.`;
      showToast(message, "success");
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not update registry conflict review: ${getErrorMessage(err)}`, "error");
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
      const [
        healthResult,
        historyResult,
        taskStateResult,
        fetcherMetricsResult,
        sourcePolicyResult,
        registryConflictsResult
      ] = await Promise.allSettled([
        getBridge("/ops/health"),
        getBridge("/ops/history?limit=80"),
        getBridge("/ops/task-state"),
        getBridge("/ops/fetcher-metrics?windowRuns=80"),
        getBridge("/source-policy/recommendations"),
        getBridge("/registry/conflicts")
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
      const registryConflictsPayload = (
        registryConflictsResult.status === "fulfilled"
        && registryConflictsResult.value
        && typeof registryConflictsResult.value === "object"
        && !Array.isArray(registryConflictsResult.value)
      )
        ? registryConflictsResult.value
        : (
          state.latestRegistryConflictsPayload
          && typeof state.latestRegistryConflictsPayload === "object"
          && !Array.isArray(state.latestRegistryConflictsPayload)
            ? state.latestRegistryConflictsPayload
            : { summary: { conflictCount: 0 }, conflicts: [] }
        );
      if (registryConflictsResult.status === "fulfilled" && registryConflictsPayload && typeof registryConflictsPayload === "object") {
        state.latestRegistryConflictsPayload = registryConflictsPayload;
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
        Promise.resolve(loadDiscoveryData())
          .then(() => {
            renderOpsTabBadges(refs, {
              health,
              discoveryReport: state.latestDiscoveryReportCache || {},
              sourcePolicyRecommendations,
              registryConflictsPayload,
              fetcherMetricsPayload
            });
          })
          .catch(() => {});
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
      renderRegistryConflictsQueue(registryConflictsPayload);
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
        onCopyRunDiagnostics: handleCopyRunDiagnostics,
        waitingForTaskState: Boolean(state.waitingForTaskState)
      });
      renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
      renderOpsTabBadges(refs, {
        health,
        discoveryReport: state.latestDiscoveryReportCache || {},
        sourcePolicyRecommendations,
        registryConflictsPayload,
        fetcherMetricsPayload
      });
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

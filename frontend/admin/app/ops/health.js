import { deriveFetcherFailureSummary } from "../../domain.js";
import {
  renderAdminOpsAlerts,
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsHistory,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsTrends
} from "../../render.js?v=18";
import {
  filterSourcePolicyReviewPairs,
  getMigrationLinkLinkedActions,
  getMigrationLinkReviewActions,
  renderAdminSourcePolicyReview
} from "../../render/source-policy-review.js?v=6";
import { renderAdminRegistryConflicts } from "../../render/registry-conflicts.js?v=6";
import { setTooltip } from "../../../shared/ui/index.js?v=6";

const OPS_TASK_STATE_SUMMARY_PATH = "/ops/task-state?view=summary";
const OPS_FETCH_KPIS_SUMMARY_PATH = "/ops/fetch-kpis?view=summary";
const OPS_HISTORY_STARTUP_PATH = "/ops/history?limit=2";
const OPS_HISTORY_DETAIL_PATH = "/ops/history?limit=80";
const OPS_FETCHER_METRICS_DETAIL_PATH = "/ops/fetcher-metrics?windowRuns=80";
const OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH = "/ops/discovery-audit-artifacts";
const OPS_TASK_FAILURE_ATTEMPTS_PATH = "/ops/task-failure-attempts";
const OPS_PERFORMANCE_PROFILE_PATH = "/ops/performance-profile";
const SOURCE_POLICY_DETAIL_PATH = "/source-policy/recommendations";
const REGISTRY_CONFLICTS_SUMMARY_PATH = "/registry/conflicts?view=summary";
const REGISTRY_CONFLICTS_DETAIL_PATH = "/registry/conflicts";

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
  const nonPrimaryMergeCounts = getObjectValue(gate?.currentRunNonPrimaryMergeCounts);
  const blockingCount = Math.max(0, Number(gate?.currentRunBlockingReviewQueueCount || 0))
    + Math.max(0, Number(gate?.carriedBlockingReviewQueueCount || 0))
    + Math.max(0, Number(gate?.providerStaticDisagreementBlockedCount || 0))
    + Math.max(0, Number(nonPrimaryMergeCounts?.blocking || 0));
  const monitorCount = Math.max(0, Number(gate?.currentRunMonitorReviewQueueCount || 0))
    + Math.max(0, Number(gate?.carriedMonitorReviewQueueCount || 0));
  const gateCount = Number(gate?.blockers?.length || 0) + Number(gate?.warnings?.length || 0);
  const count = Math.max(reviewCount, blockingCount, gateCount);
  return {
    count,
    tone: String(gate?.status || "").toLowerCase() === "blocked" || Number(gate?.blockers?.length || 0) > 0
      ? "critical"
      : count > 0 || monitorCount > 0
        ? "warning"
        : "neutral",
    title: count > 0
      ? formatBadgeTitle(count, "dedup blocker", "dedup blockers")
      : monitorCount > 0
        ? formatBadgeTitle(monitorCount, "dedup diagnostic", "dedup diagnostics")
        : "No dedup blockers"
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
  const registrySummaryStatus = String(registryConflictsPayload?.summaryStatus || "").toLowerCase();
  const registryConflictCount = Number(registryConflictsSummary?.conflictCount || 0);
  const registryConflictBadgeState = registrySummaryStatus === "pending" || registrySummaryStatus === "unavailable"
    ? {
        count: 0,
        tone: "neutral",
        title: registrySummaryStatus === "unavailable"
          ? "Registry conflict summary unavailable"
          : "Registry conflict summary pending"
      }
    : {
        count: registryConflictCount,
        tone: registryConflictCount > 0 ? "warning" : "neutral",
        title: registryConflictCount > 0
          ? formatBadgeTitle(registryConflictCount, "registry conflict", "registry conflicts")
          : "No registry conflicts"
      };
  const badgeStates = {
    overview: toAlertBadgeState(health?.alerts || []),
    discovery: toDiscoveryBadgeState(discovery),
    "source-policy": toSourcePolicyBadgeState(sourcePolicyRecommendations || {}),
    "registry-conflicts": registryConflictBadgeState,
    dedup: toDedupBadgeState(dedupEvidence)
  };
  badges.forEach(badge => {
    const key = String(badge?.dataset?.opsTab || badge?.getAttribute?.("data-ops-tab") || "");
    const state = badgeStates[key] || { count: 0, tone: "neutral", title: "No items" };
    if (badge) {
      badge.textContent = Number(state.count || 0).toLocaleString();
      badge.setAttribute?.("data-badge-tone", state.tone);
      setTooltip(badge, state.title);
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
  idlePollIntervalMs,
  taskStateController,
  getBridgeStatus,
  awaitBridgeReady = async () => true,
  markAdminStep,
  measureAdminStep,
  getFrontendPerfCounters = () => {
    try {
      return globalThis.__baluffoSnapshotFrontendPerfCounters?.() || {};
    } catch {
      return {};
    }
  },
  renderScheduler
}) {
  let initialBridgeReadyResolved = false;
  let opsRenderToken = 0;
  let opsOverviewDetailLoad = null;
  let opsOverviewDetailLoadToken = 0;
  let fetchKpisLoad = null;
  let opsHistoryLoad = null;
  let opsHistoryLoadLimit = 0;
  let sourcePolicyDetailLoad = null;
  let registryConflictsDetailLoad = null;

  function markStep(name, payload = {}) {
    if (typeof markAdminStep === "function") markAdminStep(name, payload);
  }

  function measureStep(name, startMark, endMark, payload = {}) {
    if (typeof measureAdminStep === "function") measureAdminStep(name, startMark, endMark, payload);
  }

  async function measuredGetBridge(path, metricName, { enabled = true } = {}) {
    const startMark = `${metricName}_start`;
    const endMark = `${metricName}_done`;
    if (enabled) markStep(startMark);
    try {
      const payload = await getBridge(path);
      if (enabled) {
        markStep(endMark, { ok: true });
        measureStep(metricName, startMark, endMark, { ok: true });
      }
      return payload;
    } catch (err) {
      if (enabled) {
        markStep(endMark, { ok: false, error: String(err?.message || err || "unknown error") });
        measureStep(metricName, startMark, endMark, { ok: false });
      }
      throw err;
    }
  }

  function isPlainObject(value) {
    return value && typeof value === "object" && !Array.isArray(value);
  }

  function hasUsefulValue(value) {
    if (value === null || value === undefined) return false;
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      return Boolean(normalized)
        && normalized !== "unknown"
        && normalized !== "never"
        && normalized !== "none"
        && normalized !== "not loaded yet";
    }
    return true;
  }

  const FETCH_KPI_ZERO_CAN_BE_PLACEHOLDER = new Set([
    "sevenDayFetchSuccessRate",
    "avgFetchDurationMs7d",
    "failedSourceRatioLatest"
  ]);

  function mergeKpis(existing = {}, incoming = {}, { preserveExisting = false } = {}) {
    const result = isPlainObject(existing) ? { ...existing } : {};
    if (!isPlainObject(incoming)) return result;
    Object.entries(incoming).forEach(([key, value]) => {
      if (key === "registrySync" && isPlainObject(value)) {
        result.registrySync = mergePlainObjects(
          isPlainObject(result.registrySync) ? result.registrySync : {},
          value,
          { preserveUnknowns: preserveExisting }
        );
        return;
      }
      if (
        preserveExisting
        && Object.prototype.hasOwnProperty.call(result, key)
        && hasUsefulValue(result[key])
        && (
          !hasUsefulValue(value)
          || (FETCH_KPI_ZERO_CAN_BE_PLACEHOLDER.has(key) && Number(value) === 0)
        )
      ) {
        return;
      }
      if (hasUsefulValue(value) || !Object.prototype.hasOwnProperty.call(result, key)) {
        result[key] = value;
      }
    });
    return result;
  }

  function mergePlainObjects(existing = {}, incoming = {}, { preserveUnknowns = false } = {}) {
    const result = isPlainObject(existing) ? { ...existing } : {};
    if (!isPlainObject(incoming)) return result;
    Object.entries(incoming).forEach(([key, value]) => {
      if (
        preserveUnknowns
        && Object.prototype.hasOwnProperty.call(result, key)
        && hasUsefulValue(result[key])
        && !hasUsefulValue(value)
      ) {
        return;
      }
      result[key] = value;
    });
    return result;
  }

  function mergeOpsHealth(existing = {}, incoming = {}, { summary = false } = {}) {
    const base = isPlainObject(existing) ? existing : {};
    const patch = isPlainObject(incoming) ? incoming : {};
    const preserveExisting = Boolean(summary);
    const merged = {
      ...base,
      ...patch,
      kpis: mergeKpis(base.kpis, patch.kpis, { preserveExisting })
    };
    if (isPlainObject(base.schedule) || isPlainObject(patch.schedule)) {
      merged.schedule = mergePlainObjects(base.schedule, patch.schedule, {
        preserveUnknowns: preserveExisting
      });
    }
    return merged;
  }

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
    return loadActiveOpsTabDetail(activeKey).catch(() => {});
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

  async function handlePipelineScheduleSave(button) {
    const root = refs.adminOpsScheduleEl;
    if (!root) return;
    const enabledEl = root.querySelector?.('[data-ui="admin-pipeline-schedule-enabled"]');
    const intervalEl = root.querySelector?.('[data-ui="admin-pipeline-schedule-interval"]');
    const intervalHours = Number(intervalEl?.value || 0);
    if (!Number.isInteger(intervalHours) || intervalHours < 1 || intervalHours > 168) {
      showToast("Pipeline schedule interval must be between 1 and 168 hours.", "error");
      return;
    }
    if (button) button.disabled = true;
    try {
      const result = await postBridge("/tasks/jobs-pipeline-schedule", {
        enabled: Boolean(enabledEl?.checked),
        intervalHours
      });
      if (result?.ok === false) {
        throw new Error(String(result?.error || "schedule save failed"));
      }
      showToast("Pipeline schedule saved.", "success");
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not save pipeline schedule: ${getErrorMessage(err)}`, "error");
    } finally {
      if (button) button.disabled = false;
    }
  }

  function setupPipelineScheduleControls() {
    const root = refs.adminOpsScheduleEl;
    if (!root || state.adminPipelineScheduleControlsInitialized) return;
    if (typeof root.addEventListener !== "function") return;
    state.adminPipelineScheduleControlsInitialized = true;
    root.addEventListener("click", event => {
      const button = event.target?.closest?.('[data-action="save-pipeline-schedule"]');
      if (!button) return;
      event.preventDefault?.();
      handlePipelineScheduleSave(button).catch(() => {});
    });
  }

  setupOpsTabs();
  setupPipelineScheduleControls();

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
      loadOpsHealthData({ fromPoll: true, summary: true }).catch(() => {});
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
      await loadSourcePolicyDetail({ force: true });
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

  async function handleRefreshAuditArtifacts() {
    state.latestDiscoveryAuditArtifactsPayload = { ok: true, artifacts: [] };
    try {
      await loadOpsOverviewDetailData(opsRenderToken);
      showToast("Discovery audit artifacts refreshed.", "success");
    } catch (err) {
      showToast(`Could not refresh discovery audit artifacts: ${getErrorMessage(err)}`, "warn");
    }
  }

  async function handleRefreshTaskFailureAttempts() {
    state.latestTaskFailureAttemptsPayload = { ok: true, fetch: {}, discovery: {}, warnings: [] };
    try {
      await loadOpsOverviewDetailData(opsRenderToken);
      showToast("Task failure-attempt diagnostics refreshed.", "success");
    } catch (err) {
      showToast(`Could not refresh task failure-attempt diagnostics: ${getErrorMessage(err)}`, "warn");
    }
  }

  async function handleRefreshPerformanceProfile() {
    state.latestOpsPerformanceProfilePayload = { ok: true, routeTimings: { routes: [] }, operationTimings: { operations: [] } };
    try {
      await loadOpsOverviewDetailData(opsRenderToken);
      showToast("Performance diagnostics refreshed.", "success");
    } catch (err) {
      showToast(`Could not refresh performance diagnostics: ${getErrorMessage(err)}`, "warn");
    }
  }

  async function handleAbortRun(row) {
    const taskType = String(row?.taskType || "").trim().toLowerCase();
    const runId = String(row?.runId || "").trim();
    if (!taskType || !runId) return;
    const confirmed = typeof globalThis.confirm === "function"
      ? globalThis.confirm(`Abort ${taskType} task ${runId}?`)
      : true;
    if (!confirmed) return;
    try {
      const result = await postBridge("/tasks/abort", {
        taskType,
        runId,
        reason: "admin_ops_abort",
      });
      if (!result?.ok && !result?.abortAccepted) {
        throw new Error(String(result?.error || "abort failed"));
      }
      showToast("Task abort requested.", "success");
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not abort task: ${getErrorMessage(err)}`, "error");
    }
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
      await loadSourcePolicyDetail({ force: true });
    } catch (err) {
      showToast(`Could not update migration identity link: ${getErrorMessage(err)}`, "error");
    }
  }

  function renderRegistryConflictsQueue(payload = state.latestRegistryConflictsPayload || {}) {
    if (payload?.summaryView && refs.adminRegistryConflictsReviewEl) {
      const conflictCount = Number(payload?.summary?.conflictCount || 0);
      const summaryStatus = String(payload?.summaryStatus || "").toLowerCase();
      refs.adminRegistryConflictsReviewEl.innerHTML = summaryStatus === "pending"
        ? '<div class="muted">Registry conflict summary is loading in the background. Details load when this panel is opened.</div>'
        : summaryStatus === "unavailable"
          ? '<div class="muted">Registry conflict summary is unavailable. Details load when this panel is opened.</div>'
          : conflictCount > 0
        ? `<div class="muted">${escapeHtml(`${conflictCount.toLocaleString()} registry conflict(s) detected. Details load when this panel is opened.`)}</div>`
        : '<div class="muted">No registry conflicts detected.</div>';
      return;
    }
    const adjudication = getObjectValue(payload?.adjudication);
    const conflictCheckRunning = Boolean(state.registryConflictCheckRunning)
      || String(adjudication?.status || "") === "running";
    renderAdminRegistryConflictsImpl(refs.adminRegistryConflictsReviewEl, payload || {}, {
      onRegistryConflictAction: handleRegistryConflictAction,
      onRegistryConflictSafeAutomation: handleRegistryConflictSafeAutomation,
      onRegistryConflictCheck: handleRegistryConflictCheck,
      checkingConflicts: conflictCheckRunning
    });
  }

  async function handleRegistryConflictCheck(options = {}) {
    if (state.registryConflictCheckRunning) return;
    state.registryConflictCheckRunning = true;
    renderRegistryConflictsQueue(state.latestRegistryConflictsPayload || {});
    try {
      const result = await postBridge("/registry/conflicts/check-sources", {
        applyAutopilot: Boolean(options?.applyAutopilot)
      });
      const started = Boolean(result?.started);
      const alreadyRunning = Boolean(result?.alreadyRunning);
      const demoted = Number(result?.demoted || 0);
      const checked = Number(result?.checkedSourceCount || 0);
      if (started || alreadyRunning || String(result?.status || "") === "running") {
        showToast(
          alreadyRunning
            ? "Conflict source check is already running."
            : (
              options?.applyAutopilot
                ? "Conflict source check started; high-confidence recommendations will apply when probes finish."
                : "Conflict source check started."
            ),
          "success"
        );
      } else {
        showToast(
          options?.applyAutopilot
            ? `Conflict source check finished: ${demoted} demoted, ${checked} checked.`
            : `Conflict source check finished: ${checked} checked.`,
          "success"
        );
      }
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not check conflicting sources: ${getErrorMessage(err)}`, "error");
    } finally {
      state.registryConflictCheckRunning = false;
      renderRegistryConflictsQueue(state.latestRegistryConflictsPayload || {});
    }
  }

  async function handleRegistryConflictSafeAutomation(safeAutomation) {
    if (!safeAutomation || !safeAutomation.action) return;
    const route = String(safeAutomation?.route || "/registry/conflicts/auto-demote-safe").trim();
    const ids = Array.isArray(safeAutomation?.targetIds)
      ? safeAutomation.targetIds.map(id => String(id).trim()).filter(Boolean)
      : [];
    if (!route) return;
    try {
      const result = await postBridge(route, {
        action: String(safeAutomation.action || "auto_demote_same_adapter_provider_alias"),
        ids
      });
      const demoted = Number(result?.demoted || 0);
      const skipped = Number(result?.skipped || 0);
      showToast(`Safe auto-demotion applied: ${demoted} demoted, ${skipped} skipped.`, "success");
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not apply safe registry automation: ${getErrorMessage(err)}`, "error");
    }
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

  function setOpsReadinessShell() {
    if (refs.adminOpsAlertsEl) refs.adminOpsAlertsEl.innerHTML = "";
    if (refs.adminOpsKpisEl) refs.adminOpsKpisEl.innerHTML = "";
    if (refs.adminOpsScheduleEl) refs.adminOpsScheduleEl.innerHTML = "";
    if (refs.adminSourcePolicyReviewEl) refs.adminSourcePolicyReviewEl.innerHTML = "";
    if (refs.adminOpsFetcherMetricsEl) refs.adminOpsFetcherMetricsEl.innerHTML = "";
    if (refs.adminOpsDedupListsEl) refs.adminOpsDedupListsEl.innerHTML = "";
    if (refs.adminOpsTrendsEl) {
      refs.adminOpsTrendsEl.textContent = "No run trend data yet.";
    }
    if (refs.adminOpsHistoryEl) refs.adminOpsHistoryEl.innerHTML = "";
    renderOpsTabBadges(refs, {
      health: { alerts: [] },
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations: getCachedSourcePolicyPayload(),
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      fetcherMetricsPayload: buildFetcherMetricsPayload()
    });
  }

  function getRenderScheduler() {
    return typeof renderScheduler === "function"
      ? renderScheduler
      : callback => {
        callback();
        return () => {};
      };
  }

  function getCachedHistoryPayload() {
    return state.latestOpsHistoryPayload
      && typeof state.latestOpsHistoryPayload === "object"
      && !Array.isArray(state.latestOpsHistoryPayload)
      ? state.latestOpsHistoryPayload
      : { runs: [] };
  }

  function getCachedSourcePolicyPayload() {
    return state.latestSourcePolicyRecommendationsPayload
      && typeof state.latestSourcePolicyRecommendationsPayload === "object"
      && !Array.isArray(state.latestSourcePolicyRecommendationsPayload)
      ? state.latestSourcePolicyRecommendationsPayload
      : { recommendations: { pairs: [] } };
  }

  function getCachedRegistryConflictsPayload() {
    return state.latestRegistryConflictsPayload
      && typeof state.latestRegistryConflictsPayload === "object"
      && !Array.isArray(state.latestRegistryConflictsPayload)
      ? state.latestRegistryConflictsPayload
      : { summary: { conflictCount: 0 }, conflicts: [] };
  }

  function getCachedDiscoveryAuditArtifactsPayload() {
    return state.latestDiscoveryAuditArtifactsPayload
      && typeof state.latestDiscoveryAuditArtifactsPayload === "object"
      && !Array.isArray(state.latestDiscoveryAuditArtifactsPayload)
      ? state.latestDiscoveryAuditArtifactsPayload
      : { ok: true, artifacts: [] };
  }

  function getCachedTaskFailureAttemptsPayload() {
    return state.latestTaskFailureAttemptsPayload
      && typeof state.latestTaskFailureAttemptsPayload === "object"
      && !Array.isArray(state.latestTaskFailureAttemptsPayload)
      ? state.latestTaskFailureAttemptsPayload
      : { ok: true, fetch: {}, discovery: {}, warnings: [] };
  }

  function getCachedPerformanceProfilePayload() {
    return state.latestOpsPerformanceProfilePayload
      && typeof state.latestOpsPerformanceProfilePayload === "object"
      && !Array.isArray(state.latestOpsPerformanceProfilePayload)
      ? state.latestOpsPerformanceProfilePayload
      : { ok: true, routeTimings: { routes: [] }, operationTimings: { operations: [] } };
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
    renderOpsTabBadges(refs, {
      health: state.latestOpsHealthCache || {},
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations: getCachedSourcePolicyPayload(),
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      fetcherMetricsPayload: buildFetcherMetricsPayload()
    });
  }

  function renderDeferredOverviewDetails(renderToken = opsRenderToken) {
    if (renderToken !== opsRenderToken) return;
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
    const fetcherMetricsPayload = buildFetcherMetricsPayload();
    rerenderOpsTabBadges();
    getRenderScheduler()(() => {
      if (renderToken !== opsRenderToken) return;
      renderAdminOpsFetcherMetricsImpl(
        refs.adminOpsFetcherMetricsEl,
        fetcherMetricsPayload,
        deriveFetcherFailureSummary(state.latestFetcherReportCache || {}),
        {
          onDedupReviewAction: handleDedupReviewAction,
          onCopySectionDiagnostics: handleCopySectionDiagnostics,
          onRefreshAuditArtifacts: handleRefreshAuditArtifacts,
          onRefreshTaskFailureAttempts: handleRefreshTaskFailureAttempts,
          onRefreshPerformanceProfile: handleRefreshPerformanceProfile,
          onLoadDebugDiagnostics: handleLoadDebugDiagnostics,
          includeDebugDiagnostics: Boolean(state.opsDebugDiagnosticsLoaded),
          debugDiagnosticsLoading: Boolean(state.opsDebugDiagnosticsLoading),
          runModel
        }
      );
      renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, fetcherMetricsPayload, {
        onDedupReviewAction: handleDedupReviewAction
      });
      renderAdminOpsHistoryImpl(refs.adminOpsHistoryEl, runModel, {
        onCopyRunDiagnostics: handleCopyRunDiagnostics,
        onAbortRun: handleAbortRun,
        waitingForTaskState: Boolean(state.waitingForTaskState),
        taskStateUnavailable: Boolean(state.taskStateUnavailable),
        historyPending: Boolean(state.opsHistoryLoadPending),
        historyLoaded: Boolean(state.opsHistoryLoaded),
        historyFullLoaded: Boolean(state.opsHistoryFullLoaded)
      });
      renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
    });
  }

  function renderDeferredHistoryDetails(renderToken = opsRenderToken) {
    if (renderToken !== opsRenderToken) return;
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
    getRenderScheduler()(() => {
      if (renderToken !== opsRenderToken) return;
      renderAdminOpsHistoryImpl(refs.adminOpsHistoryEl, runModel, {
        onCopyRunDiagnostics: handleCopyRunDiagnostics,
        onAbortRun: handleAbortRun,
        waitingForTaskState: Boolean(state.waitingForTaskState),
        taskStateUnavailable: Boolean(state.taskStateUnavailable),
        historyPending: Boolean(state.opsHistoryLoadPending),
        historyLoaded: Boolean(state.opsHistoryLoaded),
        historyFullLoaded: Boolean(state.opsHistoryFullLoaded)
      });
      renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
    });
  }

  function loadOpsHistoryData(options = {}) {
    const renderToken = Number(options?.renderToken) || opsRenderToken;
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
    renderDeferredHistoryDetails(renderToken);
    opsHistoryLoad = measuredGetBridge(
      path,
      "admin_ops_history_fetch",
      { enabled: !options?.silent }
    )
      .then(payload => {
        if (payload && typeof payload === "object" && !Array.isArray(payload)) {
          state.latestOpsHistoryPayload = payload;
          state.opsHistoryLoaded = true;
          if (requestedLimit >= 80) state.opsHistoryFullLoaded = true;
          renderDeferredHistoryDetails(renderToken);
        }
        return payload || null;
      })
      .finally(() => {
        state.opsHistoryLoadPending = false;
        opsHistoryLoad = null;
        opsHistoryLoadLimit = 0;
        renderDeferredHistoryDetails(renderToken);
      });
    return opsHistoryLoad;
  }

  function loadOpsOverviewDetailData(renderToken = opsRenderToken) {
    if (opsOverviewDetailLoad && opsOverviewDetailLoadToken === renderToken) return opsOverviewDetailLoad;
    state.opsDebugDiagnosticsLoading = true;
    opsOverviewDetailLoadToken = renderToken;
    renderDeferredOverviewDetails(renderToken);
    const detailLoad = (async () => {
      const [
        historyResult,
        fetcherMetricsResult,
        auditArtifactsResult,
        taskFailureAttemptsResult,
        performanceProfileResult
      ] = await Promise.allSettled([
        getBridge(OPS_HISTORY_DETAIL_PATH),
        getBridge(OPS_FETCHER_METRICS_DETAIL_PATH),
        getBridge(OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH),
        getBridge(OPS_TASK_FAILURE_ATTEMPTS_PATH),
        getBridge(OPS_PERFORMANCE_PROFILE_PATH)
      ]);
      if (renderToken !== opsRenderToken) return;
      let changed = false;
      if (
        historyResult.status === "fulfilled"
        && historyResult.value
        && typeof historyResult.value === "object"
        && !Array.isArray(historyResult.value)
      ) {
        state.latestOpsHistoryPayload = historyResult.value;
        state.opsHistoryLoaded = true;
        state.opsHistoryFullLoaded = true;
        changed = true;
      }
      if (
        fetcherMetricsResult.status === "fulfilled"
        && fetcherMetricsResult.value
        && typeof fetcherMetricsResult.value === "object"
        && !Array.isArray(fetcherMetricsResult.value)
      ) {
        state.latestOpsFetcherMetricsPayload = fetcherMetricsResult.value;
        changed = true;
      }
      if (
        auditArtifactsResult.status === "fulfilled"
        && auditArtifactsResult.value
        && typeof auditArtifactsResult.value === "object"
        && !Array.isArray(auditArtifactsResult.value)
      ) {
        state.latestDiscoveryAuditArtifactsPayload = auditArtifactsResult.value;
        changed = true;
      }
      if (
        taskFailureAttemptsResult.status === "fulfilled"
        && taskFailureAttemptsResult.value
        && typeof taskFailureAttemptsResult.value === "object"
        && !Array.isArray(taskFailureAttemptsResult.value)
      ) {
        state.latestTaskFailureAttemptsPayload = taskFailureAttemptsResult.value;
        changed = true;
      }
      if (
        performanceProfileResult.status === "fulfilled"
        && performanceProfileResult.value
        && typeof performanceProfileResult.value === "object"
        && !Array.isArray(performanceProfileResult.value)
      ) {
        state.latestOpsPerformanceProfilePayload = performanceProfileResult.value;
        changed = true;
      }
      if (changed) {
        state.opsDebugDiagnosticsLoaded = true;
        renderDeferredOverviewDetails(renderToken);
      }
    })().finally(() => {
      if (opsOverviewDetailLoad === detailLoad) {
        opsOverviewDetailLoad = null;
        opsOverviewDetailLoadToken = 0;
        state.opsDebugDiagnosticsLoading = false;
        renderDeferredOverviewDetails(renderToken);
      }
    });
    opsOverviewDetailLoad = detailLoad;
    return opsOverviewDetailLoad;
  }

  function handleLoadDebugDiagnostics() {
    return loadOpsOverviewDetailData(opsRenderToken).catch(err => {
      showToast(`Could not load debug diagnostics: ${getErrorMessage(err)}`, "error");
    });
  }

  function scheduleOpsOverviewDetailData(renderToken = opsRenderToken) {
    maybeUnrefTimer(setTimeout(() => {
      loadOpsOverviewDetailData(renderToken).catch(() => {});
    }, 0));
  }

  async function loadSourcePolicyDetail({ force = false } = {}) {
    if (!force && state.sourcePolicyRecommendationsDetailLoaded) {
      return getCachedSourcePolicyPayload();
    }
    if (sourcePolicyDetailLoad) return sourcePolicyDetailLoad;
    sourcePolicyDetailLoad = (async () => {
      try {
        const payload = await getBridge(SOURCE_POLICY_DETAIL_PATH);
        const sourcePolicyRecommendations = payload
          && typeof payload === "object"
          && !Array.isArray(payload)
          ? payload
          : { recommendations: { pairs: [] } };
        state.latestSourcePolicyRecommendationsPayload = sourcePolicyRecommendations;
        state.sourcePolicyRecommendationsDetailLoaded = true;
        renderSourcePolicyReviewQueue(sourcePolicyRecommendations);
        rerenderOpsTabBadges();
        return sourcePolicyRecommendations;
      } catch (err) {
        if (refs.adminSourcePolicyReviewEl) {
          refs.adminSourcePolicyReviewEl.innerHTML = `<div class="muted">${escapeHtml(`Could not load source policy details: ${getErrorMessage(err)}`)}</div>`;
        }
        return null;
      }
    })().finally(() => {
      sourcePolicyDetailLoad = null;
    });
    return sourcePolicyDetailLoad;
  }

  async function loadRegistryConflictsDetail({ force = false } = {}) {
    if (!force && state.registryConflictsDetailLoaded) {
      return getCachedRegistryConflictsPayload();
    }
    if (registryConflictsDetailLoad) return registryConflictsDetailLoad;
    registryConflictsDetailLoad = (async () => {
      try {
        const payload = await getBridge(REGISTRY_CONFLICTS_DETAIL_PATH);
        const registryConflictsPayload = payload
          && typeof payload === "object"
          && !Array.isArray(payload)
          ? payload
          : { summary: { conflictCount: 0 }, conflicts: [] };
        state.latestRegistryConflictsPayload = registryConflictsPayload;
        state.registryConflictsDetailLoaded = true;
        state.registryConflictCheckRunning = String(registryConflictsPayload?.adjudication?.status || "") === "running";
        renderRegistryConflictsQueue(registryConflictsPayload);
        rerenderOpsTabBadges();
        return registryConflictsPayload;
      } catch (err) {
        if (refs.adminRegistryConflictsReviewEl) {
          refs.adminRegistryConflictsReviewEl.innerHTML = `<div class="muted">${escapeHtml(`Could not load registry conflict details: ${getErrorMessage(err)}`)}</div>`;
        }
        return null;
      }
    })().finally(() => {
      registryConflictsDetailLoad = null;
    });
    return registryConflictsDetailLoad;
  }

  function loadActiveOpsTabDetail(tabKey = state.adminOpsActiveTab || "overview", { force = false } = {}) {
    if (tabKey === "source-policy") return loadSourcePolicyDetail({ force });
    if (tabKey === "registry-conflicts") return loadRegistryConflictsDetail({ force });
    return Promise.resolve(null);
  }

  function getCachedTaskStatePayload() {
    return getObjectValue(state.latestOpsTaskStatePayload);
  }

  function hasActiveRows(taskStatePayload = getCachedTaskStatePayload()) {
    const rows = Array.isArray(taskStatePayload?.tasks) ? taskStatePayload.tasks : [];
    return rows.some(row => row && row.active !== false && !row.finishedAt);
  }

  function hasOptimisticRows() {
    return Boolean(state.discoveryOptimisticRun || state.fetchOptimisticRun);
  }

  function hasFetchKpiValues(kpis = state.latestOpsHealthCache?.kpis || {}) {
    return [
      "lastSuccessfulFetchAt",
      "lastSuccessfulFetchAge",
      "sevenDayFetchSuccessRate",
      "avgFetchDurationMs7d",
      "failedSourceRatioLatest"
    ].some(key => hasUsefulValue(kpis?.[key]));
  }

  function deriveLiveRunContext(taskStatePayload, registryConflictsPayload) {
    const historyPayload = getCachedHistoryPayload();
    const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
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
    const registryConflictRunning = String(registryConflictsPayload?.adjudication?.status || "") === "running";
    return {
      historyRuns,
      runModel,
      liveTaskRows,
      liveTypes,
      registryConflictRunning
    };
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
    if (renderToken !== opsRenderToken) return;
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
    renderOpsTabBadges(refs, {
      health,
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations,
      registryConflictsPayload,
      fetcherMetricsPayload
    });
    renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
    const historyRenderOptions = {
      onCopyRunDiagnostics: handleCopyRunDiagnostics,
      onAbortRun: handleAbortRun,
      waitingForTaskState: Boolean(state.waitingForTaskState),
      taskStateUnavailable: Boolean(state.taskStateUnavailable),
      historyPending: Boolean(state.opsHistoryLoadPending),
      historyLoaded: Boolean(state.opsHistoryLoaded),
      historyFullLoaded: Boolean(state.opsHistoryFullLoaded)
    };
    if (renderActivityPanel) {
      renderAdminOpsHistoryImpl(refs.adminOpsHistoryEl, runModel, historyRenderOptions);
    }
    if (renderDeferredPanels) {
      getRenderScheduler()(() => {
        if (renderToken !== opsRenderToken) return;
        renderSourcePolicyReviewQueue(getCachedSourcePolicyPayload());
        renderRegistryConflictsQueue(getCachedRegistryConflictsPayload());
        renderAdminOpsFetcherMetricsImpl(
          refs.adminOpsFetcherMetricsEl,
          fetcherMetricsPayload,
          deriveFetcherFailureSummary(state.latestFetcherReportCache || {}),
          {
            onDedupReviewAction: handleDedupReviewAction,
            onCopySectionDiagnostics: handleCopySectionDiagnostics,
            onRefreshAuditArtifacts: handleRefreshAuditArtifacts,
            onRefreshTaskFailureAttempts: handleRefreshTaskFailureAttempts,
            onRefreshPerformanceProfile: handleRefreshPerformanceProfile,
            onLoadDebugDiagnostics: handleLoadDebugDiagnostics,
            includeDebugDiagnostics: Boolean(state.opsDebugDiagnosticsLoaded),
            debugDiagnosticsLoading: Boolean(state.opsDebugDiagnosticsLoading),
            runModel
          }
        );
        renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, fetcherMetricsPayload, {
          onDedupReviewAction: handleDedupReviewAction
        });
        renderAdminOpsHistoryImpl(refs.adminOpsHistoryEl, runModel, {
          ...historyRenderOptions
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

  function applyBootstrapPayload(payload = {}) {
    const renderToken = ++opsRenderToken;
    const tasks = payload?.tasks && typeof payload.tasks === "object" ? payload.tasks : {};
    const currentRows = Array.isArray(tasks.current) ? tasks.current : [];
    const recentRows = Array.isArray(tasks.recent) ? tasks.recent : [];
    const taskStatePayload = {
      tasks: currentRows,
      count: currentRows.length,
      summary: true
    };
    const historyPayload = {
      runs: recentRows,
      count: recentRows.length,
      summaryView: true
    };
    const registrySummary = payload?.registrySummary && typeof payload.registrySummary === "object"
      ? payload.registrySummary
      : {};
    const kpis = {};
    if (Object.keys(registrySummary).length) {
      kpis.registrySync = { ...registrySummary };
      if (Object.prototype.hasOwnProperty.call(registrySummary, "pendingCount")) {
        kpis.pendingApprovalsCount = registrySummary.pendingCount;
      }
    }
    const health = {
      ok: true,
      status: "healthy",
      summaryView: true,
      alerts: [],
      kpis,
      schedule: payload?.schedule && typeof payload.schedule === "object" ? payload.schedule : {},
      appVersion: String(payload?.app?.version || "")
    };
    state.latestOpsHealthCache = mergeOpsHealth(state.latestOpsHealthCache || {}, health, { summary: true });
    state.latestOpsTaskStatePayload = taskStatePayload;
    state.latestOpsHistoryPayload = historyPayload;
    state.taskStateUnavailable = false;
    state.opsHistoryLoaded = true;
    state.opsHistoryFullLoaded = false;
    renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || health, {
      taskStatePayload,
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      syncTaskState: true,
      dispatchRefresh: true,
      scheduleDetails: false,
      renderDeferredPanels: false,
      renderActivityPanel: true,
      schedulePolling: false
    });
    loadFetchKpisSummaryData(renderToken, { silent: true }).catch(() => {});
    if (hasActiveRows(taskStatePayload)) {
      maybeUnrefTimer(setTimeout(() => {
        loadTaskStateSummaryData(renderToken, { fromPoll: true, summary: true }).catch(() => {});
      }, 0));
    }
    return { taskStatePayload, historyPayload };
  }

  async function loadTaskStateSummaryData(renderToken, options = {}) {
    const previousTaskStatePayload = getCachedTaskStatePayload();
    try {
      const payload = await measuredGetBridge(
        OPS_TASK_STATE_SUMMARY_PATH,
        "admin_ops_task_summary_fetch",
        { enabled: !options?.fromPoll }
      );
      if (renderToken !== opsRenderToken) return null;
      const taskStatePayload = taskStateController.resolveTaskStatePayload({
        status: "fulfilled",
        value: payload
      });
      state.latestOpsTaskStatePayload = taskStatePayload || {};
      state.taskStateUnavailable = Boolean(taskStatePayload?.taskStateUnavailable);
      const renderActivityPanel = Boolean(options?.summary)
        || !options?.fromPoll
        || hasActiveRows(taskStatePayload)
        || hasActiveRows(previousTaskStatePayload)
        || hasOptimisticRows();
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload,
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        syncTaskState: true,
        renderDeferredPanels: false,
        renderActivityPanel
      });
      return taskStatePayload;
    } catch (err) {
      if (renderToken !== opsRenderToken) return null;
      const taskStatePayload = taskStateController.resolveTaskStatePayload({
        status: "rejected",
        reason: err
      });
      state.latestOpsTaskStatePayload = taskStatePayload || {};
      state.taskStateUnavailable = true;
      const renderActivityPanel = Boolean(options?.summary)
        || !options?.fromPoll
        || hasActiveRows(taskStatePayload)
        || hasActiveRows(previousTaskStatePayload)
        || hasOptimisticRows();
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload,
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        syncTaskState: true,
        renderDeferredPanels: false,
        renderActivityPanel
      });
      return null;
    }
  }

  async function loadRegistryConflictsSummaryData(renderToken, options = {}) {
    if (state.registryConflictsDetailLoaded) return getCachedRegistryConflictsPayload();
    try {
      const payload = await measuredGetBridge(
        REGISTRY_CONFLICTS_SUMMARY_PATH,
        "admin_registry_conflicts_summary_fetch",
        { enabled: !options?.fromPoll }
      );
      if (renderToken !== opsRenderToken || state.registryConflictsDetailLoaded) return null;
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
      if (renderToken !== opsRenderToken || state.registryConflictsDetailLoaded) return null;
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

  async function loadFetchKpisSummaryData(renderToken = opsRenderToken, options = {}) {
    if (!fetchKpisLoad) {
      fetchKpisLoad = measuredGetBridge(
        OPS_FETCH_KPIS_SUMMARY_PATH,
        "admin_ops_fetch_kpis_summary_fetch",
        { enabled: !options?.fromPoll && !options?.silent }
      ).finally(() => {
        fetchKpisLoad = null;
      });
    }
    const payload = await fetchKpisLoad;
    if (renderToken !== opsRenderToken) return payload || null;
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      state.latestOpsHealthCache = mergeOpsHealth(
        state.latestOpsHealthCache || {},
        { kpis: payload.kpis || {}, summaryView: true },
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
    return payload || null;
  }

  async function loadOpsHealthData(options = {}) {
    if (state.adminBusyState.opsLoad) {
      if (options?.fromPoll) scheduleOpsHealthPolling(idlePollIntervalMs);
      return;
    }
    const renderToken = ++opsRenderToken;
    if (!initialBridgeReadyResolved) {
      initialBridgeReadyResolved = true;
      if (!(await awaitBridgeReady())) {
        scheduleOpsHealthPolling(idlePollIntervalMs);
        return;
      }
    }
    setBusyFlag("opsLoad", true);
    const showLoadingState = !options?.fromPoll && !state.latestOpsHealthCache;
    if (showLoadingState) setOpsReadinessShell();
    const measureFirstRender = !options?.fromPoll;
    if (measureFirstRender) markStep("admin_ops_health_first_render_start");
    try {
      let health;
      const useSummaryView = Boolean(options?.summary);
      const dashboardHealthPath = useSummaryView
        ? "/ops/dashboard-health?view=summary"
        : "/ops/dashboard-health";
      try {
        health = await measuredGetBridge(
          dashboardHealthPath,
          "admin_dashboard_health_fetch",
          { enabled: !options?.fromPoll }
        );
      } catch (err) {
        if (!state.latestOpsHealthCache) throw err;
        health = state.latestOpsHealthCache;
      }
      if (renderToken !== opsRenderToken) return;
      if (health && typeof health === "object" && !Array.isArray(health)) {
        state.latestOpsHealthCache = mergeOpsHealth(
          state.latestOpsHealthCache || {},
          health || {},
          { summary: useSummaryView }
        );
      }
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || health || {}, {
        taskStatePayload: getCachedTaskStatePayload(),
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        syncTaskState: Boolean(state.latestOpsTaskStatePayload),
        dispatchRefresh: true,
        scheduleDetails: false,
        renderDeferredPanels: false
      });
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", { ok: true });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: true }
        );
      }
      loadTaskStateSummaryData(renderToken, options).catch(() => {});
      loadRegistryConflictsSummaryData(renderToken, options).catch(() => {});
      if (!hasFetchKpiValues(state.latestOpsHealthCache?.kpis || {})) {
        loadFetchKpisSummaryData(renderToken, { silent: Boolean(options?.fromPoll) }).catch(() => {});
      }
    } catch (err) {
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", {
          ok: false,
          error: String(err?.message || err || "unknown error")
        });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: false }
        );
      }
      taskStateController.resetLifecycleTaskState();
      setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
      taskStateController.syncLiveBusyFlags(new Set());
      scheduleOpsHealthPolling(idlePollIntervalMs);
    } finally {
      setBusyFlag("opsLoad", false);
    }
  }

  return {
    setOpsPlaceholders,
    setOpsReadinessShell,
    stopOpsHealthPolling,
    scheduleOpsHealthPolling,
    applyBootstrapPayload,
    loadOpsHealthData,
    loadOpsHistoryData,
    loadOpsOverviewDetailData,
    selectOpsTab
  };
}

import {
  mergeOpsHealth,
  isDegradedControlFallbackPayload
} from "../../domain/ops-merge-model.js";
import { hasActionablePipelineSchedule } from "../../domain/ops-schedule-model.js";
import { hasFetchKpiValues } from "../../domain/ops-fetch-kpi-model.js";

const OPS_DASHBOARD_HEALTH_SUMMARY_PATH = "/ops/dashboard-health?view=summary";

/**
 * Polling lanes and the top-level ops health conductor.
 *
 * The idle heavy-hydration lane lives in the coordinator (`health.js`) because
 * tests/frontend/unit/admin-startup-diagnostics.test.mjs asserts on its source
 * shape; this leaf receives `scheduleIdleOpsHeavyHydration` as a dependency.
 */
export function createOpsLoaders({
  state,
  idlePollIntervalMs,
  awaitBridgeReady,
  setBusyFlag,
  markStep,
  measureStep,
  measuredGetBridge,
  getErrorMessage,
  maybeUnrefTimer,
  getOpsPollIntervalMs,
  opsRenderToken,
  nextRenderToken,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  hasActiveRows,
  hasActiveAdminWorkRows,
  hasPossibleActiveRunEvidence,
  markOpsDegradedActive,
  markOpsRouteFailure,
  canHydrateCompactDuringActiveRun,
  loadOpsHealthData,
  loadPipelineStatusFallbackData,
  loadPipelineScheduleData,
  loadFetchKpisSummaryData,
  loadDashboardHealthSummaryData,
  loadTaskStateSummaryData,
  loadOpsHistoryData,
  loadActiveOpsSummaryData,
  loadActiveOpsSupplementalData,
  scheduleIdleOpsHeavyHydration,
  renderOpsHealthSnapshot,
  setOpsPlaceholders,
  setOpsReadinessShell,
  taskStateController
}) {
  let initialBridgeReadyResolved = false;
  let idleRecoveryHealthLoad = null;

  function queueIdleRecoveryHealthLoad(options = { summary: true }) {
    if (!idleRecoveryHealthLoad) {
      idleRecoveryHealthLoad = Promise.resolve()
        .then(() => loadOpsHealthData(options))
        .catch(() => {})
        .finally(() => {
          idleRecoveryHealthLoad = null;
        });
    }
    return idleRecoveryHealthLoad;
  }

  function stopPipelineStatusPolling() {
    if (!state.pipelineStatusPollTimer) return;
    clearTimeout(state.pipelineStatusPollTimer);
    state.pipelineStatusPollTimer = null;
  }

  function stopOpsHealthPolling() {
    if (state.opsHealthPollTimer) clearTimeout(state.opsHealthPollTimer);
    state.opsHealthPollTimer = null;
    stopPipelineStatusPolling();
  }

  function schedulePipelineStatusPolling(delayMs) {
    // ponytail: exclusive lanes — starting the active lane must cancel any
    // pending idle poll or the two loops stack duplicate summary requests.
    stopOpsHealthPolling();
    const waitMs = Math.max(600, Number(delayMs) || 2000);
    state.pipelineStatusPollTimer = maybeUnrefTimer(setTimeout(() => {
      loadActiveOpsSummaryData(opsRenderToken(), { fromPoll: true }).catch(() => {});
    }, waitMs));
  }

  function scheduleOpsHealthPolling(delayMs) {
    stopOpsHealthPolling();
    const waitMs = Math.max(600, Number(delayMs) || 10000);
    if (hasPossibleActiveRunEvidence({ includeRecent: false })) {
      // Route through the active lane while run evidence exists: it keeps
      // cadence adaptive and skips the heavy dashboard-health summary.
      state.pipelineStatusPollTimer = maybeUnrefTimer(setTimeout(() => {
        loadActiveOpsSummaryData(opsRenderToken(), { fromPoll: true }).catch(() => {});
      }, waitMs));
      return;
    }
    state.opsHealthPollTimer = maybeUnrefTimer(setTimeout(() => {
      loadOpsHealthData({ fromPoll: true, summary: true }).catch(() => {});
    }, waitMs));
  }

  async function runOpsHealthData(options = {}) {
    if (state.adminBusyState.opsLoad) {
      if (options?.fromPoll) scheduleOpsHealthPolling(idlePollIntervalMs);
      return;
    }
    const renderToken = nextRenderToken();
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
    const pipelinePayload = await loadPipelineStatusFallbackData(renderToken, {
      fromPoll: Boolean(options?.fromPoll)
    });
    if (pipelinePayload?.active) {
      state.opsActiveAdminWorkLastActive = true;
      state.opsActivePipelineOrFetchLastActive = true;
      loadPipelineScheduleData({ force: true, silent: true }).catch(() => {});
      if (canHydrateCompactDuringActiveRun() || !hasFetchKpiValues(state.latestOpsHealthCache?.kpis || {})) {
        loadFetchKpisSummaryData(renderToken, {
          force: true,
          silent: true,
          fromPoll: Boolean(options?.fromPoll)
        }).catch(() => {});
      }
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", { ok: true, source: "pipeline-status" });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: true, source: "pipeline-status" }
        );
      }
      setBusyFlag("opsLoad", false);
      loadActiveOpsSupplementalData(renderToken, {
        fromPoll: Boolean(options?.fromPoll)
      }).finally(() => {
        if (renderToken === opsRenderToken() && (
          state.opsActiveAdminWorkLastActive || hasActiveAdminWorkRows()
        )) {
          schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
        }
      }).catch(() => {});
      return;
    }
    if (pipelinePayload?.degradedActive || hasPossibleActiveRunEvidence()) {
      markOpsDegradedActive(pipelinePayload?.degradedActive ? "pipeline_status_unavailable" : "possible_active_unresolved");
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", { ok: true, source: "degraded-active" });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: true, source: "degraded-active" }
        );
      }
      setBusyFlag("opsLoad", false);
      loadActiveOpsSummaryData(renderToken, {
        fromPoll: Boolean(options?.fromPoll)
      }).catch(() => {
        if (renderToken === opsRenderToken() && hasPossibleActiveRunEvidence()) {
          schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
        }
      });
      return;
    }
    try {
      let health;
      const useSummaryView = Boolean(options?.summary);
      const dashboardHealthPath = useSummaryView
        ? OPS_DASHBOARD_HEALTH_SUMMARY_PATH
        : "/ops/dashboard-health";
      try {
        health = useSummaryView
          ? await loadDashboardHealthSummaryData(renderToken, options)
          : await measuredGetBridge(
              dashboardHealthPath,
              "admin_dashboard_health_fetch",
              { enabled: !options?.fromPoll }
            );
      } catch (err) {
        markOpsRouteFailure("dashboard-health");
        health = state.latestOpsHealthCache || {
          ok: true,
          status: "degraded",
          summaryView: true,
          degraded: true,
          alerts: [],
          alertsEvaluated: false,
          alertBasis: "bridge-degraded",
          suppressedAlertsCount: 0,
          kpis: {},
          schedule: {},
          scheduleDelayed: true,
          message: `Admin data delayed; retrying: ${getErrorMessage(err)}`
        };
      }
      if (renderToken !== opsRenderToken()) return;
      if (health && typeof health === "object" && !Array.isArray(health)) {
        const healthForCache = isDegradedControlFallbackPayload(health)
          ? { ...health, schedule: hasActionablePipelineSchedule(health) ? health.schedule : {}, kpis: {} }
          : health;
        state.latestOpsHealthCache = mergeOpsHealth(
          state.latestOpsHealthCache || {},
          healthForCache || {},
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
      await Promise.allSettled([
        loadPipelineScheduleData({ force: true, silent: true }),
        loadOpsHistoryData({ force: true, silent: true })
      ]);
      scheduleIdleOpsHeavyHydration(renderToken, options);
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
      if (hasActiveRows()) {
        renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
          taskStatePayload: getCachedTaskStatePayload(),
          registryConflictsPayload: getCachedRegistryConflictsPayload(),
          syncTaskState: true,
          renderDeferredPanels: false,
          renderActivityPanel: true,
          schedulePolling: true
        });
      } else {
        taskStateController.resetLifecycleTaskState();
        setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
        taskStateController.syncLiveBusyFlags(new Set());
        scheduleOpsHealthPolling(idlePollIntervalMs);
      }
    } finally {
      setBusyFlag("opsLoad", false);
    }
  }

  return {
    queueIdleRecoveryHealthLoad,
    stopOpsHealthPolling,
    stopPipelineStatusPolling,
    scheduleOpsHealthPolling,
    schedulePipelineStatusPolling,
    runOpsHealthData
  };
}

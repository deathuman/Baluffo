import test from "node:test";
import assert from "node:assert/strict";

import {
  bootstrapScheduleNeedsRefresh,
  hasKnownPipelineSchedule,
  normalizePipelineSchedulePayload
} from "../../../frontend/admin/domain/ops-schedule-model.js";

const BOOTSTRAP_SCHEDULE = {
  pipeline: {
    enabled: true,
    intervalHours: 11,
    nextRunAt: "2026-08-24T22:51:33+02:00",
    due: false,
    pending: false
  }
};

test("bootstrapScheduleNeedsRefresh truth table", () => {
  const seededState = {
    pipelineScheduleModel: normalizePipelineSchedulePayload({
      schedule: BOOTSTRAP_SCHEDULE
    })
  };

  assert.equal(hasKnownPipelineSchedule(seededState.pipelineScheduleModel), true);
  // Healthy payload + successfully seeded model -> no early GET needed.
  assert.equal(
    bootstrapScheduleNeedsRefresh({ schedule: BOOTSTRAP_SCHEDULE }, seededState),
    false
  );

  // Payload without a schedule section -> refresh.
  assert.equal(bootstrapScheduleNeedsRefresh({}, seededState), true);
  // Schedule present but model failed to seed (unknown) -> refresh.
  assert.equal(bootstrapScheduleNeedsRefresh({ schedule: BOOTSTRAP_SCHEDULE }, {}), true);

  for (const payload of [
    { degraded: true, schedule: BOOTSTRAP_SCHEDULE },
    { overview: { degraded: true }, schedule: BOOTSTRAP_SCHEDULE },
    { ops: { scheduleDelayed: true }, schedule: BOOTSTRAP_SCHEDULE },
    { scheduleDelayed: true, schedule: BOOTSTRAP_SCHEDULE }
  ]) {
    assert.equal(bootstrapScheduleNeedsRefresh(payload, seededState), true, JSON.stringify(payload));
  }
});

test("applyBootstrapPayload seeds the schedule panel model without network", async () => {
  const { createClassList, createElement } = await import(
    "./helpers/admin-controller-test-helpers.mjs"
  );
  const { createAdminOpsController } = await import("../../../frontend/admin/app/ops.js");

  const state = {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  let renderedModel = null;
  const controller = createAdminOpsController({
    state,
    refs: {
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
      adminOpsAlertsEl: createElement(),
      adminOpsKpisEl: createElement(),
      adminOpsScheduleEl: createElement(),
      adminOpsFetcherMetricsEl: createElement(),
      adminOpsHistoryEl: createElement(),
      adminOpsTrendsEl: createElement(),
      adminRegistryConflictsReviewEl: createElement()
    },
    getBridge: async path => {
      throw new Error(`unexpected network call: ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    getOpsPollIntervalMs: () => 10000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule(_el, model) {
      renderedModel = model;
    },
    renderAdminOpsDedupLists() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminSourcePolicyReview() {},
    renderAdminRegistryConflicts() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    bridgeStatusPollIntervalMs: 10000,
    idlePollIntervalMs: 10000
  });

  const result = controller.applyBootstrapPayload({
    tasks: { current: [], recent: [] },
    app: { version: "0.2.134" },
    schedule: BOOTSTRAP_SCHEDULE
  });

  assert.ok(result);
  assert.equal(state.pipelineScheduleModel?.pipeline?.intervalHours, 11);
  assert.equal(state.pipelineScheduleModel?.pipeline?.nextRunAt, BOOTSTRAP_SCHEDULE.pipeline.nextRunAt);
  assert.equal(renderedModel?.pipeline?.intervalHours, 11);
});

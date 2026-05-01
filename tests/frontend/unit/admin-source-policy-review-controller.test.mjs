import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createElement,
  stubDateNow
} from "./helpers/admin-controller-test-helpers.mjs";

function createOpsReviewQueueFixture({
  nowMs = Date.parse("2026-05-01T10:00:00.000Z"),
  failMigrationPost = false
} = {}) {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
    latestFetcherReportCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminSourcePolicyReviewEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  const calls = [];
  const posts = [];
  const rendered = [];
  const toasts = [];
  const rows = [
    {
      staticSourceId: "static:studio",
      staticSourceName: "static_source::studio",
      providerSourceId: "provider:studio",
      providerSourceName: "Studio Provider",
      currentRecommendation: "stable_safe_redundant",
      reviewState: "new",
      manualSuppressionOverride: "none"
    }
  ];
  const reviewCandidates = [
    {
      providerSourceId: "greenhouse:slug:studio",
      selectedStaticSourceId: "static:studio",
      selectedStaticSourceName: "static_source::studio",
      apiEligible: true,
      recommendedApiPayload: {
        action: "apply_migration_identity_link",
        providerSourceId: "greenhouse:slug:studio",
        staticSourceId: "static:studio",
        staticSourceName: "static_source::studio",
        confidence: 0.8,
        reasons: ["source_state_disambiguation"],
        recommendationSource: "provider_coverage_link_backfill",
        recommendedAction: "needs_review"
      }
    }
  ];
  const dateStub = stubDateNow(nowMs);
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state") return { tasks: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      if (path === "/source-policy/recommendations") {
        return {
          ok: true,
          recommendations: { pairs: rows },
          providerCoverageLinkBackfill: { reviewCandidates }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async (path, payload) => {
      posts.push({ path, payload });
      if (failMigrationPost && path === "/source-policy/migration-link-action") {
        throw new Error("backend_rejected_link");
      }
      return {};
    },
    deriveAdminRunsModel: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminSourcePolicyReview(_el, payload, handlers) {
      rendered.push({ payload, handlers });
    },
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast(message, level) {
      toasts.push({ message, level });
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });
  return {
    controller,
    calls,
    posts,
    rendered,
    rows,
    reviewCandidates,
    toasts,
    restore() {
      controller.stopOpsHealthPolling();
      dateStub.restore();
    }
  };
}

test("admin ops controller loads source policy recommendations for the review queue", async () => {
  const fixture = createOpsReviewQueueFixture();
  try {
    await fixture.controller.loadOpsHealthData();

    assert.ok(fixture.calls.includes("/source-policy/recommendations"));
    assert.equal(fixture.rendered.length, 1);
    assert.equal(fixture.rendered[0].payload.recommendations.pairs[0].staticSourceId, "static:studio");
  } finally {
    fixture.restore();
  }
});

test("admin ops controller posts source policy review action payloads", async () => {
  const fixture = createOpsReviewQueueFixture();
  try {
    await fixture.controller.loadOpsHealthData();
    await fixture.rendered[0].handlers.onSourcePolicyAction(fixture.rows[0], "clear_override");

    assert.equal(fixture.posts.length, 1);
    assert.deepEqual(fixture.posts[0], {
      path: "/source-policy/review-action",
      payload: {
        action: "clear_override",
        staticSourceId: "static:studio",
        staticSourceName: "static_source::studio",
        providerSourceId: "provider:studio",
        providerSourceName: "Studio Provider"
      }
    });
  } finally {
    fixture.restore();
  }
});

test("admin ops controller posts snooze with a future snoozedUntil", async () => {
  const nowMs = Date.parse("2026-05-01T10:00:00.000Z");
  const fixture = createOpsReviewQueueFixture({ nowMs });
  try {
    await fixture.controller.loadOpsHealthData();
    await fixture.rendered[0].handlers.onSourcePolicyAction(fixture.rows[0], "snooze");

    assert.equal(fixture.posts.length, 1);
    assert.equal(fixture.posts[0].path, "/source-policy/review-action");
    assert.equal(fixture.posts[0].payload.action, "snooze");
    assert.equal(fixture.posts[0].payload.staticSourceId, "static:studio");
    assert.ok(Date.parse(fixture.posts[0].payload.snoozedUntil) > nowMs);
  } finally {
    fixture.restore();
  }
});

test("admin ops controller posts migration link apply payload and reloads", async () => {
  const fixture = createOpsReviewQueueFixture();
  try {
    await fixture.controller.loadOpsHealthData();
    await fixture.rendered[0].handlers.onMigrationLinkAction(
      fixture.reviewCandidates[0],
      "apply_migration_identity_link"
    );

    assert.equal(fixture.posts.length, 1);
    assert.deepEqual(fixture.posts[0], {
      path: "/source-policy/migration-link-action",
      payload: fixture.reviewCandidates[0].recommendedApiPayload
    });
    assert.equal(fixture.calls.filter(path => path === "/source-policy/recommendations").length, 2);
    assert.deepEqual(fixture.toasts[0], {
      message: "Migration identity link applied.",
      level: "success"
    });
  } finally {
    fixture.restore();
  }
});

test("admin ops controller posts migration link clear payload", async () => {
  const fixture = createOpsReviewQueueFixture();
  try {
    await fixture.controller.loadOpsHealthData();
    await fixture.rendered[0].handlers.onMigrationLinkAction(
      fixture.reviewCandidates[0],
      "clear_migration_identity_link"
    );

    assert.equal(fixture.posts.length, 1);
    assert.deepEqual(fixture.posts[0], {
      path: "/source-policy/migration-link-action",
      payload: {
        action: "clear_migration_identity_link",
        providerSourceId: "greenhouse:slug:studio",
        staticSourceId: "static:studio"
      }
    });
  } finally {
    fixture.restore();
  }
});

test("admin ops controller surfaces migration link backend errors", async () => {
  const fixture = createOpsReviewQueueFixture({ failMigrationPost: true });
  try {
    await fixture.controller.loadOpsHealthData();
    await fixture.rendered[0].handlers.onMigrationLinkAction(
      fixture.reviewCandidates[0],
      "apply_migration_identity_link"
    );

    assert.equal(fixture.posts.length, 1);
    assert.deepEqual(fixture.toasts[0], {
      message: "Could not update migration identity link: backend_rejected_link",
      level: "error"
    });
  } finally {
    fixture.restore();
  }
});

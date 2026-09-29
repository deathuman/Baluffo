/**
 * Bridge reconnect presentation: honest badge, banner, and bounded boot gate.
 *
 * Guards the packaged-Windows Admin stall UX. When the local bridge refused
 * connections, `loadAdminBootstrap` never rejected, so the badge reported
 * "Bridge Online" for a failed bootstrap while the panels were blank and the
 * page only appeared to recover on a later poll.
 */

import assert from "node:assert/strict";
import { describe, it, test } from "node:test";

import { createBridgeReconnectBanner } from "../../../frontend/admin/app/ops/bridge-reconnect-banner.js";
import {
  createClassList,
  createElement,
  createOpsRefs,
  createOpsState
} from "./helpers/admin-controller-test-helpers.mjs";

// Reachability is pushed into the banner, the same way runtime.js pushes it
// from every bridge call.
function createFixture({ bootstrapStatus = "ready", bridgeReachable = true } = {}) {
  const clicks = [];
  const listeners = {};
  const state = { bootstrapStatus };
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["checking"]) }),
    bridgeReconnectBannerEl: createElement({ classList: createClassList(["hidden"]) }),
    bridgeReconnectLabelEl: createElement(),
    bridgeReconnectElapsedEl: createElement(),
    bridgeReconnectRetryBtnEl: createElement({
      dataset: {},
      addEventListener(type, fn) {
        listeners[type] = fn;
      },
      click() {
        listeners.click?.();
      }
    }),
    adminContentEl: createElement({ classList: createClassList([]) })
  };
  const banner = createBridgeReconnectBanner({
    refs,
    getBootstrapStatus: () => state.bootstrapStatus,
    onRetryNow: () => clicks.push("retry"),
    nowFn: () => 1_000_000
  });
  banner.setBridgeReachable(bridgeReachable);
  return { banner, refs, state, clicks, listeners };
}

// Top-level `test(...)` calls so the suite-contract guard sees real cases.
test("banner is hidden and the badge untouched while the bridge is healthy", () => {
  const { banner, refs } = createFixture();
  banner.sync();

  assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), true);
  assert.equal(refs.bridgeReconnectBannerEl.attributes["aria-hidden"], "true");
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("reconnecting"), false);
  assert.equal(refs.bridgeReconnectElapsedEl.textContent, "");
});

describe("admin bridge reconnect banner", () => {
  it("stays hidden when the bridge is healthy", () => {
    const { banner, refs } = createFixture();
    banner.sync();

    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), true);
    assert.equal(refs.bridgeReconnectBannerEl.attributes["aria-hidden"], "true");
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("reconnecting"), false);
  });

  it("names the wait and marks the badge reconnecting while the bridge is offline", () => {
    const { banner, refs } = createFixture({ bridgeReachable: false });
    banner.sync();

    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), false);
    assert.equal(refs.bridgeReconnectLabelEl.textContent, "Reconnecting to the Baluffo bridge...");
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("reconnecting"), true);
    // It must never claim the bridge is healthy while waiting on it.
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("online"), false);

    banner.setBridgeReachable(true);
    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), true);
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("reconnecting"), false);
  });

  it("follows reachability without depending on a status watch or CSS classes", () => {
    // Regression: the banner originally read the bridge status controller's
    // internal last-status, which only advances when startBridgeStatusWatch
    // runs. The Admin runtime never starts that watch, so the controller read
    // "checking" forever and the banner stayed hidden while the badge read
    // "Bridge Offline" - the exact silent failure this feature prevents. It
    // then briefly read back the badge's CSS class, which setBridgeStatusBadge
    // rewrites on every render. Reachability is now pushed in explicitly.
    const { banner, refs } = createFixture();
    banner.sync();
    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), true);

    // A plain reachability drop, with no controller and no class changes.
    banner.setBridgeReachable(false);
    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), false);
    assert.equal(refs.bridgeReconnectLabelEl.textContent, "Reconnecting to the Baluffo bridge...");
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("reconnecting"), true);
  });

  it("reports the bootstrap wait separately from a reconnect", () => {
    const { banner, refs, state } = createFixture({ bootstrapStatus: "pending" });
    banner.sync();

    assert.equal(refs.bridgeReconnectLabelEl.textContent, "Starting the Baluffo bridge...");
    assert.equal(refs.adminBridgeStatusBadgeEl.textContent, "Bridge Starting");
    // The gate holds the blank content rather than rendering an empty shell.
    assert.equal(refs.adminContentEl.classList.contains("hidden"), true);

    state.bootstrapStatus = "ready";
    banner.sync();
    assert.equal(refs.adminContentEl.classList.contains("hidden"), false);
  });

  it("force-reveals the content once the gate expires", () => {
    let clock = 0;
    const contentEl = createElement({ classList: createClassList([]) });
    const banner = createBridgeReconnectBanner({
      refs: {
        adminBridgeStatusBadgeEl: createElement({ classList: createClassList([]) }),
        bridgeReconnectBannerEl: createElement({ classList: createClassList(["hidden"]) }),
        bridgeReconnectLabelEl: createElement(),
        bridgeReconnectElapsedEl: createElement(),
        bridgeReconnectRetryBtnEl: createElement({ dataset: {}, addEventListener() {} }),
        adminContentEl: contentEl
      },
      getBootstrapStatus: () => "pending",
      onRetryNow() {},
      nowFn: () => clock
    });

    banner.sync();
    assert.equal(contentEl.classList.contains("hidden"), true);

    // Past the bounded wait the page is shown anyway: degraded but usable,
    // rather than left blank forever on a bridge that never answers.
    clock = banner.bootstrapGateMs + 1;
    banner.render();
    assert.equal(contentEl.classList.contains("hidden"), false);
  });

  it("resolves the bridge wait before the gate expires on a fast start", () => {
    let clock = 0;
    const contentEl = createElement({ classList: createClassList([]) });
    const refs = {
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList([]) }),
      bridgeReconnectBannerEl: createElement({ classList: createClassList(["hidden"]) }),
      bridgeReconnectLabelEl: createElement(),
      bridgeReconnectElapsedEl: createElement(),
      bridgeReconnectRetryBtnEl: createElement({ dataset: {}, addEventListener() {} }),
      adminContentEl: contentEl
    };
    const banner = createBridgeReconnectBanner({
      refs,
      getBootstrapStatus: () => "pending",
      onRetryNow() {},
      nowFn: () => clock
    });
    banner.sync();
    assert.equal(contentEl.classList.contains("hidden"), true);

    // A normal cold bridge start is well inside the gate, so the page is
    // revealed as soon as the bootstrap settles.
    clock = 1000;
    banner.sync();
    assert.equal(contentEl.classList.contains("hidden"), true);

    const settled = createBridgeReconnectBanner({
      refs,
      getBootstrapStatus: () => "ready",
      onRetryNow() {},
      nowFn: () => clock
    });
    settled.sync();
    assert.equal(contentEl.classList.contains("hidden"), false);
    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), true);
  });

  it("reads bridge status from the ops controller the runtime passes", async () => {
    // Regression: the banner originally read reachability from
    // opsController.getBridgeStatus. If the ops controller does not expose it,
    // the read silently falls back to a non-offline value. The banner now keys
    // off the rendered badge, but the controller is still the retry path, so
    // both must be exported and pollable.
    const ops = await import("../../../frontend/admin/app/ops.js");
    const refs = { adminBridgeStatusBadgeEl: createElement({ classList: createClassList([]) }) };
    const controller = ops.createAdminOpsController({
      state: createOpsState(),
      refs: { ...createOpsRefs(), ...refs },
      getBridge: async () => {
        throw new TypeError("Failed to fetch");
      },
      postBridge: async () => ({}),
      deriveAdminRunsModel: () => ({}),
      getOpsPollIntervalMs: () => 1000,
      renderAdminOpsAlerts() {},
      renderAdminOpsKpis() {},
      renderAdminOpsSchedule() {},
      renderAdminOpsDedupLists() {},
      renderAdminOpsFetcherMetrics() {},
      renderAdminSourcePolicyReview() {},
      renderAdminRegistryConflicts() {},
      renderAdminOpsTrends() {},
      renderAdminOpsHistory() {},
      setBusyFlag() {},
      showToast() {},
      getErrorMessage: () => "err",
      adminDispatch: { dispatch() {} },
      adminActions: {},
      escapeHtml: v => String(v),
      idlePollIntervalMs: 1000,
      taskStateController: {},
      loadLatestDiscoveryReport: async () => ({}),
      onActivePipelineIdle() {},
      bridgeStatusPollIntervalMs: 1000,
      markAdminStep() {},
      measureAdminStep() {}
    });
    assert.equal(typeof controller.getBridgeStatus, "function", "ops controller must expose getBridgeStatus");
    assert.equal(typeof controller.pollBridgeStatus, "function");

    // Two failed polls are required before the status reads offline, so one
    // bad tick does not make a healthy bridge look dead.
    await controller.pollBridgeStatus();
    assert.equal(controller.getBridgeStatus(), "checking");
    await controller.pollBridgeStatus();
    assert.equal(controller.getBridgeStatus(), "offline");
  });

  it("invokes the retry handler and disables the button while it runs", () => {
    const { banner, refs, clicks } = createFixture({ bridgeStatus: "offline" });
    banner.sync();

    refs.bridgeReconnectRetryBtnEl.click();

    assert.deepEqual(clicks, ["retry"]);
    assert.equal(refs.bridgeReconnectRetryBtnEl.disabled, true);
  });
});

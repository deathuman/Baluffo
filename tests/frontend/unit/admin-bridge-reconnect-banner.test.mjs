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

function createFixture({ bootstrapStatus = "ready", bridgeStatus = "online" } = {}) {
  const clicks = [];
  const listeners = {};
  const state = { bootstrapStatus, bridgeStatus };
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
    getBridgeStatus: () => state.bridgeStatus,
    getBootstrapStatus: () => state.bootstrapStatus,
    onRetryNow: () => clicks.push("retry"),
    nowFn: () => 1_000_000
  });
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
    const { banner, refs, state } = createFixture({ bridgeStatus: "offline" });
    banner.sync();

    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), false);
    assert.equal(refs.bridgeReconnectLabelEl.textContent, "Reconnecting to the Baluffo bridge...");
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("reconnecting"), true);
    // It must never claim the bridge is healthy while waiting on it.
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("online"), false);

    state.bridgeStatus = "online";
    banner.sync();
    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), true);
    assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("reconnecting"), false);
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
      getBridgeStatus: () => "checking",
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
      getBridgeStatus: () => "checking",
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
      getBridgeStatus: () => "checking",
      getBootstrapStatus: () => "ready",
      onRetryNow() {},
      nowFn: () => clock
    });
    settled.sync();
    assert.equal(contentEl.classList.contains("hidden"), false);
    assert.equal(refs.bridgeReconnectBannerEl.classList.contains("hidden"), true);
  });

  it("reads bridge status from the ops controller the runtime passes", async () => {
    // Regression: the runtime wires the banner's getBridgeStatus to
    // opsController.getBridgeStatus. If the ops controller does not expose it,
    // the banner silently reads "checking" forever and never shows, which is
    // exactly the failure this feature exists to prevent.
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

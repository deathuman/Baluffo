/**
 * Shared fixture for `createAdminAuthController` tests.
 *
 * The controller has a wide option surface and each test only exercises a slice
 * of it, so a full literal per test dominated the file. `createAuthControllerFixture`
 * supplies the inert majority and lets a test override just what it asserts on.
 */

import { createAdminAuthController } from "../../../../frontend/admin/app/auth.js";
import { createClassList, createElement } from "./admin-controller-test-helpers.mjs";

export { createClassList, createElement };

const NOOP = () => {};
const NOOP_ASYNC = async () => {};

export function createAuthControllerRefs() {
  return {
    adminContentEl: createElement({ classList: createClassList(["hidden"]) }),
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["hidden"]) }),
    adminSyncStatusEl: createElement()
  };
}

export function createAuthControllerOptions(overrides = {}) {
  return {
    emitAdminStartupMetric: NOOP,
    markAdminFirstInteractive: NOOP,
    markAdminStep: NOOP,
    measureAdminStep: NOOP,
    syncAdminBusyUi: NOOP,
    syncDiscoveryLogDisclosure: NOOP,
    resetBusyFlags: NOOP,
    setSourceFilter: NOOP,
    setSourceStatus: NOOP,
    setFetcherLogPlaceholder: NOOP,
    setDiscoveryLogPlaceholder: NOOP,
    clearOptimisticFetchRun: NOOP,
    clearOptimisticDiscoveryRun: NOOP,
    setManualSourceFeedback: NOOP,
    setOpsPlaceholders: NOOP,
    setOpsReadinessShell: NOOP,
    setBridgeStatusBadge: NOOP,
    startBridgeStatusWatch: NOOP,
    refreshOverview: NOOP_ASYNC,
    loadOpsHealthData: NOOP_ASYNC,
    loadSyncStatus: NOOP_ASYNC,
    loadDiscoveryConfig: NOOP_ASYNC,
    loadPipelineStatusFallbackData: async () => ({ active: false }),
    loadAdminBootstrap: async () => ({ ok: true, summaryView: true }),
    logAdminError: NOOP,
    showToast: NOOP,
    ...overrides
  };
}

/**
 * @param {object} [overrides] - Controller option overrides.
 * @returns {object} Controller plus `refs` and `calls` for assertions.
 */
export function createAuthControllerFixture(overrides = {}) {
  const calls = [];
  const refs = createAuthControllerRefs();
  const controller = createAdminAuthController(
    createAuthControllerOptions({ refs, ...overrides })
  );
  return { controller, refs, calls };
}

/**
 * Runs `initAdminPage` and returns the recorded bridge-badge transitions.
 *
 * @param {object} bootstrapPayload - What `loadAdminBootstrap` resolves with.
 * @returns {Promise<string[]>} Entries shaped `bridge:<state>:<label>`.
 */
export async function bridgeBadgeAfterBootstrap(bootstrapPayload) {
  const calls = [];
  const controller = createAdminAuthController(
    createAuthControllerOptions({
      refs: createAuthControllerRefs(),
      setBridgeStatusBadge: (badgeState, label) => calls.push(`bridge:${badgeState}:${label}`),
      loadAdminBootstrap: async () => bootstrapPayload
    })
  );
  controller.initAdminPage();
  await new Promise(resolve => setTimeout(resolve, 0));
  return calls;
}

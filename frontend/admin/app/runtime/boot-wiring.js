/**
 * Reconnect banner and post-composition event wiring for the Admin runtime.
 *
 * Keeps `app/runtime.js` inside its 320-line budget by owning the boot tail:
 * the bridge-reconnect presentation, the runtime event bindings, and the final
 * controller kick-off.
 *
 * The banner is display-only. It never changes when routes are attempted or
 * which degradation windows apply, so it cannot alter the startup or hydration
 * behaviour the packaged startup probes measure.
 */

import { createBridgeReconnectBanner } from "../ops/bridge-reconnect-banner.js";
import { getDesktopBootstrapStatus } from "../../../shared/local-data/desktop-client.js";

function wireReconnectBanner({ state, refs, getOpsController, awaitBridgeReady, logAdminError }) {
  const banner = createBridgeReconnectBanner({
    refs,
    getBridgeStatus: () => getOpsController()?.getBridgeStatus?.() || "checking",
    getBootstrapStatus: getDesktopBootstrapStatus,
    onRetryNow: () => {
      void getOpsController()?.pollBridgeStatus?.({ forceChecking: true });
    }
  });
  state.bridgeReconnectBanner = banner;
  banner.sync();
  void Promise.resolve(awaitBridgeReady())
    .then(() => banner.sync())
    .catch(err => logAdminError("Admin desktop bootstrap failed", err));
  return banner;
}

/**
 * @param {object} params
 * @param {object} params.state - Admin runtime state.
 * @param {object} params.refs - Cached admin DOM refs.
 * @param {object} params.controllers - Composition result.
 * @param {Function} params.bindAdminRuntimeEvents - Event binder.
 * @param {Function} params.getOpsController - Resolves the ops controller.
 * @param {Function} params.awaitBridgeReady - Resolves when the bootstrap settles.
 * @param {Function} params.logAdminError - Error logger.
 * @param {object} params.helpers - Small runtime helpers the binder needs.
 */
export function wireAdminBoot({
  state,
  refs,
  controllers,
  bindAdminRuntimeEvents,
  getOpsController,
  awaitBridgeReady,
  logAdminError,
  helpers
}) {
  wireReconnectBanner({ state, refs, getOpsController, awaitBridgeReady, logAdminError });

  const opsController = controllers.opsController;
  controllers.fetcherController.applyFetcherPresetMetadata();
  bindAdminRuntimeEvents({
    state,
    refs,
    onRestoreActiveRunWatches: controllers.restoreActiveRunWatches,
    onRefreshOverview: controllers.overviewController.refreshOverview,
    fetcherController: controllers.fetcherController,
    discoveryController: controllers.discoveryController,
    registryController: controllers.registryController,
    opsController,
    syncController: controllers.syncController,
    ...helpers
  });
  controllers.authController.initAdminPage();
  // initAdminPage unhides the Ops content, so the bounded boot gate is
  // re-applied now that the banner owns the presentation.
  state.bridgeReconnectBanner?.sync();
  controllers.actionCenterController.startPolling();
  controllers.inspectorController.init();
  return opsController;
}

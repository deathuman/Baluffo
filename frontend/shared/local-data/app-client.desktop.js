import { bindStartupProbeErrorHandlers, resolveStartupProbePage } from "../../../probes/startup-probe.js";
import { bootBridgeLocalData } from "./bridge-boot.js";
import {
  awaitDesktopBootstrap,
  getBridgeBootstrapStats,
  initDesktopLocalDataClient
} from "./desktop-client.js";

// The packaged desktop composition. The launcher opens every page with
// `?desktop=1&bridgePort=...` and the packaged server is the bridge, so the data
// lives outside the page here too; `runtime.desktop` in the served
// `frontend-runtime-config.js` is the other half of that contract. Importing
// this module instead of the dispatcher is what keeps the browser (IndexedDB)
// client out of the packaged pages, and `desktop-client.js` is what keeps the
// window heartbeat and close signals in.
window.__baluffoModuleLoading = true;
window.__baluffoRuntimeMode = "desktop";
window.__baluffoDesktopMode = true;
window.__baluffoBridgeLocalDataMode = true;
window.__baluffoInitErrors = window.__baluffoInitErrors || [];

try {
  bindStartupProbeErrorHandlers();
} catch (err) {
  console.error("[baluffo] Error in frontend/shared/local-data/app-client.desktop.js:", err);
}

bootBridgeLocalData({
  page: resolveStartupProbePage(),
  localData: {
    init: () => initDesktopLocalDataClient({ enableLifecycle: true }),
    awaitBootstrap: () => awaitDesktopBootstrap({ enableLifecycle: true }),
    getStats: getBridgeBootstrapStats
  }
});

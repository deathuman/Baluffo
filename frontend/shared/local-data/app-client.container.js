import { bindStartupProbeErrorHandlers, resolveStartupProbePage } from "../../../probes/startup-probe.js";
import { bootBridgeLocalData } from "./bridge-boot.js";
import {
  awaitBridgeBootstrap,
  getBridgeBootstrapStats,
  initBridgeLocalDataClient
} from "./bridge-client.js";

// The hosted composition. A container deployment serves every page with
// `runtime: {mode: "container", localDataMode: "bridge"}`
// (`render_container_runtime_config_js`), so the runtime is known here rather
// than detected: no browser (IndexedDB) branch and no desktop session lifecycle
// is imported at all. `app-client.js` stays the development dispatcher, and
// `app-client.desktop.js` is the packaged app's composition.
window.__baluffoModuleLoading = true;
window.__baluffoRuntimeMode = "container";
window.__baluffoDesktopMode = false;
window.__baluffoBridgeLocalDataMode = true;
window.__baluffoInitErrors = window.__baluffoInitErrors || [];

try {
  bindStartupProbeErrorHandlers();
} catch (err) {
  console.error("[baluffo] Error in frontend/shared/local-data/app-client.container.js:", err);
}

bootBridgeLocalData({
  page: resolveStartupProbePage(),
  localData: {
    init: () => initBridgeLocalDataClient({ enableLifecycle: false }),
    awaitBootstrap: () => awaitBridgeBootstrap({ enableLifecycle: false }),
    getStats: getBridgeBootstrapStats
  }
});

window.__baluffoModuleLoading = true;
console.log("[baluffo] frontend/shared/local-data/app-client.js: module script loading...");

import { initBrowserLocalDataClient } from "./browser-client.js";
import {
  awaitDesktopBootstrap,
  getBridgeBootstrapStats,
  initDesktopLocalDataClient
} from "./desktop-client.js";
import { bootBridgeLocalData } from "./bridge-boot.js";
import {
  resolveBridgeLocalDataMode,
  resolveDesktopRuntimeMode,
  resolveRuntimeMode
} from "./runtime-context.js";
import {
  bindStartupProbeErrorHandlers,
  resolveStartupProbePage
} from "../../../probes/startup-probe.js";

// The development dispatcher. The shipped pages do not use it: the container
// build boots `app-client.container.js` and the desktop build boots
// `app-client.desktop.js`, so each one omits the branch it can never take. This
// module keeps a repo page working both ways -- plain browser mode against
// IndexedDB, and `?desktop=1` against a bridge -- which is what developing the
// pages needs.
try {
  bindStartupProbeErrorHandlers();
  window.__baluffoInitErrors = window.__baluffoInitErrors || [];
  window.__baluffoRuntimeMode = resolveRuntimeMode();
  window.__baluffoDesktopMode = resolveDesktopRuntimeMode();
  window.__baluffoBridgeLocalDataMode = resolveBridgeLocalDataMode();
} catch (err) {
  console.error("[baluffo] Error in frontend/shared/local-data/app-client.js:", err);
}

if (window.__baluffoBridgeLocalDataMode) {
  const enableDesktopLifecycle = Boolean(window.__baluffoDesktopMode);
  bootBridgeLocalData({
    page: resolveStartupProbePage(),
    localData: {
      init: () => initDesktopLocalDataClient({ enableLifecycle: enableDesktopLifecycle }),
      awaitBootstrap: () => awaitDesktopBootstrap({ enableLifecycle: enableDesktopLifecycle }),
      getStats: getBridgeBootstrapStats
    }
  });
} else {
  try {
    initBrowserLocalDataClient();
    window.__baluffoLocalDataLoaded = true;
    console.log("[baluffo] Browser local data initialized successfully");
  } catch (err) {
    console.error("[baluffo] Browser local data init failed:", err);
    window.__baluffoInitErrors.push(err);
  }
}

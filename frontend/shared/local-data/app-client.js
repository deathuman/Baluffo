window.__baluffoModuleLoading = true;
console.log("[baluffo] frontend/shared/local-data/app-client.js: module script loading...");

import { initBrowserLocalDataClient } from "./browser-client.js";
import {
  awaitDesktopBootstrap,
  getDesktopBootstrapStats,
  initDesktopLocalDataClient
} from "./desktop-client.js";
import { hydrateDesktopVersionLabels } from "../app-version.js";
import {
  resolveBridgeLocalDataMode,
  resolveDesktopRuntimeMode,
  resolveRuntimeMode
} from "./runtime-context.js";
import {
  bindStartupProbeErrorHandlers,
  emitStartupProbeMetric,
  resolveStartupProbePage
} from "../../../probes/startup-probe.js";

try {
  bindStartupProbeErrorHandlers();
  window.__baluffoRuntimeMode = resolveRuntimeMode();
  window.__baluffoDesktopMode = resolveDesktopRuntimeMode();
  window.__baluffoBridgeLocalDataMode = resolveBridgeLocalDataMode();
} catch (err) {
  console.error("[baluffo] Error in frontend/shared/local-data/app-client.js:", err);
}

if (window.__baluffoBridgeLocalDataMode) {
  const page = resolveStartupProbePage();
  emitStartupProbeMetric(`${page}_page_boot_start`);
  emitStartupProbeMetric(`${page}_local_data_init_start`);
  const enableDesktopLifecycle = Boolean(window.__baluffoDesktopMode);
  try {
    initDesktopLocalDataClient({ enableLifecycle: enableDesktopLifecycle });
    emitStartupProbeMetric(`${page}_local_data_api_ready`);
    const desktopBootstrapWaitStartedAt =
      typeof performance !== "undefined" && typeof performance.now === "function"
        ? performance.now()
        : Date.now();
    window.__baluffoLocalDataLoaded = false;
    awaitDesktopBootstrap({ enableLifecycle: enableDesktopLifecycle }).then(ready => {
      if (!ready) {
        return;
      }
      window.__baluffoLocalDataLoaded = true;
      console.log("[baluffo] Bridge local data initialized successfully");
      if (enableDesktopLifecycle) {
        hydrateDesktopVersionLabels().catch(() => {});
      }
      const desktopBootstrapReadyAt =
        typeof performance !== "undefined" && typeof performance.now === "function"
          ? performance.now()
          : Date.now();
      const desktopBootstrapStats = getDesktopBootstrapStats();
      emitStartupProbeMetric(`${page}_local_data_init_ready`, {
        bootstrapWaitMs: Math.max(
          0,
          Math.round(desktopBootstrapReadyAt - desktopBootstrapWaitStartedAt)
        ),
        bootstrapAttemptCount: Math.max(
          0,
          Number(desktopBootstrapStats?.attemptCount || 0)
        ),
        bootstrapRetryCount: Math.max(
          0,
          Number(desktopBootstrapStats?.failureCount || 0)
        ),
        firstSuccessfulBootstrapAttemptMs:
          desktopBootstrapStats?.firstSuccessfulAttemptMs ?? null
      });
    }).catch(err => {
      console.error("[baluffo] Bridge local data init failed:", err);
      window.__baluffoInitErrors.push(err);
    });
  } catch (err) {
    console.error("[baluffo] Bridge local data init failed:", err);
    window.__baluffoInitErrors.push(err);
  }
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

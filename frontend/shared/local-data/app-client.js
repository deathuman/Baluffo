window.__baluffoModuleLoading = true;
console.log("[baluffo] frontend/shared/local-data/app-client.js: module script loading...");

import { initBrowserLocalDataClient } from "./browser-client.js";
import { initDesktopLocalDataClient } from "./desktop-client.js";
import { AdminConfig } from "../config/admin-config.js";
import {
  bindStartupProbeErrorHandlers,
  emitStartupProbeMetric,
  resolveStartupProbePage
} from "../../../probes/startup-probe.js";

const RUNTIME_MODE_KEY = "baluffo_runtime_mode";
const HEARTBEAT_INTERVAL_MS = 10000;

let desktopHeartbeatStarted = false;

function resolveDesktopMode() {
  try {
    const url = new URL(window.location.href);
    const explicitDesktop = url.searchParams.get("desktop");
    if (explicitDesktop === "1") {
      window.sessionStorage.setItem(RUNTIME_MODE_KEY, "desktop");
      return true;
    }
    return window.sessionStorage.getItem(RUNTIME_MODE_KEY) === "desktop";
  } catch {
    return false;
  }
}

try {
  bindStartupProbeErrorHandlers();
  window.__baluffoDesktopMode = resolveDesktopMode();
} catch (err) {
  console.error("[baluffo] Error in frontend/shared/local-data/app-client.js:", err);
}

function startDesktopHeartbeat() {
  if (desktopHeartbeatStarted) return;
  desktopHeartbeatStarted = true;
  const emitHeartbeat = () => {
    fetch(`${AdminConfig.ADMIN_BRIDGE_BASE}/desktop-local-data/startup-metric?t=${Date.now()}`, {
      method: "POST",
      cache: "no-store",
      keepalive: true,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        event: "desktop_browser_heartbeat",
        payload: {
          page: window.location.pathname,
          visibility: document.visibilityState || "unknown"
        }
      })
    }).catch(() => {});
  };
  emitHeartbeat();
  window.setInterval(emitHeartbeat, HEARTBEAT_INTERVAL_MS);
}

if (window.__baluffoDesktopMode) {
  const page = resolveStartupProbePage();
  emitStartupProbeMetric(`${page}_page_boot_start`);
  emitStartupProbeMetric(`${page}_local_data_init_start`);
  try {
    initDesktopLocalDataClient();
    window.__baluffoLocalDataLoaded = true;
    console.log("[baluffo] Desktop local data initialized successfully");
    emitStartupProbeMetric(`${page}_local_data_init_ready`);
  } catch (err) {
    console.error("[baluffo] Desktop local data init failed:", err);
    window.__baluffoInitErrors.push(err);
  }
  startDesktopHeartbeat();
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

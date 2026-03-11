const RUNTIME_MODE_KEY = "baluffo_runtime_mode";
const HEARTBEAT_INTERVAL_MS = 10000;
import { initBrowserLocalDataClient } from "./local-data-client.js";
import { initDesktopLocalDataClient } from "./desktop-local-data-client.js";
import { AdminConfig } from "./admin-config.js";
import { bindStartupProbeErrorHandlers, emitStartupProbeMetric, resolveStartupProbePage } from "./startup-probe.js";

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

bindStartupProbeErrorHandlers();

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

if (resolveDesktopMode()) {
  const page = resolveStartupProbePage();
  emitStartupProbeMetric(`${page}_page_boot_start`);
  emitStartupProbeMetric(`${page}_local_data_init_start`);
  initDesktopLocalDataClient();
  emitStartupProbeMetric(`${page}_local_data_init_ready`);
  startDesktopHeartbeat();
} else {
  initBrowserLocalDataClient();
}

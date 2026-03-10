const RUNTIME_MODE_KEY = "baluffo_runtime_mode";
import { initBrowserLocalDataClient } from "./local-data-client.js";
import { initDesktopLocalDataClient } from "./desktop-local-data-client.js";
import { bindStartupProbeErrorHandlers, emitStartupProbeMetric, resolveStartupProbePage } from "./startup-probe.js";

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

if (resolveDesktopMode()) {
  const page = resolveStartupProbePage();
  emitStartupProbeMetric(`${page}_page_boot_start`);
  emitStartupProbeMetric(`${page}_local_data_init_start`);
  initDesktopLocalDataClient();
  emitStartupProbeMetric(`${page}_local_data_init_ready`);
} else {
  initBrowserLocalDataClient();
}

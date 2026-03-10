const STARTUP_PROBE_KEY = "baluffo_startup_probe_enabled";
const RUNTIME_BRIDGE_BASE_KEY = "baluffo_runtime_bridge_base";

export function resolveStartupProbeEnabled() {
  try {
    const url = new URL(window.location.href);
    const explicit = String(url.searchParams.get("startupProbe") || "").trim();
    if (explicit === "1") {
      window.sessionStorage.setItem(STARTUP_PROBE_KEY, "1");
      return true;
    }
    return window.sessionStorage.getItem(STARTUP_PROBE_KEY) === "1";
  } catch {
    return false;
  }
}

function resolveBridgeBase() {
  try {
    const url = new URL(window.location.href);
    const bridgePort = String(url.searchParams.get("bridgePort") || "").trim();
    const bridgeHost = String(url.searchParams.get("bridgeHost") || "").trim() || "127.0.0.1";
    if (/^\d+$/.test(bridgePort)) {
      const runtimeBase = `http://${bridgeHost}:${bridgePort}`;
      window.sessionStorage.setItem(RUNTIME_BRIDGE_BASE_KEY, runtimeBase);
      return runtimeBase;
    }
    return String(window.sessionStorage.getItem(RUNTIME_BRIDGE_BASE_KEY) || "").trim();
  } catch {
    return "";
  }
}

export function resolveStartupProbePage() {
  try {
    const path = String(new URL(window.location.href).pathname || "").split("/").pop() || "";
    const stem = path.replace(/\.html?$/i, "").trim().toLowerCase();
    return stem || "jobs";
  } catch {
    return "jobs";
  }
}

export function emitStartupProbeMetric(event, payload = {}) {
  if (!resolveStartupProbeEnabled()) return;
  const bridgeBase = resolveBridgeBase();
  if (!bridgeBase) return;
  fetch(`${bridgeBase}/desktop-local-data/startup-metric?t=${Date.now()}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event,
      payload
    })
  }).catch(() => {});
}

let startupProbeErrorBindingDone = false;

export function bindStartupProbeErrorHandlers() {
  if (!resolveStartupProbeEnabled()) return;
  if (startupProbeErrorBindingDone) return;
  startupProbeErrorBindingDone = true;

  window.addEventListener("error", event => {
    emitStartupProbeMetric(`${resolveStartupProbePage()}_probe_error`, {
      message: String(event?.message || "unknown error"),
      filename: String(event?.filename || ""),
      line: Number(event?.lineno || 0),
      column: Number(event?.colno || 0)
    });
  });

  window.addEventListener("unhandledrejection", event => {
    const reason = event?.reason;
    emitStartupProbeMetric(`${resolveStartupProbePage()}_probe_unhandledrejection`, {
      message:
        typeof reason === "string"
          ? reason
          : String(reason?.message || reason?.stack || reason || "unknown rejection")
    });
  });
}

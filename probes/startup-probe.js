const STARTUP_PROBE_KEY = "baluffo_startup_probe_enabled";
const RUNTIME_BRIDGE_BASE_KEY = "baluffo_runtime_bridge_base";
const STARTUP_PROBE_RETRY_MS = 200;

let startupProbeRetryHandle = null;
let startupProbeFlushPromise = null;
const pendingStartupProbeMetrics = [];

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

function normalizeStartupMetricPayload(payload = {}) {
  const normalized = payload && typeof payload === "object" ? { ...payload } : {};
  if (!Number.isFinite(Number(normalized.browserCreatedAtMs))) {
    let browserCreatedAtMs = Date.now();
    try {
      if (
        typeof performance !== "undefined"
        && Number.isFinite(Number(performance.timeOrigin))
        && typeof performance.now === "function"
      ) {
        browserCreatedAtMs = Number(performance.timeOrigin) + Number(performance.now());
      }
    } catch {
      browserCreatedAtMs = Date.now();
    }
    normalized.browserCreatedAtMs = Math.max(0, Math.round(browserCreatedAtMs));
  }
  return normalized;
}

function clearStartupProbeRetry() {
  if (startupProbeRetryHandle === null) return;
  clearTimeout(startupProbeRetryHandle);
  startupProbeRetryHandle = null;
}

function scheduleStartupProbeRetry() {
  if (startupProbeRetryHandle !== null) return;
  startupProbeRetryHandle = setTimeout(() => {
    startupProbeRetryHandle = null;
    void flushStartupProbeMetricQueue();
  }, STARTUP_PROBE_RETRY_MS);
}

async function postStartupProbeMetric(bridgeBase, entry) {
  const response = await fetch(`${bridgeBase}/desktop-local-data/startup-metric?t=${Date.now()}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event: entry.event,
      payload: entry.payload
    })
  });
  if (response && Object.prototype.hasOwnProperty.call(response, "ok") && response.ok === false) {
    throw new Error(`startup metric post failed: ${response.status || "request failed"}`);
  }
}

export async function flushStartupProbeMetricQueue() {
  if (pendingStartupProbeMetrics.length === 0) {
    clearStartupProbeRetry();
    return true;
  }
  const bridgeBase = resolveBridgeBase();
  if (!bridgeBase) {
    scheduleStartupProbeRetry();
    return false;
  }
  if (startupProbeFlushPromise) {
    return startupProbeFlushPromise;
  }
  startupProbeFlushPromise = (async () => {
    while (pendingStartupProbeMetrics.length > 0) {
      const entry = pendingStartupProbeMetrics[0];
      try {
        await postStartupProbeMetric(bridgeBase, entry);
        pendingStartupProbeMetrics.shift();
      } catch {
        scheduleStartupProbeRetry();
        return false;
      }
    }
    clearStartupProbeRetry();
    return true;
  })().finally(() => {
    startupProbeFlushPromise = null;
  });
  return startupProbeFlushPromise;
}

export function postStartupMetricToBridge(event, payload = {}) {
  const normalizedEvent = String(event || "").trim();
  if (!normalizedEvent) return;
  pendingStartupProbeMetrics.push({
    event: normalizedEvent,
    payload: normalizeStartupMetricPayload(payload)
  });
  void flushStartupProbeMetricQueue();
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
  postStartupMetricToBridge(event, payload);
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

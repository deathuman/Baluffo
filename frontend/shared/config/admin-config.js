const RUNTIME_BRIDGE_BASE_KEY = "baluffo_runtime_bridge_base";
const DEFAULT_CONFIG = {
  bridge: {
    host: "127.0.0.1",
    port: 8877
  },
  security: {
    github_app_enabled_default: true
  }
};

const BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze(
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG || {}
);

const BALUFFO_RUNTIME_CONFIG = {
  ...DEFAULT_CONFIG,
  ...BALUFFO_FRONTEND_RUNTIME_CONFIG,
  bridge: {
    ...DEFAULT_CONFIG.bridge,
    ...(BALUFFO_FRONTEND_RUNTIME_CONFIG?.bridge || {})
  },
  security: {
    ...DEFAULT_CONFIG.security,
    ...(BALUFFO_FRONTEND_RUNTIME_CONFIG?.security || {})
  }
};

function safeSetRuntimeBridgeBase(value) {
  try {
    window.sessionStorage.setItem(RUNTIME_BRIDGE_BASE_KEY, value);
  } catch {
    // Ignore storage write failures and use the resolved bridge for this page load.
  }
}

function runtimeBridgeBaseFromConfig() {
  if (BALUFFO_RUNTIME_CONFIG?.bridge?.sameOrigin) {
    return "";
  }
  if (!BALUFFO_RUNTIME_CONFIG?.runtime?.desktop) {
    return "";
  }
  const host =
    String(BALUFFO_RUNTIME_CONFIG?.bridge?.host || DEFAULT_CONFIG.bridge.host).trim() ||
    DEFAULT_CONFIG.bridge.host;
  const port = Number(BALUFFO_RUNTIME_CONFIG?.bridge?.port || 0);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    return "";
  }
  return `http://${host}:${port}`;
}

function resolveRuntimeBridgeBase() {
  const defaultHost =
    String(BALUFFO_RUNTIME_CONFIG?.bridge?.host || DEFAULT_CONFIG.bridge.host).trim() ||
    DEFAULT_CONFIG.bridge.host;
  const defaultPort =
    Number(BALUFFO_RUNTIME_CONFIG?.bridge?.port || DEFAULT_CONFIG.bridge.port) ||
    DEFAULT_CONFIG.bridge.port;
  const defaultBase = `http://${defaultHost}:${defaultPort}`;
  try {
    const configBase = runtimeBridgeBaseFromConfig();
    if (BALUFFO_RUNTIME_CONFIG?.bridge?.sameOrigin) {
      safeSetRuntimeBridgeBase(configBase);
      return configBase;
    }
    const url = new URL(window.location.href);
    const bridgePort = String(url.searchParams.get("bridgePort") || "").trim();
    const bridgeHost = String(url.searchParams.get("bridgeHost") || "").trim() || defaultHost;
    if (/^\d+$/.test(bridgePort)) {
      const runtimeBase = `http://${bridgeHost}:${bridgePort}`;
      safeSetRuntimeBridgeBase(runtimeBase);
      return runtimeBase;
    }
    if (configBase) {
      safeSetRuntimeBridgeBase(configBase);
      return configBase;
    }
    const cached = String(window.sessionStorage.getItem(RUNTIME_BRIDGE_BASE_KEY) || "").trim();
    if (cached) {
      return cached;
    }
  } catch {
    // Ignore URL/session parsing failures and fall back to the default bridge.
  }
  return defaultBase;
}

function queryJobsColdStartFlag() {
  try {
    const url = new URL(window.location.href);
    const value = String(url.searchParams.get("jobsColdStart") || "").trim().toLowerCase();
    return ["1", "true", "yes", "on"].includes(value);
  } catch {
    return false;
  }
}

function resolveDesktopJobsColdStart() {
  const runtimeValue = BALUFFO_RUNTIME_CONFIG?.runtime?.jobsColdStart;
  if (typeof runtimeValue === "boolean") {
    return runtimeValue;
  }
  return queryJobsColdStartFlag();
}

export const AdminConfig = {
  JOBS_LAST_URL_KEY: "baluffo_jobs_last_url",
  JOBS_FETCHER_COMMAND: "python -m src.jobs_fetcher",
  JOBS_FETCHER_TASK_LABEL: "Run jobs fetcher",
  JOBS_FETCH_REPORT_URL: "data/jobs-fetch-report.json",
  JOBS_AUTO_REFRESH_SIGNAL_KEY: "baluffo_jobs_auto_refresh_signal",
  FETCH_REPORT_POLL_INTERVAL_MS: 5000,
  FETCH_REPORT_POLL_TIMEOUT_MS: 10 * 60 * 1000,
  ADMIN_BRIDGE_BASE: resolveRuntimeBridgeBase(),
  BRIDGE_STATUS_POLL_INTERVAL_MS: 10000,
  DESKTOP_JOBS_COLD_START: resolveDesktopJobsColdStart(),
  GITHUB_APP_ENABLED_DEFAULT: Boolean(
    BALUFFO_FRONTEND_RUNTIME_CONFIG?.security?.github_app_enabled_default ?? true
  )
};

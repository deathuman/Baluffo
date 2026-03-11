import { BALUFFO_FRONTEND_RUNTIME_CONFIG } from "./frontend-runtime-config.js";

const RUNTIME_BRIDGE_BASE_KEY = "baluffo_runtime_bridge_base";
const DEFAULT_CONFIG = {
  bridge: {
    host: "127.0.0.1",
    port: 8877
  },
  security: {
    admin_pin_default: "1234",
    github_app_enabled_default: true
  }
};

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

function resolveRuntimeBridgeBase() {
  const defaultHost = String(BALUFFO_RUNTIME_CONFIG?.bridge?.host || DEFAULT_CONFIG.bridge.host).trim() || DEFAULT_CONFIG.bridge.host;
  const defaultPort = Number(BALUFFO_RUNTIME_CONFIG?.bridge?.port || DEFAULT_CONFIG.bridge.port) || DEFAULT_CONFIG.bridge.port;
  const defaultBase = `http://${defaultHost}:${defaultPort}`;
  try {
    const url = new URL(window.location.href);
    const bridgePort = String(url.searchParams.get("bridgePort") || "").trim();
    const bridgeHost = String(url.searchParams.get("bridgeHost") || "").trim() || defaultHost;
    if (/^\d+$/.test(bridgePort)) {
      const runtimeBase = `http://${bridgeHost}:${bridgePort}`;
      window.sessionStorage.setItem(RUNTIME_BRIDGE_BASE_KEY, runtimeBase);
      return runtimeBase;
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

export const AdminConfig = {
  JOBS_LAST_URL_KEY: "baluffo_jobs_last_url",
  JOBS_FETCHER_COMMAND: "python scripts/jobs_fetcher.py",
  JOBS_FETCHER_TASK_LABEL: "Run jobs fetcher",
  JOBS_FETCH_REPORT_URL: "data/jobs-fetch-report.json",
  JOBS_AUTO_REFRESH_SIGNAL_KEY: "baluffo_jobs_auto_refresh_signal",
  FETCH_REPORT_POLL_INTERVAL_MS: 5000,
  FETCH_REPORT_POLL_TIMEOUT_MS: 10 * 60 * 1000,
  ADMIN_BRIDGE_BASE: resolveRuntimeBridgeBase(),
  BRIDGE_STATUS_POLL_INTERVAL_MS: 10000,
  ADMIN_PIN_DEFAULT: String(BALUFFO_RUNTIME_CONFIG?.security?.admin_pin_default || DEFAULT_CONFIG.security.admin_pin_default),
  GITHUB_APP_ENABLED_DEFAULT: Boolean(
    BALUFFO_RUNTIME_CONFIG?.security?.github_app_enabled_default ?? DEFAULT_CONFIG.security.github_app_enabled_default
  )
};

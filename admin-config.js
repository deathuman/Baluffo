const RUNTIME_BRIDGE_BASE_KEY = "baluffo_runtime_bridge_base";
const DEFAULT_ADMIN_BRIDGE_BASE = "http://127.0.0.1:8877";

function resolveRuntimeBridgeBase() {
  try {
    const url = new URL(window.location.href);
    const bridgePort = String(url.searchParams.get("bridgePort") || "").trim();
    const bridgeHost = String(url.searchParams.get("bridgeHost") || "").trim() || "127.0.0.1";
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
  return DEFAULT_ADMIN_BRIDGE_BASE;
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
  BRIDGE_STATUS_POLL_INTERVAL_MS: 10000
};

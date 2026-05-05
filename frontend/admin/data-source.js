import { fetchJson, postJson } from "../shared/api-client.js";
import { timeFrontendAsync } from "../shared/perf-counters.js";

export async function fetchJobsFetchReportJson(jobsFetchReportUrl, options = {}) {
  try {
    const url = new URL(String(jobsFetchReportUrl || ""), globalThis.location?.href || "http://127.0.0.1/");
    url.searchParams.set("t", String(Date.now()));
    if (options?.live) {
      url.searchParams.set("view", "live");
    }
    const response = await timeFrontendAsync("frontend_fetch_admin_jobs_fetch_report", () => (
      fetch(String(url), { cache: "no-store" })
    ), {
      live: Boolean(options?.live)
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } catch {
    return null;
  }
}

// Admin data-source owns bridge/http IO concerns used by admin app orchestration.
export function emitAdminStartupMetric(adminBridgeBase, event, payload = {}) {
  postJson(adminBridgeBase, "/desktop-local-data/startup-metric", { event, payload: payload || {} }).catch(() => {});
}

export async function getBridge(adminBridgeBase, path, options = {}) {
  return fetchJson(adminBridgeBase, path, options);
}

export async function postBridge(adminBridgeBase, path, payload, options = {}) {
  return postJson(adminBridgeBase, path, payload || {}, options);
}

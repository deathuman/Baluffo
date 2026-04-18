import { fetchJson, postJson } from "../shared/api-client.js";

export async function fetchJobsFetchReportJson(jobsFetchReportUrl, options = {}) {
  try {
    const url = new URL(String(jobsFetchReportUrl || ""), globalThis.location?.href || "http://127.0.0.1/");
    url.searchParams.set("t", String(Date.now()));
    if (options?.live) {
      url.searchParams.set("view", "live");
    }
    const response = await fetch(String(url), { cache: "no-store" });
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

export async function getBridge(adminBridgeBase, path) {
  return fetchJson(adminBridgeBase, path);
}

export async function postBridge(adminBridgeBase, path, payload) {
  return postJson(adminBridgeBase, path, payload || {});
}

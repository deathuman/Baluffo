import { fetchJson, postJson } from "../shared/api-client.js";

export async function fetchJobsFetchReportJson(jobsFetchReportUrl) {
  try {
    const response = await fetch(`${jobsFetchReportUrl}?t=${Date.now()}`, { cache: "no-store" });
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

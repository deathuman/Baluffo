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

export function emitAdminStartupMetricsBatch(adminBridgeBase, metrics = []) {
  const rows = Array.isArray(metrics) ? metrics : [];
  if (!rows.length) return;
  postJson(adminBridgeBase, "/desktop-local-data/startup-metrics/batch", { metrics: rows }).catch(() => {
    rows.forEach(row => {
      if (!row || typeof row !== "object") return;
      emitAdminStartupMetric(adminBridgeBase, row.event, row.payload || {});
    });
  });
}

const LIGHTWEIGHT_GET_DEDUPE_PATHS = new Set([
  "/app/ready",
  "/admin/bootstrap",
  "/admin/ops-tab-counts?view=summary",
  "/ops/health?view=ready",
  "/ops/dashboard-health?view=summary",
  "/ops/fetch-report?view=summary",
  "/ops/fetch-kpis?view=summary",
  "/ops/task-state?view=summary",
  "/sync/status?view=summary",
  "/registry/conflicts?view=summary",
  "/discovery/report?view=summary"
]);
const lightweightGetRequests = new Map();

export async function getBridge(adminBridgeBase, path, options = {}) {
  if (LIGHTWEIGHT_GET_DEDUPE_PATHS.has(path)) {
    const key = `${adminBridgeBase || ""}|${path}|${Number(options.timeoutMs) || 0}`;
    if (lightweightGetRequests.has(key)) return lightweightGetRequests.get(key);
    const request = fetchJson(adminBridgeBase, path, options)
      .finally(() => {
        lightweightGetRequests.delete(key);
      });
    lightweightGetRequests.set(key, request);
    return request;
  }
  return fetchJson(adminBridgeBase, path, options);
}

export async function postBridge(adminBridgeBase, path, payload, options = {}) {
  return postJson(adminBridgeBase, path, payload || {}, options);
}

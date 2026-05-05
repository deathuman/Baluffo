/**
 * Unified bridge API client. Single place for fetch calls to the admin bridge
 * and consistent error handling (401 Unauthorized, 5xx Bridge error, network errors).
 * Callers pass baseUrl (from admin config) so config remains per-page.
 * Bridge paths used by frontend: see docs/admin-bridge-api.md; jobs/saved/admin use
 * fetch, pipeline, saved-jobs, ops, sync, discovery endpoints via this module.
 */

import { timeFrontendAsync } from "./perf-counters.js";

const DEFAULT_TIMEOUT_MS = 18000;

/**
 * @param {string} baseUrl - Bridge base URL (e.g. from adminConfig.ADMIN_BRIDGE_BASE)
 * @returns {string} baseUrl normalized (no trailing slash)
 */
function normalizeBaseUrl(baseUrl) {
  const s = String(baseUrl || "").trim();
  return s.endsWith("/") ? s.slice(0, -1) : s || "http://127.0.0.1:8877";
}

/**
 * Map response status to a short error message for callers/toasts.
 * @param {number} status
 * @returns {string}
 */
function getBridgeErrorMessage(status) {
  if (status === 401) return "Unauthorized";
  if (status === 403) return "Forbidden";
  if (status >= 500) return "Bridge error";
  if (status >= 400) return `Request failed (${status})`;
  return "Request failed";
}

/**
 * Low-level fetch against the bridge. Adds cache-bust query, optional timeout, and rejects with a consistent Error.
 * @param {string} baseUrl - Bridge base URL
 * @param {string} path - Path (e.g. "/ops/fetch-report")
 * @param {{ method?: string, body?: object, headers?: object, cache?: RequestCache, timeoutMs?: number }} [options]
 * @returns {Promise<Response>} - Raw response; caller can .json() or .text()
 */
export async function fetchBridge(baseUrl, path, options = {}) {
  const base = normalizeBaseUrl(baseUrl);
  const pathWithQuery = path.includes("?") ? `${path}&t=${Date.now()}` : `${path}?t=${Date.now()}`;
  const url = `${base}${pathWithQuery}`;
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : DEFAULT_TIMEOUT_MS;
  const allowedStatuses = new Set(
    Array.isArray(options.allowStatuses) ? options.allowStatuses.map(status => Number(status)) : []
  );
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  const method = options.method || "GET";
  return timeFrontendAsync(`frontend_fetch_bridge_${method}_${path}`, async () => {
    try {
      const response = await fetch(url, {
        method,
        cache: options.cache ?? "no-store",
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {})
        },
        body: options.body != null ? JSON.stringify(options.body) : undefined,
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      if (!response.ok && !allowedStatuses.has(response.status)) {
        const msg = getBridgeErrorMessage(response.status);
        throw new Error(`Bridge ${method} ${path} failed: ${msg} (HTTP ${response.status})`);
      }
      return response;
    } catch (err) {
      clearTimeout(timeoutId);
      if (err.name === "AbortError") {
        throw new Error("Bridge request timed out");
      }
      if (err instanceof TypeError && err.message && err.message.includes("fetch")) {
        throw new Error("Network error: bridge unreachable");
      }
      throw err;
    }
  }, { method, path });
}

/**
 * GET path and parse JSON. Rejects with Error on non-ok or parse failure.
 * @param {string} baseUrl
 * @param {string} path
 * @param {{ timeoutMs?: number, allowStatuses?: number[] }} [options]
 * @returns {Promise<object>}
 */
export async function fetchJson(baseUrl, path, options = {}) {
  const response = await fetchBridge(baseUrl, path, { ...options, method: "GET" });
  return response.json();
}

/**
 * POST path with JSON body and parse JSON response.
 * @param {string} baseUrl
 * @param {string} path
 * @param {object} payload
 * @param {{ timeoutMs?: number, allowStatuses?: number[] }} [options]
 * @returns {Promise<object>}
 */
export async function postJson(baseUrl, path, payload, options = {}) {
  const response = await fetchBridge(baseUrl, path, {
    ...options,
    method: "POST",
    body: payload
  });
  const data = await response.json();
  if (options.returnMeta) {
    return {
      status: response.status,
      ok: response.ok,
      data
    };
  }
  return data;
}

/**
 * @fileoverview Shared value/formatting helpers.
 * Small pure coercions used by more than one page slice.
 */

/**
 * Formats a millisecond duration as a compact human string.
 * @param {*} ms
 * @returns {string}
 */
export function formatDuration(ms) {
  const value = Math.max(0, Number(ms) || 0);
  if (!value) return "0s";
  if (value < 1000) return `${value}ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(1)}s`;
  return `${(value / 60_000).toFixed(1)}m`;
}

/**
 * Formats a byte count as B/KB/MB/GB with one decimal place.
 * @param {*} bytes
 * @returns {string}
 */
export function formatBytes(bytes) {
  const value = Number(bytes) || 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

/**
 * Formats a timestamp as a short en-US date, or "" when unparseable.
 * @param {*} value
 * @returns {string}
 */
export function formatDateForStatus(value) {
  const parsed = new Date(String(value || ""));
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

/**
 * Resolves a "now" input (function, Date, number, or nothing) to epoch ms.
 * @param {*} value
 * @returns {number}
 */
export function resolveNowMs(value) {
  if (typeof value === "function") return Number(value()) || Date.now();
  if (value instanceof Date) return value.getTime();
  return Number(value) || Date.now();
}

/**
 * Clamps a value into the 0..1 ratio range; non-finite input becomes 0.
 * @param {*} value
 * @returns {number}
 */
export function clampRatio(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(1, numeric));
}

/**
 * Trims text and shortens it past `limit` with a trailing ellipsis.
 * @param {*} value
 * @param {number} [limit]
 * @returns {string}
 */
export function truncateText(value, limit = 180) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.length > limit ? `${text.slice(0, Math.max(0, limit - 1)).trimEnd()}...` : text;
}

/**
 * Coerces a value to a trimmed string, falling back when it is empty.
 * @param {*} value
 * @param {string} [fallback]
 * @returns {string}
 */
export function stringValue(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

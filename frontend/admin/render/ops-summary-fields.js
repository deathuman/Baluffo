/**
 * Admin Ops summary rendering — pending-aware scalar field formatters.
 *
 * Split out of ``ops-summary.js``; that module stays the thin coordinator
 * owning the five public render entrypoints.
 *
 * @module ops-summary-fields
 */

import { escapeHtml } from "../../shared/ui/index.js";
import { formatDuration } from "./ops-shared.js";

function hasOwnField(object, key) {
  return Boolean(object && typeof object === "object" && Object.prototype.hasOwnProperty.call(object, key));
}

function formatPendingField(label = "Not loaded yet") {
  return `<span class="muted">${escapeHtml(label)}</span>`;
}

function formatOptionalNumber(object, key, { pending = "Not loaded yet" } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = Number(object?.[key]);
  if (!Number.isFinite(value)) return formatPendingField(pending);
  return value.toLocaleString();
}

function formatOptionalPercent(object, key, { pending = "Not loaded yet" } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = Number(object?.[key]);
  if (!Number.isFinite(value)) return formatPendingField(pending);
  return `${(value * 100).toFixed(1)}%`;
}

function formatOptionalDuration(object, key, { pending = "Not loaded yet" } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = Number(object?.[key]);
  if (!Number.isFinite(value)) return formatPendingField(pending);
  return escapeHtml(formatDuration(value));
}

function formatOptionalText(object, key, { pending = "Not loaded yet", formatter = value => (value == null ? "" : String(value)) } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = object?.[key];
  const text = formatter(value);
  return escapeHtml(text || pending);
}

// The probe-wave receipt's own counters, in operator terms. `fetchesBoundedOut` is the abandoned
// count (a fetch that outlived its per-call bound and kept running), `clientsUnclosed` is the leak
// flag, and the two outstanding counts say whether the work was still in flight when its batch left.
const PROBE_LIFECYCLE_ATTENTION_KEYS = [
  "fetchesBoundedOut",
  "fetchesOutstandingAtExit",
  "clientFetchesOutstandingAtExit",
  "clientsUnclosed",
  "deadlineExits"
];

function probeLifecycleCount(receipt, key) {
  if (!hasOwnField(receipt, key)) return null;
  const value = Number(receipt?.[key]);
  return Number.isFinite(value) ? value : null;
}

/**
 * One line an operator can read without opening the audit artifact: did the probe wave abandon
 * fetches, and was any shared client left unclosed. Returns "" when the receipt itself is absent.
 */
function formatProbeLifecycleReceipt(receipt) {
  if (!hasOwnField(receipt, "fetchesStarted")) return "";
  const count = key => probeLifecycleCount(receipt, key) ?? 0;
  const abandoned = count("fetchesBoundedOut");
  const outstanding = count("fetchesOutstandingAtExit");
  const insideClient = count("clientFetchesOutstandingAtExit");
  const unclosed = count("clientsUnclosed");
  const deadlineExits = count("deadlineExits");
  const rows = receipt && typeof receipt === "object" ? receipt : {};
  const batches = probeLifecycleCount(rows, "batches");
  const partial = PROBE_LIFECYCLE_ATTENTION_KEYS.some(key => probeLifecycleCount(rows, key) === null);
  const attention = PROBE_LIFECYCLE_ATTENTION_KEYS.some(key => count(key) > 0) || partial;
  const chipClass = unclosed > 0 ? "critical" : attention ? "warning" : "healthy";
  const chipLabel = unclosed > 0
    ? "client left unclosed"
    : attention ? "probe attention" : "clean";
  const batchText = batches === null ? "" : `${batches.toLocaleString()} batch${batches === 1 ? "" : "es"}, `;
  return `
      <span class="admin-status-chip ${chipClass}">${escapeHtml(chipLabel)}</span>
      ${batchText}${count("fetchesStarted").toLocaleString()} probe fetches:
      ${abandoned.toLocaleString()} abandoned by their own bound,
      ${outstanding.toLocaleString()} still out when the batch left (${insideClient.toLocaleString()} inside the shared client),
      ${unclosed.toLocaleString()} clients unclosed,
      ${deadlineExits.toLocaleString()} deadline exits${partial ? ", receipt fields incomplete" : ""}.
    `;
}

/**
 * One line per source an operator can read without opening the audit artifact: how many page
 * fetches the source's batches started, how many were still outstanding when the batch left, and
 * whether the reading is complete. Returns "" when no source published a receipt.
 *
 * A truncated reading is never rendered as clean: `coverage`/`truncated`/`omittedCount` travel with
 * each source entry, so a bounded receipt gets its own chip label rather than "clean".
 */
function formatPageFetchLifecycleReceipt(receipt) {
  const sources = receipt && typeof receipt === "object" ? receipt.sources : null;
  if (!sources || typeof sources !== "object") return "";
  const names = Object.keys(sources).sort();
  if (!names.length) return "";
  const cells = names.map(source => {
    const entry = sources[source] && typeof sources[source] === "object" ? sources[source] : {};
    const rollup = entry.rollup && typeof entry.rollup === "object" ? entry.rollup : {};
    const count = key => {
      const value = Number(rollup[key]);
      return Number.isFinite(value) ? value : 0;
    };
    const truncated = entry.truncated === true || entry.coverage === "partial";
    const state = typeof entry.state === "string" ? entry.state : "";
    const outstanding = count("outstandingNow");
    const failed = count("failed");
    const pendingAsync = count("pendingAsyncCleanup");
    const attention = outstanding > 0 || failed > 0 || pendingAsync > 0 || truncated || state === "pending";
    const chipClass = truncated ? "warning" : outstanding > 0 || pendingAsync > 0 ? "warning" : failed > 0 ? "warning" : state === "pending" ? "warning" : "healthy";
    const chipLabel = truncated ? "reading truncated" : state === "pending" ? "settling" : attention ? "page attention" : "clean";
    const omitted = Number(entry.omittedCount);
    const omittedText = truncated && Number.isFinite(omitted) && omitted > 0
      ? `, ${omitted.toLocaleString()} omitted`
      : "";
    return `
      <span class="admin-status-chip ${chipClass}">${escapeHtml(chipLabel)}</span>
      ${escapeHtml(String(source))}: ${count("submitted").toLocaleString()} page fetches,
      ${count("finished").toLocaleString()} finished,
      ${failed.toLocaleString()} failed,
      ${outstanding.toLocaleString()} still out when the batch left,
      ${pendingAsync.toLocaleString()} awaiting cleanup${omittedText}.
    `;
  });
  return cells.join("");
}

export {
  hasOwnField,
  formatPendingField,
  formatOptionalNumber,
  formatOptionalPercent,
  formatOptionalDuration,
  formatOptionalText,
  formatPageFetchLifecycleReceipt,
  formatProbeLifecycleReceipt
};

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

// The probe-wave receipt's own counters, in operator terms. `fetchesBoundedOut` is the number
// that outlived its per-call bound, `clientsUnclosed` is the leak flag, and the two outstanding
// counts say whether work was still in flight when the batch left. A bounded fetch is not itself
// a leak: if it settled by exit, it belongs in the explanation, not in the attention chip.
const PROBE_LIFECYCLE_ATTENTION_KEYS = [
  "fetchesOutstandingAtExit",
  "clientFetchesOutstandingAtExit",
  "clientsUnclosed",
  "deadlineExits"
];
const PROBE_LIFECYCLE_DISPLAY_KEYS = ["fetchesBoundedOut", ...PROBE_LIFECYCLE_ATTENTION_KEYS];

function probeLifecycleCount(receipt, key) {
  if (!hasOwnField(receipt, key)) return null;
  const value = Number(receipt?.[key]);
  return Number.isFinite(value) ? value : null;
}

/**
 * One line an operator can read without opening the audit artifact: did the probe wave leave
 * work running, and was any shared client left unclosed. Returns "" when the receipt itself is
 * absent.
 */
function formatProbeLifecycleReceipt(receipt) {
  if (!hasOwnField(receipt, "fetchesStarted")) return "";
  const count = key => probeLifecycleCount(receipt, key) ?? 0;
  const bounded = count("fetchesBoundedOut");
  const outstanding = count("fetchesOutstandingAtExit");
  const insideClient = count("clientFetchesOutstandingAtExit");
  const unclosed = count("clientsUnclosed");
  const deadlineExits = count("deadlineExits");
  const rows = receipt && typeof receipt === "object" ? receipt : {};
  const batches = probeLifecycleCount(rows, "batches");
  const partial = PROBE_LIFECYCLE_DISPLAY_KEYS.some(key => probeLifecycleCount(rows, key) === null);
  const attention = PROBE_LIFECYCLE_ATTENTION_KEYS.some(key => count(key) > 0) || partial;
  const chipClass = unclosed > 0 ? "critical" : attention ? "warning" : "healthy";
  const chipLabel = unclosed > 0
    ? "client left unclosed"
    : attention ? "probe attention" : "clean";
  const settledAfterBound = Math.max(0, bounded - outstanding);
  const settledText = settledAfterBound > 0
    ? ` (${settledAfterBound.toLocaleString()} settled before the batch left)`
    : "";
  const batchText = batches === null ? "" : `${batches.toLocaleString()} batch${batches === 1 ? "" : "es"}, `;
  return `
      <span class="admin-status-chip ${chipClass}">${escapeHtml(chipLabel)}</span>
      ${batchText}${count("fetchesStarted").toLocaleString()} probe fetches:
      ${bounded.toLocaleString()} hit their per-call timeout${settledText},
      ${outstanding.toLocaleString()} still running when the batch left (${insideClient.toLocaleString()} inside the shared client),
      ${unclosed.toLocaleString()} clients unclosed,
      ${deadlineExits.toLocaleString()} deadline exits${partial ? ", receipt fields incomplete" : ""}.
    `;
}

function pageFailureStageLabel(stage, count = 0) {
  const normalized = String(stage || "").trim().toLowerCase();
  const singular = count === 1;
  if (normalized === "recovery_fetch") return singular ? "recovery guess" : "recovery guesses";
  if (normalized === "page_fetch") return singular ? "page fetch" : "page fetches";
  if (normalized === "website_fetch" || normalized === "directory_page_fetch") {
    return singular ? "homepage fetch" : "homepage fetches";
  }
  const label = normalized.replace(/_/g, " ") || "unknown";
  return singular ? label : `${label}s`;
}

/**
 * One line per source an operator can read without opening the audit artifact: how many page
 * fetches the source's batches started, how many succeeded or failed, and whether the reading is
 * complete. Returns "" when no source published a receipt.
 *
 * `failed` is an outcome count, not a lifecycle leak. A source can be lifecycle-clean while its
 * recovery wave misses generated URLs; the outcome chip and stage breakdown name those failures
 * separately instead of turning the whole row into "page attention". A truncated reading is never
 * rendered as clean: `coverage`/`truncated`/`omittedCount` travel with each source entry.
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
    const submitted = count("submitted");
    const finished = count("finished");
    const failed = count("failed");
    const succeeded = Math.max(0, finished - failed);
    const outstanding = count("outstandingNow");
    const pendingAsync = count("pendingAsyncCleanup");
    const lifecyclePending = outstanding > 0 || pendingAsync > 0 || state === "pending";
    const chipClass = truncated || lifecyclePending ? "warning" : "healthy";
    const chipLabel = truncated
      ? "reading truncated"
      : lifecyclePending ? "settling" : "lifecycle clean";
    const stageCounts = entry.failureStageCounts && typeof entry.failureStageCounts === "object"
      ? entry.failureStageCounts
      : {};
    const allFailureStages = Object.entries(stageCounts)
      .map(([stage, value]) => [String(stage), Number(value)])
      .filter(([, value]) => Number.isFinite(value) && value > 0)
      .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
    const recoveryFailures = allFailureStages
      .filter(([stage]) => stage.toLowerCase().includes("recovery"))
      .reduce((total, [, value]) => total + value, 0);
    const failureChipLabel = failed > 0
      ? recoveryFailures === failed
        ? `${failed.toLocaleString()} recovery ${failed === 1 ? "guess" : "guesses"} failed`
        : `${failed.toLocaleString()} page ${failed === 1 ? "fetch" : "fetches"} failed`
      : "";
    const failureChip = failureChipLabel
      ? `\n      <span class="admin-status-chip warning">${escapeHtml(failureChipLabel)}</span>`
      : "";
    const visibleStages = allFailureStages.slice(0, 4);
    const stageParts = visibleStages.map(([stage, value]) =>
      `${value.toLocaleString()} ${pageFailureStageLabel(stage, value)}`
    );
    if (allFailureStages.length > visibleStages.length) {
      stageParts.push(`+${allFailureStages.length - visibleStages.length} more stages`);
    }
    const stageText = stageParts.length ? ` Failure stages: ${stageParts.join(", ")}.` : "";
    const failurePercentText = finished > 0 ? ` (${((failed / finished) * 100).toFixed(1)}%)` : "";
    const omitted = Number(entry.omittedCount);
    const omittedText = truncated && Number.isFinite(omitted) && omitted > 0
      ? `, ${omitted.toLocaleString()} omitted`
      : "";
    return `
      <span class="admin-status-chip ${chipClass}">${escapeHtml(chipLabel)}</span>${failureChip}
      ${escapeHtml(String(source))}: ${submitted.toLocaleString()} page fetches,
      ${finished.toLocaleString()} finished: ${succeeded.toLocaleString()} succeeded,
      ${failed.toLocaleString()} failed${failurePercentText},
      ${outstanding.toLocaleString()} still out when the batch left,
      ${pendingAsync.toLocaleString()} awaiting cleanup${omittedText}.${escapeHtml(stageText)}
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

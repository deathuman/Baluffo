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

export {
  hasOwnField,
  formatPendingField,
  formatOptionalNumber,
  formatOptionalPercent,
  formatOptionalDuration,
  formatOptionalText
};

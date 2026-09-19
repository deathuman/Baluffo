/**
 * Admin Ops summary rendering — pipeline schedule status, controls, and the
 * schedule entrypoint.
 *
 * Split out of ``ops-summary.js``; that module stays the thin coordinator
 * owning the five public render entrypoints.
 *
 * @module ops-summary-schedule
 */

import { escapeHtml } from "../../shared/ui/index.js";
import {
  formatDateTime,
  stableOpsSignature
} from "./ops-shared.js";

function formatPipelineScheduleStatus(entry) {
  const interval = Number(entry?.intervalHours || 0);
  const nextRaw = String(entry?.nextRunAt || "").trim();
  const next = formatDateTime(nextRaw);
  const hasNext = Boolean(nextRaw) && next !== "unknown";
  const error = String(entry?.lastTriggerError || "").trim();
  if (!entry || Object.keys(entry).length === 0 || entry.scheduleLoading) {
    return entry?.scheduleRetrying ? "schedule delayed; retrying" : "loading schedule...";
  }
  if (!entry.enabled) return "disabled";
  if (error) return `needs attention: ${error}`;
  if (interval > 0 && entry.nextAfterCurrentCompletes) {
    return `every ${interval}h, running now; next after this pipeline finishes`;
  }
  if (interval > 0 && entry.scheduleStatusRefreshing) {
    return "schedule details refreshing";
  }
  if (entry.pending) return "pending; waiting for idle";
  if (interval > 0 && hasNext) return `every ${interval}h, next ${next}`;
  if (entry.due) return "due now";
  if (interval > 0) return `every ${interval}h`;
  return "enabled";
}

function renderPipelineScheduleControls(pipeline) {
  const loading = Boolean(!pipeline || Object.keys(pipeline).length === 0 || pipeline.scheduleLoading);
  const interval = Number(loading ? Number.NaN : (pipeline?.intervalHours || 24));
  const safeInterval = Number.isFinite(interval)
    ? Math.max(1, Math.min(168, Math.trunc(interval)))
    : "";
  const enabled = !loading && Boolean(pipeline?.enabled);
  const disabled = loading ? "disabled" : "";
  return `
    <div class="admin-ops-schedule-item admin-ops-pipeline-schedule admin-ops-full-row">
      <div class="admin-ops-pipeline-schedule-summary">
        <strong>Pipeline</strong>: ${escapeHtml(formatPipelineScheduleStatus(pipeline || {}))}
      </div>
      <div class="admin-ops-pipeline-schedule-controls" data-ui="admin-pipeline-schedule-controls">
        <label class="admin-ops-pipeline-schedule-toggle">
          <input type="checkbox" data-ui="admin-pipeline-schedule-enabled" ${enabled ? "checked" : ""} ${disabled}>
          <span>Enable</span>
        </label>
        <label class="admin-ops-pipeline-schedule-interval">
          <span>Every</span>
          <input type="number" min="1" max="168" step="1" value="${safeInterval}" data-ui="admin-pipeline-schedule-interval" ${disabled}>
          <span>h</span>
        </label>
        <button type="button" class="btn clear-filters-btn" data-action="save-pipeline-schedule" ${disabled}>Save</button>
      </div>
    </div>
  `;
}

function renderAdminOpsSchedule(scheduleEl, schedule) {
  if (!scheduleEl) return;
  const canPatchInPlace = Boolean(scheduleEl && scheduleEl.dataset);
  const signature = stableOpsSignature({
    pipeline: schedule?.pipeline || {}
  });
  if (canPatchInPlace && scheduleEl.dataset.opsScheduleSig === signature) return;
  if (canPatchInPlace) scheduleEl.dataset.opsScheduleSig = signature;
  const pipeline = schedule?.pipeline || {};
  scheduleEl.innerHTML = `
    ${renderPipelineScheduleControls(pipeline)}
  `;
}

export { renderAdminOpsSchedule };

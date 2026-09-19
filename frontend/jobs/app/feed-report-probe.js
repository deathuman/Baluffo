/**
 * Jobs feed — fetcher-report and bootstrap-progress probing.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 *
 * @module feed-report-probe
 */

import {
  FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS,
  FIRST_RUN_BOOTSTRAP_UNCONFIRMED_MESSAGE
} from "./feed-constants.js";

function reportFinishedTimestamp(report) {
  const finishedAt = String(report?.finishedAt || "").trim();
  if (!finishedAt) return null;
  const timestamp = Date.parse(finishedAt);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function reportSummary(report) {
  return report?.summary && typeof report.summary === "object" ? report.summary : {};
}

function reportStatus(report) {
  const summary = reportSummary(report);
  return String(summary.status || report?.status || "").trim().toLowerCase();
}

function isSuccessfulJobsFetchReport(report) {
  if (!report || typeof report !== "object") return false;
  if (!reportFinishedTimestamp(report)) return false;
  const summary = reportSummary(report);
  const status = reportStatus(report);
  if (status === "error" || status === "failed") return false;
  return Number(summary.outputCount || 0) > 0;
}

function isTerminalFailedJobsFetchReport(report) {
  if (!report || typeof report !== "object") return false;
  if (!reportFinishedTimestamp(report)) return false;
  const summary = reportSummary(report);
  return ["error", "failed"].includes(reportStatus(report))
    || Boolean(summary.error);
}

function isNonTerminalJobsFetchReport(report) {
  return Boolean(report && typeof report === "object")
    && !isSuccessfulJobsFetchReport(report)
    && !isTerminalFailedJobsFetchReport(report);
}

function coverageScope(report) {
  const runtime = report?.runtime && typeof report.runtime === "object" ? report.runtime : {};
  const summary = reportSummary(report);
  return String(summary.coverageScope || runtime.coverageScope || "").trim();
}

function reportRunId(report) {
  const summary = reportSummary(report);
  return String(report?.runId || summary.runId || "").trim();
}

function isActiveBootstrapReport(report) {
  if (!report || typeof report !== "object") return false;
  if (reportFinishedTimestamp(report)) return false;
  const scope = coverageScope(report).toLowerCase();
  const runId = reportRunId(report);
  return scope === "bootstrap_sheets" || runId.startsWith("jobs_bootstrap_");
}

function timestampMs(value) {
  const timestamp = Date.parse(String(value || ""));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function latestBootstrapProgressTimestamp(payload) {
  if (!payload || typeof payload !== "object") return 0;
  const candidates = [
    payload.heartbeatAt,
    payload.updatedAt,
    payload.taskProgress?.updatedAt,
    payload.runtime?.lifecycle?.heartbeatAt
  ].map(timestampMs);
  for (const workItem of (Array.isArray(payload.workItems) ? payload.workItems : [])) {
    candidates.push(timestampMs(workItem?.heartbeatAt));
    candidates.push(timestampMs(workItem?.progress?.updatedAt));
  }
  return Math.max(0, ...candidates);
}

function isFreshBootstrapProgress(payload, { now = Date.now(), staleMs } = {}) {
  if (!payload || typeof payload !== "object") return false;
  const status = String(payload.status || "").trim().toLowerCase();
  const active = Boolean(
    payload.active
      || payload.taskProgress?.active
      || status === "running"
      || isActiveBootstrapReport(payload)
  );
  if (!active) return false;
  const latestProgressAt = latestBootstrapProgressTimestamp(payload);
  if (!latestProgressAt) return false;
  return now - latestProgressAt <= Math.max(1000, Number(staleMs) || FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS);
}

function bootstrapStartHasRunningEvidence(payload) {
  return Boolean(
    payload
      && typeof payload === "object"
      && (payload.started || payload.alreadyRunning || payload.alreadyCompleted)
  );
}

function isUncertainBootstrapStartError(err) {
  const message = String(err?.message || err || "").toLowerCase();
  return message.includes("timed out")
    || message.includes("bridge unreachable")
    || message.includes("network error")
    || message.includes("failed to fetch");
}

function bootstrapStartUnconfirmedError() {
  const error = new Error(FIRST_RUN_BOOTSTRAP_UNCONFIRMED_MESSAGE);
  error.bootstrapStartUnconfirmed = true;
  return error;
}

export {
  reportFinishedTimestamp,
  reportSummary,
  isSuccessfulJobsFetchReport,
  isTerminalFailedJobsFetchReport,
  isNonTerminalJobsFetchReport,
  coverageScope,
  reportRunId,
  isActiveBootstrapReport,
  isFreshBootstrapProgress,
  bootstrapStartHasRunningEvidence,
  isUncertainBootstrapStartError,
  bootstrapStartUnconfirmedError
};

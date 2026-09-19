/**
 * Jobs feed — first-run bootstrap marker storage and cold-start decisions.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 *
 * @module feed-bootstrap-marker
 */

import {
  BOOTSTRAP_AUTO_START_KEY,
  BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY,
  FIRST_RUN_BOOTSTRAP_NOTICE
} from "./feed-constants.js";
import {
  isNonTerminalJobsFetchReport,
  isSuccessfulJobsFetchReport,
  isTerminalFailedJobsFetchReport,
  reportSummary
} from "./feed-report-probe.js";

function localStorageFor(windowObject) {
  try {
    return windowObject?.localStorage || null;
  } catch {
    return null;
  }
}

function sessionStorageFor(windowObject) {
  try {
    return windowObject?.sessionStorage || null;
  } catch {
    return null;
  }
}

function bootstrapAutoStartMarker(windowObject) {
  const value = localStorageFor(windowObject)?.getItem(BOOTSTRAP_AUTO_START_KEY);
  if (!value) return { status: "none" };
  if (value === "1") return { status: "legacy" };
  try {
    const parsed = JSON.parse(value);
    const status = String(parsed?.status || "").trim().toLowerCase();
    if (status === "running" || status === "failed") {
      return {
        status,
        runId: String(parsed?.runId || "").trim(),
        error: String(parsed?.error || "").trim()
      };
    }
  } catch {
    // Fall through to legacy handling for older/corrupt markers.
  }
  return { status: "legacy" };
}

function writeBootstrapAutoStartMarker(windowObject, status, details = {}) {
  localStorageFor(windowObject)?.setItem(BOOTSTRAP_AUTO_START_KEY, JSON.stringify({
    status,
    runId: String(details.runId || "").trim(),
    error: String(details.error || "").trim(),
    updatedAt: new Date().toISOString()
  }));
}

function markBootstrapRunning(windowObject, details = {}) {
  writeBootstrapAutoStartMarker(windowObject, "running", details);
}

function markBootstrapFailed(windowObject, error) {
  writeBootstrapAutoStartMarker(windowObject, "failed", { error });
}

function clearBootstrapAutoStart(windowObject) {
  localStorageFor(windowObject)?.removeItem(BOOTSTRAP_AUTO_START_KEY);
}

function markLaunchColdStartHandled(windowObject) {
  sessionStorageFor(windowObject)?.setItem(BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY, "1");
}

function launchColdStartAlreadyHandled(windowObject) {
  return sessionStorageFor(windowObject)?.getItem(BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY) === "1";
}

function bootstrapColdStartAction(report, windowObject, { forceStart = false } = {}) {
  if (!forceStart && isSuccessfulJobsFetchReport(report)) return false;
  if (!forceStart && isTerminalFailedJobsFetchReport(report)) return "retry";
  const marker = bootstrapAutoStartMarker(windowObject);
  if (marker.status === "failed") return forceStart ? "start" : "retry";
  if (marker.status === "running" || marker.status === "legacy") {
    return forceStart || isNonTerminalJobsFetchReport(report) ? "reattach" : "retry";
  }
  return "start";
}

function bootstrapRetryMessage(report) {
  const summary = reportSummary(report);
  const error = String(summary.error || "").trim();
  return error
    ? `First-run sheet refresh failed: ${error}`
    : "No fresh local jobs feed is available yet. Retry the quick sheet refresh.";
}

function notifyFirstRunBootstrap(showFirstRunBootstrapNotice, reason = "") {
  if (typeof showFirstRunBootstrapNotice !== "function") return;
  try {
    showFirstRunBootstrapNotice({
      ...FIRST_RUN_BOOTSTRAP_NOTICE,
      reason: String(reason || "")
    });
  } catch {
    // The notice is informational only; bootstrap must continue if UI setup fails.
  }
}

function sleep(ms) {
  const delay = Math.max(0, Number(ms) || 0);
  if (delay <= 0) return Promise.resolve();
  return new Promise(resolve => setTimeout(resolve, delay));
}

export {
  bootstrapAutoStartMarker,
  markBootstrapRunning,
  markBootstrapFailed,
  clearBootstrapAutoStart,
  markLaunchColdStartHandled,
  launchColdStartAlreadyHandled,
  bootstrapColdStartAction,
  bootstrapRetryMessage,
  notifyFirstRunBootstrap,
  sleep
};

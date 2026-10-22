import { createElapsedMsTracker, pageClockMs } from "../../../shared/perf-marks.js";

export function createAdminStartupMetrics({
  emitStartupMetric,
  emitStartupMetricsBatch,
  now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now())
}) {
  let firstInteractiveSent = false;
  let flushTimer = null;
  const startedAtMs = Number(now()) || 0;
  const pendingMetrics = [];
  const withElapsedMs = createElapsedMsTracker(now, startedAtMs);
  function stamped(payload) {
    // The page clock travels with every row so the startup profile can order admin
    // page events by when they happened instead of when a queued POST reached the
    // bridge. Without it the admin stages (and their bottleneck attribution) ride on
    // arrival time, which a busy metric queue can skew by hundreds of milliseconds.
    return withElapsedMs({ ...payload, pageTsMs: pageClockMs(now) });
  }
  function enqueue(event, payload = {}) {
    const cleanEvent = String(event || "").trim();
    if (!cleanEvent) return;
    pendingMetrics.push({ event: cleanEvent, payload: stamped(payload) });
    if (pendingMetrics.length > 200) pendingMetrics.splice(0, pendingMetrics.length - 200);
    if (firstInteractiveSent) scheduleFlush();
  }
  function flush() {
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    const rows = pendingMetrics.splice(0, pendingMetrics.length);
    if (!rows.length) return;
    if (typeof emitStartupMetricsBatch === "function") {
      emitStartupMetricsBatch(rows);
      return;
    }
    rows.forEach(row => emitStartupMetric(row.event, row.payload));
  }
  function scheduleFlush(delayMs = 250) {
    if (flushTimer !== null) return;
    flushTimer = setTimeout(flush, Math.max(0, Number(delayMs) || 0));
  }
  return {
    emit(event, payload = {}) {
      enqueue(event, payload);
    },
    markFirstInteractive(reason) {
      if (firstInteractiveSent) return;
      enqueue("admin_first_interactive", {
        reason: String(reason || "unknown")
      });
      firstInteractiveSent = true;
      scheduleFlush(0);
    }
  };
}

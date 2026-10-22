import { createElapsedMsTracker, pageClockMs } from "../../../shared/perf-marks.js";

export function createJobsStartupMetrics({
  emitMetric,
  now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now())
}) {
  let startupRenderMetricSent = false;
  let startupInteractiveMetricSent = false;
  const startedAtMs = Number(now()) || 0;
  const withElapsedMs = createElapsedMsTracker(now, startedAtMs);
  function stamped(payload) {
    return withElapsedMs({ ...payload, pageTsMs: pageClockMs(now) });
  }
  return {
    emit(event, payload = {}) {
      emitMetric(event, stamped(payload));
    },
    markRendered(stage, rowCount = 0) {
      if (startupRenderMetricSent) return;
      startupRenderMetricSent = true;
      emitMetric("jobs_first_render", stamped({
        stage: String(stage || "unknown"),
        rowCount: Math.max(0, Number(rowCount || 0))
      }));
    },
    markInteractive(reason) {
      if (startupInteractiveMetricSent) return;
      startupInteractiveMetricSent = true;
      emitMetric("jobs_first_interactive", stamped({
        reason: String(reason || "unknown")
      }));
    }
  };
}

export function createJobsStartupMetrics({
  emitMetric,
  now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now())
}) {
  let startupRenderMetricSent = false;
  let startupInteractiveMetricSent = false;
  const startedAtMs = Number(now()) || 0;
  function withElapsedMs(payload = {}) {
    if (Object.prototype.hasOwnProperty.call(payload, "elapsedMs")) return payload;
    return {
      ...payload,
      elapsedMs: Math.max(0, Math.round((Number(now()) || startedAtMs) - startedAtMs))
    };
  }
  return {
    emit(event, payload = {}) {
      emitMetric(event, withElapsedMs(payload));
    },
    markRendered(stage, rowCount = 0) {
      if (startupRenderMetricSent) return;
      startupRenderMetricSent = true;
      emitMetric("jobs_first_render", withElapsedMs({
        stage: String(stage || "unknown"),
        rowCount: Math.max(0, Number(rowCount || 0))
      }));
    },
    markInteractive(reason) {
      if (startupInteractiveMetricSent) return;
      startupInteractiveMetricSent = true;
      emitMetric("jobs_first_interactive", withElapsedMs({
        reason: String(reason || "unknown")
      }));
    }
  };
}

export function createJobsStartupMetrics({
  emitMetric
}) {
  let startupRenderMetricSent = false;
  let startupInteractiveMetricSent = false;
  return {
    emit(event, payload = {}) {
      emitMetric(event, payload);
    },
    markRendered(stage, rowCount = 0) {
      if (startupRenderMetricSent) return;
      startupRenderMetricSent = true;
      emitMetric("jobs_first_render", {
        stage: String(stage || "unknown"),
        rowCount: Math.max(0, Number(rowCount || 0))
      });
    },
    markInteractive(reason) {
      if (startupInteractiveMetricSent) return;
      startupInteractiveMetricSent = true;
      emitMetric("jobs_first_interactive", {
        reason: String(reason || "unknown")
      });
    }
  };
}

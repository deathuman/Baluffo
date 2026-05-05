function safePerformance() {
  return typeof performance !== "undefined" && performance ? performance : null;
}

function emitMetric(startupMetrics, event, payload = {}) {
  if (!startupMetrics || typeof startupMetrics.emit !== "function") return;
  try {
    startupMetrics.emit(event, payload && typeof payload === "object" ? payload : {});
  } catch {
    // Startup metrics are best-effort diagnostics.
  }
}

export function markStep(startupMetrics, name, payload = {}) {
  const event = String(name || "").trim();
  if (!event) return;
  const perf = safePerformance();
  if (typeof perf?.mark === "function") {
    try {
      perf.mark(event);
    } catch {
      // User Timing is best-effort instrumentation.
    }
  }
  emitMetric(startupMetrics, event, payload);
}

export function measureStep(startupMetrics, name, startMark, endMark, payload = {}) {
  const event = String(name || "").trim();
  if (!event) return;
  let durationMs = null;
  const perf = safePerformance();
  if (typeof perf?.measure === "function") {
    try {
      const measure = perf.measure(event, startMark, endMark);
      if (Number.isFinite(Number(measure?.duration))) {
        durationMs = Math.max(0, Math.round(Number(measure.duration)));
      }
      if (durationMs === null && typeof perf.getEntriesByName === "function") {
        const entries = perf.getEntriesByName(event, "measure");
        const latest = Array.isArray(entries) ? entries.at(-1) : null;
        if (Number.isFinite(Number(latest?.duration))) {
          durationMs = Math.max(0, Math.round(Number(latest.duration)));
        }
      }
    } catch {
      // User Timing is best-effort instrumentation.
    }
  }
  emitMetric(startupMetrics, event, {
    ...(
      payload && typeof payload === "object"
        ? payload
        : {}
    ),
    ...(durationMs === null ? {} : { durationMs })
  });
}

export function createPerfMarks(startupMetrics) {
  return {
    markStep: (name, payload = {}) => markStep(startupMetrics, name, payload),
    measureStep: (name, startMark, endMark, payload = {}) => measureStep(startupMetrics, name, startMark, endMark, payload)
  };
}

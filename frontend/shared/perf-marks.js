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

function markStep(startupMetrics, name, payload = {}) {
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

function measureStep(startupMetrics, name, startMark, endMark, payload = {}) {
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

/**
 * Epoch milliseconds for a page-side startup metric, comparable with the launcher's
 * `ts` rows. A page metric otherwise only carries the bridge's arrival time, which a
 * queued metric POST can delay by hundreds of milliseconds -- the startup profile then
 * orders and differences page events at the wrong point on the launch timeline.
 * @param {Function} now - Page clock returning milliseconds since time origin.
 * @returns {number} Epoch milliseconds, rounded.
 */
export function pageClockMs(now) {
  const origin = typeof performance !== "undefined" ? Number(performance.timeOrigin) : 0;
  const elapsed = Number(now());
  const epoch =
    Number.isFinite(origin) && origin > 0
      ? origin + (Number.isFinite(elapsed) ? elapsed : 0)
      : Date.now();
  return Math.round(epoch);
}

export function createPerfMarks(startupMetrics) {
  return {
    markStep: (name, payload = {}) => markStep(startupMetrics, name, payload),
    measureStep: (name, startMark, endMark, payload = {}) => measureStep(startupMetrics, name, startMark, endMark, payload)
  };
}

/**
 * Builds a payload decorator that stamps elapsedMs since the given start.
 * An existing elapsedMs on the payload is preserved untouched.
 * @param {Function} now - Clock returning milliseconds.
 * @param {number} startedAtMs - Start of the measured window.
 * @returns {(payload?: object) => object}
 */
export function createElapsedMsTracker(now, startedAtMs) {
  return function withElapsedMs(payload = {}) {
    if (Object.prototype.hasOwnProperty.call(payload, "elapsedMs")) return payload;
    return {
      ...payload,
      elapsedMs: Math.max(0, Math.round((Number(now()) || startedAtMs) - startedAtMs))
    };
  };
}

/**
 * Shared boot/runtime glue used by multiple app entrypoints.
 *
 * Intentionally does NOT try to unify domain/page logic; it only provides small,
 * repeated primitives for logging and startup-metrics wiring.
 */

export function emitStartupMetric(startupMetrics, event, payload = {}) {
  if (!startupMetrics || typeof startupMetrics.emit !== "function") return;
  startupMetrics.emit(event, payload && typeof payload === "object" ? payload : {});
}

export function markFirstInteractive(startupMetrics, reason) {
  if (!startupMetrics) return;
  if (typeof startupMetrics.markFirstInteractive === "function") {
    startupMetrics.markFirstInteractive(reason);
    return;
  }
  if (typeof startupMetrics.markInteractive === "function") {
    startupMetrics.markInteractive(reason);
    return;
  }
}

export function logInfo(scope, message, ...args) {
  console.info(`[${scope}] ${message}`, ...args);
}

export function logError(scope, context, err) {
  console.error(`[${scope}] ${context}:`, err);
}

export function createAdminStartupMetrics({
  emitStartupMetric,
  now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now())
}) {
  let firstInteractiveSent = false;
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
      emitStartupMetric(event, withElapsedMs(payload));
    },
    markFirstInteractive(reason) {
      if (firstInteractiveSent) return;
      firstInteractiveSent = true;
      emitStartupMetric("admin_first_interactive", withElapsedMs({
        reason: String(reason || "unknown")
      }));
    }
  };
}

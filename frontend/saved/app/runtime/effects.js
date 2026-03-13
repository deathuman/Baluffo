export function createSavedStartupMetrics({
  emitMetric,
  now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now())
}) {
  let sent = false;
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
    markFirstInteractive(reason) {
      if (sent) return;
      sent = true;
      emitMetric("saved_first_interactive", withElapsedMs({
        reason: String(reason || "unknown")
      }));
    }
  };
}

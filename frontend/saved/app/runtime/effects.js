export function createSavedStartupMetrics({
  emitMetric
}) {
  let sent = false;
  return {
    emit(event, payload = {}) {
      emitMetric(event, payload);
    },
    markFirstInteractive(reason) {
      if (sent) return;
      sent = true;
      emitMetric("saved_first_interactive", {
        reason: String(reason || "unknown")
      });
    }
  };
}

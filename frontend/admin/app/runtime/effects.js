export function createAdminStartupMetrics({
  emitStartupMetric
}) {
  let firstInteractiveSent = false;
  return {
    emit(event, payload = {}) {
      emitStartupMetric(event, payload);
    },
    markFirstInteractive(reason) {
      if (firstInteractiveSent) return;
      firstInteractiveSent = true;
      emitStartupMetric("admin_first_interactive", {
        reason: String(reason || "unknown")
      });
    }
  };
}

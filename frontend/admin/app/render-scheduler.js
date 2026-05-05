export function scheduleAdminRender(callback, {
  timeoutMs = 500,
  fallbackDelayMs = 50
} = {}) {
  if (typeof callback !== "function") {
    return () => {};
  }
  let cancelled = false;
  const run = () => {
    if (!cancelled) callback();
  };
  if (typeof globalThis.requestIdleCallback === "function") {
    const idleId = globalThis.requestIdleCallback(run, { timeout: timeoutMs });
    return () => {
      cancelled = true;
      globalThis.cancelIdleCallback?.(idleId);
    };
  }
  const timerId = globalThis.setTimeout(run, Math.max(0, Number(fallbackDelayMs) || 0));
  return () => {
    cancelled = true;
    globalThis.clearTimeout?.(timerId);
  };
}

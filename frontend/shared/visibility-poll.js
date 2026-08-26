export function createVisibilityPausedInterval(callback, delayMs, windowObject = globalThis) {
  let timer = null;
  let stopped = false;
  const start = () => {
    if (stopped || timer !== null) return;
    if (windowObject.document?.hidden) return;
    timer = windowObject.setInterval(callback, delayMs);
    timer?.unref?.();
  };
  const stop = () => {
    stopped = true;
    if (timer !== null) {
      windowObject.clearInterval(timer);
      timer = null;
    }
  };
  const onVisibility = () => {
    if (windowObject.document?.hidden) {
      if (timer !== null) {
        windowObject.clearInterval(timer);
        timer = null;
      }
    } else {
      start();
    }
  };
  start();
  windowObject.addEventListener?.("visibilitychange", onVisibility);
  return { stop };
}

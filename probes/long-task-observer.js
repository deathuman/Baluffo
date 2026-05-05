const NOOP_HANDLE = Object.freeze({
  disconnect() {}
});

function resolvePerformanceObserver(performanceObserver) {
  if (performanceObserver !== undefined) {
    return performanceObserver;
  }
  return typeof globalThis !== "undefined" ? globalThis.PerformanceObserver : null;
}

function supportsLongTask(performanceObserver) {
  const supportedEntryTypes = performanceObserver?.supportedEntryTypes;
  if (!Array.isArray(supportedEntryTypes)) {
    return true;
  }
  return supportedEntryTypes.includes("longtask");
}

function normalizeNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? Math.max(0, Math.round(numeric)) : 0;
}

function normalizeAttribution(attribution) {
  if (!Array.isArray(attribution)) {
    return [];
  }
  return attribution.map(item => ({
    name: String(item?.name || ""),
    durationMs: normalizeNumber(item?.duration),
    containerType: String(item?.containerType || ""),
    containerName: String(item?.containerName || ""),
    containerId: String(item?.containerId || "")
  }));
}

function normalizeLongTaskEntry(entry) {
  return {
    durationMs: normalizeNumber(entry?.duration),
    startTimeMs: normalizeNumber(entry?.startTime),
    name: String(entry?.name || ""),
    entryType: String(entry?.entryType || "longtask"),
    attribution: normalizeAttribution(entry?.attribution)
  };
}

export function observeLongTasks({
  page = "page",
  emitMetric,
  performanceObserver
} = {}) {
  const pageName = String(page || "page").trim() || "page";
  const Observer = resolvePerformanceObserver(performanceObserver);
  if (typeof Observer !== "function" || !supportsLongTask(Observer)) {
    return NOOP_HANDLE;
  }

  let observer = null;
  try {
    observer = new Observer(list => {
      const entries = typeof list?.getEntries === "function" ? list.getEntries() : [];
      for (const entry of entries || []) {
        try {
          emitMetric?.(`${pageName}_long_task`, normalizeLongTaskEntry(entry));
        } catch {
          // Long Task telemetry is diagnostic-only and must never affect page boot.
        }
      }
    });
    observer.observe({ type: "longtask", buffered: true });
  } catch {
    return NOOP_HANDLE;
  }

  return {
    disconnect() {
      try {
        observer?.disconnect?.();
      } catch {
        // Best-effort cleanup only.
      }
    }
  };
}

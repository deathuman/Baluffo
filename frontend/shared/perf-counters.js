const MAX_SAMPLES_PER_COUNTER = 200;

function nowMs() {
  return typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();
}

function normalizeCounterName(name) {
  return String(name || "frontend_unknown")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    || "frontend_unknown";
}

function getCounterStore() {
  if (!globalThis.__baluffoFrontendPerfCounters) {
    globalThis.__baluffoFrontendPerfCounters = {};
  }
  return globalThis.__baluffoFrontendPerfCounters;
}

export function recordFrontendDuration(name, durationMs, detail = {}) {
  const key = normalizeCounterName(name);
  const store = getCounterStore();
  if (!store[key]) {
    store[key] = [];
  }
  const sample = {
    durationMs: Math.max(0, Math.round(Number(durationMs) || 0)),
    ts: Date.now(),
    detail: detail && typeof detail === "object" ? { ...detail } : {}
  };
  store[key].push(sample);
  if (store[key].length > MAX_SAMPLES_PER_COUNTER) {
    store[key].splice(0, store[key].length - MAX_SAMPLES_PER_COUNTER);
  }
  return sample;
}

export async function timeFrontendAsync(name, fn, detail = {}) {
  const startedAt = nowMs();
  try {
    return await fn();
  } finally {
    recordFrontendDuration(name, nowMs() - startedAt, detail);
  }
}

export function timeFrontendSync(name, fn, detail = {}) {
  const startedAt = nowMs();
  try {
    return fn();
  } finally {
    recordFrontendDuration(name, nowMs() - startedAt, detail);
  }
}

export function setTimedInnerHTML(element, html, name, detail = {}) {
  return timeFrontendSync(name, () => {
    if (element) {
      element.innerHTML = html;
    }
  }, detail);
}

function percentile(sorted, ratio) {
  if (!sorted.length) {
    return 0;
  }
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index];
}

export function snapshotFrontendPerfCounters() {
  const snapshot = {};
  const store = getCounterStore();
  Object.entries(store).forEach(([key, samples]) => {
    const durations = Array.isArray(samples)
      ? samples.map(row => Math.max(0, Math.round(Number(row?.durationMs) || 0))).sort((a, b) => a - b)
      : [];
    if (!durations.length) {
      return;
    }
    const sumMs = durations.reduce((sum, value) => sum + value, 0);
    snapshot[key] = {
      count: durations.length,
      minMs: durations[0],
      maxMs: durations[durations.length - 1],
      sumMs,
      avgMs: Math.round(sumMs / durations.length),
      p50Ms: percentile(durations, 0.5),
      p95Ms: percentile(durations, 0.95)
    };
  });
  return snapshot;
}

globalThis.__baluffoSnapshotFrontendPerfCounters = snapshotFrontendPerfCounters;

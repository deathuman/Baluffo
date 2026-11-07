function isPayload(value) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

/**
 * Shares `/ops/fetch-kpis?view=summary` between the Ops surface and the Action
 * Center. Both consumers read the same payload shape, so one network refresh is
 * enough: concurrent calls join the in-flight request and every subscriber sees
 * the result.
 */
export function createFetchKpisSummaryFeed({ now = () => Date.now() } = {}) {
  const listeners = new Set();
  let inFlight = null;
  let lastPayload = null;
  let lastLoadedAt = 0;

  function publish(payload) {
    lastPayload = payload;
    lastLoadedAt = now();
    for (const listener of listeners) {
      try {
        listener(payload);
      } catch {
        // One broken consumer must not stop the other surface from updating.
      }
    }
  }

  function load(fetchPayload, { maxAgeMs = 0 } = {}) {
    const cachedForMs = Math.max(0, Number(maxAgeMs) || 0);
    if (cachedForMs > 0 && lastPayload && (now() - lastLoadedAt) <= cachedForMs) {
      return Promise.resolve(lastPayload);
    }
    if (!inFlight) {
      let request;
      try {
        request = fetchPayload();
      } catch (err) {
        request = Promise.reject(err);
      }
      inFlight = Promise.resolve(request)
        .then(payload => {
          if (isPayload(payload)) publish(payload);
          return payload;
        })
        .finally(() => {
          inFlight = null;
        });
    }
    return inFlight;
  }

  function subscribe(listener) {
    if (typeof listener !== "function") return () => {};
    listeners.add(listener);
    return () => listeners.delete(listener);
  }

  return { load, subscribe };
}
